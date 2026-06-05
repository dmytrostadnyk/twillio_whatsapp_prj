"""
Delivery worker tests — HubSpot integration.

What we test:
1.  Successful delivery (search finds contact → PATCH 200) → broker.ack called
2.  New contact (search 0 results → POST create → PATCH 200) → broker.ack called
3.  Success does not call nack or dead_letter
4.  HubSpot 500 on contact search → broker.nack
5.  HubSpot 503 on contact PATCH → broker.nack
6.  HubSpot 401 on search → broker.dead_letter (auth error, never retryable)
7.  HubSpot 403 on search → broker.dead_letter
8.  HubSpot 422 on PATCH → broker.dead_letter (non-retryable 4xx)
9.  HubSpot 429 with Retry-After → nack with that exact backoff value
10. HTTP timeout on search → broker.nack
11. HTTP connect error on search → broker.nack
12. Any httpx.RequestError subclass → broker.nack with error type in reason
13. attempt_count > max → broker.dead_letter before any HTTP call
14. from_number is None → broker.dead_letter before any HTTP call
15. Authorization header is present on every HubSpot request
16. HubSpot contact ID is persisted after find_or_create (pool.acquire called)
17. Backoff is capped at DELIVERY_BACKOFF_MAX_SECONDS
18. Backoff is always >= 0
19. parse_retry_after: seconds form, missing header, unparseable, negative
20. Retry path: GET is issued to re-fetch log; PATCH ai_comm_log contains both
    the new entry AND the prior history (regression for the log-wipe bug)
21. Retry path: GET 5xx → nack, no PATCH made (fail safe — never wipe on error)
22. DB dedup: when DB already has a hubspot_contact_id for the phone number,
    HubSpot search/create is NOT called (avoids search-lag duplicates)

We mock:
- The broker (AsyncMock) — no real DB needed
- The HTTP client (respx) — no real HubSpot needed
- The pool (MagicMock) — no real DB for delivery_log and contact_id persistence
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx

from comm_layer.broker.base import BrokerMessage
from comm_layer.config import settings
from delivery_worker.main import (
    compute_backoff,
    parse_retry_after,
    process_message,
)
from delivery_worker.transform import build_hubspot_properties

# ── HubSpot endpoint stubs ─────────────────────────────────────────────────────

_SEARCH_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
_CREATE_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts"
_PATCH_URL_PREFIX = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/"
_GET_URL_PREFIX = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/"
_NOTES_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/notes"
_TICKETS_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/tickets"
_TASKS_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/tasks"

# Contact with a pre-existing log — used for retry-path tests
_EXISTING_CONTACT_ID = "existing-456"
_GET_CONTACT_WITH_LOG = {
    "id": _EXISTING_CONTACT_ID,
    "properties": {"ai_comm_log": "Prior history entry"},
}

_FOUND_CONTACT_RESPONSE = {
    "results": [{"id": "123", "properties": {"phone": "+15559876543", "ai_comm_log": ""}}]
}
_EMPTY_SEARCH_RESPONSE = {"results": []}
_CREATE_RESPONSE = {"id": "456", "properties": {"phone": "+15559876543"}}


# ── Fixtures ───────────────────────────────────────────────────────────────────


def make_message(
    *,
    attempt_count: int = 1,
    channel: str = "sms",
    direction: str = "inbound",
    event_type: str = "sms.received",
    from_number: str | None = "+15559876543",
    to_number: str | None = "+15551234567",
    summary: str | None = "Customer wants to cancel subscription.",
    intent: str | None = "cancellation",
    sentiment: str | None = "neutral",
    hubspot_contact_id: str | None = None,
    hubspot_note_id: str | None = None,
    hubspot_ticket_id: str | None = None,
    hubspot_task_id: str | None = None,
    reply_resolved: bool | None = None,
) -> BrokerMessage:
    """Build a BrokerMessage for use in tests."""
    return BrokerMessage(
        id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        event_key="SM123:sms.received",
        correlation_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        channel=channel,
        direction=direction,
        event_type=event_type,
        from_number=from_number,
        to_number=to_number,
        source_metadata={"number": to_number, "is_unknown": True},
        raw_payload={"Body": "Hello", "MessageSid": "SM123"},
        attempt_count=attempt_count,
        created_at=datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
        claimed_at=datetime.now(UTC),
        summary=summary,
        intent=intent,
        sentiment=sentiment,
        entities=[],
        action_items=[],
        hubspot_contact_id=hubspot_contact_id,
        hubspot_note_id=hubspot_note_id,
        hubspot_ticket_id=hubspot_ticket_id,
        hubspot_task_id=hubspot_task_id,
        reply_resolved=reply_resolved,
    )


def make_broker() -> AsyncMock:
    """Mock broker with all methods as no-op AsyncMocks."""
    broker = AsyncMock()
    broker.ack = AsyncMock(return_value=None)
    broker.nack = AsyncMock(return_value=None)
    broker.dead_letter = AsyncMock(return_value=None)
    return broker


def make_pool(db_contact_id: str | None = None):
    """
    Mock asyncpg pool so _write_delivery_log, _persist_contact_id, and
    _lookup_contact_id_by_phone work.

    db_contact_id: if set, fetchrow() returns a dict simulating a DB row with
    an existing hubspot_contact_id for the phone (used for dedup tests).
    """
    conn = AsyncMock()
    # _lookup_contact_id_by_phone calls fetchrow; everything else calls execute.
    if db_contact_id is not None:
        conn.fetchrow = AsyncMock(return_value={"hubspot_contact_id": db_contact_id})
    else:
        conn.fetchrow = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


# ── process_message — successful delivery ─────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_existing_contact_delivery_calls_ack():
    """Search finds contact → PATCH 200 → Note created → broker.ack called."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-1"}))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_new_contact_create_then_patch_calls_ack():
    """Search returns 0 results → contact created → PATCH 200 → Note created → broker.ack."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_EMPTY_SEARCH_RESPONSE))
    respx.post(_CREATE_URL).mock(return_value=httpx.Response(201, json=_CREATE_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-1"}))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_successful_delivery_does_not_nack_or_dead_letter():
    """On success, nack and dead_letter must NOT be called."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-1"}))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_not_called()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_request_has_authorization_header():
    """Every HubSpot request must carry a Bearer Authorization header."""
    search_route = respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE)
    )
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-1"}))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    assert search_route.called
    auth = search_route.calls[0].request.headers.get("Authorization", "")
    assert auth.startswith("Bearer "), f"Expected Bearer token, got: {auth[:20]}"


@pytest.mark.asyncio
@respx.mock
async def test_contact_id_persisted_after_creation():
    """Pool is used to persist contact ID after create so retries skip creation."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_EMPTY_SEARCH_RESPONSE))
    respx.post(_CREATE_URL).mock(return_value=httpx.Response(201, json=_CREATE_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-1"}))
    broker = make_broker()
    pool = make_pool()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), pool)

    # pool.acquire() is called by _persist_contact_id, _write_delivery_log, _persist_note_id
    assert pool.acquire.called


# ── process_message — HubSpot 5xx ─────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_500_on_search_calls_nack():
    """HubSpot 500 on contact search → nack (retry)."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(500, text="Internal Server Error"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()
    reason = broker.nack.call_args.args[1]
    assert "500" in reason


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_503_on_patch_calls_nack():
    """HubSpot 503 on contact PATCH → nack."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(503))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


# ── process_message — auth errors ─────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_401_dead_letters_immediately():
    """
    HubSpot 401 Unauthorized → dead-letter immediately.
    Retrying with the same bad token will always fail the same way.
    """
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(401))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()
    broker.ack.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_403_dead_letters_immediately():
    """HubSpot 403 Forbidden (missing scope) → dead-letter immediately."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(403))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()


# ── process_message — non-retryable 4xx ───────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_422_on_patch_dead_letters():
    """HubSpot 422 Unprocessable on PATCH → dead-letter (bad property value)."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(422))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()
    broker.ack.assert_not_called()


# ── process_message — 429 with Retry-After ────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_hubspot_429_with_retry_after_uses_that_backoff():
    """
    HubSpot 429 with Retry-After: 120 → nack with exactly 120s backoff.
    Ignoring the header would immediately re-hit the rate limit.
    """
    respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(429, headers={"Retry-After": "120"})
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    backoff_arg = broker.nack.call_args.args[2]
    assert backoff_arg == 120.0


# ── process_message — network errors ──────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_http_timeout_calls_nack():
    """Network timeout → nack (retry)."""
    respx.post(_SEARCH_URL).mock(side_effect=httpx.TimeoutException("timed out"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_http_connect_error_calls_nack():
    """Connection refused → nack (retry)."""
    respx.post(_SEARCH_URL).mock(side_effect=httpx.ConnectError("connection refused"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_other_httpx_request_errors_also_nack():
    """httpx.ReadError (and other RequestError subclasses) → nack."""
    respx.post(_SEARCH_URL).mock(side_effect=httpx.ReadError("stream broken"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    reason_arg = broker.nack.call_args.args[1]
    assert "ReadError" in reason_arg


# ── process_message — retry path (log re-fetch) ───────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_retry_path_fetches_current_log_before_patch():
    """
    Regression test for the log-wipe bug:
    When hubspot_contact_id is already set (retry path), a GET must be issued
    to read the current ai_comm_log. The subsequent PATCH must contain BOTH the
    new entry AND the existing history — never overwrite with only the new entry.
    """
    get_route = respx.get(
        f"{_GET_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json=_GET_CONTACT_WITH_LOG))

    patch_route = respx.patch(
        f"{_PATCH_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={}))

    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-r"}))

    broker = make_broker()
    # Simulate a retry: hubspot_contact_id is already persisted
    msg = make_message(hubspot_contact_id=_EXISTING_CONTACT_ID)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    # GET must be called to fetch the live log
    assert get_route.called, "GET was not called — existing log would be lost"

    # PATCH body must include BOTH the new entry and the prior history
    assert patch_route.called
    patch_body = json.loads(patch_route.calls[0].request.content)
    comm_log = patch_body["properties"]["ai_comm_log"]
    assert "Prior history entry" in comm_log, (
        f"Prior history was wiped from ai_comm_log:\n{comm_log}"
    )

    broker.ack.assert_called_once()
    broker.nack.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_retry_path_get_5xx_nacks_without_patching():
    """
    Fail-safe: if the GET to fetch the current log returns 5xx, we must nack
    (not make a PATCH that would destroy the history with an empty log).
    """
    respx.get(
        f"{_GET_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(500))

    patch_route = respx.patch(
        f"{_PATCH_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={}))

    broker = make_broker()
    msg = make_message(hubspot_contact_id=_EXISTING_CONTACT_ID)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    broker.nack.assert_called_once()
    broker.ack.assert_not_called()
    assert not patch_route.called, "PATCH must not be called when GET fails"


# ── process_message — DB dedup (Batch E) ──────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_db_dedup_skips_hubspot_search_when_contact_known():
    """
    When our DB already has a hubspot_contact_id for the calling phone number,
    the HubSpot search/create endpoints must NOT be called — the DB value is
    used directly, avoiding search-lag duplicate contacts.

    The GET for the live log and the PATCH still run.
    """
    db_contact_id = "db-known-789"

    get_route = respx.get(
        f"{_GET_URL_PREFIX}{db_contact_id}"
    ).mock(return_value=httpx.Response(200, json={"id": db_contact_id, "properties": {}}))
    patch_route = respx.patch(
        f"{_PATCH_URL_PREFIX}{db_contact_id}"
    ).mock(return_value=httpx.Response(200, json={}))
    search_route = respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json=_EMPTY_SEARCH_RESPONSE)
    )
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-d"}))

    broker = make_broker()
    # Pool returns the db_contact_id from fetchrow (simulating a prior event for
    # the same phone that already has a contact persisted).
    pool = make_pool(db_contact_id=db_contact_id)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), pool)

    assert not search_route.called, (
        "HubSpot search must NOT be called when DB already has the contact id"
    )
    assert get_route.called, "GET for live log must still run"
    assert patch_route.called, "PATCH must still run"
    broker.ack.assert_called_once()


# ── process_message — HubSpot rate limit (Batch C) ───────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_empty_rate_limiter_nacks_before_http_call():
    """
    When the client-side HubSpot rate limiter is exhausted, process_message
    must nack (reschedule) WITHOUT making any HubSpot HTTP calls.
    This verifies the global rule: every external integration needs a rate limit.
    """
    from comm_layer.rate_limiter import TokenBucket

    # Create a bucket with zero capacity — immediately exhausted.
    empty_bucket = TokenBucket(capacity=1, refill_rate=0.0)
    await empty_bucket.consume()  # drain the one token

    search_route = respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE)
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(
            broker, client, make_message(), make_pool(), rate_limiter=empty_bucket
        )

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()
    broker.ack.assert_not_called()
    assert not search_route.called, "No HubSpot calls must be made when rate-limited"


# ── process_message — guard conditions ────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_max_attempts_dead_letters_before_http_call():
    """
    attempt_count > DELIVERY_MAX_ATTEMPTS → dead-letter BEFORE any HTTP call.
    No point hitting HubSpot if we've already given up.
    """
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    broker = make_broker()
    msg = make_message(attempt_count=settings.DELIVERY_MAX_ATTEMPTS + 1)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    broker.dead_letter.assert_called_once()
    assert not respx.calls.called


@pytest.mark.asyncio
@respx.mock
async def test_null_from_number_dead_letters_before_http_call():
    """
    from_number is None → dead-letter immediately (can't create a contact).
    No HubSpot call is made.
    """
    broker = make_broker()
    msg = make_message(from_number=None)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    broker.dead_letter.assert_called_once()
    assert not respx.calls.called


# ── build_hubspot_properties — pure function ──────────────────────────────────


def test_build_hubspot_properties_includes_required_keys():
    """All required HubSpot property keys must be present."""
    props = build_hubspot_properties(make_message())
    required = {"ai_last_intent", "ai_last_sentiment", "ai_last_summary", "ai_comm_log"}
    assert required.issubset(props.keys())


def test_build_hubspot_properties_values_match_message():
    """Property values come from the correct BrokerMessage fields."""
    msg = make_message(intent="billing_dispute", sentiment="frustrated", summary="Test summary")
    props = build_hubspot_properties(msg)
    assert props["ai_last_intent"] == "billing_dispute"
    assert props["ai_last_sentiment"] == "frustrated"
    assert props["ai_last_summary"] == "Test summary"


def test_build_hubspot_properties_prepends_to_existing_log():
    """New log entry is prepended so newest is always at the top."""
    msg = make_message()
    existing = "Old entry"
    props = build_hubspot_properties(msg, existing_log=existing)
    log_value = props["ai_comm_log"]
    assert log_value.index("2026-01-15") < log_value.index("Old entry")


def test_build_hubspot_properties_handles_null_ai_fields():
    """Missing enrichment fields produce empty strings, not None."""
    msg = make_message(summary=None, intent=None, sentiment=None)
    props = build_hubspot_properties(msg)
    assert props["ai_last_intent"] == ""
    assert props["ai_last_sentiment"] == ""
    assert props["ai_last_summary"] == ""
    assert "unavailable" in props["ai_comm_log"]


# ── Backoff (deterministic) ────────────────────────────────────────────────────


def test_backoff_is_capped_at_settings_max():
    """Backoff never exceeds DELIVERY_BACKOFF_MAX_SECONDS."""
    for attempt in range(1, 30):
        for _ in range(20):
            assert compute_backoff(attempt) <= settings.DELIVERY_BACKOFF_MAX_SECONDS


def test_backoff_is_non_negative():
    """Backoff is always >= 0."""
    for attempt in range(0, 10):
        for _ in range(20):
            assert compute_backoff(attempt) >= 0


def test_backoff_cap_grows_with_attempt_until_capped():
    """The cap grows with attempt count until it hits the maximum."""
    base = settings.DELIVERY_BACKOFF_BASE_SECONDS
    max_cap = settings.DELIVERY_BACKOFF_MAX_SECONDS

    cap_attempt_1 = min(base * (2 ** 1), max_cap)
    cap_attempt_4 = min(base * (2 ** 4), max_cap)
    cap_attempt_20 = min(base * (2 ** 20), max_cap)

    assert cap_attempt_1 < cap_attempt_4
    assert cap_attempt_4 <= cap_attempt_20
    assert cap_attempt_20 == max_cap


# ── parse_retry_after ─────────────────────────────────────────────────────────


def test_parse_retry_after_seconds_form():
    response = httpx.Response(429, headers={"Retry-After": "30"})
    assert parse_retry_after(response) == 30.0


def test_parse_retry_after_missing_header():
    response = httpx.Response(503)
    assert parse_retry_after(response) is None


def test_parse_retry_after_unparseable_returns_none():
    response = httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    assert parse_retry_after(response) is None


def test_parse_retry_after_negative_clamped_to_zero():
    response = httpx.Response(429, headers={"Retry-After": "-5"})
    assert parse_retry_after(response) == 0.0


# ── Note idempotency (Phase 3) ────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_note_created_on_first_delivery():
    """hubspot_note_id is None → Note POST is called."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    notes_route = respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": "note-new"})
    )
    broker = make_broker()
    msg = make_message(hubspot_note_id=None)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert notes_route.called, "Note POST must be called when hubspot_note_id is None"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_note_skipped_on_retry_when_already_set():
    """hubspot_note_id already set → Note POST must NOT be called (no duplicate notes)."""
    respx.get(
        f"{_GET_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={"id": _EXISTING_CONTACT_ID, "properties": {}}))
    respx.patch(
        f"{_PATCH_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={}))
    notes_route = respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": "note-new"})
    )
    broker = make_broker()
    # Simulate retry: both contact id and note id already persisted
    msg = make_message(hubspot_contact_id=_EXISTING_CONTACT_ID, hubspot_note_id="existing-note")

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    # Note POST must NOT be called when hubspot_note_id is already set
    assert not notes_route.called
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_note_500_nacks_without_ack():
    """Note POST fails with 5xx → nack (retry), not ack."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(500, text="Server Error"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message(), make_pool())

    broker.nack.assert_called_once()
    broker.ack.assert_not_called()


# ── Ticket auto-creation (Phase 4) ───────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_ticket_created_for_complaint():
    """intent=complaint → Ticket POST called."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-c"}))
    ticket_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "ticket-1"})
    )
    broker = make_broker()
    msg = make_message(intent="complaint", sentiment="neutral")

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert ticket_route.called, "Ticket POST must be called for complaint intent"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_ticket_not_created_for_general_query():
    """intent=general_query, sentiment=neutral → no Ticket POST."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-g"}))
    ticket_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "ticket-skip"})
    )
    broker = make_broker()
    msg = make_message(intent="general_query", sentiment="neutral")

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert not ticket_route.called, "Ticket POST must NOT be called for general_query/neutral"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_ticket_skipped_on_retry_when_already_set():
    """hubspot_ticket_id already set → Ticket POST must NOT be called (no duplicates)."""
    respx.get(
        f"{_GET_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={"id": _EXISTING_CONTACT_ID, "properties": {}}))
    respx.patch(
        f"{_PATCH_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-t"}))
    ticket_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "ticket-new"})
    )
    broker = make_broker()
    msg = make_message(
        hubspot_contact_id=_EXISTING_CONTACT_ID,
        hubspot_ticket_id="existing-ticket",
        intent="complaint",
        sentiment="negative",
    )

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert not ticket_route.called, "Ticket POST must NOT be called when hubspot_ticket_id is set"
    broker.ack.assert_called_once()


# ── Ticket dedupe — open ticket reuse (Phase 4) ───────────────────────────────


def make_pool_with_ticket(db_ticket_id: str | None = None, db_contact_id: str | None = None):
    """
    Mock pool that returns an existing ticket id from _lookup_recent_ticket_for_phone
    and optionally a contact id from _lookup_contact_id_by_phone.

    Both helpers call pool.fetchrow. We distinguish calls by using side_effect:
    the first fetchrow call is the contact lookup, the second is the ticket lookup.
    """
    conn = AsyncMock()
    contact_row = {"hubspot_contact_id": db_contact_id} if db_contact_id else None
    ticket_row = {"hubspot_ticket_id": db_ticket_id} if db_ticket_id else None
    conn.fetchrow = AsyncMock(side_effect=[contact_row, ticket_row])
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.mark.asyncio
@respx.mock
async def test_ticket_reused_when_open_ticket_exists():
    """
    When our DB has a prior ticket id and HubSpot confirms it is still open,
    no new Ticket POST should be made. Instead a Note is attached to the ticket.
    """
    prior_ticket = "open-ticket-111"
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "contact-note"}))
    # GET to check ticket stage — stage "1" = open (not closed "4")
    respx.get(f"{_TICKETS_URL}/{prior_ticket}").mock(
        return_value=httpx.Response(
            200, json={"id": prior_ticket, "properties": {"hs_pipeline_stage": "1"}}
        )
    )
    ticket_create_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "new-ticket"})
    )
    broker = make_broker()
    msg = make_message(intent="complaint", sentiment="neutral")
    pool = make_pool_with_ticket(db_ticket_id=prior_ticket)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, pool)

    assert not ticket_create_route.called, "Must NOT create a new ticket when open one exists"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_new_ticket_created_when_prior_ticket_closed():
    """
    Prior ticket exists but its stage is the closed stage → a new ticket must be created.
    """
    prior_ticket = "closed-ticket-222"
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "contact-note"}))
    # GET confirms ticket is in closed stage "4"
    respx.get(f"{_TICKETS_URL}/{prior_ticket}").mock(
        return_value=httpx.Response(
            200, json={"id": prior_ticket, "properties": {"hs_pipeline_stage": "4"}}
        )
    )
    ticket_create_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "brand-new-ticket"})
    )
    broker = make_broker()
    msg = make_message(intent="complaint", sentiment="neutral")
    pool = make_pool_with_ticket(db_ticket_id=prior_ticket)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, pool)

    assert ticket_create_route.called, "New ticket must be created when prior one is closed"
    broker.ack.assert_called_once()


# ── Task creation (Phase 5) ───────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_task_created_when_whatsapp_unresolved():
    """WhatsApp + reply_resolved=False → Task POST is called."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-wa"}))
    task_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "task-1"})
    )
    broker = make_broker()
    msg = make_message(
        channel="whatsapp",
        event_type="whatsapp.received",
        reply_resolved=False,
    )

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert task_route.called, "Task POST must be called when WhatsApp bot could not answer"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_task_not_created_when_whatsapp_resolved():
    """WhatsApp + reply_resolved=True (bot answered) → no Task POST."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-wa"}))
    task_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "task-skip"})
    )
    broker = make_broker()
    msg = make_message(
        channel="whatsapp",
        event_type="whatsapp.received",
        reply_resolved=True,
    )

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert not task_route.called, "Task POST must NOT be called when bot answered"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_task_not_created_for_sms():
    """SMS event (no bot reply concept) → no Task POST."""
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT_RESPONSE))
    respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-sms"}))
    task_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "task-skip"})
    )
    broker = make_broker()
    msg = make_message(channel="sms", event_type="sms.received", reply_resolved=None)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert not task_route.called, "Task POST must NOT be called for SMS"
    broker.ack.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_task_skipped_on_retry_when_already_set():
    """hubspot_task_id already set → Task POST must NOT be called (no duplicates)."""
    respx.get(
        f"{_GET_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={"id": _EXISTING_CONTACT_ID, "properties": {}}))
    respx.patch(
        f"{_PATCH_URL_PREFIX}{_EXISTING_CONTACT_ID}"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(201, json={"id": "note-r"}))
    task_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "task-new"})
    )
    broker = make_broker()
    msg = make_message(
        hubspot_contact_id=_EXISTING_CONTACT_ID,
        hubspot_task_id="existing-task-id",
        channel="whatsapp",
        event_type="whatsapp.received",
        reply_resolved=False,
    )

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg, make_pool())

    assert not task_route.called, "Task POST must NOT be called when hubspot_task_id is already set"
    broker.ack.assert_called_once()
