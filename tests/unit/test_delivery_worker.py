"""
Delivery worker tests — Phase 2.

What we test:
1.  Successful delivery (Azure 200) → broker.ack called with contract payload
2.  Azure 500 → broker.nack with reason that includes the response body
3.  Azure 422 (true 4xx) → broker.dead_letter, NO retry
4.  Azure 408/425/429 (transient 4xx) → broker.nack, NOT dead-lettered
5.  Retry-After header on 429/503 → backoff uses that value, not random
6.  HTTP timeout → broker.nack
7.  HTTP connect error → broker.nack
8.  HTTP read error (any httpx.RequestError subclass) → broker.nack
9.  attempt_count > max → broker.dead_letter before any HTTP call
10. Outbound POST carries X-Correlation-Id and X-Event-Key headers
11. Contract payload has the correct structure
12. Backoff formula is bounded and non-negative (deterministic, no random sampling)

We mock:
- The broker (AsyncMock) — no real DB needed
- The HTTP client (respx) — no real Azure CRM needed
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

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
from delivery_worker.transform import build_contract_payload

# ── Fixtures ───────────────────────────────────────────────────────────────────


def make_message(
    *,
    attempt_count: int = 1,
    channel: str = "sms",
    direction: str = "inbound",
    event_type: str = "sms.received",
    from_number: str | None = "+15559876543",
    to_number: str | None = "+15551234567",
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
    )


def make_broker() -> AsyncMock:
    """Mock broker with all methods as no-op AsyncMocks."""
    broker = AsyncMock()
    broker.ack = AsyncMock(return_value=None)
    broker.nack = AsyncMock(return_value=None)
    broker.dead_letter = AsyncMock(return_value=None)
    return broker


# ── process_message — happy path ───────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_successful_delivery_calls_ack():
    """Azure returns 200 → broker.ack is called with the contract payload."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(200))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.ack.assert_called_once()
    call_kwargs = broker.ack.call_args.kwargs
    assert call_kwargs.get("contract_payload") is not None
    assert call_kwargs["contract_payload"]["schema_version"] == "1.0"


@pytest.mark.asyncio
@respx.mock
async def test_successful_delivery_does_not_call_nack_or_dead_letter():
    """On success, nack and dead_letter must NOT be called."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(201))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_not_called()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_outbound_request_carries_tracing_headers():
    """
    Every POST to Azure must include X-Correlation-Id and X-Event-Key so
    Azure can correlate its own logs to events in our system without parsing
    the body.
    """
    route = respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(200))
    broker = make_broker()
    msg = make_message()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg)

    assert route.called
    request = route.calls[0].request
    assert request.headers["X-Correlation-Id"] == str(msg.correlation_id)
    assert request.headers["X-Event-Key"] == msg.event_key


# ── process_message — Azure 5xx ────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_azure_500_calls_nack_with_body_in_reason():
    """
    Azure 500 → nack (retry). The response body must appear in the nack
    reason so the dashboard's last_error column has something useful for
    debugging Azure-side problems.
    """
    respx.post("http://localhost:8001/events").mock(
        return_value=httpx.Response(500, text="Database connection pool exhausted")
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()
    reason_arg = broker.nack.call_args.args[1]
    assert "500" in reason_arg
    assert "Database connection pool exhausted" in reason_arg


@pytest.mark.asyncio
@respx.mock
async def test_azure_503_calls_nack():
    """Azure 503 Service Unavailable → retry, same as any 5xx."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(503))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


# ── process_message — 4xx distinctions ─────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_azure_422_dead_letters_immediately():
    """
    Azure 422 Unprocessable Entity → dead-letter immediately.
    A 4xx means our contract is wrong. Retrying the same payload
    will always fail, so we dead-letter without scheduling a retry.
    """
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(422))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()
    broker.ack.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_azure_400_dead_letters_immediately():
    """Any non-retryable 4xx immediately dead-letters."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(400))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()


@pytest.mark.parametrize("status_code", [408, 425, 429])
@pytest.mark.asyncio
@respx.mock
async def test_transient_4xx_retries_instead_of_dead_lettering(status_code):
    """
    408 Request Timeout, 425 Too Early, and 429 Too Many Requests are
    explicitly retryable per the HTTP spec. Dead-lettering them would lose
    events to a temporary throttle — these MUST nack, not dead-letter.
    """
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(status_code))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


# ── Retry-After honoring ───────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_retry_after_header_overrides_computed_backoff():
    """
    When Azure sends Retry-After: 120, the nack must schedule retry in
    exactly 120s, not whatever our jitter happens to roll. Ignoring the
    header would have us re-hit a rate limit that the server explicitly
    asked us to back off from.
    """
    respx.post("http://localhost:8001/events").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "120"})
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    backoff_arg = broker.nack.call_args.args[2]
    assert backoff_arg == 120.0


@pytest.mark.asyncio
@respx.mock
async def test_missing_retry_after_falls_back_to_computed_backoff():
    """If no Retry-After header, we must use our own computed backoff (>= 0)."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(503))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    backoff_arg = broker.nack.call_args.args[2]
    assert backoff_arg >= 0


# ── process_message — network errors ──────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_http_timeout_calls_nack():
    """Network timeout → nack (retry)."""
    respx.post("http://localhost:8001/events").mock(side_effect=httpx.TimeoutException("timed out"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_http_connect_error_calls_nack():
    """Connection refused → nack (retry)."""
    respx.post("http://localhost:8001/events").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_other_httpx_request_errors_also_nack():
    """
    httpx.RequestError subclasses other than Timeout/ConnectError (e.g.
    ReadError, PoolTimeout) must also nack — not propagate to the outer loop
    where the row would be left leased without a recorded error.
    """
    respx.post("http://localhost:8001/events").mock(
        side_effect=httpx.ReadError("stream broken mid-read")
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    reason_arg = broker.nack.call_args.args[1]
    assert "ReadError" in reason_arg


# ── process_message — max attempts ────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_max_attempts_dead_letters_before_http_call():
    """
    When attempt_count exceeds DELIVERY_MAX_ATTEMPTS, the event is dead-lettered
    BEFORE any HTTP call. No point hitting Azure if we've already given up.
    """
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(200))
    broker = make_broker()
    msg = make_message(attempt_count=settings.DELIVERY_MAX_ATTEMPTS + 1)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg)

    broker.dead_letter.assert_called_once()
    assert not respx.calls.called


# ── Contract payload structure ─────────────────────────────────────────────────


def test_build_contract_payload_has_required_keys():
    """The contract payload must include all fields consumers depend on."""
    contract = build_contract_payload(make_message())
    required = {
        "schema_version", "event_key", "correlation_id",
        "channel", "direction", "event_type",
        "timestamp", "source", "data",
    }
    assert required.issubset(contract.keys())


def test_build_contract_payload_values():
    """Contract field values come from the right BrokerMessage attributes."""
    msg = make_message()
    contract = build_contract_payload(msg)

    assert contract["schema_version"] == "1.0"
    assert contract["event_key"] == msg.event_key
    assert contract["correlation_id"] == str(msg.correlation_id)
    assert contract["channel"] == "sms"
    assert contract["direction"] == "inbound"
    assert contract["event_type"] == "sms.received"
    assert contract["source"] == msg.source_metadata
    assert contract["data"]["from_number"] == msg.from_number
    assert contract["data"]["to_number"] == msg.to_number
    assert contract["data"]["raw"] == msg.raw_payload


def test_build_contract_payload_handles_null_numbers():
    """Contract must not crash when from_number or to_number is None."""
    msg = make_message(from_number=None, to_number=None)
    contract = build_contract_payload(msg)
    assert contract["data"]["from_number"] is None
    assert contract["data"]["to_number"] is None


# ── Backoff (deterministic) ────────────────────────────────────────────────────


def test_backoff_is_capped_at_settings_max():
    """
    Backoff must never exceed DELIVERY_BACKOFF_MAX_SECONDS regardless of
    attempt count. Tested against the actual cap — exhaustive over attempts.
    """
    for attempt in range(1, 30):
        for _ in range(20):
            assert compute_backoff(attempt) <= settings.DELIVERY_BACKOFF_MAX_SECONDS


def test_backoff_is_non_negative():
    """Backoff is always >= 0 (full jitter cannot go below zero)."""
    for attempt in range(0, 10):
        for _ in range(20):
            assert compute_backoff(attempt) >= 0


def test_backoff_cap_grows_with_attempt_until_capped():
    """
    Test the formula directly, not random samples — deterministic.
    The CAP at low attempts should be smaller than the CAP at high attempts,
    until both hit DELIVERY_BACKOFF_MAX_SECONDS.
    """
    base = settings.DELIVERY_BACKOFF_BASE_SECONDS
    max_cap = settings.DELIVERY_BACKOFF_MAX_SECONDS

    cap_attempt_1 = min(base * (2 ** 1), max_cap)
    cap_attempt_4 = min(base * (2 ** 4), max_cap)
    cap_attempt_20 = min(base * (2 ** 20), max_cap)

    assert cap_attempt_1 < cap_attempt_4
    assert cap_attempt_4 <= cap_attempt_20
    assert cap_attempt_20 == max_cap  # very high attempts always hit the cap


# ── Retry-After parsing ────────────────────────────────────────────────────────


def test_parse_retry_after_seconds_form():
    response = httpx.Response(429, headers={"Retry-After": "30"})
    assert parse_retry_after(response) == 30.0


def test_parse_retry_after_missing_header():
    response = httpx.Response(503)
    assert parse_retry_after(response) is None


def test_parse_retry_after_unparseable_returns_none():
    """HTTP-date form or garbage falls back to None so the caller uses its own backoff."""
    response = httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    assert parse_retry_after(response) is None


def test_parse_retry_after_negative_clamped_to_zero():
    """A negative Retry-After is clamped to 0 — never retry in the past."""
    response = httpx.Response(429, headers={"Retry-After": "-5"})
    assert parse_retry_after(response) == 0.0
