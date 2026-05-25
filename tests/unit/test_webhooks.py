"""
Webhook handler tests — Phases 1, 4, and 5.

What we test here:
1.  Valid inbound SMS → 200, TwiML response, event persisted, broker notified
2.  Duplicate SMS (same MessageSid) → 200, second insert silently ignored
3.  Duplicate SMS keeps the ORIGINAL correlation_id (not a new one)
4.  Invalid Twilio signature → 403
5.  Missing signature header → 403 (NOT 422 — both failures are security incidents)
6.  Unknown to_number (not in registry) → 200, event still captured
7.  Malformed payload (missing MessageSid) → 200 with empty TwiML (graceful)
8.  Missing From/To → stored as NULL in DB, never empty string
9.  Valid inbound voice call → 200, TwiML with Say + Record
10. Valid inbound WhatsApp → 200, TwiML response
11. Voice status with Direction=outbound-api → recorded as outbound
12. SMS status callback persisted as event, broker notified
13. SMS status event_key includes the status value
14. WhatsApp status callback persisted as event
15. WhatsApp status event_key includes the status value
16. Valid recording-ready callback → 200, event persisted
17. Recording with RecordingStatus=absent → 200, event NOT persisted
18. Recording with missing RecordingSid → 200, event NOT persisted
19. Recording event_key is {RecordingSid}:recording.ready
20. Recording-ready with invalid Twilio signature → 403
21. Voice TwiML includes <Record> with recordingStatusCallback URL
22. Recording.ready inherits correlation_id/direction/from/to/source from call.started
23. Recording.ready falls back gracefully when call.started lookup misses
24. Recording with RecordingStatus=failed → captured as recording.failed event
25. Voice TwiML's maxLength attribute reflects the configured cost guardrail
26. Voice TwiML callback URL is fully qualified (includes scheme+host)
27. make_voice_recording_twiml XML-escapes special chars in the URL

We mock the DB pool and broker so no real database is needed.
The Twilio signature validation is NOT mocked — it runs end-to-end
using real HMAC-SHA1 with the test auth token.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tests.fixtures.twilio_fixtures import (
    signed_recording_payload,
    signed_sms_payload,
    signed_sms_status_payload,
    signed_voice_payload,
    signed_whatsapp_payload,
    signed_whatsapp_status_payload,
)

# ── Helpers ────────────────────────────────────────────────────────────────────


def make_fetchrow_handler(
    *,
    insert_returns: dict | None,
    existing_correlation_id: uuid.UUID | None = None,
    call_context: dict | None = None,
):
    """
    Build a clean async side_effect for mock_conn.fetchrow.

    WHY a helper: every test wires up the same query branches — number_registry
    lookup, INSERT INTO comm_events, the duplicate SELECT correlation_id lookup,
    and (Phase 5) the recording handler's call.started context lookup.
    Centralising means one fix updates every test.

    call_context — when set, returned for the recording handler's lookup_call_context
    query. When None, that query returns None (simulates a recording arriving
    without a matching call.started row).
    """

    async def handler(query: str, *args):
        if "number_registry" in query:
            return None
        if "INSERT INTO comm_events" in query:
            return insert_returns
        if "SELECT correlation_id FROM comm_events" in query:
            if existing_correlation_id is None:
                return None
            return {"correlation_id": existing_correlation_id}
        if "raw_payload->>'CallSid'" in query:
            return call_context
        return None

    return handler


# ── Test app fixture ───────────────────────────────────────────────────────────


@pytest.fixture
async def client(mock_asyncpg_pool):
    """
    An HTTPX async test client wired to the FastAPI app with mocked DB dependencies.

    We patch create_pool and create_supabase_client so the lifespan startup
    doesn't try to connect to a real database.
    """
    mock_pool, mock_conn = mock_asyncpg_pool

    mock_broker = AsyncMock()
    mock_broker.publish = AsyncMock(return_value=None)

    # Default: registry returns nothing (unknown number), INSERT succeeds (new event).
    event_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )
    mock_conn.fetchval = AsyncMock(return_value=1)  # for health check

    from comm_layer.deps import get_broker, get_pool
    from comm_layer.main import app

    app.dependency_overrides[get_pool] = lambda: mock_pool
    app.dependency_overrides[get_broker] = lambda: mock_broker

    with (
        patch("comm_layer.main.create_pool", AsyncMock(return_value=mock_pool)),
        patch("comm_layer.main.create_supabase_client", AsyncMock(return_value=AsyncMock())),
        patch("comm_layer.main.PostgresBroker", return_value=mock_broker),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac, mock_conn, mock_broker

    app.dependency_overrides.clear()


# ── SMS tests ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sms_valid_returns_200_twiml(client):
    """Valid inbound SMS returns 200 with TwiML content."""
    ac, mock_conn, mock_broker = client
    params, signature = signed_sms_payload()

    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    assert "application/xml" in response.headers["content-type"]
    assert "<Response>" in response.text


@pytest.mark.asyncio
async def test_sms_event_is_persisted(client):
    """A new SMS causes an INSERT into comm_events."""
    ac, mock_conn, mock_broker = client
    params, signature = signed_sms_payload()

    await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1


@pytest.mark.asyncio
async def test_sms_broker_publish_called(client):
    """After a new event is persisted, broker.publish() is called."""
    ac, mock_conn, mock_broker = client
    params, signature = signed_sms_payload()

    await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    mock_broker.publish.assert_called_once()


@pytest.mark.asyncio
async def test_sms_duplicate_returns_200_without_second_insert(client):
    """
    Duplicate webhook delivery returns 200 silently — broker is NOT called again.

    We simulate a duplicate by making the INSERT return None (ON CONFLICT DO NOTHING).
    """
    ac, mock_conn, _ = client

    # Override: INSERT returns None → duplicate. No existing row needed for this test.
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns=None)
    )

    duplicate_broker = AsyncMock()
    from comm_layer.deps import get_broker
    from comm_layer.main import app
    app.dependency_overrides[get_broker] = lambda: duplicate_broker

    params, signature = signed_sms_payload(message_sid="SM_DUPLICATE")
    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    duplicate_broker.publish.assert_not_called()


@pytest.mark.asyncio
async def test_sms_duplicate_looks_up_original_correlation_id(client):
    """
    On duplicate, ingest_event must SELECT the original correlation_id so logs
    tie back to the real DB row, not a phantom UUID from the duplicate request.
    """
    ac, mock_conn, _ = client

    original_cid = uuid.UUID("11111111-2222-3333-4444-555555555555")
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(
            insert_returns=None,
            existing_correlation_id=original_cid,
        )
    )

    params, signature = signed_sms_payload(message_sid="SM_DUP_TRACE")
    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    # Confirm the duplicate path queried for the original correlation_id
    correlation_lookups = [
        call for call in mock_conn.fetchrow.call_args_list
        if "SELECT correlation_id FROM comm_events" in str(call)
    ]
    assert len(correlation_lookups) == 1


@pytest.mark.asyncio
async def test_sms_invalid_signature_returns_403(client):
    """A request with a wrong signature is rejected with 403."""
    ac, _, _ = client
    params, _ = signed_sms_payload()

    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": "invalid_signature_value"},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_sms_missing_signature_returns_403(client):
    """
    A request with no X-Twilio-Signature header is rejected with 403 — NOT 422.
    Both 'missing' and 'invalid' are security failures and must produce the
    same status code so monitoring can alert on a single signal.
    """
    ac, _, _ = client
    params, _ = signed_sms_payload()

    response = await ac.post("/webhooks/sms", data=params)  # no signature header

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_sms_unknown_number_still_captured(client):
    """
    An SMS to a number not in the registry is still captured.
    We never drop events — source.is_unknown = True.
    """
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_sms_payload(to_number="+15550000099")
    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    mock_broker.publish.assert_called()


@pytest.mark.asyncio
async def test_sms_malformed_payload_no_sid_returns_200(client):
    """
    A payload missing MessageSid (malformed) returns 200 gracefully.
    We never return 5xx to Twilio — it would keep retrying.
    """
    ac, _, _ = client
    from tests.fixtures.twilio_fixtures import TEST_BASE_URL, make_signature

    bad_params = {"From": "+15559876543", "To": "+15551234567", "Body": "test"}
    url = f"{TEST_BASE_URL}/webhooks/sms"
    sig = make_signature(url, bad_params)

    response = await ac.post(
        "/webhooks/sms",
        data=bad_params,
        headers={"x-twilio-signature": sig},
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_sms_missing_from_and_to_passed_as_none_to_insert(client):
    """
    If From/To are missing in the form, the INSERT must receive None — not "".
    Empty string in a nullable column silently breaks `IS NULL` analytics.
    """
    ac, mock_conn, _ = client
    from tests.fixtures.twilio_fixtures import TEST_BASE_URL, make_signature

    # Only MessageSid present — no From, no To
    params = {"MessageSid": "SM_NO_NUMBERS", "Body": "hi"}
    url = f"{TEST_BASE_URL}/webhooks/sms"
    sig = make_signature(url, params)

    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": sig},
    )
    assert response.status_code == 200

    # Find the INSERT call and verify positions 5 (from_number) and 6 (to_number) are None
    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0].args
    # args[0] is the SQL query; positional params follow
    from_number_arg = args[5]
    to_number_arg = args[6]
    assert from_number_arg is None, f"from_number must be None, got {from_number_arg!r}"
    assert to_number_arg is None, f"to_number must be None, got {to_number_arg!r}"


# ── Voice tests ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_voice_valid_returns_200_with_greeting(client):
    """Inbound call returns 200 with a TwiML greeting."""
    ac, _, _ = client
    params, signature = signed_voice_payload()

    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    assert "<Say" in response.text


@pytest.mark.asyncio
async def test_voice_invalid_signature_returns_403(client):
    """Voice webhook also enforces signature validation."""
    ac, _, _ = client
    params, _ = signed_voice_payload()

    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": "bad_sig"},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_voice_status_outbound_direction_recorded(client):
    """
    A voice status callback with Direction=outbound-api must be persisted with
    direction='outbound', NOT 'inbound'. Hardcoding inbound corrupts analytics
    the moment we make our first outbound call (Phase 4).
    """
    ac, mock_conn, _ = client
    from tests.fixtures.twilio_fixtures import TEST_BASE_URL, make_signature

    params = {
        "CallSid": "CA_outbound_demo",
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": "+15551234567",  # our number
        "To": "+15559876543",    # the customer
        "CallStatus": "completed",
        "Direction": "outbound-api",
    }
    url = f"{TEST_BASE_URL}/webhooks/voice/status"
    sig = make_signature(url, params)

    response = await ac.post(
        "/webhooks/voice/status",
        data=params,
        headers={"x-twilio-signature": sig},
    )
    assert response.status_code == 200

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    # args: 0=sql, 1=event_key, 2=channel, 3=direction, 4=event_type, 5=from, 6=to, ...
    direction_arg = insert_calls[0].args[3]
    assert direction_arg == "outbound", f"expected outbound, got {direction_arg!r}"


# ── WhatsApp tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_whatsapp_valid_returns_200(client):
    """Inbound WhatsApp message returns 200 with TwiML."""
    ac, _, _ = client
    params, signature = signed_whatsapp_payload()

    response = await ac.post(
        "/webhooks/whatsapp",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    assert "<Response>" in response.text


# ── Status callback tests ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sms_status_callback_is_persisted(client):
    """
    SMS delivery status callbacks must be ingested as their own events so the
    dashboard can show queued → sent → delivered progression.
    The event_key must embed the status so each transition is a unique row.
    """
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_sms_status_payload(status="delivered")
    response = await ac.post(
        "/webhooks/sms/status",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    # Event should be queued for delivery
    mock_broker.publish.assert_called_once()


@pytest.mark.asyncio
async def test_sms_status_event_key_includes_status(client):
    """
    Two status transitions for the same SID (e.g. 'sent' then 'delivered')
    must produce different event_keys so both rows are inserted. If the status
    is not in the key, the second transition silently duplicates and is lost.
    """
    ac, mock_conn, _ = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_sms_status_payload(
        message_sid="SM_specific", status="sent"
    )
    await ac.post(
        "/webhooks/sms/status",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    event_key = insert_calls[0].args[1]
    assert "SM_specific" in event_key
    assert "sent" in event_key


@pytest.mark.asyncio
async def test_whatsapp_status_callback_is_persisted(client):
    """
    WhatsApp status callbacks must be ingested — including 'read' receipts
    (which SMS doesn't have). These feed the dashboard's message status column.
    """
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_whatsapp_status_payload(status="read")
    response = await ac.post(
        "/webhooks/whatsapp/status",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    mock_broker.publish.assert_called_once()


@pytest.mark.asyncio
async def test_whatsapp_status_event_key_includes_status(client):
    """WhatsApp status event_key must include the status value."""
    ac, mock_conn, _ = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_whatsapp_status_payload(
        message_sid="WA_specific", status="read"
    )
    await ac.post(
        "/webhooks/whatsapp/status",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    event_key = insert_calls[0].args[1]
    assert "WA_specific" in event_key
    assert "read" in event_key


# ── Voice recording tests (Phase 5) ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_voice_twiml_includes_record_verb(client):
    """
    After Phase 5, inbound calls must receive TwiML that includes a <Record> verb.
    Without it, no recording is made — the caller hears the greeting and the call
    ends with nothing captured.
    """
    ac, _, _ = client
    params, signature = signed_voice_payload()

    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    assert "<Record" in response.text


@pytest.mark.asyncio
async def test_voice_twiml_record_has_status_callback_url(client):
    """
    The <Record> verb must include recordingStatusCallback pointing to our
    handler. Without it, Twilio never POSTs the recording-ready notification and
    the recording sits on Twilio's servers unprocessed forever.
    """
    ac, _, _ = client
    params, signature = signed_voice_payload()

    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert "recordingStatusCallback" in response.text
    assert "/webhooks/voice/recording" in response.text


@pytest.mark.asyncio
async def test_recording_ready_webhook_persists_event(client):
    """Valid recording-ready callback → event is persisted and broker is notified."""
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_recording_payload()
    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    mock_broker.publish.assert_called_once()


@pytest.mark.asyncio
async def test_recording_ready_event_key_uses_recording_sid(client):
    """
    The event_key must be {RecordingSid}:recording.ready so each recording
    produces a unique, idempotent key. Using CallSid alone would collide if
    a call has multiple recordings (which Twilio allows on re-record).
    """
    ac, mock_conn, _ = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_recording_payload(recording_sid="RE_specific")
    await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    event_key = insert_calls[0].args[1]
    assert event_key == "RE_specific:recording.ready"


@pytest.mark.asyncio
async def test_recording_absent_not_persisted(client):
    """
    RecordingStatus=absent means the caller hung up before speaking — no audio
    file exists. We must NOT ingest this as an event because the intelligence
    layer would try to fetch a file that doesn't exist.
    """
    ac, mock_conn, mock_broker = client

    params, signature = signed_recording_payload(recording_status="absent")
    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    # No INSERT should be attempted for absent recordings
    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 0
    mock_broker.publish.assert_not_called()


@pytest.mark.asyncio
async def test_recording_missing_sid_not_persisted(client):
    """
    A recording callback with no RecordingSid cannot form a valid event_key.
    We return 200 (so Twilio doesn't retry) but do not ingest anything.
    """
    ac, mock_conn, mock_broker = client

    from tests.fixtures.twilio_fixtures import TEST_BASE_URL, make_signature

    params = {
        "AccountSid": "ACtest00000000000000000000000000000",
        "CallSid": "CA1234567890abcdef",
        # RecordingSid intentionally omitted
        "RecordingStatus": "completed",
    }
    url = f"{TEST_BASE_URL}/webhooks/voice/recording"
    sig = make_signature(url, params)

    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": sig},
    )

    assert response.status_code == 200
    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 0
    mock_broker.publish.assert_not_called()


@pytest.mark.asyncio
async def test_recording_invalid_signature_returns_403(client):
    """Recording-ready callback also enforces Twilio signature validation."""
    ac, _, _ = client
    params, _ = signed_recording_payload()

    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": "forged_sig"},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_recording_ready_inherits_context_from_call_started(client):
    """
    The recording.ready event must inherit correlation_id, direction,
    from_number, to_number, and source from the originating call.started event.
    Without this, log tracing breaks across the call → recording chain.

    We use direction='outbound' in the call_context so the test proves
    inheritance happened — the handler's fallback would set 'inbound'.
    """
    ac, mock_conn, _ = client

    original_cid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    original_source_metadata = {
        "number": "+15551234567",
        "source_type": "campaign",
        "source_id": "demo-campaign-1",
        "label": "Demo Campaign",
        "is_unknown": False,
        "metadata": {},
    }
    call_context = {
        "correlation_id": original_cid,
        "direction": "outbound",
        "from_number": "+15551234567",
        "to_number": "+15559876543",
        "source_metadata": original_source_metadata,
    }

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(
            insert_returns={"id": event_id},
            call_context=call_context,
        )
    )

    params, signature = signed_recording_payload(call_sid="CA_with_origin")
    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0].args
    # args layout: 0=sql, 1=event_key, 2=channel, 3=direction, 4=event_type,
    #              5=from, 6=to, 7=source, 8=raw_payload, 9=correlation_id
    assert args[3] == "outbound", "direction must be inherited from call.started"
    assert args[5] == "+15551234567", "from_number must be inherited"
    assert args[6] == "+15559876543", "to_number must be inherited"
    assert args[9] == original_cid, "correlation_id must be inherited"
    # source is passed via .model_dump() — verify the inherited fields are present
    assert args[7]["source_id"] == "demo-campaign-1"
    assert args[7]["is_unknown"] is False


@pytest.mark.asyncio
async def test_recording_ready_falls_back_when_no_originating_call(client):
    """
    If the originating call.started can't be found (out-of-order webhook, or
    we somehow received a recording for a call we never persisted), we must
    still ingest the event — never drop it — using safe placeholder values.
    """
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(
            insert_returns={"id": event_id},
            call_context=None,  # lookup misses
        )
    )

    params, signature = signed_recording_payload()
    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    # Event must still be ingested and published
    mock_broker.publish.assert_called_once()

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0].args
    # Fallback values
    assert args[3] == "inbound", "fallback direction must be 'inbound'"
    assert args[5] is None, "from_number must be None when call context missing"
    assert args[6] is None, "to_number must be None when call context missing"
    assert args[7]["is_unknown"] is True, "source must be unknown when call context missing"


@pytest.mark.asyncio
async def test_recording_failed_persisted_as_distinct_event(client):
    """
    RecordingStatus=failed means Twilio's encoder broke — the audio is gone,
    but we still capture an event for operational visibility (dashboard,
    investigation, alerting). The event_type and event_key must differ from
    a successful recording so dedup constraints don't collide.
    """
    ac, mock_conn, mock_broker = client

    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(
        side_effect=make_fetchrow_handler(insert_returns={"id": event_id})
    )

    params, signature = signed_recording_payload(
        recording_sid="RE_failed_demo",
        recording_status="failed",
    )
    response = await ac.post(
        "/webhooks/voice/recording",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert response.status_code == 200
    mock_broker.publish.assert_called_once()

    insert_calls = [
        call for call in mock_conn.fetchrow.call_args_list
        if "INSERT INTO comm_events" in str(call)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0].args
    assert args[1] == "RE_failed_demo:recording.failed"
    assert args[4] == "recording.failed"


@pytest.mark.asyncio
async def test_voice_twiml_includes_max_length(client):
    """
    The configured MAX_RECORDING_DURATION_SECONDS must end up in the TwiML's
    maxLength attribute. Otherwise the env-tuned cost guardrail is decorative —
    Twilio would default to 3600s regardless of what we set.
    """
    ac, _, _ = client
    from comm_layer.config import settings

    params, signature = signed_voice_payload()
    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    assert f'maxLength="{settings.MAX_RECORDING_DURATION_SECONDS}"' in response.text


@pytest.mark.asyncio
async def test_voice_twiml_callback_url_is_fully_qualified(client):
    """
    The recordingStatusCallback URL must be a complete public URL. A relative
    path or 'localhost' won't be reachable from Twilio's network — the recording
    file is uploaded but the notification is never delivered.
    """
    ac, _, _ = client
    from tests.fixtures.twilio_fixtures import TEST_BASE_URL

    params, signature = signed_voice_payload()
    response = await ac.post(
        "/webhooks/voice",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    expected_url = f'{TEST_BASE_URL}/webhooks/voice/recording'
    assert expected_url in response.text
    assert "https://" in response.text


def test_voice_twiml_xml_escapes_special_chars_in_url():
    """
    If base_url contains XML-special characters (e.g. '&' in a query string),
    they must be escaped or Twilio rejects the entire response as malformed XML.
    """
    from comm_layer.webhooks.responses import make_voice_recording_twiml

    twiml = make_voice_recording_twiml("https://example.com?a=1&b=2", 3600)
    # The unescaped '&' must NOT appear (other than inside an entity reference)
    assert "a=1&b=2" not in twiml
    # The escaped form must be present
    assert "a=1&amp;b=2" in twiml


# ── Health check tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_live(client):
    """Liveness endpoint always returns 200."""
    ac, _, _ = client
    response = await ac.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
