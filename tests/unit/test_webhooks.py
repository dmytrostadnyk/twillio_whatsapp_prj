"""
Webhook handler tests — Phase 1.

What we test here:
1. Valid inbound SMS → 200, TwiML response, event persisted, broker notified
2. Duplicate SMS (same MessageSid) → 200, second insert silently ignored
3. Invalid Twilio signature → 403
4. Missing signature header → 422 (FastAPI rejects it before our code runs)
5. Unknown to_number (not in registry) → 200, event still captured
6. Malformed payload (missing MessageSid) → 200 with empty TwiML (graceful)
7. Valid inbound voice call → 200, TwiML with greeting
8. Valid inbound WhatsApp → 200, TwiML response

We mock the DB pool and broker so no real database is needed.
The Twilio signature validation is NOT mocked — it runs end-to-end
using real HMAC-SHA1 with the test auth token.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tests.fixtures.twilio_fixtures import (
    signed_sms_payload,
    signed_voice_payload,
    signed_whatsapp_payload,
)

# ── Test app fixture ───────────────────────────────────────────────────────────


@pytest.fixture
async def client(mock_asyncpg_pool):
    """
    An HTTPX async test client wired to the FastAPI app with mocked DB dependencies.

    We patch create_pool and create_supabase_client so the lifespan startup
    doesn't try to connect to a real database.
    """
    mock_pool, mock_conn = mock_asyncpg_pool

    # When broker.publish() is called, just return None (async no-op)
    mock_broker = AsyncMock()
    mock_broker.publish = AsyncMock(return_value=None)

    # fetchrow for number_registry lookup → return None (unknown number)
    # fetchrow for comm_events INSERT → return a fake row with an id
    event_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    def fetchrow_side_effect(query, *args):
        """
        Simulate DB responses based on which query is being run.
        - number_registry SELECT → None (unknown number, but event still captured)
        - comm_events INSERT → return a row with an id (new event)
        """
        if "number_registry" in query:
            return AsyncMock(return_value=None)()
        if "INSERT INTO comm_events" in query:
            return AsyncMock(return_value={"id": event_id})()
        return AsyncMock(return_value=None)()

    mock_conn.fetchrow = MagicMock(side_effect=fetchrow_side_effect)
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

    # Verify comm_events INSERT was called
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

    # Override fetchrow: INSERT returns None → duplicate
    mock_conn.fetchrow = MagicMock(side_effect=lambda query, *args: (
        AsyncMock(return_value=None)() if "INSERT INTO comm_events" in query
        else AsyncMock(return_value=None)()
    ))

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
    # Broker should NOT be called because the event was a duplicate
    duplicate_broker.publish.assert_not_called()


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
async def test_sms_missing_signature_returns_422(client):
    """A request with no X-Twilio-Signature header is rejected by FastAPI (422)."""
    ac, _, _ = client
    params, _ = signed_sms_payload()

    response = await ac.post("/webhooks/sms", data=params)  # no signature header

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sms_unknown_number_still_captured(client):
    """
    An SMS to a number not in the registry is still captured.
    We never drop events — source.is_unknown = True.
    """
    ac, mock_conn, mock_broker = client

    # Simulate: number_registry returns nothing → unknown number
    # comm_events INSERT succeeds → new event
    event_id = uuid.uuid4()

    def fetchrow_unknown(query, *args):
        if "number_registry" in query:
            return AsyncMock(return_value=None)()
        if "INSERT INTO comm_events" in query:
            return AsyncMock(return_value={"id": event_id})()
        return AsyncMock(return_value=None)()

    mock_conn.fetchrow = MagicMock(side_effect=fetchrow_unknown)

    params, signature = signed_sms_payload(to_number="+15550000099")
    response = await ac.post(
        "/webhooks/sms",
        data=params,
        headers={"x-twilio-signature": signature},
    )

    # Must return 200 — unknown number is NOT a reason to reject
    assert response.status_code == 200
    # Broker publish was called — event was captured
    mock_broker.publish.assert_called()


@pytest.mark.asyncio
async def test_sms_malformed_payload_no_sid_returns_200(client):
    """
    A payload missing MessageSid (malformed) returns 200 gracefully.
    We never return 5xx to Twilio — it would keep retrying.
    """
    ac, _, _ = client
    # Build a payload without MessageSid, then sign it
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


# ── Health check tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_live(client):
    """Liveness endpoint always returns 200."""
    ac, _, _ = client
    response = await ac.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
