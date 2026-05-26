"""
Integration test — Case 1: SMS webhook → comm_events row.

Sends a real signed POST to /webhooks/sms using the ASGI test transport
(no network, but runs the full FastAPI middleware stack including Twilio
signature validation). Asserts that a comm_events row is created with
delivery_status='pending'.

WHY ASGI transport instead of a subprocess: the test exercises the full
request cycle — routing, dependency injection, DB insert, broker publish —
without the overhead of starting a real server. The DB writes are real.
"""

from __future__ import annotations

import pytest
import httpx
from httpx import ASGITransport
from twilio.request_validator import RequestValidator

from comm_layer.config import settings
from comm_layer.main import app

pytestmark = pytest.mark.integration


def _sign_request(url: str, params: dict[str, str]) -> str:
    """Compute the X-Twilio-Signature for a POST request against a real URL."""
    validator = RequestValidator(settings.TWILIO_AUTH_TOKEN)
    return validator.compute_signature(url, params)


@pytest.mark.asyncio
async def test_sms_ingest_creates_pending_row(pool, test_prefix):
    """
    A valid signed SMS webhook should:
    - Return HTTP 200 with TwiML body
    - Insert exactly one comm_events row whose delivery_status is 'pending'
    """
    message_sid = f"{test_prefix}SM-ingest-1"
    event_key = f"{message_sid}:sms.received"

    # Build the same URL the signature validator reconstructs inside the app.
    # settings.PUBLIC_BASE_URL is the ngrok/production base, path is /webhooks/sms.
    url = f"{settings.PUBLIC_BASE_URL.rstrip('/')}/webhooks/sms"

    params = {
        "MessageSid": message_sid,
        "From": "+15550000001",
        "To": settings.TWILIO_PHONE_NUMBER,
        "Body": "Integration test inbound SMS",
        "NumMedia": "0",
    }
    signature = _sign_request(url, params)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url=settings.PUBLIC_BASE_URL
    ) as client:
        response = await client.post(
            "/webhooks/sms",
            data=params,
            headers={"X-Twilio-Signature": signature},
        )

    assert response.status_code == 200, (
        f"Expected 200, got {response.status_code}. Body: {response.text}"
    )
    assert "Response" in response.text or response.text == "", (
        "Expected TwiML response body"
    )

    # Verify the row was actually written to the real DB
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT delivery_status FROM comm_events WHERE event_key = $1",
            event_key,
        )

    assert row is not None, f"No comm_events row found for event_key={event_key}"
    assert row["delivery_status"] == "pending", (
        f"Expected delivery_status='pending', got '{row['delivery_status']}'"
    )


@pytest.mark.asyncio
async def test_sms_ingest_idempotent(pool, test_prefix):
    """
    Sending the same webhook twice should not create a duplicate row.
    The second POST must still return 200 (so Twilio stops retrying),
    but the DB should have exactly one comm_events row.
    """
    message_sid = f"{test_prefix}SM-ingest-idem"
    url = f"{settings.PUBLIC_BASE_URL.rstrip('/')}/webhooks/sms"
    params = {
        "MessageSid": message_sid,
        "From": "+15550000002",
        "To": settings.TWILIO_PHONE_NUMBER,
        "Body": "Duplicate webhook test",
        "NumMedia": "0",
    }
    signature = _sign_request(url, params)
    headers = {"X-Twilio-Signature": signature}

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url=settings.PUBLIC_BASE_URL
    ) as client:
        r1 = await client.post("/webhooks/sms", data=params, headers=headers)
        r2 = await client.post("/webhooks/sms", data=params, headers=headers)

    assert r1.status_code == 200
    assert r2.status_code == 200

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM comm_events WHERE event_key = $1",
            f"{message_sid}:sms.received",
        )

    assert count == 1, f"Expected 1 row (idempotent), found {count}"
