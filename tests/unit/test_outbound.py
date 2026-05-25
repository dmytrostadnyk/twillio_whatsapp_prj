"""
Tests for outbound helpers and the token bucket rate limiter.

What we test:
Token bucket:
1.  Full bucket allows consumption without error.
2.  Empty bucket raises RateLimitExceededError.
3.  Bucket with capacity=0 always raises (acts as an "off" switch).

check_whatsapp_window:
4.  Returns True when DB row found (recent inbound exists).
5.  Returns False when DB returns None (no recent inbound).
6.  Normalises plain number (no prefix) before querying — adds whatsapp:.
7.  Passes already-prefixed number through unchanged.

send_sms:
8.  Calls Twilio with the correct to/from/body.
9.  Returns the MessageSid from Twilio.
10. Raises RateLimitExceededError when the bucket is empty before making the call.

send_whatsapp:
11. Uses provided body when the session window is open.
12. Falls back to template_body when window is expired.
13. Raises WindowExpiredError when window expired and no template_body given.
14. Adds whatsapp: prefix to a plain phone number.
15. Passes a number that already has the prefix through unchanged.

initiate_call:
16. Calls Twilio with the correct to/from/url.
17. Returns the CallSid from Twilio.
18. Raises RateLimitExceededError when bucket is empty.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from comm_layer.outbound import (
    WindowExpiredError,
    check_whatsapp_window,
    initiate_call,
    send_sms,
    send_whatsapp,
)
from comm_layer.rate_limiter import RateLimitExceededError, TokenBucket

# ── Helpers ────────────────────────────────────────────────────────────────────


def full_bucket(capacity: int = 10) -> TokenBucket:
    """A token bucket that starts full and refills quickly — never rate-limits in tests."""
    return TokenBucket(capacity=capacity, refill_rate=float(capacity))


def empty_bucket() -> TokenBucket:
    """A bucket with capacity=0 — always raises RateLimitExceededError."""
    return TokenBucket(capacity=0, refill_rate=0.0)


def make_twilio_client(message_sid: str = "SM_test", call_sid: str = "CA_test") -> MagicMock:
    """A mock Twilio Client with stubbed messages.create and calls.create."""
    client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.sid = message_sid
    client.messages.create.return_value = mock_msg

    mock_call = MagicMock()
    mock_call.sid = call_sid
    client.calls.create.return_value = mock_call
    return client


# ── Token bucket ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bucket_allows_consume_when_full():
    """A full bucket should let you consume without raising."""
    bucket = full_bucket(capacity=3)
    # Consume all three tokens — none should raise
    await bucket.consume()
    await bucket.consume()
    await bucket.consume()
    assert bucket.available < 1.0


@pytest.mark.asyncio
async def test_bucket_raises_when_empty():
    """The fourth consume on a capacity=3 bucket must raise RateLimitExceededError."""
    bucket = full_bucket(capacity=3)
    await bucket.consume()
    await bucket.consume()
    await bucket.consume()
    with pytest.raises(RateLimitExceededError):
        await bucket.consume()


@pytest.mark.asyncio
async def test_bucket_capacity_zero_always_raises():
    """
    capacity=0 acts as a kill switch — no calls are ever allowed.
    Useful for completely disabling outbound in test environments.
    """
    bucket = empty_bucket()
    with pytest.raises(RateLimitExceededError):
        await bucket.consume()


# ── check_whatsapp_window ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_window_returns_true_when_recent_inbound(mock_asyncpg_pool):
    """DB returns a row → the 24-hour window is open → True."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value={"1": 1})

    result = await check_whatsapp_window(mock_pool, "+15551234567")

    assert result is True


@pytest.mark.asyncio
async def test_window_returns_false_when_no_inbound(mock_asyncpg_pool):
    """DB returns None (no recent inbound) → window is expired → False."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)

    result = await check_whatsapp_window(mock_pool, "+15551234567")

    assert result is False


@pytest.mark.asyncio
async def test_window_normalises_plain_number_to_whatsapp_prefix(mock_asyncpg_pool):
    """
    Plain number (+15551234567) must be queried as whatsapp:+15551234567
    because comm_events stores from_number with the prefix from Twilio.
    """
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)

    await check_whatsapp_window(mock_pool, "+15551234567")

    sql, param = mock_conn.fetchrow.call_args[0]
    assert param == "whatsapp:+15551234567"


@pytest.mark.asyncio
async def test_window_passes_prefixed_number_unchanged(mock_asyncpg_pool):
    """If the caller already includes 'whatsapp:', don't double-prefix it."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)

    await check_whatsapp_window(mock_pool, "whatsapp:+15551234567")

    _, param = mock_conn.fetchrow.call_args[0]
    assert param == "whatsapp:+15551234567"


# ── send_sms ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_sms_calls_twilio_with_correct_params():
    """The SDK must receive the correct to, from_, and body arguments."""
    client = make_twilio_client()

    await send_sms(client, to="+15559876543", body="Hello!", rate_limiter=full_bucket())

    client.messages.create.assert_called_once()
    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["to"] == "+15559876543"
    assert kwargs["body"] == "Hello!"
    # from_ must be our Twilio number, not hardcoded
    assert kwargs["from_"] is not None


@pytest.mark.asyncio
async def test_send_sms_returns_message_sid():
    """The returned value must be the SID Twilio assigns to the message."""
    client = make_twilio_client(message_sid="SM_expected")

    sid = await send_sms(client, to="+15559876543", body="Hi", rate_limiter=full_bucket())

    assert sid == "SM_expected"


@pytest.mark.asyncio
async def test_send_sms_raises_when_rate_limited():
    """Rate limit hit → RateLimitExceededError before any Twilio call is made."""
    client = make_twilio_client()

    with pytest.raises(RateLimitExceededError):
        await send_sms(client, to="+15559876543", body="Hi", rate_limiter=empty_bucket())

    client.messages.create.assert_not_called()


# ── send_whatsapp ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_whatsapp_uses_body_when_window_active():
    """When the 24-hour window is open, the provided body is sent as-is."""
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=True),
    ):
        await send_whatsapp(
            client, pool=None, to="+15551234567", body="Hello!", rate_limiter=full_bucket()
        )

    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["body"] == "Hello!"


@pytest.mark.asyncio
async def test_send_whatsapp_uses_template_when_window_expired():
    """When the window has expired and template_body is given, the template is used."""
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=False),
    ):
        await send_whatsapp(
            client,
            pool=None,
            to="+15551234567",
            body="This should NOT be sent",
            template_body="Pre-approved template message",
            rate_limiter=full_bucket(),
        )

    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["body"] == "Pre-approved template message"


@pytest.mark.asyncio
async def test_send_whatsapp_raises_when_window_expired_and_no_template():
    """
    Window expired + no template_body → WindowExpiredError is raised before any
    Twilio call. Forces the caller to make an explicit decision rather than
    silently sending the free-form message (which Twilio would reject anyway).
    """
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=False),
    ):
        with pytest.raises(WindowExpiredError):
            await send_whatsapp(
                client, pool=None, to="+15551234567", body="Hello!", rate_limiter=full_bucket()
            )

    client.messages.create.assert_not_called()


@pytest.mark.asyncio
async def test_send_whatsapp_adds_prefix_to_plain_number():
    """Plain number is sent to Twilio as whatsapp:+number."""
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=True),
    ):
        await send_whatsapp(
            client, pool=None, to="+15551234567", body="Hi", rate_limiter=full_bucket()
        )

    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["to"] == "whatsapp:+15551234567"


@pytest.mark.asyncio
async def test_send_whatsapp_preserves_existing_prefix():
    """A to number that already has whatsapp: is not double-prefixed."""
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=True),
    ):
        await send_whatsapp(
            client,
            pool=None,
            to="whatsapp:+15551234567",
            body="Hi",
            rate_limiter=full_bucket(),
        )

    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["to"] == "whatsapp:+15551234567"


@pytest.mark.asyncio
async def test_send_whatsapp_raises_when_rate_limited():
    """Rate limit hit → RateLimitExceededError, no Twilio call."""
    client = make_twilio_client()

    with patch(
        "comm_layer.outbound.check_whatsapp_window",
        new=AsyncMock(return_value=True),
    ):
        with pytest.raises(RateLimitExceededError):
            await send_whatsapp(
                client, pool=None, to="+15551234567", body="Hi", rate_limiter=empty_bucket()
            )

    client.messages.create.assert_not_called()


# ── initiate_call ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_initiate_call_calls_twilio_with_correct_params():
    """The SDK must receive the correct to, from_, and url arguments."""
    client = make_twilio_client()

    await initiate_call(
        client,
        to="+15559876543",
        twiml_url="https://example.com/twiml",
        rate_limiter=full_bucket(),
    )

    client.calls.create.assert_called_once()
    kwargs = client.calls.create.call_args.kwargs
    assert kwargs["to"] == "+15559876543"
    assert kwargs["url"] == "https://example.com/twiml"
    assert kwargs["from_"] is not None


@pytest.mark.asyncio
async def test_initiate_call_returns_call_sid():
    """The returned value must be the CallSid Twilio assigns."""
    client = make_twilio_client(call_sid="CA_expected")

    sid = await initiate_call(
        client,
        to="+15559876543",
        twiml_url="https://example.com/twiml",
        rate_limiter=full_bucket(),
    )

    assert sid == "CA_expected"


@pytest.mark.asyncio
async def test_initiate_call_raises_when_rate_limited():
    """Rate limit hit → RateLimitExceededError before any Twilio call is made."""
    client = make_twilio_client()

    with pytest.raises(RateLimitExceededError):
        await initiate_call(
            client,
            to="+15559876543",
            twiml_url="https://example.com/twiml",
            rate_limiter=empty_bucket(),
        )

    client.calls.create.assert_not_called()
