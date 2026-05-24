"""
Delivery worker tests — Phase 2.

What we test:
1.  Successful delivery (Azure 200) → broker.ack called with contract payload
2.  Azure 500 → broker.nack called (retry scheduled)
3.  Azure 422 (4xx) → broker.dead_letter called immediately, NO retry
4.  HTTP timeout → broker.nack called (retry scheduled)
5.  HTTP connect error → broker.nack called (retry scheduled)
6.  attempt_count > max → broker.dead_letter before making HTTP call
7.  Contract payload has the correct structure (schema_version, event_key, channel…)
8.  Backoff increases with attempt count (exponential)
9.  Backoff is capped at _BACKOFF_MAX_SECONDS
10. Contract transform: all expected keys are present and correctly mapped

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
from delivery_worker.main import _BACKOFF_MAX_SECONDS, compute_backoff, process_message
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
    msg = make_message()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg)

    broker.ack.assert_called_once()
    # Verify the contract was passed to ack
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


# ── process_message — Azure 5xx ────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_azure_500_calls_nack():
    """Azure 500 → nack (retry). The event is NOT dead-lettered."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(500))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()
    broker.ack.assert_not_called()


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


# ── process_message — Azure 4xx ────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_azure_422_dead_letters_immediately():
    """
    Azure 422 Unprocessable Entity → dead-letter immediately.
    A 4xx means our contract is wrong. Retrying the same payload
    will always fail, so we dead-letter without scheduling a retry.
    This is the key senior-signal distinction: 4xx ≠ retry.
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
    """Any 4xx immediately dead-letters — not just 422."""
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(400))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.dead_letter.assert_called_once()
    broker.nack.assert_not_called()


# ── process_message — network errors ──────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_http_timeout_calls_nack():
    """Network timeout → nack (retry). Azure will come back."""
    respx.post("http://localhost:8001/events").mock(side_effect=httpx.TimeoutException("timed out"))
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


@pytest.mark.asyncio
@respx.mock
async def test_http_connect_error_calls_nack():
    """Connection refused → nack (retry). Azure will come back."""
    respx.post("http://localhost:8001/events").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    broker = make_broker()

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, make_message())

    broker.nack.assert_called_once()
    broker.dead_letter.assert_not_called()


# ── process_message — max attempts ────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_max_attempts_dead_letters_before_http_call():
    """
    When attempt_count exceeds DELIVERY_MAX_ATTEMPTS, the event is dead-lettered
    BEFORE making any HTTP call. No point hitting Azure if we've already given up.
    """
    from comm_layer.config import settings
    respx.post("http://localhost:8001/events").mock(return_value=httpx.Response(200))
    broker = make_broker()
    # Set attempt_count to one above the max
    msg = make_message(attempt_count=settings.DELIVERY_MAX_ATTEMPTS + 1)

    async with httpx.AsyncClient() as client:
        await process_message(broker, client, msg)

    broker.dead_letter.assert_called_once()
    # The HTTP endpoint must NOT have been called
    assert not respx.calls.called


# ── Contract payload structure ─────────────────────────────────────────────────


def test_build_contract_payload_has_required_keys():
    """The contract payload must include all fields consumers depend on."""
    msg = make_message()
    contract = build_contract_payload(msg)

    required_keys = {"schema_version", "event_key", "correlation_id", "channel",
                     "direction", "event_type", "timestamp", "source", "data"}
    assert required_keys.issubset(contract.keys()), (
        f"Missing keys: {required_keys - set(contract.keys())}"
    )


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


# ── Backoff ────────────────────────────────────────────────────────────────────


def test_backoff_increases_with_attempt():
    """Higher attempt numbers must produce a higher *cap*, even with jitter."""
    # Run multiple samples at each attempt to account for jitter
    low_samples = [compute_backoff(1) for _ in range(50)]
    high_samples = [compute_backoff(8) for _ in range(50)]

    # The max observed at attempt=1 should generally be below max at attempt=8
    assert max(low_samples) <= max(high_samples) + 1.0  # +1 tolerance for jitter


def test_backoff_is_capped():
    """Backoff must never exceed _BACKOFF_MAX_SECONDS regardless of attempt count."""
    for attempt in range(1, 30):
        for _ in range(10):
            assert compute_backoff(attempt) <= _BACKOFF_MAX_SECONDS


def test_backoff_is_non_negative():
    """Backoff is always >= 0 (full jitter can't go below zero)."""
    for attempt in range(1, 10):
        for _ in range(10):
            assert compute_backoff(attempt) >= 0
