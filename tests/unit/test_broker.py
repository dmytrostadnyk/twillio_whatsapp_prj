"""
Unit tests for the PostgresBroker.

What we're testing:
1.  publish() updates delivery_status to 'pending'.
2.  claim_next() returns None when no rows match.
3.  claim_next() returns a BrokerMessage with all fields populated.
4.  claim_next() increments attempt_count.
5.  claim_next() sets a lease (next_retry_at) so the row is hidden
    from other workers while being processed.
6.  ack() sets delivery_status to 'delivered'.
7.  ack() stores contract_payload when provided.
8.  nack() sets delivery_status to 'failed' and sets next_retry_at.
9.  nack() truncates long error messages.
10. dead_letter() sets delivery_status to 'dead'.
11. Contention: claim_next() returns None when the only row is locked
    (SKIP LOCKED simulation).
12. PostgresBroker implements every abstract method in the Broker ABC.

WHY we mock asyncpg:
Unit tests must never connect to a real database. We mock at the connection
pool level so every SQL operation is captured without any network I/O.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from comm_layer.broker.base import BrokerMessage
from comm_layer.broker.postgres import PostgresBroker


def make_broker(mock_pool) -> PostgresBroker:
    return PostgresBroker(pool=mock_pool)


def make_db_row(
    *,
    event_id: uuid.UUID | None = None,
    attempt_count: int = 0,
) -> dict:
    """Build a fake asyncpg record dict with all fields claim_next now selects."""
    return {
        "id": event_id or uuid.uuid4(),
        "event_key": "SM123:sms.received",
        "correlation_id": uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
        "channel": "sms",
        "direction": "inbound",
        "event_type": "sms.received",
        "from_number": "+15559876543",
        "to_number": "+15551234567",
        "source_metadata": {"number": "+15551234567", "is_unknown": True},
        "raw_payload": {"Body": "Hello", "From": "+15559876543"},
        "attempt_count": attempt_count,
        "created_at": datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
        # Enrichment fields (joined from enrichments table in the new claim_next)
        "summary": "Test summary.",
        "intent": "general_query",
        "sentiment": "neutral",
        "entities": [],
        "action_items": [],
        "hubspot_contact_id": None,
        "hubspot_note_id": None,
        "hubspot_ticket_id": None,
        "hubspot_task_id": None,
        "reply_resolved": None,
    }


# ── publish ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_executes_update(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.publish(event_id)

    mock_conn.execute.assert_called_once()
    sql = mock_conn.execute.call_args[0][0]
    assert "comm_events" in sql
    assert "delivery_status" in sql
    assert "pending" in sql


# ── claim_next — no rows ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_empty(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)
    broker = make_broker(mock_pool)

    result = await broker.claim_next()

    assert result is None


# ── claim_next — row available ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_message_when_row_available(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    event_id = uuid.uuid4()
    mock_conn.fetchrow = AsyncMock(return_value=make_db_row(event_id=event_id))
    broker = make_broker(mock_pool)

    result = await broker.claim_next()

    assert result is not None
    assert isinstance(result, BrokerMessage)
    assert result.id == event_id
    assert result.event_key == "SM123:sms.received"
    assert result.channel == "sms"
    assert result.direction == "inbound"
    assert result.event_type == "sms.received"
    assert result.from_number == "+15559876543"
    assert result.to_number == "+15551234567"
    assert result.raw_payload == {"Body": "Hello", "From": "+15559876543"}
    assert isinstance(result.claimed_at, datetime)


@pytest.mark.asyncio
async def test_claim_next_increments_attempt_count(mock_asyncpg_pool):
    """After claiming, attempt_count is bumped so retries are tracked accurately."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=make_db_row(attempt_count=3))
    broker = make_broker(mock_pool)

    result = await broker.claim_next()

    assert result.attempt_count == 4


@pytest.mark.asyncio
async def test_claim_next_sets_lease_on_claimed_row(mock_asyncpg_pool):
    """
    claim_next must set next_retry_at to a future time (the lease).
    Without a lease, the same row would be claimed again on the next poll —
    any number of workers could process the same event concurrently.
    """
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=make_db_row())
    broker = make_broker(mock_pool)

    await broker.claim_next()

    # The UPDATE call must reference next_retry_at (the lease)
    update_sql = mock_conn.execute.call_args[0][0]
    assert "next_retry_at" in update_sql


# ── ack ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ack_sets_delivered_status(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.ack(event_id)

    mock_conn.execute.assert_called_once()
    sql = mock_conn.execute.call_args[0][0]
    assert "delivered" in sql


@pytest.mark.asyncio
async def test_ack_stores_contract_payload_when_provided(mock_asyncpg_pool):
    """The contract payload sent to Azure must be stored in comm_events for auditability."""
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()
    contract = {"schema_version": "1.0", "event_key": "SM123:sms.received"}

    await broker.ack(event_id, contract_payload=contract)

    sql = mock_conn.execute.call_args[0][0]
    args = mock_conn.execute.call_args[0]
    assert "contract_payload" in sql
    # The contract dict should be the second positional arg after event_id
    assert args[2] == contract


# ── nack ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_nack_sets_failed_status_and_retry_time(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.nack(event_id, error="Connection timeout", retry_after_seconds=30.0)

    mock_conn.execute.assert_called_once()
    sql = mock_conn.execute.call_args[0][0]
    assert "failed" in sql
    assert "next_retry_at" in sql


@pytest.mark.asyncio
async def test_nack_truncates_long_error_messages(mock_asyncpg_pool):
    """Very long error messages must be truncated to avoid overflowing the DB column."""
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.nack(event_id, error="x" * 1000, retry_after_seconds=5.0)

    stored_error = mock_conn.execute.call_args[0][2]  # positional $2
    assert len(stored_error) <= 500


# ── dead_letter ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dead_letter_sets_dead_status(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.dead_letter(event_id, reason="Max attempts exceeded")

    mock_conn.execute.assert_called_once()
    sql = mock_conn.execute.call_args[0][0]
    assert "dead" in sql


# ── contention simulation ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_row_locked(mock_asyncpg_pool):
    """
    Simulate SKIP LOCKED: if the only row is locked by another worker,
    fetchrow returns None and claim_next should return None (not raise).
    """
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)
    broker = make_broker(mock_pool)

    result = await broker.claim_next()
    assert result is None


# ── Broker interface compliance ────────────────────────────────────────────────


def test_postgres_broker_implements_all_abstract_methods():
    """Verify PostgresBroker implements every method defined in the Broker ABC."""
    from comm_layer.broker.base import Broker

    for method_name in Broker.__abstractmethods__:
        assert hasattr(PostgresBroker, method_name), (
            f"PostgresBroker is missing abstract method: {method_name}"
        )
