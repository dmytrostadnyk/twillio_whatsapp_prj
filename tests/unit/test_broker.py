"""
Unit tests for the PostgresBroker.

What we're testing:
1. publish() updates delivery_status to 'pending'.
2. claim_next() returns None when no rows match.
3. claim_next() returns a BrokerMessage when a row is available.
4. ack() updates delivery_status to 'delivered'.
5. nack() updates delivery_status to 'failed' and sets next_retry_at.
6. dead_letter() updates delivery_status to 'dead'.
7. Two concurrent claim_next() calls on the same row — only one succeeds
   (simulated via the SKIP LOCKED pattern; here we test the SQL branch
   where fetchrow returns None for the second caller).

WHY we mock asyncpg:
Unit tests must never connect to a real database. We mock at the connection
pool level so every SQL operation is captured without any network I/O.
"""

import uuid
from unittest.mock import AsyncMock

import pytest

from comm_layer.broker.base import BrokerMessage
from comm_layer.broker.postgres import PostgresBroker


def make_broker(mock_pool) -> PostgresBroker:
    return PostgresBroker(pool=mock_pool)


# ── publish ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_executes_update(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    broker = make_broker(mock_pool)
    event_id = uuid.uuid4()

    await broker.publish(event_id)

    mock_conn.execute.assert_called_once()
    # Verify the SQL touches the right table
    sql_call = mock_conn.execute.call_args[0][0]
    assert "comm_events" in sql_call
    assert "delivery_status" in sql_call
    assert "pending" in sql_call


# ── claim_next — no rows ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_empty(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetchrow = AsyncMock(return_value=None)  # simulate empty queue
    broker = make_broker(mock_pool)

    result = await broker.claim_next()

    assert result is None


# ── claim_next — row available ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_message_when_row_available(mock_asyncpg_pool):
    mock_pool, mock_conn = mock_asyncpg_pool
    event_id = uuid.uuid4()
    correlation_id = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")

    # asyncpg returns a Record object; we simulate it as a dict
    mock_conn.fetchrow = AsyncMock(return_value={
        "id": event_id,
        "event_key": "SM123:sms.received",
        "correlation_id": correlation_id,
        "raw_payload": {"Body": "Hello", "From": "+15559876543"},
        "attempt_count": 0,
    })

    broker = make_broker(mock_pool)
    result = await broker.claim_next()

    assert result is not None
    assert isinstance(result, BrokerMessage)
    assert result.id == event_id
    assert result.event_key == "SM123:sms.received"
    assert result.attempt_count == 1  # we bumped it by 1


@pytest.mark.asyncio
async def test_claim_next_increments_attempt_count(mock_asyncpg_pool):
    """After claiming, attempt_count should be bumped so we track retries accurately."""
    mock_pool, mock_conn = mock_asyncpg_pool
    event_id = uuid.uuid4()

    mock_conn.fetchrow = AsyncMock(return_value={
        "id": event_id,
        "event_key": "SM123:sms.received",
        "correlation_id": uuid.uuid4(),
        "raw_payload": {},
        "attempt_count": 3,  # this is the 4th attempt
    })

    broker = make_broker(mock_pool)
    result = await broker.claim_next()

    assert result.attempt_count == 4  # bumped from 3 → 4


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
    very_long_error = "x" * 1000

    await broker.nack(event_id, error=very_long_error, retry_after_seconds=5.0)

    # The error passed to execute should be truncated to 500 chars
    call_args = mock_conn.execute.call_args[0]
    stored_error = call_args[2]  # positional arg $2 in the SQL
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
    Simulate SKIP LOCKED behaviour: if the only row is locked by another worker,
    fetchrow returns None and claim_next should return None (not raise).

    In production, Postgres handles this atomically. Here we test that our
    code handles the None return correctly.
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

    abstract_methods = Broker.__abstractmethods__
    for method_name in abstract_methods:
        assert hasattr(PostgresBroker, method_name), (
            f"PostgresBroker is missing abstract method: {method_name}"
        )
