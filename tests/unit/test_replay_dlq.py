"""
Tests for the DLQ replay logic.

What we test:
1. Nothing to replay → returns 0, no UPDATE called.
2. Multiple dead events → returns count, exactly one UPDATE executed
   (atomic batch reset, not per-row).
3. dry_run=True → returns count but NO UPDATE called.
4. UPDATE targets only delivery_status='dead' rows.

We mock the asyncpg pool — no real DB needed.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from delivery_worker.replay import replay_dead_letters


def make_dead_rows(count: int) -> list[dict]:
    """Build fake asyncpg records representing dead-lettered events."""
    return [
        {"id": uuid.uuid4(), "event_key": f"SM{i}:sms.received"}
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_replay_returns_zero_when_no_dead_events(mock_asyncpg_pool):
    """If the dead-letter queue is empty, return 0 and do not touch the DB."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=[])

    result = await replay_dead_letters(mock_pool, dry_run=False)

    assert result == 0
    mock_conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_replay_returns_count_when_dead_events_exist(mock_asyncpg_pool):
    """Returns the number of events replayed."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=make_dead_rows(3))

    result = await replay_dead_letters(mock_pool, dry_run=False)

    assert result == 3


@pytest.mark.asyncio
async def test_replay_uses_single_batch_update(mock_asyncpg_pool):
    """
    The reset should happen via a single UPDATE for all dead rows — not
    one UPDATE per row. Per-row updates are slow at scale and create partial
    failure windows if the script is interrupted.
    """
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=make_dead_rows(5))

    await replay_dead_letters(mock_pool, dry_run=False)

    # Exactly one UPDATE for the whole batch
    assert mock_conn.execute.call_count == 1


@pytest.mark.asyncio
async def test_replay_update_targets_only_dead_rows(mock_asyncpg_pool):
    """The UPDATE must filter on delivery_status = 'dead' so it can't accidentally
    reset rows in other states."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=make_dead_rows(1))

    await replay_dead_letters(mock_pool, dry_run=False)

    sql = mock_conn.execute.call_args[0][0]
    assert "delivery_status = 'dead'" in sql
    assert "delivery_status = 'pending'" in sql  # what we set it to
    assert "attempt_count   = 0" in sql or "attempt_count = 0" in sql


@pytest.mark.asyncio
async def test_replay_resets_attempt_count_and_clears_error(mock_asyncpg_pool):
    """Replayed events must start fresh — attempt count back to 0, last_error cleared,
    next_retry_at cleared so the worker picks them up immediately."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=make_dead_rows(1))

    await replay_dead_letters(mock_pool, dry_run=False)

    sql = mock_conn.execute.call_args[0][0]
    assert "attempt_count" in sql
    assert "last_error" in sql and "NULL" in sql
    assert "next_retry_at" in sql


# ── Dry-run ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_returns_count_but_makes_no_changes(mock_asyncpg_pool):
    """dry_run=True must NOT call execute — only count and log."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=make_dead_rows(7))

    result = await replay_dead_letters(mock_pool, dry_run=True)

    assert result == 7
    mock_conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_on_empty_queue_returns_zero(mock_asyncpg_pool):
    """dry_run on an empty queue returns 0 and makes no changes."""
    mock_pool, mock_conn = mock_asyncpg_pool
    mock_conn.fetch = AsyncMock(return_value=[])

    result = await replay_dead_letters(mock_pool, dry_run=True)

    assert result == 0
    mock_conn.execute.assert_not_called()
