"""
PostgresBroker — Postgres-as-queue implementation.

WHY Postgres instead of Redis/Kafka:
- We already have Postgres (Supabase) — no extra infrastructure.
- SELECT FOR UPDATE SKIP LOCKED gives us atomic claiming without a separate service.
- Events are durable (survives restarts) without any extra config.
- The table IS the audit trail — no separate logging needed.
- Easy to swap for Azure Service Bus later (see AzureServiceBusBroker stub).

The critical SQL pattern:
    SELECT ... FROM comm_events
    WHERE delivery_status IN ('pending', 'failed')
      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED

SKIP LOCKED means if Worker A holds a lock on row X, Worker B skips it and
claims the next available row. This is how multiple workers can run without
stepping on each other.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import asyncpg
import structlog

from comm_layer.broker.base import Broker, BrokerMessage

log = structlog.get_logger(__name__)


class PostgresBroker(Broker):
    """Broker backed by Postgres using SELECT FOR UPDATE SKIP LOCKED."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        # The pool is created by the application on startup and shared across workers.
        # We don't own it — we just use it.
        self._pool = pool

    async def publish(self, event_id: uuid.UUID) -> None:
        """Set delivery_status = 'pending' so the worker picks this event up."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status = 'pending',
                    updated_at = NOW()
                WHERE id = $1
                """,
                event_id,
            )
        log.debug("broker.published", event_id=str(event_id))

    async def claim_next(self) -> BrokerMessage | None:
        """
        Atomically claim the next deliverable event.

        Returns None immediately if no events are ready — the caller should
        sleep before polling again (see delivery worker's poll loop).
        """
        async with self._pool.acquire() as conn:
            # We must be in a transaction for FOR UPDATE to work correctly.
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, event_key, correlation_id, raw_payload, attempt_count
                    FROM comm_events
                    WHERE delivery_status IN ('pending', 'failed')
                      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )

                if row is None:
                    return None

                # Bump attempt_count and mark as in-progress (we use 'failed' until ack'd
                # to ensure the row is retried if the worker crashes mid-processing)
                await conn.execute(
                    """
                    UPDATE comm_events
                    SET attempt_count = attempt_count + 1,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    row["id"],
                )

                msg = BrokerMessage(
                    id=row["id"],
                    event_key=row["event_key"],
                    correlation_id=row["correlation_id"],
                    payload=dict(row["raw_payload"]),
                    attempt_count=row["attempt_count"] + 1,  # reflect the bump we just did
                    claimed_at=datetime.now(UTC),
                )

        log.debug(
            "broker.claimed",
            event_id=str(msg.id),
            event_key=msg.event_key,
            attempt=msg.attempt_count,
        )
        return msg

    async def ack(self, event_id: uuid.UUID) -> None:
        """Mark event as successfully delivered."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status = 'delivered',
                    next_retry_at = NULL,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE id = $1
                """,
                event_id,
            )
        log.info("broker.acked", event_id=str(event_id))

    async def nack(self, event_id: uuid.UUID, error: str, retry_after_seconds: float) -> None:
        """Release event back to queue with a backoff delay."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status = 'failed',
                    last_error = $2,
                    next_retry_at = NOW() + ($3 || ' seconds')::INTERVAL,
                    updated_at = NOW()
                WHERE id = $1
                """,
                event_id,
                error[:500],  # truncate so we never overflow the text column
                str(retry_after_seconds),
            )
        log.warning(
            "broker.nacked",
            event_id=str(event_id),
            error=error[:200],
            retry_after_seconds=retry_after_seconds,
        )

    async def dead_letter(self, event_id: uuid.UUID, reason: str) -> None:
        """Give up permanently — move to the dead-letter state."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status = 'dead',
                    last_error = $2,
                    next_retry_at = NULL,
                    updated_at = NOW()
                WHERE id = $1
                """,
                event_id,
                reason[:500],
            )
        log.error("broker.dead_lettered", event_id=str(event_id), reason=reason[:200])

    async def close(self) -> None:
        """We don't own the pool — the app lifecycle manages it."""
        pass
