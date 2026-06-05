"""
PostgresBroker — Postgres-as-queue implementation.

WHY Postgres instead of Redis/Kafka:
- We already have Postgres (Supabase) — no extra infrastructure.
- SELECT FOR UPDATE SKIP LOCKED gives us atomic claiming without a separate service.
- Events are durable (survives restarts) without any extra config.
- The table IS the audit trail — no separate logging needed.
- Easy to swap for Azure Service Bus later (see azure_servicebus.py stub).

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
from typing import Any

import asyncpg
import structlog

from comm_layer.broker.base import Broker, BrokerMessage
from comm_layer.config import settings

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

        WHY we set next_retry_at as a lease:
        After the transaction commits, the FOR UPDATE lock is released. Without
        a lease, the same row would immediately be visible to the next poll. By
        setting next_retry_at = NOW() + DELIVERY_LEASE_SECONDS, the row is
        hidden from other workers while we process it. If the worker crashes
        before ack/nack, the lease expires and another worker re-claims the row.

        WHY INNER JOIN enrichments with status IN ('completed','failed','skipped'):
        Delivery is gated on enrichment completion. This ensures the HubSpot
        contact update always carries the AI summary/intent/sentiment. Events
        stay invisible to the delivery worker until the intelligence layer finishes
        (or gives up). FOR UPDATE OF ce locks only comm_events — not enrichments —
        so the enrichment worker is never blocked by the delivery worker.

        WHY the extra LEFT JOIN whatsapp_replies condition:
        For WhatsApp events the delivery worker needs to know whether the bot
        could answer (wr.resolved) before it can decide whether to inject a
        follow-up action item in HubSpot. We gate on the reply reaching a
        terminal status so the resolved flag is always set before delivery.
        SMS and voice events are unaffected (LEFT JOIN returns NULL → first OR
        branch is true → they pass the gate immediately).
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT ce.id, ce.event_key, ce.correlation_id,
                           ce.channel, ce.direction, ce.event_type,
                           ce.from_number, ce.to_number,
                           ce.source_metadata, ce.raw_payload,
                           ce.attempt_count, ce.created_at,
                           ce.hubspot_contact_id,
                           ce.hubspot_note_id,
                           ce.hubspot_ticket_id,
                           ce.hubspot_task_id,
                           e.summary, e.intent, e.sentiment,
                           e.entities, e.action_items,
                           wr.resolved AS reply_resolved
                    FROM comm_events ce
                    INNER JOIN enrichments e ON e.comm_event_id = ce.id
                    LEFT JOIN whatsapp_replies wr ON wr.comm_event_id = ce.id
                    WHERE ce.delivery_status IN ('pending', 'failed')
                      AND (ce.next_retry_at IS NULL OR ce.next_retry_at <= NOW())
                      AND e.status IN ('completed', 'failed', 'skipped')
                      AND (
                          ce.event_type <> 'whatsapp.received'
                          OR wr.status IN ('sent', 'skipped', 'failed')
                      )
                    ORDER BY ce.created_at
                    LIMIT 1
                    FOR UPDATE OF ce SKIP LOCKED
                    """
                )

                if row is None:
                    return None

                # Bump attempt_count and set the processing lease in one UPDATE.
                await conn.execute(
                    """
                    UPDATE comm_events
                    SET attempt_count   = attempt_count + 1,
                        next_retry_at   = NOW() + ($2 || ' seconds')::INTERVAL,
                        updated_at      = NOW()
                    WHERE id = $1
                    """,
                    row["id"],
                    str(settings.DELIVERY_LEASE_SECONDS),
                )

                msg = BrokerMessage(
                    id=row["id"],
                    event_key=row["event_key"],
                    correlation_id=row["correlation_id"],
                    channel=row["channel"],
                    direction=row["direction"],
                    event_type=row["event_type"],
                    from_number=row["from_number"],
                    to_number=row["to_number"],
                    source_metadata=dict(row["source_metadata"]) if row["source_metadata"] else {},
                    raw_payload=dict(row["raw_payload"]) if row["raw_payload"] else {},
                    attempt_count=row["attempt_count"] + 1,
                    created_at=row["created_at"],
                    claimed_at=datetime.now(UTC),
                    summary=row["summary"],
                    intent=row["intent"],
                    sentiment=row["sentiment"],
                    entities=row["entities"] if row["entities"] is not None else [],
                    action_items=row["action_items"] if row["action_items"] is not None else [],
                    hubspot_contact_id=row["hubspot_contact_id"],
                    hubspot_note_id=row["hubspot_note_id"],
                    hubspot_ticket_id=row["hubspot_ticket_id"],
                    hubspot_task_id=row["hubspot_task_id"],
                    reply_resolved=row["reply_resolved"],
                )

        log.debug(
            "broker.claimed",
            event_id=str(msg.id),
            event_key=msg.event_key,
            attempt=msg.attempt_count,
        )
        return msg

    async def ack(
        self, event_id: uuid.UUID, contract_payload: dict[str, Any] | None = None
    ) -> None:
        """
        Mark event as successfully delivered.

        contract_payload, if provided, is written to comm_events.contract_payload
        so the DB has an immutable record of exactly what was sent to the consumer.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status  = 'delivered',
                    contract_payload = COALESCE($2::jsonb, contract_payload),
                    next_retry_at    = NULL,
                    last_error       = NULL,
                    updated_at       = NOW()
                WHERE id = $1
                """,
                event_id,
                contract_payload,
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
