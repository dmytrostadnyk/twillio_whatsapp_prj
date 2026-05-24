"""
Shared event ingestion function used by all webhook handlers.

WHY one shared function instead of repeating the logic in every handler:
Every channel (SMS, Voice, WhatsApp) needs to do the same three things —
persist the event, handle duplicates, and publish to the broker. Centralising
this means a bug fix or improvement here applies to all channels at once.

The idempotency guarantee lives here: ON CONFLICT (event_key) DO NOTHING.
If Twilio retries the same webhook (which it does on non-2xx responses or
slow responses), the second insert silently does nothing. We return 200 so
Twilio stops retrying. No duplicate rows. No duplicate deliveries.
"""

from __future__ import annotations

import json
import uuid

import asyncpg
import structlog

from comm_layer.broker.base import Broker
from comm_layer.contracts.base import EventSource

log = structlog.get_logger(__name__)


async def ingest_event(
    pool: asyncpg.Pool,
    broker: Broker,
    *,
    event_key: str,
    channel: str,
    direction: str,
    event_type: str,
    from_number: str | None,
    to_number: str | None,
    source: EventSource,
    raw_payload: dict,
    correlation_id: uuid.UUID,
) -> bool:
    """
    Persist an event to comm_events and publish it to the broker queue.

    Returns:
        True  — event was new and has been queued for delivery.
        False — event was a duplicate (already processed); caller should
                still return 200 to Twilio so it stops retrying.

    WHY ON CONFLICT DO NOTHING instead of checking first:
    Checking then inserting has a race condition: two simultaneous Twilio
    retries could both pass the check and both insert. Using the unique
    constraint as the lock eliminates that race entirely.
    """
    # Bind correlation_id to structlog context so it appears in every log line
    # for this request without having to pass it manually everywhere.
    structlog.contextvars.bind_contextvars(
        correlation_id=str(correlation_id),
        event_key=event_key,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO comm_events (
                event_key,
                channel,
                direction,
                event_type,
                from_number,
                to_number,
                source_metadata,
                raw_payload,
                correlation_id,
                delivery_status
            )
            VALUES (
                $1,
                $2::comm_channel,
                $3::comm_direction,
                $4,
                $5,
                $6,
                $7::jsonb,
                $8::jsonb,
                $9,
                'received'
            )
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
            """,
            event_key,
            channel,
            direction,
            event_type,
            from_number,
            to_number,
            json.dumps(source.model_dump()),
            json.dumps(raw_payload),
            correlation_id,
        )

    if row is None:
        # Duplicate delivery from Twilio — already processed, safe to ignore
        log.info("ingest.duplicate_ignored", event_key=event_key)
        return False

    # New event — publish to broker so the delivery worker picks it up
    await broker.publish(row["id"])
    log.info("ingest.event_queued", event_id=str(row["id"]), channel=channel)
    return True
