"""
Dead-letter queue replay script.

Re-queues all dead-lettered events so the delivery worker will attempt
delivery again from the beginning (attempt_count reset to 0).

Usage:
    python scripts/replay_dlq.py

WHY you need this:
Events land in the dead-letter state ('dead') when they've failed
DELIVERY_MAX_ATTEMPTS times. The most common causes are:

  1. Azure CRM was down for a long time and retries were exhausted.
     Fix: bring Azure back up, then replay.

  2. A bug in the contract payload caused Azure to return 4xx.
     Fix: fix the bug, deploy the worker, then replay.

  3. A bad environment variable (wrong AZURE_CRM_URL).
     Fix: correct the variable, then replay.

This script sets delivery_status = 'pending' and resets attempt_count = 0
so the worker treats each event as fresh. The event_key uniqueness constraint
still protects against double-writes downstream — the Azure CRM will
idempotently handle events it already received.

Events are replayed in created_at order (oldest first) to preserve original
arrival order as closely as possible.
"""

from __future__ import annotations

import asyncio

import structlog

from comm_layer.broker.postgres import PostgresBroker
from comm_layer.config import settings
from comm_layer.db import create_pool
from comm_layer.logging_config import configure_logging

log = structlog.get_logger(__name__)


async def replay_dlq() -> None:
    """Reset all dead-lettered events to 'pending' and requeue them."""
    configure_logging(settings.LOG_LEVEL)
    pool = await create_pool()

    try:
        async with pool.acquire() as conn:
            # Fetch dead-lettered events in arrival order before we modify them.
            rows = await conn.fetch(
                """
                SELECT id, event_key
                FROM comm_events
                WHERE delivery_status = 'dead'
                ORDER BY created_at
                """
            )

            if not rows:
                log.info("replay_dlq.nothing_to_replay")
                return

            log.info("replay_dlq.starting", count=len(rows))

            # Reset each event atomically. We reset attempt_count so the
            # worker treats each event as a fresh delivery attempt and doesn't
            # immediately re-dead-letter it due to the old attempt count.
            for row in rows:
                await conn.execute(
                    """
                    UPDATE comm_events
                    SET delivery_status = 'pending',
                        attempt_count   = 0,
                        last_error      = NULL,
                        next_retry_at   = NULL,
                        updated_at      = NOW()
                    WHERE id = $1
                    """,
                    row["id"],
                )
                log.info("replay_dlq.requeued", event_key=row["event_key"])

        # Notify the broker so a LISTEN-based worker wakes up immediately
        # (no-op for poll-based workers, harmless in all cases).
        broker = PostgresBroker(pool=pool)
        for row in rows:
            await broker.publish(row["id"])

        log.info("replay_dlq.done", replayed=len(rows))

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(replay_dlq())
