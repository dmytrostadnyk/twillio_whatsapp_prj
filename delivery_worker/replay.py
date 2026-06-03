"""
Dead-letter queue replay logic.

Used by scripts/replay_dlq.py. Lives here (instead of inside the script)
so it's importable and unit-testable.

Re-queues all dead-lettered events so the delivery worker will attempt
delivery again from the beginning (attempt_count reset to 0).

WHY you need this:
Events land in the dead-letter state ('dead') when they've failed
DELIVERY_MAX_ATTEMPTS times. The most common causes are:

  1. HubSpot was down for an extended time and retries were exhausted.
     Fix: confirm HubSpot is healthy, then replay.

  2. A bad HUBSPOT_PRIVATE_APP_TOKEN (expired or wrong scopes → 401/403).
     Fix: regenerate the token in HubSpot → Settings → Integrations, update
     the env var, restart the delivery worker, then replay.

  3. A bug in the property values caused HubSpot to return 4xx.
     Fix: fix the bug, deploy the worker, then replay.

This module sets delivery_status = 'pending' and resets attempt_count = 0
so the worker treats each event as fresh. The HubSpot contact update is
idempotent — replaying the same event just overwrites the same properties.

Events are replayed in created_at order (oldest first) to preserve original
arrival order as closely as possible.
"""

from __future__ import annotations

import asyncpg
import structlog

log = structlog.get_logger(__name__)


async def replay_dead_letters(pool: asyncpg.Pool, dry_run: bool = False) -> int:
    """
    Replay all dead-lettered events. Returns the number of events affected.

    If dry_run=True, just counts and logs what WOULD be replayed without
    making any changes. Use this as a safety check before running for real.

    A single transaction wraps the whole reset so partial failures don't
    leave the queue in a half-replayed state.
    """
    async with pool.acquire() as conn:
        # Fetch first so we know the count and can log per-event detail
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
            return 0

        if dry_run:
            log.info("replay_dlq.dry_run", would_replay=len(rows))
            for row in rows:
                log.info("replay_dlq.dry_run.event", event_key=row["event_key"])
            return len(rows)

        log.info("replay_dlq.starting", count=len(rows))

        # Single atomic UPDATE for all dead rows. The worker's claim_next will
        # pick them up on its next poll — no separate broker.publish needed
        # because we already set delivery_status = 'pending' here.
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE comm_events
                SET delivery_status = 'pending',
                    attempt_count   = 0,
                    last_error      = NULL,
                    next_retry_at   = NULL,
                    updated_at      = NOW()
                WHERE delivery_status = 'dead'
                """
            )

        for row in rows:
            log.info("replay_dlq.requeued", event_key=row["event_key"])

    log.info("replay_dlq.done", replayed=len(rows))
    return len(rows)
