"""
Number registry — resolves a phone number to its business source.

WHY this exists: Twilio gives us a raw phone number (e.g. +15551234567).
The business wants to know WHICH campaign, affiliate, or business unit that
number belongs to. The number_registry table maps numbers to sources.

If the number is not in the registry, we still capture the event — we just
flag it as 'unknown' rather than dropping it. Dropping events is never acceptable.
"""

from __future__ import annotations

import asyncpg
import structlog

from comm_layer.contracts.base import EventSource

log = structlog.get_logger(__name__)


async def resolve_source(pool: asyncpg.Pool, number: str) -> EventSource:
    """
    Look up a phone number in the registry and return its source metadata.

    Always returns an EventSource — if the number is not registered,
    returns one with is_unknown=True. Events are NEVER dropped for unknown numbers.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT source_type, source_id, label, metadata
            FROM number_registry
            WHERE number = $1
            """,
            number,
        )

    if row is None:
        log.info("number_registry.unknown_number", number=number)
        return EventSource(number=number, is_unknown=True)

    return EventSource(
        number=number,
        source_type=row["source_type"],
        source_id=row["source_id"],
        label=row["label"],
        is_unknown=False,
        metadata=dict(row["metadata"]) if row["metadata"] else {},
    )
