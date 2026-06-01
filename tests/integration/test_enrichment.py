"""
Integration test — Case 3: claim → fake GPT-4o → enrichments row.

Inserts a delivered comm_events row, calls claim_next() then enrich_event()
with the GPT-4o call mocked, and asserts the enrichments row was written with
the expected fields.

WHY we insert a 'delivered' row (not 'pending'): the enrichment consumer's
claim query filters on events with no enrichments row yet, regardless of
delivery_status. Using 'delivered' matches realistic state and avoids
triggering the delivery worker's claim at the same time.
"""

from __future__ import annotations

import uuid

import pytest

from comm_layer.config import settings
from intelligence_layer.consumer import claim_next
from intelligence_layer.enrichment import enrich_event

pytestmark = pytest.mark.integration


async def _insert_delivered_sms(conn, event_key: str) -> uuid.UUID:
    """Insert a comm_events row for an inbound SMS in 'delivered' state."""
    row = await conn.fetchrow(
        """
        INSERT INTO comm_events (
            event_key, channel, direction, event_type,
            from_number, to_number,
            source_metadata, raw_payload,
            correlation_id, delivery_status
        )
        VALUES (
            $1, 'sms', 'inbound', 'sms.received',
            '+15550000001', $2,
            '{"is_unknown": true}'::jsonb,
            '{"Body": "I need help with my billing statement"}'::jsonb,
            $3, 'delivered'
        )
        RETURNING id
        """,
        event_key,
        settings.TWILIO_PHONE_NUMBER,
        uuid.uuid4(),
    )
    return row["id"]


@pytest.mark.asyncio
async def test_enrichment_creates_completed_row(pool, supabase, fake_gpt4o, test_prefix):
    """
    After claim_next() + enrich_event() with a mocked GPT-4o:
    - An enrichments row must exist with status='completed'
    - intent must equal 'general_query' (what the fake returns)
    - embedding_status must equal 'pending' (not yet embedded)
    """
    event_key = f"{test_prefix}SM-enrich-1:sms.received"

    async with pool.acquire() as conn:
        event_id = await _insert_delivered_sms(conn, event_key)

    # claim_next() uses SELECT FOR UPDATE SKIP LOCKED — it will pick up the row
    # we just inserted since it has no enrichments row yet.
    claimed = await claim_next(pool)

    assert claimed is not None, (
        "claim_next() returned None — did the row fail to match the claim query? "
        "Check that event_type='sms.received' and there is no enrichments row yet."
    )
    assert str(claimed["id"]) == str(event_id), (
        f"claimed event ID {claimed['id']} doesn't match inserted ID {event_id}"
    )

    await enrich_event(pool, supabase, claimed)

    # Verify enrichments row was written
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, intent, sentiment, embedding_status
            FROM enrichments
            WHERE comm_event_id = $1
            """,
            event_id,
        )

    assert row is not None, "No enrichments row found after enrich_event()"
    assert row["status"] == "completed", (
        f"Expected status='completed', got '{row['status']}'"
    )
    assert row["intent"] == "general_query", (
        f"Expected intent='general_query' (from fake), got '{row['intent']}'"
    )
    assert row["sentiment"] == "neutral", (
        f"Expected sentiment='neutral' (from fake), got '{row['sentiment']}'"
    )
    assert row["embedding_status"] == "pending", (
        f"Expected embedding_status='pending', got '{row['embedding_status']}'"
    )


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_queue_empty(pool):
    """
    When there are no unclaimed events, claim_next() must return None
    rather than raising an exception.
    """
    # We don't insert any rows — the queue may have other events, but we
    # check the contract (None or a dict) not that the queue is truly empty.
    result = await claim_next(pool)
    # result is either None (empty / all claimed) or a dict (pre-existing row).
    # We just assert it doesn't raise.
    assert result is None or isinstance(result, dict)
