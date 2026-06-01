"""
Integration test — Case 4: claim → fake embedding → embeddings row.

Inserts a completed enrichments row (embedding_status='pending'), calls
claim_next_for_embedding() then embed_event() with the OpenAI embedding
call mocked, and asserts the embeddings row was written with the right
number of dimensions.

WHY we insert the enrichments row directly rather than going through
enrich_event(): this test only covers the embedding pipeline in isolation.
Mixing enrichment + embedding in one test would conflate two failure modes.
"""

from __future__ import annotations

import uuid

import pytest

from comm_layer.config import settings
from intelligence_layer.embedding import claim_next_for_embedding, embed_event

pytestmark = pytest.mark.integration

# The fake vector has this many dimensions (must match OpenAI text-embedding-3-small).
EXPECTED_DIMS = 1536


async def _insert_enriched_event(conn, event_key: str) -> tuple[uuid.UUID, uuid.UUID]:
    """
    Insert a comm_events row + a completed enrichments row (embedding_status='pending').
    Returns (comm_event_id, enrichment_id).
    """
    event_row = await conn.fetchrow(
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
            '{"Body": "Embedding integration test message"}'::jsonb,
            $3, 'delivered'
        )
        RETURNING id
        """,
        event_key,
        settings.TWILIO_PHONE_NUMBER,
        uuid.uuid4(),
    )
    comm_event_id = event_row["id"]

    enrichment_row = await conn.fetchrow(
        """
        INSERT INTO enrichments (
            comm_event_id, status, model, schema_version,
            summary, intent, sentiment,
            entities, action_items,
            embedding_status
        )
        VALUES (
            $1, 'completed', 'gpt-4o', '1.0',
            'Test summary for embedding.', 'general_query', 'neutral',
            '[]'::jsonb, '[]'::jsonb,
            'pending'
        )
        RETURNING id
        """,
        comm_event_id,
    )
    return comm_event_id, enrichment_row["id"]


@pytest.mark.asyncio
async def test_embed_event_creates_embedding_row(pool, fake_openai_embedding, test_prefix):
    """
    After claim_next_for_embedding() + embed_event() with a mocked OpenAI call:
    - An embeddings row must exist for the comm_event_id
    - The stored vector must have exactly 1536 dimensions
    - enrichments.embedding_status must be flipped to 'completed'
    """
    event_key = f"{test_prefix}SM-embed-1:sms.received"

    async with pool.acquire() as conn:
        comm_event_id, enrichment_id = await _insert_enriched_event(conn, event_key)

    claimed = await claim_next_for_embedding(pool)

    assert claimed is not None, (
        "claim_next_for_embedding() returned None — did the row fail to match? "
        "Check that embedding_status='pending' and status='completed'."
    )
    assert str(claimed["comm_event_id"]) == str(comm_event_id), (
        f"Claimed event {claimed['comm_event_id']} != inserted event {comm_event_id}"
    )

    await embed_event(pool, claimed)

    # Verify the embeddings row
    async with pool.acquire() as conn:
        embed_row = await conn.fetchrow(
            "SELECT content, embedding FROM embeddings WHERE comm_event_id = $1",
            comm_event_id,
        )
        enrich_status = await conn.fetchval(
            "SELECT embedding_status FROM enrichments WHERE id = $1",
            enrichment_id,
        )

    assert embed_row is not None, "No embeddings row found after embed_event()"
    assert embed_row["content"], "embeddings.content should be non-empty"

    # pgvector stores the vector; asyncpg returns it as a string like "[0.001,0.002,...]"
    raw_vector = embed_row["embedding"]
    assert raw_vector is not None, "embeddings.embedding column should be non-null"

    # Parse the vector string to count dimensions
    vector_str = str(raw_vector).strip("[]")
    dims = len([v for v in vector_str.split(",") if v.strip()])
    assert dims == EXPECTED_DIMS, (
        f"Expected {EXPECTED_DIMS} dimensions, got {dims}"
    )

    assert enrich_status == "completed", (
        f"Expected enrichments.embedding_status='completed', got '{enrich_status}'"
    )
