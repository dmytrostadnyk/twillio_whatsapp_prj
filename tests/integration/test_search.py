"""
Integration test — Case 5: pre-seeded embedding → semantic search returns it.

Inserts a comm_events + enrichments + embeddings row using the same fixed
1536-dim vector that fake_openai_embedding returns. Because the query vector
and the stored vector are identical, cosine similarity = 1.0, so the row
must appear at the top of results.

WHY this test matters: it confirms that search_events() can round-trip through
the DB — embed query → pgvector cosine distance → Cohere rerank (mocked) →
return results — without calling any real AI API.
"""

from __future__ import annotations

import uuid

import pytest

from comm_layer.config import settings
from intelligence_layer.search import search_events

pytestmark = pytest.mark.integration

# Match the vector produced by the fake_openai_embedding fixture in conftest.py.
_FAKE_VEC = [0.001 * (i % 1000) for i in range(1536)]
_VEC_LITERAL = "[" + ",".join(str(v) for v in _FAKE_VEC) + "]"


async def _insert_searchable_event(conn, event_key: str) -> uuid.UUID:
    """
    Insert a comm_events + enrichments + embeddings row with the fixed test vector.
    Returns comm_event_id.
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
            '{"Body": "Search integration test"}'::jsonb,
            $3, 'delivered'
        )
        RETURNING id
        """,
        event_key,
        settings.TWILIO_PHONE_NUMBER,
        uuid.uuid4(),
    )
    comm_event_id = event_row["id"]

    await conn.execute(
        """
        INSERT INTO enrichments (
            comm_event_id, status, model, schema_version,
            summary, intent, sentiment,
            entities, action_items,
            embedding_status
        )
        VALUES (
            $1, 'completed', 'gpt-4o', '1.0',
            'Integration test summary for semantic search.',
            'general_query', 'neutral',
            '[]'::jsonb, '[]'::jsonb,
            'completed'
        )
        """,
        comm_event_id,
    )

    # Insert the embedding using the same vector the fake fixture returns.
    # $1::vector requires pgvector — uses the same cast as the real embed_event().
    await conn.execute(
        """
        INSERT INTO embeddings (comm_event_id, content, embedding)
        VALUES ($1, $2, $3::vector)
        """,
        comm_event_id,
        "Integration test content for search.",
        _VEC_LITERAL,
    )

    return comm_event_id


@pytest.mark.asyncio
async def test_search_returns_seeded_row(
    pool, fake_openai_embedding, fake_cohere, test_prefix
):
    """
    The pre-seeded embedding has identical cosine similarity = 1.0 to the
    query vector, so it must appear in the results with similarity >= 0.99.
    """
    event_key = f"{test_prefix}SM-search-1:sms.received"

    async with pool.acquire() as conn:
        comm_event_id = await _insert_searchable_event(conn, event_key)

    results = await search_events(pool, "integration test search query")

    assert isinstance(results, list), "search_events() must return a list"
    assert len(results) > 0, "Expected at least one search result"

    # Find our specific test row among the results
    matching = [r for r in results if str(r["comm_event_id"]) == str(comm_event_id)]
    assert matching, (
        f"Test row (comm_event_id={comm_event_id}) not found in search results. "
        f"Got IDs: {[r['comm_event_id'] for r in results]}"
    )

    result = matching[0]
    assert result["similarity"] >= 0.99, (
        f"Expected similarity >= 0.99 (identical vectors), got {result['similarity']}"
    )
    assert "rerank_score" in result, "Result must include rerank_score from Cohere step"


@pytest.mark.asyncio
async def test_search_returns_empty_for_ai_disabled(pool, monkeypatch):
    """
    When AI_ENABLED=False, search_events() must return an empty list
    without hitting the DB or raising an exception.
    """
    monkeypatch.setattr(settings, "AI_ENABLED", False)
    results = await search_events(pool, "any query")
    assert results == [], f"Expected [] when AI disabled, got {results}"
