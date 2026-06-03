"""
Unit tests for the Phase 8 semantic search function.

What we test:
1. AI kill switch: AI_ENABLED=False → returns [] immediately. No OpenAI,
   no DB query, no Cohere.
2. No candidates from pgvector → returns [] without calling Cohere.
3. Happy path: query embedded → pgvector returns 20 → Cohere reorders →
   we return `limit` results in Cohere's order, with rerank_score.
4. Cohere fails → graceful fallback to pgvector ordering with rerank_score
   = similarity. No exception propagated.

We mock the asyncpg pool, OpenAI, and Cohere — no real network calls.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_layer.search import search_events

# ── Helpers ────────────────────────────────────────────────────────────────────


def make_mock_pool_with_rows(rows: list[dict]):
    """asyncpg pool whose .fetch returns the given rows."""
    mock_pool = MagicMock()
    mock_pool.fetch = AsyncMock(return_value=rows)
    return mock_pool


def make_pg_row(comm_event_id: uuid.UUID, content: str, similarity: float) -> dict:
    """Shape returned by the pgvector SELECT in search_events."""
    return {
        "comm_event_id": comm_event_id,
        "content": content,
        "similarity": similarity,
    }


def make_embedding_response(dim: int = 1536) -> SimpleNamespace:
    """Mock OpenAI embeddings.create response."""
    fake_vector = [0.001 * i for i in range(dim)]
    return SimpleNamespace(data=[SimpleNamespace(embedding=fake_vector)])


def make_rerank_response(indices_and_scores: list[tuple[int, float]]) -> SimpleNamespace:
    """
    Build a mock Cohere rerank response.

    `indices_and_scores` is the new ranking — each tuple is (original_index,
    relevance_score). The order in the list is the order Cohere returns.
    """
    results = [
        SimpleNamespace(index=idx, relevance_score=score)
        for idx, score in indices_and_scores
    ]
    return SimpleNamespace(results=results)


# ── Test 1: AI kill switch ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_returns_empty_when_ai_disabled():
    """DB kill switch returns False → instant [] return. No OpenAI, no Cohere."""
    mock_pool = make_mock_pool_with_rows([])

    with patch("intelligence_layer.search.settings") as mock_settings:
        mock_settings.SEARCH_DEFAULT_LIMIT = 10
        mock_settings.SEARCH_CANDIDATE_POOL = 20

        with patch("intelligence_layer.search.ai_enabled", AsyncMock(return_value=False)):
            with patch("intelligence_layer.search.OpenAI") as mock_openai_cls:
                with patch("intelligence_layer.search.cohere.Client") as mock_cohere_cls:
                    results = await search_events(mock_pool, "anything")

    assert results == []
    mock_openai_cls.assert_not_called()
    mock_cohere_cls.assert_not_called()
    mock_pool.fetch.assert_not_called()


# ── Test 2: No candidates from pgvector ────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_returns_empty_when_no_candidates():
    """pgvector returns no rows → return [], don't call Cohere."""
    mock_pool = make_mock_pool_with_rows([])

    with patch("intelligence_layer.search.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.COHERE_API_KEY = "co-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"
        mock_settings.RERANK_MODEL = "rerank-english-v3.0"
        mock_settings.SEARCH_DEFAULT_LIMIT = 10
        mock_settings.SEARCH_CANDIDATE_POOL = 20

        with patch("intelligence_layer.search.OpenAI") as mock_openai_cls:
            mock_openai = MagicMock()
            mock_openai.embeddings.create.return_value = make_embedding_response()
            mock_openai_cls.return_value = mock_openai

            with patch("intelligence_layer.search.cohere.Client") as mock_cohere_cls:
                results = await search_events(mock_pool, "billing complaint")

    assert results == []
    # pgvector WAS queried, but Cohere was NOT
    mock_pool.fetch.assert_called_once()
    mock_cohere_cls.assert_not_called()


# ── Test 3: Happy path — pgvector + Cohere rerank ──────────────────────────────


@pytest.mark.asyncio
async def test_search_happy_path_returns_reranked_results():
    """
    pgvector returns 3 candidates, Cohere reorders them (worst → best).
    We expect the result list in Cohere's order, with both scores attached.
    """
    id_a = uuid.UUID("11111111-1111-1111-1111-111111111111")
    id_b = uuid.UUID("22222222-2222-2222-2222-222222222222")
    id_c = uuid.UUID("33333333-3333-3333-3333-333333333333")

    pg_rows = [
        make_pg_row(id_a, "summary about pricing", 0.75),
        make_pg_row(id_b, "summary about billing dispute", 0.70),
        make_pg_row(id_c, "summary about delivery", 0.65),
    ]
    mock_pool = make_mock_pool_with_rows(pg_rows)

    # Cohere swaps order — the billing-dispute candidate (index 1) ranks #1.
    rerank_resp = make_rerank_response(
        [(1, 0.95), (0, 0.30), (2, 0.10)]
    )

    with patch("intelligence_layer.search.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.COHERE_API_KEY = "co-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"
        mock_settings.RERANK_MODEL = "rerank-english-v3.0"
        mock_settings.SEARCH_DEFAULT_LIMIT = 10
        mock_settings.SEARCH_CANDIDATE_POOL = 20

        with patch("intelligence_layer.search.OpenAI") as mock_openai_cls:
            mock_openai = MagicMock()
            mock_openai.embeddings.create.return_value = make_embedding_response()
            mock_openai_cls.return_value = mock_openai

            with patch("intelligence_layer.search.cohere.Client") as mock_cohere_cls:
                mock_cohere = MagicMock()
                mock_cohere.rerank.return_value = rerank_resp
                mock_cohere_cls.return_value = mock_cohere

                results = await search_events(mock_pool, "billing problem")

    assert len(results) == 3
    # First result should be the billing-dispute candidate (was pg index 1)
    assert results[0]["comm_event_id"] == str(id_b)
    assert results[0]["rerank_score"] == pytest.approx(0.95)
    assert results[0]["similarity"] == pytest.approx(0.70)
    # Second is the pricing one (was pg index 0)
    assert results[1]["comm_event_id"] == str(id_a)
    assert results[1]["rerank_score"] == pytest.approx(0.30)
    # Third is delivery (was pg index 2)
    assert results[2]["comm_event_id"] == str(id_c)


# ── Test 4: Cohere fails → graceful fallback ───────────────────────────────────


@pytest.mark.asyncio
async def test_search_falls_back_when_cohere_fails():
    """
    If the Cohere call raises, search returns top `limit` candidates in
    the original pgvector order — no exception propagated to the caller.
    """
    id_a = uuid.UUID("11111111-1111-1111-1111-111111111111")
    id_b = uuid.UUID("22222222-2222-2222-2222-222222222222")

    pg_rows = [
        make_pg_row(id_a, "first content", 0.80),
        make_pg_row(id_b, "second content", 0.70),
    ]
    mock_pool = make_mock_pool_with_rows(pg_rows)

    with patch("intelligence_layer.search.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.COHERE_API_KEY = "co-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"
        mock_settings.RERANK_MODEL = "rerank-english-v3.0"
        mock_settings.SEARCH_DEFAULT_LIMIT = 10
        mock_settings.SEARCH_CANDIDATE_POOL = 20

        with patch("intelligence_layer.search.OpenAI") as mock_openai_cls:
            mock_openai = MagicMock()
            mock_openai.embeddings.create.return_value = make_embedding_response()
            mock_openai_cls.return_value = mock_openai

            with patch("intelligence_layer.search.cohere.Client") as mock_cohere_cls:
                mock_cohere = MagicMock()
                mock_cohere.rerank.side_effect = RuntimeError("Cohere API down")
                mock_cohere_cls.return_value = mock_cohere

                # Must not raise
                results = await search_events(mock_pool, "anything")

    # Fallback path: pgvector order preserved, rerank_score == similarity
    assert len(results) == 2
    assert results[0]["comm_event_id"] == str(id_a)
    assert results[0]["similarity"] == pytest.approx(0.80)
    assert results[0]["rerank_score"] == pytest.approx(0.80)
    assert results[1]["comm_event_id"] == str(id_b)
    assert results[1]["rerank_score"] == pytest.approx(0.70)
