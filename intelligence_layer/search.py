"""
Semantic search over embedded comm_events.

Pipeline:
1. Embed the user's query with the SAME OpenAI model used at index time.
   (Mixing models produces meaningless cosine distances.)
2. Ask pgvector for the top SEARCH_CANDIDATE_POOL nearest vectors using the
   HNSW cosine index built in migration 0005.
3. Rerank those candidates with Cohere — pgvector is fast but coarse;
   Cohere is the actual quality lever.
4. Return the top `limit` results with both scores so the dashboard can
   display them.

WHY a plain Python function (no HTTP endpoint in this phase):
Phase 9 is a Streamlit dashboard, which runs Python in-process. It will
import and call this function directly — adding a FastAPI route would
introduce serialization overhead and a second deployment target for no
benefit.
"""

from __future__ import annotations

import asyncio
import time

import cohere
import structlog
from openai import OpenAI

from comm_layer.config import settings
from comm_layer.db import ai_enabled

log = structlog.get_logger()

# How we read the cosine distance back from pgvector. The <=> operator is
# pgvector's cosine-distance operator (lower = more similar). similarity
# = 1 - distance for an intuitive 0..1 scale where higher = better.
_PGVECTOR_QUERY = """\
SELECT comm_event_id, content,
       1 - (embedding <=> $1::vector) AS similarity
FROM embeddings
ORDER BY embedding <=> $1::vector
LIMIT $2;
"""


async def search_events(
    pool,
    query: str,
    *,
    limit: int | None = None,
    candidate_pool: int | None = None,
) -> list[dict]:
    """
    Run a semantic search and return ranked results.

    Returns a list of dicts:
        {
          "comm_event_id": str,
          "content": str,           # the indexed content snippet
          "similarity": float,      # pgvector cosine similarity (0..1)
          "rerank_score": float,    # Cohere relevance (0..1)
        }

    Empty list is returned (no exception) when:
    - AI_ENABLED is False
    - The query is empty / whitespace
    - No embeddings exist yet

    If Cohere fails, results fall back to pgvector ordering — search degrades
    gracefully instead of crashing the dashboard. In that fallback path the
    `rerank_score` field equals the `similarity` value.
    """
    limit = limit or settings.SEARCH_DEFAULT_LIMIT
    candidate_pool = candidate_pool or settings.SEARCH_CANDIDATE_POOL

    if not await ai_enabled(pool):
        log.info("search.skipped_ai_disabled")
        return []

    cleaned = (query or "").strip()
    if not cleaned:
        return []

    # 1. Embed the query
    start = time.monotonic()
    query_vector = await asyncio.to_thread(_embed_query_sync, cleaned)
    embed_ms = int((time.monotonic() - start) * 1000)

    vector_literal = _to_pgvector_literal(query_vector)

    # 2. pgvector top-K
    start = time.monotonic()
    rows = await pool.fetch(_PGVECTOR_QUERY, vector_literal, candidate_pool)
    pgvector_ms = int((time.monotonic() - start) * 1000)

    if not rows:
        log.info(
            "search.no_candidates",
            embed_ms=embed_ms,
            pgvector_ms=pgvector_ms,
        )
        return []

    candidates = [
        {
            "comm_event_id": str(r["comm_event_id"]),
            "content": r["content"],
            "similarity": float(r["similarity"]),
        }
        for r in rows
    ]

    # 3. Cohere rerank — falls back to pgvector ordering on any failure
    start = time.monotonic()
    reranked = await asyncio.to_thread(
        _rerank_with_fallback, cleaned, candidates, limit
    )
    rerank_ms = int((time.monotonic() - start) * 1000)

    log.info(
        "search.completed",
        candidate_count=len(candidates),
        result_count=len(reranked),
        embed_ms=embed_ms,
        pgvector_ms=pgvector_ms,
        rerank_ms=rerank_ms,
    )

    return reranked


# ── Internal helpers ───────────────────────────────────────────────────────────


def _embed_query_sync(query: str) -> list[float]:
    """Same OpenAI model + same dimensionality as the indexer."""
    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(
        model=settings.EMBEDDING_MODEL,
        input=query,
    )
    return list(response.data[0].embedding)


def _rerank_with_fallback(query: str, candidates: list[dict], limit: int) -> list[dict]:
    """
    Try to rerank with Cohere. On any error, return the top `limit` of the
    pgvector results unchanged. Search should NEVER raise to the caller —
    a third-party blip should not break the dashboard.
    """
    try:
        client = cohere.Client(api_key=settings.COHERE_API_KEY)
        documents = [c["content"] for c in candidates]
        response = client.rerank(
            model=settings.RERANK_MODEL,
            query=query,
            documents=documents,
            top_n=min(limit, len(documents)),
        )
        results: list[dict] = []
        for hit in response.results:
            original = candidates[hit.index]
            results.append(
                {
                    "comm_event_id": original["comm_event_id"],
                    "content": original["content"],
                    "similarity": original["similarity"],
                    "rerank_score": float(hit.relevance_score),
                }
            )
        return results
    except Exception as exc:
        log.warning(
            "search.rerank_failed_falling_back",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        fallback: list[dict] = []
        for c in candidates[:limit]:
            fallback.append(
                {
                    "comm_event_id": c["comm_event_id"],
                    "content": c["content"],
                    "similarity": c["similarity"],
                    # No rerank happened — surface similarity for both fields
                    # so callers don't need to handle a missing key.
                    "rerank_score": c["similarity"],
                }
            )
        return fallback


def _to_pgvector_literal(vector: list[float]) -> str:
    """Match the format used at index time (intelligence_layer.embedding)."""
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"
