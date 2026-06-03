"""
Embedding consumer: takes completed enrichments, generates an OpenAI embedding
from the summary + content, and writes the vector to the embeddings table.

WHY this lives in intelligence_layer/ alongside enrichment.py:
- Both consumers operate on the same enrichments + comm_events tables.
- They share the same AI kill switch and the same poll/sleep pattern.
- Running them in one process keeps deployment simple — one container, one
  python -m intelligence_layer.main starts both pipelines.

WHY a separate state machine (enrichment.embedding_status) instead of using
the embeddings table as the claim marker:
- pgvector requires a real, non-null vector at insert time. We cannot insert
  a placeholder before the OpenAI call returns. So we mark the SOURCE row as
  'processing' first, do the slow API call, then commit the embedding + flip
  the source row to 'completed' in a short, blocking-free transaction.
"""

from __future__ import annotations

import asyncio
import json
import time

import structlog
from openai import OpenAI

from comm_layer.config import settings
from comm_layer.db import ai_enabled

log = structlog.get_logger()

# ── Constants ──────────────────────────────────────────────────────────────────

# Max characters sent to the embedding API per event. text-embedding-3-small
# accepts up to 8 191 tokens (~32 000 chars). We truncate slightly below that
# to leave headroom for prefix labels ("Summary:", "Transcript:", etc.).
MAX_CONTENT_CHARS = 30_000

# Retry policy: 1 initial attempt + MAX_RETRIES extra attempts.
MAX_RETRIES = 2
RETRY_SLEEP_SECONDS = 2.0

# SQL — claim one enrichment whose embedding is still pending.
# DISTINCT ON picks the LATEST transcript per call so multiple transcript
# rows for one recording don't multiply the result set.
_CLAIM_QUERY = """\
SELECT e.id AS enrichment_id, e.comm_event_id, e.summary,
       ce.event_type, ce.raw_payload, lt.text AS transcript_text
FROM enrichments e
JOIN comm_events ce ON ce.id = e.comm_event_id
LEFT JOIN (
    SELECT DISTINCT ON (comm_event_id) comm_event_id, text
    FROM transcripts
    ORDER BY comm_event_id, created_at DESC
) lt ON lt.comm_event_id = ce.id
WHERE e.status = 'completed'
  AND e.embedding_status = 'pending'
ORDER BY e.created_at
LIMIT 1
FOR UPDATE OF e SKIP LOCKED;
"""

_MARK_PROCESSING = """\
UPDATE enrichments SET embedding_status = 'processing' WHERE id = $1;
"""

_INSERT_EMBEDDING = """\
INSERT INTO embeddings (comm_event_id, content, embedding)
VALUES ($1, $2, $3::vector);
"""

_MARK_COMPLETED = """\
UPDATE enrichments SET embedding_status = 'completed' WHERE id = $1;
"""

_MARK_FAILED = """\
UPDATE enrichments SET embedding_status = 'failed' WHERE id = $1;
"""


# ── Public entry points ────────────────────────────────────────────────────────


async def claim_next_for_embedding(pool) -> dict | None:
    """
    Claim the next enrichment that still needs an embedding.

    Returns the row data on success, or None if the queue is empty.
    The claim happens inside a single short transaction: we select with
    FOR UPDATE SKIP LOCKED and immediately UPDATE the row to 'processing'
    so other workers skip it.

    The slow OpenAI call happens AFTER this function returns and the
    transaction has closed — never hold a DB lock across a network call.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(_CLAIM_QUERY)
            if row is None:
                return None

            raw_payload = row["raw_payload"]
            if isinstance(raw_payload, str):
                raw_payload = json.loads(raw_payload)

            event = {
                "enrichment_id": row["enrichment_id"],
                "comm_event_id": row["comm_event_id"],
                "summary": row["summary"],
                "event_type": row["event_type"],
                "raw_payload": raw_payload,
                "transcript_text": row["transcript_text"],
            }
            await conn.execute(_MARK_PROCESSING, event["enrichment_id"])
    return event


async def embed_event(pool, event: dict) -> None:
    """
    Generate the embedding for one claimed event and persist it.

    On success: INSERT into embeddings + UPDATE enrichments to 'completed'.
    On total failure: UPDATE enrichments to 'failed' (no embeddings row written).
    Both DB writes happen in a single transaction so we never end up with an
    orphan embedding or a stuck 'processing' status.
    """
    enrichment_id = event["enrichment_id"]
    comm_event_id = str(event["comm_event_id"])
    event_type = event["event_type"]

    content = _build_content(event)
    if not content:
        log.warning(
            "embedding.no_content",
            comm_event_id=comm_event_id,
            event_type=event_type,
        )
        await _mark_failed(pool, enrichment_id)
        return

    truncated = content[:MAX_CONTENT_CHARS]

    log.info(
        "embedding.started",
        comm_event_id=comm_event_id,
        event_type=event_type,
        content_length=len(truncated),
    )

    start = time.monotonic()
    vector = await asyncio.to_thread(_embed_with_retries, truncated, comm_event_id)
    latency_ms = int((time.monotonic() - start) * 1000)

    if vector is None:
        await _mark_failed(pool, enrichment_id)
        log.error(
            "embedding.failed",
            comm_event_id=comm_event_id,
            latency_ms=latency_ms,
        )
        return

    vector_literal = _to_pgvector_literal(vector)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(_INSERT_EMBEDDING, event["comm_event_id"], truncated, vector_literal)
            await conn.execute(_MARK_COMPLETED, enrichment_id)

    log.info(
        "embedding.completed",
        comm_event_id=comm_event_id,
        event_type=event_type,
        latency_ms=latency_ms,
        # vector length is a safe non-PII signal of "we got a real response"
        vector_dim=len(vector),
    )


async def run_embedding_consumer(pool) -> None:
    """
    Spawn EMBEDDING_CONCURRENCY embedding workers.

    return_exceptions=True so one worker dying does NOT cancel the others.
    Each worker also catches its own exceptions per-iteration so the loop
    survives an unexpected error from the DB or OpenAI without dying.
    """
    workers = [
        _embedding_worker(pool, i)
        for i in range(settings.EMBEDDING_CONCURRENCY)
    ]
    log.info(
        "embedding_consumer.starting",
        concurrency=settings.EMBEDDING_CONCURRENCY,
        model=settings.EMBEDDING_MODEL,
    )
    await asyncio.gather(*workers, return_exceptions=True)


# ── Internal helpers ───────────────────────────────────────────────────────────


async def _embedding_worker(pool, worker_id: int) -> None:
    """One crash-isolated poll loop for embeddings."""
    log.info("embedding_consumer.worker_started", worker_id=worker_id)

    while True:
        try:
            if not await ai_enabled(pool):
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue

            event = await claim_next_for_embedding(pool)
            if event is None:
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue

            await embed_event(pool, event)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("embedding_consumer.worker_crashed", worker_id=worker_id)
            await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)


def _build_content(event: dict) -> str:
    """
    Assemble the text we send to the embedding model.

    Summary first (the most concentrated semantic signal), then the actual
    user-facing text. We keep the labels so the model sees clearly-separated
    sections — helps cluster on semantic intent rather than message format.
    """
    parts: list[str] = []

    summary = (event.get("summary") or "").strip()
    if summary:
        parts.append(f"Summary: {summary}")

    event_type = event.get("event_type", "")
    raw_payload = event.get("raw_payload") or {}

    if event_type in ("sms.received", "whatsapp.received"):
        body = (raw_payload.get("Body") or "").strip()
        if body:
            parts.append(f"Message: {body}")
    elif event_type == "recording.ready":
        transcript = (event.get("transcript_text") or "").strip()
        if transcript:
            parts.append(f"Transcript: {transcript}")

    return "\n\n".join(parts)


def _embed_with_retries(content: str, comm_event_id: str) -> list[float] | None:
    """
    Sync wrapper (intended for asyncio.to_thread). Calls OpenAI's embedding
    endpoint up to MAX_RETRIES+1 times. Returns the vector on success,
    or None if every attempt failed.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            return _embed_sync(content)
        except Exception as exc:
            log.warning(
                "embedding.attempt_failed",
                comm_event_id=comm_event_id,
                attempt=attempt + 1,
                max_attempts=MAX_RETRIES + 1,
                error=str(exc),
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    return None


def _embed_sync(content: str) -> list[float]:
    """One blocking OpenAI embedding call. Returns the 1536-dim vector."""
    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(
        model=settings.EMBEDDING_MODEL,
        input=content,
    )
    return list(response.data[0].embedding)


def _to_pgvector_literal(vector: list[float]) -> str:
    """
    Convert a Python list of floats to the pgvector text literal '[v1,v2,...]'.

    WHY a string and not a typed parameter: avoiding a new dependency on the
    pgvector-python package. asyncpg passes the string through unchanged and
    we explicitly cast to ::vector in the INSERT statement.
    """
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


async def _mark_failed(pool, enrichment_id) -> None:
    """Flip the source enrichment row to embedding_status='failed'."""
    async with pool.acquire() as conn:
        await conn.execute(_MARK_FAILED, enrichment_id)
