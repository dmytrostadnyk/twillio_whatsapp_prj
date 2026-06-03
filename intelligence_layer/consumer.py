"""
Intelligence Layer consumer: polls for unenriched comm_events, claims them
atomically, and dispatches enrichment work.

WHY a separate consumer process (not a background task in the FastAPI app):
- The FastAPI app must stay fast and stateless.  AI work (GPT-4o, 3-5 seconds
  per event) would tie up the event loop if run inside the webhook process.
- Running separately means we can scale, restart, or kill AI work independently
  of the webhook handler.

Claiming without a separate queue table:
- We use SELECT FOR UPDATE OF ce SKIP LOCKED to lock the comm_events row,
  then UPSERT into enrichments to win the claim atomically.
- The UNIQUE constraint on enrichments(comm_event_id) (migration 0007) ensures
  only one worker wins; others get 0 rows back from RETURNING.

Crash recovery (lease):
- If the worker dies after inserting enrichments(status='processing') but before
  writing a terminal status, the row stays 'processing'. Migration 0010 adds
  updated_at to enrichments. The claim query treats a 'processing' row older than
  ENRICHMENT_LEASE_SECONDS as reclaimable, mirroring the delivery worker's lease.

Transcription for voice recordings:
- recording.ready events are claimed immediately (no transcript-existence gate).
  The enrichment worker runs transcription inline before calling GPT-4o, so it
  inherits the lease for crash recovery. The webhook tier no longer spawns a
  fire-and-forget background task.
"""

from __future__ import annotations

import asyncio
import json

import structlog

from comm_layer.config import settings
from comm_layer.db import ai_enabled
from intelligence_layer.enrichment import enrich_event

log = structlog.get_logger()

# SQL that atomically picks the next unenriched (or stale) event.
#
# WHY no transcript CTE gate for recording.ready:
# Transcription now runs INSIDE enrich_event (intelligence_layer/transcription.py).
# We claim recording.ready as soon as it lands; the enrichment worker handles
# download → Whisper → INSERT before calling GPT-4o. This gives transcription
# the enrichment lease for crash recovery.
#
# WHY we still LEFT JOIN latest_transcript:
# SMS/WhatsApp transcripts are joined so they remain available in transcript_text
# for _extract_content. For recording.ready, transcript_text is NULL at claim
# time — enrich_event fills it in before calling GPT-4o.
#
# WHY the enrichment predicate allows stale 'processing' rows:
# A row qualifies for claiming when EITHER no enrichment exists (e.id IS NULL) OR
# the existing enrichment is stuck at 'processing' past the lease window. We pass
# the lease in seconds as $1 so tests can override it.
_CLAIM_QUERY = """\
WITH latest_transcript AS (
    SELECT DISTINCT ON (comm_event_id) comm_event_id, text
    FROM transcripts
    ORDER BY comm_event_id, created_at DESC
)
SELECT ce.id, ce.event_type, ce.raw_payload, ce.correlation_id,
       lt.text AS transcript_text
FROM comm_events ce
LEFT JOIN latest_transcript lt ON lt.comm_event_id = ce.id
LEFT JOIN enrichments e ON e.comm_event_id = ce.id
WHERE ce.event_type IN ('sms.received', 'whatsapp.received', 'recording.ready')
  AND (
      e.id IS NULL
      OR (e.status = 'processing'
          AND e.updated_at < NOW() - ($1 || ' seconds')::INTERVAL)
  )
ORDER BY ce.created_at
LIMIT 1
FOR UPDATE OF ce SKIP LOCKED;
"""

# Upsert claim: inserts a fresh row, or re-claims a stale 'processing' row by
# resetting updated_at. A fresh (non-stale) 'processing' row is not touched
# (the WHERE prevents the DO UPDATE) so RETURNING returns 0 rows → lost race.
_INSERT_CLAIM = """\
INSERT INTO enrichments (comm_event_id, status, model, schema_version)
VALUES ($1, 'processing', 'gpt-4o', '1.0')
ON CONFLICT (comm_event_id) DO UPDATE
    SET status = 'processing', updated_at = NOW()
    WHERE enrichments.status = 'processing'
      AND enrichments.updated_at < NOW() - ($2 || ' seconds')::INTERVAL
RETURNING id;
"""


async def claim_next(pool, lease_seconds: int | None = None) -> dict | None:
    """
    Claim the next unenriched comm_event inside a single transaction.

    Returns the event dict if a claim was won, or None if:
    - the queue is empty, or
    - another worker claimed the same event (race on a non-stale row).

    lease_seconds: how long before a 'processing' row is considered stale and
    re-claimable. Defaults to settings.ENRICHMENT_LEASE_SECONDS. Exposed as a
    parameter so tests can pass a small value without patching settings.

    WHY a transaction: SELECT FOR UPDATE and UPSERT must be atomic.
    If we committed the SELECT first and then inserted separately, another
    worker could claim the same event in the gap.
    """
    if lease_seconds is None:
        lease_seconds = settings.ENRICHMENT_LEASE_SECONDS
    lease_str = str(lease_seconds)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(_CLAIM_QUERY, lease_str)
            if row is None:
                return None

            # raw_payload is stored as JSONB — asyncpg returns it as a string.
            raw_payload = row["raw_payload"]
            if isinstance(raw_payload, str):
                raw_payload = json.loads(raw_payload)

            event = {
                "id": row["id"],
                "event_type": row["event_type"],
                "raw_payload": raw_payload,
                "correlation_id": row["correlation_id"],
                "transcript_text": row["transcript_text"],
            }

            # Upsert claim. RETURNING id is empty if a NON-stale processing row
            # already exists for this event — another worker owns it, bail out.
            claim_row = await conn.fetchrow(_INSERT_CLAIM, event["id"], lease_str)
            if claim_row is None:
                log.warning(
                    "consumer.claim_lost_race",
                    comm_event_id=str(event["id"]),
                )
                return None

    return event


async def _worker(pool, supabase, worker_id: int) -> None:
    """
    One crash-isolated poll loop.  Runs until the process is killed.

    WHY the inner try/except: a crash in enrich_event (e.g. unexpected DB error)
    must not kill this worker — it just logs the failure and sleeps before
    retrying.  The outer asyncio.gather uses return_exceptions=True so one
    dead worker doesn't cancel the others.
    """
    log.info("consumer.worker_started", worker_id=worker_id)

    while True:
        try:
            # DB-backed kill switch: reads from app_settings table (migration 0011).
            # Unlike settings.AI_ENABLED (frozen at import), this can be toggled
            # live with a single DB UPDATE and takes effect on the next iteration.
            if not await ai_enabled(pool):
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue

            event = await claim_next(pool)
            if event is None:
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue

            await enrich_event(pool, supabase, event)

        except asyncio.CancelledError:
            # Propagate cancellation so the process can shut down cleanly.
            raise
        except Exception:
            log.exception("consumer.worker_crashed", worker_id=worker_id)
            # Brief pause to avoid a tight crash loop burning CPU.
            await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)


async def run_enrichment_consumer(pool, supabase) -> None:
    """
    Start ENRICHMENT_CONCURRENCY independent worker coroutines.

    WHY return_exceptions=True: if one worker raises an uncaught exception
    (beyond what _worker catches internally), we don't want it to cancel the
    other workers.  The exception is logged at the worker level.

    Phase 8 note: this is now called `run_enrichment_consumer` (was `run_consumer`)
    to distinguish it from `run_embedding_consumer` which runs in the same process.
    """
    workers = [
        _worker(pool, supabase, i)
        for i in range(settings.ENRICHMENT_CONCURRENCY)
    ]
    log.info(
        "enrichment_consumer.starting",
        concurrency=settings.ENRICHMENT_CONCURRENCY,
    )
    await asyncio.gather(*workers, return_exceptions=True)
