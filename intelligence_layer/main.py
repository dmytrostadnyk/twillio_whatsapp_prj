"""
Intelligence Layer entry point — AI enrichment + embedding consumers.

Run with:
    python -m intelligence_layer.main

Or via Makefile:
    make intelligence

This is a long-running process, separate from the FastAPI webhook server and
the delivery worker. It runs TWO consumer pipelines side by side:
  1. Enrichment consumer (Phase 7) — calls GPT-4o for summary/intent/sentiment.
  2. Embedding consumer (Phase 8) — calls OpenAI embeddings for semantic search.

Reliability model:
- Both consumers run under one asyncio.gather(return_exceptions=True) so a
  failure inside one pipeline does NOT kill the other. Each consumer in turn
  isolates per-worker exceptions internally.
- If a worker is SIGKILLed between claiming and completing, the corresponding
  status field stays at 'processing'. No automatic stale-row reaper at this
  scope — acceptable for portfolio use, but noted as a known limitation.
- AI kill switch: if AI_ENABLED=False, workers sleep without calling any AI
  provider. Flip the env var to resume without restarting the process.
"""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog

from comm_layer.config import settings
from comm_layer.db import create_pool, create_supabase_client
from comm_layer.logging_config import configure_logging
from intelligence_layer.consumer import run_enrichment_consumer
from intelligence_layer.embedding import run_embedding_consumer

log = structlog.get_logger(__name__)


async def main() -> None:
    """Start up both intelligence consumers: pool, supabase, run both loops."""
    configure_logging(settings.LOG_LEVEL)
    log.info(
        "intelligence_layer.initialising",
        enrichment_concurrency=settings.ENRICHMENT_CONCURRENCY,
        embedding_concurrency=settings.EMBEDDING_CONCURRENCY,
    )

    pool = await create_pool()
    supabase = create_supabase_client()

    try:
        # return_exceptions=True so one consumer crashing doesn't take down the
        # other. The per-worker try/except inside each consumer already keeps
        # individual failures from escaping; this is belt-and-braces.
        await asyncio.gather(
            run_enrichment_consumer(pool, supabase),
            run_embedding_consumer(pool),
            return_exceptions=True,
        )
    finally:
        await pool.close()
        log.info("intelligence_layer.stopped")


def _handle_signal(sig, loop: asyncio.AbstractEventLoop) -> None:
    """Graceful shutdown on SIGINT / SIGTERM."""
    log.info("intelligence_layer.signal_received", signal=sig)
    for task in asyncio.all_tasks(loop):
        task.cancel()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig, loop)
    try:
        loop.run_until_complete(main())
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
    sys.exit(0)
