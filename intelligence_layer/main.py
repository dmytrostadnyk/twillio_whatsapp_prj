"""
Intelligence Layer entry point — AI enrichment + embedding + WhatsApp reply consumers.

Run with:
    python -m intelligence_layer.main

Or via Makefile:
    make intel

This is a long-running process, separate from the FastAPI webhook server and
the delivery worker. It runs THREE consumer pipelines side by side:
  1. Enrichment consumer  — calls GPT-4o for summary/intent/sentiment.
  2. Embedding consumer   — calls OpenAI embeddings for semantic search.
  3. WhatsApp reply       — generates and sends AI replies via Twilio.

Reliability model:
- All three consumers run under one asyncio.gather(return_exceptions=True) so a
  failure inside one pipeline does NOT kill the others. Each consumer in turn
  isolates per-worker exceptions internally.
- Enrichment crash recovery: migration 0010 adds a lease on enrichments rows so
  stale 'processing' rows are re-claimed after ENRICHMENT_LEASE_SECONDS.
- Reply crash recovery: migration 0012 adds the same lease pattern to
  whatsapp_replies. The at-most-once rule (stale 'sending' → 'failed') prevents
  double-texting customers even if the process is killed mid-send.
- AI kill switch: the DB-backed ai_enabled flag (migration 0011) halts all AI work
  instantly without a process restart. WHATSAPP_AUTOREPLY_ENABLED is an additional
  flag that controls only the reply consumer.
"""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog
from twilio.http.http_client import TwilioHttpClient
from twilio.rest import Client

from comm_layer.config import settings
from comm_layer.db import create_pool, create_supabase_client
from comm_layer.logging_config import configure_logging
from intelligence_layer.consumer import run_enrichment_consumer
from intelligence_layer.embedding import run_embedding_consumer
from intelligence_layer.whatsapp_reply import run_whatsapp_reply_consumer

log = structlog.get_logger(__name__)


async def main() -> None:
    """Start up all three intelligence consumers: pool, supabase, Twilio client."""
    configure_logging(settings.LOG_LEVEL)
    log.info(
        "intelligence_layer.initialising",
        enrichment_concurrency=settings.ENRICHMENT_CONCURRENCY,
        embedding_concurrency=settings.EMBEDDING_CONCURRENCY,
        reply_concurrency=settings.WHATSAPP_REPLY_CONCURRENCY,
    )

    pool = await create_pool()
    supabase = await create_supabase_client()

    # Construct the Twilio client with an explicit timeout (per outbound.py's caller
    # responsibility note). The SDK's default has NO timeout — a network partition
    # would hang the executor thread indefinitely and block the reply consumer.
    twilio_client = Client(
        settings.TWILIO_ACCOUNT_SID,
        settings.TWILIO_AUTH_TOKEN,
        http_client=TwilioHttpClient(timeout=10.0),
    )

    try:
        # return_exceptions=True so one consumer crashing doesn't take down the
        # others. The per-worker try/except inside each consumer already keeps
        # individual failures from escaping; this is belt-and-braces.
        await asyncio.gather(
            run_enrichment_consumer(pool, supabase),
            run_embedding_consumer(pool),
            run_whatsapp_reply_consumer(pool, twilio_client),
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
