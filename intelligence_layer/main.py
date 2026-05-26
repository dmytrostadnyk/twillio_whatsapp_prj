"""
Intelligence Layer entry point — AI enrichment consumer.

Run with:
    python -m intelligence_layer.main

Or via Makefile:
    make intelligence

This is a long-running process, separate from the FastAPI webhook server and
the delivery worker. It has one job: claim unenriched comm_events and enrich
them with GPT-4o structured output.

Reliability model:
- Each worker goroutine catches its own exceptions so one crash does not
  kill the others (see consumer._worker).
- If a worker is SIGKILLed between claiming and completing, the enrichments
  row stays at status='processing'. There is no automatic stale-row reaper
  at this scope — acceptable for portfolio use, but noted as a known limitation.
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
from intelligence_layer.consumer import run_consumer

log = structlog.get_logger(__name__)


async def main() -> None:
    """Start up the intelligence consumer: create pool, supabase client, run loop."""
    configure_logging(settings.LOG_LEVEL)
    log.info("intelligence_layer.initialising", concurrency=settings.ENRICHMENT_CONCURRENCY)

    pool = await create_pool()
    supabase = create_supabase_client()

    try:
        await run_consumer(pool, supabase)
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
