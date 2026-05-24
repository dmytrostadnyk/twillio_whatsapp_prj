"""
Delivery worker — polls the broker queue and ships events to Azure CRM.

Run with:
    python delivery_worker/main.py

Or via Makefile:
    make worker

This is a long-running process, separate from the FastAPI webhook server.
It has exactly one job: claim pending events from the queue and POST them to
the Azure CRM endpoint with guaranteed delivery semantics.

Reliability model:
  - Crash recovery: if the worker dies mid-processing, the lease on the claimed
    row expires (DELIVERY_LEASE_SECONDS) and another worker instance re-claims it.
  - 4xx from Azure → dead-letter immediately. The schema is wrong; retrying
    is pointless and would just eat up DELIVERY_MAX_ATTEMPTS for no reason.
  - 5xx or timeout from Azure → retry with exponential backoff + jitter.
    Azure is temporarily down or overloaded; we will get there eventually.
  - Max attempts: after DELIVERY_MAX_ATTEMPTS the event moves to 'dead'.
    Use scripts/replay_dlq.py to re-queue after fixing the root cause.
"""

from __future__ import annotations

import asyncio
import random
import signal
import sys

import httpx
import structlog

from comm_layer.broker.base import BrokerMessage
from comm_layer.broker.postgres import PostgresBroker
from comm_layer.config import settings
from comm_layer.db import create_pool
from comm_layer.logging_config import configure_logging
from delivery_worker.transform import build_contract_payload

log = structlog.get_logger(__name__)

# ── Backoff ────────────────────────────────────────────────────────────────────

_BACKOFF_MAX_SECONDS = 300.0  # cap at 5 minutes regardless of attempt count


def compute_backoff(attempt: int) -> float:
    """
    Exponential backoff with full jitter.

    WHY full jitter (random between 0 and cap) instead of plain exponential:
    If Azure goes down and all workers wake up at the same time after a fixed
    delay, they create a retry thunderstorm that can overwhelm Azure when it
    comes back up. Spreading retries randomly across the window avoids this.

    Formula: cap = min(base * 2^attempt, max); jitter = random(0, cap)
    """
    cap = min(settings.DELIVERY_BACKOFF_BASE_SECONDS * (2 ** attempt), _BACKOFF_MAX_SECONDS)
    return random.uniform(0, cap)


# ── Per-message processing ─────────────────────────────────────────────────────


async def process_message(
    broker: PostgresBroker,
    http_client: httpx.AsyncClient,
    msg: BrokerMessage,
) -> None:
    """
    Deliver one event to Azure CRM. Exactly three outcomes are possible:
      1. Success (2xx)  → ack
      2. Our schema bug (4xx) → dead-letter immediately
      3. Azure down (5xx/timeout) → nack with backoff

    WHY we dead-letter on 4xx but retry on 5xx:
    A 422 from Azure means our contract payload is malformed — retrying the
    exact same payload will always fail. A 503 means Azure is temporarily
    unavailable — the same payload will succeed once Azure recovers.
    """
    structlog.contextvars.bind_contextvars(
        event_key=msg.event_key,
        correlation_id=str(msg.correlation_id),
        attempt=msg.attempt_count,
    )

    # Guard: give up before even making the HTTP call if we've exhausted retries
    if msg.attempt_count > settings.DELIVERY_MAX_ATTEMPTS:
        await broker.dead_letter(
            msg.id,
            f"Exceeded maximum delivery attempts ({settings.DELIVERY_MAX_ATTEMPTS})",
        )
        log.error("delivery.max_attempts_exceeded", event_key=msg.event_key)
        return

    contract = build_contract_payload(msg)

    try:
        response = await http_client.post(
            f"{settings.AZURE_CRM_URL}/events",
            json=contract,
            timeout=10.0,
        )
    except httpx.TimeoutException as exc:
        backoff = compute_backoff(msg.attempt_count)
        await broker.nack(msg.id, f"HTTP timeout: {exc}", backoff)
        log.warning("delivery.timeout_retry", backoff_seconds=backoff)
        return
    except httpx.ConnectError as exc:
        backoff = compute_backoff(msg.attempt_count)
        await broker.nack(msg.id, f"Connection error: {exc}", backoff)
        log.warning("delivery.connect_error_retry", backoff_seconds=backoff)
        return

    if response.status_code < 400:
        # Success — record the exact contract we shipped for auditability
        await broker.ack(msg.id, contract_payload=contract)
        log.info("delivery.success", http_status=response.status_code)

    elif 400 <= response.status_code < 500:
        # Our contract is wrong — retrying the same payload won't help
        reason = f"HTTP {response.status_code}: {response.text[:300]}"
        await broker.dead_letter(msg.id, reason)
        log.error("delivery.dead_lettered_4xx", http_status=response.status_code, reason=reason)

    else:
        # Azure is down or overloaded — retry later
        backoff = compute_backoff(msg.attempt_count)
        await broker.nack(msg.id, f"HTTP {response.status_code}", backoff)
        log.warning(
            "delivery.server_error_retry",
            http_status=response.status_code,
            backoff_seconds=backoff,
        )


# ── Poll loop ──────────────────────────────────────────────────────────────────


async def run_worker(broker: PostgresBroker, http_client: httpx.AsyncClient) -> None:
    """
    Main poll loop. Runs forever until a shutdown signal is received.

    WHY sleep on empty queue instead of tight-looping:
    A tight loop would hit the DB thousands of times per second when the queue
    is empty, burning CPU and DB connection quota for nothing. Sleeping
    DELIVERY_POLL_INTERVAL_SECONDS between empty polls keeps resource use flat.
    """
    log.info("delivery_worker.started", poll_interval=settings.DELIVERY_POLL_INTERVAL_SECONDS)

    while True:
        try:
            msg = await broker.claim_next()
            if msg is None:
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue
            await process_message(broker, http_client, msg)

        except asyncio.CancelledError:
            # Clean shutdown — let the finally block in main() close resources
            log.info("delivery_worker.stopping")
            raise

        except Exception as exc:
            # Unexpected error in the loop itself (not in process_message).
            # Log and keep running — one bad row should not kill the worker.
            log.exception("delivery_worker.unexpected_error", error=str(exc))
            await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)


# ── Entry point ────────────────────────────────────────────────────────────────


async def main() -> None:
    """Start up the worker: create pool, wire broker + HTTP client, run loop."""
    configure_logging(settings.LOG_LEVEL)
    log.info("delivery_worker.initialising")

    pool = await create_pool()
    broker = PostgresBroker(pool=pool)

    # A single shared httpx client uses connection pooling, which is more
    # efficient than creating a new connection for every event.
    async with httpx.AsyncClient() as http_client:
        try:
            await run_worker(broker, http_client)
        finally:
            await pool.close()
            log.info("delivery_worker.stopped")


def _handle_signal(sig, loop: asyncio.AbstractEventLoop) -> None:
    """Graceful shutdown on SIGINT / SIGTERM."""
    log.info("delivery_worker.signal_received", signal=sig)
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
