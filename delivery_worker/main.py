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
  - 4xx from Azure (most codes) → dead-letter immediately. The payload is bad
    and retrying will fail the same way. Exceptions are 408/425/429, which
    are transient and DO get retried.
  - 5xx, timeout, or network error → retry with exponential backoff + jitter.
    If Azure sends a Retry-After header on 429/503, we honor it instead.
  - Max attempts: after DELIVERY_MAX_ATTEMPTS the event moves to 'dead'.
    Use scripts/replay_dlq.py to re-queue after fixing the root cause.
"""

from __future__ import annotations

import asyncio
import random
import signal
import sys
import time

import httpx
import structlog

from comm_layer.broker.base import BrokerMessage
from comm_layer.broker.postgres import PostgresBroker
from comm_layer.config import settings
from comm_layer.db import create_pool
from comm_layer.logging_config import configure_logging
from delivery_worker.transform import build_contract_payload

log = structlog.get_logger(__name__)

# 4xx status codes that are actually transient and SHOULD be retried, per HTTP spec:
#   408 Request Timeout — server gave up waiting for the request
#   425 Too Early       — server unwilling to process replay-able request right now
#   429 Too Many Requests — rate limit, retry with backoff
# Every other 4xx means "your request is wrong" and we dead-letter immediately.
_RETRYABLE_4XX = frozenset({408, 425, 429})


# ── Backoff ────────────────────────────────────────────────────────────────────


def compute_backoff(attempt: int) -> float:
    """
    Exponential backoff with full jitter.

    WHY full jitter (random between 0 and cap) instead of plain exponential:
    If Azure goes down and all workers wake up at the same time after a fixed
    delay, they create a retry thunderstorm that can overwhelm Azure when it
    comes back up. Spreading retries randomly across the window avoids this.

    Formula: cap = min(base * 2^attempt, max); jitter = random(0, cap)
    """
    cap = min(
        settings.DELIVERY_BACKOFF_BASE_SECONDS * (2 ** attempt),
        settings.DELIVERY_BACKOFF_MAX_SECONDS,
    )
    return random.uniform(0, cap)


def parse_retry_after(response: httpx.Response) -> float | None:
    """
    Parse the Retry-After header from an HTTP response.

    Per RFC 7231 the value can be either a number of seconds or an HTTP-date.
    We only handle the seconds form because that's what nearly every API uses
    in practice; the HTTP-date form is rare enough to not be worth the parser.

    Returns the delay in seconds, or None if the header is missing or unparseable.
    """
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        # HTTP-date form or garbage — fall back to our own backoff
        return None


# ── Delivery log ───────────────────────────────────────────────────────────────


async def _write_delivery_log(
    pool,
    comm_event_id,
    correlation_id,
    attempt_number: int,
    status: str,
    http_status: int | None = None,
    latency_ms: int | None = None,
    error_message: str | None = None,
) -> None:
    """Insert one row into delivery_log for every delivery attempt (pass or fail)."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO delivery_log (
                comm_event_id, correlation_id, attempt_number,
                status, http_status, latency_ms, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            comm_event_id,
            correlation_id,
            attempt_number,
            status,
            http_status,
            latency_ms,
            error_message,
        )


# ── Per-message processing ─────────────────────────────────────────────────────


async def process_message(
    broker: PostgresBroker,
    http_client: httpx.AsyncClient,
    msg: BrokerMessage,
    pool,
) -> None:
    """
    Deliver one event to Azure CRM. Outcomes:
      1. Success (2xx)                       → ack
      2. Transient 4xx (408/425/429)         → nack (retry; Retry-After honored)
      3. Other 4xx (schema/auth error)       → dead-letter immediately
      4. 5xx                                 → nack (retry; Retry-After honored)
      5. Network/timeout error               → nack (retry)
      6. Max attempts exceeded               → dead-letter before any HTTP call

    Tracing headers (X-Correlation-Id, X-Event-Key) are added to every request
    so Azure CRM can correlate its own logs to events in our system without
    parsing the body.
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
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", error_message="max_attempts_exceeded",
        )
        log.error("delivery.max_attempts_exceeded", event_key=msg.event_key)
        return

    contract = build_contract_payload(msg)
    headers = {
        "X-Correlation-Id": str(msg.correlation_id),
        "X-Event-Key": msg.event_key,
    }

    t0 = time.monotonic()
    try:
        response = await http_client.post(
            f"{settings.AZURE_CRM_URL}/events",
            json=contract,
            headers=headers,
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        # Covers TimeoutException, ConnectError, ReadError, WriteError, PoolTimeout,
        # NetworkError — any I/O failure during the request. All retryable.
        latency_ms = int((time.monotonic() - t0) * 1000)
        backoff = compute_backoff(msg.attempt_count)
        error_type = type(exc).__name__
        await broker.nack(msg.id, f"{error_type}: {exc}", backoff)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", latency_ms=latency_ms, error_message=f"{error_type}: {exc}",
        )
        log.warning("delivery.network_error_retry", error_type=error_type, backoff_seconds=backoff)
        return

    latency_ms = int((time.monotonic() - t0) * 1000)

    if response.status_code < 400:
        # Success — record the exact contract we shipped for auditability
        await broker.ack(msg.id, contract_payload=contract)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="success", http_status=response.status_code, latency_ms=latency_ms,
        )
        log.info("delivery.success", http_status=response.status_code)
        return

    # Some 4xx codes are actually transient — treat them like 5xx
    if 400 <= response.status_code < 500 and response.status_code not in _RETRYABLE_4XX:
        # Real client error — retrying the same payload won't help
        reason = f"HTTP {response.status_code}: {response.text[:300]}"
        await broker.dead_letter(msg.id, reason)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", http_status=response.status_code,
            latency_ms=latency_ms, error_message=reason,
        )
        log.error("delivery.dead_lettered_4xx", http_status=response.status_code, reason=reason)
        return

    # All retryable cases: 5xx, 408, 425, 429.
    # If Azure told us how long to wait, honor that; otherwise use our backoff.
    retry_after = parse_retry_after(response)
    backoff = retry_after if retry_after is not None else compute_backoff(msg.attempt_count)
    reason = f"HTTP {response.status_code}: {response.text[:300]}"
    await broker.nack(msg.id, reason, backoff)
    await _write_delivery_log(
        pool, msg.id, msg.correlation_id, msg.attempt_count,
        status="failure", http_status=response.status_code,
        latency_ms=latency_ms, error_message=reason,
    )
    log.warning(
        "delivery.retry_scheduled",
        http_status=response.status_code,
        backoff_seconds=backoff,
        honored_retry_after=retry_after is not None,
    )


# ── Poll loop ──────────────────────────────────────────────────────────────────


async def run_worker(broker: PostgresBroker, http_client: httpx.AsyncClient, pool) -> None:
    """
    Main poll loop. Runs forever until a shutdown signal is received.

    WHY clear contextvars at the top of each iteration:
    structlog binds correlation_id/event_key per message. Without clearing,
    the previous message's context lingers across the empty-queue sleep and
    is attached to any unrelated error log — pointing investigators at the
    wrong event. One clear per iteration costs nothing and prevents this.

    WHY sleep on empty queue instead of tight-looping:
    A tight loop would hit the DB thousands of times per second when the queue
    is empty, burning CPU and DB connection quota for nothing. Sleeping
    DELIVERY_POLL_INTERVAL_SECONDS between empty polls keeps resource use flat.
    """
    log.info("delivery_worker.started", poll_interval=settings.DELIVERY_POLL_INTERVAL_SECONDS)

    while True:
        structlog.contextvars.clear_contextvars()
        try:
            msg = await broker.claim_next()
            if msg is None:
                await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)
                continue
            await process_message(broker, http_client, msg, pool)

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
            await run_worker(broker, http_client, pool)
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
