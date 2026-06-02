"""
Delivery worker — polls the broker queue and ships enriched events to HubSpot.

Run with:
    python delivery_worker/main.py

Or via Makefile:
    make worker

This is a long-running process, separate from the FastAPI webhook server.
It has exactly one job: claim pending events (gated on enrichment completion)
and create or update HubSpot contacts with the AI-generated insights.

Reliability model:
  - Claim gating: an event stays invisible until its enrichment row reaches a
    terminal status (completed / failed / skipped). This ensures every HubSpot
    contact update always carries a GPT-4o summary, intent, and sentiment.
  - Crash recovery: if the worker dies mid-processing, the lease on the claimed
    row expires (DELIVERY_LEASE_SECONDS) and another worker instance re-claims it.
  - 401/403 from HubSpot → dead-letter immediately (auth/scope error; retrying
    the same credentials will fail the same way).
  - Other 4xx (validation) → dead-letter immediately.
  - 429 (rate limit) → nack with HubSpot's Retry-After value honored.
  - 5xx, timeout, or network error → retry with exponential backoff + jitter.
  - Max attempts: after DELIVERY_MAX_ATTEMPTS the event moves to 'dead'.
    Use scripts/replay_dlq.py to re-queue after fixing the root cause.
  - Idempotency: hubspot_contact_id is persisted to the DB immediately after
    contact creation. On retry, the worker skips find_or_create and goes
    straight to the PATCH — no duplicate contacts on retry.
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
from delivery_worker.hubspot_client import (
    ensure_custom_properties,
    find_or_create_contact,
    update_contact,
)
from delivery_worker.transform import build_hubspot_properties

log = structlog.get_logger(__name__)

# 4xx codes that are transient and SHOULD be retried (not dead-lettered).
#   429 Too Many Requests — rate limit, honor Retry-After
#   408 Request Timeout   — server gave up waiting
#   425 Too Early         — server unwilling to process replay-able request
_RETRYABLE_4XX = frozenset({408, 425, 429})

# 4xx codes that mean our credentials are bad — never worth retrying.
_AUTH_ERROR_CODES = frozenset({401, 403})


# ── Backoff ────────────────────────────────────────────────────────────────────


def compute_backoff(attempt: int) -> float:
    """
    Exponential backoff with full jitter.

    WHY full jitter (random between 0 and cap) instead of plain exponential:
    If HubSpot goes down and all workers wake up at the same time after a fixed
    delay, they create a retry thunderstorm. Spreading retries randomly across
    the window avoids this.

    Formula: cap = min(base * 2^attempt, max); jitter = random(0, cap)
    """
    cap = min(
        settings.DELIVERY_BACKOFF_BASE_SECONDS * (2 ** attempt),
        settings.DELIVERY_BACKOFF_MAX_SECONDS,
    )
    return random.uniform(0, cap)


def parse_retry_after(response: httpx.Response) -> float | None:
    """
    Parse the Retry-After header value in seconds.

    Returns the delay, or None if the header is missing or unparseable.
    Negative values are clamped to 0 (never retry in the past).
    """
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
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


async def _persist_contact_id(pool, event_id, contact_id: str) -> None:
    """
    Write the HubSpot contact ID to comm_events immediately after creation.

    WHY immediately (before the PATCH): if the worker crashes between contact
    creation and the contact update, the retry will re-use the existing contact
    instead of creating a duplicate. This is the idempotency guarantee.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE comm_events SET hubspot_contact_id = $1 WHERE id = $2",
            contact_id,
            event_id,
        )


async def _handle_http_error(
    broker: PostgresBroker,
    msg: BrokerMessage,
    pool,
    response: httpx.Response,
    latency_ms: int,
) -> None:
    """Map an HTTP error response to the correct broker action (nack or dead-letter)."""
    status = response.status_code
    reason = f"HTTP {status}: {response.text[:300]}"

    if status in _AUTH_ERROR_CODES:
        # Auth errors never recover without a token/scope fix. Dead-letter and
        # wait for a human to fix the token and replay from the DLQ.
        await broker.dead_letter(msg.id, reason)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", http_status=status,
            latency_ms=latency_ms, error_message=reason,
        )
        log.error("delivery.dead_lettered_auth_error", http_status=status)

    elif 400 <= status < 500 and status not in _RETRYABLE_4XX:
        # Non-transient 4xx — the payload or endpoint is wrong, retrying won't help.
        await broker.dead_letter(msg.id, reason)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", http_status=status,
            latency_ms=latency_ms, error_message=reason,
        )
        log.error("delivery.dead_lettered_4xx", http_status=status)

    else:
        # 5xx, 429, 408, 425 — all transient; retry with backoff.
        retry_after = parse_retry_after(response)
        backoff = retry_after if retry_after is not None else compute_backoff(msg.attempt_count)
        await broker.nack(msg.id, reason, backoff)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", http_status=status,
            latency_ms=latency_ms, error_message=reason,
        )
        log.warning(
            "delivery.retry_scheduled",
            http_status=status,
            backoff_seconds=backoff,
            honored_retry_after=retry_after is not None,
        )


# ── Per-message processing ─────────────────────────────────────────────────────


async def process_message(
    broker: PostgresBroker,
    http_client: httpx.AsyncClient,
    msg: BrokerMessage,
    pool,
) -> None:
    """
    Deliver one enriched event to HubSpot. Two-step flow:

      Phase 1 — Resolve contact:
        If hubspot_contact_id is already set (from a prior partial attempt),
        skip straight to Phase 2. Otherwise, search HubSpot by phone number
        and create a contact if none is found. Persist the contact ID to the DB
        immediately so retries never create duplicate contacts.

      Phase 2 — Update contact:
        PATCH the contact with AI-generated properties (intent, sentiment,
        summary) plus a prepended log entry that builds the conversation history.

    Outcome mapping:
      2xx           → ack
      401 / 403     → dead-letter (fix token, replay from DLQ)
      Other 4xx     → dead-letter (bad payload — retrying won't help)
      429 / 5xx     → nack (retry; Retry-After honored if present)
      Network error → nack (retry with backoff)
      Max attempts  → dead-letter before any HTTP call
    """
    structlog.contextvars.bind_contextvars(
        event_key=msg.event_key,
        correlation_id=str(msg.correlation_id),
        attempt=msg.attempt_count,
    )

    # Guard: give up before even calling HubSpot if retries are exhausted.
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

    t0 = time.monotonic()

    # ── Phase 1: Resolve HubSpot contact ──────────────────────────────────────

    contact_id = msg.hubspot_contact_id
    existing_log = ""

    if not contact_id:
        if not msg.from_number:
            # We cannot create a contact without a phone number. This should
            # never happen for the three deliverable event types (sms.received,
            # whatsapp.received, recording.ready), but guard defensively.
            reason = "Cannot create HubSpot contact: from_number is null"
            await broker.dead_letter(msg.id, reason)
            await _write_delivery_log(
                pool, msg.id, msg.correlation_id, msg.attempt_count,
                status="failure", error_message=reason,
            )
            log.error("delivery.dead_lettered_no_phone", event_key=msg.event_key)
            return

        try:
            contact_id, existing_log = await find_or_create_contact(
                http_client,
                settings.HUBSPOT_PRIVATE_APP_TOKEN,
                settings.HUBSPOT_BASE_URL,
                msg.from_number,
            )
        except httpx.HTTPStatusError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            await _handle_http_error(broker, msg, pool, exc.response, latency_ms)
            return
        except httpx.RequestError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            backoff = compute_backoff(msg.attempt_count)
            error_type = type(exc).__name__
            reason = f"{error_type}: {exc}"
            await broker.nack(msg.id, reason, backoff)
            await _write_delivery_log(
                pool, msg.id, msg.correlation_id, msg.attempt_count,
                status="failure", latency_ms=latency_ms, error_message=reason,
            )
            log.warning(
                "delivery.network_error_retry",
                error_type=error_type,
                backoff_seconds=backoff,
            )
            return

        # Persist the contact ID immediately so retries skip creation.
        await _persist_contact_id(pool, msg.id, contact_id)

    # ── Phase 2: Update contact with AI properties ─────────────────────────────

    properties = build_hubspot_properties(msg, existing_log)

    try:
        response = await update_contact(
            http_client,
            settings.HUBSPOT_PRIVATE_APP_TOKEN,
            settings.HUBSPOT_BASE_URL,
            contact_id,
            properties,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        await _handle_http_error(broker, msg, pool, exc.response, latency_ms)
        return
    except httpx.RequestError as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        backoff = compute_backoff(msg.attempt_count)
        error_type = type(exc).__name__
        reason = f"{error_type}: {exc}"
        await broker.nack(msg.id, reason, backoff)
        await _write_delivery_log(
            pool, msg.id, msg.correlation_id, msg.attempt_count,
            status="failure", latency_ms=latency_ms, error_message=reason,
        )
        log.warning("delivery.network_error_retry", error_type=error_type, backoff_seconds=backoff)
        return

    # ── Success ────────────────────────────────────────────────────────────────

    latency_ms = int((time.monotonic() - t0) * 1000)
    ack_payload = {
        "hubspot_contact_id": contact_id,
        "properties_updated": list(properties.keys()),
        "channel": msg.channel,
        "event_type": msg.event_type,
    }
    await broker.ack(msg.id, contract_payload=ack_payload)
    await _write_delivery_log(
        pool, msg.id, msg.correlation_id, msg.attempt_count,
        status="success", http_status=response.status_code, latency_ms=latency_ms,
    )
    log.info(
        "delivery.success",
        contact_id=contact_id,
        channel=msg.channel,
        http_status=response.status_code,
    )


# ── Poll loop ──────────────────────────────────────────────────────────────────


async def run_worker(broker: PostgresBroker, http_client: httpx.AsyncClient, pool) -> None:
    """
    Main poll loop. Runs forever until a shutdown signal is received.

    WHY clear contextvars at the top of each iteration:
    structlog binds correlation_id/event_key per message. Without clearing,
    the previous message's context lingers across the empty-queue sleep.
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
            log.info("delivery_worker.stopping")
            raise

        except Exception as exc:
            log.exception("delivery_worker.unexpected_error", error=str(exc))
            await asyncio.sleep(settings.DELIVERY_POLL_INTERVAL_SECONDS)


# ── Entry point ────────────────────────────────────────────────────────────────


async def main() -> None:
    """Start up the worker: create pool, ensure HubSpot properties, run loop."""
    configure_logging(settings.LOG_LEVEL)
    log.info("delivery_worker.initialising")

    pool = await create_pool()
    broker = PostgresBroker(pool=pool)

    async with httpx.AsyncClient() as http_client:
        # Create AI property group + custom fields in HubSpot once at startup.
        # Safe to re-run — 409 Conflict is silently ignored.
        await ensure_custom_properties(
            http_client,
            settings.HUBSPOT_PRIVATE_APP_TOKEN,
            settings.HUBSPOT_BASE_URL,
        )
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
