"""
Status callback handlers — SMS, WhatsApp, Voice, and Voice Recording.

Twilio fires these when the status of an outbound message or a call changes.
Status callbacks follow the same fast-ack pattern as inbound webhooks:
validate → persist → return 200. Never block on Azure or AI.

IMPORTANT: Status callbacks can arrive OUT OF ORDER.
For example, you might receive 'delivered' before 'sent' for the same message.
This is normal Twilio behaviour. We capture every status transition as its own
event with a unique event_key so the consumer can reconcile them.

The event_key format for status events uses the SID + the specific status,
not just the SID, so each transition is a separate idempotent record.
e.g. SM123:sms.status.delivered, SM123:sms.status.sent

DIRECTION HANDLING for voice status:
The voice status callback fires for BOTH inbound and outbound calls. We read
the Direction field Twilio sends ('inbound', 'outbound-api', 'outbound-dial')
rather than hardcoding 'inbound'. For inbound, the registry lookup uses
to_number (our Twilio number). For outbound, it uses from_number.

SMS and WhatsApp status callbacks fire only for outbound messages, so the
'from' field is always our number and direction is always 'outbound'.

RECORDING-READY CALLBACK:
The recording callback fires ASYNCHRONOUSLY after the call ends — Twilio takes
a few seconds to encode the audio. It is a separate HTTP POST from the call
status callback and arrives at /webhooks/voice/recording.
"""

from __future__ import annotations

import uuid
from typing import Annotated

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import Response

from comm_layer.broker.base import Broker
from comm_layer.contracts.base import EventSource
from comm_layer.deps import get_broker, get_pool
from comm_layer.number_registry import resolve_source
from comm_layer.twilio_security import require_twilio_signature
from comm_layer.webhooks.ingest import ingest_event
from comm_layer.webhooks.responses import EMPTY_TWIML

log = structlog.get_logger(__name__)
router = APIRouter()


# ── Helpers ────────────────────────────────────────────────────────────────────


async def lookup_call_context(
    pool: asyncpg.Pool, call_sid: str
) -> dict | None:
    """
    Fetch the originating call.started event by CallSid.

    Returns a dict with correlation_id, direction, from_number, to_number,
    and source — or None if no originating call exists (out-of-order webhook,
    or we received a recording for a call we never persisted).

    WHY this exists:
    Twilio's recording callback doesn't include From/To or any stable identifier
    that ties it to the original call's correlation_id. Without joining back to
    call.started, every downstream event has a fresh UUID and lacks caller
    attribution — breaking log tracing and forcing analytics to do JSONB
    lookups by CallSid for every report.

    PERFORMANCE NOTE:
    We filter on raw_payload->>'CallSid' which is unindexed. For our expected
    volume (per-call lookup, single row result) this is fine. If recording
    traffic grows, add an expression index in a migration:

        CREATE INDEX comm_events_call_sid_idx
            ON comm_events ((raw_payload->>'CallSid'))
            WHERE event_type = 'call.started';
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT correlation_id, direction, from_number, to_number, source_metadata
            FROM comm_events
            WHERE event_type = 'call.started'
              AND raw_payload->>'CallSid' = $1
            LIMIT 1
            """,
            call_sid,
        )
    if row is None:
        return None
    return {
        "correlation_id": row["correlation_id"],
        "direction": row["direction"],
        "from_number": row["from_number"],
        "to_number": row["to_number"],
        "source": EventSource(**dict(row["source_metadata"])),
    }


@router.post("/sms/status", response_class=Response)
async def sms_status(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    SMS status callback — queued, sent, delivered, failed, undelivered.
    Each status transition is captured as a separate event.
    Fires only for outbound messages, so 'From' is always our Twilio number.
    """
    message_sid = form.get("MessageSid") or form.get("SmsSid", "")
    status = form.get("MessageStatus", "unknown")
    from_number = form.get("From") or None
    to_number = form.get("To") or None

    if not message_sid:
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # Outbound: source is keyed off OUR sending number (from_number).
    source = await resolve_source(pool, from_number or "")

    await ingest_event(
        pool,
        broker,
        # Include the status in the event_key so each transition is unique
        event_key=f"{message_sid}:sms.status.{status}",
        channel="sms",
        direction="outbound",
        event_type="sms.status",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=uuid.uuid4(),
    )

    return Response(content=EMPTY_TWIML, media_type="application/xml")


@router.post("/whatsapp/status", response_class=Response)
async def whatsapp_status(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    WhatsApp status callback — including 'read' receipts (not available for SMS).
    Fires only for outbound messages, so 'From' is always our WhatsApp number.
    """
    message_sid = form.get("MessageSid") or form.get("SmsSid", "")
    status = form.get("MessageStatus", "unknown")
    from_number = form.get("From") or None
    to_number = form.get("To") or None

    if not message_sid:
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    plain_from = (from_number or "").replace("whatsapp:", "")
    source = await resolve_source(pool, plain_from)

    await ingest_event(
        pool,
        broker,
        event_key=f"{message_sid}:whatsapp.status.{status}",
        channel="whatsapp",
        direction="outbound",
        event_type="whatsapp.status",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=uuid.uuid4(),
    )

    return Response(content=EMPTY_TWIML, media_type="application/xml")


@router.post("/voice/status", response_class=Response)
async def voice_status(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    Voice call status callback — fired when a call ends (completed, no-answer, etc.).
    Can fire for BOTH inbound and outbound calls, so we read Direction from the
    payload instead of hardcoding it.

    This is separate from the recording-ready callback (Phase 5).
    """
    call_sid = form.get("CallSid", "")
    from_number = form.get("From") or None
    to_number = form.get("To") or None

    # Twilio's Direction values: 'inbound', 'outbound-api', 'outbound-dial'.
    # We collapse the two outbound variants into one for our enum.
    raw_direction = form.get("Direction", "inbound")
    direction = "outbound" if raw_direction.startswith("outbound") else "inbound"

    if not call_sid:
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # Source resolution is direction-aware:
    #   inbound  → our number is the destination (to_number)
    #   outbound → our number is the originator (from_number)
    lookup_number = from_number if direction == "outbound" else to_number
    source = await resolve_source(pool, lookup_number or "")

    await ingest_event(
        pool,
        broker,
        event_key=f"{call_sid}:call.completed",
        channel="voice",
        direction=direction,
        event_type="call.completed",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=uuid.uuid4(),
    )

    return Response(content=EMPTY_TWIML, media_type="application/xml")


@router.post("/voice/recording", response_class=Response)
async def voice_recording_ready(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    Recording-ready callback — fires when Twilio finishes encoding a recording.

    WHY this is separate from voice_status:
    call.completed fires immediately when the call ends. Recording processing
    happens AFTER the call on Twilio's servers, taking seconds to minutes. This
    separate webhook fires only when the audio file is actually accessible.
    Trying to download from call.completed would fail because the file isn't
    ready yet.

    WHY we inherit context from call.started:
    The recording callback omits From, To, and any stable correlation_id. To
    keep one logical call traceable end-to-end (call.started → call.completed
    → recording.ready), we look up the originating call.started by CallSid and
    inherit its correlation_id, direction, source, From and To. Without this,
    every event in the chain has a fresh UUID and broken caller attribution.

    WHY we ingest 'failed' as its own event:
    completed → audio is ready, transcribe it.
    failed    → Twilio's encoder broke. Audio is gone but we still want a
                durable record so ops can investigate and the dashboard can
                show the loss. Captured as event_type='recording.failed'.
    absent    → caller hung up before speaking. Nothing to record; we skip.

    WHY fallback when lookup_call_context returns None:
    Out-of-order webhooks or a missing call.started row should never cause us
    to drop the recording event. We log a warning and ingest with placeholder
    values so the recording is still durable.

    SECURITY — Secure Media:
    RecordingUrl in raw_payload requires HTTP Basic Auth (AccountSid:AuthToken)
    ONLY IF "Secure Media" is enabled in the Twilio Console (Account Settings
    → Recordings → HTTP authentication for media). Enable before going live.
    Without it, anyone with the URL can download the audio.
    """
    recording_sid = form.get("RecordingSid", "")
    recording_status = form.get("RecordingStatus", "")
    call_sid = form.get("CallSid", "")

    if not recording_sid:
        log.warning("voice_recording.missing_sid", form_keys=list(form.keys()))
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # Pick the event type. Anything other than completed/failed (notably
    # 'absent' — caller hung up before speaking) is operationally a no-op.
    if recording_status == "completed":
        event_type = "recording.ready"
    elif recording_status == "failed":
        event_type = "recording.failed"
    else:
        log.info(
            "voice_recording.skipped",
            recording_sid=recording_sid,
            status=recording_status,
        )
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # Inherit attribution + correlation_id from the originating call so log
    # tracing, analytics, and the intelligence layer can join across the chain.
    call_ctx = await lookup_call_context(pool, call_sid) if call_sid else None
    if call_ctx is None:
        # Never drop the event — log loudly and fall back to placeholder values.
        log.warning(
            "voice_recording.originating_call_not_found",
            recording_sid=recording_sid,
            call_sid=call_sid,
        )
        from_number = None
        to_number = None
        # 'inbound' is the safe default for our current architecture (only
        # inbound calls have <Record> wired). Phase 4's outbound calls don't.
        direction = "inbound"
        # Construct unknown source directly — avoids a guaranteed-empty DB query.
        source = EventSource(number="", is_unknown=True)
        correlation_id = uuid.uuid4()
    else:
        from_number = call_ctx["from_number"]
        to_number = call_ctx["to_number"]
        direction = call_ctx["direction"]
        source = call_ctx["source"]
        correlation_id = call_ctx["correlation_id"]

    await ingest_event(
        pool,
        broker,
        event_key=f"{recording_sid}:{event_type}",
        channel="voice",
        direction=direction,
        event_type=event_type,
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=correlation_id,
    )

    return Response(content=EMPTY_TWIML, media_type="application/xml")
