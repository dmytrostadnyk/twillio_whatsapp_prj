"""
Status callback handlers — SMS, WhatsApp, and Voice.

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
"""

from __future__ import annotations

import uuid
from typing import Annotated

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import Response

from comm_layer.broker.base import Broker
from comm_layer.deps import get_broker, get_pool
from comm_layer.number_registry import resolve_source
from comm_layer.twilio_security import require_twilio_signature
from comm_layer.webhooks.ingest import ingest_event
from comm_layer.webhooks.responses import EMPTY_TWIML

log = structlog.get_logger(__name__)
router = APIRouter()


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
