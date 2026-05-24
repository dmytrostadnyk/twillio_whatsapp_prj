"""
Voice inbound webhook handler.

Twilio calls POST /webhooks/voice when someone calls our number.
We respond immediately with TwiML (a greeting) so the caller hears something
while we persist the event in the background.

Phase 5 will add recording — this handler will be updated then.
Phase 6 will add real-time Media Streams transcription.
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

log = structlog.get_logger(__name__)
router = APIRouter()

# TwiML response for inbound calls — a simple greeting while we capture the event.
# Phase 5 will replace this with a <Record> verb.
_VOICE_TWIML = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="alice">Thank you for calling. Please hold while we connect you.</Say>
</Response>"""


@router.post("/voice", response_class=Response)
async def receive_call(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    Inbound voice call webhook — fired when a call first connects.

    Twilio form fields we use:
    - CallSid:    unique ID for this call
    - From:       caller's number
    - To:         our Twilio number
    - CallStatus: current status (ringing, in-progress, etc.)
    """
    call_sid = form.get("CallSid", "")
    from_number = form.get("From", "")
    to_number = form.get("To", "")

    if not call_sid:
        log.warning("voice.missing_call_sid", form_keys=list(form.keys()))
        return Response(content=_VOICE_TWIML, media_type="application/xml")

    source = await resolve_source(pool, to_number)
    correlation_id = uuid.uuid4()

    await ingest_event(
        pool,
        broker,
        event_key=f"{call_sid}:call.started",
        channel="voice",
        direction="inbound",
        event_type="call.started",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=correlation_id,
    )

    return Response(content=_VOICE_TWIML, media_type="application/xml")
