"""
SMS inbound webhook handler.

Twilio calls POST /webhooks/sms when someone sends an SMS to our number.
This handler must respond in < 1 second. It does exactly four things:
  1. Validate the Twilio signature (handled by the dependency)
  2. Resolve the destination number to a business source
  3. Persist the event (idempotent — duplicates are silently dropped)
  4. Return empty TwiML so Twilio knows we received it

No AI. No Azure. No business logic. Just durable capture and fast ACK.
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

# Empty TwiML response — tells Twilio "received, no auto-reply"
_EMPTY_TWIML = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'


@router.post("/sms", response_class=Response)
async def receive_sms(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    Inbound SMS webhook.

    Twilio form fields we use:
    - MessageSid: unique ID for this message (our idempotency key)
    - From:       sender's phone number
    - To:         our Twilio number (used to resolve the business source)
    - Body:       message text
    - NumMedia:   number of attachments (0 for plain SMS)
    """
    message_sid = form.get("MessageSid") or form.get("SmsSid", "")
    from_number = form.get("From", "")
    to_number = form.get("To", "")

    if not message_sid:
        # Malformed payload — Twilio always sends MessageSid, so this
        # indicates a fake or malformed request that passed signature check.
        log.warning("sms.missing_message_sid", form_keys=list(form.keys()))
        return Response(content=_EMPTY_TWIML, media_type="application/xml")

    source = await resolve_source(pool, to_number)
    correlation_id = uuid.uuid4()

    await ingest_event(
        pool,
        broker,
        event_key=f"{message_sid}:sms.received",
        channel="sms",
        direction="inbound",
        event_type="sms.received",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=correlation_id,
    )

    return Response(content=_EMPTY_TWIML, media_type="application/xml")
