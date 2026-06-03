"""
SMS inbound webhook handler.

Twilio calls POST /webhooks/sms when someone sends an SMS to our number.
This handler must respond in < 1 second. It does exactly four things:
  1. Validate the Twilio signature (handled by the dependency)
  2. Resolve the destination number to a business source
  3. Persist the event (idempotent — duplicates are silently dropped)
  4. Return empty TwiML so Twilio knows we received it

No AI. No CRM. No business logic. Just durable capture and fast ACK.
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
    # Coerce empty strings to None so the DB stores NULL — analytics queries
    # using `WHERE from_number IS NOT NULL` must not match these rows.
    from_number = form.get("From") or None
    to_number = form.get("To") or None

    if not message_sid:
        # Malformed payload — Twilio always sends MessageSid, so this
        # indicates a fake or malformed request that passed signature check.
        log.warning("sms.missing_message_sid", form_keys=list(form.keys()))
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # For inbound SMS the destination (to_number) is our Twilio number — that's
    # what we look up in the registry to know which business owns this traffic.
    source = await resolve_source(pool, to_number or "")
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

    return Response(content=EMPTY_TWIML, media_type="application/xml")
