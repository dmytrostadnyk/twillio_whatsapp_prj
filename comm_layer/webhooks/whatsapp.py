"""
WhatsApp inbound webhook handler.

Twilio routes WhatsApp messages through the same Messaging API as SMS
but with 'whatsapp:' prefixed on phone numbers (e.g. whatsapp:+15551234567).
We keep WhatsApp as a separate handler because:
- The 24-hour session window logic (Phase 4) is WhatsApp-specific
- Read receipts work differently
- Template message rules differ

For now (Phase 1): capture, persist, ACK. No auto-reply.
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


@router.post("/whatsapp", response_class=Response)
async def receive_whatsapp(
    form: Annotated[dict[str, str], Depends(require_twilio_signature)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    broker: Annotated[Broker, Depends(get_broker)],
) -> Response:
    """
    Inbound WhatsApp message webhook.

    Note: Twilio sends WhatsApp numbers with the 'whatsapp:' prefix.
    We preserve this in the DB so consumers can tell WhatsApp from SMS.
    """
    message_sid = form.get("MessageSid") or form.get("SmsSid", "")
    from_number = form.get("From") or None  # e.g. whatsapp:+15559876543
    to_number = form.get("To") or None      # e.g. whatsapp:+14155238886

    if not message_sid:
        log.warning("whatsapp.missing_message_sid", form_keys=list(form.keys()))
        return Response(content=EMPTY_TWIML, media_type="application/xml")

    # Strip 'whatsapp:' prefix for registry lookup — the registry stores plain E.164
    plain_to = (to_number or "").replace("whatsapp:", "")
    source = await resolve_source(pool, plain_to)

    correlation_id = uuid.uuid4()

    await ingest_event(
        pool,
        broker,
        event_key=f"{message_sid}:whatsapp.received",
        channel="whatsapp",
        direction="inbound",
        event_type="whatsapp.received",
        from_number=from_number,
        to_number=to_number,
        source=source,
        raw_payload=dict(form),
        correlation_id=correlation_id,
    )

    return Response(content=EMPTY_TWIML, media_type="application/xml")
