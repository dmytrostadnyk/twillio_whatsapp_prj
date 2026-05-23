"""
WhatsApp event contract models.

Two events:
1. WhatsAppReceivedEvent — an inbound WhatsApp message from a user.
2. WhatsAppStatusEvent   — status callbacks for outbound WhatsApp messages.

WHY WhatsApp is separate from SMS even though Twilio uses the same API:
- WhatsApp has a 24-hour messaging window restriction.
- Template messages are required outside that window (and must be pre-approved by Meta).
- Read receipts work differently.
Separating the types lets consumers apply WhatsApp-specific business logic without
checking the channel on every SMS event.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from comm_layer.contracts.base import BaseCommEvent, Channel, Direction


class WhatsAppReceivedEvent(BaseCommEvent):
    """Emitted when Twilio delivers an inbound WhatsApp message."""

    channel: Channel = Channel.WHATSAPP
    direction: Direction = Direction.INBOUND
    event_type: Literal["whatsapp.received"] = "whatsapp.received"

    message_sid: str = Field(..., description="Twilio Message SID (starts with SM)")
    # WhatsApp numbers have the 'whatsapp:' prefix in Twilio payloads
    from_number: str = Field(
        ..., description="Sender's WhatsApp number e.g. 'whatsapp:+15551234567'"
    )
    to_number: str = Field(..., description="Our WhatsApp number e.g. 'whatsapp:+14155238886'")
    body: str = Field(..., description="Message body text")
    profile_name: str | None = Field(None, description="Sender's WhatsApp display name")
    num_media: int = Field(0, description="Number of media attachments")
    media_urls: list[str] = Field(default_factory=list, description="Media attachment URLs")

    model_config = {"extra": "forbid", "use_enum_values": True}


class WhatsAppStatusEvent(BaseCommEvent):
    """
    Emitted on status callbacks for outbound WhatsApp messages.

    Status includes read receipts ('read') which SMS does not have.
    Progression: queued → sent → delivered → read (or failed).
    """

    channel: Channel = Channel.WHATSAPP
    direction: Direction = Direction.OUTBOUND
    event_type: Literal["whatsapp.status"] = "whatsapp.status"

    message_sid: str = Field(..., description="Twilio Message SID")
    from_number: str = Field(..., description="Our WhatsApp number")
    to_number: str = Field(..., description="Recipient's WhatsApp number")
    message_status: str = Field(
        ...,
        description=(
            "Status: 'queued' | 'sent' | 'delivered' | 'read' | 'failed' | 'undelivered'"
        ),
    )
    # Whether this message used a pre-approved template (required outside the 24h window)
    is_template: bool = Field(
        False,
        description="True if this was sent as a WhatsApp template message",
    )
    error_code: str | None = Field(None, description="Twilio error code if applicable")
    error_message: str | None = Field(None, description="Human-readable error if applicable")

    model_config = {"extra": "forbid", "use_enum_values": True}
