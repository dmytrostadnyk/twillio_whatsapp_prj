"""
SMS event contract models.

Two events:
1. SmsReceivedEvent — an inbound SMS from a user.
2. SmsStatusEvent   — a status callback for an outbound SMS we sent
                       (queued → sent → delivered → failed → undelivered).
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from comm_layer.contracts.base import BaseCommEvent, Channel, Direction


class SmsReceivedEvent(BaseCommEvent):
    """Emitted when Twilio delivers an inbound SMS to our webhook."""

    channel: Channel = Channel.SMS
    direction: Direction = Direction.INBOUND
    event_type: Literal["sms.received"] = "sms.received"

    message_sid: str = Field(..., description="Twilio Message SID (starts with SM)")
    from_number: str = Field(..., description="Sender's phone number (E.164)")
    to_number: str = Field(..., description="Our Twilio number (E.164)")
    body: str = Field(..., description="Message body text")
    num_media: int = Field(0, description="Number of media attachments (MMS)")
    media_urls: list[str] = Field(
        default_factory=list,
        description="List of media URLs if this is an MMS. May be empty.",
    )

    model_config = {"extra": "forbid", "use_enum_values": True}


class SmsStatusEvent(BaseCommEvent):
    """
    Emitted when Twilio sends a status callback for an outbound SMS.

    Status progression: queued → sent → delivered → (failed | undelivered)
    Each transition fires a separate webhook, and they can arrive out of order.
    Always reconcile by message_sid, never assume ordering.
    """

    channel: Channel = Channel.SMS
    direction: Direction = Direction.OUTBOUND
    event_type: Literal["sms.status"] = "sms.status"

    message_sid: str = Field(..., description="Twilio Message SID")
    from_number: str = Field(..., description="Our Twilio number that sent the message")
    to_number: str = Field(..., description="Recipient's phone number")
    message_status: str = Field(
        ...,
        description=(
            "Twilio message status: 'queued' | 'sent' | 'delivered' | 'undelivered' | 'failed'"
        ),
    )
    error_code: str | None = Field(
        None, description="Twilio error code if status is failed/undelivered"
    )
    error_message: str | None = Field(None, description="Human-readable error if applicable")

    model_config = {"extra": "forbid", "use_enum_values": True}
