"""
Base contract model shared by every event type.

WHY: Every event the Communication Layer emits carries a fixed set of fields
that consumers rely on for routing, deduplication, and tracing. By putting
them in a base class, we guarantee no event type can accidentally omit them.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Channel(StrEnum):
    """The Twilio channel that originated the event."""

    VOICE = "voice"
    SMS = "sms"
    WHATSAPP = "whatsapp"


class Direction(StrEnum):
    """Whether the event was triggered by an inbound or outbound communication."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"


class EventSource(BaseModel):
    """
    Resolved source metadata from the number registry.
    Tells consumers which campaign/affiliate/business unit owns this number.
    """

    number: str = Field(..., description="E.164 phone number e.g. +15551234567")
    source_type: str | None = Field(
        None, description="'affiliate' | 'campaign' | 'business_unit' | None if unknown"
    )
    source_id: str | None = Field(None, description="Internal source identifier")
    label: str | None = Field(None, description="Human-readable label")
    is_unknown: bool = Field(
        False, description="True when the number is not in the registry"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional registry attributes"
    )


class BaseCommEvent(BaseModel):
    """
    Base class for every versioned event emitted by the Communication Layer.

    Fields every consumer will receive regardless of event type:
    - schema_version: bump this when the contract changes (consumers must handle old versions)
    - event_key:      unique natural key; consumers use it to deduplicate
    - correlation_id: UUID that traces this communication end-to-end through every system
    - channel:        voice | sms | whatsapp
    - direction:      inbound | outbound
    - timestamp:      when the event occurred (UTC, ISO-8601)
    - source:         resolved number registry metadata
    """

    schema_version: str = Field(
        "1.0",
        description="Contract version. Consumers must check this before processing.",
    )
    event_key: str = Field(
        ...,
        description="Natural idempotency key in format '{TwilioSid}:{event_type}'",
    )
    correlation_id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        description="UUID threaded through every log line, DB row, and outbound payload",
    )
    channel: Channel
    direction: Direction
    event_type: str = Field(..., description="Machine-readable event name e.g. 'sms.received'")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the event occurred (UTC)",
    )
    source: EventSource = Field(
        ..., description="Resolved source metadata from the number registry"
    )

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, v: Any) -> datetime:
        """Coerce naive datetimes to UTC so all timestamps are comparable."""
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v

    model_config = {
        # Pydantic v2: reject extra fields so typos in consumer code surface immediately
        "extra": "forbid",
        # Make enums serialize as their string value (not the enum object)
        "use_enum_values": True,
    }
