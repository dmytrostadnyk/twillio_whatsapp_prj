"""
Versioned event contract models.

Every event type the Communication Layer emits is defined here as a Pydantic model.
These models are the single source of truth for what consumers (the Delivery Worker,
the Intelligence Layer, the mock Azure CRM) receive.

Import from here, not from the individual modules:
    from comm_layer.contracts import SmsReceivedEvent, CallStartedEvent
"""

from comm_layer.contracts.base import BaseCommEvent, Channel, Direction, EventSource
from comm_layer.contracts.enriched import EnrichedCommEvent
from comm_layer.contracts.sms import SmsReceivedEvent, SmsStatusEvent
from comm_layer.contracts.voice import CallCompletedEvent, CallStartedEvent, RecordingReadyEvent
from comm_layer.contracts.whatsapp import WhatsAppReceivedEvent, WhatsAppStatusEvent

__all__ = [
    "BaseCommEvent",
    "Channel",
    "Direction",
    "EventSource",
    "CallStartedEvent",
    "CallCompletedEvent",
    "RecordingReadyEvent",
    "SmsReceivedEvent",
    "SmsStatusEvent",
    "WhatsAppReceivedEvent",
    "WhatsAppStatusEvent",
    "EnrichedCommEvent",
]
