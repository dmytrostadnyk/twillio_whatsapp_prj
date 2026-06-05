"""
Broker abstract base class.

WHY an ABC (abstract base class): every method here represents a contract that
both PostgresBroker and AzureServiceBusBroker must honour. The delivery worker
and intelligence layer depend only on this interface — they never import a
concrete implementation directly. This is what makes swapping broker backends
a config-only change.

The five operations a broker must support:
- publish:     record that an event is ready to be delivered
- claim_next:  atomically claim one event for processing (no other worker can take it)
- ack:         mark an event as successfully delivered (done)
- nack:        release an event back to the queue after a transient failure
- dead_letter: permanently give up on an event after max attempts
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class BrokerMessage:
    """
    A message claimed from the broker, ready for processing.

    WHY a dataclass: lightweight, no Pydantic overhead, only used internally
    between the broker and the worker — never serialised over the wire.

    All fields needed to build the versioned contract payload are included here
    so the delivery worker never needs a second DB round-trip to fetch metadata.
    """

    id: uuid.UUID
    event_key: str
    correlation_id: uuid.UUID
    channel: str                   # 'sms' | 'voice' | 'whatsapp'
    direction: str                 # 'inbound' | 'outbound'
    event_type: str                # e.g. 'sms.received', 'call.completed'
    from_number: str | None
    to_number: str | None
    source_metadata: dict[str, Any]  # resolved at ingestion time
    raw_payload: dict[str, Any]    # original Twilio form fields
    attempt_count: int
    created_at: datetime
    claimed_at: datetime
    # Enrichment fields — populated from the enrichments table when status is terminal.
    # None means enrichment hasn't completed or AI was disabled for this event.
    summary: str | None = None
    intent: str | None = None
    sentiment: str | None = None
    entities: list | None = None
    action_items: list | None = None
    # Set after a successful HubSpot contact find-or-create so retries skip it.
    hubspot_contact_id: str | None = None
    # Set after a Note is created on the contact timeline — retries skip creation.
    hubspot_note_id: str | None = None
    # Set after a Ticket is created for complaint/negative events — retries skip it.
    hubspot_ticket_id: str | None = None
    # True/False = WhatsApp bot answered / couldn't answer. None = not a WhatsApp event.
    reply_resolved: bool | None = None
    # Set after a real HubSpot Task is created for bot-can't-answer events — retries skip it.
    hubspot_task_id: str | None = None


class Broker(ABC):
    """
    Abstract broker interface.

    All methods are async because both PostgresBroker (asyncpg) and a real
    Azure Service Bus broker client use async I/O.
    """

    @abstractmethod
    async def publish(self, event_id: uuid.UUID) -> None:
        """
        Mark a comm_events row as ready for delivery.

        WHY we pass only the event_id (not the full payload):
        The payload already lives in comm_events. The broker just updates
        delivery_status to 'pending' so the worker picks it up.
        This avoids serialising the payload twice.
        """
        ...

    @abstractmethod
    async def claim_next(self) -> BrokerMessage | None:
        """
        Atomically claim the next pending event for processing.

        Returns None if there are no events ready right now.

        WHY atomic: if two worker processes both poll simultaneously, only one
        must receive each message. PostgresBroker uses SELECT FOR UPDATE SKIP LOCKED
        to guarantee this. An Azure Service Bus implementation uses message locking.
        """
        ...

    @abstractmethod
    async def ack(
        self, event_id: uuid.UUID, contract_payload: dict[str, Any] | None = None
    ) -> None:
        """
        Mark an event as successfully delivered.

        If contract_payload is provided, it is written to comm_events.contract_payload
        so there is an immutable record of exactly what was delivered.
        """
        ...

    @abstractmethod
    async def nack(self, event_id: uuid.UUID, error: str, retry_after_seconds: float) -> None:
        """
        Release an event back to the queue after a transient failure.

        The caller is responsible for computing retry_after_seconds using
        exponential backoff logic — the broker just records when to retry.
        """
        ...

    @abstractmethod
    async def dead_letter(self, event_id: uuid.UUID, reason: str) -> None:
        """
        Permanently give up on an event after max attempts.

        Sets delivery_status = 'dead'. The event stays in the database for
        inspection and can be replayed via the replay command.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release any held connections or resources."""
        ...
