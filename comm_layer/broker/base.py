"""
Broker abstract base class.

WHY an ABC (abstract base class): every method here represents a contract that
both PostgresBroker and AzureServiceBusBroker must honour. The delivery worker
and intelligence layer depend only on this interface — they never import a
concrete implementation directly. This is what makes the Azure swap config-only.

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
    """

    id: uuid.UUID          # the comm_events.id for this event
    event_key: str         # natural idempotency key
    correlation_id: uuid.UUID
    payload: dict[str, Any]  # the raw_payload from comm_events
    attempt_count: int
    claimed_at: datetime


class Broker(ABC):
    """
    Abstract broker interface.

    All methods are async because both PostgresBroker (asyncpg) and a real
    Azure Service Bus client use async I/O.
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
        to guarantee this. The Azure implementation uses message locking.
        """
        ...

    @abstractmethod
    async def ack(self, event_id: uuid.UUID) -> None:
        """Mark an event as successfully delivered. Removes it from the work queue."""
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
