"""
AzureServiceBusBroker — stub implementation.

WHY this stub exists:
In production against a real Azure backend, you would replace PostgresBroker with
this class by changing a single environment variable (BROKER_BACKEND=azure).
Zero application code changes are required — the delivery worker, intelligence layer,
and all other consumers depend only on the Broker interface, not on PostgresBroker.

This is the architectural promise documented in ARCHITECTURE.md.

HOW to implement (when the time comes):
1. Install: pip install azure-servicebus
2. Create an Azure Service Bus namespace and queue in the Azure portal.
3. Set AZURE_SERVICEBUS_CONNECTION_STRING in your environment.
4. Implement each method below using the azure-servicebus async client:
   - publish:    send a message to the queue with event_id as the message body
   - claim_next: receive a message with peek_lock=True (gives you a lock token)
   - ack:        complete the message using the lock token
   - nack:       abandon the message (Service Bus will redeliver after lock timeout)
   - dead_letter: dead_letter the message using the lock token

The message locking pattern in Azure Service Bus provides the same SKIP LOCKED
guarantee that Postgres gives us — only one consumer holds a message at a time.

For reference, see:
https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-python-how-to-use-queues
"""

from __future__ import annotations

import uuid

from comm_layer.broker.base import Broker, BrokerMessage


class AzureServiceBusBroker(Broker):
    """
    Azure Service Bus implementation of the Broker interface.
    Not yet implemented — swap by setting BROKER_BACKEND=azure in your environment.
    """

    def __init__(self, connection_string: str, queue_name: str) -> None:
        # TODO: initialise azure.servicebus.aio.ServiceBusClient here
        raise NotImplementedError(
            "AzureServiceBusBroker is not yet implemented. "
            "See the docstring in this file for implementation guidance."
        )

    async def publish(self, event_id: uuid.UUID) -> None:
        raise NotImplementedError

    async def claim_next(self) -> BrokerMessage | None:
        raise NotImplementedError

    async def ack(self, event_id: uuid.UUID) -> None:
        raise NotImplementedError

    async def nack(self, event_id: uuid.UUID, error: str, retry_after_seconds: float) -> None:
        raise NotImplementedError

    async def dead_letter(self, event_id: uuid.UUID, reason: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
