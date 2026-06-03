"""
Broker interface and implementations.

The Broker is the queue abstraction that decouples event producers (the webhook handlers)
from event consumers (the delivery worker, the intelligence layer).

Why an interface (abstract base class)?
In production, you can swap PostgresBroker for AzureServiceBusBroker by changing a
single environment variable — zero application code changes (see azure_servicebus.py).

Usage:
    from comm_layer.broker import get_broker
    broker = await get_broker()
    await broker.publish(event_id, payload)
"""

from comm_layer.broker.base import Broker, BrokerMessage
from comm_layer.broker.postgres import PostgresBroker

__all__ = ["Broker", "BrokerMessage", "PostgresBroker"]
