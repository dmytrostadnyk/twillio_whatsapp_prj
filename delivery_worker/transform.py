"""
Contract payload builder for the delivery worker.

WHY a separate module:
Building the versioned contract is the only piece of logic in the delivery
worker that isn't about queue mechanics. Keeping it separate makes it easy
to test in isolation and easy to extend when new event types are added.

The contract format is documented in CONTRACT.md. The shape here must match
exactly — if you change the structure, bump schema_version and update CONTRACT.md.
"""

from __future__ import annotations

from typing import Any

from comm_layer.broker.base import BrokerMessage


def build_contract_payload(msg: BrokerMessage) -> dict[str, Any]:
    """
    Transform a claimed BrokerMessage into the versioned contract JSON
    that will be POSTed to the Azure CRM endpoint.

    The contract separates normalised metadata (channel, direction, source)
    from the raw Twilio payload so consumers don't have to parse Twilio-specific
    fields themselves. Both are always present so consumers can choose.

    Returns a plain dict — the caller (worker) serialises it to JSON when
    building the HTTP request.
    """
    return {
        "schema_version": "1.0",
        "event_key": msg.event_key,
        "correlation_id": str(msg.correlation_id),
        "channel": msg.channel,
        "direction": msg.direction,
        "event_type": msg.event_type,
        # ISO-8601 UTC timestamp of when the event was first received
        "timestamp": msg.created_at.isoformat(),
        # Resolved at ingestion time from number_registry
        "source": msg.source_metadata,
        "data": {
            # Normalised numbers from the comm_events columns (may be None for
            # some status callbacks that don't carry From/To).
            "from_number": msg.from_number,
            "to_number": msg.to_number,
            # Original Twilio form payload — consumers can extract any field
            # not explicitly normalised above (e.g. Body, NumMedia, CallStatus).
            "raw": msg.raw_payload,
        },
    }
