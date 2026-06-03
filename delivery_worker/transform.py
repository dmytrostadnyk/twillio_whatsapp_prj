"""
HubSpot contact property builder for the delivery worker.

WHY a separate module:
Building the set of properties to write is the only pure-logic piece in the
delivery worker. Keeping it separate makes it easy to test in isolation without
mocking any HTTP or DB calls.
"""

from __future__ import annotations

from comm_layer.broker.base import BrokerMessage


def build_hubspot_properties(msg: BrokerMessage, existing_log: str = "") -> dict[str, str]:
    """
    Build the dict of HubSpot contact properties to write on delivery.

    The 'last_' fields always reflect the most recent event — HubSpot's contact
    view shows them prominently and they are filterable (e.g. build a list of
    all contacts whose last sentiment was 'frustrated').

    The 'ai_comm_log' field is a running, prepend-only history of every
    AI-enriched communication. Prepending means the newest entry is always
    at the top of the field when viewed in HubSpot.

    WHY empty string instead of None for missing AI fields:
    HubSpot's PATCH API ignores null values, but some property types reject them.
    Empty string is always safe and renders cleanly in the HubSpot UI.

    Residual race: if two worker processes deliver two events for the same contact
    at exactly the same instant, both may read the same existing_log and one
    prepend can be lost. This is acceptable for single-worker deployments and very
    unlikely in practice. The 'ai_last_*' fields are always idempotent (same value
    overwritten on retry).
    """
    timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M UTC")
    # e.g. "Inbound SMS", "Inbound Voice", "Inbound WhatsApp"
    channel_label = f"Inbound {msg.channel.title()}"

    # Build the one-event block that will be prepended to the log.
    lines = [f"[{timestamp} · {channel_label}]"]
    if msg.summary:
        lines.append(f"Summary: {msg.summary}")
    if msg.intent:
        lines.append(f"Intent: {msg.intent}")
    if msg.sentiment:
        lines.append(f"Sentiment: {msg.sentiment}")
    if not (msg.summary or msg.intent or msg.sentiment):
        lines.append("(AI enrichment unavailable for this event)")

    new_entry = "\n".join(lines)
    separator = "\n───\n"  # ─── visual divider between entries
    updated_log = new_entry + (separator + existing_log if existing_log else "")

    return {
        "ai_last_intent": msg.intent or "",
        "ai_last_sentiment": msg.sentiment or "",
        "ai_last_summary": msg.summary or "",
        "ai_comm_log": updated_log,
    }
