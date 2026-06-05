"""
HubSpot contact property builder for the delivery worker.

WHY a separate module:
Building the set of properties to write is the only pure-logic piece in the
delivery worker. Keeping it separate makes it easy to test in isolation without
mocking any HTTP or DB calls.

Public functions:
  format_event_block       — format one event's AI data as a readable text block
                             (shared between the comm log, Note body, and Ticket content)
  build_hubspot_properties — build the full dict of contact properties to PATCH
  should_create_ticket     — True when the event warrants an auto-created Ticket
"""

from __future__ import annotations

from comm_layer.broker.base import BrokerMessage

# Action item injected when the WhatsApp bot could not answer the customer's question.
# resolved=False is set by whatsapp_reply.py when the bot deferred to the team.
_BOT_UNRESOLVED_ACTION = {
    "description": "Follow up — chatbot could not answer the customer's question",
    "priority": "high",
}


def format_action_items(action_items: list) -> str:
    """
    Format a list of action item dicts as a readable multi-line string.

    Each item is `- [priority] description`. Empty list returns "".
    """
    if not action_items:
        return ""
    lines = []
    for item in action_items:
        if isinstance(item, dict):
            priority = item.get("priority", "")
            desc = item.get("description", "")
        else:
            priority = ""
            desc = str(item)
        lines.append(f"- [{priority}] {desc}" if priority else f"- {desc}")
    return "\n".join(lines)


def _effective_action_items(msg: BrokerMessage) -> list:
    """
    Return the action items to use for this message, injecting the bot-unresolved
    follow-up if the WhatsApp bot could not answer.

    reply_resolved=False means the bot deferred to the team — always inject the
    high-priority follow-up regardless of what the enrichment produced. The
    enrichment analyses the customer's message before the bot replies, so it has
    no visibility into whether the bot actually answered.
    """
    items = list(msg.action_items or [])
    if msg.channel == "whatsapp" and msg.reply_resolved is False:
        # Prepend so it appears first — most urgent item at the top.
        items = [_BOT_UNRESOLVED_ACTION] + items
    return items


def format_event_block(msg: BrokerMessage) -> str:
    """
    Format one event's AI data as a readable text block.

    Used in three places:
    - ai_comm_log entry (prepended on every delivery)
    - HubSpot Note body (timeline entry per event)
    - HubSpot Ticket content (for complaint/negative events)

    Keeping this in one function guarantees the three representations stay in sync.
    """
    timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M UTC")
    channel_label = f"Inbound {msg.channel.title()}"

    lines = [f"[{timestamp} · {channel_label}]"]
    if msg.summary:
        lines.append(f"Summary: {msg.summary}")
    if msg.intent:
        lines.append(f"Intent: {msg.intent}")
    if msg.sentiment:
        lines.append(f"Sentiment: {msg.sentiment}")
    if not (msg.summary or msg.intent or msg.sentiment):
        lines.append("(AI enrichment unavailable for this event)")

    action_items = _effective_action_items(msg)
    formatted = format_action_items(action_items)
    if formatted:
        lines.append(f"Action items:\n{formatted}")

    return "\n".join(lines)


def build_hubspot_properties(msg: BrokerMessage, existing_log: str = "") -> dict[str, str]:
    """
    Build the dict of HubSpot contact properties to write on delivery.

    The 'last_' fields always reflect the most recent event — HubSpot's contact
    view shows them prominently and they are filterable via the enum properties
    (intent, sentiment) which enable native reports, list filters, and workflows.

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
    new_entry = format_event_block(msg)
    separator = "\n───\n"

    # Idempotency guard for the prepend-only log.
    # This function runs on EVERY delivery attempt. If a later phase (Note or
    # Ticket creation) fails, the broker retries the whole event and we land here
    # again for the SAME event. Unlike the other properties (which a PATCH simply
    # overwrites), ai_comm_log is prepend-only — so without this guard each retry
    # would stack another identical copy of the same entry.
    # We compare the current top entry against this event's block and skip the
    # prepend when they already match. Newlines are normalised first because
    # HubSpot can return textarea values with \r\n line endings.
    existing_top = existing_log.replace("\r\n", "\n").split(separator, 1)[0]
    if existing_log and existing_top == new_entry.replace("\r\n", "\n"):
        # We already wrote this entry on a prior attempt — leave the log untouched.
        updated_log = existing_log
    else:
        updated_log = new_entry + (separator + existing_log if existing_log else "")

    action_items = _effective_action_items(msg)
    formatted_actions = format_action_items(action_items)

    return {
        "ai_last_intent": msg.intent or "",
        "ai_last_sentiment": msg.sentiment or "",
        "ai_last_summary": msg.summary or "",
        "ai_last_action_items": formatted_actions,
        "ai_last_channel": msg.channel or "",
        "ai_comm_log": updated_log,
    }


def should_create_ticket(msg: BrokerMessage) -> bool:
    """
    Return True if this event warrants an auto-created HubSpot Ticket.

    Trigger conditions (either is sufficient):
    - intent == 'complaint'
    - sentiment == 'negative'

    WHY either-or: a complaint may be neutrally worded; a message can be
    intensely negative without using the word "complaint". Both signals matter.
    """
    return msg.intent == "complaint" or msg.sentiment == "negative"


def _short_phone(msg: BrokerMessage) -> str:
    """Return last 7 digits of the from_number for privacy-safe display."""
    phone = (msg.from_number or "unknown").replace("whatsapp:", "")
    return phone[-7:] if len(phone) > 7 else phone


def build_ticket_subject(msg: BrokerMessage) -> str:
    """Build a short ticket subject line from the event metadata."""
    channel_label = msg.channel.upper()
    reason = msg.intent if msg.intent == "complaint" else "negative sentiment"
    return f"[{channel_label}] {reason.title()} from …{_short_phone(msg)}"


def should_create_task(msg: BrokerMessage) -> bool:
    """
    Return True if a real HubSpot Task should be created for staff follow-up.

    Triggered only when the WhatsApp bot explicitly could not answer the customer's
    question (reply_resolved=False). SMS and voice events are never gated on a bot
    reply, so reply_resolved stays None for them — this returns False.
    """
    return msg.channel == "whatsapp" and msg.reply_resolved is False


def build_task_subject(msg: BrokerMessage) -> str:
    """Build the Task subject line shown in the rep's to-do queue."""
    return f"Follow up — bot could not answer (WhatsApp …{_short_phone(msg)})"


def build_task_body(msg: BrokerMessage) -> str:
    """Build the Task body using the same event block as Notes and Tickets."""
    return format_event_block(msg)
