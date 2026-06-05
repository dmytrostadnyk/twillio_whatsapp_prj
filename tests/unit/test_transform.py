"""
Unit tests for delivery_worker/transform.py.

What we test:
1.  build_hubspot_properties includes all six expected keys
2.  ai_comm_log prepends new entry before existing log
3.  ai_comm_log is not duplicated on retry (idempotency guard)
4.  ai_comm_log idempotency guard handles HubSpot CRLF line endings
5.  ai_last_action_items is formatted multi-line text
6.  ai_last_action_items is "" when action_items is empty
7.  ai_last_channel reflects msg.channel
8.  follow-up action item injected when reply_resolved=False (WhatsApp unresolved)
9.  follow-up NOT injected when reply_resolved=True (bot answered)
10. follow-up NOT injected for SMS (channel != whatsapp)
11. follow-up NOT injected when reply_resolved=None (not a WhatsApp event)
12. should_create_ticket: True for complaint intent
13. should_create_ticket: True for negative sentiment
14. should_create_ticket: False for general_query + neutral
15. build_ticket_subject: contains channel + reason fragment
16. format_event_block: contains summary, intent, sentiment
17. format_event_block: shows action items in block
18. format_action_items: empty list → empty string
19. format_action_items: handles non-dict items safely
20. should_create_task: True when whatsapp + reply_resolved=False
21. should_create_task: False when reply_resolved=True
22. should_create_task: False for SMS (not a WhatsApp event)
23. should_create_task: False when reply_resolved=None
24. build_task_subject: contains "Follow up" and phone fragment
25. build_task_body: same content as format_event_block
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from comm_layer.broker.base import BrokerMessage
from delivery_worker.transform import (
    build_hubspot_properties,
    build_task_body,
    build_task_subject,
    build_ticket_subject,
    format_action_items,
    format_event_block,
    should_create_task,
    should_create_ticket,
)

_TS = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def make_msg(
    *,
    channel: str = "sms",
    event_type: str = "sms.received",
    intent: str | None = "general_query",
    sentiment: str | None = "neutral",
    summary: str | None = "Customer asked about hours.",
    action_items: list | None = None,
    reply_resolved: bool | None = None,
    from_number: str | None = "+15559876543",
) -> BrokerMessage:
    return BrokerMessage(
        id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        event_key="SM123:sms.received",
        correlation_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        channel=channel,
        direction="inbound",
        event_type=event_type,
        from_number=from_number,
        to_number="+15551234567",
        source_metadata={},
        raw_payload={"Body": "Hello"},
        attempt_count=1,
        created_at=_TS,
        claimed_at=datetime.now(UTC),
        summary=summary,
        intent=intent,
        sentiment=sentiment,
        entities=[],
        action_items=action_items or [],
        reply_resolved=reply_resolved,
    )


# ── build_hubspot_properties ───────────────────────────────────────────────────

def test_properties_has_all_required_keys():
    props = build_hubspot_properties(make_msg())
    for key in (
        "ai_last_intent", "ai_last_sentiment", "ai_last_summary",
        "ai_last_action_items", "ai_last_channel", "ai_comm_log",
    ):
        assert key in props, f"Missing key: {key}"


def test_ai_comm_log_prepends_to_existing():
    props = build_hubspot_properties(make_msg(), existing_log="Old entry")
    assert props["ai_comm_log"].startswith("[")
    assert "Old entry" in props["ai_comm_log"]
    # New entry must appear BEFORE the old one
    new_pos = props["ai_comm_log"].index("[")
    old_pos = props["ai_comm_log"].index("Old entry")
    assert new_pos < old_pos


def test_ai_comm_log_not_duplicated_on_retry():
    """
    Regression: a retry (e.g. after a Note or Ticket creation failure) must NOT
    prepend a second copy of the same event's entry. build_hubspot_properties
    runs again with the log it already wrote to HubSpot; the idempotency guard
    detects the matching top entry and leaves the log unchanged.
    """
    msg = make_msg()
    first = build_hubspot_properties(msg)["ai_comm_log"]
    # Simulate the retry: feed back the exact log we already wrote to HubSpot.
    second = build_hubspot_properties(msg, existing_log=first)["ai_comm_log"]
    assert second == first
    # The event's header (timestamp · channel) must appear exactly once.
    assert second.count("· Inbound") == 1


def test_ai_comm_log_handles_crlf_from_hubspot():
    """
    HubSpot may return textarea values with \\r\\n. The guard normalises newlines
    so a retry whose stored log uses \\r\\n is still recognised as a duplicate.
    """
    msg = make_msg()
    first = build_hubspot_properties(msg)["ai_comm_log"]
    crlf_log = first.replace("\n", "\r\n")
    second = build_hubspot_properties(msg, existing_log=crlf_log)["ai_comm_log"]
    assert second.count("· Inbound") == 1


def test_ai_last_channel_reflects_msg_channel():
    props = build_hubspot_properties(make_msg(channel="whatsapp"))
    assert props["ai_last_channel"] == "whatsapp"


def test_ai_last_action_items_formatted():
    items = [{"description": "Call customer", "priority": "high"}]
    props = build_hubspot_properties(make_msg(action_items=items))
    assert "Call customer" in props["ai_last_action_items"]
    assert "[high]" in props["ai_last_action_items"]


def test_ai_last_action_items_empty_when_no_items():
    props = build_hubspot_properties(make_msg(action_items=[]))
    assert props["ai_last_action_items"] == ""


def test_empty_fields_become_empty_string():
    props = build_hubspot_properties(make_msg(intent=None, sentiment=None, summary=None))
    assert props["ai_last_intent"] == ""
    assert props["ai_last_sentiment"] == ""
    assert props["ai_last_summary"] == ""


# ── Follow-up injection for unresolved WhatsApp bot ───────────────────────────

def test_follow_up_injected_when_whatsapp_unresolved():
    """reply_resolved=False on a WhatsApp event → high-priority follow-up action item."""
    msg = make_msg(channel="whatsapp", event_type="whatsapp.received", reply_resolved=False)
    props = build_hubspot_properties(msg)
    assert "Follow up" in props["ai_last_action_items"]
    assert "[high]" in props["ai_last_action_items"]


def test_follow_up_not_injected_when_whatsapp_resolved():
    """reply_resolved=True → bot answered, no follow-up needed."""
    msg = make_msg(channel="whatsapp", event_type="whatsapp.received", reply_resolved=True)
    props = build_hubspot_properties(msg)
    assert "Follow up" not in props["ai_last_action_items"]


def test_follow_up_not_injected_for_sms():
    """SMS has no reply_resolved concept — no follow-up injection."""
    msg = make_msg(channel="sms", reply_resolved=None)
    props = build_hubspot_properties(msg)
    assert "Follow up" not in props["ai_last_action_items"]


def test_follow_up_not_injected_when_reply_resolved_is_none():
    """reply_resolved=None (non-WhatsApp or reply not set) → no injection."""
    msg = make_msg(channel="whatsapp", reply_resolved=None)
    props = build_hubspot_properties(msg)
    assert "Follow up" not in props["ai_last_action_items"]


def test_follow_up_prepended_before_existing_action_items():
    """When injected, the follow-up appears first (highest priority)."""
    existing = [{"description": "Check billing", "priority": "medium"}]
    msg = make_msg(channel="whatsapp", reply_resolved=False, action_items=existing)
    props = build_hubspot_properties(msg)
    follow_up_pos = props["ai_last_action_items"].index("Follow up")
    billing_pos = props["ai_last_action_items"].index("Check billing")
    assert follow_up_pos < billing_pos


# ── should_create_ticket ───────────────────────────────────────────────────────

def test_ticket_created_for_complaint_intent():
    assert should_create_ticket(make_msg(intent="complaint", sentiment="neutral")) is True


def test_ticket_created_for_negative_sentiment():
    assert should_create_ticket(make_msg(intent="general_query", sentiment="negative")) is True


def test_ticket_not_created_for_general_neutral():
    assert should_create_ticket(make_msg(intent="general_query", sentiment="neutral")) is False


def test_ticket_created_when_both_complaint_and_negative():
    assert should_create_ticket(make_msg(intent="complaint", sentiment="negative")) is True


# ── build_ticket_subject ───────────────────────────────────────────────────────

def test_ticket_subject_contains_channel():
    msg = make_msg(channel="whatsapp", intent="complaint")
    subject = build_ticket_subject(msg)
    assert "WHATSAPP" in subject


def test_ticket_subject_contains_complaint():
    msg = make_msg(intent="complaint", sentiment="neutral")
    subject = build_ticket_subject(msg)
    assert "complaint" in subject.lower() or "Complaint" in subject


def test_ticket_subject_contains_negative_when_not_complaint():
    msg = make_msg(intent="general_query", sentiment="negative")
    subject = build_ticket_subject(msg)
    assert "negative" in subject.lower() or "Negative" in subject


# ── format_event_block ─────────────────────────────────────────────────────────

def test_format_event_block_contains_summary():
    block = format_event_block(make_msg(summary="Test summary"))
    assert "Test summary" in block


def test_format_event_block_contains_intent_and_sentiment():
    block = format_event_block(make_msg(intent="support_request", sentiment="positive"))
    assert "support_request" in block
    assert "positive" in block


def test_format_event_block_shows_action_items():
    items = [{"description": "Reply now", "priority": "high"}]
    block = format_event_block(make_msg(action_items=items))
    assert "Reply now" in block


def test_format_event_block_unavailable_when_all_none():
    block = format_event_block(make_msg(summary=None, intent=None, sentiment=None))
    assert "unavailable" in block.lower()


# ── format_action_items ────────────────────────────────────────────────────────

def test_format_action_items_empty_list():
    assert format_action_items([]) == ""


def test_format_action_items_dict_items():
    items = [{"description": "Call back", "priority": "medium"}]
    result = format_action_items(items)
    assert "Call back" in result
    assert "[medium]" in result


def test_format_action_items_handles_non_dict():
    """Defensive: if an item is a plain string, it should not crash."""
    result = format_action_items(["Just a string"])
    assert "Just a string" in result


# ── should_create_task ─────────────────────────────────────────────────────────

def test_should_create_task_true_when_whatsapp_unresolved():
    """WhatsApp + reply_resolved=False → Task must be created for staff follow-up."""
    msg = make_msg(channel="whatsapp", event_type="whatsapp.received", reply_resolved=False)
    assert should_create_task(msg) is True


def test_should_create_task_false_when_whatsapp_resolved():
    """Bot answered the question — no need for a staff Task."""
    msg = make_msg(channel="whatsapp", event_type="whatsapp.received", reply_resolved=True)
    assert should_create_task(msg) is False


def test_should_create_task_false_for_sms():
    """SMS has no bot-reply concept — never create a Task based on reply_resolved."""
    msg = make_msg(channel="sms", reply_resolved=None)
    assert should_create_task(msg) is False


def test_should_create_task_false_when_resolved_is_none():
    """reply_resolved=None (no reply row) → no Task."""
    msg = make_msg(channel="whatsapp", reply_resolved=None)
    assert should_create_task(msg) is False


# ── build_task_subject / build_task_body ───────────────────────────────────────

def test_build_task_subject_contains_follow_up():
    msg = make_msg(channel="whatsapp", from_number="+15559876543", reply_resolved=False)
    subject = build_task_subject(msg)
    assert "Follow up" in subject


def test_build_task_subject_contains_phone_fragment():
    """Subject must include the last digits of the phone so the rep can identify the contact."""
    msg = make_msg(channel="whatsapp", from_number="+15559876543", reply_resolved=False)
    subject = build_task_subject(msg)
    assert "9876543" in subject


def test_build_task_body_equals_format_event_block():
    """Task body reuses format_event_block so the rep sees the full context."""
    msg = make_msg(channel="whatsapp", reply_resolved=False, summary="Test summary")
    assert build_task_body(msg) == format_event_block(msg)
