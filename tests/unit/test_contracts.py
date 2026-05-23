"""
Unit tests for the Pydantic event contract models.

What we're testing:
1. Every event type round-trips through Pydantic without errors.
2. Required fields raise ValidationError when missing.
3. The schema_version field defaults to "1.0".
4. Naive datetimes are coerced to UTC.
5. EventSource.is_unknown=True is accepted.
6. Extra fields are rejected (extra='forbid' catches typos in consumer code).
"""

import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from comm_layer.contracts import (
    CallCompletedEvent,
    CallStartedEvent,
    Channel,
    Direction,
    EnrichedCommEvent,
    EventSource,
    RecordingReadyEvent,
    SmsReceivedEvent,
    SmsStatusEvent,
    WhatsAppReceivedEvent,
    WhatsAppStatusEvent,
)
from comm_layer.contracts.enriched import ActionItem, EnrichmentData, Entity

# ── Shared helpers ─────────────────────────────────────────────────────────────


def make_source(is_unknown: bool = False) -> dict:
    """Build a valid EventSource dict for use in test constructors."""
    if is_unknown:
        return {
            "number": "+15559999999",
            "source_type": None,
            "source_id": None,
            "label": None,
            "is_unknown": True,
            "metadata": {},
        }
    return {
        "number": "+15551234567",
        "source_type": "campaign",
        "source_id": "camp_spring_2025",
        "label": "Spring 2025 Campaign",
        "is_unknown": False,
        "metadata": {"region": "us-east"},
    }


CID = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
TS = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)


# ── EventSource ────────────────────────────────────────────────────────────────


def test_event_source_known():
    src = EventSource(**make_source())
    assert src.is_unknown is False
    assert src.source_type == "campaign"


def test_event_source_unknown():
    src = EventSource(**make_source(is_unknown=True))
    assert src.is_unknown is True
    assert src.source_type is None


# ── SmsReceivedEvent ───────────────────────────────────────────────────────────


def test_sms_received_round_trip():
    event = SmsReceivedEvent(
        event_key="SM123:sms.received",
        correlation_id=CID,
        timestamp=TS,
        source=make_source(),
        message_sid="SM123",
        from_number="+15559876543",
        to_number="+15551234567",
        body="Hello!",
    )
    assert event.schema_version == "1.0"
    assert event.channel == "sms"
    assert event.direction == "inbound"
    assert event.event_type == "sms.received"
    assert event.num_media == 0
    assert event.media_urls == []


def test_sms_received_missing_body_raises():
    with pytest.raises(ValidationError):
        SmsReceivedEvent(
            event_key="SM123:sms.received",
            source=make_source(),
            message_sid="SM123",
            from_number="+15559876543",
            to_number="+15551234567",
            # body intentionally missing
        )


def test_sms_received_extra_field_rejected():
    """Extra fields must raise a ValidationError — catches consumer typos."""
    with pytest.raises(ValidationError):
        SmsReceivedEvent(
            event_key="SM123:sms.received",
            source=make_source(),
            message_sid="SM123",
            from_number="+15559876543",
            to_number="+15551234567",
            body="Hello",
            unknown_extra_field="this should be rejected",
        )


def test_sms_received_with_unknown_source():
    """Events with unknown source numbers must still be valid — we never drop them."""
    event = SmsReceivedEvent(
        event_key="SM123:sms.received",
        source=make_source(is_unknown=True),
        message_sid="SM123",
        from_number="+15559876543",
        to_number="+15559999999",
        body="Hello from an unknown number",
    )
    assert event.source.is_unknown is True


# ── SmsStatusEvent ────────────────────────────────────────────────────────────


def test_sms_status_round_trip():
    event = SmsStatusEvent(
        event_key="SM123:sms.status",
        source=make_source(),
        message_sid="SM123",
        from_number="+15551234567",
        to_number="+15559876543",
        message_status="delivered",
    )
    assert event.direction == "outbound"
    assert event.error_code is None


def test_sms_status_failed_includes_error():
    event = SmsStatusEvent(
        event_key="SM123:sms.status",
        source=make_source(),
        message_sid="SM123",
        from_number="+15551234567",
        to_number="+15559876543",
        message_status="failed",
        error_code="30003",
        error_message="Unreachable destination handset",
    )
    assert event.error_code == "30003"


# ── CallStartedEvent ──────────────────────────────────────────────────────────


def test_call_started_round_trip():
    event = CallStartedEvent(
        event_key="CA123:call.started",
        source=make_source(),
        call_sid="CA123",
        from_number="+15559876543",
        to_number="+15551234567",
        call_status="in-progress",
        direction=Direction.INBOUND,
    )
    assert event.channel == "voice"
    assert event.event_type == "call.started"


# ── CallCompletedEvent ────────────────────────────────────────────────────────


def test_call_completed_round_trip():
    event = CallCompletedEvent(
        event_key="CA123:call.completed",
        source=make_source(),
        call_sid="CA123",
        from_number="+15559876543",
        to_number="+15551234567",
        call_status="completed",
        duration="346",
        direction=Direction.INBOUND,
    )
    assert event.duration == "346"


def test_call_completed_null_duration_allowed():
    """Duration can be null for very short calls."""
    event = CallCompletedEvent(
        event_key="CA123:call.completed",
        source=make_source(),
        call_sid="CA123",
        from_number="+15559876543",
        to_number="+15551234567",
        call_status="no-answer",
        duration=None,
        direction=Direction.INBOUND,
    )
    assert event.duration is None


# ── RecordingReadyEvent ───────────────────────────────────────────────────────


def test_recording_ready_round_trip():
    event = RecordingReadyEvent(
        event_key="RE123:call.recording_ready",
        source=make_source(),
        call_sid="CA123",
        recording_sid="RE123",
        recording_api_path="/2010-04-01/Accounts/AC.../Recordings/RE...",
        recording_status="completed",
        duration="346",
    )
    assert event.event_type == "call.recording_ready"
    # Verify we are NOT storing a public URL — this is security-sensitive
    assert not event.recording_api_path.startswith("http")


# ── WhatsApp events ────────────────────────────────────────────────────────────


def test_whatsapp_received_round_trip():
    event = WhatsAppReceivedEvent(
        event_key="SM123:whatsapp.received",
        source=make_source(),
        message_sid="SM123",
        from_number="whatsapp:+15559876543",
        to_number="whatsapp:+14155238886",
        body="Hi there",
        profile_name="John Doe",
    )
    assert event.channel == "whatsapp"
    assert event.direction == "inbound"


def test_whatsapp_status_with_read_receipt():
    event = WhatsAppStatusEvent(
        event_key="SM123:whatsapp.status",
        source=make_source(),
        message_sid="SM123",
        from_number="whatsapp:+14155238886",
        to_number="whatsapp:+15559876543",
        message_status="read",
        is_template=False,
    )
    assert event.message_status == "read"


# ── EnrichedCommEvent ─────────────────────────────────────────────────────────


def test_enriched_event_round_trip():
    enrichment = EnrichmentData(
        summary="Customer asked about pricing.",
        intent="sales_inquiry",
        sentiment="positive",
        entities=[Entity(entity_type="PRODUCT", value="premium plan")],
        action_items=[ActionItem(description="Send brochure", priority="high")],
    )
    event = EnrichedCommEvent(
        event_key="SM123:comm.enriched",
        channel=Channel.SMS,
        direction=Direction.INBOUND,
        source=make_source(),
        original_event_key="SM123:sms.received",
        original_event_type="sms.received",
        enrichment=enrichment,
        model_used="gpt-4o",
    )
    assert event.event_type == "comm.enriched"
    assert event.enrichment.sentiment == "positive"
    assert len(event.enrichment.entities) == 1


def test_enrichment_invalid_sentiment_rejected():
    """The LLM sometimes returns unexpected values — Pydantic must catch them."""
    with pytest.raises(ValidationError):
        EnrichmentData(
            summary="Test",
            intent="sales_inquiry",
            sentiment="VERY_POSITIVE",  # not in the Literal type
        )


# ── schema_version and timestamp coercion ─────────────────────────────────────


def test_schema_version_defaults_to_1_0():
    event = SmsReceivedEvent(
        event_key="SM123:sms.received",
        source=make_source(),
        message_sid="SM123",
        from_number="+15559876543",
        to_number="+15551234567",
        body="Hello",
    )
    assert event.schema_version == "1.0"


def test_naive_timestamp_coerced_to_utc():
    """Naive datetimes must be treated as UTC — never as local time."""
    from datetime import datetime

    naive = datetime(2026, 1, 15, 14, 30, 0)  # no tzinfo
    event = SmsReceivedEvent(
        event_key="SM123:sms.received",
        source=make_source(),
        message_sid="SM123",
        from_number="+15559876543",
        to_number="+15551234567",
        body="Hello",
        timestamp=naive,
    )
    assert event.timestamp.tzinfo is not None
    assert event.timestamp.tzinfo == UTC


# ── JSON serialisation round-trip ─────────────────────────────────────────────


def test_sms_event_json_round_trip():
    """The event must survive a JSON serialise → deserialise cycle unchanged."""
    original = SmsReceivedEvent(
        event_key="SM123:sms.received",
        source=make_source(),
        message_sid="SM123",
        from_number="+15559876543",
        to_number="+15551234567",
        body="Round-trip test",
    )
    json_str = original.model_dump_json()
    restored = SmsReceivedEvent.model_validate_json(json_str)
    assert restored.event_key == original.event_key
    assert restored.body == original.body
    assert restored.source.source_id == original.source.source_id
