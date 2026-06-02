"""
Integration test — HubSpot delivery: enriched event → contact updated.

Seeds a comm_events row (delivery_status='pending') and a terminal enrichments
row, then calls process_message() with mocked HubSpot endpoints and asserts:
- The comm_events row is marked 'delivered'
- hubspot_contact_id is persisted on the row
- A delivery_log success row is written

WHY we call process_message() directly instead of running the poll loop:
The poll loop adds timing dependencies (sleep intervals, lease expiry) that
make tests slow and flaky. Calling the per-message processor directly proves
the delivery logic without the polling machinery.

WHY we seed an enrichments row:
claim_next() now gates on enrichments.status IN ('completed','failed','skipped').
Without a terminal enrichments row the event is invisible to the delivery worker
and process_message can't be demonstrated end-to-end via the DB-backed broker.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import httpx
import pytest
import respx

from comm_layer.broker.base import BrokerMessage
from comm_layer.broker.postgres import PostgresBroker
from comm_layer.config import settings
from delivery_worker.main import process_message

pytestmark = pytest.mark.integration

_SEARCH_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
_CREATE_URL = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts"
_PATCH_URL_PREFIX = f"{settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/"

_FOUND_CONTACT = {
    "results": [
        {"id": "hs-contact-999", "properties": {"phone": "+15550000001", "ai_comm_log": ""}}
    ]
}


async def _insert_pending_event_with_enrichment(conn, event_key: str) -> uuid.UUID:
    """
    Insert a comm_events row in 'pending' state and a terminal enrichments row.

    Returns the comm_events UUID.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO comm_events (
            event_key, channel, direction, event_type,
            from_number, to_number,
            source_metadata, raw_payload,
            correlation_id, delivery_status
        )
        VALUES (
            $1, 'sms', 'inbound', 'sms.received',
            '+15550000001', $2,
            '{"is_unknown": true}'::jsonb,
            '{"Body": "integration test"}'::jsonb,
            $3, 'pending'
        )
        RETURNING id
        """,
        event_key,
        settings.TWILIO_PHONE_NUMBER,
        uuid.uuid4(),
    )
    event_id = row["id"]

    # Insert a terminal enrichments row so claim_next() can see this event.
    await conn.execute(
        """
        INSERT INTO enrichments (comm_event_id, status, model, schema_version,
                                  summary, intent, sentiment)
        VALUES ($1, 'completed', 'gpt-4o', '1.0',
                'Test summary.', 'general_query', 'neutral')
        ON CONFLICT (comm_event_id) DO NOTHING
        """,
        event_id,
    )
    return event_id


@pytest.mark.asyncio
async def test_delivery_marks_event_delivered_and_persists_contact_id(pool, test_prefix):
    """
    End-to-end: pending event + enrichment → HubSpot mocked → row delivered.
    Verifies that:
    - delivery_status is updated to 'delivered'
    - hubspot_contact_id is persisted on the comm_events row
    - contract_payload (ack payload) is not null
    """
    event_key = f"{test_prefix}SM-hubspot-1:sms.received"

    async with pool.acquire() as conn:
        event_id = await _insert_pending_event_with_enrichment(conn, event_key)

    broker = PostgresBroker(pool)
    msg = BrokerMessage(
        id=event_id,
        event_key=event_key,
        correlation_id=uuid.uuid4(),
        channel="sms",
        direction="inbound",
        event_type="sms.received",
        from_number="+15550000001",
        to_number=settings.TWILIO_PHONE_NUMBER,
        source_metadata={"is_unknown": True},
        raw_payload={"Body": "integration test"},
        attempt_count=1,
        created_at=datetime.now(UTC),
        claimed_at=datetime.now(UTC),
        summary="Test summary.",
        intent="general_query",
        sentiment="neutral",
        entities=[],
        action_items=[],
        hubspot_contact_id=None,
    )

    with respx.mock:
        respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=_FOUND_CONTACT))
        respx.patch(url__startswith=_PATCH_URL_PREFIX).mock(
            return_value=httpx.Response(200, json={})
        )
        async with httpx.AsyncClient() as http_client:
            await process_message(broker, http_client, msg, pool)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT delivery_status, contract_payload, hubspot_contact_id
            FROM comm_events WHERE id = $1
            """,
            event_id,
        )

    assert row["delivery_status"] == "delivered", (
        f"Expected 'delivered', got '{row['delivery_status']}'"
    )
    assert row["contract_payload"] is not None, "contract_payload should be written on success"
    assert row["hubspot_contact_id"] == "hs-contact-999", (
        f"Expected 'hs-contact-999', got '{row['hubspot_contact_id']}'"
    )


@pytest.mark.asyncio
async def test_delivery_nacks_on_hubspot_5xx(pool, test_prefix):
    """
    HubSpot 500 on contact search → event stays failed (nacked), not delivered.
    """
    event_key = f"{test_prefix}SM-hubspot-nack:sms.received"

    async with pool.acquire() as conn:
        event_id = await _insert_pending_event_with_enrichment(conn, event_key)

    broker = PostgresBroker(pool)
    msg = BrokerMessage(
        id=event_id,
        event_key=event_key,
        correlation_id=uuid.uuid4(),
        channel="sms",
        direction="inbound",
        event_type="sms.received",
        from_number="+15550000002",
        to_number=settings.TWILIO_PHONE_NUMBER,
        source_metadata={"is_unknown": True},
        raw_payload={"Body": "5xx test"},
        attempt_count=1,
        created_at=datetime.now(UTC),
        claimed_at=datetime.now(UTC),
        summary="Test summary.",
        intent="general_query",
        sentiment="neutral",
        entities=[],
        action_items=[],
        hubspot_contact_id=None,
    )

    with respx.mock:
        respx.post(_SEARCH_URL).mock(return_value=httpx.Response(500))
        async with httpx.AsyncClient() as http_client:
            await process_message(broker, http_client, msg, pool)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT delivery_status FROM comm_events WHERE id = $1",
            event_id,
        )

    assert row["delivery_status"] == "failed", (
        f"Expected 'failed' after 5xx, got '{row['delivery_status']}'"
    )
