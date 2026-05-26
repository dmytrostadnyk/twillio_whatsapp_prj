"""
Integration test — Case 2: claim → mock Azure CRM → delivered.

Inserts a comm_events row directly, uses respx to stub the Azure CRM HTTP
endpoint, calls process_message() once, and asserts the row is marked
'delivered' with a contract_payload.

WHY we call process_message() directly instead of running the poll loop:
The poll loop adds timing dependencies (sleep intervals, lease expiry) that
make tests slow and flaky. Calling the per-message processor directly proves
the delivery logic without the polling machinery.
"""

from __future__ import annotations

import json
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


async def _insert_pending_event(conn, event_key: str) -> uuid.UUID:
    """Insert a minimal comm_events row in 'pending' state. Returns the UUID."""
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
    return row["id"]


@pytest.mark.asyncio
async def test_delivery_worker_delivers_event(pool, test_prefix):
    """
    Given a 'pending' comm_events row and a CRM stub that returns 200,
    process_message() should mark the row as 'delivered' with a non-null
    contract_payload.
    """
    event_key = f"{test_prefix}SM-deliv-1:sms.received"

    async with pool.acquire() as conn:
        event_id = await _insert_pending_event(conn, event_key)

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
    )

    crm_url = f"{settings.AZURE_CRM_URL}/events"

    with respx.mock:
        respx.post(crm_url).mock(return_value=httpx.Response(200))
        async with httpx.AsyncClient() as http_client:
            await process_message(broker, http_client, msg)

    # Verify the DB row was updated
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT delivery_status, contract_payload FROM comm_events WHERE id = $1",
            event_id,
        )

    assert row is not None
    assert row["delivery_status"] == "delivered", (
        f"Expected 'delivered', got '{row['delivery_status']}'"
    )
    assert row["contract_payload"] is not None, "contract_payload should be written on success"


@pytest.mark.asyncio
async def test_delivery_worker_nacks_on_5xx(pool, test_prefix):
    """
    A 500 response from Azure CRM should nack the event (delivery_status='failed')
    rather than dead-lettering it (attempt_count=1 is within the retry limit).
    """
    event_key = f"{test_prefix}SM-deliv-nack:sms.received"

    async with pool.acquire() as conn:
        event_id = await _insert_pending_event(conn, event_key)

    broker = PostgresBroker(pool)
    msg = BrokerMessage(
        id=event_id,
        event_key=event_key,
        correlation_id=uuid.uuid4(),
        channel="sms",
        direction="inbound",
        event_type="sms.received",
        from_number="+15550000003",
        to_number=settings.TWILIO_PHONE_NUMBER,
        source_metadata={"is_unknown": True},
        raw_payload={"Body": "5xx test"},
        attempt_count=1,
        created_at=datetime.now(UTC),
        claimed_at=datetime.now(UTC),
    )

    with respx.mock:
        respx.post(f"{settings.AZURE_CRM_URL}/events").mock(
            return_value=httpx.Response(500)
        )
        async with httpx.AsyncClient() as http_client:
            await process_message(broker, http_client, msg)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT delivery_status FROM comm_events WHERE id = $1",
            event_id,
        )

    assert row["delivery_status"] == "failed", (
        f"Expected 'failed' after 5xx, got '{row['delivery_status']}'"
    )
