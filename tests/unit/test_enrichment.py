"""
Unit tests for the Intelligence Layer (Phase 7).

What we test:
1. AI kill switch: worker loop sleeps when AI_ENABLED=False, never calls claim_next.
2. claim_next returns None when the DB returns no row (empty queue).
3. claim_next returns None when the INSERT claim is lost to another worker (race).
4. SMS happy path: GPT-4o returns valid EnrichmentData → supabase UPDATE called
   with status='completed' and all enrichment fields.
5. Voice happy path: recording.ready row uses transcript_text, not raw_payload.
6. Retry then fail: GPT-4o raises on all 3 attempts → supabase UPDATE called with
   status='failed'; sleep was called between attempts.

We mock asyncpg pool, Supabase client, and OpenAI so no real network calls are made.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_layer.consumer import _worker, claim_next
from intelligence_layer.enrichment import enrich_event

# ── Helpers ────────────────────────────────────────────────────────────────────

FAKE_COMM_EVENT_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
FAKE_CORRELATION_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
FAKE_ENRICHMENT_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def make_sms_event() -> dict:
    """Build a minimal sms.received comm_event dict as consumer.claim_next returns."""
    return {
        "id": FAKE_COMM_EVENT_ID,
        "event_type": "sms.received",
        "raw_payload": {"Body": "Hello, I need help with my order"},
        "correlation_id": FAKE_CORRELATION_ID,
        "transcript_text": None,
    }


def make_voice_event() -> dict:
    """Build a minimal recording.ready comm_event dict with transcript text."""
    return {
        "id": FAKE_COMM_EVENT_ID,
        "event_type": "recording.ready",
        "raw_payload": {"RecordingUrl": "https://api.twilio.com/RE123"},
        "correlation_id": FAKE_CORRELATION_ID,
        "transcript_text": "Hi I would like to cancel my subscription please",
    }


def make_gpt4o_response(
    summary: str = "Customer needs order help.",
    intent: str = "support_request",
    sentiment: str = "neutral",
    entities: list | None = None,
    action_items: list | None = None,
) -> SimpleNamespace:
    """
    Build a mock return value for client.beta.chat.completions.parse().

    The real API returns an object where .choices[0].message.parsed is the
    Pydantic model instance.
    """
    from comm_layer.contracts.enriched import ActionItem, EnrichmentData, Entity

    if entities is None:
        entities = [Entity(entity_type="PRODUCT", value="order")]
    if action_items is None:
        action_items = [ActionItem(description="Check order status", priority="high")]

    parsed = EnrichmentData(
        summary=summary,
        intent=intent,
        sentiment=sentiment,  # type: ignore[arg-type]
        entities=entities,
        action_items=action_items,
    )
    message = SimpleNamespace(parsed=parsed)
    choice = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice])


def make_mock_supabase():
    """
    Build a mock Supabase client that records update().eq().execute() calls.

    WHY a self-returning builder mock: supabase uses a fluent interface —
    table(...).update(...).eq(...).execute(). Each step returns the same
    builder object so we can chain arbitrary calls and still reach .execute.
    """
    mock_execute = AsyncMock(return_value=None)
    # A single builder object whose methods all return itself (fluent pattern).
    mock_builder = MagicMock()
    mock_builder.execute = mock_execute
    mock_builder.update = MagicMock(return_value=mock_builder)
    mock_builder.eq = MagicMock(return_value=mock_builder)
    mock_builder.insert = MagicMock(return_value=mock_builder)
    mock_supabase = MagicMock()
    mock_supabase.table = MagicMock(return_value=mock_builder)
    return mock_supabase, mock_execute, mock_builder


def make_mock_pool(fetchrow_return=None, claim_row_return=None):
    """
    Build a mock asyncpg pool that returns the given values from fetchrow.

    fetchrow_return: the row returned by the SELECT FOR UPDATE query.
    claim_row_return: the row returned by the INSERT ... RETURNING id query.
    """
    mock_conn = AsyncMock()

    # fetchrow is called twice in claim_next: once for SELECT, once for INSERT.
    mock_conn.fetchrow = AsyncMock(side_effect=[fetchrow_return, claim_row_return])

    # transaction() is a context manager: async with conn.transaction()
    mock_conn.transaction = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=False),
    ))
    return mock_pool, mock_conn


# ── Test 1: AI kill switch ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_worker_sleeps_when_ai_disabled():
    """
    When AI_ENABLED=False the worker must sleep and NOT call claim_next.
    We run the worker for one iteration by cancelling it after the first sleep.
    """
    mock_pool = MagicMock()
    mock_supabase = MagicMock()

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        # Cancel the task after the first sleep so the loop exits.
        raise asyncio.CancelledError

    with patch("intelligence_layer.consumer.settings") as mock_settings:
        mock_settings.AI_ENABLED = False
        mock_settings.DELIVERY_POLL_INTERVAL_SECONDS = 5.0

        with patch("intelligence_layer.consumer.claim_next") as mock_claim:
            with patch("intelligence_layer.consumer.asyncio.sleep", side_effect=fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await _worker(mock_pool, mock_supabase, worker_id=0)

    # Worker slept but never tried to claim work.
    assert len(sleep_calls) == 1
    mock_claim.assert_not_called()


# ── Test 2: claim_next returns None when queue is empty ────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_queue_empty():
    """
    When the SELECT FOR UPDATE returns no row, claim_next must return None
    without attempting an INSERT.
    """
    mock_pool, mock_conn = make_mock_pool(
        fetchrow_return=None,   # empty queue
        claim_row_return=None,  # INSERT never called, but set defensively
    )

    result = await claim_next(mock_pool)

    assert result is None
    # INSERT should not have been called because SELECT returned None first.
    # fetchrow was called exactly once (the SELECT).
    assert mock_conn.fetchrow.call_count == 1


# ── Test 3: claim_next returns None when claim INSERT is lost ──────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_claim_lost():
    """
    If the INSERT ... RETURNING id returns nothing, another worker already
    claimed this event. claim_next must return None without calling enrich_event.
    """
    # Build an asyncpg Record-like object for the SELECT FOR UPDATE result.
    select_row = {
        "id": FAKE_COMM_EVENT_ID,
        "event_type": "sms.received",
        "raw_payload": '{"Body": "test"}',
        "correlation_id": FAKE_CORRELATION_ID,
        "transcript_text": None,
    }

    mock_pool, mock_conn = make_mock_pool(
        fetchrow_return=select_row,
        claim_row_return=None,  # INSERT returned nothing → race lost
    )

    result = await claim_next(mock_pool)

    assert result is None
    # Both queries were issued: SELECT then INSERT.
    assert mock_conn.fetchrow.call_count == 2


# ── Test 4: SMS happy path ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_sms_happy_path():
    """
    Given an sms.received event and a successful GPT-4o response, enrich_event
    must call supabase.update with status='completed' and all enrichment fields.
    """
    event = make_sms_event()
    mock_supabase, mock_execute, mock_builder = make_mock_supabase()
    gpt4o_response = make_gpt4o_response()

    with patch("intelligence_layer.enrichment.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.return_value = gpt4o_response
            mock_openai_cls.return_value = mock_client

            await enrich_event(pool=None, supabase=mock_supabase, event=event)

    # UPDATE was called once with status='completed'
    mock_execute.assert_called_once()
    update_payload = mock_builder.update.call_args[0][0]
    assert update_payload["status"] == "completed"
    assert update_payload["summary"] == "Customer needs order help."
    assert update_payload["intent"] == "support_request"
    assert update_payload["sentiment"] == "neutral"
    assert len(update_payload["entities"]) == 1
    assert update_payload["entities"][0]["value"] == "order"
    assert len(update_payload["action_items"]) == 1


# ── Test 5: Voice happy path ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_voice_uses_transcript_text():
    """
    For recording.ready events, GPT-4o must be called with transcript_text
    (not with anything from raw_payload).  We verify this by checking the
    user message passed to completions.parse().
    """
    from comm_layer.contracts.enriched import ActionItem, EnrichmentData

    event = make_voice_event()
    mock_supabase, mock_execute, mock_builder = make_mock_supabase()

    parsed = EnrichmentData(
        summary="Customer wants to cancel subscription.",
        intent="cancellation",
        sentiment="negative",
        entities=[],
        action_items=[ActionItem(description="Process cancellation", priority="high")],
    )
    gpt4o_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))]
    )

    captured_messages = []

    def fake_parse(**kwargs):
        captured_messages.extend(kwargs["messages"])
        return gpt4o_response

    with patch("intelligence_layer.enrichment.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.side_effect = fake_parse
            mock_openai_cls.return_value = mock_client

            await enrich_event(pool=None, supabase=mock_supabase, event=event)

    # The user message must contain the transcript text, not a URL.
    user_msg = next(m for m in captured_messages if m["role"] == "user")
    assert "cancel my subscription" in user_msg["content"]
    assert "RecordingUrl" not in user_msg["content"]

    mock_execute.assert_called_once()
    update_payload = mock_builder.update.call_args[0][0]
    assert update_payload["status"] == "completed"
    assert update_payload["intent"] == "cancellation"


# ── Test 6: Retry then fail ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_retries_then_marks_failed():
    """
    If GPT-4o raises on all 3 attempts, enrich_event must:
    - call the API exactly 3 times (1 initial + 2 retries),
    - sleep RETRY_SLEEP_SECONDS between each attempt,
    - call supabase UPDATE with status='failed'.
    """
    event = make_sms_event()
    mock_supabase, mock_execute, mock_builder = make_mock_supabase()

    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    with patch("intelligence_layer.enrichment.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.side_effect = RuntimeError("API error")
            mock_openai_cls.return_value = mock_client

            # time.sleep is used inside the sync _call_gpt4o_with_retries helper.
            with patch("intelligence_layer.enrichment.time.sleep", side_effect=fake_sleep):
                await enrich_event(pool=None, supabase=mock_supabase, event=event)

    # 3 total attempts (MAX_RETRIES=2 means 1 initial + 2 retries)
    assert mock_client.beta.chat.completions.parse.call_count == 3

    # Sleep called between attempts: after attempt 0 and after attempt 1.
    assert len(sleep_calls) == 2
    from intelligence_layer.enrichment import RETRY_SLEEP_SECONDS
    assert all(s == RETRY_SLEEP_SECONDS for s in sleep_calls)

    # Final update must be status='failed'
    mock_execute.assert_called_once()
    update_payload = mock_builder.update.call_args[0][0]
    assert update_payload["status"] == "failed"
    assert "failure_reason" in update_payload
