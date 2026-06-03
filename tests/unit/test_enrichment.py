"""
Unit tests for the Intelligence Layer (Phase 7).

What we test:
1. AI kill switch: worker loop sleeps when ai_enabled() DB call returns False.
2. claim_next returns None when the DB returns no row (empty queue).
3. claim_next returns None when the INSERT claim is lost to another worker (race).
4. SMS happy path: GPT-4o returns valid EnrichmentData → asyncpg UPDATE called
   with status='completed' and all enrichment fields.
5. Voice happy path: recording.ready row uses transcript_text, not raw_payload.
6. Retry then fail: GPT-4o raises on all 3 attempts → asyncpg UPDATE called with
   status='failed'; sleep was called between attempts.
7. Stale 'processing' row: claim_next re-claims it when lease has expired (returns event).
8. Empty SMS body → enrich_event writes status='skipped' (not 'failed').

We mock asyncpg pool and OpenAI so no real network calls are made.
Note: _update_enrichment now uses asyncpg (pool) not supabase-py.
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


def make_mock_pool_for_enrichment():
    """
    Build a mock asyncpg pool for enrich_event tests.

    _update_enrichment now uses asyncpg (pool.acquire → conn.execute).
    Returns (pool, conn_execute_mock) so tests can assert on the UPDATE call.
    """
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=False),
    ))
    return mock_pool, mock_conn.execute


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
    When the DB kill switch returns False, the worker must sleep and NOT call
    claim_next. We run for one iteration by cancelling after the first sleep.
    """
    mock_pool = MagicMock()
    mock_supabase = MagicMock()

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        raise asyncio.CancelledError

    with patch("intelligence_layer.consumer.settings") as mock_settings:
        mock_settings.DELIVERY_POLL_INTERVAL_SECONDS = 5.0

        with patch("intelligence_layer.consumer.ai_enabled", AsyncMock(return_value=False)):
            with patch("intelligence_layer.consumer.claim_next") as mock_claim:
                with patch("intelligence_layer.consumer.asyncio.sleep", side_effect=fake_sleep):
                    with pytest.raises(asyncio.CancelledError):
                        await _worker(mock_pool, mock_supabase, worker_id=0)

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
    must call asyncpg execute (UPDATE enrichments) with status='completed'.
    """
    event = make_sms_event()
    mock_pool, mock_execute = make_mock_pool_for_enrichment()
    gpt4o_response = make_gpt4o_response()

    with patch("intelligence_layer.enrichment.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.return_value = gpt4o_response
            mock_openai_cls.return_value = mock_client

            await enrich_event(pool=mock_pool, supabase=None, event=event)

    # execute called once — the UPDATE enrichments SQL
    mock_execute.assert_called_once()
    sql_call = mock_execute.call_args[0][0]
    args = mock_execute.call_args[0]
    assert "UPDATE enrichments" in sql_call
    # $2 = status (second positional param after comm_event_id)
    assert "completed" in args


# ── Test 5: Voice happy path ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_voice_uses_transcript_text():
    """
    For recording.ready events, GPT-4o must be called with transcript_text
    (not with anything from raw_payload).
    """
    from comm_layer.contracts.enriched import ActionItem, EnrichmentData

    event = make_voice_event()
    mock_pool, mock_execute = make_mock_pool_for_enrichment()

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
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.side_effect = fake_parse
            mock_openai_cls.return_value = mock_client

            await enrich_event(pool=mock_pool, supabase=None, event=event)

    user_msg = next(m for m in captured_messages if m["role"] == "user")
    assert "cancel my subscription" in user_msg["content"]
    assert "RecordingUrl" not in user_msg["content"]

    mock_execute.assert_called_once()
    args = mock_execute.call_args[0]
    assert "completed" in args


# ── Test 6: Retry then fail ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_retries_then_marks_failed():
    """
    If GPT-4o raises on all 3 attempts, enrich_event must:
    - call the API exactly 3 times (1 initial + 2 retries),
    - sleep RETRY_SLEEP_SECONDS between each attempt,
    - call asyncpg execute with status='failed'.
    """
    event = make_sms_event()
    mock_pool, mock_execute = make_mock_pool_for_enrichment()

    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    with patch("intelligence_layer.enrichment.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("intelligence_layer.enrichment.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.beta.chat.completions.parse.side_effect = RuntimeError("API error")
            mock_openai_cls.return_value = mock_client

            with patch("intelligence_layer.enrichment.time.sleep", side_effect=fake_sleep):
                await enrich_event(pool=mock_pool, supabase=None, event=event)

    assert mock_client.beta.chat.completions.parse.call_count == 3
    assert len(sleep_calls) == 2
    from intelligence_layer.enrichment import RETRY_SLEEP_SECONDS
    assert all(s == RETRY_SLEEP_SECONDS for s in sleep_calls)

    mock_execute.assert_called_once()
    args = mock_execute.call_args[0]
    assert "failed" in args


# ── Test 7: Stale processing row is re-claimable ───────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_reclaims_stale_processing_row():
    """
    A 'processing' enrichment row older than the lease must be treated as if it
    doesn't exist — claim_next should return the event so it gets re-enriched.
    This covers the crash-recovery path: worker dies mid-enrichment, leaving a
    stale 'processing' row that would otherwise block delivery forever.
    """
    select_row = {
        "id": FAKE_COMM_EVENT_ID,
        "event_type": "sms.received",
        "raw_payload": '{"Body": "Help please"}',
        "correlation_id": FAKE_CORRELATION_ID,
        "transcript_text": None,
    }
    # The upsert succeeds — the stale row was reclaimed.
    claim_row = {"id": FAKE_ENRICHMENT_ID}

    mock_pool, mock_conn = make_mock_pool(
        fetchrow_return=select_row,
        claim_row_return=claim_row,
    )

    # Use a very short lease so the query matches easily.
    result = await claim_next(mock_pool, lease_seconds=1)

    assert result is not None
    assert result["id"] == FAKE_COMM_EVENT_ID
    # Both SELECT and UPSERT were issued.
    assert mock_conn.fetchrow.call_count == 2


# ── Test 8: Empty SMS body → status='skipped' ─────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_event_empty_body_writes_skipped():
    """
    An SMS with an empty body has no text to enrich. enrich_event should write
    status='skipped' (not 'failed') so it is distinct from a GPT-4o API failure
    and the delivery gate can still ship the event to HubSpot.
    """
    event = {
        "id": FAKE_COMM_EVENT_ID,
        "event_type": "sms.received",
        "raw_payload": {"Body": ""},
        "correlation_id": FAKE_CORRELATION_ID,
        "transcript_text": None,
    }
    mock_pool, mock_execute = make_mock_pool_for_enrichment()

    await enrich_event(pool=mock_pool, supabase=None, event=event)

    mock_execute.assert_called_once()
    # The SQL and positional args are all in call_args[0].
    # $2 = 'skipped' (the status), present in the args tuple.
    args = mock_execute.call_args[0]
    assert "skipped" in args, (
        f"Expected 'skipped' in execute args, got: {args}"
    )
