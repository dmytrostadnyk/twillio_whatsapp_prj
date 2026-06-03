"""
Unit tests for the Phase 8 embedding consumer.

What we test:
1. AI kill switch: AI_ENABLED=False → worker sleeps, never claims, never calls OpenAI.
2. claim_next_for_embedding returns None when no pending rows exist.
3. SMS happy path: content sent to OpenAI starts with "Summary:" and includes
   the SMS body. INSERT into embeddings + UPDATE to embedding_status='completed'.
4. Voice happy path: the transcript_text is what gets embedded, NOT the raw payload
   recording URL.
5. OpenAI fails 3 times: enrichment marked embedding_status='failed', no INSERT
   into embeddings.

We mock asyncpg pool and OpenAI so no real network calls happen.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from intelligence_layer.embedding import (
    _embedding_worker,
    claim_next_for_embedding,
    embed_event,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

FAKE_ENRICHMENT_ID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
FAKE_COMM_EVENT_ID = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")


def make_mock_pool(
    fetchrow_return=None,
    execute_results: list | None = None,
):
    """
    Build a mock asyncpg pool.

    fetchrow_return: row returned by the SELECT FOR UPDATE in claim_next.
    execute_results: ordered list of return values for conn.execute (UPDATE/INSERT).
    """
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    mock_conn.execute = AsyncMock(side_effect=execute_results or [None] * 10)

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


def make_embedding_response(dim: int = 1536) -> SimpleNamespace:
    """Build a mock OpenAI embeddings.create response with `dim`-length vector."""
    fake_vector = [0.001 * i for i in range(dim)]
    datum = SimpleNamespace(embedding=fake_vector)
    return SimpleNamespace(data=[datum])


def make_sms_claim_row() -> dict:
    """Row shape returned by the SELECT in claim_next_for_embedding for an SMS."""
    return {
        "enrichment_id": FAKE_ENRICHMENT_ID,
        "comm_event_id": FAKE_COMM_EVENT_ID,
        "summary": "Customer needs help with their order.",
        "event_type": "sms.received",
        "raw_payload": '{"Body": "Hello, please help me with my order"}',
        "transcript_text": None,
    }


def make_voice_claim_row() -> dict:
    """Row shape returned by the SELECT for a recording.ready event."""
    return {
        "enrichment_id": FAKE_ENRICHMENT_ID,
        "comm_event_id": FAKE_COMM_EVENT_ID,
        "summary": "Customer wants to cancel their subscription.",
        "event_type": "recording.ready",
        "raw_payload": '{"RecordingUrl": "https://api.twilio.com/RE123"}',
        "transcript_text": "Hi I would like to cancel my subscription please",
    }


# ── Test 1: AI kill switch ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_embedding_worker_sleeps_when_ai_disabled():
    """
    When the DB kill switch returns False, the worker must sleep and NEVER call
    claim_next. We cancel after the first sleep call to exit the infinite loop.
    """
    mock_pool = MagicMock()
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        raise asyncio.CancelledError

    with patch("intelligence_layer.embedding.settings") as mock_settings:
        mock_settings.DELIVERY_POLL_INTERVAL_SECONDS = 5.0

        with patch("intelligence_layer.embedding.ai_enabled", AsyncMock(return_value=False)):
            with patch("intelligence_layer.embedding.claim_next_for_embedding") as mock_claim:
                with patch("intelligence_layer.embedding.asyncio.sleep", side_effect=fake_sleep):
                    with pytest.raises(asyncio.CancelledError):
                        await _embedding_worker(mock_pool, worker_id=0)

    assert len(sleep_calls) == 1
    mock_claim.assert_not_called()


# ── Test 2: claim returns None when queue is empty ─────────────────────────────


@pytest.mark.asyncio
async def test_claim_next_returns_none_when_empty():
    """No pending rows → claim_next_for_embedding returns None without UPDATE."""
    mock_pool, mock_conn = make_mock_pool(fetchrow_return=None)

    result = await claim_next_for_embedding(mock_pool)

    assert result is None
    # We must NOT have issued the UPDATE marker if the SELECT found nothing.
    mock_conn.execute.assert_not_called()


# ── Test 3: SMS happy path ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_embed_event_sms_happy_path():
    """
    SMS event: builds Summary + Message content, calls OpenAI, persists
    embedding, flips embedding_status to 'completed'.
    """
    mock_pool, mock_conn = make_mock_pool()

    embedding_response = make_embedding_response()
    captured_inputs = []

    def fake_create(**kwargs):
        captured_inputs.append(kwargs)
        return embedding_response

    event = {
        "enrichment_id": FAKE_ENRICHMENT_ID,
        "comm_event_id": FAKE_COMM_EVENT_ID,
        "summary": "Customer needs help with their order.",
        "event_type": "sms.received",
        "raw_payload": {"Body": "Hello, please help me with my order"},
        "transcript_text": None,
    }

    with patch("intelligence_layer.embedding.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"

        with patch("intelligence_layer.embedding.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.embeddings.create.side_effect = fake_create
            mock_openai_cls.return_value = mock_client

            await embed_event(mock_pool, event)

    # OpenAI was called once with content containing summary + body
    assert len(captured_inputs) == 1
    content_sent = captured_inputs[0]["input"]
    assert content_sent.startswith("Summary: Customer needs help")
    assert "Message: Hello, please help me with my order" in content_sent

    # Both DB writes happened: INSERT into embeddings + UPDATE enrichments
    assert mock_conn.execute.call_count == 2
    insert_sql = mock_conn.execute.call_args_list[0][0][0]
    assert "INSERT INTO embeddings" in insert_sql
    update_sql = mock_conn.execute.call_args_list[1][0][0]
    assert "UPDATE enrichments" in update_sql
    assert "completed" in update_sql


# ── Test 4: Voice happy path ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_embed_event_voice_uses_transcript_not_payload():
    """
    For recording.ready, the embedded content MUST come from transcript_text
    and MUST NOT include anything from raw_payload (like the RecordingUrl).
    """
    mock_pool, mock_conn = make_mock_pool()

    captured_inputs = []

    def fake_create(**kwargs):
        captured_inputs.append(kwargs)
        return make_embedding_response()

    event = {
        "enrichment_id": FAKE_ENRICHMENT_ID,
        "comm_event_id": FAKE_COMM_EVENT_ID,
        "summary": "Customer wants to cancel.",
        "event_type": "recording.ready",
        "raw_payload": {"RecordingUrl": "https://api.twilio.com/RE123"},
        "transcript_text": "Hi I would like to cancel my subscription please",
    }

    with patch("intelligence_layer.embedding.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"

        with patch("intelligence_layer.embedding.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.embeddings.create.side_effect = fake_create
            mock_openai_cls.return_value = mock_client

            await embed_event(mock_pool, event)

    content_sent = captured_inputs[0]["input"]
    assert "Transcript: Hi I would like to cancel" in content_sent
    # The raw recording URL must NOT leak into the embedding payload
    assert "RecordingUrl" not in content_sent
    assert "api.twilio.com" not in content_sent


# ── Test 5: OpenAI fails all retries → marks embedding_status='failed' ─────────


@pytest.mark.asyncio
async def test_embed_event_marks_failed_when_openai_fails_all_retries():
    """
    OpenAI raises 3 times → enrichment row marked embedding_status='failed'
    and NO INSERT into embeddings.
    """
    mock_pool, mock_conn = make_mock_pool()

    event = {
        "enrichment_id": FAKE_ENRICHMENT_ID,
        "comm_event_id": FAKE_COMM_EVENT_ID,
        "summary": "A summary.",
        "event_type": "sms.received",
        "raw_payload": {"Body": "hi"},
        "transcript_text": None,
    }

    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    with patch("intelligence_layer.embedding.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.OPENAI_API_KEY = "sk-fake"
        mock_settings.EMBEDDING_MODEL = "text-embedding-3-small"

        with patch("intelligence_layer.embedding.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_client.embeddings.create.side_effect = RuntimeError("API error")
            mock_openai_cls.return_value = mock_client

            with patch("intelligence_layer.embedding.time.sleep", side_effect=fake_sleep):
                await embed_event(mock_pool, event)

    # 3 attempts total, 2 sleeps between them
    assert mock_client.embeddings.create.call_count == 3
    assert len(sleep_calls) == 2

    # Exactly ONE DB write — the UPDATE to embedding_status='failed'.
    # No INSERT into embeddings since we never got a vector.
    assert mock_conn.execute.call_count == 1
    failed_sql = mock_conn.execute.call_args_list[0][0][0]
    assert "UPDATE enrichments" in failed_sql
    assert "failed" in failed_sql
