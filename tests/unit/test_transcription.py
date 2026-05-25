"""
Unit tests for comm_layer/transcription.py.

What we test:
1. AI kill switch: when AI_ENABLED=False, transcribe_recording returns early
   without making any network calls.
2. Happy path: audio is downloaded, Whisper returns a transcript with words,
   the result is written to the transcripts table.
3. Exception in Whisper: error is logged and NOT re-raised (background tasks
   must never propagate exceptions — they would be silently swallowed by the
   event loop anyway, but we log them explicitly so ops can see the failure).
4. Empty response from Whisper: no speech detected — null text and empty
   segments are written to DB so the row still exists and Phase 7 won't
   re-trigger transcription for the same recording.

We mock httpx, OpenAI, and supabase so no real network calls are made.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from comm_layer.transcription import transcribe_recording

# ── Helpers ────────────────────────────────────────────────────────────────────

FAKE_CALL_EVENT_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
FAKE_RECORDING_URL = "https://api.twilio.com/2010-04-01/Accounts/AC123/Recordings/RE123"
FAKE_RECORDING_SID = "RE123"


def make_whisper_response(
    text: str | None = "Hello world",
    words: list[dict] | None = None,
) -> SimpleNamespace:
    """
    Build a mock Whisper verbose_json response with the structure that
    _run_whisper_sync reads: .text (str) and .words (list of word objects).
    """
    if words is None:
        words = [
            {"word": "Hello", "start": 0.0, "end": 0.5},
            {"word": "world", "start": 0.6, "end": 1.0},
        ]
    mock_words = [
        SimpleNamespace(word=w["word"], start=w["start"], end=w["end"])
        for w in words
    ]
    return SimpleNamespace(text=text, words=mock_words)


def make_mock_supabase():
    """Build a mock Supabase client that records the insert call."""
    mock_execute = AsyncMock(return_value=None)
    mock_insert = MagicMock()
    mock_insert.execute = mock_execute
    mock_table = MagicMock()
    mock_table.insert = MagicMock(return_value=mock_insert)
    mock_supabase = MagicMock()
    mock_supabase.table = MagicMock(return_value=mock_table)
    return mock_supabase, mock_execute, mock_table


def make_mock_http_client(audio_content: bytes = b"fake_audio_bytes") -> AsyncMock:
    """Build an httpx async client mock that returns the given audio bytes."""
    mock_response = MagicMock()
    mock_response.content = audio_content
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    return mock_client


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_transcription_skipped_when_ai_disabled():
    """
    When AI_ENABLED=False (the default in tests), transcribe_recording must
    return immediately without calling httpx or OpenAI.
    This proves the AI kill switch works at the transcription boundary.
    """
    mock_supabase, mock_execute, _ = make_mock_supabase()

    # AI_ENABLED is already False in conftest.py — no patching needed.
    with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx:
        with patch("comm_layer.transcription.OpenAI") as mock_openai:
            await transcribe_recording(
                supabase=mock_supabase,
                call_event_id=FAKE_CALL_EVENT_ID,
                recording_url=FAKE_RECORDING_URL,
                recording_sid=FAKE_RECORDING_SID,
            )

    # Neither httpx nor OpenAI should have been touched.
    mock_httpx.assert_not_called()
    mock_openai.assert_not_called()
    mock_execute.assert_not_called()


@pytest.mark.asyncio
async def test_transcription_happy_path():
    """
    With AI_ENABLED=True, a successful transcription downloads audio, calls
    Whisper, and inserts a row into the transcripts table with the transcript
    text and word-level segments.
    """
    mock_supabase, mock_execute, mock_table = make_mock_supabase()
    whisper_response = make_whisper_response(
        text="Hello world",
        words=[
            {"word": "Hello", "start": 0.0, "end": 0.5},
            {"word": "world", "start": 0.6, "end": 1.0},
        ],
    )
    mock_http_client = make_mock_http_client()

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.OpenAI") as mock_openai_cls:
                mock_openai_instance = MagicMock()
                mock_openai_instance.audio.transcriptions.create.return_value = whisper_response
                mock_openai_cls.return_value = mock_openai_instance

                await transcribe_recording(
                    supabase=mock_supabase,
                    call_event_id=FAKE_CALL_EVENT_ID,
                    recording_url=FAKE_RECORDING_URL,
                    recording_sid=FAKE_RECORDING_SID,
                )

    # Audio was downloaded from the right URL (with .mp3 appended)
    mock_http_client.get.assert_called_once()
    called_url = mock_http_client.get.call_args[0][0]
    assert called_url == FAKE_RECORDING_URL + ".mp3"

    # Transcript was inserted with correct values
    mock_execute.assert_called_once()
    inserted = mock_table.insert.call_args[0][0]
    assert inserted["comm_event_id"] == str(FAKE_CALL_EVENT_ID)
    assert inserted["text"] == "Hello world"
    assert inserted["source"] == "batch"
    assert inserted["is_partial"] is False
    assert len(inserted["segments"]) == 2
    assert inserted["segments"][0]["text"] == "Hello"
    assert inserted["segments"][0]["start_ms"] == 0
    assert inserted["segments"][1]["text"] == "world"
    assert inserted["segments"][1]["start_ms"] == 600


@pytest.mark.asyncio
async def test_transcription_exception_is_logged_not_raised():
    """
    If Whisper raises an exception, transcribe_recording logs it and returns
    cleanly — it must NOT propagate the exception. An uncaught exception in a
    detached asyncio.Task is effectively swallowed by the event loop, but we
    log it explicitly so failures appear in structured logs and monitoring.
    """
    mock_supabase, mock_execute, _ = make_mock_supabase()
    mock_http_client = make_mock_http_client()

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.OpenAI") as mock_openai_cls:
                mock_openai_instance = MagicMock()
                mock_openai_instance.audio.transcriptions.create.side_effect = RuntimeError(
                    "Whisper API error"
                )
                mock_openai_cls.return_value = mock_openai_instance

                # Must NOT raise — exception is absorbed inside transcribe_recording
                await transcribe_recording(
                    supabase=mock_supabase,
                    call_event_id=FAKE_CALL_EVENT_ID,
                    recording_url=FAKE_RECORDING_URL,
                    recording_sid=FAKE_RECORDING_SID,
                )

    # Supabase insert should not have been called if Whisper failed
    mock_execute.assert_not_called()


@pytest.mark.asyncio
async def test_transcription_empty_whisper_response_writes_null_text():
    """
    When Whisper returns empty text and no words (e.g. completely silent audio),
    we write a transcript row with null text and empty segments. This ensures
    the row still exists so Phase 7 enrichment knows transcription was attempted
    — it won't re-trigger for the same recording.
    """
    mock_supabase, mock_execute, mock_table = make_mock_supabase()
    empty_response = SimpleNamespace(text=None, words=[])
    mock_http_client = make_mock_http_client(b"silent_audio")

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.OPENAI_API_KEY = "sk-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.OpenAI") as mock_openai_cls:
                mock_openai_instance = MagicMock()
                mock_openai_instance.audio.transcriptions.create.return_value = empty_response
                mock_openai_cls.return_value = mock_openai_instance

                await transcribe_recording(
                    supabase=mock_supabase,
                    call_event_id=FAKE_CALL_EVENT_ID,
                    recording_url=FAKE_RECORDING_URL,
                    recording_sid=FAKE_RECORDING_SID,
                )

    # Row is still written — null text, empty segments
    mock_execute.assert_called_once()
    inserted = mock_table.insert.call_args[0][0]
    assert inserted["text"] is None
    assert inserted["segments"] == []
    assert inserted["is_partial"] is False
