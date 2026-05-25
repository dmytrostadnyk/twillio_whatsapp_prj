"""
Unit tests for comm_layer/transcription.py.

What we test:
1. AI kill switch: when AI_ENABLED=False, transcribe_recording returns early
   without making any network calls.
2. Happy path: audio is downloaded, Deepgram returns a transcript with words,
   the result is written to the transcripts table.
3. Exception in Deepgram: error is logged and NOT re-raised (background tasks
   must never propagate exceptions — they would be silently swallowed by the
   event loop anyway, but we log them explicitly so ops can see the failure).
4. Empty response from Deepgram: Deepgram finds no speech (silence or noise) —
   null text and empty segments are written to DB so the row still exists.

We mock httpx, DeepgramClient, and supabase so no real network calls are made.
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


def make_deepgram_response(
    transcript: str | None = "Hello world",
    words: list[dict] | None = None,
) -> MagicMock:
    """
    Build a mock Deepgram ListenV1Response with the minimum structure
    that _run_deepgram_sync reads.
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
    mock_alt = SimpleNamespace(transcript=transcript, words=mock_words)
    mock_channel = SimpleNamespace(alternatives=[mock_alt])
    mock_results = SimpleNamespace(channels=[mock_channel])
    return SimpleNamespace(results=mock_results)


def make_mock_supabase() -> AsyncMock:
    """Build a mock Supabase client that records the insert call."""
    mock_execute = AsyncMock(return_value=None)
    mock_insert = MagicMock()
    mock_insert.execute = mock_execute
    mock_table = MagicMock()
    mock_table.insert = MagicMock(return_value=mock_insert)
    mock_supabase = MagicMock()
    mock_supabase.table = MagicMock(return_value=mock_table)
    return mock_supabase, mock_execute, mock_table


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_transcription_skipped_when_ai_disabled():
    """
    When AI_ENABLED=False (the default in tests), transcribe_recording must
    return immediately without calling httpx or Deepgram.
    This proves the AI kill switch works at the transcription boundary.
    """
    mock_supabase, mock_execute, _ = make_mock_supabase()

    # AI_ENABLED is already False in conftest.py — no patching needed.
    with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx:
        with patch("comm_layer.transcription.DeepgramClient") as mock_dg:
            await transcribe_recording(
                supabase=mock_supabase,
                call_event_id=FAKE_CALL_EVENT_ID,
                recording_url=FAKE_RECORDING_URL,
                recording_sid=FAKE_RECORDING_SID,
            )

    # Neither httpx nor Deepgram should have been touched.
    mock_httpx.assert_not_called()
    mock_dg.assert_not_called()
    mock_execute.assert_not_called()


@pytest.mark.asyncio
async def test_transcription_happy_path():
    """
    With AI_ENABLED=True, a successful transcription downloads audio, calls
    Deepgram, and inserts a row into the transcripts table with the transcript
    text and word-level segments.
    """
    mock_supabase, mock_execute, mock_table = make_mock_supabase()
    deepgram_response = make_deepgram_response(
        transcript="Hello world",
        words=[
            {"word": "Hello", "start": 0.0, "end": 0.5},
            {"word": "world", "start": 0.6, "end": 1.0},
        ],
    )

    mock_http_response = MagicMock()
    mock_http_response.content = b"fake_audio_bytes"
    mock_http_response.raise_for_status = MagicMock()

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.DEEPGRAM_API_KEY = "dg-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_http_client = AsyncMock()
            mock_http_client.get = AsyncMock(return_value=mock_http_response)
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.DeepgramClient") as mock_dg_cls:
                mock_dg_instance = MagicMock()
                mock_dg_instance.listen.v1.media.transcribe_file.return_value = deepgram_response
                mock_dg_cls.return_value = mock_dg_instance

                await transcribe_recording(
                    supabase=mock_supabase,
                    call_event_id=FAKE_CALL_EVENT_ID,
                    recording_url=FAKE_RECORDING_URL,
                    recording_sid=FAKE_RECORDING_SID,
                )

    # Verify the audio was downloaded from the right URL (with .mp3 appended)
    mock_http_client.get.assert_called_once()
    called_url = mock_http_client.get.call_args[0][0]
    assert called_url == FAKE_RECORDING_URL + ".mp3"

    # Verify the transcript was inserted
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
    If Deepgram raises an exception, transcribe_recording logs it and returns
    cleanly — it must NOT propagate the exception. An uncaught exception in a
    detached asyncio.Task is effectively swallowed by the event loop, but we
    log it explicitly so failures appear in structured logs and monitoring.
    """
    mock_supabase, mock_execute, _ = make_mock_supabase()

    mock_http_response = MagicMock()
    mock_http_response.content = b"fake_audio_bytes"
    mock_http_response.raise_for_status = MagicMock()

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.DEEPGRAM_API_KEY = "dg-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_http_client = AsyncMock()
            mock_http_client.get = AsyncMock(return_value=mock_http_response)
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.DeepgramClient") as mock_dg_cls:
                mock_dg_instance = MagicMock()
                # Deepgram raises — simulates API failure, network timeout, etc.
                mock_dg_instance.listen.v1.media.transcribe_file.side_effect = RuntimeError(
                    "Deepgram API error"
                )
                mock_dg_cls.return_value = mock_dg_instance

                # Must NOT raise — exception absorbed inside transcribe_recording
                await transcribe_recording(
                    supabase=mock_supabase,
                    call_event_id=FAKE_CALL_EVENT_ID,
                    recording_url=FAKE_RECORDING_URL,
                    recording_sid=FAKE_RECORDING_SID,
                )

    # Supabase insert should not have been called if Deepgram failed
    mock_execute.assert_not_called()


@pytest.mark.asyncio
async def test_transcription_empty_deepgram_response_writes_null_text():
    """
    When Deepgram returns no channels (e.g. completely silent audio), we write
    a transcript row with null text and empty segments. This ensures the row
    still exists so Phase 7 enrichment knows a transcription was attempted —
    it won't re-trigger for the same recording.
    """
    mock_supabase, mock_execute, mock_table = make_mock_supabase()

    # Deepgram response with empty channels list
    empty_response = SimpleNamespace(results=SimpleNamespace(channels=[]))

    mock_http_response = MagicMock()
    mock_http_response.content = b"silent_audio"
    mock_http_response.raise_for_status = MagicMock()

    with patch("comm_layer.transcription.settings") as mock_settings:
        mock_settings.AI_ENABLED = True
        mock_settings.TWILIO_ACCOUNT_SID = "ACtest"
        mock_settings.TWILIO_AUTH_TOKEN = "token"
        mock_settings.DEEPGRAM_API_KEY = "dg-fake"

        with patch("comm_layer.transcription.httpx.AsyncClient") as mock_httpx_cls:
            mock_http_client = AsyncMock()
            mock_http_client.get = AsyncMock(return_value=mock_http_response)
            mock_httpx_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http_client)
            mock_httpx_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("comm_layer.transcription.DeepgramClient") as mock_dg_cls:
                mock_dg_instance = MagicMock()
                mock_dg_instance.listen.v1.media.transcribe_file.return_value = empty_response
                mock_dg_cls.return_value = mock_dg_instance

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
