"""
Batch transcription via OpenAI Whisper — webhook-tier variant (superseded).

NOTE: Transcription is no longer triggered from the webhook tier.
It is now handled by intelligence_layer/transcription.py, which runs inside
the enrichment worker's crash-isolated poll loop and inherits the enrichment
lease for crash recovery (migration 0010).

This module is kept because:
  - It may be referenced by tests that test the transcription helpers directly.
  - The _download_audio / _run_whisper_sync helpers are still valid utilities.

The public entry point transcribe_recording() is no longer called from status.py.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import structlog
from openai import OpenAI
from supabase import AsyncClient

from comm_layer.config import settings

log = structlog.get_logger(__name__)


async def transcribe_recording(
    supabase: AsyncClient,
    call_event_id: uuid.UUID,
    recording_url: str,
    recording_sid: str,
) -> None:
    """Download a Twilio recording and write its Whisper transcript to the DB."""
    if not settings.AI_ENABLED:
        log.info("transcription.skipped", reason="ai_disabled", recording_sid=recording_sid)
        return
    try:
        audio_bytes = await _download_audio(recording_url)
        segments, full_text = await asyncio.to_thread(_run_whisper_sync, audio_bytes)
        await _save_transcript(supabase, call_event_id, segments, full_text)
        log.info(
            "transcription.complete",
            recording_sid=recording_sid,
            text_length=len(full_text or ""),
        )
    except Exception:
        log.exception("transcription.failed", recording_sid=recording_sid)


async def _download_audio(url: str) -> bytes:
    """
    Download the recording MP3 from Twilio.

    WHY we append .mp3:
    RecordingUrl from Twilio has no file extension. Appending .mp3 tells
    Twilio to serve the audio in MP3 format, which Deepgram accepts directly.

    WHY Basic Auth:
    With "Secure Media" enabled in the Twilio Console (Account Settings →
    Recordings → HTTP authentication for media), every download requires
    Basic Auth using AccountSid:AuthToken. Without it anyone who obtains
    the URL can download the audio — a GDPR risk for call recordings.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url + ".mp3",
            auth=(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN),
            timeout=30.0,
        )
        response.raise_for_status()
        return response.content


def _run_whisper_sync(audio_bytes: bytes) -> tuple[list[dict], str | None]:
    """
    Send audio bytes to OpenAI Whisper and return (word-level segments, full transcript).

    WHY sync and not async:
    The default OpenAI Python SDK uses a synchronous HTTP client. The caller
    wraps this in asyncio.to_thread() to avoid blocking the event loop.

    WHY verbose_json with timestamp_granularities=["word"]:
    The plain "json" format only returns the full text string — no timestamps.
    verbose_json gives us word-level start/end times so we can build the
    segments list that the dashboard and enrichment layer use for alignment.

    WHY "recording.mp3" as the filename:
    The OpenAI API infers the audio format from the filename extension. Without
    it, the API rejects the upload. We pass the bytes as a (name, bytes, mime)
    tuple so the SDK sends the correct Content-Type without a real file on disk.
    """
    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.audio.transcriptions.create(
        model="whisper-1",
        file=("recording.mp3", audio_bytes, "audio/mpeg"),
        response_format="verbose_json",
        timestamp_granularities=["word"],
        language="en",
    )
    words = response.words or []
    segments = [
        {
            "text": w.word,
            "start_ms": int(w.start * 1000),
            "end_ms": int(w.end * 1000),
        }
        for w in words
    ]
    return segments, response.text or None


async def _save_transcript(
    supabase: AsyncClient,
    call_event_id: uuid.UUID,
    segments: list[dict],
    full_text: str | None,
) -> None:
    """Write the completed transcript row to the transcripts table."""
    await (
        supabase.table("transcripts")
        .insert(
            {
                "comm_event_id": str(call_event_id),
                "text": full_text,
                "language": "en-US",
                "segments": segments,
                "source": "batch",
                "is_partial": False,
            }
        )
        .execute()
    )
