"""
Batch transcription via OpenAI Whisper — intelligence layer variant.

Transcription now lives in the intelligence layer (not the webhook tier) so it:
  - Runs inside the enrichment worker's crash-isolated poll loop.
  - Inherits the enrichment lease (Batch B) for crash recovery — a stuck
    'processing' row is re-claimed and re-transcribed after the lease expires.
  - Never blocks the webhook tier (fast-ack is preserved; the webhook only
    persists recording.ready and returns 200 immediately).

Call flow:
  recording.ready event lands →
  enrichment worker claims it →
  transcribe_recording_for_event() downloads + Whispers + writes to DB →
  enrich_event() runs GPT-4o on the transcript text →
  enrichments row moves to 'completed' →
  delivery worker delivers to HubSpot.

WHY we store the transcript on the recording.ready comm_event_id (not call.started):
  - Simplifies every downstream join — no correlation-id bridge needed.
  - The enrichment worker already has the recording.ready id when it claims work.
  - Transcripts are only needed for enrichment and embedding; both now select
    transcripts directly by the claiming event's id.

WHY asyncio.to_thread for the Whisper call:
  The OpenAI Python SDK uses a synchronous HTTP client. Calling it directly
  inside a coroutine would freeze the event loop. to_thread() offloads it.

WHY we write transcripts via asyncpg (pool) instead of the supabase-py client:
  Keeps all write paths for enrichment-related data on one transport, making
  failure modes consistent and enabling future shared-transaction patterns.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import structlog
from openai import OpenAI

from comm_layer.config import settings
from comm_layer.db import ai_enabled

log = structlog.get_logger(__name__)


async def transcribe_recording_for_event(
    pool,
    recording_event_id: uuid.UUID,
    recording_url: str,
    recording_sid: str,
) -> str | None:
    """
    Download a Twilio recording, transcribe with Whisper, and persist the result.

    Stores the transcript with comm_event_id = recording_event_id (the
    recording.ready event id) so downstream queries can join directly without
    a correlation-id bridge.

    Returns the full transcript text, or None if transcription fails.
    Never raises — failure is logged and absorbed so the enrichment worker can
    write status='failed' and let the delivery gate handle the skipped transcript.
    """
    if not await ai_enabled(pool):
        log.info(
            "transcription.skipped",
            reason="ai_disabled",
            recording_sid=recording_sid,
        )
        return None
    try:
        audio_bytes = await _download_audio(recording_url)
        segments, full_text = await asyncio.to_thread(_run_whisper_sync, audio_bytes)
        await _save_transcript_asyncpg(pool, recording_event_id, segments, full_text)
        log.info(
            "transcription.complete",
            recording_sid=recording_sid,
            text_length=len(full_text or ""),
        )
        return full_text
    except Exception:
        log.exception("transcription.failed", recording_sid=recording_sid)
        return None


async def _download_audio(url: str) -> bytes:
    """
    Download the recording MP3 from Twilio.

    WHY we append .mp3:
    RecordingUrl from Twilio has no file extension. Appending .mp3 tells
    Twilio to serve the audio in MP3 format, which Whisper accepts directly.

    WHY Basic Auth:
    With "Secure Media" enabled in the Twilio Console, every download requires
    Basic Auth using AccountSid:AuthToken. Without it anyone with the URL can
    download the audio — a GDPR risk for call recordings.
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
    One blocking OpenAI Whisper call. Returns (word-level segments, full text).

    Intended to be called via asyncio.to_thread() from async callers.
    verbose_json + timestamp_granularities=["word"] gives word-level timestamps
    that the dashboard uses for call playback alignment.
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


async def _save_transcript_asyncpg(
    pool,
    recording_event_id: uuid.UUID,
    segments: list[dict],
    full_text: str | None,
) -> None:
    """Write the transcript row to the DB via asyncpg (consistent with enrichment writes)."""
    import json as _json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transcripts (comm_event_id, text, language, segments, source, is_partial)
            VALUES ($1, $2, 'en-US', $3::jsonb, 'batch', FALSE)
            ON CONFLICT DO NOTHING
            """,
            recording_event_id,
            full_text,
            _json.dumps(segments),
        )
