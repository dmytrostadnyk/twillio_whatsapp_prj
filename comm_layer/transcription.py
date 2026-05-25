"""
Batch transcription via Deepgram.

Triggered as a background task from the recording-ready webhook handler after
Twilio confirms a recording is encoded and accessible. The flow is:

  1. Download the audio file from Twilio (authenticated with Basic Auth).
  2. Send raw bytes to Deepgram's prerecorded transcription API (sync, so we
     run it in a thread pool to avoid blocking the async event loop).
  3. Write the transcript and word-level segments to the transcripts table.

WHY a background task and not inline in the webhook handler:
The recording-ready webhook must return 200 to Twilio within a few seconds or
Twilio retries — producing duplicate events. Transcription of a real call can
take several seconds. Detaching it as an asyncio.Task keeps the webhook fast.

WHY we never re-raise exceptions:
This runs as a detached task. An uncaught exception would silently kill the
task with no retry path. We log and absorb, accepting occasional lost
transcripts. The Phase 7 enrichment worker checks for the transcript before
running — no downstream crash if one is missing.

WHY asyncio.to_thread for Deepgram:
The Deepgram SDK v7 (Fern-generated) uses a synchronous HTTP client. Calling
a blocking function directly inside a coroutine freezes every other coroutine
running on the same event loop. to_thread() offloads it to the thread pool.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import structlog
from deepgram import DeepgramClient
from supabase import AsyncClient

from comm_layer.config import settings

log = structlog.get_logger(__name__)


async def transcribe_recording(
    supabase: AsyncClient,
    call_event_id: uuid.UUID,
    recording_url: str,
    recording_sid: str,
) -> None:
    """Download a Twilio recording and write its Deepgram transcript to the DB."""
    if not settings.AI_ENABLED:
        log.info("transcription.skipped", reason="ai_disabled", recording_sid=recording_sid)
        return
    try:
        audio_bytes = await _download_audio(recording_url)
        segments, full_text = await asyncio.to_thread(_run_deepgram_sync, audio_bytes)
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


def _run_deepgram_sync(audio_bytes: bytes) -> tuple[list[dict], str | None]:
    """
    Send audio bytes to Deepgram and return (word-level segments, full transcript).

    WHY sync and not async:
    The Deepgram SDK v7 uses a synchronous HTTP client (no async variant).
    The caller wraps this in asyncio.to_thread() to avoid blocking the loop.

    WHY nova-2:
    Deepgram's most accurate general-purpose model at the time this was
    written. nova-3 exists but nova-2 has broader stability and language
    support for voicemail/call use cases.
    """
    dg = DeepgramClient(api_key=settings.DEEPGRAM_API_KEY)
    response = dg.listen.v1.media.transcribe_file(
        request=audio_bytes,
        model="nova-2",
        smart_format=True,
        punctuate=True,
        language="en-US",
    )
    channels = response.results.channels if response.results else []
    if not channels:
        return [], None
    alts = channels[0].alternatives or []
    if not alts:
        return [], None
    alt = alts[0]
    words = alt.words or []
    segments = [
        {
            "text": w.word,
            "start_ms": int((w.start or 0.0) * 1000),
            "end_ms": int((w.end or 0.0) * 1000),
        }
        for w in words
        if w.word  # skip any None-word entries Deepgram might return
    ]
    return segments, alt.transcript or None


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
