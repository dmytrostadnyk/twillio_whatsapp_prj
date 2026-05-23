"""
Voice event contract models.

Three distinct events in a voice call lifecycle:
1. CallStartedEvent   — Twilio fires the webhook when the call begins.
2. CallCompletedEvent — Twilio fires the status callback when the call ends.
3. RecordingReadyEvent — Twilio fires a separate callback when the recording is ready.
                         This is NOT synchronous with the call ending — it arrives later.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from comm_layer.contracts.base import BaseCommEvent, Channel, Direction


class CallStartedEvent(BaseCommEvent):
    """
    Emitted when Twilio fires the initial voice webhook (call is ringing/answered).

    WHY a separate event from CallCompleted: the call lifecycle has two distinct
    moments — start and end — and consumers may want to react to each independently
    (e.g. start a live transcript immediately, then process the recording on completion).
    """

    channel: Channel = Channel.VOICE
    event_type: Literal["call.started"] = "call.started"

    call_sid: str = Field(..., description="Twilio Call SID (starts with CA)")
    from_number: str = Field(..., description="Caller's phone number (E.164)")
    to_number: str = Field(..., description="Dialled number (E.164)")
    call_status: str = Field(..., description="Twilio call status e.g. 'ringing', 'in-progress'")
    direction: Direction

    model_config = {"extra": "forbid", "use_enum_values": True}


class CallCompletedEvent(BaseCommEvent):
    """
    Emitted when Twilio fires the status callback after a call ends.

    NOTE: duration is in seconds as a string (how Twilio sends it).
    We keep it as a string to match the raw payload faithfully.
    """

    channel: Channel = Channel.VOICE
    event_type: Literal["call.completed"] = "call.completed"

    call_sid: str = Field(..., description="Twilio Call SID")
    from_number: str = Field(..., description="Caller's phone number (E.164)")
    to_number: str = Field(..., description="Dialled number (E.164)")
    call_status: str = Field(
        ..., description="Final call status e.g. 'completed', 'no-answer', 'busy', 'failed'"
    )
    duration: str | None = Field(None, description="Call duration in seconds (as string)")
    direction: Direction

    model_config = {"extra": "forbid", "use_enum_values": True}


class RecordingReadyEvent(BaseCommEvent):
    """
    Emitted when Twilio fires the recording-ready callback.

    IMPORTANT: This arrives AFTER the call ends — sometimes 30s, sometimes minutes later.
    Never assume a recording is available at call completion time.

    WHY we store a secured reference instead of a public URL:
    Twilio recording URLs are publicly accessible by default. We enable authenticated
    media access so the URL requires a valid Twilio credential to fetch.
    """

    channel: Channel = Channel.VOICE
    event_type: Literal["call.recording_ready"] = "call.recording_ready"
    direction: Direction = Direction.INBOUND

    call_sid: str = Field(..., description="The Call SID this recording belongs to")
    recording_sid: str = Field(..., description="Twilio Recording SID (starts with RE)")

    # We store the API path, not the public media URL, because:
    # - Public URLs are accessible without auth (security risk)
    # - The path + credentials lets us fetch the audio on demand
    recording_api_path: str = Field(
        ...,
        description=(
            "Twilio API path for the recording e.g. /2010-04-01/Accounts/{SID}/Recordings/{RE...}"
            " — fetch with Twilio client + auth, never expose as a public URL"
        ),
    )
    duration: str | None = Field(None, description="Recording duration in seconds")
    recording_status: str = Field(..., description="e.g. 'completed'")

    model_config = {"extra": "forbid", "use_enum_values": True}
