"""
Shared TwiML response constants and builders for webhook handlers.

WHY a separate module: every channel returns a small fixed XML string in the
common case. Defining them once here means the XML format is consistent across
all handlers and we don't have three copies drifting apart over time.
"""

from __future__ import annotations

# Empty TwiML — tells Twilio "received, no auto-reply needed."
# Used for SMS, WhatsApp, and all status callbacks.
EMPTY_TWIML = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'


def make_voice_recording_twiml(base_url: str, max_length: int) -> str:
    """
    Returns TwiML that greets the caller and records the call.

    WHY recordingStatusCallback instead of action:
    The 'action' attribute fires DURING the call when recording stops (timeout
    or caller hangs up). 'recordingStatusCallback' fires ASYNCHRONOUSLY, several
    seconds after the call ends, once Twilio has finished encoding the audio file.
    That is the correct hook for "recording is ready to download." Without it,
    you have no reliable way to know when the file is actually accessible.

    WHY transcribe="false":
    Phase 6 adds Deepgram streaming transcription — significantly better than
    Twilio's built-in transcription for our use case. Disabling it here avoids
    paying Twilio for a transcript we won't use.

    IMPORTANT — Twilio account requirement:
    Enable "Secure Media" in the Twilio Console (Account Settings → Recordings
    → HTTP authentication for media). Without it, anyone with the RecordingUrl
    from the callback payload can download the audio without authentication.
    With it enabled, the URL requires HTTP Basic Auth (AccountSid:AuthToken).
    """
    callback_url = f"{base_url.rstrip('/')}/webhooks/voice/recording"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response>"
        '<Say voice="alice">Thank you for calling. Please leave a message after the tone.'
        " Press any key when finished.</Say>"
        f'<Record maxLength="{max_length}"'
        f' recordingStatusCallback="{callback_url}"'
        ' recordingStatusCallbackMethod="POST"'
        ' transcribe="false"/>'
        "</Response>"
    )
