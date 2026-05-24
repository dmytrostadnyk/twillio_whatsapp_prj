"""
Shared TwiML response constants for webhook handlers.

WHY a separate module: every channel returns a small fixed XML string in the
common case. Defining them once here means the XML format is consistent across
all handlers and we don't have three copies drifting apart over time.
"""

from __future__ import annotations

# Empty TwiML — tells Twilio "received, no auto-reply needed."
# Used for SMS, WhatsApp, and all status callbacks.
EMPTY_TWIML = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'

# Greeting TwiML for inbound voice calls.
# Phase 5 will replace this with a <Record> verb so we can capture audio.
VOICE_GREETING_TWIML = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="alice">Thank you for calling. Please hold while we connect you.</Say>
</Response>"""
