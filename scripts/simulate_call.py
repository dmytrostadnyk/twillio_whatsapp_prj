"""
Simulate a full inbound voice call without a voice-capable Twilio number.

WHY this script exists:
Lithuanian (and many non-US) Twilio numbers do not support voice calls.
Rather than buying an additional number, this script fakes the three HTTP
webhooks Twilio would send in a real call:

  1. POST /webhooks/voice         — call connected, app responds with TwiML
  2. POST /webhooks/voice/status  — call ended (completed)
  3. POST /webhooks/voice/recording — recording is ready, triggers Whisper

It computes a REAL Twilio HMAC-SHA1 signature using your actual auth token,
so your server's signature validation passes exactly as it would for a genuine
Twilio request. No code changes needed.

HOW TO USE:
  1. Record a short voice message and save it as:
         tests/fixtures/sample_call.mp3

     Quickest way on Mac (no extra tools needed):
       a. Open QuickTime Player → File → New Audio Recording
       b. Click the red button, say your demo message, stop
       c. File → Export As → Audio Only  (saves as .m4a)
       d. Rename the file to sample_call.mp3 and move it to tests/fixtures/
          (Whisper accepts M4A content even with an .mp3 name)

     If you have ffmpeg installed:
       say -o /tmp/demo.aiff "I need to cancel my subscription. I was charged twice."
       ffmpeg -i /tmp/demo.aiff tests/fixtures/sample_call.mp3 -y

  2. Start three services in separate terminals:
       uvicorn comm_layer.main:app --host 0.0.0.0 --port 8000 --reload
       make intel
       make worker

  3. Run this script:
       python scripts/simulate_call.py

  4. Wait ~30 seconds, then check:
       Supabase comm_events  → 3 rows (call.started, call.completed, recording.ready)
       Supabase transcripts  → Whisper transcript text
       Supabase enrichments  → GPT-4o intent / sentiment / summary
       HubSpot               → contact updated with AI fields, Note on timeline,
                               Ticket created if complaint/negative sentiment
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.server
import secrets
import sys
import threading
import time
from pathlib import Path

import requests

# Import settings so we get values from .env automatically
# (the venv must be active — same one you use for make worker / make intel)
try:
    from comm_layer.config import settings
except ImportError:
    print("ERROR: comm_layer not importable. Activate your venv first:")
    print("  source .venv/bin/activate")
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────

# Where your FastAPI server is listening (do NOT use the ngrok URL here;
# we post directly to localhost to avoid the extra network hop)
LOCAL_APP_URL = "http://localhost:8000"

# Port we use to serve the sample MP3 so the transcription code can download it
FILE_SERVER_PORT = 8002

# Fake caller number — not a real number, just needs to look like E.164
FAKE_CALLER = "+15550000001"

# ── Helpers ────────────────────────────────────────────────────────────────────


def _compute_signature(auth_token: str, url: str, params: dict[str, str]) -> str:
    """
    Compute the X-Twilio-Signature value for a fake webhook POST.

    Twilio's algorithm:
      1. Start with the full URL (scheme + host + path)
      2. Sort POST params by key, concatenate key+value pairs
      3. HMAC-SHA1 the result with your auth token
      4. Base64-encode the digest

    We must sign against PUBLIC_BASE_URL (the ngrok URL) because that is
    what the server reconstructs internally in twilio_security.py —
    even though we POST to localhost.
    """
    s = url
    for key in sorted(params.keys()):
        s += key + params[key]
    digest = hmac.new(auth_token.encode(), s.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def _post_webhook(path: str, params: dict[str, str]) -> int:
    """POST one fake webhook with a valid Twilio signature. Returns HTTP status."""
    # The server reconstructs the URL as PUBLIC_BASE_URL + path, so we sign that.
    signed_url = f"{settings.PUBLIC_BASE_URL.rstrip('/')}{path}"
    sig = _compute_signature(settings.TWILIO_AUTH_TOKEN, signed_url, params)

    response = requests.post(
        f"{LOCAL_APP_URL}{path}",
        data=params,
        headers={"X-Twilio-Signature": sig},
        timeout=10,
    )
    status = response.status_code
    ok = "OK" if status == 200 else f"UNEXPECTED — {response.text[:200]}"
    print(f"    POST {path} → {status} {ok}")
    return status


def _start_file_server(directory: str, port: int) -> None:
    """
    Serve files from `directory` on a background thread.

    WHY we need this:
    transcription.py downloads the recording with:
        client.get(recording_url + ".mp3", auth=(...))
    We tell it RecordingUrl = "http://localhost:{port}/sample_call",
    so it fetches "http://localhost:{port}/sample_call.mp3" — which this
    server delivers from tests/fixtures/sample_call.mp3.

    The auth header is ignored by a plain HTTP server, so Basic Auth
    (AccountSid:AuthToken) doesn't cause any problem here.
    """
    class _QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def log_message(self, *args):
            pass  # suppress per-request logs

    httpd = http.server.HTTPServer(("localhost", port), _QuietHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()


# ── Main simulation ────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate an inbound Twilio voice call.")
    parser.add_argument(
        "--audio",
        default="tests/fixtures/sample_call.mp3",
        help="Path to MP3 recording (default: tests/fixtures/sample_call.mp3)",
    )
    args = parser.parse_args()

    fixture = Path(args.audio)
    if not fixture.exists():
        print("\nERROR: Missing audio file.")
        print(f"  Expected: {fixture.resolve()}")
        print("\nQuickest way to create it on Mac (no extra tools):")
        print("  1. Open QuickTime Player → File → New Audio Recording")
        print("  2. Record your demo message, then stop")
        print("  3. File → Export As → Audio Only  (saves as .m4a)")
        print("  4. Rename the .m4a to sample_call.mp3 and move it to tests/fixtures/")
        print("\nOr, with ffmpeg:")
        print("  say -o /tmp/demo.aiff 'I need to cancel my subscription.'")
        print("  ffmpeg -i /tmp/demo.aiff tests/fixtures/sample_call.mp3 -y")
        sys.exit(1)

    print("\n=== Voice Call Simulation ===")
    print(f"  App URL       : {LOCAL_APP_URL}")
    print(f"  Public URL    : {settings.PUBLIC_BASE_URL}")
    print(f"  Twilio number : {settings.TWILIO_PHONE_NUMBER}")
    print(f"  Audio file    : {fixture}")
    print()

    # Serve the MP3 on a background thread so transcription can download it.
    _start_file_server(str(fixture.parent), FILE_SERVER_PORT)
    recording_url = f"http://localhost:{FILE_SERVER_PORT}/{fixture.stem}"

    # Random SIDs so each simulation run is treated as a fresh call by the
    # idempotency guard (which deduplicates on event_key = SID + event_type).
    call_sid = "CA" + secrets.token_hex(16)
    recording_sid = "RE" + secrets.token_hex(16)

    # Common fields Twilio always includes on voice webhooks
    base_voice = {
        "AccountSid": settings.TWILIO_ACCOUNT_SID,
        "CallSid": call_sid,
        "From": FAKE_CALLER,
        "To": settings.TWILIO_PHONE_NUMBER,
        "Direction": "inbound",
    }

    # Step 1: Someone calls — Twilio posts to /webhooks/voice
    print("Step 1/3  Inbound call arrives...")
    _post_webhook("/webhooks/voice", {**base_voice, "CallStatus": "ringing"})

    time.sleep(1)

    # Step 2: Call ends — Twilio posts to /webhooks/voice/status
    print("Step 2/3  Call ends (completed)...")
    _post_webhook("/webhooks/voice/status", {**base_voice, "CallStatus": "completed"})

    time.sleep(2)

    # Step 3: Recording ready — Twilio posts to /webhooks/voice/recording
    # This triggers the background Whisper transcription task.
    print("Step 3/3  Recording is ready — transcription will start now...")
    _post_webhook("/webhooks/voice/recording", {
        "AccountSid": settings.TWILIO_ACCOUNT_SID,
        "CallSid": call_sid,
        "RecordingSid": recording_sid,
        "RecordingStatus": "completed",
        "RecordingUrl": recording_url,
        "RecordingDuration": "15",
    })

    print()
    print("Done. Transcription is running in the background inside your uvicorn process.")
    print("Wait ~30 seconds, then check Supabase:")
    print("  comm_events  → 3 rows (call.started, call.completed, recording.ready)")
    print("  transcripts  → Whisper transcript text")
    print("  enrichments  → GPT-4o intent / sentiment / summary")
    print("  delivery_log → delivery attempt logged by the worker")
    print()
    print("Keeping file server alive for 60 seconds so transcription can download...")

    # Stay alive long enough for the background transcription task to pull the file.
    time.sleep(60)
    print("File server stopped. Simulation complete.")


if __name__ == "__main__":
    main()
