"""
Outbound Twilio helpers — send_sms, send_whatsapp, initiate_call.

These are the ONLY functions that should make outbound Twilio API calls.
All three run through a shared rate limiter to prevent runaway spending.

Usage (e.g. from the intelligence layer or a demo script):

    from twilio.rest import Client
    from comm_layer.outbound import send_sms

    client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
    sid = await send_sms(client, to="+15551234567", body="Hello!")

WHY the Twilio client is a parameter:
Tests pass a MagicMock. In production, the caller creates one client and
reuses it across calls (connection pooling). Instantiating inside each
function would re-parse env vars on every call and make mocking awkward.

WHY asyncio.get_running_loop().run_in_executor:
The Twilio Python SDK uses `requests` internally — it's fully synchronous.
Calling it directly inside an async function blocks the event loop for the
entire HTTP round-trip to Twilio's API. run_in_executor offloads the blocking
call to a thread so other coroutines keep running.

WHY rate_limiter is keyword-only with a None default:
Tests pass an explicit (full) bucket so they don't depend on the module-level
singleton. Production callers omit it and get the shared default.
"""

from __future__ import annotations

import asyncio

import asyncpg
import structlog
from twilio.rest import Client

from comm_layer.config import settings
from comm_layer.rate_limiter import RateLimitExceededError, TokenBucket  # noqa: F401 — re-export

log = structlog.get_logger(__name__)

# Shared rate limiter — all outbound calls (SMS, WhatsApp, Voice) draw from
# the same bucket to ensure the total outbound rate stays within the configured limit.
_rate_limiter = TokenBucket(
    capacity=settings.OUTBOUND_RATE_LIMIT_PER_MINUTE,
    refill_rate=settings.OUTBOUND_RATE_LIMIT_PER_MINUTE / 60.0,
)


class WindowExpiredError(Exception):
    """
    Raised by send_whatsapp when the 24-hour WhatsApp session window has expired
    and no template_body fallback was provided.

    WHY this window exists:
    WhatsApp Business API policy requires that free-form outbound messages are
    only sent within 24 hours of the last inbound message from that number. After
    the window closes, only pre-approved template messages are permitted. Violating
    this results in Twilio error 63016 — the message is rejected, not queued.

    Fix: provide a template_body to send_whatsapp() so the caller explicitly
    acknowledges the fallback, rather than silently truncating the message.
    """


async def check_whatsapp_window(pool: asyncpg.Pool, to_number: str) -> bool:
    """
    Return True if there was an inbound WhatsApp message from to_number
    within the last 24 hours (i.e. the session window is open).

    to_number may include or omit the "whatsapp:" prefix — we normalise before
    querying because comm_events stores the prefix-inclusive form from Twilio.
    """
    # Twilio sends from_number as "whatsapp:+15551234567" so the DB stores that form
    wa_number = to_number if to_number.startswith("whatsapp:") else f"whatsapp:{to_number}"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT 1 FROM comm_events
            WHERE channel    = 'whatsapp'
              AND direction  = 'inbound'
              AND from_number = $1
              AND created_at > NOW() - INTERVAL '24 hours'
            LIMIT 1
            """,
            wa_number,
        )
    return row is not None


async def send_sms(
    client: Client,
    to: str,
    body: str,
    *,
    rate_limiter: TokenBucket | None = None,
) -> str:
    """
    Send an outbound SMS. Returns the MessageSid on success.

    Raises:
        RateLimitExceeded — outbound rate bucket is empty, try later.
        twilio.base.exceptions.TwilioRestException — Twilio API error.
    """
    limiter = rate_limiter or _rate_limiter
    await limiter.consume()

    loop = asyncio.get_running_loop()
    msg = await loop.run_in_executor(
        None,
        lambda: client.messages.create(
            to=to,
            from_=settings.TWILIO_PHONE_NUMBER,
            body=body,
        ),
    )
    log.info("outbound.sms_sent", to=to, sid=msg.sid)
    return msg.sid


async def send_whatsapp(
    client: Client,
    pool: asyncpg.Pool,
    to: str,
    body: str,
    *,
    rate_limiter: TokenBucket | None = None,
    template_body: str | None = None,
) -> str:
    """
    Send an outbound WhatsApp message. Returns the MessageSid on success.

    Checks the 24-hour session window first. If expired:
      - Uses template_body when provided (logged as a warning so ops can see it).
      - Raises WindowExpired when no template_body is given, forcing the caller
        to make an explicit decision rather than silently sending the wrong thing.

    Raises:
        WindowExpired          — window expired and no template_body was given.
        RateLimitExceeded      — outbound rate bucket is empty.
        TwilioRestException    — Twilio API error.
    """
    limiter = rate_limiter or _rate_limiter
    in_window = await check_whatsapp_window(pool, to)

    if not in_window:
        if template_body is None:
            raise WindowExpiredError(
                f"WhatsApp 24-hour session window expired for {to}. "
                "Provide template_body= to fall back to a pre-approved template."
            )
        actual_body = template_body
        log.warning("outbound.whatsapp_window_expired_using_template", to=to)
    else:
        actual_body = body

    await limiter.consume()

    # Normalise the to address — Twilio requires the whatsapp: prefix
    wa_to = to if to.startswith("whatsapp:") else f"whatsapp:{to}"

    loop = asyncio.get_running_loop()
    msg = await loop.run_in_executor(
        None,
        lambda: client.messages.create(
            to=wa_to,
            from_=settings.TWILIO_WHATSAPP_NUMBER,
            body=actual_body,
        ),
    )
    log.info(
        "outbound.whatsapp_sent",
        to=to,
        sid=msg.sid,
        used_template=not in_window,
    )
    return msg.sid


async def initiate_call(
    client: Client,
    to: str,
    twiml_url: str,
    *,
    rate_limiter: TokenBucket | None = None,
) -> str:
    """
    Initiate an outbound voice call. Returns the CallSid on success.

    twiml_url must be a publicly accessible HTTPS URL that returns TwiML.
    Twilio will fetch this URL when the callee answers.

    Raises:
        RateLimitExceeded   — outbound rate bucket is empty.
        TwilioRestException — Twilio API error.
    """
    limiter = rate_limiter or _rate_limiter
    await limiter.consume()

    loop = asyncio.get_running_loop()
    call = await loop.run_in_executor(
        None,
        lambda: client.calls.create(
            to=to,
            from_=settings.TWILIO_PHONE_NUMBER,
            url=twiml_url,
        ),
    )
    log.info("outbound.call_initiated", to=to, sid=call.sid)
    return call.sid
