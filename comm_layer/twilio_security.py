"""
Twilio webhook signature validation.

WHY this matters: our webhook URL is public (exposed via ngrok). Without
signature validation, anyone on the internet could POST fake Twilio events
to it and inject fraudulent data. Twilio signs every request with HMAC-SHA1
using your auth token. Validating the signature proves the request genuinely
came from Twilio.

HOW it works:
1. Twilio computes a signature = HMAC-SHA1(auth_token, url + sorted_params)
2. Twilio sends the signature in the X-Twilio-Signature header
3. We recompute the same signature on our side
4. If they match → request is genuine. If not → reject with 403.

IMPORTANT — the URL must match EXACTLY what Twilio used to sign the request,
including scheme, host, path, and query string. We reconstruct it using
PUBLIC_BASE_URL from settings so ngrok tunnelling works correctly.
"""

from __future__ import annotations

import structlog
from fastapi import Header, HTTPException, Request
from twilio.request_validator import RequestValidator

from comm_layer.config import settings

log = structlog.get_logger(__name__)


async def require_twilio_signature(
    request: Request,
    x_twilio_signature: str = Header(),
) -> dict[str, str]:
    """
    FastAPI dependency: validates X-Twilio-Signature on every inbound webhook.

    Returns the parsed form parameters if valid — handlers use these directly
    so they don't need to call request.form() a second time.

    Raises HTTP 403 if the signature is missing or invalid.
    WHY fail closed (403 not 200): silently accepting unsigned requests would
    let anyone inject fake events into the system.
    """
    # Read and cache the form data (Starlette caches this on the request object)
    form_data = await request.form()
    params = dict(form_data)

    # Reconstruct the URL Twilio signed against.
    # Behind ngrok, requests arrive at localhost but Twilio signed against the
    # ngrok URL, so we must use PUBLIC_BASE_URL as the host.
    path = request.url.path
    if request.url.query:
        path += f"?{request.url.query}"
    url = f"{settings.PUBLIC_BASE_URL.rstrip('/')}{path}"

    validator = RequestValidator(settings.TWILIO_AUTH_TOKEN)

    if not validator.validate(url, params, x_twilio_signature):
        log.warning(
            "twilio.signature_invalid",
            url=url,
            # Never log the signature itself — it's derived from your auth token
        )
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    log.debug("twilio.signature_valid", url=url)
    return params
