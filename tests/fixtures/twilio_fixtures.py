"""
Twilio request fixtures for tests.

These helpers generate the exact form payloads and signed headers that
Twilio would send in production, so our tests exercise the real validation
path rather than mocking it away.

WHY generate real signatures in tests:
If we mock out signature validation, our tests wouldn't catch a bug where
we accidentally broke signature validation. Using real signatures means the
validator runs end-to-end in every test, just with a test auth token.
"""

from __future__ import annotations

from twilio.request_validator import RequestValidator

# The test auth token — must match TWILIO_AUTH_TOKEN in conftest.py os.environ
TEST_AUTH_TOKEN = "test_auth_token_for_unit_tests_only"
# The test base URL — must match PUBLIC_BASE_URL in conftest.py
TEST_BASE_URL = "https://fake.ngrok-free.app"


def make_signature(url: str, params: dict[str, str]) -> str:
    """Compute a valid Twilio signature for the given URL and params."""
    validator = RequestValidator(TEST_AUTH_TOKEN)
    return validator.compute_signature(url, params)


def signed_sms_payload(
    message_sid: str = "SM1234567890abcdef",
    from_number: str = "+15559876543",
    to_number: str = "+15551234567",
    body: str = "Hello, test message",
    path: str = "/webhooks/sms",
) -> tuple[dict[str, str], str]:
    """
    Returns (form_params, x_twilio_signature) for a valid inbound SMS request.
    Use this in tests to POST to the webhook with a real signature.
    """
    params = {
        "MessageSid": message_sid,
        "SmsSid": message_sid,
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": from_number,
        "To": to_number,
        "Body": body,
        "NumMedia": "0",
        "MessageStatus": "received",
    }
    url = f"{TEST_BASE_URL}{path}"
    signature = make_signature(url, params)
    return params, signature


def signed_voice_payload(
    call_sid: str = "CA1234567890abcdef",
    from_number: str = "+15559876543",
    to_number: str = "+15551234567",
    call_status: str = "in-progress",
    path: str = "/webhooks/voice",
) -> tuple[dict[str, str], str]:
    """Returns (form_params, x_twilio_signature) for a valid inbound call."""
    params = {
        "CallSid": call_sid,
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": from_number,
        "To": to_number,
        "CallStatus": call_status,
        "Direction": "inbound",
    }
    url = f"{TEST_BASE_URL}{path}"
    signature = make_signature(url, params)
    return params, signature


def signed_sms_status_payload(
    message_sid: str = "SM1234567890abcdef",
    from_number: str = "+15551234567",
    to_number: str = "+15559876543",
    status: str = "delivered",
    path: str = "/webhooks/sms/status",
) -> tuple[dict[str, str], str]:
    """Returns (form_params, x_twilio_signature) for an SMS status callback."""
    params = {
        "MessageSid": message_sid,
        "SmsSid": message_sid,
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": from_number,
        "To": to_number,
        "MessageStatus": status,
    }
    url = f"{TEST_BASE_URL}{path}"
    signature = make_signature(url, params)
    return params, signature


def signed_whatsapp_status_payload(
    message_sid: str = "SM1234567890abcdef",
    from_number: str = "whatsapp:+14155238886",
    to_number: str = "whatsapp:+15559876543",
    status: str = "delivered",
    path: str = "/webhooks/whatsapp/status",
) -> tuple[dict[str, str], str]:
    """Returns (form_params, x_twilio_signature) for a WhatsApp status callback."""
    params = {
        "MessageSid": message_sid,
        "SmsSid": message_sid,
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": from_number,
        "To": to_number,
        "MessageStatus": status,
    }
    url = f"{TEST_BASE_URL}{path}"
    signature = make_signature(url, params)
    return params, signature


def signed_whatsapp_payload(
    message_sid: str = "SM1234567890abcdef",
    from_number: str = "whatsapp:+15559876543",
    to_number: str = "whatsapp:+14155238886",
    body: str = "Hello from WhatsApp",
    path: str = "/webhooks/whatsapp",
) -> tuple[dict[str, str], str]:
    """Returns (form_params, x_twilio_signature) for a valid inbound WhatsApp message."""
    params = {
        "MessageSid": message_sid,
        "SmsSid": message_sid,
        "AccountSid": "ACtest00000000000000000000000000000",
        "From": from_number,
        "To": to_number,
        "Body": body,
        "NumMedia": "0",
        "ProfileName": "Test User",
    }
    url = f"{TEST_BASE_URL}{path}"
    signature = make_signature(url, params)
    return params, signature
