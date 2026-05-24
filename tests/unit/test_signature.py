"""
Unit tests for Twilio signature validation.

We test the validator logic in isolation — no FastAPI, no DB.
"""

from __future__ import annotations

from twilio.request_validator import RequestValidator

from tests.fixtures.twilio_fixtures import TEST_AUTH_TOKEN, TEST_BASE_URL, make_signature


def test_valid_signature_passes():
    """A correctly signed request is accepted."""
    params = {"Body": "Hello", "From": "+15559876543", "MessageSid": "SM123"}
    url = f"{TEST_BASE_URL}/webhooks/sms"
    sig = make_signature(url, params)

    validator = RequestValidator(TEST_AUTH_TOKEN)
    assert validator.validate(url, params, sig) is True


def test_wrong_auth_token_fails():
    """Signature computed with the wrong token is rejected."""
    params = {"Body": "Hello", "From": "+15559876543", "MessageSid": "SM123"}
    url = f"{TEST_BASE_URL}/webhooks/sms"
    sig = make_signature(url, params)  # signed with TEST_AUTH_TOKEN

    validator = RequestValidator("completely_wrong_token")
    assert validator.validate(url, params, sig) is False


def test_tampered_params_fail():
    """Changing any param after signing invalidates the signature."""
    params = {"Body": "Hello", "From": "+15559876543", "MessageSid": "SM123"}
    url = f"{TEST_BASE_URL}/webhooks/sms"
    sig = make_signature(url, params)

    # Attacker modifies the body after signing
    tampered = {**params, "Body": "I am an attacker"}
    validator = RequestValidator(TEST_AUTH_TOKEN)
    assert validator.validate(url, tampered, sig) is False


def test_wrong_url_fails():
    """Signature is URL-specific — reusing it on a different endpoint fails."""
    params = {"Body": "Hello", "From": "+15559876543", "MessageSid": "SM123"}
    sig = make_signature(f"{TEST_BASE_URL}/webhooks/sms", params)

    validator = RequestValidator(TEST_AUTH_TOKEN)
    # Attacker tries to replay the signature on a different endpoint
    assert validator.validate(f"{TEST_BASE_URL}/webhooks/voice", params, sig) is False


def test_empty_params_valid():
    """Status callbacks sometimes send no params — should still validate."""
    params: dict = {}
    url = f"{TEST_BASE_URL}/webhooks/sms/status"
    sig = make_signature(url, params)

    validator = RequestValidator(TEST_AUTH_TOKEN)
    assert validator.validate(url, params, sig) is True
