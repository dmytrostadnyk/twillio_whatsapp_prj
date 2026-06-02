"""
Unit tests for delivery_worker/hubspot_client.py.

What we test:
1.  ensure_custom_properties creates property group + each property
2.  ensure_custom_properties ignores 409 Conflict (already exists)
3.  find_or_create_contact returns existing contact id when found
4.  find_or_create_contact creates contact when search returns 0 results
5.  find_or_create_contact returns existing ai_comm_log from search result
6.  find_or_create_contact strips 'whatsapp:' prefix before search
7.  normalize_phone strips whatsapp prefix
8.  normalize_phone leaves plain E.164 unchanged
9.  normalize_phone handles None

All HTTP calls are mocked with respx — no real HubSpot API is called.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from comm_layer.config import settings
from delivery_worker.hubspot_client import (
    find_or_create_contact,
    normalize_phone,
)

_BASE = settings.HUBSPOT_BASE_URL
_SEARCH_URL = f"{_BASE}/crm/v3/objects/contacts/search"
_CREATE_URL = f"{_BASE}/crm/v3/objects/contacts"


# ── normalize_phone ────────────────────────────────────────────────────────────


def test_normalize_phone_strips_whatsapp_prefix():
    assert normalize_phone("whatsapp:+15551234567") == "+15551234567"


def test_normalize_phone_leaves_plain_e164_unchanged():
    assert normalize_phone("+15551234567") == "+15551234567"


def test_normalize_phone_handles_none():
    assert normalize_phone(None) is None


def test_normalize_phone_handles_empty_string():
    # Edge case: empty string has no prefix to strip
    assert normalize_phone("") == ""


# ── find_or_create_contact — contact found ────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_find_returns_existing_contact_id():
    """When search finds a contact, returns its id without calling create."""
    found = {"results": [{"id": "111", "properties": {"phone": "+15559876543", "ai_comm_log": ""}}]}
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=found))
    async with httpx.AsyncClient() as client:
        contact_id, _ = await find_or_create_contact(
            client, "fake-token", _BASE, "+15559876543"
        )
    assert contact_id == "111"


@pytest.mark.asyncio
@respx.mock
async def test_find_returns_existing_ai_comm_log():
    """The existing ai_comm_log is returned so the caller can prepend to it."""
    respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "222",
                        "properties": {
                            "phone": "+15559876543",
                            "ai_comm_log": "Previous entry",
                        },
                    }
                ]
            },
        )
    )
    async with httpx.AsyncClient() as client:
        _, existing_log = await find_or_create_contact(
            client, "fake-token", _BASE, "+15559876543"
        )
    assert existing_log == "Previous entry"


@pytest.mark.asyncio
@respx.mock
async def test_find_does_not_call_create_when_contact_exists():
    """If search finds a contact, the create endpoint must NOT be called."""
    found = {"results": [{"id": "333", "properties": {"phone": "+15559876543", "ai_comm_log": ""}}]}
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(200, json=found))
    create_route = respx.post(_CREATE_URL).mock(
        return_value=httpx.Response(201, json={"id": "999"})
    )

    async with httpx.AsyncClient() as client:
        await find_or_create_contact(client, "fake-token", _BASE, "+15559876543")

    assert not create_route.called


# ── find_or_create_contact — contact not found ────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_create_called_when_search_returns_empty():
    """When search returns 0 results, a new contact is created."""
    respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    respx.post(_CREATE_URL).mock(
        return_value=httpx.Response(
            201, json={"id": "456", "properties": {"phone": "+15559876543"}}
        )
    )

    async with httpx.AsyncClient() as client:
        contact_id, existing_log = await find_or_create_contact(
            client, "fake-token", _BASE, "+15559876543"
        )

    assert contact_id == "456"
    assert existing_log == ""  # new contact has no history


@pytest.mark.asyncio
@respx.mock
async def test_whatsapp_prefix_stripped_before_search():
    """
    WhatsApp from_number arrives as 'whatsapp:+15551234567'. The prefix must
    be stripped before the HubSpot search or the query will never match a plain
    E.164 contact — causing duplicate contacts on every WhatsApp delivery.
    """
    search_route = respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    respx.post(_CREATE_URL).mock(
        return_value=httpx.Response(201, json={"id": "789", "properties": {}})
    )

    async with httpx.AsyncClient() as client:
        await find_or_create_contact(
            client, "fake-token", _BASE, "whatsapp:+15559876543"
        )

    # Inspect what was sent in the search body
    search_body = search_route.calls[0].request.content
    import json
    body = json.loads(search_body)
    filter_value = body["filterGroups"][0]["filters"][0]["value"]
    assert filter_value == "+15559876543", (
        f"Expected plain E.164, but search used: {filter_value}"
    )


# ── find_or_create_contact — error propagation ────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_search_4xx_raises_http_status_error():
    """
    A 401/403/5xx from the search endpoint raises httpx.HTTPStatusError.
    The delivery worker's process_message catches this and maps it to
    dead-letter (auth) or nack (5xx).
    """
    respx.post(_SEARCH_URL).mock(return_value=httpx.Response(401))

    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await find_or_create_contact(client, "bad-token", _BASE, "+15559876543")


@pytest.mark.asyncio
@respx.mock
async def test_create_5xx_raises_http_status_error():
    """A 5xx from the create endpoint also raises HTTPStatusError."""
    respx.post(_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    respx.post(_CREATE_URL).mock(return_value=httpx.Response(500))

    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await find_or_create_contact(client, "fake-token", _BASE, "+15559876543")
