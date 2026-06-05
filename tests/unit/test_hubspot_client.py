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
10. get_contact_log returns ai_comm_log from a successful GET
11. get_contact_log returns "" when ai_comm_log property is absent
12. get_contact_log raises HTTPStatusError on non-2xx

All HTTP calls are mocked with respx — no real HubSpot API is called.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from comm_layer.config import settings
from delivery_worker.hubspot_client import (
    add_note_to_ticket,
    create_note,
    create_task,
    find_or_create_contact,
    find_or_create_ticket,
    get_contact_log,
    normalize_phone,
)

_BASE = settings.HUBSPOT_BASE_URL
_SEARCH_URL = f"{_BASE}/crm/v3/objects/contacts/search"
_CREATE_URL = f"{_BASE}/crm/v3/objects/contacts"
_NOTES_URL = f"{_BASE}/crm/v3/objects/notes"
_TICKETS_URL = f"{_BASE}/crm/v3/objects/tickets"
_TASKS_URL = f"{_BASE}/crm/v3/objects/tasks"


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


# ── get_contact_log ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_get_contact_log_returns_existing_log():
    """GET contact by ID → returns the ai_comm_log property value."""
    contact_id = "hs-789"
    respx.get(f"{_BASE}/crm/v3/objects/contacts/{contact_id}").mock(
        return_value=httpx.Response(
            200,
            json={"id": contact_id, "properties": {"ai_comm_log": "Entry from last call"}},
        )
    )
    async with httpx.AsyncClient() as client:
        log_value = await get_contact_log(client, "fake-token", _BASE, contact_id)

    assert log_value == "Entry from last call"


@pytest.mark.asyncio
@respx.mock
async def test_get_contact_log_returns_empty_string_when_property_absent():
    """If ai_comm_log is not in properties (new contact), return "" not None."""
    contact_id = "hs-789"
    respx.get(f"{_BASE}/crm/v3/objects/contacts/{contact_id}").mock(
        return_value=httpx.Response(
            200,
            json={"id": contact_id, "properties": {}},
        )
    )
    async with httpx.AsyncClient() as client:
        log_value = await get_contact_log(client, "fake-token", _BASE, contact_id)

    assert log_value == ""


@pytest.mark.asyncio
@respx.mock
async def test_get_contact_log_raises_on_non_2xx():
    """
    A 5xx or 4xx from the GET endpoint raises HTTPStatusError.
    The delivery worker catches this and nacks so we never proceed to a
    PATCH that would overwrite history with an empty log.
    """
    contact_id = "hs-789"
    respx.get(f"{_BASE}/crm/v3/objects/contacts/{contact_id}").mock(
        return_value=httpx.Response(500)
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await get_contact_log(client, "fake-token", _BASE, contact_id)


# ── create_note ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_create_note_returns_note_id():
    """Successful Note POST → returns the note id from the response."""
    note_id = "note-abc123"
    respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": note_id})
    )
    async with httpx.AsyncClient() as client:
        result = await create_note(
            client, "fake-token", _BASE,
            contact_id="contact-1",
            body="Customer asked about opening hours.",
            timestamp_ms=1700000000000,
        )
    assert result == note_id


@pytest.mark.asyncio
@respx.mock
async def test_create_note_sends_body_and_timestamp():
    """Note POST body must contain hs_note_body and hs_timestamp."""
    import json as _json
    notes_route = respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": "n1"})
    )
    async with httpx.AsyncClient() as client:
        await create_note(
            client, "fake-token", _BASE,
            contact_id="c1",
            body="My note body",
            timestamp_ms=1700000000123,
        )
    request_body = _json.loads(notes_route.calls[0].request.content)
    assert request_body["properties"]["hs_note_body"] == "My note body"
    assert request_body["properties"]["hs_timestamp"] == "1700000000123"


@pytest.mark.asyncio
@respx.mock
async def test_create_note_includes_contact_association():
    """Note POST must include an association to the contact."""
    import json as _json
    notes_route = respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": "n2"})
    )
    async with httpx.AsyncClient() as client:
        await create_note(
            client, "fake-token", _BASE,
            contact_id="contact-99",
            body="Body",
            timestamp_ms=0,
        )
    request_body = _json.loads(notes_route.calls[0].request.content)
    associations = request_body.get("associations", [])
    assert any(
        assoc.get("to", {}).get("id") == "contact-99"
        for assoc in associations
    ), "Association to the contact must be in the Note POST body"


@pytest.mark.asyncio
@respx.mock
async def test_create_note_raises_on_non_2xx():
    """Note POST 5xx → raises HTTPStatusError so the caller can nack."""
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await create_note(client, "fake-token", _BASE, "c1", "body", 0)


# ── find_or_create_ticket ──────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_returns_ticket_id_and_false():
    """Successful Ticket POST (no prior ticket) → returns (ticket_id, False)."""
    ticket_id = "ticket-xyz"
    respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": ticket_id})
    )
    async with httpx.AsyncClient() as client:
        result_id, reused = await find_or_create_ticket(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="[SMS] Complaint from …9876",
            content="Customer complained about delivery.",
            pipeline="0",
            pipeline_stage="1",
        )
    assert result_id == ticket_id
    assert reused is False


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_sends_correct_properties():
    """Ticket POST body must contain subject, content, pipeline, and HIGH priority."""
    import json as _json
    tickets_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "t1"})
    )
    async with httpx.AsyncClient() as client:
        await find_or_create_ticket(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="[SMS] Complaint from …9876",
            content="Customer complained.",
            pipeline="0",
            pipeline_stage="1",
        )
    request_body = _json.loads(tickets_route.calls[0].request.content)
    props = request_body["properties"]
    assert props["subject"] == "[SMS] Complaint from …9876"
    assert props["hs_ticket_priority"] == "HIGH"
    assert props["hs_pipeline"] == "0"
    assert props["hs_pipeline_stage"] == "1"


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_raises_on_non_2xx():
    """Ticket POST 5xx → raises HTTPStatusError so the caller can nack."""
    respx.post(_TICKETS_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await find_or_create_ticket(
                client, "fake-token", _BASE, "c1", "subj", "content", "0", "1"
            )


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_reuses_open_ticket():
    """existing_ticket_id provided + stage != closed_stage → (id, True), no POST."""
    ticket_id = "existing-open-123"
    get_route = respx.get(f"{_BASE}/crm/v3/objects/tickets/{ticket_id}").mock(
        return_value=httpx.Response(
            200, json={"id": ticket_id, "properties": {"hs_pipeline_stage": "1"}}
        )
    )
    create_route = respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "new-ticket"})
    )
    async with httpx.AsyncClient() as client:
        result_id, reused = await find_or_create_ticket(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="subj",
            content="content",
            pipeline="0",
            pipeline_stage="1",
            existing_ticket_id=ticket_id,
            closed_stage="4",
        )
    assert result_id == ticket_id
    assert reused is True
    assert get_route.called
    assert not create_route.called, "Must NOT create a new ticket when reusing open one"


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_creates_new_when_prior_closed():
    """existing_ticket_id with closed stage → new ticket created, reused=False."""
    ticket_id = "closed-456"
    respx.get(f"{_BASE}/crm/v3/objects/tickets/{ticket_id}").mock(
        return_value=httpx.Response(
            200, json={"id": ticket_id, "properties": {"hs_pipeline_stage": "4"}}
        )
    )
    respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "brand-new"})
    )
    async with httpx.AsyncClient() as client:
        result_id, reused = await find_or_create_ticket(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="subj",
            content="content",
            pipeline="0",
            pipeline_stage="1",
            existing_ticket_id=ticket_id,
            closed_stage="4",
        )
    assert result_id == "brand-new"
    assert reused is False


@pytest.mark.asyncio
@respx.mock
async def test_find_or_create_ticket_creates_new_when_prior_404():
    """existing_ticket_id that returns 404 (deleted) → new ticket created."""
    ticket_id = "deleted-789"
    respx.get(f"{_BASE}/crm/v3/objects/tickets/{ticket_id}").mock(
        return_value=httpx.Response(404)
    )
    respx.post(_TICKETS_URL).mock(
        return_value=httpx.Response(201, json={"id": "fresh-ticket"})
    )
    async with httpx.AsyncClient() as client:
        result_id, reused = await find_or_create_ticket(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="subj",
            content="content",
            pipeline="0",
            pipeline_stage="1",
            existing_ticket_id=ticket_id,
            closed_stage="4",
        )
    assert result_id == "fresh-ticket"
    assert reused is False


# ── add_note_to_ticket ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_add_note_to_ticket_returns_note_id():
    """Successful Note POST linked to ticket → returns the note id."""
    note_id = "note-for-ticket-1"
    respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": note_id})
    )
    async with httpx.AsyncClient() as client:
        result = await add_note_to_ticket(
            client, "fake-token", _BASE,
            ticket_id="ticket-99",
            body="Customer sent another complaint.",
            timestamp_ms=1700000000000,
        )
    assert result == note_id


@pytest.mark.asyncio
@respx.mock
async def test_add_note_to_ticket_uses_ticket_association():
    """Note POST must use association typeId 228 (note→ticket)."""
    import json as _json
    notes_route = respx.post(_NOTES_URL).mock(
        return_value=httpx.Response(201, json={"id": "n99"})
    )
    async with httpx.AsyncClient() as client:
        await add_note_to_ticket(
            client, "fake-token", _BASE,
            ticket_id="ticket-42",
            body="Body",
            timestamp_ms=0,
        )
    request_body = _json.loads(notes_route.calls[0].request.content)
    associations = request_body.get("associations", [])
    assert any(
        assoc.get("to", {}).get("id") == "ticket-42"
        and any(t.get("associationTypeId") == 228 for t in assoc.get("types", []))
        for assoc in associations
    ), "Note must be associated to the ticket with typeId 228"


@pytest.mark.asyncio
@respx.mock
async def test_add_note_to_ticket_raises_on_non_2xx():
    """5xx from Notes API → raises HTTPStatusError."""
    respx.post(_NOTES_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await add_note_to_ticket(
                client, "fake-token", _BASE, "ticket-1", "body", 0
            )


# ── create_task ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_create_task_returns_task_id():
    """Successful Task POST → returns the task id."""
    task_id = "task-abc"
    respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": task_id})
    )
    async with httpx.AsyncClient() as client:
        result = await create_task(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="Follow up — bot could not answer",
            body="Customer asked about brewing process.",
            due_at_ms=1700000000000,
        )
    assert result == task_id


@pytest.mark.asyncio
@respx.mock
async def test_create_task_sends_correct_properties():
    """Task POST body must include subject, body, status, priority, timestamp, type."""
    import json as _json
    tasks_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "t1"})
    )
    async with httpx.AsyncClient() as client:
        await create_task(
            client, "fake-token", _BASE,
            contact_id="c1",
            subject="Follow up",
            body="Event body here",
            due_at_ms=1700000000123,
        )
    request_body = _json.loads(tasks_route.calls[0].request.content)
    props = request_body["properties"]
    assert props["hs_task_subject"] == "Follow up"
    assert props["hs_task_body"] == "Event body here"
    assert props["hs_task_status"] == "NOT_STARTED"
    assert props["hs_task_priority"] == "HIGH"
    assert props["hs_timestamp"] == "1700000000123"
    assert props["hs_task_type"] == "TODO"


@pytest.mark.asyncio
@respx.mock
async def test_create_task_includes_contact_association():
    """Task POST must include an association to the contact with typeId 204."""
    import json as _json
    tasks_route = respx.post(_TASKS_URL).mock(
        return_value=httpx.Response(201, json={"id": "t2"})
    )
    async with httpx.AsyncClient() as client:
        await create_task(
            client, "fake-token", _BASE,
            contact_id="contact-88",
            subject="subj",
            body="body",
            due_at_ms=0,
        )
    request_body = _json.loads(tasks_route.calls[0].request.content)
    associations = request_body.get("associations", [])
    assert any(
        assoc.get("to", {}).get("id") == "contact-88"
        and any(t.get("associationTypeId") == 204 for t in assoc.get("types", []))
        for assoc in associations
    ), "Task must be associated to the contact with typeId 204"


@pytest.mark.asyncio
@respx.mock
async def test_create_task_raises_on_non_2xx():
    """5xx from Tasks API → raises HTTPStatusError so caller can nack."""
    respx.post(_TASKS_URL).mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await create_task(
                client, "fake-token", _BASE, "c1", "subj", "body", 0
            )
