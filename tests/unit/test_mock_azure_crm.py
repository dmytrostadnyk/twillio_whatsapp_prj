"""
Tests for the Mock Azure CRM.

What we test:
1.  Health endpoint always returns 200.
2.  Valid contract payload → 200 + stored.
3.  Duplicate event_key → 200 + duplicate=True + only one entry stored.
4.  Missing event_key → 400.
5.  Wrong schema_version → 422.
6.  Chaos toggle ON  → POST /events returns 503.
7.  Chaos toggle OFF (second toggle) → POST /events returns 200 again.
8.  /admin/events returns stored events.
9.  /admin/events DELETE clears the store.
10. Non-JSON body → 400.

Each test creates a fresh app via create_app() for full isolation — no shared state.
"""

from __future__ import annotations

import httpx
import pytest

from mock_azure_crm.main import create_app

# ── Shared helpers ─────────────────────────────────────────────────────────────


def make_contract(event_key: str = "SM123:sms.received") -> dict:
    """Minimal valid contract payload matching schema_version 1.0."""
    return {
        "schema_version": "1.0",
        "event_key": event_key,
        "correlation_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "channel": "sms",
        "direction": "inbound",
        "event_type": "sms.received",
        "timestamp": "2026-01-15T12:00:00+00:00",
        "source": {"number": "+15551234567", "is_unknown": False},
        "data": {
            "from_number": "+15559876543",
            "to_number": "+15551234567",
            "raw": {"Body": "Hello", "MessageSid": "SM123"},
        },
    }


# ── Fixture ────────────────────────────────────────────────────────────────────


@pytest.fixture
def client() -> httpx.AsyncClient:
    """
    Fresh AsyncClient per test, backed by a fresh create_app() instance.

    WHY create_app() per fixture: each test gets an empty event store and
    chaos=False. Tests cannot interfere with each other's state.
    """
    app = create_app()
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


# ── Health ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_returns_200(client):
    async with client as c:
        response = await c.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# ── /events — happy path ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_receive_valid_contract_returns_200(client):
    """A well-formed contract payload is accepted with status 200."""
    async with client as c:
        response = await c.post("/events", json=make_contract())
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["event_key"] == "SM123:sms.received"
    assert body["duplicate"] is False


@pytest.mark.asyncio
async def test_first_event_is_stored(client):
    """After POSTing an event, GET /admin/events reflects it in the store."""
    async with client as c:
        await c.post("/events", json=make_contract())
        response = await c.get("/admin/events")
    body = response.json()
    assert body["count"] == 1
    assert body["events"][0]["payload"]["event_key"] == "SM123:sms.received"


# ── /events — idempotency ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_duplicate_event_key_returns_200(client):
    """The second POST with the same event_key returns 200, not a 4xx/5xx."""
    async with client as c:
        await c.post("/events", json=make_contract())
        response = await c.post("/events", json=make_contract())
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_duplicate_event_key_sets_duplicate_flag(client):
    """The response body on the second POST must have duplicate=True."""
    async with client as c:
        await c.post("/events", json=make_contract())
        response = await c.post("/events", json=make_contract())
    assert response.json()["duplicate"] is True


@pytest.mark.asyncio
async def test_duplicate_event_key_stores_only_one_entry(client):
    """
    Two POSTs with the same event_key must result in exactly one stored entry.
    This is the idempotency guarantee: the delivery worker can retry safely
    without creating duplicate entries in the CRM.
    """
    async with client as c:
        await c.post("/events", json=make_contract())
        await c.post("/events", json=make_contract())
        response = await c.get("/admin/events")
    assert response.json()["count"] == 1


@pytest.mark.asyncio
async def test_different_event_keys_stored_separately(client):
    """Two different event_keys are stored as two separate entries."""
    async with client as c:
        await c.post("/events", json=make_contract(event_key="SM001:sms.received"))
        await c.post("/events", json=make_contract(event_key="SM002:sms.received"))
        response = await c.get("/admin/events")
    assert response.json()["count"] == 2


# ── /events — validation ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_event_key_returns_400(client):
    """A payload without event_key is rejected immediately — we can't deduplicate it."""
    payload = make_contract()
    del payload["event_key"]
    async with client as c:
        response = await c.post("/events", json=payload)
    assert response.status_code == 400
    assert "event_key" in response.json()["error"]


@pytest.mark.asyncio
async def test_wrong_schema_version_returns_422(client):
    """An unrecognized schema_version means the contract changed — reject it."""
    payload = make_contract()
    payload["schema_version"] = "2.0"
    async with client as c:
        response = await c.post("/events", json=payload)
    assert response.status_code == 422
    assert "schema_version" in response.json()["error"]


@pytest.mark.asyncio
async def test_non_json_body_returns_400(client):
    """A non-JSON body cannot be parsed into a contract — reject with 400."""
    async with client as c:
        response = await c.post(
            "/events",
            content=b"not json at all",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "body",
    [["a", "b"], "just a string", 42, 3.14, True],
    ids=["array", "string", "int", "float", "bool"],
)
@pytest.mark.asyncio
async def test_non_object_json_body_returns_400(client, body):
    """
    Valid JSON that isn't an object (arrays, scalars) must be rejected with 400
    — not crash the route with AttributeError when we try payload.get(...).
    """
    async with client as c:
        response = await c.post("/events", json=body)
    assert response.status_code == 400
    assert "object" in response.json()["error"].lower()


@pytest.mark.parametrize(
    "bad_key",
    [12345, ["a", "b"], {"nested": "dict"}, 3.14, True],
    ids=["int", "list", "dict", "float", "bool"],
)
@pytest.mark.asyncio
async def test_non_string_event_key_returns_400(client, bad_key):
    """
    event_key must be a string. A numeric key would be stored with the wrong
    type and break idempotency on retry; a list/dict would crash on dict insert.
    """
    payload = make_contract()
    payload["event_key"] = bad_key
    async with client as c:
        response = await c.post("/events", json=payload)
    assert response.status_code == 400
    assert "event_key" in response.json()["error"]


@pytest.mark.asyncio
async def test_whitespace_only_event_key_returns_400(client):
    """An event_key of '   ' is effectively empty — reject."""
    payload = make_contract()
    payload["event_key"] = "   "
    async with client as c:
        response = await c.post("/events", json=payload)
    assert response.status_code == 400


# ── /admin/toggle — chaos mode ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_toggle_on_makes_events_return_503(client):
    """
    After toggling the chaos flag, /events must return 503. This simulates
    Azure CRM downtime so the delivery worker starts queuing and retrying.
    """
    async with client as c:
        await c.post("/admin/toggle")
        response = await c.post("/events", json=make_contract())
    assert response.status_code == 503


@pytest.mark.asyncio
async def test_toggle_on_response_shows_down_true(client):
    """The toggle response must report the new state."""
    async with client as c:
        response = await c.post("/admin/toggle")
    body = response.json()
    assert body["down"] is True
    assert body["state"] == "DOWN"


@pytest.mark.asyncio
async def test_double_toggle_restores_service(client):
    """
    Two toggles bring the service back UP. This is the demo reset: toggle off,
    replay the DLQ, watch everything deliver successfully.
    """
    async with client as c:
        await c.post("/admin/toggle")  # → DOWN
        await c.post("/admin/toggle")  # → UP
        response = await c.post("/events", json=make_contract())
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_toggle_does_not_store_events_made_during_downtime(client):
    """Events rejected while down must NOT appear in the store."""
    async with client as c:
        await c.post("/admin/toggle")           # → DOWN
        await c.post("/events", json=make_contract())  # rejected
        await c.post("/admin/toggle")           # → UP
        response = await c.get("/admin/events")
    assert response.json()["count"] == 0


@pytest.mark.asyncio
async def test_admin_events_still_works_during_chaos(client):
    """
    Even when chaos is ON, operators need to see what's in the store. The 503
    must apply ONLY to /events — admin endpoints must remain reachable so the
    dashboard keeps working during the demo.
    """
    async with client as c:
        # Store something first
        await c.post("/events", json=make_contract())
        # Then go into chaos mode
        await c.post("/admin/toggle")
        # Admin endpoints must still respond
        response = await c.get("/admin/events")
    assert response.status_code == 200
    assert response.json()["count"] == 1


@pytest.mark.asyncio
async def test_health_still_works_during_chaos(client):
    """Health check must NOT report unhealthy just because chaos is on —
    chaos is a synthetic flag, not actual process death."""
    async with client as c:
        await c.post("/admin/toggle")
        response = await c.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["down"] is True


# ── /admin/events ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_admin_events_empty_on_fresh_app(client):
    """A fresh app has no events."""
    async with client as c:
        response = await c.get("/admin/events")
    body = response.json()
    assert body["count"] == 0
    assert body["events"] == []


@pytest.mark.asyncio
async def test_admin_events_includes_received_at_timestamp(client):
    """Each stored event entry must have a received_at field for audit/debug."""
    async with client as c:
        await c.post("/events", json=make_contract())
        response = await c.get("/admin/events")
    entry = response.json()["events"][0]
    assert "received_at" in entry


@pytest.mark.asyncio
async def test_admin_delete_clears_all_events(client):
    """DELETE /admin/events wipes the store — used to reset between demo runs."""
    async with client as c:
        await c.post("/events", json=make_contract(event_key="SM001:sms.received"))
        await c.post("/events", json=make_contract(event_key="SM002:sms.received"))
        delete_response = await c.delete("/admin/events")
        list_response = await c.get("/admin/events")

    assert delete_response.json()["cleared"] == 2
    assert list_response.json()["count"] == 0
