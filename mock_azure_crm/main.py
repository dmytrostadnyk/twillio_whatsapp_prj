"""
Mock Azure CRM.

A minimal but real FastAPI app that simulates the Azure CRM endpoint the
delivery worker posts events to.

Two purposes:
  1. Local integration target — lets you run the full stack without a real CRM.
  2. Chaos testing — /admin/toggle makes the service return 503 so you can
     demonstrate the retry/DLQ/replay flow without killing the process.

WHY idempotency here:
The contract payload includes an event_key that is unique per event. The mock
stores events keyed by event_key, so posting the same event twice only ever
results in one stored copy. This proves the full stack is idempotent end-to-end,
not just within our own DB.

WHY a create_app() factory:
Tests call create_app() to get a fresh instance with its own empty store. If
we used module-level globals for the store, tests would bleed state into each
other. The factory pattern gives each test a clean slate at zero extra cost.

Run standalone:
    uvicorn mock_azure_crm.main:app --port 8001 --reload

Or via Makefile:
    make mock-crm
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

log = structlog.get_logger(__name__)

SUPPORTED_SCHEMA_VERSION = "1.0"


def create_app() -> FastAPI:
    """
    Build and return a FastAPI app with its own isolated in-memory state.

    WHY not module-level globals: each call to create_app() gets a fresh store
    and chaos flag. This is the key to clean test isolation — tests call this
    directly rather than importing the module-level `app`.
    """
    _app = FastAPI(title="Mock Azure CRM", version="1.0.0")

    # In-memory event store: event_key → {payload, received_at}
    _app.state.events: dict[str, dict[str, Any]] = {}
    # Chaos flag: when True all /events requests return 503
    _app.state.down: bool = False

    @_app.get("/health")
    async def health() -> JSONResponse:
        """Health/readiness endpoint — always 200 while the process is alive."""
        return JSONResponse({"status": "ok", "down": _app.state.down})

    @_app.post("/events")
    async def receive_event(request: Request) -> JSONResponse:
        """
        Accept a delivery-worker contract payload.

        Returns 200 for both new and duplicate events (idempotent by event_key).
        Returns 503 when chaos mode is active — the delivery worker will retry.
        Returns 400 if event_key is missing.
        Returns 422 if schema_version is not "1.0".
        """
        if _app.state.down:
            log.warning("mock_crm.down.rejected_request")
            return JSONResponse(
                {"error": "Service Unavailable — chaos mode is active"},
                status_code=503,
            )

        try:
            payload: dict[str, Any] = await request.json()
        except Exception:
            return JSONResponse({"error": "Request body must be valid JSON"}, status_code=400)

        event_key = payload.get("event_key")
        if not event_key:
            return JSONResponse({"error": "Missing required field: event_key"}, status_code=400)

        schema_version = payload.get("schema_version")
        if schema_version != SUPPORTED_SCHEMA_VERSION:
            return JSONResponse(
                {
                    "error": (
                        f"Unsupported schema_version: {schema_version!r}. "
                        f"Expected {SUPPORTED_SCHEMA_VERSION!r}."
                    )
                },
                status_code=422,
            )

        is_duplicate = event_key in _app.state.events
        if not is_duplicate:
            _app.state.events[event_key] = {
                "payload": payload,
                "received_at": datetime.now(UTC).isoformat(),
            }
            log.info("mock_crm.event_stored", event_key=event_key)
        else:
            log.info("mock_crm.duplicate_ignored", event_key=event_key)

        return JSONResponse(
            {
                "status": "accepted",
                "event_key": event_key,
                "duplicate": is_duplicate,
            }
        )

    @_app.post("/admin/toggle")
    async def toggle_downtime() -> JSONResponse:
        """
        Flip the chaos flag.

        When down=True, all POST /events requests return 503. Toggle again to
        restore service. Use this during the demo to show the DLQ replay flow.
        """
        _app.state.down = not _app.state.down
        state_label = "DOWN" if _app.state.down else "UP"
        log.info("mock_crm.chaos_toggled", state=state_label)
        return JSONResponse({"down": _app.state.down, "state": state_label})

    @_app.get("/admin/events")
    async def list_events() -> JSONResponse:
        """Return all stored events. Used by the dashboard and integration tests."""
        return JSONResponse(
            {
                "count": len(_app.state.events),
                "events": list(_app.state.events.values()),
            }
        )

    @_app.delete("/admin/events")
    async def clear_events() -> JSONResponse:
        """Wipe all stored events — useful for resetting between demo runs."""
        count = len(_app.state.events)
        _app.state.events.clear()
        log.info("mock_crm.events_cleared", count=count)
        return JSONResponse({"cleared": count})

    return _app


# Module-level app for `uvicorn mock_azure_crm.main:app`
app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("mock_azure_crm.main:app", host="0.0.0.0", port=8001, reload=True)
