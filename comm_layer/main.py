"""
Communication Layer — FastAPI application entry point.

This file wires together:
- Application lifespan (startup/shutdown: DB pool, supabase client)
- All webhook routers (added in Phase 1)
- Health check endpoints

WHY FastAPI: native async support, auto-generated OpenAPI docs, and tight
Pydantic integration (same library we use for the event contracts).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from comm_layer.config import settings
from comm_layer.db import create_pool, create_supabase_client
from comm_layer.logging_config import configure_logging

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan context manager.

    WHY lifespan over @app.on_event (deprecated):
    Lifespan gives us a single place to manage startup and shutdown resources.
    Code before 'yield' runs on startup; code after 'yield' runs on shutdown.
    """
    configure_logging(settings.LOG_LEVEL)
    log.info("comm_layer.starting")

    # Create shared DB connections at startup so handlers don't pay this cost per-request
    app.state.pool = await create_pool()
    app.state.supabase = await create_supabase_client()

    log.info("comm_layer.ready", public_url=settings.PUBLIC_BASE_URL)
    yield

    # Shutdown: close the connection pool cleanly
    await app.state.pool.close()
    log.info("comm_layer.stopped")


def create_app() -> FastAPI:
    """Factory function that creates and configures the FastAPI app."""
    app = FastAPI(
        title="Twilio Communication Layer",
        description=(
            "Durable Twilio event ingestion. Validates, deduplicates, persists, "
            "and forwards all Voice/SMS/WhatsApp events."
        ),
        version="1.0.0",
        lifespan=lifespan,
        # Disable the interactive docs in production (they expose your API surface)
        # Set to None in prod via an env var check if desired
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── Health checks ──────────────────────────────────────────────────────────
    # Liveness: is the process up? (used by docker / orchestrators to decide if it's healthy)
    @app.get("/health/live", tags=["Health"])
    async def liveness() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    # Readiness: can the process serve traffic? (DB connected, etc.)
    @app.get("/health/ready", tags=["Health"])
    async def readiness() -> JSONResponse:
        try:
            # A lightweight query to confirm the DB is reachable
            await app.state.pool.fetchval("SELECT 1")
            return JSONResponse({"status": "ready", "db": "ok"})
        except Exception as exc:
            log.error("health.db_unreachable", error=str(exc))
            return JSONResponse({"status": "not_ready", "db": "unreachable"}, status_code=503)

    # ── Webhook routers (added in Phase 1) ─────────────────────────────────────
    # from comm_layer.webhooks.sms import router as sms_router
    # from comm_layer.webhooks.voice import router as voice_router
    # from comm_layer.webhooks.whatsapp import router as whatsapp_router
    # from comm_layer.webhooks.status import router as status_router
    # app.include_router(sms_router, prefix="/webhooks")
    # app.include_router(voice_router, prefix="/webhooks")
    # app.include_router(whatsapp_router, prefix="/webhooks")
    # app.include_router(status_router, prefix="/webhooks")

    return app


app = create_app()
