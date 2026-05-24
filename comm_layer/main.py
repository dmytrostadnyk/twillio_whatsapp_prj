"""
Communication Layer — FastAPI application entry point.

Startup sequence:
  1. Configure structured logging
  2. Create asyncpg connection pool
  3. Create Supabase client
  4. Create the PostgresBroker (wraps the pool)
  5. Register all webhook routers
  6. Expose health check endpoints

Shutdown: close the connection pool cleanly.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from comm_layer.broker.postgres import PostgresBroker
from comm_layer.config import settings
from comm_layer.db import create_pool, create_supabase_client
from comm_layer.logging_config import configure_logging
from comm_layer.webhooks import sms, status, voice, whatsapp

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Runs on startup (before first request) and shutdown (after last request).
    All shared resources — DB pool, supabase client, broker — live here.
    """
    configure_logging(settings.LOG_LEVEL)
    log.info("comm_layer.starting")

    # Shared asyncpg pool — used by the broker and all webhook handlers
    app.state.pool = await create_pool()

    # Supabase client — used for higher-level CRUD operations
    app.state.supabase = await create_supabase_client()

    # Broker wraps the pool; used by webhook handlers to queue events
    app.state.broker = PostgresBroker(pool=app.state.pool)

    log.info("comm_layer.ready", public_url=settings.PUBLIC_BASE_URL)
    yield

    await app.state.pool.close()
    log.info("comm_layer.stopped")


def create_app() -> FastAPI:
    """Factory that creates and configures the FastAPI application."""
    app = FastAPI(
        title="Twilio Communication Layer",
        description=(
            "Durable Twilio event ingestion — validates, deduplicates, "
            "persists, and queues all Voice/SMS/WhatsApp events."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    # ── Clear structlog context on every request so correlation IDs from one
    #    request never bleed into the next.
    @app.middleware("http")
    async def reset_log_context(request: Request, call_next):
        structlog.contextvars.clear_contextvars()
        return await call_next(request)

    # ── Health checks ─────────────────────────────────────────────────────────

    @app.get("/health/live", tags=["Health"])
    async def liveness() -> JSONResponse:
        """Is the process running? Used by Docker / orchestrators."""
        return JSONResponse({"status": "ok"})

    @app.get("/health/ready", tags=["Health"])
    async def readiness() -> JSONResponse:
        """Can the process serve traffic? Confirms DB is reachable."""
        try:
            await app.state.pool.fetchval("SELECT 1")
            return JSONResponse({"status": "ready", "db": "ok"})
        except Exception as exc:
            log.error("health.db_unreachable", error=str(exc))
            return JSONResponse(
                {"status": "not_ready", "db": "unreachable"}, status_code=503
            )

    # ── Webhook routers ───────────────────────────────────────────────────────
    app.include_router(sms.router, prefix="/webhooks", tags=["Webhooks"])
    app.include_router(voice.router, prefix="/webhooks", tags=["Webhooks"])
    app.include_router(whatsapp.router, prefix="/webhooks", tags=["Webhooks"])
    app.include_router(status.router, prefix="/webhooks", tags=["Webhooks"])

    return app


app = create_app()
