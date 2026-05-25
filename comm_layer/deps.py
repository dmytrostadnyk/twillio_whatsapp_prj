"""
Shared FastAPI dependencies.

WHY a separate deps.py:
Every webhook handler needs access to the DB pool and the broker.
By centralising them here as dependency functions, we get two benefits:
1. In production, they read from app.state (set in the lifespan).
2. In tests, we override them with mocks using app.dependency_overrides —
   no real database connection needed for unit tests.

Usage in a handler:
    from comm_layer.deps import get_pool, get_broker, get_supabase

    @router.post("/webhooks/sms")
    async def sms_handler(
        pool: Annotated[asyncpg.Pool, Depends(get_pool)],
        broker: Annotated[Broker, Depends(get_broker)],
        supabase: Annotated[AsyncClient, Depends(get_supabase)],
    ):
        ...
"""

from __future__ import annotations

import asyncpg
from fastapi import Request
from supabase import AsyncClient

from comm_layer.broker.base import Broker


def get_pool(request: Request) -> asyncpg.Pool:
    """Return the shared asyncpg connection pool from app state."""
    return request.app.state.pool


def get_broker(request: Request) -> Broker:
    """Return the shared Broker instance from app state."""
    return request.app.state.broker


def get_supabase(request: Request) -> AsyncClient:
    """Return the shared Supabase async client from app state."""
    return request.app.state.supabase
