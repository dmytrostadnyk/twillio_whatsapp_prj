"""
Database connections.

Two clients:
1. asyncpg pool  — raw SQL for the broker queue (SELECT FOR UPDATE SKIP LOCKED).
2. supabase-py   — higher-level CRUD for everything else (inserts, lookups).

WHY two clients:
The supabase-py client provides a clean API for reads and writes but doesn't
expose the low-level Postgres connection we need for SELECT FOR UPDATE SKIP LOCKED.
asyncpg gives us that control for the queue pattern only — everything else
goes through the supabase client.

Usage in FastAPI lifespan:
    app.state.pool = await create_pool()
    app.state.supabase = create_supabase_client()
    # ... on shutdown ...
    await app.state.pool.close()
"""

from __future__ import annotations

import asyncpg
import structlog
from supabase import AsyncClient, acreate_client

from comm_layer.config import settings

log = structlog.get_logger(__name__)


async def create_pool() -> asyncpg.Pool:
    """
    Create the asyncpg connection pool.

    min_size=2: keep a couple of connections warm so the first request
                doesn't pay a connection setup cost.
    max_size=10: cap connections to avoid overwhelming Supabase's free tier
                 (which has a limit of ~15 concurrent connections).
    """
    pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=30,    # raise an error if a single query takes > 30s
    )
    log.info("db.pool_created", min_size=2, max_size=10)
    return pool


async def create_supabase_client() -> AsyncClient:
    """
    Create the Supabase async client.

    We use the service-role key here because this is backend code.
    The service-role key bypasses Row Level Security, which is what we want
    for server-side operations. NEVER expose this key to a browser or client.
    """
    client = await acreate_client(
        supabase_url=settings.SUPABASE_URL,
        supabase_key=settings.SUPABASE_SERVICE_ROLE_KEY,
    )
    log.info("db.supabase_client_created")
    return client
