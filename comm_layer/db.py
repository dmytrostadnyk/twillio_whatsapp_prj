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

import json

import asyncpg
import structlog
from supabase import AsyncClient, acreate_client

from comm_layer.config import settings

log = structlog.get_logger(__name__)


async def _init_connection(conn: asyncpg.Connection) -> None:
    """
    Register a JSONB codec on every new connection in the pool.

    WHY: asyncpg returns JSONB columns as raw strings by default. Any code that
    reads `raw_payload` (or any jsonb column) back as a dict would have to call
    json.loads() manually — easy to forget, and a single missed spot becomes a
    crash. Registering once at connection setup means asyncpg returns native
    dicts/lists for jsonb columns everywhere in the codebase.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


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
        init=_init_connection,
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
