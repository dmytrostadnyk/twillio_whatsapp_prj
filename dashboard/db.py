"""
Shared database helpers for the Streamlit dashboard.

Two access patterns:
1. get_supabase() — sync Supabase client via PostgREST for all tabular reads
   (feed, stats, delivery). Cached once per Streamlit session.
2. run_async(coro_factory) — spins up a small asyncpg pool, runs the coroutine,
   closes the pool. Used for search (search_events) and DLQ replay
   (replay_dead_letters), which are both async by nature.

WHY not asyncio everywhere: Streamlit re-runs the whole script on every interaction.
Creating event loops inside a Streamlit script is fragile. Restricting async to the
two places that truly need it (search, replay) keeps the rest of the code simple.
"""

from __future__ import annotations

import asyncio

import asyncpg
import streamlit as st
import structlog
from supabase import Client, create_client

from comm_layer.config import settings

log = structlog.get_logger(__name__)


@st.cache_resource
def get_supabase() -> Client:
    """Return the shared sync Supabase client. Created once per Streamlit session."""
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)


def run_async(coro_factory) -> object:
    """
    Run an async function that needs an asyncpg pool.

    coro_factory is a callable that accepts a pool and returns a coroutine:
        run_async(lambda pool: search_events(pool, query))

    WHY a factory instead of a coroutine: passing an already-created coroutine
    across asyncio.run() boundaries is not safe. The factory is called INSIDE
    the new event loop so the coroutine is always created in the right context.
    """
    async def _inner():
        pool = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=1,
            max_size=2,
            command_timeout=30,
        )
        try:
            return await coro_factory(pool)
        finally:
            await pool.close()

    return asyncio.run(_inner())


def normalize_embed(embed) -> dict | None:
    """
    PostgREST may return an embedded resource as a list (one-to-many) or a single
    dict (when it detects the UNIQUE constraint). Handle both so the views don't crash.
    """
    if isinstance(embed, list):
        return embed[0] if embed else None
    if isinstance(embed, dict):
        return embed
    return None
