"""
Shared fixtures for integration tests.

WHY this file exists:
- Integration tests need a real asyncpg pool (not a mock) connected to a real DB.
- They patch AI calls so tests are deterministic and free.
- A safety guard prevents accidental runs against a production database.

HOW to enable integration tests:
    export DATABASE_URL="postgresql://..."
    export INTEGRATION_TEST_CONFIRMED=1
    pytest tests/integration -v

Without both env vars, every test in this folder is SKIPPED.
"""

from __future__ import annotations

import json
import os
import pathlib
import uuid

import asyncpg
import pytest

from comm_layer.contracts.enriched import EnrichmentData

# ── Safety guard ──────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
CONFIRMED = os.environ.get("INTEGRATION_TEST_CONFIRMED", "").strip() == "1"

_SKIP_REASON = (
    "Integration tests are disabled. "
    "Set DATABASE_URL and INTEGRATION_TEST_CONFIRMED=1 to enable them. "
    "WARNING: tests INSERT and DELETE rows — only point at a dev/test database."
)

_INTEGRATION_DIR = str(pathlib.Path(__file__).parent)


def pytest_collection_modifyitems(config, items):
    """
    Skip all tests in tests/integration/ if the required env vars are absent.

    WHY a hook instead of pytestmark: pytestmark in conftest.py applies only to
    tests defined directly in conftest.py, not to tests in sibling test files.
    pytest_collection_modifyitems runs after collection but before fixture setup,
    so marking tests here prevents the asyncpg pool from being created needlessly.
    """
    if DATABASE_URL and CONFIRMED:
        return  # env vars present — run normally

    skip = pytest.mark.skip(reason=_SKIP_REASON)
    for item in items:
        if _INTEGRATION_DIR in str(item.fspath):
            item.add_marker(skip, append=False)


# ── asyncpg JSONB codec ────────────────────────────────────────────────────────

async def _init_conn(conn: asyncpg.Connection) -> None:
    """Register JSON/JSONB codecs so asyncpg returns dicts instead of raw strings."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


# ── Session-scoped pool ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
async def pool() -> asyncpg.Pool:
    """
    One asyncpg pool shared across all integration tests in the session.

    WHY session scope: pool creation takes ~100-300ms. Creating one per test
    would add several seconds to the suite for no benefit.
    """
    p = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=2,
        max_size=5,
        command_timeout=30,
        init=_init_conn,
    )
    try:
        yield p
    finally:
        await p.close()


# ── Per-test prefix + cleanup ──────────────────────────────────────────────────

@pytest.fixture
def test_prefix() -> str:
    """
    Unique prefix for all rows this test inserts.

    WHY: Lets multiple tests run in parallel without colliding on event_key.
    The cleanup fixture below deletes all rows with this prefix, so tests
    are isolated without needing nested transactions (which would conflict
    with SELECT FOR UPDATE SKIP LOCKED inside the functions under test).
    """
    return f"INTEG-{uuid.uuid4().hex[:8]}-"


@pytest.fixture(autouse=True)
async def _cleanup(pool: asyncpg.Pool, test_prefix: str) -> None:
    """
    Delete every comm_events row created by this test, regardless of pass/fail.

    WHY DELETE instead of transaction rollback: the functions under test open
    their own internal transactions (SELECT FOR UPDATE SKIP LOCKED, broker
    ack/nack). Wrapping them in an outer transaction and rolling back would
    conflict with those internal transactions.

    FK CASCADE on enrichments/embeddings/transcripts/delivery_log means we
    only need to DELETE from comm_events — child rows disappear automatically.
    """
    yield
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM comm_events WHERE event_key LIKE $1",
            f"{test_prefix}%",
        )


# ── Supabase async client ──────────────────────────────────────────────────────

@pytest.fixture
async def supabase():
    """Real async Supabase client for tests that exercise enrichment writes."""
    from comm_layer.db import create_supabase_client
    return await create_supabase_client()


# ── AI mock fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def fake_gpt4o(monkeypatch):
    """
    Patch GPT-4o so enrichment tests never call the real OpenAI API.

    Returns a deterministic EnrichmentData so assertions are stable.
    The patch target is the lowest-level retry wrapper — not the SDK class —
    so it survives SDK version changes.
    """
    def _fake(content: str, event_type: str, comm_event_id: str) -> EnrichmentData:
        return EnrichmentData(
            summary="Test summary for integration test.",
            intent="general_query",
            sentiment="neutral",
            entities=[],
            action_items=[],
        )

    monkeypatch.setattr(
        "intelligence_layer.enrichment._call_gpt4o_with_retries", _fake
    )


@pytest.fixture
def fake_openai_embedding(monkeypatch):
    """
    Patch the OpenAI embedding call so embedding tests never call the real API.

    The same fixed 1536-dim vector is used for both indexing and querying,
    which means cosine similarity between any two test rows = 1.0 (identical
    vectors), making search assertions simple and deterministic.
    """
    vec = [0.001 * (i % 1000) for i in range(1536)]

    monkeypatch.setattr(
        "intelligence_layer.embedding._embed_with_retries",
        lambda content, comm_event_id: vec,
    )
    monkeypatch.setattr(
        "intelligence_layer.search._embed_query_sync",
        lambda text: vec,
    )


@pytest.fixture
def fake_cohere(monkeypatch):
    """
    Make Cohere rerank a pass-through so search tests use pgvector order.

    WHY: Cohere reranking requires a live network call. The integration test
    for search cares about whether the right row is returned — not about
    reranking correctness (which is tested in unit tests). Pass-through keeps
    the pgvector similarity score intact as the rerank_score.
    """
    def _passthrough(query: str, candidates: list[dict], limit: int) -> list[dict]:
        return [{**c, "rerank_score": c["similarity"]} for c in candidates[:limit]]

    monkeypatch.setattr(
        "intelligence_layer.search._rerank_with_fallback", _passthrough
    )
