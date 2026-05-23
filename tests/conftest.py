"""
Shared pytest fixtures available to all tests.

WHY a conftest.py at the top level:
pytest automatically loads conftest.py files up the directory tree.
Fixtures defined here are available to every test without any import.

Approach to external dependencies in unit tests:
- We NEVER connect to a real database or call real APIs in unit tests.
- asyncpg and supabase clients are mocked using unittest.mock.
- This keeps tests fast, free, and deterministic.
- Integration tests (tests/integration/) use a real database and are
  clearly separated so you never accidentally run them in CI.
"""

import os
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

# ── Set minimal env vars before any imports so pydantic-settings doesn't fail ──
# These are fake values — unit tests never make real network calls.
os.environ.setdefault("TWILIO_ACCOUNT_SID", "ACtest00000000000000000000000000000")
os.environ.setdefault("TWILIO_AUTH_TOKEN", "test_auth_token_for_unit_tests_only")
os.environ.setdefault("TWILIO_PHONE_NUMBER", "+15550000000")
os.environ.setdefault("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
os.environ.setdefault("SUPABASE_URL", "https://fake.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "fake_service_role_key")
os.environ.setdefault("DATABASE_URL", "postgresql://fake:fake@localhost:5432/fake")
os.environ.setdefault("OPENAI_API_KEY", "sk-fake")
os.environ.setdefault("COHERE_API_KEY", "fake_cohere_key")
os.environ.setdefault("DEEPGRAM_API_KEY", "fake_deepgram_key")
os.environ.setdefault("AZURE_CRM_URL", "http://localhost:8001")
os.environ.setdefault("AI_ENABLED", "false")
os.environ.setdefault("PUBLIC_BASE_URL", "https://fake.ngrok-free.app")


# ── Reusable fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def sample_correlation_id() -> uuid.UUID:
    """A stable UUID for use in tests — same value every test run."""
    return uuid.UUID("550e8400-e29b-41d4-a716-446655440000")


@pytest.fixture
def sample_timestamp() -> datetime:
    """A stable UTC datetime for use in tests."""
    return datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)


@pytest.fixture
def mock_asyncpg_pool():
    """
    A mock asyncpg connection pool.

    Returns an AsyncMock that mimics the context manager pattern:
        async with pool.acquire() as conn:
            await conn.execute(...)
    """
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=None)
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_conn.fetchval = AsyncMock(return_value=1)

    # transaction() is also a context manager
    mock_conn.transaction = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))

    mock_pool = AsyncMock()
    mock_pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=False),
    ))
    mock_pool.fetchval = AsyncMock(return_value=1)

    return mock_pool, mock_conn
