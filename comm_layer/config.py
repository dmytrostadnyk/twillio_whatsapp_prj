"""
Application configuration.

WHY pydantic-settings:
- Reads values from environment variables automatically.
- Fails loudly at startup if a required variable is missing — no silent None values.
- Type-coerces automatically (e.g. AI_ENABLED="false" becomes bool False).
- One import gives you a fully validated settings object anywhere in the codebase.

Usage:
    from comm_layer.config import settings
    print(settings.TWILIO_ACCOUNT_SID)
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute path to the project-root .env so settings load correctly regardless
# of the process CWD (e.g. Streamlit changes CWD to the dashboard/ folder).
_ENV_FILE = Path(__file__).parent.parent / ".env"


class Settings(BaseSettings):
    """All configuration is read from environment variables (or .env file in dev)."""

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),  # load .env in development
        env_file_encoding="utf-8",
        case_sensitive=True,      # variable names are case-sensitive
        extra="ignore",           # ignore unknown env vars (other services may set extras)
    )

    # ── Twilio ──────────────────────────────────────────────────────────────────
    TWILIO_ACCOUNT_SID: str
    TWILIO_AUTH_TOKEN: str
    TWILIO_PHONE_NUMBER: str
    TWILIO_WHATSAPP_NUMBER: str

    # ── Supabase ────────────────────────────────────────────────────────────────
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: str
    DATABASE_URL: str             # direct Postgres URL for asyncpg

    # ── AI providers ────────────────────────────────────────────────────────────
    OPENAI_API_KEY: str
    COHERE_API_KEY: str

    # ── HubSpot CRM ─────────────────────────────────────────────────────────────
    # Service Key token — required, no default. Fails loudly at startup if unset.
    # Create at: HubSpot → Settings → Integrations → Service Keys.
    # Required scopes: crm.objects.contacts.read, crm.objects.contacts.write,
    #                  crm.schemas.contacts.write
    HUBSPOT_PRIVATE_APP_TOKEN: str
    # Base URL for all HubSpot API calls. Overridable in tests.
    HUBSPOT_BASE_URL: str = "https://api.hubapi.com"

    # ── AI kill switch (env-var seed only) ─────────────────────────────────────
    # This env-var seeds the app_settings.ai_enabled DB flag (migration 0011).
    # At runtime, all AI checks read from the DB (comm_layer.db.ai_enabled).
    # Changing this env-var alone requires a process restart; use the DB flag
    # for live toggles without restart.
    AI_ENABLED: bool = True

    # ── Delivery worker tuning ──────────────────────────────────────────────────
    DELIVERY_MAX_ATTEMPTS: int = 8
    DELIVERY_BACKOFF_BASE_SECONDS: float = 5.0
    # Cap on how long any single backoff can be. Without this, attempt counts
    # in the double digits would compute multi-hour delays. 5 minutes is long
    # enough to let HubSpot recover but short enough to keep the queue moving.
    DELIVERY_BACKOFF_MAX_SECONDS: float = 300.0
    DELIVERY_POLL_INTERVAL_SECONDS: float = 5.0
    # How long a claimed row is hidden from other workers while being processed.
    # If the worker crashes before ack/nack, the row becomes claimable again
    # after this many seconds. Must be well above the expected processing time.
    DELIVERY_LEASE_SECONDS: int = 60

    # ── HubSpot rate limiting ────────────────────────────────────────────────────
    # Client-side throttle so we never accidentally burst into HubSpot's limits.
    # HubSpot free tier allows ~110 requests/10s with daily caps — stay well under.
    HUBSPOT_RATE_LIMIT_PER_MINUTE: int = 100

    # ── Rate limiting ───────────────────────────────────────────────────────────
    ENRICHMENT_CONCURRENCY: int = 3
    OUTBOUND_RATE_LIMIT_PER_MINUTE: int = 5
    # How long an enrichments row may stay 'processing' before another worker
    # re-claims it. Must exceed worst-case GPT-4o latency incl. retries (~30s).
    # A crashed worker leaves 'processing' rows; the lease bounds the silence gap.
    ENRICHMENT_LEASE_SECONDS: int = 120

    # ── Embeddings + semantic search (Phase 8) ──────────────────────────────────
    # Number of concurrent embedding workers (background consumer). Each worker
    # holds an open OpenAI HTTP connection while embedding — 3 is well under
    # the default text-embedding-3-small rate limits.
    EMBEDDING_CONCURRENCY: int = 3
    # OpenAI model used at BOTH index time and query time. They MUST match —
    # mixing models produces meaningless cosine distances.
    EMBEDDING_MODEL: str = "text-embedding-3-small"
    # Cohere reranker model. v3.0 is English-tuned; for multilingual use the
    # multilingual variant.
    RERANK_MODEL: str = "rerank-english-v3.0"
    # How many candidates we pull from pgvector before reranking. Bigger pool
    # = better Cohere rerank quality, but a small Cohere bill per query.
    SEARCH_CANDIDATE_POOL: int = 20
    # Default page size returned to the caller after reranking.
    SEARCH_DEFAULT_LIMIT: int = 10

    # ── Application ─────────────────────────────────────────────────────────────
    LOG_LEVEL: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    PUBLIC_BASE_URL: str = "https://localhost"

    # ── Voice recording ─────────────────────────────────────────────────────────
    MAX_RECORDING_DURATION_SECONDS: int = 3600


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the cached settings singleton.

    WHY lru_cache: we only want to parse env vars once. Every subsequent call
    returns the same object — no repeated disk reads or validation.
    """
    return Settings()


# Module-level alias so you can do: from comm_layer.config import settings
settings = get_settings()
