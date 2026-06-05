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
    # Create at: HubSpot → Settings → Integrations → Private Apps.
    # Required scopes: crm.objects.contacts.read, crm.objects.contacts.write,
    #                  crm.schemas.contacts.write, crm.objects.tickets.write,
    #                  crm.objects.tickets.read, crm.objects.tasks.write
    HUBSPOT_PRIVATE_APP_TOKEN: str
    # Base URL for all HubSpot API calls. Overridable in tests.
    HUBSPOT_BASE_URL: str = "https://api.hubapi.com"

    # Write a native Note on the contact timeline for every communication event.
    # Disable to keep properties-only mode (e.g. during migration or testing).
    HUBSPOT_NOTES_ENABLED: bool = True

    # Auto-create a Ticket for complaint intent or negative sentiment events.
    # Requires crm.objects.tickets.write scope on the Private App.
    HUBSPOT_TICKETS_ENABLED: bool = True

    # HubSpot Service pipeline and first-stage IDs for auto-created Tickets.
    # Defaults: "0" = default support pipeline, "1" = first open stage.
    # Find your values in HubSpot → Settings → Objects → Tickets → Pipelines.
    HUBSPOT_TICKET_PIPELINE: str = "0"
    HUBSPOT_TICKET_PIPELINE_STAGE: str = "1"
    # Stage id that means "Closed" in the support pipeline.
    # Used to decide whether an existing ticket can be reused (open) or a new one
    # should be created (closed). Default "4" matches HubSpot's default pipeline.
    # Confirm in HubSpot → Settings → Objects → Tickets → Pipelines if customised.
    HUBSPOT_TICKET_CLOSED_STAGE: str = "4"

    # Auto-create a real HubSpot Task (appears in the rep's to-do queue) when the
    # WhatsApp bot cannot answer a customer's question (reply_resolved=False).
    # Requires crm.objects.tasks.write scope on the Private App.
    HUBSPOT_TASKS_ENABLED: bool = True
    # How many hours from the original message time the Task should be due.
    HUBSPOT_TASK_DUE_OFFSET_HOURS: int = 24

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

    # ── WhatsApp auto-reply ──────────────────────────────────────────────────────
    # Master switch for the auto-reply chatbot. Set to False to stop replying
    # without disabling AI enrichment (which uses the DB-backed ai_enabled flag).
    WHATSAPP_AUTOREPLY_ENABLED: bool = True

    # Enable / disable the prompt-injection classifier guard (Layer 2).
    # Disable only in tests or to save cost on a zero-risk internal deployment.
    WHATSAPP_INJECTION_GUARD_ENABLED: bool = True

    # Model used by the prompt-injection classifier. GPT-4o-mini is cheap (~$0.0001
    # per screen) and fast enough for the secondary guard. Never use GPT-4o here —
    # the guard must be cheaper than the thing it protects.
    WHATSAPP_GUARD_MODEL: str = "gpt-4o-mini"

    # How long a claimed 'processing' reply row can stay in-flight before another
    # worker re-claims it (same lease pattern as ENRICHMENT_LEASE_SECONDS).
    # Must exceed worst-case GPT-4o + Twilio round-trip time (well under 30s).
    WHATSAPP_REPLY_LEASE_SECONDS: int = 120

    # Number of concurrent reply workers. Default 1 so messages from the same contact
    # are never answered in parallel (ordering risk). Increase only for high volume.
    WHATSAPP_REPLY_CONCURRENCY: int = 1

    # How many prior conversation turns to include in the GPT-4o context window
    # for multi-turn memory. Each turn = one inbound message + one reply (if any).
    WHATSAPP_REPLY_HISTORY_LIMIT: int = 10

    # Path to the markdown file describing the business (products, hours, policies).
    # Loaded once at worker startup and injected into every reply's system prompt.
    # You can edit this file without restarting the process — it's read at startup.
    BUSINESS_CONTEXT_PATH: str = str(
        Path(__file__).parent.parent / "intelligence_layer" / "business_context.md"
    )


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
