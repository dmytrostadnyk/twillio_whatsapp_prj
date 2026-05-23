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

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration is read from environment variables (or .env file in dev)."""

    model_config = SettingsConfigDict(
        env_file=".env",          # load .env in development
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
    DEEPGRAM_API_KEY: str

    # ── Mock Azure CRM ──────────────────────────────────────────────────────────
    AZURE_CRM_URL: str = "http://localhost:8001"

    # ── AI kill switch ──────────────────────────────────────────────────────────
    # Set to False to instantly halt ALL AI calls without restarting the service.
    AI_ENABLED: bool = True

    # ── Delivery worker tuning ──────────────────────────────────────────────────
    DELIVERY_MAX_ATTEMPTS: int = 8
    DELIVERY_BACKOFF_BASE_SECONDS: float = 5.0
    DELIVERY_POLL_INTERVAL_SECONDS: float = 5.0

    # ── Rate limiting ───────────────────────────────────────────────────────────
    ENRICHMENT_CONCURRENCY: int = 3
    OUTBOUND_RATE_LIMIT_PER_MINUTE: int = 5

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
