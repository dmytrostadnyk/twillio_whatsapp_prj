"""
Structured JSON logging setup using structlog.

WHY structlog over Python's built-in logging:
- Every log line is valid JSON — parseable by log aggregators (Datadog, Loki, Cloudwatch).
- Correlation IDs and other fields are first-class citizens, not string interpolation.
- The developer sees human-readable output in dev; the CI/production sees JSON.

Usage:
    import structlog
    log = structlog.get_logger(__name__)
    log.info("event.ingested", event_key="SM123:sms.received", correlation_id=str(cid))

Call configure_logging() once at application startup (in main.py).
"""

import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog for JSON output in production, pretty output in dev.

    WHY we configure both stdlib logging AND structlog:
    Libraries (FastAPI, asyncpg, etc.) use stdlib logging. By routing it through
    structlog's processor chain, all log lines — ours AND the library's — end up
    in the same JSON format with the same field names.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Shared processors run on every log record regardless of source
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,     # thread-local context (correlation_id, etc.)
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    # Quieten noisy library loggers that aren't useful at INFO level
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
