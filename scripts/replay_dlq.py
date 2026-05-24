"""
Dead-letter queue replay CLI.

Thin wrapper around delivery_worker.replay.replay_dead_letters so the
ops command and the unit-tested implementation stay in sync.

Usage:
    python scripts/replay_dlq.py             # actually replay
    python scripts/replay_dlq.py --dry-run   # list what would be replayed
"""

from __future__ import annotations

import argparse
import asyncio

from comm_layer.config import settings
from comm_layer.db import create_pool
from comm_layer.logging_config import configure_logging
from delivery_worker.replay import replay_dead_letters


async def _run(dry_run: bool) -> None:
    configure_logging(settings.LOG_LEVEL)
    pool = await create_pool()
    try:
        await replay_dead_letters(pool, dry_run=dry_run)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay dead-lettered events.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be replayed without modifying any rows.",
    )
    args = parser.parse_args()
    asyncio.run(_run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
