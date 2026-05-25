"""
Toggle Mock Azure CRM up/down for chaos demo.

Usage:
    python scripts/toggle_azure.py                    # flip current state
    python scripts/toggle_azure.py on                 # ensure service is UP
    python scripts/toggle_azure.py off                # ensure service is DOWN
    python scripts/toggle_azure.py --url http://...   # override mock CRM URL

The script calls POST /admin/toggle on the mock CRM and prints the new state.
Run this while the delivery worker is running to simulate Azure downtime and
watch events queue up in the DLQ — then bring it back up and replay.

WHY no `from comm_layer.config import settings`:
That import triggers pydantic-settings validation, which requires every Twilio,
Supabase, and OpenAI secret to be set just to flip a chaos flag. Reading the URL
directly from the env (with a sensible default) lets the script run on a fresh
checkout with zero secrets configured.
"""

from __future__ import annotations

import argparse
import os
import sys

import httpx

DEFAULT_URL = "http://localhost:8001"


def main() -> None:
    parser = argparse.ArgumentParser(description="Toggle mock Azure CRM up/down.")
    parser.add_argument(
        "state",
        nargs="?",
        choices=["on", "off"],
        help="'on' = service UP, 'off' = service DOWN. Omit to flip current state.",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("AZURE_CRM_URL", DEFAULT_URL),
        help=f"Mock CRM base URL (default: $AZURE_CRM_URL or {DEFAULT_URL}).",
    )
    args = parser.parse_args()
    base_url = args.url.rstrip("/")

    # If a specific state was requested, no-op when we're already there.
    if args.state is not None:
        try:
            health = httpx.get(f"{base_url}/health", timeout=5.0)
            health.raise_for_status()
        except httpx.RequestError as exc:
            print(f"ERROR: Cannot reach mock CRM at {base_url}: {exc}", file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as exc:
            print(f"ERROR: Health check failed: HTTP {exc.response.status_code}", file=sys.stderr)
            sys.exit(1)

        # Fail loudly if the health response shape is unexpected — silent
        # default-to-False would let toggle decisions stand on broken assumptions.
        body = health.json()
        if "down" not in body:
            print(
                f"ERROR: Unexpected /health response shape (no 'down' field): {body}",
                file=sys.stderr,
            )
            sys.exit(1)
        currently_down = bool(body["down"])
        want_down = args.state == "off"

        if currently_down == want_down:
            state_label = "DOWN" if currently_down else "UP"
            print(f"Mock CRM is already {state_label} — no change needed.")
            return

    try:
        response = httpx.post(f"{base_url}/admin/toggle", timeout=5.0)
        response.raise_for_status()
    except httpx.RequestError as exc:
        print(f"ERROR: Cannot reach mock CRM at {base_url}: {exc}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as exc:
        print(f"ERROR: Toggle failed with HTTP {exc.response.status_code}", file=sys.stderr)
        sys.exit(1)

    body = response.json()
    print(f"Mock Azure CRM is now {body['state']}")


if __name__ == "__main__":
    main()
