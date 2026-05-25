"""
Toggle Mock Azure CRM up/down for chaos demo.

Usage:
    python scripts/toggle_azure.py        # toggle (flip current state)
    python scripts/toggle_azure.py on     # ensure service is UP
    python scripts/toggle_azure.py off    # ensure service is DOWN

The script calls POST /admin/toggle on the mock CRM and prints the new state.
Run this while the delivery worker is running to simulate Azure downtime and
watch events queue up in the DLQ — then bring it back up and replay.
"""

from __future__ import annotations

import argparse
import sys

import httpx

from comm_layer.config import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Toggle mock Azure CRM up/down.")
    parser.add_argument(
        "state",
        nargs="?",
        choices=["on", "off"],
        help="'on' = service UP, 'off' = service DOWN. Omit to flip current state.",
    )
    args = parser.parse_args()

    base_url = settings.AZURE_CRM_URL

    # Check current state first if a specific target was requested
    if args.state is not None:
        try:
            current = httpx.get(f"{base_url}/health", timeout=5.0).json()
        except httpx.RequestError as exc:
            print(f"ERROR: Cannot reach mock CRM at {base_url}: {exc}", file=sys.stderr)
            sys.exit(1)

        currently_down = current.get("down", False)
        want_down = args.state == "off"

        if currently_down == want_down:
            state_label = "DOWN" if currently_down else "UP"
            print(f"Mock CRM is already {state_label} — no change needed.")
            return

    # Perform the toggle
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
