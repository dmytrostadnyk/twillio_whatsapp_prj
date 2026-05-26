"""
Delivery Health view — shows delivery pipeline status and dead-letter queue.

The DLQ replay is wrapped in a two-step safety mechanism:
1. "Dry-run" button: counts what would be replayed without changing anything.
2. Confirmation checkbox + "Replay now" primary button: actual replay.

replay_dead_letters() is async (uses asyncpg), so it goes through run_async().
"""

from __future__ import annotations

from collections import Counter

import pandas as pd
import streamlit as st
import structlog

from dashboard.db import get_supabase, run_async
from delivery_worker.replay import replay_dead_letters

log = structlog.get_logger(__name__)

_ALL_STATUSES = ["received", "pending", "delivered", "failed", "dead"]


@st.cache_data(ttl=10)
def _fetch_status_counts() -> list[dict]:
    """Fetch delivery_status for all events. Returns [] on error."""
    client = get_supabase()
    try:
        result = client.table("comm_events").select("delivery_status").execute()
        return result.data or []
    except Exception as exc:
        log.error("dashboard.delivery.status_count_failed", error=str(exc))
        st.error(f"Failed to load delivery status counts: {exc}")
        return []


@st.cache_data(ttl=10)
def _fetch_dlq() -> list[dict]:
    """Fetch dead-lettered events for the DLQ table. Returns [] on error."""
    client = get_supabase()
    try:
        result = client.table("comm_events").select(
            "id, event_key, channel, event_type, "
            "from_number, last_error, attempt_count, created_at"
        ).eq("delivery_status", "dead").order("created_at", desc=True).limit(50).execute()
        return result.data or []
    except Exception as exc:
        log.error("dashboard.delivery.dlq_fetch_failed", error=str(exc))
        st.error(f"Failed to load dead-letter queue: {exc}")
        return []


def _run_replay(dry_run: bool) -> int | None:
    """Bridge async replay_dead_letters() into synchronous Streamlit context."""
    try:
        return run_async(lambda pool: replay_dead_letters(pool, dry_run=dry_run))
    except Exception as exc:
        log.error("dashboard.delivery.replay_failed", dry_run=dry_run, error=str(exc))
        st.error(f"Replay failed: {exc}")
        return None


def show() -> None:
    """Render the Delivery Health page."""
    st.header("Delivery Health")
    st.caption("Pipeline delivery status and dead-letter queue management")

    # ── Delivery status bar chart ──────────────────────────────────────────────
    st.subheader("Events by delivery status")
    rows = _fetch_status_counts()
    if rows:
        counts = Counter(r["delivery_status"] for r in rows)
        status_data = {s: counts.get(s, 0) for s in _ALL_STATUSES}
        status_df = pd.DataFrame.from_dict(
            status_data, orient="index", columns=["count"]
        )
        st.bar_chart(status_df)

        # Quick metrics
        c1, c2, c3, c4, c5 = st.columns(5)
        for col, status in zip([c1, c2, c3, c4, c5], _ALL_STATUSES):
            col.metric(status.capitalize(), counts.get(status, 0))
    else:
        st.info("No events in the database yet.")

    st.divider()

    # ── Dead-letter queue table ────────────────────────────────────────────────
    st.subheader("Dead-letter queue")
    dlq_rows = _fetch_dlq()

    if not dlq_rows:
        st.success("Dead-letter queue is empty — all events delivered or in progress.")
    else:
        st.warning(f"{len(dlq_rows)} event(s) in dead-letter state")
        dlq_df = pd.DataFrame(dlq_rows)[[
            "created_at", "event_key", "channel", "event_type",
            "attempt_count", "last_error"
        ]]
        st.dataframe(dlq_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── DLQ Replay controls ────────────────────────────────────────────────────
    st.subheader("Replay dead-letter queue")
    st.warning(
        "Authorized operators only. Replaying resets all dead-lettered events to "
        "'pending' so the delivery worker attempts them again from scratch."
    )

    if st.button("Dry-run replay (safe — no changes)"):
        with st.spinner("Counting dead-letter events…"):
            count = _run_replay(dry_run=True)
        if count is not None:
            if count == 0:
                st.info("Nothing to replay — dead-letter queue is empty.")
            else:
                st.info(f"Dry run: would replay {count} dead-lettered event(s).")

    st.divider()

    confirm = st.checkbox("I confirm I have authorization to replay all dead-letter events.")
    if confirm:
        if st.button("Replay now", type="primary"):
            with st.spinner("Replaying dead-letter events…"):
                replayed = _run_replay(dry_run=False)
            if replayed is not None:
                if replayed == 0:
                    st.info("Nothing to replay — dead-letter queue was already empty.")
                else:
                    st.success(
                        f"Replayed {replayed} event(s). "
                        "The delivery worker will pick them up on its next poll."
                    )
                # Clear the cached DLQ so the table refreshes immediately
                _fetch_dlq.clear()
                _fetch_status_counts.clear()
