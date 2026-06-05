"""
Live Feed view — shows the 100 most recent comm_events with enrichment detail.

The PostgREST select embeds the related enrichments row via the FK
(enrichments.comm_event_id → comm_events.id). PostgREST may return the embed
as a list or a single dict depending on whether it detected the UNIQUE constraint;
normalize_embed() handles both cases.

Auto-refresh: when the checkbox is on, the script sleeps 10s then calls st.rerun().
Known UX limitation: the Streamlit thread is blocked during that sleep, so toggling
the checkbox off has up to 10s lag. Acceptable for a portfolio demo.
"""

from __future__ import annotations

import json
import time

import pandas as pd
import streamlit as st
import structlog

from dashboard.db import get_supabase, normalize_embed

log = structlog.get_logger(__name__)

# Columns shown in the summary table — no raw_payload or PII-heavy fields
_TABLE_COLUMNS = [
    "created_at",
    "channel",
    "direction",
    "event_type",
    "delivery_status",
    "attempt_count",
    "intent",
    "sentiment",
]


def _fetch_events() -> list[dict]:
    """Fetch recent events joined with enrichments. Returns [] on error."""
    client = get_supabase()
    try:
        result = client.table("comm_events").select(
            "id, event_key, channel, direction, event_type, "
            "from_number, to_number, delivery_status, attempt_count, created_at, "
            "enrichments(summary, intent, sentiment, action_items, status, failure_reason)"
        ).eq("direction", "inbound").order("created_at", desc=True).limit(100).execute()
        return result.data or []
    except Exception as exc:
        log.error("dashboard.feed.query_failed", error=str(exc))
        st.error(f"Failed to load events: {exc}")
        return []


def _flatten_rows(rows: list[dict]) -> list[dict]:
    """Move enrichment fields up to the top level for easy table display."""
    flat = []
    for row in rows:
        enrichment = normalize_embed(row.get("enrichments"))
        flat.append({
            "id": row["id"],
            "created_at": row["created_at"],
            "channel": row["channel"],
            "direction": row["direction"],
            "event_type": row["event_type"],
            "from_number": row.get("from_number", ""),
            "to_number": row.get("to_number", ""),
            "delivery_status": row["delivery_status"],
            "attempt_count": row["attempt_count"],
            "intent": enrichment.get("intent", "") if enrichment else "",
            "sentiment": enrichment.get("sentiment", "") if enrichment else "",
            # Keep full enrichment for the expander detail
            "_enrichment": enrichment,
        })
    return flat


def show() -> None:
    """Render the Live Feed page."""
    st.header("Live Feed")
    st.caption("100 most recent communication events with AI enrichment")

    auto_refresh = st.checkbox("Auto-refresh every 10s")

    rows = _fetch_events()
    if not rows:
        st.info("No events found.")
        if auto_refresh:
            # Short sleep so the refresh works even on an empty DB
            time.sleep(10)
            st.rerun()
        return

    flat = _flatten_rows(rows)
    df = pd.DataFrame(flat)

    # Show the summary table — drop internal _enrichment column
    display_df = df[_TABLE_COLUMNS].copy()
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Event detail")

    # Let operator pick any event by its event_key
    event_keys = [r["event_key"] for r in rows]
    selected_key = st.selectbox("Select event", event_keys)

    if selected_key:
        selected = next((r for r in rows if r["event_key"] == selected_key), None)
        if selected:
            enrichment = normalize_embed(selected.get("enrichments"))

            col1, col2 = st.columns(2)
            with col1:
                st.write("**Channel:**", selected["channel"])
                st.write("**Direction:**", selected["direction"])
                st.write("**Event type:**", selected["event_type"])
                st.write("**Delivery status:**", selected["delivery_status"])
                st.write("**Attempt count:**", selected["attempt_count"])
            with col2:
                st.write("**From:**", selected.get("from_number") or "—")
                st.write("**To:**", selected.get("to_number") or "—")
                st.write("**Created:**", selected["created_at"])

            if enrichment:
                with st.expander("AI enrichment", expanded=True):
                    st.write("**Intent:**", enrichment.get("intent") or "—")
                    st.write("**Sentiment:**", enrichment.get("sentiment") or "—")
                    if enrichment.get("summary"):
                        st.write("**Summary:**")
                        st.write(enrichment["summary"])
                    raw = enrichment.get("action_items") or []
                    if isinstance(raw, str):
                        try:
                            raw = json.loads(raw)
                        except (json.JSONDecodeError, ValueError):
                            raw = []
                    action_items = raw if isinstance(raw, list) else []
                    if action_items:
                        st.write("**Action items:**")
                        for item in action_items:
                            if isinstance(item, dict):
                                priority = item.get("priority", "")
                                desc = item.get("description", str(item))
                            else:
                                desc = str(item)
                                priority = ""
                            st.write(f"- [{priority}] {desc}")
                    if enrichment.get("status") == "failed":
                        st.warning(f"Enrichment failed: {enrichment.get('failure_reason')}")
            else:
                st.info("No enrichment yet for this event.")

    # Auto-refresh — sleeps here; toggling checkbox off has up to 10s lag
    if auto_refresh:
        time.sleep(10)
        st.rerun()
