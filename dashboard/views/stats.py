"""
Enrichment Stats view — charts and metrics for the AI enrichment pipeline.

Queries are wrapped in @st.cache_data(ttl=10) so clicking around the dashboard
doesn't hammer Supabase on every render — data is at most 10s stale.

Intent taxonomy (closed set from HANDOFF — do not change without updating
the dashboard groupings too):
    support_request, sales_inquiry, complaint, appointment,
    billing_question, cancellation, general_query
"""

from __future__ import annotations

from collections import Counter

import pandas as pd
import streamlit as st
import structlog

from dashboard.db import get_supabase

log = structlog.get_logger(__name__)

_ALL_INTENTS = [
    "support_request",
    "sales_inquiry",
    "complaint",
    "appointment",
    "billing_question",
    "cancellation",
    "general_query",
]

_ALL_SENTIMENTS = ["positive", "neutral", "negative"]

_ALL_EMBED_STATUSES = ["pending", "processing", "completed", "failed"]


@st.cache_data(ttl=10)
def _fetch_enrichments() -> list[dict]:
    """Fetch enrichment fields for aggregation. Returns [] on error."""
    client = get_supabase()
    try:
        result = client.table("enrichments").select(
            "intent, sentiment, status, embedding_status, failure_reason"
        ).execute()
        return result.data or []
    except Exception as exc:
        log.error("dashboard.stats.enrichments_failed", error=str(exc))
        st.error(f"Failed to load enrichments: {exc}")
        return []


@st.cache_data(ttl=10)
def _fetch_event_count() -> int | None:
    """Total number of comm_events. Returns None on error."""
    client = get_supabase()
    try:
        result = client.table("comm_events").select("id", count="exact").execute()
        return result.count
    except Exception as exc:
        log.error("dashboard.stats.event_count_failed", error=str(exc))
        st.error(f"Failed to count events: {exc}")
        return None


@st.cache_data(ttl=10)
def _fetch_enrichment_count() -> int | None:
    """Total number of enrichment rows. Returns None on error."""
    client = get_supabase()
    try:
        result = client.table("enrichments").select("id", count="exact").execute()
        return result.count
    except Exception as exc:
        log.error("dashboard.stats.enrichment_count_failed", error=str(exc))
        st.error(f"Failed to count enrichments: {exc}")
        return None


def show() -> None:
    """Render the Enrichment Stats page."""
    st.header("Enrichment Stats")
    st.caption("Aggregated AI enrichment output — refreshes every 10s")

    enrichments = _fetch_enrichments()
    total_events = _fetch_event_count()
    total_enrichments = _fetch_enrichment_count()

    # ── Summary metrics ────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total events", total_events if total_events is not None else "—")
    with col2:
        completed = sum(1 for e in enrichments if e.get("status") == "completed")
        st.metric("Enrichments completed", completed)
    with col3:
        if total_events is not None and total_enrichments is not None:
            backlog = max(0, total_events - total_enrichments)
        else:
            backlog = "—"
        st.metric("Pending enrichment backlog", backlog)
    with col4:
        embed_pending = sum(
            1 for e in enrichments if e.get("embedding_status") in ("pending", "processing")
        )
        st.metric("Embeddings pending", embed_pending)

    st.divider()

    if not enrichments:
        st.info("No enrichment data yet. Run the intelligence layer to populate.")
        return

    col_left, col_right = st.columns(2)

    # ── Intent distribution bar chart ─────────────────────────────────────────
    with col_left:
        st.subheader("Intent distribution")
        intent_counts = Counter(
            e["intent"] for e in enrichments if e.get("intent")
        )
        # Ensure all 7 closed-set values appear even if count is 0
        intent_data = {intent: intent_counts.get(intent, 0) for intent in _ALL_INTENTS}
        intent_df = pd.DataFrame.from_dict(
            intent_data, orient="index", columns=["count"]
        ).sort_values("count", ascending=False)
        st.bar_chart(intent_df)

    # ── Sentiment distribution ─────────────────────────────────────────────────
    with col_right:
        st.subheader("Sentiment distribution")
        sentiment_counts = Counter(
            e["sentiment"] for e in enrichments if e.get("sentiment")
        )
        sentiment_data = {s: sentiment_counts.get(s, 0) for s in _ALL_SENTIMENTS}
        sentiment_df = pd.DataFrame.from_dict(
            sentiment_data, orient="index", columns=["count"]
        )
        st.bar_chart(sentiment_df)

    # ── Embedding queue health ─────────────────────────────────────────────────
    st.subheader("Embedding queue health")
    embed_counts = Counter(
        e["embedding_status"] for e in enrichments if e.get("embedding_status")
    )
    embed_data = {s: embed_counts.get(s, 0) for s in _ALL_EMBED_STATUSES}
    embed_df = pd.DataFrame.from_dict(
        embed_data, orient="index", columns=["count"]
    )
    st.bar_chart(embed_df)

    # ── Recent enrichment failures ─────────────────────────────────────────────
    failures = [e for e in enrichments if e.get("status") == "failed"]
    if failures:
        st.subheader(f"Enrichment failures ({len(failures)})")
        failure_df = pd.DataFrame(failures)[["intent", "sentiment", "failure_reason"]]
        st.dataframe(failure_df, width="stretch", hide_index=True)
