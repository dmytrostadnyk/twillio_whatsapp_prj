"""
Semantic Search view — text box → pgvector HNSW + Cohere rerank → results table.

search_events() is async (uses asyncio.to_thread internally), so we call it via
run_async() which spins up a temporary asyncpg pool and closes it after the call.

After getting the IDs from search_events(), we do a follow-up sync Supabase query
to fetch timestamp/channel/event_type for each result — search_events() only returns
comm_event_id, content, similarity, rerank_score.

Content is truncated to 200 chars in the table; full text is under st.expander().
"""

from __future__ import annotations

import streamlit as st
import structlog

from comm_layer.config import settings
from dashboard.db import get_supabase, run_async
from intelligence_layer.search import search_events

log = structlog.get_logger(__name__)

_SNIPPET_LEN = 200


def _truncate(text: str) -> str:
    """Truncate long content for the results table."""
    if not text:
        return ""
    return text[:_SNIPPET_LEN] + ("…" if len(text) > _SNIPPET_LEN else "")


def _run_search(query: str) -> list[dict]:
    """Bridge async search_events() into synchronous Streamlit context."""
    try:
        return run_async(lambda pool: search_events(pool, query))
    except Exception as exc:
        log.error("dashboard.search.failed", error=str(exc))
        st.error(f"Search failed: {exc}")
        return []


def _fetch_event_meta(ids: list[str]) -> dict[str, dict]:
    """
    Fetch comm_events metadata for a list of IDs.
    Returns a dict keyed by id so callers can merge by ID.
    """
    if not ids:
        return {}
    client = get_supabase()
    try:
        result = client.table("comm_events").select(
            "id, channel, event_type, from_number, created_at"
        ).in_("id", ids).execute()
        return {row["id"]: row for row in (result.data or [])}
    except Exception as exc:
        log.error("dashboard.search.meta_fetch_failed", error=str(exc))
        st.warning(f"Could not fetch event metadata: {exc}")
        return {}


def show() -> None:
    """Render the Semantic Search page."""
    st.header("Semantic Search")
    st.caption(
        "Searches embedded message content and call transcripts using "
        "pgvector HNSW + Cohere rerank."
    )

    if not settings.AI_ENABLED:
        st.warning(
            "AI is disabled (AI_ENABLED=False in your environment). "
            "Set AI_ENABLED=true and restart to enable semantic search."
        )
        return

    query = st.text_input("Search query", placeholder="e.g. billing problem, cancel subscription")
    search_clicked = st.button("Search", type="primary")

    if not search_clicked:
        return

    if not query.strip():
        st.warning("Enter a query before searching.")
        return

    with st.spinner("Searching…"):
        results = _run_search(query.strip())

    if not results:
        st.info("No matching events found.")
        return

    # Follow-up query to get timestamp / channel / event_type
    ids = [r["comm_event_id"] for r in results]
    meta_by_id = _fetch_event_meta(ids)

    # Build display rows
    display_rows = []
    for r in results:
        meta = meta_by_id.get(r["comm_event_id"], {})
        display_rows.append({
            "timestamp": meta.get("created_at", "—"),
            "channel": meta.get("channel", "—"),
            "event_type": meta.get("event_type", "—"),
            "snippet": _truncate(r.get("content", "")),
            "similarity": round(r["similarity"], 3),
            "rerank_score": round(r["rerank_score"], 3),
        })

    st.success(f"Found {len(results)} result(s)")
    st.dataframe(display_rows, use_container_width=True, hide_index=True)

    # Full content for each result under individual expanders
    st.divider()
    st.subheader("Full content")
    for i, r in enumerate(results):
        meta = meta_by_id.get(r["comm_event_id"], {})
        label = (
            f"#{i + 1}  {meta.get('channel', '')}  {meta.get('event_type', '')}  "
            f"— rerank: {round(r['rerank_score'], 3)}"
        )
        with st.expander(label):
            st.text(r.get("content", ""))
            st.caption(f"comm_event_id: {r['comm_event_id']}")
