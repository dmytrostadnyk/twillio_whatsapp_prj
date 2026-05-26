"""
Streamlit operator dashboard — entry point.

Run with:
    streamlit run dashboard/main.py
or:
    make dashboard

Four views are available via the sidebar:
    Live Feed        — recent comm_events + enrichment detail
    Enrichment Stats — intent/sentiment charts + queue health
    Semantic Search  — pgvector HNSW + Cohere rerank over embedded content
    Delivery Health  — delivery pipeline status + dead-letter queue + replay

SECURITY: This dashboard reads PII (phone numbers, call summaries, transcript
content) using the Supabase service-role key, which bypasses Row Level Security.
Deploy ONLY behind a VPN or authenticated reverse proxy. Never expose port 8501
to the public internet.
"""

import os
import pathlib

# Streamlit changes CWD to the directory containing this script (dashboard/).
# comm_layer/config.py looks for .env relative to CWD, so it would fail to find
# the project root .env. Restore CWD to the project root before any imports
# that trigger settings loading.
os.chdir(pathlib.Path(__file__).parent.parent)

import streamlit as st

from dashboard.views import delivery, feed, search, stats

st.set_page_config(
    page_title="Comm Intelligence Dashboard",
    page_icon=None,
    layout="wide",
)

# Permanent PII / auth warning — shown on every page
st.warning(
    "Internal operator console. Displays PII (call content, summaries, phone numbers) "
    "and bypasses Supabase RLS via the service-role key. "
    "Deploy only behind a VPN or authenticated reverse proxy."
)

PAGES = {
    "Live Feed": feed.show,
    "Enrichment Stats": stats.show,
    "Semantic Search": search.show,
    "Delivery Health": delivery.show,
}

choice = st.sidebar.radio("Navigation", list(PAGES.keys()))
st.sidebar.divider()
st.sidebar.caption("Twilio Comm Intelligence — Phase 9")

PAGES[choice]()
