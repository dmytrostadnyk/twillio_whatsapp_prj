"""
Intelligence Layer — AI enrichment consumer (Phase 7).

Polls comm_events for unenriched SMS, WhatsApp, and voice recordings,
calls GPT-4o for structured analysis, and writes results to the enrichments table.

Entry point: python -m intelligence_layer.main
"""
