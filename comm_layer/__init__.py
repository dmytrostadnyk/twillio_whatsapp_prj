"""
Communication Layer — Phase 0 package.

This package handles all Twilio webhook ingestion:
- Validates Twilio signatures
- Deduplicates events by event_key
- Persists events to Supabase
- Returns responses to Twilio in < 1 second

It contains zero AI logic and zero business logic.
"""
