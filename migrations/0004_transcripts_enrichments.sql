-- Migration 0004: Transcripts and AI enrichments
-- Transcripts are written by the Intelligence Layer after a call or message lands.
-- Enrichments are written after the LLM has processed the transcript.
-- Both are independent consumers of comm_events — they never block ingestion.

CREATE TABLE IF NOT EXISTS transcripts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    comm_event_id   UUID NOT NULL REFERENCES comm_events(id) ON DELETE CASCADE,

    text            TEXT,                          -- full transcript text (may be null if empty call)
    language        TEXT,                          -- e.g. 'en-US'
    confidence      NUMERIC(4, 3),                 -- 0.000 to 1.000; null if provider doesn't return it
    segments        JSONB NOT NULL DEFAULT '[]',   -- array of {start_ms, end_ms, speaker, text}
    source          TEXT NOT NULL,                 -- 'streaming' (Deepgram) | 'batch' (Whisper)
    is_partial      BOOLEAN NOT NULL DEFAULT FALSE, -- true if WebSocket disconnected mid-call

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transcripts_event ON transcripts (comm_event_id);

ALTER TABLE transcripts ENABLE ROW LEVEL SECURITY;

-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS enrichments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    comm_event_id   UUID NOT NULL REFERENCES comm_events(id) ON DELETE CASCADE,

    -- Structured output fields from GPT-4o
    summary         TEXT,
    intent          TEXT,                          -- e.g. 'support_request', 'sales_inquiry'
    sentiment       TEXT,                          -- 'positive' | 'neutral' | 'negative'
    entities        JSONB NOT NULL DEFAULT '[]',   -- [{type, value}] e.g. [{type: 'PRODUCT', value: 'Plan A'}]
    action_items    JSONB NOT NULL DEFAULT '[]',   -- [{description, priority}]

    -- Meta about how this enrichment was produced
    model           TEXT NOT NULL,                 -- e.g. 'gpt-4o'
    schema_version  TEXT NOT NULL DEFAULT '1.0',
    status          TEXT NOT NULL DEFAULT 'completed', -- 'completed' | 'failed' | 'skipped'
    failure_reason  TEXT,                          -- populated if status = 'failed'

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrichments_event ON enrichments (comm_event_id);

ALTER TABLE enrichments ENABLE ROW LEVEL SECURITY;
