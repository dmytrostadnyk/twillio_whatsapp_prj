-- Migration 0002: Number registry
-- Maps Twilio phone numbers to business sources (campaigns, affiliates, business units).
-- The communication layer resolves every inbound "to_number" against this table.
-- If a number is not in the registry, the event is still captured with source = 'unknown'.

CREATE TABLE IF NOT EXISTS number_registry (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    number      TEXT NOT NULL UNIQUE,              -- E.164 format e.g. +15551234567
    source_type TEXT NOT NULL,                     -- 'affiliate' | 'campaign' | 'business_unit'
    source_id   TEXT NOT NULL,                     -- your internal identifier for the source
    label       TEXT NOT NULL,                     -- human-readable label for the dashboard
    active      BOOLEAN NOT NULL DEFAULT TRUE,     -- false = number retired; still resolve but flag it
    metadata    JSONB NOT NULL DEFAULT '{}',        -- any extra attributes you need
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast lookup by phone number (the hot path on every inbound webhook)
CREATE INDEX IF NOT EXISTS idx_number_registry_number ON number_registry (number);

-- Auto-update updated_at on every row change
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_number_registry_updated_at
    BEFORE UPDATE ON number_registry
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Row Level Security: enabled as a baseline. Service-role key bypasses it.
ALTER TABLE number_registry ENABLE ROW LEVEL SECURITY;
