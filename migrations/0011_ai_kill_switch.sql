-- Migration 0011: DB-backed AI kill switch
--
-- Problem: settings.AI_ENABLED is an lru_cache singleton built at import time.
-- Changing the env var has no effect until a full process restart, making
-- "instantly halt AI without restarting" (as claimed in the comments) false.
--
-- Fix: a one-row app_settings table. The application reads ai_enabled from
-- this row on each poll-loop iteration, caching the value briefly in process
-- to avoid a DB round-trip per event. A DB UPDATE flips the flag immediately
-- without any process restart.
--
-- WHY one row / no composite PK: we only ever have one settings row.
-- The CHECK constraint enforces this invariant.
--
-- RLS is enabled (required before go-live) — service-role key bypasses it
-- from the backend, consistent with all other tables in this project.

CREATE TABLE IF NOT EXISTS app_settings (
    id          INT PRIMARY KEY DEFAULT 1,
    ai_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ensures only one settings row can ever exist.
    CONSTRAINT single_row CHECK (id = 1)
);

-- Seed the initial row (using the env-var default of true).
INSERT INTO app_settings (id, ai_enabled)
VALUES (1, TRUE)
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE TRIGGER trg_app_settings_updated_at
    BEFORE UPDATE ON app_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE app_settings ENABLE ROW LEVEL SECURITY;
