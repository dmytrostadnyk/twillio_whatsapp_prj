-- Migration 0010: Enrichment lease — crash recovery for the intelligence layer
--
-- Problem: if the intelligence worker dies after inserting enrichments(status='processing')
-- but before writing a terminal status, the row is stuck at 'processing' forever.
-- The claim query only selects events where no enrichment row exists (e.id IS NULL),
-- so a stuck 'processing' row blocks both the enrichment AND delivery workers.
--
-- Fix: mirror the delivery worker's lease pattern.
-- Add updated_at so we can detect stale 'processing' rows and re-claim them after
-- ENRICHMENT_LEASE_SECONDS (set in application config, default 120s).
--
-- The claim query (intelligence_layer/consumer.py) is updated to treat a
-- 'processing' row older than the lease as if it did not exist — safe to re-claim.

ALTER TABLE enrichments
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Attach the same trigger function already used by comm_events (migration 0003).
-- This keeps updated_at current on every status transition without app-level code.
CREATE TRIGGER trg_enrichments_updated_at
    BEFORE UPDATE ON enrichments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Partial index for fast lease-expiry lookups: only covers the tiny in-flight set.
CREATE INDEX IF NOT EXISTS idx_enrichments_processing_lease
    ON enrichments (updated_at)
    WHERE status = 'processing';
