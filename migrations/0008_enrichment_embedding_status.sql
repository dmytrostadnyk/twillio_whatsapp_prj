-- Migration 0008: Track embedding generation state on enrichments
--
-- Phase 8 introduces a background embedding consumer that watches for completed
-- enrichments and writes a vector(1536) to the embeddings table. We need a way
-- for many workers to claim "the next enrichment that still needs an embedding"
-- without two of them racing on the same row.
--
-- The embeddings table itself cannot be the claim marker (Phase 7 used that
-- trick on enrichments) because pgvector requires a real, non-null vector at
-- insert time — we cannot insert a placeholder before the OpenAI call returns.
--
-- Instead, we add a tiny state machine on enrichments:
--     pending → processing → completed | failed
-- Workers claim via SELECT FOR UPDATE SKIP LOCKED on rows where
-- embedding_status='pending', then transition to 'processing'.
--
-- The partial index keeps the workload tiny — once the queue catches up,
-- the index covers only the small number of rows still in flight.

ALTER TABLE enrichments
    ADD COLUMN embedding_status TEXT NOT NULL DEFAULT 'pending';

CREATE INDEX idx_enrichments_embedding_status_pending
    ON enrichments (embedding_status)
    WHERE embedding_status IN ('pending', 'processing');
