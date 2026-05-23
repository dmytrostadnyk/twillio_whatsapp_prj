-- Migration 0005: Vector embeddings for semantic search
-- Requires pgvector extension (enabled in migration 0001).
-- Dimension 1536 matches OpenAI text-embedding-3-small output.
-- Each embedded chunk stores provenance so long transcripts stay traceable.

CREATE TABLE IF NOT EXISTS embeddings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    comm_event_id   UUID NOT NULL REFERENCES comm_events(id) ON DELETE CASCADE,

    -- The text that was embedded (may be a chunk of a longer transcript)
    content         TEXT NOT NULL,

    -- The vector itself — 1536 dims for text-embedding-3-small
    embedding       vector(1536) NOT NULL,

    -- Chunk provenance for long transcripts
    chunk_index     INT NOT NULL DEFAULT 0,        -- which chunk this is (0-based)
    total_chunks    INT NOT NULL DEFAULT 1,        -- how many chunks total

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- HNSW index — better recall at query time than ivfflat, no training needed.
-- m=16 and ef_construction=64 are sensible defaults for a few thousand rows.
CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw
    ON embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_embeddings_event ON embeddings (comm_event_id);

ALTER TABLE embeddings ENABLE ROW LEVEL SECURITY;
