-- Migration 0001: Enable required Postgres extensions
-- Run this FIRST before any other migration.

-- pgvector: stores and queries AI embedding vectors for semantic search
CREATE EXTENSION IF NOT EXISTS vector;

-- uuid-ossp: generate UUID primary keys in SQL (used by default values)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
