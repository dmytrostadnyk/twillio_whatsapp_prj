-- Migration 0009: Add hubspot_contact_id to comm_events
--
-- WHY this column exists:
-- The delivery worker creates or finds a HubSpot contact for the caller's phone
-- number. We persist the resulting contact ID immediately after creation so that
-- if the worker crashes before the PATCH succeeds, retries reuse the existing
-- contact instead of creating a duplicate.
--
-- HOW TO RUN:
-- Paste this into the Supabase SQL editor and click Run.
-- It is safe to run multiple times (IF NOT EXISTS / DO NOTHING).

ALTER TABLE comm_events
    ADD COLUMN IF NOT EXISTS hubspot_contact_id TEXT;

COMMENT ON COLUMN comm_events.hubspot_contact_id IS
    'HubSpot contact ID set after the delivery worker creates or finds the contact. '
    'Null until the first successful contact resolution. Used for idempotent retries.';
