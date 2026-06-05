-- Migration 0014: Add idempotency column for HubSpot Tasks.
--
-- When the WhatsApp bot cannot answer a customer's question (reply_resolved=FALSE),
-- the delivery worker creates a real HubSpot Task in the rep's to-do queue.
-- This column stores the task id immediately after creation so retries skip the
-- create call and never produce duplicate tasks.
--
-- Additive (IF NOT EXISTS). Safe to run multiple times.

ALTER TABLE comm_events
    ADD COLUMN IF NOT EXISTS hubspot_task_id TEXT;
