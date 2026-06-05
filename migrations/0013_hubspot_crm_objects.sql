-- Migration 0013: Add idempotency columns for HubSpot Notes and Tickets,
-- and a resolved flag on whatsapp_replies for bot-can't-answer detection.
--
-- All changes are additive (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
-- Safe to run multiple times.

-- hubspot_note_id: persisted immediately after a Note is created so retries
-- skip the create call and never produce duplicate timeline entries.
ALTER TABLE comm_events
    ADD COLUMN IF NOT EXISTS hubspot_note_id TEXT;

-- hubspot_ticket_id: same idempotency guarantee for auto-created Tickets
-- (created only for complaint intent or negative sentiment events).
ALTER TABLE comm_events
    ADD COLUMN IF NOT EXISTS hubspot_ticket_id TEXT;

-- resolved: set by the WhatsApp reply worker when it knows whether GPT-4o
-- could answer the customer's question from the business context.
-- NULL  = not a whatsapp.received event (or reply not yet complete)
-- TRUE  = bot answered successfully
-- FALSE = bot deferred to email/staff → delivery worker injects a follow-up action item
ALTER TABLE whatsapp_replies
    ADD COLUMN IF NOT EXISTS resolved BOOLEAN;
