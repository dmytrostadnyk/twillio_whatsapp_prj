-- Migration 0012: WhatsApp auto-reply tracking table
--
-- Every inbound whatsapp.received event that the reply worker processes gets a
-- row here. This table plays the same role as 'enrichments' but for the outbound
-- reply: it makes the reply worker idempotent, gives us an audit trail, and
-- enables the at-most-once send guarantee.
--
-- Status lifecycle:
--   processing  → generating the reply text via GPT-4o. Nothing has been sent.
--                  A stale 'processing' row (past WHATSAPP_REPLY_LEASE_SECONDS) is
--                  safe to re-claim — no Twilio call was made.
--   sending     → flipped IMMEDIATELY before calling Twilio. A stale 'sending' row
--                  is ambiguous: Twilio may or may not have delivered the message.
--                  Policy: NEVER re-send — mark it 'failed' with reason
--                  'ambiguous_send_crash'. Better to miss a reply than double-text.
--   sent        → Twilio acknowledged; sent_message_sid holds the SID.
--   failed      → permanent failure (ambiguous_send_crash, GPT error after retries).
--   skipped     → no reply appropriate: empty body, window expired, guard blocked,
--                  kill switch off, or auto-reply disabled.

CREATE TABLE IF NOT EXISTS whatsapp_replies (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    comm_event_id    UUID        NOT NULL UNIQUE REFERENCES comm_events(id) ON DELETE CASCADE,

    status           TEXT        NOT NULL,   -- see lifecycle above
    reply_text       TEXT,                   -- the text we sent (or planned to send)
    sent_message_sid TEXT,                   -- Twilio MessageSid on success
    failure_reason   TEXT,                   -- human-readable reason on failed/skipped

    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Reuse the trigger function defined in migration 0003 (comm_events) so updated_at
-- stays current on every status transition without application-level code.
CREATE TRIGGER trg_whatsapp_replies_updated_at
    BEFORE UPDATE ON whatsapp_replies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Partial index covering only the in-flight rows (tiny set) for fast stale-claim scans.
-- Covers both 'processing' and 'sending' so the stale-sending sweep is also cheap.
CREATE INDEX IF NOT EXISTS idx_whatsapp_replies_processing_lease
    ON whatsapp_replies (updated_at)
    WHERE status IN ('processing', 'sending');

-- Audit index — look up all replies for a given event quickly.
CREATE INDEX IF NOT EXISTS idx_whatsapp_replies_event
    ON whatsapp_replies (comm_event_id);

-- RLS required on every table per project security rules.
-- The intelligence worker connects as the service-role user which bypasses RLS,
-- so no policies are needed for normal operation. Enabling it here ensures that
-- any future application-role connection cannot read/write without explicit policy.
ALTER TABLE whatsapp_replies ENABLE ROW LEVEL SECURITY;
