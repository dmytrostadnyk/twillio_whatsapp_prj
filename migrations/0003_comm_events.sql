-- Migration 0003: Communication events — the spine of the entire system
-- Every inbound/outbound Twilio event (voice, SMS, WhatsApp) lands here first.
-- The unique constraint on event_key is the idempotency guarantee:
-- duplicate webhook deliveries from Twilio will fail the constraint and be silently ignored.

-- Enum types keep delivery_status and channel values valid at the DB level
CREATE TYPE comm_channel   AS ENUM ('voice', 'sms', 'whatsapp');
CREATE TYPE comm_direction AS ENUM ('inbound', 'outbound');
CREATE TYPE delivery_status AS ENUM ('received', 'pending', 'delivered', 'failed', 'dead');

CREATE TABLE IF NOT EXISTS comm_events (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Natural idempotency key: "{TwilioSid}:{event_type}"
    -- The UNIQUE constraint means a duplicate delivery just gets a conflict error — no double-write.
    event_key       TEXT NOT NULL UNIQUE,

    channel         comm_channel   NOT NULL,
    direction       comm_direction NOT NULL,
    event_type      TEXT NOT NULL,                 -- e.g. 'sms.received', 'call.completed'

    from_number     TEXT,                          -- null on some status callbacks
    to_number       TEXT,

    -- Resolved from number_registry at ingestion time.
    -- Stored as JSONB so adding new registry fields doesn't require a schema change.
    source_metadata JSONB NOT NULL DEFAULT '{}',

    -- The exact payload Twilio sent — never modified, never lost.
    raw_payload     JSONB NOT NULL,

    -- Versioned contract JSON emitted to consumers.
    -- Populated by the delivery worker just before shipping.
    contract_payload JSONB,

    schema_version  TEXT NOT NULL DEFAULT '1.0',

    -- Single UUID that threads through every log line, DB row, and outbound payload
    -- so any event can be traced end to end.
    correlation_id  UUID NOT NULL DEFAULT uuid_generate_v4(),

    -- Delivery lifecycle
    delivery_status delivery_status NOT NULL DEFAULT 'received',
    attempt_count   INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,                   -- null = ready to process now
    last_error      TEXT,                          -- last failure message for the dashboard

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The delivery worker polls this index constantly.
-- Partial index only covers rows that are not yet done, keeping it small.
CREATE INDEX IF NOT EXISTS idx_comm_events_delivery_poll
    ON comm_events (delivery_status, next_retry_at)
    WHERE delivery_status IN ('pending', 'failed');

-- For the dashboard: recent events by channel
CREATE INDEX IF NOT EXISTS idx_comm_events_channel_created
    ON comm_events (channel, created_at DESC);

-- For correlation tracing
CREATE INDEX IF NOT EXISTS idx_comm_events_correlation_id
    ON comm_events (correlation_id);

CREATE OR REPLACE TRIGGER trg_comm_events_updated_at
    BEFORE UPDATE ON comm_events
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE comm_events ENABLE ROW LEVEL SECURITY;
