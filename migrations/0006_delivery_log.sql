-- Migration 0006: Delivery log (append-only audit trail)
-- Every attempt to deliver an event to the downstream consumer (HubSpot) is recorded here.
-- This is intentionally append-only — we never update or delete rows.
-- The dashboard reads this to show the delivery timeline per event.

CREATE TABLE IF NOT EXISTS delivery_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    comm_event_id   UUID NOT NULL REFERENCES comm_events(id) ON DELETE CASCADE,
    correlation_id  UUID NOT NULL,

    attempt_number  INT NOT NULL,
    status          TEXT NOT NULL,                 -- 'success' | 'failure'
    http_status     INT,                           -- HTTP response code from the consumer, if any
    latency_ms      INT,                           -- round-trip time in milliseconds
    error_message   TEXT,                          -- null on success

    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- For fast lookup of all attempts for a given event (delivery timeline view)
CREATE INDEX IF NOT EXISTS idx_delivery_log_event ON delivery_log (comm_event_id, attempted_at);

ALTER TABLE delivery_log ENABLE ROW LEVEL SECURITY;
