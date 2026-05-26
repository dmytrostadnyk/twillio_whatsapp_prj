-- Migration 0007: One enrichment per comm_event
-- The intelligence layer claims work by inserting an enrichments row with
-- status='processing'. To make that claim atomic, comm_event_id must be unique.
-- Without this, two workers could each insert a row and run GPT-4o on the same
-- conversation, wasting a paid API call and producing a duplicate row.

ALTER TABLE enrichments
    ADD CONSTRAINT enrichments_comm_event_id_unique UNIQUE (comm_event_id);
