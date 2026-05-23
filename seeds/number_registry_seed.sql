-- Seed data for number_registry
-- Add your Twilio phone numbers here with their business source mapping.
-- Replace the example numbers with your actual Twilio numbers (E.164 format).
--
-- source_type options: 'affiliate' | 'campaign' | 'business_unit'
-- active = false means the number is retired but historical events are still attributed to it.

INSERT INTO number_registry (number, source_type, source_id, label, active, metadata)
VALUES
    -- Example: a number used for a marketing campaign
    ('+15550000001', 'campaign',      'camp_spring_2025', 'Spring 2025 Campaign',   TRUE, '{"channel": "sms", "region": "us-east"}'),

    -- Example: a number used by a specific business unit
    ('+15550000002', 'business_unit', 'bu_support',       'Customer Support Line',  TRUE, '{"team": "support", "hours": "9-5 EST"}'),

    -- Example: a number tied to an affiliate partner
    ('+15550000003', 'affiliate',     'aff_partner_001',  'Affiliate Partner #001', TRUE, '{"partner_id": "P001"}')

ON CONFLICT (number) DO NOTHING;  -- safe to re-run; won't overwrite existing rows
