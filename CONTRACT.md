# Communication Layer — Versioned Event Contract

**Schema Version:** `1.0`
**Last Updated:** 2026-06-03

This document is the authoritative reference for every event the Communication Layer emits to downstream consumers (HubSpot CRM, Intelligence Layer, or any integration partner). Treat it as a first-class deliverable — consumers depend on this contract, not on the underlying code.

---

## Principles

1. **Every event carries `schema_version`.** Consumers must check this field before processing. A version bump means the shape changed; consumers must be updated before consuming the new version.
2. **Every event carries `event_key`.** Consumers must deduplicate by `event_key` on their side. The Communication Layer guarantees at-least-once delivery; consumers guarantee exactly-once processing.
3. **Every event carries `correlation_id`.** A UUID that traces a single communication (call, SMS, WhatsApp thread) through every system — logs, database rows, this payload.
4. **`source` is always present.** Even when the `to_number` is not in the number registry, `source.is_unknown = true` and the event still arrives — nothing is dropped.

---

## Schema-Versioning Policy

- The current version is `1.0`.
- **Backwards-compatible changes** (adding optional fields, adding enum values): version stays `1.0`. Consumers should ignore unknown fields.
- **Breaking changes** (removing fields, changing types, renaming fields): version bumps to `1.1`, `2.0`, etc. Consumers that check `schema_version` will detect the change and can fail gracefully.
- The `CONTRACT.md` changelog section at the bottom of this document records every change.

---

## Common Fields (present on every event)

| Field | Type | Description |
|---|---|---|
| `schema_version` | `string` | Contract version. Currently `"1.0"`. |
| `event_key` | `string` | Natural idempotency key: `"{TwilioSid}:{event_type}"` e.g. `"SM123:sms.received"` |
| `correlation_id` | `UUID` | End-to-end trace ID. Thread this through your own logs. |
| `channel` | `"voice" \| "sms" \| "whatsapp"` | The Twilio channel. |
| `direction` | `"inbound" \| "outbound"` | Whether we received or initiated the communication. |
| `event_type` | `string` | Machine-readable event name (see per-event docs below). |
| `timestamp` | `ISO-8601 datetime (UTC)` | When the event occurred. |
| `source` | `EventSource` | Resolved source metadata (see below). |

### EventSource object

```json
{
  "number": "+15551234567",
  "source_type": "campaign",
  "source_id": "camp_spring_2025",
  "label": "Spring 2025 Campaign",
  "is_unknown": false,
  "metadata": { "region": "us-east" }
}
```

When the `to_number` is not in the number registry:
```json
{
  "number": "+15559999999",
  "source_type": null,
  "source_id": null,
  "label": null,
  "is_unknown": true,
  "metadata": {}
}
```

---

## Event Types

---

### `call.started`

**When:** Twilio fires the initial voice webhook when an inbound call arrives or an outbound call connects.

```json
{
  "schema_version": "1.0",
  "event_key": "CA1234567890abcdef:call.started",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "channel": "voice",
  "direction": "inbound",
  "event_type": "call.started",
  "timestamp": "2026-01-15T14:23:01Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "call_sid": "CA1234567890abcdef",
  "from_number": "+15559876543",
  "to_number": "+15551234567",
  "call_status": "in-progress"
}
```

| Field | Type | Description |
|---|---|---|
| `call_sid` | `string` | Twilio Call SID (starts with `CA`) |
| `from_number` | `string` | Caller's phone number (E.164) |
| `to_number` | `string` | Dialled number (E.164) |
| `call_status` | `string` | `"ringing"` \| `"in-progress"` |

---

### `call.completed`

**When:** Twilio fires a status callback after a call ends.

```json
{
  "schema_version": "1.0",
  "event_key": "CA1234567890abcdef:call.completed",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "channel": "voice",
  "direction": "inbound",
  "event_type": "call.completed",
  "timestamp": "2026-01-15T14:28:47Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "call_sid": "CA1234567890abcdef",
  "from_number": "+15559876543",
  "to_number": "+15551234567",
  "call_status": "completed",
  "duration": "346"
}
```

| Field | Type | Description |
|---|---|---|
| `call_sid` | `string` | Twilio Call SID |
| `from_number` | `string` | Caller's phone number (E.164) |
| `to_number` | `string` | Dialled number (E.164) |
| `call_status` | `string` | `"completed"` \| `"no-answer"` \| `"busy"` \| `"failed"` |
| `duration` | `string \| null` | Call duration in seconds (as a string, matching Twilio's format) |

---

### `call.recording_ready`

**When:** Twilio fires a separate recording-ready callback. This arrives **after** `call.completed`, sometimes 30 seconds to several minutes later.

> ⚠️ **Security note:** `recording_api_path` is an API path, not a public URL. Fetch the audio using the Twilio client with credentials. Never expose it as a direct link.

```json
{
  "schema_version": "1.0",
  "event_key": "RE1234567890abcdef:call.recording_ready",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "channel": "voice",
  "direction": "inbound",
  "event_type": "call.recording_ready",
  "timestamp": "2026-01-15T14:29:30Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "call_sid": "CA1234567890abcdef",
  "recording_sid": "RE1234567890abcdef",
  "recording_api_path": "/2010-04-01/Accounts/AC.../Recordings/RE...",
  "duration": "346",
  "recording_status": "completed"
}
```

---

### `sms.received`

**When:** A user sends an inbound SMS to one of our Twilio numbers.

```json
{
  "schema_version": "1.0",
  "event_key": "SM1234567890abcdef:sms.received",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440001",
  "channel": "sms",
  "direction": "inbound",
  "event_type": "sms.received",
  "timestamp": "2026-01-15T14:30:00Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "message_sid": "SM1234567890abcdef",
  "from_number": "+15559876543",
  "to_number": "+15551234567",
  "body": "Hello, I'd like more information about your service.",
  "num_media": 0,
  "media_urls": []
}
```

---

### `sms.status`

**When:** Twilio fires status callbacks for outbound SMS messages. Status progression: `queued → sent → delivered → (failed | undelivered)`. **Can arrive out of order — reconcile by `message_sid`, never assume ordering.**

```json
{
  "schema_version": "1.0",
  "event_key": "SM1234567890abcdef:sms.status",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440002",
  "channel": "sms",
  "direction": "outbound",
  "event_type": "sms.status",
  "timestamp": "2026-01-15T14:30:05Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "message_sid": "SM1234567890abcdef",
  "from_number": "+15551234567",
  "to_number": "+15559876543",
  "message_status": "delivered",
  "error_code": null,
  "error_message": null
}
```

---

### `whatsapp.received`

**When:** A user sends an inbound WhatsApp message. Note the `whatsapp:` prefix on phone numbers — this matches Twilio's format.

```json
{
  "schema_version": "1.0",
  "event_key": "SM1234567890abcdef:whatsapp.received",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440003",
  "channel": "whatsapp",
  "direction": "inbound",
  "event_type": "whatsapp.received",
  "timestamp": "2026-01-15T14:31:00Z",
  "source": { "number": "whatsapp:+14155238886", "source_type": "business_unit", "source_id": "bu_support", "label": "Customer Support Line", "is_unknown": false, "metadata": {} },
  "message_sid": "SM1234567890abcdef",
  "from_number": "whatsapp:+15559876543",
  "to_number": "whatsapp:+14155238886",
  "body": "Hi, I need help with my account.",
  "profile_name": "John Doe",
  "num_media": 0,
  "media_urls": []
}
```

---

### `whatsapp.status`

**When:** Status callbacks for outbound WhatsApp messages. Includes `"read"` status (not available for SMS).

| Field | Description |
|---|---|
| `is_template` | `true` when this message was sent as a pre-approved WhatsApp template (required outside the 24-hour session window) |

---

### `comm.enriched`

**When:** The Intelligence Layer emits this after GPT-4o processes a transcript or message. This is a superset of the original event.

> **Note:** This event type is produced by the Intelligence Layer, not the Communication Layer. It is a separate consumer of the same event stream.

```json
{
  "schema_version": "1.0",
  "event_key": "SM1234567890abcdef:comm.enriched",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440001",
  "channel": "sms",
  "direction": "inbound",
  "event_type": "comm.enriched",
  "timestamp": "2026-01-15T14:30:45Z",
  "source": { "number": "+15551234567", "source_type": "campaign", "source_id": "camp_spring_2025", "label": "Spring 2025 Campaign", "is_unknown": false, "metadata": {} },
  "original_event_key": "SM1234567890abcdef:sms.received",
  "original_event_type": "sms.received",
  "enrichment": {
    "summary": "Customer enquired about pricing for the premium plan.",
    "intent": "sales_inquiry",
    "sentiment": "positive",
    "entities": [
      { "entity_type": "PRODUCT", "value": "premium plan" }
    ],
    "action_items": [
      { "description": "Send pricing brochure to customer", "priority": "high" }
    ]
  },
  "model_used": "gpt-4o",
  "enrichment_schema_version": "1.0"
}
```

---

## Consumer Responsibilities

1. **Deduplicate by `event_key`.** The Communication Layer delivers at-least-once. Your system must handle receiving the same `event_key` twice without creating duplicate records.
2. **Check `schema_version`.** If you receive an unexpected version, reject the event and raise an alert rather than silently processing it incorrectly.
3. **Thread `correlation_id` through your own logs.** This is the only way to trace an event end-to-end when something goes wrong.
4. **Handle `source.is_unknown = true`.** Some events will arrive from numbers not in the registry. Your system must accept them.

---

## HubSpot Delivery (current production consumer)

The delivery worker writes the following custom contact properties for each enriched `sms.received`, `whatsapp.received`, and `recording.ready` event:

| HubSpot Property | Type | Description |
|---|---|---|
| `ai_last_intent` | `string` | Most recent call intent (e.g. `support_request`, `cancellation`) |
| `ai_last_sentiment` | `string` | Most recent call sentiment (`positive`, `neutral`, `negative`) |
| `ai_last_summary` | `string` | 1–3 sentence GPT-4o summary of the most recent conversation |
| `ai_comm_log` | `string` (textarea) | Prepend-only log of all AI-enriched events, newest at top |

Properties live in the **AI Insights** custom property group (created automatically on worker startup, idempotent — 409 Conflict is silently ignored).

**Contact deduplication:** the delivery worker searches HubSpot by E.164 phone number before creating a contact. It also checks the local DB for a previously persisted `hubspot_contact_id` to avoid search-lag duplicates. A contact is created once and reused on every subsequent delivery for that number.

**Deliverable event types only:** status callbacks (`sms.status`, `whatsapp.status`, `call.started`, `call.completed`) are persisted for audit but are never delivered to HubSpot — they are not enriched by GPT-4o and would sit pending forever under the enrichment gate.

---

## WhatsApp Auto-Reply (intelligence layer)

The intelligence layer sends an AI-generated reply for every `whatsapp.received`
event via Twilio. This is tracked in the `whatsapp_replies` table (migration 0012).

**At-most-once guarantee:** the worker flips to `status='sending'` immediately before
calling the Twilio API. If the process crashes after that, the stale `sending` row is
swept to `status='failed'` on restart rather than being re-sent. This prevents
double-texting the customer at the cost of occasionally missing a reply after a crash.

**Prompt-injection defense (four layers):**
1. Structural: customer text is always placed in `role="user"` turns, never spliced
   into the system prompt string.
2. Input screening: a heuristic pass escalates to a GPT-4o-mini classifier that
   returns `safe | injection | jailbreak | off_topic_abuse`. On anything except
   `safe`, the `SAFE_FALLBACK_REPLY` is sent and generation is skipped.
3. Output canary: a random token embedded in the system prompt; if it appears in the
   generated reply, the reply is blocked and the fallback is sent instead.
4. Capability minimisation: the reply bot has no tools, no DB writes, no external
   actions. Even a successful injection can only produce bad text — it cannot
   exfiltrate data or take actions.

**Multi-turn memory:** the worker loads the last `WHATSAPP_REPLY_HISTORY_LIMIT` (default
10) message/reply pairs for the sending phone number and builds a multi-turn GPT-4o
conversation so follow-up questions work correctly.

**Kill switches:**
- `UPDATE app_settings SET ai_enabled = false;` — halts all AI work (enrichment,
  embedding, and reply) without a process restart.
- `WHATSAPP_AUTOREPLY_ENABLED=false` env var — halts only the reply consumer without
  affecting enrichment or embedding.

**Business context:** replies are grounded in `intelligence_layer/business_context.md`
(the imaginary NovaBrew Coffee shop). Edit this file and restart the worker to update
the knowledge base. RAG over a document corpus is a deliberately-deferred "v2" — the
entire knowledge base fits in the context window, so retrieval would add operational
complexity with no quality gain.

---

## Changelog

| Date | Version | Change |
|---|---|---|
| 2026-05-23 | 1.0 | Initial contract — Voice, SMS, WhatsApp, and Enriched event types |
| 2026-06-03 | 1.0 | Added WhatsApp auto-reply section (migration 0012, prompt-guard, business context) |
