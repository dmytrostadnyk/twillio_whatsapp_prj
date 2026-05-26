"""
Enrichment logic: extract content from a comm_event, call GPT-4o for structured
analysis, and write the result back to the enrichments table.

WHY this module is separate from consumer.py:
- consumer.py handles claiming work (DB transactions, poll loop, concurrency).
- This module handles the actual work (content extraction, AI call, DB write).
- Keeping them separate makes both easier to test independently.
"""

from __future__ import annotations

import asyncio
import time

import structlog
from openai import OpenAI

from comm_layer.config import settings
from comm_layer.contracts.enriched import EnrichmentData

log = structlog.get_logger()

# ── Constants ──────────────────────────────────────────────────────────────────

# Closed set of allowed intents — GPT-4o is instructed to pick from this list.
# The dashboard (Phase 9) groups by intent, so we need a stable taxonomy.
VALID_INTENTS = {
    "support_request",
    "sales_inquiry",
    "complaint",
    "appointment",
    "billing_question",
    "cancellation",
    "general_query",
}

# Maximum characters sent to GPT-4o — avoids very long transcripts burning tokens.
MAX_CONTENT_CHARS = 6000

# How many extra attempts after the first failure (total = MAX_RETRIES + 1 = 3).
MAX_RETRIES = 2

# Seconds to wait between GPT-4o retry attempts.
RETRY_SLEEP_SECONDS = 2.0

SYSTEM_PROMPT = """\
You analyse customer-service communications (SMS, WhatsApp, voice transcripts)
and produce structured metadata. Be concise and accurate.

Rules:
- summary: 1–3 sentences, no quotes, third person.
- intent: pick exactly one from this list:
    support_request, sales_inquiry, complaint, appointment,
    billing_question, cancellation, general_query.
- sentiment: exactly one of positive, neutral, negative.
- entities: only meaningful nouns (products, people, dates, amounts).
  Skip filler. Empty list is fine.
- action_items: only concrete next steps the operator should take.
  Empty list is fine. Priority is one of high, medium, low.
- Output ONLY the structured fields. No prose outside them.\
"""


# ── Public entry point ─────────────────────────────────────────────────────────


async def enrich_event(pool, supabase, event: dict) -> None:
    """
    Orchestrate enrichment for one comm_event row.

    Steps:
    1. Extract the text content we will send to GPT-4o.
    2. Call GPT-4o (in a thread, up to 3 total attempts).
    3. Write the result (or failure) to the enrichments table.

    The enrichments row was already inserted with status='processing' by
    consumer.claim_next() before this function is called.
    """
    comm_event_id = str(event["id"])
    event_type = event["event_type"]
    correlation_id = str(event.get("correlation_id", ""))

    content = _extract_content(event)
    if content is None:
        # This should not happen — consumer.py filters these out — but guard anyway.
        log.warning(
            "enrichment.no_content",
            comm_event_id=comm_event_id,
            event_type=event_type,
        )
        await _update_enrichment(
            supabase,
            comm_event_id,
            status="failed",
            failure_reason="no_content",
        )
        return

    log.info(
        "enrichment.started",
        comm_event_id=comm_event_id,
        event_type=event_type,
        correlation_id=correlation_id,
        content_length=len(content),
    )

    enrichment_data = await asyncio.to_thread(
        _call_gpt4o_with_retries, content, event_type, comm_event_id
    )

    if enrichment_data is None:
        await _update_enrichment(
            supabase,
            comm_event_id,
            status="failed",
            failure_reason="gpt4o_all_attempts_failed",
        )
        return

    await _update_enrichment(
        supabase,
        comm_event_id,
        status="completed",
        summary=enrichment_data.summary,
        intent=enrichment_data.intent,
        sentiment=enrichment_data.sentiment,
        entities=[e.model_dump() for e in enrichment_data.entities],
        action_items=[a.model_dump() for a in enrichment_data.action_items],
    )

    log.info(
        "enrichment.completed",
        comm_event_id=comm_event_id,
        event_type=event_type,
        # intent is one of 7 closed values — not PII, safe to log
        intent=enrichment_data.intent,
    )


# ── Internal helpers ───────────────────────────────────────────────────────────


def _extract_content(event: dict) -> str | None:
    """
    Return the text we will send to GPT-4o, or None if no text is available.

    - SMS / WhatsApp: the message body from raw_payload.
    - recording.ready: the transcript text set by Phase 6 transcription.

    WHY we check event_type: raw_payload shapes differ per event type.
    """
    event_type = event.get("event_type", "")
    raw_payload = event.get("raw_payload") or {}

    if event_type in ("sms.received", "whatsapp.received"):
        body = raw_payload.get("Body") or ""
        return body.strip() or None

    if event_type == "recording.ready":
        return event.get("transcript_text") or None

    return None


def _call_gpt4o_with_retries(
    content: str, event_type: str, comm_event_id: str
) -> EnrichmentData | None:
    """
    Synchronous wrapper (intended for asyncio.to_thread) that calls GPT-4o up to
    MAX_RETRIES + 1 times total.  Returns None only if all attempts fail.

    WHY sync: OpenAI's structured-output parse() method blocks on the network.
    We offload it to a thread pool so the event loop stays free.
    """
    truncated = content[:MAX_CONTENT_CHARS]

    for attempt in range(MAX_RETRIES + 1):
        try:
            result = _call_gpt4o_sync(truncated, event_type)
            return result
        except Exception as exc:
            log.warning(
                "enrichment.gpt4o_attempt_failed",
                comm_event_id=comm_event_id,
                attempt=attempt + 1,
                max_attempts=MAX_RETRIES + 1,
                error=str(exc),
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)

    return None


def _call_gpt4o_sync(content: str, event_type: str) -> EnrichmentData:
    """
    Make one synchronous GPT-4o call with structured output.

    Uses client.beta.chat.completions.parse() which validates the response
    against EnrichmentData automatically — raises if the model returns an
    unexpected structure, which triggers a retry in the caller.
    """
    client = OpenAI(api_key=settings.OPENAI_API_KEY)

    user_message = f"event_type: {event_type}\n\n{content}"

    completion = client.beta.chat.completions.parse(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        response_format=EnrichmentData,
        temperature=0,
    )

    result: EnrichmentData = completion.choices[0].message.parsed

    # Log a warning if GPT-4o returned an intent outside our closed taxonomy.
    # We still accept the row — this is an ops visibility signal, not a hard error.
    if result.intent not in VALID_INTENTS:
        log.warning(
            "enrichment.unexpected_intent",
            intent=result.intent,
            valid_intents=sorted(VALID_INTENTS),
        )

    return result


async def _update_enrichment(supabase, comm_event_id: str, *, status: str, **fields) -> None:
    """
    Write enrichment results (or failure) back to the enrichments table.

    WHY **fields: the happy path needs summary/intent/sentiment/entities/action_items;
    the failure path only needs status + failure_reason. One function handles both.
    """
    payload = {"status": status, **fields}

    await (
        supabase.table("enrichments")
        .update(payload)
        .eq("comm_event_id", comm_event_id)
        .execute()
    )
