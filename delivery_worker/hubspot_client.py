"""
HubSpot CRM client for the delivery worker.

Three public functions:
  ensure_custom_properties  — create AI property group + fields at worker startup
  find_or_create_contact    — search by phone, create if not found; returns (id, log)
  update_contact            — PATCH contact with AI insight properties

WHY httpx instead of the hubspot-api-client SDK:
The project already uses httpx for outbound HTTP. Adding the official SDK would
introduce a heavy dependency (many generated classes) for only three API calls
we need. Using httpx directly keeps the dependency surface small and reuses the
same connection pooling and retry logic already in the delivery worker.

WHY we need three scopes:
  crm.objects.contacts.read   — search for existing contacts by phone
  crm.objects.contacts.write  — create new contacts, update properties
  crm.schemas.contacts.write  — create the custom AI property group and fields

The token is never logged (security rule). Authorization header is built by the
private _auth_headers() helper so the token never appears in call sites.
"""

from __future__ import annotations

import httpx
import structlog

log = structlog.get_logger(__name__)

# HubSpot property group that holds all AI-generated fields.
_GROUP_NAME = "ai_insights"

# Custom contact properties we create once and write on every delivery.
# 'text' fieldType = single-line. 'textarea' = multi-line, good for long summaries.
_PROPERTY_DEFINITIONS = [
    {
        "name": "ai_last_intent",
        "label": "AI Last Intent",
        "type": "string",
        "fieldType": "text",
    },
    {
        "name": "ai_last_sentiment",
        "label": "AI Last Sentiment",
        "type": "string",
        "fieldType": "text",
    },
    {
        "name": "ai_last_summary",
        "label": "AI Last Summary",
        "type": "string",
        "fieldType": "textarea",
    },
    {
        "name": "ai_comm_log",
        "label": "AI Communication Log",
        "type": "string",
        "fieldType": "textarea",
    },
]


def _auth_headers(token: str) -> dict[str, str]:
    """Build the Authorization header. Token is intentionally never logged."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def normalize_phone(phone: str | None) -> str | None:
    """
    Strip the 'whatsapp:' prefix Twilio adds to WhatsApp numbers.

    WhatsApp from_number arrives as 'whatsapp:+15551234567'. HubSpot expects
    a plain E.164 string. Contacts created with the prefix would never match
    a search for the plain number, causing duplicate contacts on every retry.
    """
    if phone and phone.startswith("whatsapp:"):
        return phone[len("whatsapp:"):]
    return phone


async def ensure_custom_properties(
    client: httpx.AsyncClient,
    token: str,
    base_url: str,
) -> None:
    """
    Create the AI Insights property group and all custom properties.

    Safe to call on every worker startup — 409 Conflict means the property
    already exists and is silently ignored. We log warnings for any other
    unexpected error but do not raise: a property creation failure should not
    prevent the worker from starting up and processing events.
    """
    headers = _auth_headers(token)

    # Create the property group that organises all AI fields in the HubSpot UI.
    resp = await client.post(
        f"{base_url}/crm/v3/properties/contacts/groups",
        headers=headers,
        json={"name": _GROUP_NAME, "label": "AI Insights", "displayOrder": -1},
        timeout=10.0,
    )
    if resp.status_code not in (200, 201, 409):
        log.warning(
            "hubspot.property_group_creation_failed",
            status=resp.status_code,
            body=resp.text[:200],
        )

    # Create each custom property inside the group.
    for prop in _PROPERTY_DEFINITIONS:
        resp = await client.post(
            f"{base_url}/crm/v3/properties/contacts",
            headers=headers,
            json={**prop, "groupName": _GROUP_NAME},
            timeout=10.0,
        )
        if resp.status_code not in (200, 201, 409):
            log.warning(
                "hubspot.property_creation_failed",
                property_name=prop["name"],
                status=resp.status_code,
                body=resp.text[:200],
            )

    log.info("hubspot.custom_properties_ensured")


async def find_or_create_contact(
    client: httpx.AsyncClient,
    token: str,
    base_url: str,
    phone: str,
) -> tuple[str, str]:
    """
    Find a HubSpot contact by phone number, or create one if not found.

    Returns (contact_id, existing_ai_comm_log).
    The existing log is used by the caller to prepend new history entries.

    WHY search before create:
    HubSpot does not enforce phone uniqueness by default. Creating without
    searching would make a new contact on every delivery attempt for the same
    caller — the classic "3,000 duplicate contacts" CRM mess. Searching first
    and reusing existing contacts keeps the CRM clean.

    WHY we request ai_comm_log in the search:
    Fetching the current log during search avoids a separate GET call later.
    The caller prepends the new entry to this existing value.

    NOTE: 'id' is intentionally NOT in the properties list — it is not a
    HubSpot property name; the contact id is always returned at the top level.
    Some HubSpot versions reject unknown property names in the list.

    Raises httpx.HTTPStatusError on non-2xx responses so the delivery worker
    can apply its standard ack/nack/dead-letter logic based on the status code.
    """
    headers = _auth_headers(token)
    clean_phone = normalize_phone(phone)

    search_resp = await client.post(
        f"{base_url}/crm/v3/objects/contacts/search",
        headers=headers,
        json={
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "phone",
                            "operator": "EQ",
                            "value": clean_phone,
                        }
                    ]
                }
            ],
            "properties": ["phone", "ai_comm_log"],
            "limit": 1,
        },
        timeout=10.0,
    )
    search_resp.raise_for_status()

    results = search_resp.json().get("results", [])
    if results:
        contact = results[0]
        existing_log = (contact.get("properties") or {}).get("ai_comm_log") or ""
        log.debug("hubspot.contact_found", contact_id=contact["id"])
        return contact["id"], existing_log

    # Contact not found — create a new one.
    create_resp = await client.post(
        f"{base_url}/crm/v3/objects/contacts",
        headers=headers,
        json={"properties": {"phone": clean_phone}},
        timeout=10.0,
    )
    create_resp.raise_for_status()

    contact_id = create_resp.json()["id"]
    log.info("hubspot.contact_created", contact_id=contact_id)
    return contact_id, ""


async def get_contact_log(
    client: httpx.AsyncClient,
    token: str,
    base_url: str,
    contact_id: str,
) -> str:
    """
    Fetch the current ai_comm_log for an existing contact by ID.

    WHY this exists:
    On retries, hubspot_contact_id is already persisted so we skip
    find_or_create_contact entirely — but we must still read the current log
    before prepending the new entry. Without this, the PATCH would overwrite
    the entire history with only the current event, destroying all prior entries.

    Returns the current log value, or "" if the property is not yet set.
    Raises httpx.HTTPStatusError on non-2xx so the delivery worker maps errors
    to nack/dead-letter via its standard _handle_http_error helper.
    """
    headers = _auth_headers(token)
    resp = await client.get(
        f"{base_url}/crm/v3/objects/contacts/{contact_id}",
        headers=headers,
        params={"properties": "ai_comm_log"},
        timeout=10.0,
    )
    resp.raise_for_status()
    properties = resp.json().get("properties") or {}
    return properties.get("ai_comm_log") or ""


async def update_contact(
    client: httpx.AsyncClient,
    token: str,
    base_url: str,
    contact_id: str,
    properties: dict[str, str],
) -> httpx.Response:
    """
    PATCH a contact with the given properties dict.

    Returns the raw httpx.Response. The caller (process_message) is responsible
    for calling raise_for_status() and mapping the status code to ack/nack/dead-letter.
    This keeps all retry-policy decisions in one place (main.py).
    """
    headers = _auth_headers(token)
    return await client.patch(
        f"{base_url}/crm/v3/objects/contacts/{contact_id}",
        headers=headers,
        json={"properties": properties},
        timeout=10.0,
    )
