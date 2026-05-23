"""
Enriched event contract model.

This is emitted by the Intelligence Layer (NOT the Communication Layer)
after GPT-4o has processed a transcript or message.

WHY a separate model rather than adding fields to the base events:
- The base events must be emitted < 1 second after a webhook arrives.
- Enrichment is slow (LLM call) — it happens minutes later.
- Consumers that only need the raw event never wait for AI.
- Keeping the models separate enforces this at the type level.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from comm_layer.contracts.base import BaseCommEvent


class ActionItem(BaseModel):
    """A follow-up task extracted from the conversation."""

    description: str
    priority: str = Field(..., description="'high' | 'medium' | 'low'")

    model_config = {"extra": "forbid"}


class Entity(BaseModel):
    """A named entity extracted from the conversation."""

    entity_type: str = Field(..., description="e.g. 'PRODUCT', 'PERSON', 'DATE', 'AMOUNT'")
    value: str

    model_config = {"extra": "forbid"}


class EnrichmentData(BaseModel):
    """
    Structured output from GPT-4o.
    Validated by Pydantic — if the LLM returns an invalid structure, we retry
    the model call up to 2 times before marking the enrichment as failed.
    """

    summary: str = Field(..., description="1-3 sentence summary of the conversation")
    intent: str = Field(
        ...,
        description=(
            "Primary intent e.g. 'support_request', 'sales_inquiry', 'complaint', 'general_query'"
        ),
    )
    sentiment: Literal["positive", "neutral", "negative"] = Field(
        ..., description="Overall sentiment of the conversation"
    )
    entities: list[Entity] = Field(
        default_factory=list, description="Named entities mentioned"
    )
    action_items: list[ActionItem] = Field(
        default_factory=list, description="Follow-up tasks extracted from the conversation"
    )

    model_config = {"extra": "forbid"}


class EnrichedCommEvent(BaseCommEvent):
    """
    Emitted by the Intelligence Layer after AI enrichment completes.

    Contains the original event fields PLUS the enrichment data.
    Consumers can treat this as a superset of the original event.
    """

    event_type: Literal["comm.enriched"] = "comm.enriched"

    # Which original event triggered this enrichment
    original_event_key: str = Field(..., description="event_key of the source event")
    original_event_type: str = Field(..., description="event_type of the source event")

    # The enrichment output
    enrichment: EnrichmentData

    # Model metadata
    model_used: str = Field(..., description="e.g. 'gpt-4o'")
    enrichment_schema_version: str = Field("1.0")

    model_config = {"extra": "forbid", "use_enum_values": True}
