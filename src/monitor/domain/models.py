"""Business models for opportunity evaluation."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


OpportunityCategory = Literal["high_interest", "review", "discard"]
RuleKind = Literal["positive", "negative", "warning"]


class AuctionRecord(BaseModel):
    """Normalized business view of one auction."""

    auction_id: str | None = None
    source_url: str | None = None
    lot_number: int | None = Field(default=None, ge=1)
    title: str
    description: str | None = None
    asset_type: str
    asset_subtype: str
    province: str | None = None
    municipality: str | None = None
    postal_code: str | None = None
    address_text: str | None = None
    appraisal_value: Decimal | None = None
    opening_bid: Decimal | None = None
    deposit: Decimal | None = None
    auction_date: date | None = None
    has_lots: bool | None = None
    lot_count: int | None = Field(default=None, ge=0)
    charges_text: str | None = None
    occupancy_text: str | None = None
    is_detail_complete: bool | None = None
    parser_warnings: list[str] = Field(default_factory=list)


class RecordDerivations(BaseModel):
    """Derived fields computed from a business record."""

    opening_bid_ratio: Decimal | None = None
    deposit_ratio: Decimal | None = None
    has_invalid_appraisal: bool = False
    has_reference_price_data: bool = False
    is_property: bool = False
    is_residential_like: bool = False
    is_in_target_area: bool = False
    has_unknown_charges: bool = False
    has_complex_lot_structure: bool = False
    has_critical_missing_data: bool = False
    has_minimum_location: bool = False
    description_is_poor: bool = False


class RuleResult(BaseModel):
    """Result of one explicit business rule."""

    rule_code: str
    kind: RuleKind
    score_delta: int
    triggered: bool
    reason: str


class OpportunityEvaluation(BaseModel):
    """Final evaluation of one auction opportunity."""

    record: AuctionRecord
    derivations: RecordDerivations
    applied_filters: list[str] = Field(default_factory=list)
    rule_results: list[RuleResult] = Field(default_factory=list)
    score: int = Field(..., ge=0, le=100)
    category: OpportunityCategory
    positive_reasons: list[str] = Field(default_factory=list)
    negative_reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
