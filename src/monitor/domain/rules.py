"""Explicit business rules for opportunity detection."""

from __future__ import annotations

from decimal import Decimal

from .models import AuctionRecord, RecordDerivations, RuleResult


def evaluate_rules(record: AuctionRecord, derivations: RecordDerivations) -> list[RuleResult]:
    """Evaluate the first batch of transparent business rules."""
    return [
        _rule_opening_bid_very_low(derivations),
        _rule_opening_bid_reasonable(derivations),
        _rule_opening_bid_weak(derivations),
        _rule_deposit_reasonable(derivations),
        _rule_is_property(derivations),
        _rule_residential_like(derivations),
        _rule_target_area(derivations),
        _rule_unknown_charges(derivations),
        _rule_invalid_appraisal(derivations),
        _rule_complex_lots(derivations),
        _rule_critical_missing_data(derivations),
        _rule_poor_description(derivations),
    ]


def _rule_opening_bid_very_low(derivations: RecordDerivations) -> RuleResult:
    """Reward a very low opening bid compared with appraisal."""
    ratio = derivations.opening_bid_ratio
    triggered = ratio is not None and ratio < Decimal("0.20")
    return RuleResult(
        rule_code="opening_bid_very_low",
        kind="positive",
        score_delta=30 if triggered else 0,
        triggered=triggered,
        reason="Opening bid is very low relative to appraisal value",
    )


def _rule_opening_bid_reasonable(derivations: RecordDerivations) -> RuleResult:
    """Reward a moderate opening bid discount."""
    ratio = derivations.opening_bid_ratio
    triggered = ratio is not None and Decimal("0.20") <= ratio <= Decimal("0.40")
    return RuleResult(
        rule_code="opening_bid_reasonable",
        kind="positive",
        score_delta=15 if triggered else 0,
        triggered=triggered,
        reason="Opening bid still leaves room below appraisal value",
    )


def _rule_opening_bid_weak(derivations: RecordDerivations) -> RuleResult:
    """Penalize opening bids that leave little upside."""
    ratio = derivations.opening_bid_ratio
    triggered = ratio is not None and ratio > Decimal("0.50")
    return RuleResult(
        rule_code="opening_bid_weak",
        kind="negative",
        score_delta=-10 if triggered else 0,
        triggered=triggered,
        reason="Opening bid leaves limited margin versus appraisal value",
    )


def _rule_deposit_reasonable(derivations: RecordDerivations) -> RuleResult:
    """Reward deposits that do not look unusually heavy."""
    ratio = derivations.deposit_ratio
    triggered = ratio is not None and ratio <= Decimal("0.10")
    return RuleResult(
        rule_code="deposit_reasonable",
        kind="positive",
        score_delta=8 if triggered else 0,
        triggered=triggered,
        reason="Deposit looks reasonable relative to the opening bid",
    )


def _rule_is_property(derivations: RecordDerivations) -> RuleResult:
    """Reward property assets as the core project focus."""
    triggered = derivations.is_property
    return RuleResult(
        rule_code="is_property",
        kind="positive",
        score_delta=12 if triggered else 0,
        triggered=triggered,
        reason="Asset matches the property-focused target",
    )


def _rule_residential_like(derivations: RecordDerivations) -> RuleResult:
    """Reward residential-like property profiles."""
    triggered = derivations.is_residential_like
    return RuleResult(
        rule_code="is_residential_like",
        kind="positive",
        score_delta=12 if triggered else 0,
        triggered=triggered,
        reason="Residential-like asset profile",
    )


def _rule_target_area(derivations: RecordDerivations) -> RuleResult:
    """Reward auctions inside the current target geography."""
    triggered = derivations.is_in_target_area
    return RuleResult(
        rule_code="target_area",
        kind="positive",
        score_delta=10 if triggered else 0,
        triggered=triggered,
        reason="Location is inside the target area",
    )


def _rule_unknown_charges(derivations: RecordDerivations) -> RuleResult:
    """Penalize unclear charges because they increase review risk."""
    triggered = derivations.has_unknown_charges
    return RuleResult(
        rule_code="unknown_charges",
        kind="negative",
        score_delta=-20 if triggered else 0,
        triggered=triggered,
        reason="Charges are unknown or unclear",
    )


def _rule_invalid_appraisal(derivations: RecordDerivations) -> RuleResult:
    """Penalize appraisal values that are explicit but not reliable for valuation."""
    triggered = derivations.has_invalid_appraisal
    return RuleResult(
        rule_code="invalid_appraisal",
        kind="negative",
        score_delta=-15 if triggered else 0,
        triggered=triggered,
        reason="Appraisal value is explicit but not reliable",
    )


def _rule_complex_lots(derivations: RecordDerivations) -> RuleResult:
    """Penalize multi-lot structures because they complicate evaluation."""
    triggered = derivations.has_complex_lot_structure
    return RuleResult(
        rule_code="complex_lot_structure",
        kind="negative",
        score_delta=-10 if triggered else 0,
        triggered=triggered,
        reason="Lot structure is complex",
    )


def _rule_critical_missing_data(derivations: RecordDerivations) -> RuleResult:
    """Penalize records missing key data for pricing or location."""
    triggered = derivations.has_critical_missing_data
    return RuleResult(
        rule_code="critical_missing_data",
        kind="negative",
        score_delta=-25 if triggered else 0,
        triggered=triggered,
        reason="Critical data is missing",
    )


def _rule_poor_description(derivations: RecordDerivations) -> RuleResult:
    """Penalize records with weak descriptions because review is harder."""
    triggered = derivations.description_is_poor
    return RuleResult(
        rule_code="poor_description",
        kind="warning",
        score_delta=-6 if triggered else 0,
        triggered=triggered,
        reason="Description is too poor for confident review",
    )
