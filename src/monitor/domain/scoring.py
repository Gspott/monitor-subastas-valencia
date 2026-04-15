"""Scoring helpers for opportunity evaluations."""

from __future__ import annotations

from .models import OpportunityCategory, OpportunityEvaluation, AuctionRecord, RecordDerivations, RuleResult


BASE_SCORE = 50


def build_evaluation(
    record: AuctionRecord,
    derivations: RecordDerivations,
    applied_filters: list[str],
    rule_results: list[RuleResult],
) -> OpportunityEvaluation:
    """Build the final evaluation from filters and rule results."""
    if applied_filters:
        return OpportunityEvaluation(
            record=record,
            derivations=derivations,
            applied_filters=applied_filters,
            rule_results=rule_results,
            score=0,
            category="discard",
            positive_reasons=[],
            negative_reasons=applied_filters,
            warnings=_collect_warnings(rule_results, record),
        )

    raw_score = BASE_SCORE + sum(result.score_delta for result in rule_results if result.triggered)
    bounded_score = max(0, min(100, raw_score))

    return OpportunityEvaluation(
        record=record,
        derivations=derivations,
        applied_filters=[],
        rule_results=rule_results,
        score=bounded_score,
        category=_assign_category(bounded_score),
        positive_reasons=_collect_reasons(rule_results, "positive"),
        negative_reasons=_collect_reasons(rule_results, "negative"),
        warnings=_collect_warnings(rule_results, record),
    )


def _assign_category(score: int) -> OpportunityCategory:
    """Assign a simple category from the final score."""
    if score >= 70:
        return "high_interest"
    if score >= 40:
        return "review"
    return "discard"


def _collect_reasons(rule_results: list[RuleResult], kind: str) -> list[str]:
    """Collect triggered reasons by rule type."""
    return [
        result.reason
        for result in rule_results
        if result.triggered and result.kind == kind
    ]


def _collect_warnings(rule_results: list[RuleResult], record: AuctionRecord) -> list[str]:
    """Collect warnings from rules and parser-level caveats."""
    warnings = [
        result.reason
        for result in rule_results
        if result.triggered and result.kind == "warning"
    ]
    warnings.extend(record.parser_warnings)
    return warnings
