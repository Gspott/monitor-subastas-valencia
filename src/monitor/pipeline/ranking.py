"""Ranking and export helpers for evaluated auction opportunities."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from pathlib import Path

from ..domain.models import OpportunityEvaluation
from ..models import Auction
from .evaluate import build_auction_record, evaluate_opportunity


CATEGORY_PRIORITY = {
    "high_interest": 0,
    "review": 1,
    "discard": 2,
}


def evaluate_auctions(auctions: Iterable[Auction]) -> list[OpportunityEvaluation]:
    """Evaluate parsed auctions through the business pipeline."""
    return [
        evaluate_opportunity(build_auction_record(auction))
        for auction in auctions
    ]


def rank_opportunities(auctions: Iterable[Auction]) -> list[OpportunityEvaluation]:
    """Evaluate and rank auctions for practical review order."""
    evaluations = evaluate_auctions(auctions)
    return sorted(evaluations, key=_build_ranking_key)


def filter_actionable_opportunities(
    evaluations: Iterable[OpportunityEvaluation],
    categories: Iterable[str] | None = None,
    min_score: int | None = None,
    top_n: int | None = None,
) -> list[OpportunityEvaluation]:
    """Filter already-ranked evaluations while preserving their current order."""
    evaluation_list = list(evaluations)
    if categories is None and min_score is None and top_n is None:
        return evaluation_list.copy()

    allowed_categories = set(categories) if categories is not None else None
    filtered = [
        evaluation
        for evaluation in evaluation_list
        if (allowed_categories is None or evaluation.category in allowed_categories)
        and (min_score is None or evaluation.score >= min_score)
    ]

    if top_n is not None:
        if top_n <= 0:
            return []
        return filtered[:top_n]

    return filtered


def rank_and_filter_opportunities(
    auctions: Iterable[Auction],
    categories: Iterable[str] | None = None,
    min_score: int | None = None,
    top_n: int | None = None,
) -> list[OpportunityEvaluation]:
    """Run the full ranking flow and keep only actionable results."""
    return filter_actionable_opportunities(
        rank_opportunities(auctions),
        categories=categories,
        min_score=min_score,
        top_n=top_n,
    )


def export_opportunities_to_csv(
    evaluations: Iterable[OpportunityEvaluation],
    filepath: str | Path,
) -> Path:
    """Export ranked opportunity evaluations to a CSV file."""
    output_path = Path(filepath)
    fieldnames = [
        "auction_id",
        "lot_number",
        "title",
        "municipality",
        "asset_type",
        "appraisal_value",
        "opening_bid",
        "opening_bid_ratio",
        "score",
        "category",
        "positive_reasons",
        "negative_reasons",
        "warnings",
        "source_url",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for evaluation in evaluations:
            writer.writerow(
                {
                    "auction_id": evaluation.record.auction_id or "",
                    "lot_number": evaluation.record.lot_number or "",
                    "title": evaluation.record.title or "",
                    "municipality": evaluation.record.municipality or "",
                    "asset_type": evaluation.record.asset_type or "",
                    "appraisal_value": _format_decimal(evaluation.record.appraisal_value),
                    "opening_bid": _format_decimal(evaluation.record.opening_bid),
                    "opening_bid_ratio": _format_decimal(evaluation.derivations.opening_bid_ratio),
                    "score": evaluation.score,
                    "category": evaluation.category,
                    "positive_reasons": " | ".join(evaluation.positive_reasons),
                    "negative_reasons": " | ".join(evaluation.negative_reasons),
                    "warnings": " | ".join(evaluation.warnings),
                    "source_url": evaluation.record.source_url or "",
                }
            )

    return output_path


def export_opportunities_to_json(
    evaluations: Iterable[OpportunityEvaluation],
    filepath: str | Path,
) -> Path:
    """Export ranked opportunity evaluations to a readable JSON file."""
    output_path = Path(filepath)
    payload = [_serialize_evaluation(evaluation) for evaluation in evaluations]

    with output_path.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, indent=2, ensure_ascii=True)

    return output_path


def _build_ranking_key(evaluation: OpportunityEvaluation) -> tuple[int, int, date]:
    """Build a stable ranking key from category, score, and date."""
    auction_date = evaluation.record.auction_date or date.max
    return (
        CATEGORY_PRIORITY[evaluation.category],
        -evaluation.score,
        auction_date,
    )


def _serialize_evaluation(evaluation: OpportunityEvaluation) -> dict[str, object]:
    """Build a compact and readable JSON structure."""
    return {
        "auction": {
            "auction_id": evaluation.record.auction_id,
            "lot_number": evaluation.record.lot_number,
            "title": evaluation.record.title,
            "municipality": evaluation.record.municipality,
            "province": evaluation.record.province,
            "asset_type": evaluation.record.asset_type,
            "asset_subtype": evaluation.record.asset_subtype,
            "appraisal_value": _format_decimal(evaluation.record.appraisal_value),
            "opening_bid": _format_decimal(evaluation.record.opening_bid),
            "opening_bid_ratio": _format_decimal(evaluation.derivations.opening_bid_ratio),
            "deposit": _format_decimal(evaluation.record.deposit),
            "auction_date": evaluation.record.auction_date.isoformat() if evaluation.record.auction_date else None,
            "source_url": evaluation.record.source_url,
        },
        "evaluation": {
            "score": evaluation.score,
            "category": evaluation.category,
            "reasons": {
                "positive": evaluation.positive_reasons,
                "negative": evaluation.negative_reasons,
            },
            "warnings": evaluation.warnings,
        },
    }


def _format_decimal(value: Decimal | None) -> str:
    """Format decimals consistently while keeping empty values simple."""
    if value is None:
        return ""
    return format(value, "f")
