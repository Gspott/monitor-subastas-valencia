"""Reusable analysis helpers for active and completed opportunity views."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any


DEFAULT_MIN_HISTORY_SAMPLE_SIZE = 3
DEFAULT_MAX_COMPLETED_HISTORY = 1500
TOP_OPPORTUNITY_MODE = "relaxed"


def build_completed_history_rows(auctions) -> list[dict[str, object]]:
    """Build lightweight completed rows used to derive historical signals."""
    rows: list[dict[str, object]] = []

    for auction in auctions:
        rows.append(
            {
                "municipality": auction.municipality or "",
                "postal_code": auction.postal_code or "",
                "_has_final_bid": auction.current_bid is not None and auction.current_bid > 0,
                "_final_bid_ratio_vs_starting_bid": compute_ratio(
                    numerator=auction.current_bid,
                    denominator=auction.starting_bid,
                ),
                "_sort_closing_date": auction.closing_date or date.max,
                "_has_closing_date": auction.closing_date is not None,
            }
        )

    return rows


def build_completed_history_signals(
    rows: list[dict[str, object]],
    *,
    min_sample_size: int = DEFAULT_MIN_HISTORY_SAMPLE_SIZE,
) -> dict[str, dict[str, dict[str, object]]]:
    """Build reusable historical signals from completed rows for active context."""
    return {
        "municipality": build_history_signal_map(
            rows,
            group_key="municipality",
            min_sample_size=min_sample_size,
        ),
        "postal_code": build_history_signal_map(
            rows,
            group_key="postal_code",
            min_sample_size=min_sample_size,
        ),
    }


def build_history_signal_map(
    rows: list[dict[str, object]],
    *,
    group_key: str,
    min_sample_size: int,
) -> dict[str, dict[str, object]]:
    """Build one grouped history map with conservative sample-size filtering."""
    grouped: dict[str, dict[str, object]] = {}
    for row in rows:
        group_value = str(row.get(group_key) or "-")
        bucket = grouped.setdefault(
            group_value,
            {
                "count": 0,
                "rows_with_bid": 0,
                "rows_without_bid": 0,
                "ratios": [],
            },
        )
        bucket["count"] += 1
        if row.get("_has_final_bid"):
            bucket["rows_with_bid"] += 1
        else:
            bucket["rows_without_bid"] += 1
        ratio = row.get("_final_bid_ratio_vs_starting_bid")
        if ratio is not None:
            bucket["ratios"].append(ratio)

    history_map: dict[str, dict[str, object]] = {}
    for group_value, metrics in grouped.items():
        sample_size = int(metrics["count"])
        if sample_size < min_sample_size:
            continue

        no_bid_rate = compute_fraction(
            int(metrics["rows_without_bid"]),
            sample_size,
        )
        avg_ratio = average_optional(metrics["ratios"])
        confidence_label = build_history_confidence_label(
            sample_size=sample_size,
            min_sample_size=min_sample_size,
        )
        history_map[group_value] = {
            "sample_size": sample_size,
            "no_bid_rate": no_bid_rate,
            "avg_final_ratio_vs_starting_bid": avg_ratio,
            "confidence_label": confidence_label,
            "heat_label": build_historical_heat_label(
                no_bid_rate=no_bid_rate,
                avg_final_ratio_vs_starting_bid=avg_ratio,
                sample_size=sample_size,
                min_sample_size=min_sample_size,
            ),
            "source_group": group_key,
        }

    return history_map


def resolve_active_history_signal(
    *,
    municipality: str | None,
    postal_code: str | None,
    historical_signals: dict[str, dict[str, dict[str, object]]],
) -> dict[str, object]:
    """Resolve the best available historical signal for one active row."""
    if postal_code:
        postal_signal = historical_signals.get("postal_code", {}).get(postal_code)
        if postal_signal is not None:
            return postal_signal

    if municipality:
        municipality_signal = historical_signals.get("municipality", {}).get(municipality)
        if municipality_signal is not None:
            return municipality_signal

    return {
        "sample_size": 0,
        "no_bid_rate": None,
        "avg_final_ratio_vs_starting_bid": None,
        "confidence_label": "insufficient",
        "heat_label": "unknown",
        "source_group": None,
    }


def build_historical_heat_label(
    *,
    no_bid_rate: Decimal | None,
    avg_final_ratio_vs_starting_bid: Decimal | None,
    sample_size: int,
    min_sample_size: int = DEFAULT_MIN_HISTORY_SAMPLE_SIZE,
) -> str:
    """Build a lightweight market-heat label from completed-auction history."""
    if no_bid_rate is None or avg_final_ratio_vs_starting_bid is None:
        return "unknown"
    confidence_label = build_history_confidence_label(
        sample_size=sample_size,
        min_sample_size=min_sample_size,
    )
    if confidence_label == "insufficient":
        return "unknown"
    if no_bid_rate >= Decimal("0.60"):
        base_label = "cold_market"
    elif no_bid_rate <= Decimal("0.25") and avg_final_ratio_vs_starting_bid >= Decimal("1.20"):
        base_label = "hot_market"
    else:
        base_label = "mixed_market"
    if confidence_label == "low":
        return f"{base_label}_low_confidence"
    return base_label


def build_history_confidence_label(
    *,
    sample_size: int,
    min_sample_size: int = DEFAULT_MIN_HISTORY_SAMPLE_SIZE,
) -> str:
    """Build a simple confidence label from historical sample size."""
    if sample_size < min_sample_size:
        return "insufficient"
    if sample_size < 10:
        return "low"
    if sample_size < 30:
        return "medium"
    return "high"


def select_recent_completed_history_rows(
    rows: list[dict[str, object]],
    *,
    max_rows: int = DEFAULT_MAX_COMPLETED_HISTORY,
) -> list[dict[str, object]]:
    """Keep only recent completed rows with a valid closing date for history signals."""
    dated_rows = [row for row in rows if row.get("_has_closing_date")]
    sorted_rows = sorted(
        dated_rows,
        key=lambda row: row["_sort_closing_date"],
        reverse=True,
    )
    return sorted_rows[:max_rows]


def build_active_history_context(
    *,
    municipality: str | None,
    postal_code: str | None,
    opening_bid_ratio: Decimal | None,
    has_price_data: bool,
    historical_signals: dict[str, dict[str, dict[str, object]]],
) -> dict[str, object]:
    """Build one reusable active-row history context from completed signals."""
    history_signal = resolve_active_history_signal(
        municipality=municipality,
        postal_code=postal_code,
        historical_signals=historical_signals,
    )
    return {
        "has_price_data": "yes" if has_price_data else "no",
        "opening_bid_ratio": format_ratio_value(opening_bid_ratio),
        "historical_no_bid_rate": format_ratio_value(history_signal["no_bid_rate"]),
        "historical_avg_final_ratio_vs_starting_bid": format_ratio_value(
            history_signal["avg_final_ratio_vs_starting_bid"]
        ),
        "historical_sample_size": history_signal["sample_size"],
        "historical_confidence": history_signal["confidence_label"],
        "historical_heat_label": history_signal["heat_label"],
    }


def build_display_location(
    *,
    municipality: str | None,
    postal_code: str | None,
    province: str | None,
) -> str:
    """Build a compact display location that prefers lot-level locality and postal code."""
    if municipality:
        if postal_code:
            return f"{municipality} ({postal_code})"
        return municipality

    if province:
        return province

    return "-"


def is_top_opportunity_evaluation(
    evaluation,
    *,
    historical_signals: dict[str, dict[str, dict[str, object]]],
) -> bool:
    """Evaluate the top-opportunity heuristic directly from one evaluation."""
    has_price_data = (
        evaluation.record.opening_bid is not None
        and evaluation.record.opening_bid > 0
        and evaluation.record.appraisal_value is not None
        and evaluation.record.appraisal_value > 0
    )
    row = build_active_history_context(
        municipality=evaluation.record.municipality,
        postal_code=evaluation.record.postal_code,
        opening_bid_ratio=evaluation.derivations.opening_bid_ratio,
        has_price_data=has_price_data,
        historical_signals=historical_signals,
    )
    return is_top_opportunity_row(row)


def is_top_opportunity_row(row: dict[str, object]) -> bool:
    """Apply the explicit top-opportunity heuristic used in the active view."""
    if row.get("has_price_data") != "yes":
        return False

    opening_bid_ratio = parse_display_ratio(str(row.get("opening_bid_ratio", "-")))
    if opening_bid_ratio is None:
        return False

    if TOP_OPPORTUNITY_MODE == "strict":
        max_opening_bid_ratio = Decimal("0.80")
        allowed_heat_labels = {"cold_market", "cold_market_low_confidence"}
    else:
        max_opening_bid_ratio = Decimal("1.00")
        allowed_heat_labels = {
            "cold_market",
            "cold_market_low_confidence",
            "mixed_market",
            "mixed_market_low_confidence",
        }

    if opening_bid_ratio > max_opening_bid_ratio:
        return False

    historical_confidence = str(row.get("historical_confidence", "insufficient"))
    if historical_confidence not in {"low", "medium", "high"}:
        return False

    historical_heat_label = str(row.get("historical_heat_label", "unknown"))
    return historical_heat_label in allowed_heat_labels


def filter_top_opportunity_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Keep only rows matching the explicit top-opportunity heuristic."""
    return [row for row in rows if is_top_opportunity_row(row)]


def parse_display_ratio(value: str | Decimal | None) -> Decimal | None:
    """Parse one already-formatted ratio value safely."""
    if value in (None, "", "-"):
        return None
    return Decimal(str(value))


def format_ratio_value(value: Decimal | None) -> str:
    """Format a ratio consistently for dashboard and Telegram reuse."""
    if value is None:
        return "-"
    return f"{float(value):.2f}"


def compute_ratio(*, numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    """Compute a safe ratio for optional decimal inputs."""
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def compute_fraction(numerator: int, denominator: int) -> Decimal | None:
    """Compute a safe fraction for grouped completed metrics."""
    if denominator <= 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def average_optional(values: list[Decimal]) -> Decimal | None:
    """Compute an average for optional decimal collections."""
    if not values:
        return None
    return sum(values) / Decimal(len(values))
