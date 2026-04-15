"""Filtering helpers for records that are not worth evaluating further."""

from __future__ import annotations

from .models import AuctionRecord, RecordDerivations


def collect_filter_reasons(record: AuctionRecord, derivations: RecordDerivations) -> list[str]:
    """Return clear reasons for discarding or downgrading a record."""
    reasons: list[str] = []

    if derivations.has_critical_missing_data:
        reasons.append("Missing critical price or location data")

    if not derivations.has_minimum_location:
        reasons.append("Missing minimum usable location data")

    if not derivations.is_property:
        reasons.append("Asset type is outside the current target")

    if (
        record.is_detail_complete is False
        and derivations.has_critical_missing_data
        and not derivations.has_complex_lot_structure
    ):
        reasons.append("Record is too incomplete for confident review")

    return reasons


def is_record_evaluable(record: AuctionRecord, derivations: RecordDerivations) -> bool:
    """Check whether the record is good enough for rule evaluation."""
    return len(collect_filter_reasons(record, derivations)) == 0
