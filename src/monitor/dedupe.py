"""Deduplication helpers for auction data."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from .models import Auction


logger = logging.getLogger(__name__)


def dedupe_auctions(auctions: Iterable[Auction]) -> list[Auction]:
    """Remove duplicates while preserving the best record for each logical item."""
    deduped: list[Auction] = []
    index_by_external_id: dict[str, int] = {}
    indices_by_fallback_base: dict[tuple[str, str, str], list[int]] = {}

    for auction in auctions:
        external_id_key = build_external_id_key(auction)
        if external_id_key is not None and external_id_key in index_by_external_id:
            logger.debug("Merging auction by external_id key %s.", external_id_key)
            deduped[index_by_external_id[external_id_key]] = merge_auctions(
                deduped[index_by_external_id[external_id_key]],
                auction,
            )
            continue

        fallback_base_key = build_fallback_base_key(auction)
        matched_index = _find_compatible_fallback_match(
            auction=auction,
            deduped=deduped,
            candidate_indices=indices_by_fallback_base.get(fallback_base_key, []),
        )
        if matched_index is not None:
            logger.debug(
                "Merging auction by fallback key %s using compatible signals.",
                fallback_base_key,
            )
            deduped[matched_index] = merge_auctions(deduped[matched_index], auction)
            continue

        deduped.append(auction)
        new_index = len(deduped) - 1

        if external_id_key is not None:
            index_by_external_id[external_id_key] = new_index
        if fallback_base_key is not None:
            indices_by_fallback_base.setdefault(fallback_base_key, []).append(new_index)

    return deduped


def merge_auctions(left: Auction, right: Auction) -> Auction:
    """Merge two auctions and keep the record with more useful information."""
    left_score = score_auction_information(left)
    right_score = score_auction_information(right)

    preferred = left if left_score >= right_score else right
    other = right if preferred is left else left

    merged_payload = preferred.model_dump()

    # Rellenar los huecos del registro preferido con datos del alternativo.
    for field_name, value in other.model_dump().items():
        if not _has_useful_value(merged_payload.get(field_name)) and _has_useful_value(value):
            merged_payload[field_name] = value

    return Auction(**merged_payload)


def build_dedupe_key(auction: Auction) -> str | None:
    """Build the technical persistence identity used for dedupe and SQLite storage."""
    external_id_key = build_external_id_key(auction)
    if external_id_key is not None:
        return external_id_key

    fallback_key = build_fallback_key(auction)
    if fallback_key is None:
        return None

    return "|".join(fallback_key)


def score_auction_information(auction: Auction) -> int:
    """Estimate how much useful information a record contains."""
    score = 0

    for field_name, value in auction.model_dump().items():
        if field_name == "is_vehicle":
            continue

        if _has_useful_value(value):
            score += 1

    return score


def build_external_id_key(auction: Auction) -> str | None:
    """Build a stable dedupe key from external_id when available."""
    if auction.external_id is None:
        return None

    external_id = auction.external_id.strip()
    if not external_id:
        return None

    return f"{auction.source.casefold()}::{external_id.casefold()}"


def build_fallback_key(auction: Auction) -> tuple[str, ...] | None:
    """Build a stronger fallback key from non-personal fields and safe signals."""
    fallback_base_key = build_fallback_base_key(auction)
    if fallback_base_key is None:
        return None

    key_parts = list(fallback_base_key)
    official_url = _normalize_optional_text(auction.official_url)
    if official_url is not None:
        key_parts.append(f"url:{official_url}")

    for date_label, date_value in (
        ("publication", auction.publication_date),
        ("opening", auction.opening_date),
        ("closing", auction.closing_date),
    ):
        if date_value is not None:
            key_parts.append(f"{date_label}:{date_value.isoformat()}")

    return tuple(key_parts)


def build_fallback_base_key(auction: Auction) -> tuple[str, str, str] | None:
    """Build the minimum non-personal fallback key required by project rules."""
    municipality = _normalize_optional_text(auction.municipality)
    asset_class = _normalize_optional_text(auction.asset_class)
    if municipality is None or asset_class is None:
        return None
    if auction.appraisal_value is None:
        return None

    return (
        municipality,
        asset_class,
        str(auction.appraisal_value),
    )


def _find_compatible_fallback_match(
    auction: Auction,
    deduped: list[Auction],
    candidate_indices: list[int],
) -> int | None:
    """Find one fallback match only when strong signals do not conflict."""
    for candidate_index in candidate_indices:
        existing = deduped[candidate_index]
        compatibility = _compare_fallback_signals(existing, auction)
        if compatibility == "match":
            return candidate_index
        if compatibility == "conflict":
            logger.info(
                "Skipping ambiguous fallback merge for municipality=%s asset_class=%s appraisal_value=%s.",
                auction.municipality,
                auction.asset_class,
                auction.appraisal_value,
            )

    return None


def _compare_fallback_signals(left: Auction, right: Auction) -> str:
    """Compare optional strong signals to avoid ambiguous fallback merges."""
    left_url = _normalize_optional_text(left.official_url)
    right_url = _normalize_optional_text(right.official_url)
    if left_url is not None and right_url is not None and left_url != right_url:
        logger.debug("Fallback conflict due to different official_url values.")
        return "conflict"

    for date_label, left_date, right_date in (
        ("publication_date", left.publication_date, right.publication_date),
        ("opening_date", left.opening_date, right.opening_date),
        ("closing_date", left.closing_date, right.closing_date),
    ):
        if left_date is not None and right_date is not None and left_date != right_date:
            logger.debug("Fallback conflict due to different %s values.", date_label)
            return "conflict"

    return "match"


def _normalize_optional_text(value: str | None) -> str | None:
    """Normalize optional text for identity comparisons."""
    if value is None:
        return None

    normalized = value.strip().casefold()
    return normalized or None


def _has_useful_value(value: object) -> bool:
    """Decide whether a value contributes meaningful information."""
    if value is None:
        return False

    if isinstance(value, str):
        return bool(value.strip())

    return True
