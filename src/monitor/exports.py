"""CSV export helpers for processed auctions."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .config import DATA_DIR
from .models import Auction
from .status import is_active_status


NEW_AUCTIONS_EXPORT_PATH = DATA_DIR / "new_auctions.csv"
ALL_ACTIVE_VALENCIA_EXPORT_PATH = DATA_DIR / "all_active_valencia.csv"

EXPORT_FIELDNAMES = [
    "source",
    "external_id",
    "title",
    "province",
    "municipality",
    "asset_class",
    "asset_subclass",
    "official_status",
    "publication_date",
    "opening_date",
    "closing_date",
    "appraisal_value",
    "starting_bid",
    "current_bid",
    "deposit",
    "score",
    "occupancy_status",
    "encumbrances_summary",
    "description",
    "official_url",
]


def export_new_auctions(auctions: Iterable[Auction]) -> Path:
    """Export auctions that are new in the current run to CSV."""
    return export_auctions_to_csv(auctions, NEW_AUCTIONS_EXPORT_PATH)


def export_all_active_valencia(auctions: Iterable[Auction]) -> Path:
    """Export all active Valencia auctions to CSV."""
    active_auctions = [
        auction
        for auction in auctions
        if auction.province == "Valencia" and not auction.is_vehicle and is_active_status(auction.official_status)
    ]
    return export_auctions_to_csv(active_auctions, ALL_ACTIVE_VALENCIA_EXPORT_PATH)


def export_auctions_to_csv(auctions: Iterable[Auction], output_path: Path) -> Path:
    """Export auctions to a CSV file ordered by score descending when available."""
    ordered_auctions = sort_auctions_for_export(auctions)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=EXPORT_FIELDNAMES)
        writer.writeheader()

        # Escribir un formato plano y estable para facilitar revisiones manuales.
        for auction in ordered_auctions:
            writer.writerow(_auction_to_export_row(auction))

    return output_path


def sort_auctions_for_export(auctions: Iterable[Auction]) -> list[Auction]:
    """Sort auctions by score descending and keep a stable secondary order."""
    return sorted(
        auctions,
        key=lambda auction: (
            auction.score is None,
            -(auction.score or 0),
            auction.municipality.casefold(),
            auction.title.casefold(),
            (auction.external_id or "").casefold(),
        ),
    )


def _auction_to_export_row(auction: Auction) -> dict[str, str | int | None]:
    """Convert an auction object to a flat export row."""
    return {
        "source": auction.source,
        "external_id": auction.external_id,
        "title": auction.title,
        "province": auction.province,
        "municipality": auction.municipality,
        "asset_class": auction.asset_class,
        "asset_subclass": auction.asset_subclass,
        "official_status": auction.official_status,
        "publication_date": _serialize_date(auction.publication_date),
        "opening_date": _serialize_date(auction.opening_date),
        "closing_date": _serialize_date(auction.closing_date),
        "appraisal_value": _serialize_decimal(auction.appraisal_value),
        "starting_bid": _serialize_decimal(auction.starting_bid),
        "current_bid": _serialize_decimal(auction.current_bid),
        "deposit": _serialize_decimal(auction.deposit),
        "score": auction.score,
        "occupancy_status": auction.occupancy_status,
        "encumbrances_summary": auction.encumbrances_summary,
        "description": auction.description,
        "official_url": auction.official_url,
    }


def _serialize_date(value) -> str | None:
    """Serialize date values to ISO strings."""
    if value is None:
        return None

    return value.isoformat()


def _serialize_decimal(value) -> str | None:
    """Serialize decimal values to string for CSV."""
    if value is None:
        return None

    return str(value)
