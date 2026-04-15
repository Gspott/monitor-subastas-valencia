"""Audit how parsed BOE auction fields are currently mapped in SQLite."""

from __future__ import annotations

import re
import sys
from decimal import Decimal
from pathlib import Path


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.pipeline.evaluate import build_auction_record
from monitor.storage import fetch_all_auctions


MAX_AUCTIONS = 20
MULTI_LOT_HINTS = (
    "cada lote",
    "varios lotes",
    "separada para cada lote",
    "ver valor de subasta en cada lote",
    "ver valor de tasacion en cada lote",
    "ver valor de tasación en cada lote",
    "adjudicacion independiente",
    "adjudicación independiente",
)
LOT_COUNT_RE = re.compile(r"\((\d+)\s+lotes?\)", re.IGNORECASE)


def main() -> None:
    """Print a compact audit of key parsed auction fields."""
    auctions = fetch_all_auctions()[:MAX_AUCTIONS]

    if not auctions:
        print("No auctions found in the local SQLite database.")
        return

    stats = {
        "ok": 0,
        "missing": 0,
        "zero_appraisal": 0,
        "multi_lot": 0,
        "possible_multi_lot_not_detected": 0,
    }

    for auction in auctions:
        record = build_auction_record(auction)
        inferred_has_lots = infer_has_lots(record.title, record.description)
        inferred_lot_count = infer_lot_count(record.title, record.description)
        has_multi_lot_text = contains_multi_lot_text(record.title, record.description)
        status_label = build_status_label(
            opening_bid=record.opening_bid,
            appraisal_value=record.appraisal_value,
            has_multi_lot_text=has_multi_lot_text,
            parser_has_lots=record.has_lots,
            inferred_has_lots=inferred_has_lots,
        )

        if status_label == "OK":
            stats["ok"] += 1
        if status_label == "WARNING - MISSING DATA":
            stats["missing"] += 1
        if status_label == "WARNING - ZERO APPRAISAL":
            stats["zero_appraisal"] += 1
        if status_label == "INFO - MULTI LOT":
            stats["multi_lot"] += 1
        if status_label == "WARNING - POSSIBLE MULTI LOT NOT DETECTED":
            stats["possible_multi_lot_not_detected"] += 1

        print(f"[{status_label}]")
        print(record.auction_id or "-")
        print(record.title)
        print(f"opening_bid={format_decimal(record.opening_bid)}")
        print(f"appraisal_value={format_decimal(record.appraisal_value)}")
        print(f"deposit={format_decimal(record.deposit)}")
        print(f"has_lots={record.has_lots}")
        print(f"inferred_has_lots={inferred_has_lots}")
        print(f"lot_count={record.lot_count}")
        print(f"inferred_lot_count={inferred_lot_count}")
        print(f"municipality={record.municipality or '-'}")
        print(f"source_url={record.source_url or '-'}")
        print()

    print("Summary")
    print(f"total_auctions={len(auctions)}")
    print(f"ok={stats['ok']}")
    print(f"missing={stats['missing']}")
    print(f"zero_appraisal={stats['zero_appraisal']}")
    print(f"multi_lot={stats['multi_lot']}")
    print(f"possible_multi_lot_not_detected={stats['possible_multi_lot_not_detected']}")


def build_status_label(
    *,
    opening_bid: Decimal | None,
    appraisal_value: Decimal | None,
    has_multi_lot_text: bool,
    parser_has_lots: bool | None,
    inferred_has_lots: bool,
) -> str:
    """Classify one auction into a small set of audit-friendly states."""
    if appraisal_value == Decimal("0"):
        return "WARNING - ZERO APPRAISAL"
    if opening_bid is None and appraisal_value is None:
        return "WARNING - MISSING DATA"
    if has_multi_lot_text and parser_has_lots is not True:
        return "WARNING - POSSIBLE MULTI LOT NOT DETECTED"
    if inferred_has_lots:
        return "INFO - MULTI LOT"
    if opening_bid is not None and opening_bid > 0 and appraisal_value is not None and appraisal_value > 0:
        return "OK"
    return "INFO - PARTIAL DATA"


def infer_has_lots(title: str, description: str | None) -> bool:
    """Infer a lot structure from already stored public text."""
    searchable_text = " ".join(value for value in (title, description) if value).casefold()
    return "lote" in searchable_text or "lotes" in searchable_text


def infer_lot_count(title: str, description: str | None) -> int | None:
    """Infer lot count only from explicit text patterns."""
    searchable_text = " ".join(value for value in (title, description) if value)
    match = LOT_COUNT_RE.search(searchable_text)
    if match is None:
        return None
    return int(match.group(1))


def contains_multi_lot_text(title: str, description: str | None) -> bool:
    """Detect phrases that strongly suggest a lot-scoped auction."""
    searchable_text = " ".join(value for value in (title, description) if value).casefold()
    return any(hint in searchable_text for hint in MULTI_LOT_HINTS)


def format_decimal(value: Decimal | None) -> str:
    """Format decimal values without adding presentation noise."""
    if value is None:
        return "None"
    return format(value, "f")


if __name__ == "__main__":
    main()
