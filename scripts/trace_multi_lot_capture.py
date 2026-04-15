"""Trace the full capture path for one real BOE multi-lot auction."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.pipeline.evaluate import build_auction_lot_record
from monitor.sources.boe import (
    REQUEST_TIMEOUT_SECONDS,
    build_lot_detail_url,
    parse_detail_lot_numbers_page,
    parse_detail_lots_page,
)
from monitor.storage import fetch_all_auctions


TARGET_AUCTION_ID = "SUB-JA-2026-257653"
LOT_NUMBERS_TO_PROBE = 10


def main() -> None:
    """Trace one multi-lot auction from HTML to persisted rows."""
    auctions = fetch_all_auctions()
    auction = next((item for item in auctions if item.external_id == TARGET_AUCTION_ID), None)
    if auction is None:
        print(f"Auction {TARGET_AUCTION_ID} was not found in SQLite.")
        return

    if auction.official_url is None:
        print(f"Auction {TARGET_AUCTION_ID} does not have source_url available.")
        return

    persisted_lots = [
        item
        for item in auctions
        if item.external_id is not None and item.external_id.startswith(f"{TARGET_AUCTION_ID}::lot:")
    ]

    print(f"auction_id: {auction.external_id}")
    print(f"source_url: {auction.official_url}")
    print(f"persisted_lots_in_sqlite: {len(persisted_lots)}")
    if persisted_lots:
        print("persisted_external_ids:")
        for item in sorted(persisted_lots, key=lambda current: current.external_id or ""):
            print(f"  - {item.external_id}")

    with requests.Session() as session:
        lot_tab_url = build_lot_detail_url(auction.official_url)
        lot_tab_response = session.get(lot_tab_url, timeout=REQUEST_TIMEOUT_SECONDS)
        lot_tab_response.raise_for_status()

        soup = BeautifulSoup(lot_tab_response.text, "html.parser")
        tab_ids = [tag.get("id") for tag in soup.select("#tabsver a[id^='idTabLote']") if isinstance(tag, Tag)]
        block_ids = [
            tag.get("id")
            for tag in soup.select("#idBloqueDatos3 div[id^='idBloqueLote']")
            if isinstance(tag, Tag)
        ]
        parsed_lot_numbers = parse_detail_lot_numbers_page(lot_tab_response.text)
        expected_lot_count = _infer_expected_lot_count(auction.title, tab_ids)

        print()
        print("HTML overview")
        print(f"  expected_lot_count: {expected_lot_count if expected_lot_count is not None else '-'}")
        print(f"  detected_tab_ids: {tab_ids or []}")
        print(f"  detected_block_ids: {block_ids or []}")
        print(f"  parsed_lot_numbers_from_tab_page: {parsed_lot_numbers}")

        parsed_lots = []
        evaluable_records = []
        numbers_to_fetch = parsed_lot_numbers[:LOT_NUMBERS_TO_PROBE]

        for lot_number in numbers_to_fetch:
            lot_url = build_lot_detail_url(auction.official_url, lot_number)
            lot_response = session.get(lot_url, timeout=REQUEST_TIMEOUT_SECONDS)
            lot_response.raise_for_status()

            lot_soup = BeautifulSoup(lot_response.text, "html.parser")
            lot_block_ids = [
                tag.get("id")
                for tag in lot_soup.select("#idBloqueDatos3 div[id^='idBloqueLote']")
                if isinstance(tag, Tag)
            ]
            lot_tab_ids = [
                tag.get("id")
                for tag in lot_soup.select("#tabsver a[id^='idTabLote']")
                if isinstance(tag, Tag)
            ]
            page_lots = parse_detail_lots_page(lot_response.text)

            print()
            print(f"Lot page {lot_number}")
            print(f"  url: {lot_url}")
            print(f"  tab_ids: {lot_tab_ids or []}")
            print(f"  block_ids: {lot_block_ids or []}")
            print(
                "  parsed_lots: "
                + str(
                    [
                        {
                            "lot_number": item.lot_number,
                            "opening_bid": _format_decimal(item.starting_bid),
                            "appraisal_value": _format_decimal(item.appraisal_value),
                            "deposit": _format_decimal(item.deposit),
                            "title": item.title,
                        }
                        for item in page_lots
                    ]
                )
            )

            parsed_lot = next((item for item in page_lots if item.lot_number == lot_number), None)
            if parsed_lot is None:
                print("  diagnosis: parser did not return the requested lot number on this page")
                continue

            parsed_lots.append(parsed_lot)
            record = build_auction_lot_record(auction, parsed_lot)
            evaluable_records.append(record)
            print(
                "  evaluable_record: "
                + str(
                    {
                        "auction_id": record.auction_id,
                        "lot_number": record.lot_number,
                        "opening_bid": _format_decimal(record.opening_bid),
                        "appraisal_value": _format_decimal(record.appraisal_value),
                        "deposit": _format_decimal(record.deposit),
                        "source_url": record.source_url,
                    }
                )
            )

    print()
    print("Summary")
    print(f"  expected_lot_count: {expected_lot_count if expected_lot_count is not None else '-'}")
    print(f"  parsed_lot_numbers_from_tab_page: {parsed_lot_numbers}")
    print(f"  parsed_lots_from_individual_pages: {[item.lot_number for item in parsed_lots]}")
    print(f"  evaluable_records_built: {len(evaluable_records)}")
    print(f"  persisted_lots_in_sqlite: {len(persisted_lots)}")

    if expected_lot_count is not None and len(parsed_lot_numbers) < expected_lot_count:
        print("  diagnosis: lot tab parsing is incomplete before fetching individual lot pages")
    elif len(parsed_lots) < len(parsed_lot_numbers):
        print("  diagnosis: some lot pages were fetched but not parsed into lot objects")
    elif len(persisted_lots) < len(parsed_lots):
        print("  diagnosis: lot parsing succeeded but persistence or loader selection is incomplete")
    else:
        print("  diagnosis: all detected lots are being captured consistently")


def _infer_expected_lot_count(title: str, tab_ids: list[str | None]) -> int | None:
    """Infer the expected lot count from visible metadata."""
    match = re.search(r"\((\d+)\s+lotes\)", title, flags=re.IGNORECASE)
    if match is not None:
        return int(match.group(1))

    cleaned_tab_ids = [value for value in tab_ids if isinstance(value, str)]
    if cleaned_tab_ids:
        return len(cleaned_tab_ids)

    return None


def _format_decimal(value) -> str:
    """Format decimal-like values for readable console output."""
    return "-" if value is None else str(value)


if __name__ == "__main__":
    main()
