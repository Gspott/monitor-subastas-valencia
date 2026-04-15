"""Trace key amount fields across HTML, parser, persistence, and adaptation."""

from __future__ import annotations

import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.pipeline.evaluate import build_auction_record
from monitor.sources.boe import (
    DETAIL_TABLE_SELECTOR,
    REQUEST_TIMEOUT_SECONDS,
    parse_detail_page,
    slugify_asset_label,
)
from monitor.storage import fetch_all_auctions


TARGET_AUCTION_IDS = [
    "SUB-AT-2026-26R4686001035",
    "SUB-AT-2026-26R4686001049",
    "SUB-JA-2025-242246",
    "SUB-JA-2026-257418",
    "SUB-JA-2026-256559",
]


def main() -> None:
    """Trace the mapping chain for a small fixed set of auctions."""
    auctions_by_id = {
        auction.external_id: auction
        for auction in fetch_all_auctions()
        if auction.external_id is not None
    }

    with requests.Session() as session:
        for auction_id in TARGET_AUCTION_IDS:
            print("=" * 80)
            print(f"Auction ID: {auction_id}")

            auction = auctions_by_id.get(auction_id)
            if auction is None:
                print("Auction not found in local SQLite database.")
                print()
                continue

            print(f"Source URL: {auction.official_url or '-'}")
            print()

            raw_trace = fetch_raw_trace(auction.official_url, session=session)
            parsed_detail = parse_detail_page(raw_trace["html"]) if raw_trace["html"] is not None else None
            record = build_auction_record(auction)

            print("Block A: Raw HTML detail values")
            print(f"  Valor subasta: {raw_trace['valor_subasta']}")
            print(f"  Tasacion: {raw_trace['tasacion']}")
            print(f"  Importe del deposito: {raw_trace['importe_del_deposito']}")
            print(f"  Lotes: {raw_trace['lotes']}")
            print()

            print("Block B: Parsed detail and persisted Auction")
            print(f"  parsed.starting_bid: {format_value(parsed_detail.starting_bid if parsed_detail else None)}")
            print(f"  parsed.appraisal_value: {format_value(parsed_detail.appraisal_value if parsed_detail else None)}")
            print(f"  parsed.deposit: {format_value(parsed_detail.deposit if parsed_detail else None)}")
            print(f"  persisted.starting_bid: {format_value(auction.starting_bid)}")
            print(f"  persisted.appraisal_value: {format_value(auction.appraisal_value)}")
            print(f"  persisted.deposit: {format_value(auction.deposit)}")
            print(f"  persisted.has_lots: None")
            print(f"  persisted.lot_count: None")
            print()

            print("Block C: AuctionRecord adaptation")
            print(f"  record.opening_bid: {format_value(record.opening_bid)}")
            print(f"  record.appraisal_value: {format_value(record.appraisal_value)}")
            print(f"  record.deposit: {format_value(record.deposit)}")
            print(f"  record.has_lots: {record.has_lots}")
            print(f"  record.lot_count: {record.lot_count}")
            print(f"  record.parser_warnings: {record.parser_warnings}")
            print()

            print("Diagnosis")
            for line in diagnose_opening_bid_loss(
                raw_opening_bid=raw_trace["valor_subasta"],
                parsed_starting_bid=parsed_detail.starting_bid if parsed_detail else None,
                persisted_starting_bid=auction.starting_bid,
                record_opening_bid=record.opening_bid,
            ):
                print(f"  - {line}")
            print()


def fetch_raw_trace(source_url: str | None, *, session: requests.Session) -> dict[str, str | None]:
    """Fetch a detail page and extract the raw row values we care about."""
    if not source_url:
        return {
            "html": None,
            "valor_subasta": None,
            "tasacion": None,
            "importe_del_deposito": None,
            "lotes": None,
        }

    try:
        response = session.get(source_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        return {
            "html": None,
            "valor_subasta": f"[fetch-error] {exc}",
            "tasacion": None,
            "importe_del_deposito": None,
            "lotes": None,
        }

    row_map = extract_detail_row_map(response.text)
    return {
        "html": response.text,
        "valor_subasta": row_map.get("valor_subasta"),
        "tasacion": row_map.get("tasacion"),
        "importe_del_deposito": row_map.get("importe_del_deposito"),
        "lotes": row_map.get("lotes"),
    }


def extract_detail_row_map(html: str) -> dict[str, str]:
    """Extract raw label/value pairs from the BOE detail table."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one(DETAIL_TABLE_SELECTOR)
    if not isinstance(table, Tag):
        return {}

    row_map: dict[str, str] = {}
    for row in table.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        header = row.find("th")
        value = row.find("td")
        if not isinstance(header, Tag) or not isinstance(value, Tag):
            continue
        key = slugify_asset_label(header.get_text(" ", strip=True))
        row_map[key] = value.get_text(" ", strip=True)
    return row_map


def diagnose_opening_bid_loss(
    *,
    raw_opening_bid: str | None,
    parsed_starting_bid,
    persisted_starting_bid,
    record_opening_bid,
) -> list[str]:
    """Explain where the opening bid appears to disappear."""
    messages: list[str] = []

    if raw_opening_bid is None:
        messages.append("Opening bid is missing in raw HTML.")
        return messages
    if isinstance(raw_opening_bid, str) and raw_opening_bid.startswith("[fetch-error]"):
        messages.append("Raw HTML could not be fetched, so the HTML layer could not be verified.")
        if persisted_starting_bid is None and record_opening_bid is None:
            messages.append("Opening bid is already missing in persisted Auction and AuctionRecord.")
        return messages

    lowered_raw = raw_opening_bid.casefold()
    if "cada lote" in lowered_raw or "ver valor" in lowered_raw or "adjudicaci" in lowered_raw:
        messages.append("Opening bid is not numeric in raw HTML because the auction is lot-scoped.")
    elif parsed_starting_bid is None:
        messages.append("Opening bid exists in raw HTML but is missing in parser output.")

    if parsed_starting_bid is not None and persisted_starting_bid is None:
        messages.append("Opening bid was parsed but is missing after persistence.")
    if persisted_starting_bid is not None and record_opening_bid is None:
        messages.append("Opening bid is present in Auction but missing after build_auction_record().")

    if not messages:
        messages.append("Opening bid mapping looks consistent across all inspected layers.")

    return messages


def format_value(value) -> str:
    """Format values without adding noise to the console output."""
    if value is None:
        return "None"
    return str(value)


if __name__ == "__main__":
    main()
