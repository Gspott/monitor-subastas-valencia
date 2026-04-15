"""Trace missing completed-auction fields across HTML, parser, SQLite, and dashboard."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.sources.boe import (
    DETAIL_TABLE_SELECTOR,
    _extract_detail_row_map,
    build_detail_view_url,
    build_lot_detail_url,
    parse_detail_bids_page,
    parse_detail_lot_general_page,
    parse_detail_lots_page,
    parse_detail_page,
)
from monitor.storage import fetch_all_completed_auctions
from scripts.load_sample_boe_data import merge_parsed_lot_data
from scripts.monitor_dashboard import build_completed_table_rows


REQUEST_TIMEOUT_SECONDS = 20
TRACE_LIMIT = 5
TRACE_AUCTION_IDS: list[str] = []
def main() -> None:
    """Trace a few completed auctions end to end."""
    auctions = fetch_all_completed_auctions()
    selected_auctions = select_target_auctions(auctions)

    if not selected_auctions:
        print("No completed auctions available for tracing.")
        return

    dashboard_rows_by_id = {
        row["auction_lot_id"]: row
        for row in build_completed_table_rows(selected_auctions)
    }

    with requests.Session() as session:
        for auction in selected_auctions:
            trace_completed_auction(
                auction=auction,
                dashboard_row=dashboard_rows_by_id.get(auction.external_id or "", {}),
                session=session,
            )


def select_target_auctions(auctions):
    """Pick either configured IDs or the first few completed rows."""
    if TRACE_AUCTION_IDS:
        auctions_by_id = {auction.external_id: auction for auction in auctions}
        return [
            auctions_by_id[auction_id]
            for auction_id in TRACE_AUCTION_IDS
            if auction_id in auctions_by_id
        ]
    return auctions[:TRACE_LIMIT]


def trace_completed_auction(*, auction, dashboard_row: dict[str, object], session: requests.Session) -> None:
    """Trace one completed auction row across the main project layers."""
    html_pages = fetch_completed_html_pages(auction=auction, session=session)
    html_snapshot = build_html_snapshot(auction=auction, html_pages=html_pages)
    parser_snapshot = build_parser_snapshot(auction=auction, html_pages=html_pages)
    persistence_snapshot = build_persistence_snapshot(auction)
    dashboard_snapshot = build_dashboard_snapshot(dashboard_row)

    print("=" * 80)
    print(f"auction_id={auction.external_id or '-'}")
    print(f"source_url={auction.official_url or '-'}")
    print("")
    print("Block A: HTML")
    print_snapshot(html_snapshot)
    print("")
    print("Block B: Parser")
    print_snapshot(parser_snapshot)
    print("")
    print("Block C: SQLite")
    print_snapshot(persistence_snapshot)
    print("")
    print("Block D: Dashboard")
    print_snapshot(dashboard_snapshot)
    print("")
    print("Diagnosis")
    for field_name in (
        "opening_date",
        "closing_date",
        "starting_bid",
        "appraisal_value",
        "current_bid",
        "official_status",
        "postal_code",
    ):
        diagnosis = diagnose_field(
            html_value=html_snapshot.get(field_name),
            parser_value=parser_snapshot.get(field_name),
            persisted_value=persistence_snapshot.get(field_name),
            dashboard_value=dashboard_snapshot.get(field_name),
        )
        print(f"- {field_name}: {diagnosis}")
    print(
        "- final_bid_ratio_vs_appraisal: "
        f"{diagnose_derived_ratio(dashboard_snapshot.get('final_bid_ratio_vs_appraisal'))}"
    )
    print(
        "- final_bid_ratio_vs_starting_bid: "
        f"{diagnose_derived_ratio(dashboard_snapshot.get('final_bid_ratio_vs_starting_bid'))}"
    )
    print("")


def fetch_completed_html_pages(*, auction, session: requests.Session) -> dict[str, str | None]:
    """Fetch the most relevant HTML variants for a completed row."""
    if not auction.official_url:
        return {
            "base": None,
            "lot_general": None,
            "lot_asset": None,
            "base_bids": None,
            "lot_bids": None,
        }

    base_url = auction.official_url.split("&ver=", maxsplit=1)[0]
    lot_number = extract_lot_number(auction.external_id)
    lot_general_url = build_lot_detail_url(base_url, lot_number).replace("ver=3", "ver=1") if lot_number else None
    lot_asset_url = build_lot_detail_url(base_url, lot_number) if lot_number else None
    base_bids_url = build_detail_view_url(base_url, view=5)
    lot_bids_url = build_detail_view_url(base_url, view=5, lot_number=lot_number) if lot_number else None

    return {
        "base": fetch_html(base_url, session=session),
        "lot_general": fetch_html(lot_general_url, session=session) if lot_general_url else None,
        "lot_asset": fetch_html(lot_asset_url, session=session) if lot_asset_url else None,
        "base_bids": fetch_html(base_bids_url, session=session),
        "lot_bids": fetch_html(lot_bids_url, session=session) if lot_bids_url else None,
    }


def fetch_html(url: str | None, *, session: requests.Session) -> str | None:
    """Fetch one HTML page safely for trace purposes."""
    if not url:
        return None

    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return None
    return response.text


def build_html_snapshot(*, auction, html_pages: dict[str, str | None]) -> dict[str, str]:
    """Read raw values directly from the most relevant BOE HTML pages."""
    base_row_map = extract_detail_row_map_from_html(html_pages["base"])
    lot_general_row_map = extract_detail_row_map_from_html(html_pages["lot_general"])
    lot_asset_row_map = extract_labeled_row_map_from_any_table(html_pages["lot_asset"])

    return {
        "opening_date": first_non_empty(
            lot_general_row_map.get("fecha_de_inicio"),
            base_row_map.get("fecha_de_inicio"),
        ),
        "closing_date": first_non_empty(
            lot_general_row_map.get("fecha_de_conclusion"),
            base_row_map.get("fecha_de_conclusion"),
        ),
        "starting_bid": first_non_empty(
            lot_general_row_map.get("valor_subasta"),
            base_row_map.get("valor_subasta"),
            lot_general_row_map.get("puja_minima"),
            base_row_map.get("puja_minima"),
        ),
        "appraisal_value": first_non_empty(
            lot_general_row_map.get("tasacion"),
            lot_general_row_map.get("valor_de_tasacion"),
            base_row_map.get("tasacion"),
            base_row_map.get("valor_de_tasacion"),
        ),
        "current_bid": first_non_empty(
            extract_public_bid_from_html(html_pages["lot_bids"]),
            extract_public_bid_from_html(html_pages["base_bids"]),
        ),
        "official_status": extract_status_text_from_html(html_pages["base"]),
        "postal_code": first_non_empty(
            lot_asset_row_map.get("codigo_postal"),
            lot_general_row_map.get("codigo_postal"),
            base_row_map.get("codigo_postal"),
        ),
    }


def build_parser_snapshot(*, auction, html_pages: dict[str, str | None]) -> dict[str, str]:
    """Build a parser-level snapshot using the same parser functions as the loaders."""
    parent_detail = parse_detail_page(html_pages["base"]) if html_pages["base"] else None
    lot_number = extract_lot_number(auction.external_id)
    parsed_general_lot = (
        parse_detail_lot_general_page(html_pages["lot_general"], lot_number=lot_number)
        if html_pages["lot_general"] and lot_number is not None
        else None
    )
    parsed_asset_lot = None
    if html_pages["lot_asset"] and lot_number is not None:
        parsed_lots = parse_detail_lots_page(html_pages["lot_asset"])
        parsed_asset_lot = next(
            (candidate for candidate in parsed_lots if candidate.lot_number == lot_number),
            None,
        )
    merged_lot = merge_parsed_lot_data(parsed_asset_lot, parsed_general_lot)
    parsed_current_bid = first_non_empty(
        safe_str(parse_detail_bids_page(html_pages["lot_bids"])) if html_pages["lot_bids"] else None,
        safe_str(parse_detail_bids_page(html_pages["base_bids"])) if html_pages["base_bids"] else None,
    )

    return {
        "opening_date": safe_str(parent_detail.opening_date if parent_detail else None),
        "closing_date": safe_str(parent_detail.closing_date if parent_detail else None),
        "starting_bid": safe_str(
            merged_lot.starting_bid if merged_lot is not None else (parent_detail.starting_bid if parent_detail else None)
        ),
        "appraisal_value": safe_str(
            merged_lot.appraisal_value if merged_lot is not None else (parent_detail.appraisal_value if parent_detail else None)
        ),
        "current_bid": parsed_current_bid,
        "official_status": safe_str(auction.official_status),
        "postal_code": safe_str(
            merged_lot.postal_code if merged_lot is not None else None
        ),
    }


def build_persistence_snapshot(auction) -> dict[str, str]:
    """Read the values as they are currently stored in SQLite."""
    return {
        "opening_date": safe_str(auction.opening_date),
        "closing_date": safe_str(auction.closing_date),
        "starting_bid": safe_str(auction.starting_bid),
        "appraisal_value": safe_str(auction.appraisal_value),
        "current_bid": safe_str(auction.current_bid),
        "official_status": safe_str(auction.official_status),
        "postal_code": safe_str(auction.postal_code),
    }


def build_dashboard_snapshot(row: dict[str, object]) -> dict[str, str]:
    """Read the values exactly as the completed dashboard row sees them."""
    return {
        "opening_date": safe_str(row.get("opening_date")),
        "closing_date": safe_str(row.get("closing_date")),
        "starting_bid": safe_str(row.get("opening_bid")),
        "appraisal_value": safe_str(row.get("appraisal_value")),
        "current_bid": safe_str(row.get("current_bid")),
        "official_status": safe_str(row.get("official_status")),
        "final_bid_ratio_vs_appraisal": safe_str(row.get("final_bid_ratio_vs_appraisal")),
        "final_bid_ratio_vs_starting_bid": safe_str(row.get("final_bid_ratio_vs_starting_bid")),
        "postal_code": safe_str(row.get("postal_code")),
    }


def print_snapshot(snapshot: dict[str, str]) -> None:
    """Print one compact field snapshot."""
    for key, value in snapshot.items():
        print(f"- {key}: {value or '-'}")


def diagnose_field(
    *,
    html_value: str | None,
    parser_value: str | None,
    persisted_value: str | None,
    dashboard_value: str | None,
) -> str:
    """Locate the first layer where one field becomes unavailable."""
    if not html_value:
        return "missing in html"
    if not parser_value:
        return "missing in parser"
    if not persisted_value:
        return "missing after persistence"
    if dashboard_value in {None, "", "-"}:
        return "missing in dashboard mapping"
    return "available through dashboard"


def diagnose_derived_ratio(value: str | None) -> str:
    """Explain why a derived dashboard ratio is blank or visible."""
    if value in {None, "", "-"}:
        return "missing in dashboard mapping or missing numeric inputs"
    return "available through dashboard"


def extract_detail_row_map_from_html(html: str | None) -> dict[str, str]:
    """Extract the main validated BOE detail table when present."""
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one(DETAIL_TABLE_SELECTOR)
    if not isinstance(table, Tag):
        return {}
    return _extract_detail_row_map(table)


def extract_labeled_row_map_from_any_table(html: str | None) -> dict[str, str]:
    """Extract label/value pairs from any table in the current HTML page."""
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    row_map: dict[str, str] = {}
    for table in soup.select("table"):
        if not isinstance(table, Tag):
            continue
        row_map.update(_extract_detail_row_map(table))
    return row_map


def extract_public_bid_from_html(html: str | None) -> str | None:
    """Extract the public maximum bid text from the BOE bids tab when present."""
    if not html:
        return None

    parsed_current_bid = parse_detail_bids_page(html)
    return safe_str(parsed_current_bid)


def extract_status_text_from_html(html: str | None) -> str | None:
    """Extract a best-effort status text from the page body."""
    if not html:
        return None

    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    match = re.search(r"Estado:\s*(.+?)(?:\s{2,}|Fecha de inicio:|Fecha de conclusion:)", text)
    if match is None:
        return None
    return match.group(1).strip()


def extract_lot_number(auction_id: str | None) -> int | None:
    """Infer the lot number from the synthetic auction identifier."""
    if auction_id is None or "::lot:" not in auction_id:
        return None

    lot_fragment = auction_id.split("::lot:", maxsplit=1)[1]
    if not lot_fragment.isdigit():
        return None
    return int(lot_fragment)


def first_non_empty(*values: str | None) -> str | None:
    """Return the first non-empty string value."""
    for value in values:
        if value:
            return value
    return None


def safe_str(value) -> str | None:
    """Format values conservatively for trace printing."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return format(value, "f") if hasattr(value, "as_tuple") else str(value)


if __name__ == "__main__":
    main()
