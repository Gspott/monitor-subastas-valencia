"""Load BOE completed auctions into a dedicated SQLite table."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.normalize import normalize_auction
from monitor.sources.boe import (
    REQUEST_TIMEOUT_SECONDS,
    build_detail_view_url,
    parse_iso_date,
    parse_detail_bids_page,
    parse_detail_bids_table_page,
    parse_detail_page,
    parse_listing_page,
)
from monitor.storage import init_db, upsert_completed_auction
from scripts.load_sample_boe_data import (
    REQUEST_DELAY_SECONDS,
    SearchConfig,
    apply_detail_to_auction,
    build_empty_search_report,
    build_lot_auctions,
    dedupe_listing_entries,
    extract_next_listing_page_url,
    map_listing_entries_to_auctions,
    propagate_parent_postal_codes_in_entries,
)

MAX_LISTING_PAGES = 8
MAX_LISTING_ITEMS = 500
INCREMENTAL_WINDOW_DAYS = 21
INCREMENTAL_MAX_LISTING_PAGES = 3

VALENCIA_COMPLETED_PORTAL_REAL_ESTATE_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=PC"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_COMPLETED_MANAGER_REAL_ESTATE_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=FS"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_COMPLETED_ALL_ASSETS_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=PC"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_COMPLETED_MANAGER_ALL_ASSETS_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=FS"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_COMPLETED_PORTAL_PARKING_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=PC"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B4%5D=BIEN.SUBTIPO"
    "&dato%5B4%5D=GA"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_COMPLETED_MANAGER_PARKING_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=FS"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B4%5D=BIEN.SUBTIPO"
    "&dato%5B4%5D=GA"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
COMPLETED_SEARCH_CONFIGS = [
    SearchConfig(
        name="valencia_completed_portal_real_estate",
        url=VALENCIA_COMPLETED_PORTAL_REAL_ESTATE_URL,
    ),
    SearchConfig(
        name="valencia_completed_manager_real_estate",
        url=VALENCIA_COMPLETED_MANAGER_REAL_ESTATE_URL,
    ),
    SearchConfig(
        name="valencia_completed_portal_all_assets",
        url=VALENCIA_COMPLETED_ALL_ASSETS_URL,
    ),
    SearchConfig(
        name="valencia_completed_manager_all_assets",
        url=VALENCIA_COMPLETED_MANAGER_ALL_ASSETS_URL,
    ),
    SearchConfig(
        name="valencia_completed_portal_parking",
        url=VALENCIA_COMPLETED_PORTAL_PARKING_URL,
    ),
    SearchConfig(
        name="valencia_completed_manager_parking",
        url=VALENCIA_COMPLETED_MANAGER_PARKING_URL,
    ),
]


@dataclass(frozen=True)
class CompletedRefreshConfig:
    """Explicit settings for incremental and full completed refreshes."""

    full_refresh: bool
    window_days: int
    max_listing_pages: int


def build_completed_refresh_config(
    *,
    full_refresh: bool,
    window_days: int = INCREMENTAL_WINDOW_DAYS,
    max_listing_pages: int | None = None,
) -> CompletedRefreshConfig:
    """Build a small immutable config for the selected refresh mode."""
    if max_listing_pages is None:
        max_listing_pages = MAX_LISTING_PAGES if full_refresh else INCREMENTAL_MAX_LISTING_PAGES

    return CompletedRefreshConfig(
        full_refresh=full_refresh,
        window_days=window_days,
        max_listing_pages=max_listing_pages,
    )


def main(
    *,
    full_refresh: bool = False,
    window_days: int = INCREMENTAL_WINDOW_DAYS,
    max_listing_pages: int | None = None,
) -> None:
    """Load completed BOE auctions into the dedicated SQLite table."""
    refresh_config = build_completed_refresh_config(
        full_refresh=full_refresh,
        window_days=window_days,
        max_listing_pages=max_listing_pages,
    )
    init_db()
    per_search_report = {
        config.name: build_completed_search_report()
        for config in COMPLETED_SEARCH_CONFIGS
    }
    load_report = {
        "searches_processed": len(COMPLETED_SEARCH_CONFIGS),
        "listing_pages_processed": 0,
        "listing_items_raw": 0,
        "listing_items_unique": 0,
        "detail_auctions_expanded": 0,
        "lot_auctions_generated": 0,
        "auctions_saved": 0,
    }
    processing_date = date.today()

    with requests.Session() as session:
        print(f"Processing {len(COMPLETED_SEARCH_CONFIGS)} BOE completed searches.")
        if refresh_config.full_refresh:
            print(
                "[INFO] Completed refresh mode: full history "
                f"(max_pages={refresh_config.max_listing_pages})."
            )
        else:
            print(
                "[INFO] Completed refresh mode: incremental "
                f"(window_days={refresh_config.window_days}, max_pages={refresh_config.max_listing_pages})."
            )
        listing_pages = fetch_completed_listing_pages_with_pagination(
            COMPLETED_SEARCH_CONFIGS,
            session=session,
            refresh_config=refresh_config,
            processing_date=processing_date,
        )
        load_report["listing_pages_processed"] = len(listing_pages)

        listing_entries = []
        for page in listing_pages:
            page_items = parse_listing_page(page["html"])
            listing_entries.extend(
                {
                    "item": item,
                    "search_name": page["search_name"],
                }
                for item in page_items
            )
            load_report["listing_items_raw"] += len(page_items)
            per_search_report[page["search_name"]]["raw_items_found"] += len(page_items)

        unique_listing_entries = dedupe_listing_entries(listing_entries)
        if not refresh_config.full_refresh:
            unique_listing_entries = filter_incremental_completed_listing_entries(
                unique_listing_entries,
                processing_date=processing_date,
                window_days=refresh_config.window_days,
            )
        unique_listing_entries = unique_listing_entries[:MAX_LISTING_ITEMS]
        load_report["listing_items_unique"] = len(unique_listing_entries)
        for entry in unique_listing_entries:
            per_search_report[entry["search_name"]]["unique_items_contributed_after_dedupe"] += 1

        auction_entries = map_listing_entries_to_auctions(unique_listing_entries)
        expanded_auction_entries = []
        for index, auction_entry in enumerate(auction_entries, start=1):
            auction = auction_entry["auction"]
            search_name = auction_entry["search_name"]
            print(
                f"Processing completed detail {index}/{len(auction_entries)}: "
                f"{auction.external_id or auction.title}"
            )
            expanded_batch = expand_completed_auction_from_detail(auction, session=session)
            load_report["detail_auctions_expanded"] += len(expanded_batch)
            per_search_report[search_name]["detail_auctions_expanded"] += len(expanded_batch)
            generated_lots = sum(
                1
                for item in expanded_batch
                if item.external_id is not None and "::lot:" in item.external_id
            )
            rows_with_current_bid = sum(
                1
                for item in expanded_batch
                if item.current_bid is not None and item.current_bid > 0
            )
            load_report["lot_auctions_generated"] += generated_lots
            per_search_report[search_name]["lot_auctions_generated"] += generated_lots
            per_search_report[search_name]["rows_with_current_bid"] += rows_with_current_bid
            expanded_auction_entries.extend(
                {
                    "auction": item,
                    "search_name": search_name,
                }
                for item in expanded_batch
            )

    stored_entries = [
        {
            "auction": normalize_auction(entry["auction"]),
            "search_name": entry["search_name"],
        }
        for entry in expanded_auction_entries
    ]
    stored_entries = propagate_parent_postal_codes_in_entries(stored_entries)
    for entry in stored_entries:
        upsert_completed_auction(entry["auction"])
        per_search_report[entry["search_name"]]["saved_to_sqlite"] += 1

    load_report["auctions_saved"] = len(stored_entries)

    print("BOE completed data load completed.")
    print(f"Searches processed: {load_report['searches_processed']}")
    print(f"Listing pages processed: {load_report['listing_pages_processed']}")
    print(f"Listing items raw: {load_report['listing_items_raw']}")
    print(f"Listing items unique: {load_report['listing_items_unique']}")
    print(f"Detail auctions expanded: {load_report['detail_auctions_expanded']}")
    print(f"Lot auctions generated: {load_report['lot_auctions_generated']}")
    print(f"Completed auctions saved to SQLite: {load_report['auctions_saved']}")
    print("Per-search contribution:")
    for search_name, report in per_search_report.items():
        print(
            f"  - {search_name}: raw_items_found={report['raw_items_found']}, "
            f"unique_items_contributed_after_dedupe={report['unique_items_contributed_after_dedupe']}, "
            f"detail_auctions_expanded={report['detail_auctions_expanded']}, "
            f"lot_auctions_generated={report['lot_auctions_generated']}, "
            f"rows_with_current_bid={report['rows_with_current_bid']}, "
            f"saved_to_sqlite={report['saved_to_sqlite']}"
        )


def expand_completed_auction_from_detail(auction, *, session: requests.Session):
    """Expand one completed listing into one enriched row or several enriched lots."""
    if not auction.official_url:
        return [auction]

    time.sleep(REQUEST_DELAY_SECONDS)
    try:
        response = session.get(auction.official_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return [auction]

    detail = parse_detail_page(response.text)
    detail_enriched_auction = apply_detail_to_auction(auction, detail)
    lot_auctions = build_lot_auctions(detail_enriched_auction, session=session)

    if lot_auctions:
        return [
            enrich_completed_auction_with_current_bid(lot_auction, session=session)
            for lot_auction in lot_auctions
        ]

    if detail is None:
        return [enrich_completed_auction_with_current_bid(auction, session=session)]

    return [enrich_completed_auction_with_current_bid(detail_enriched_auction, session=session)]


def enrich_completed_auction_with_current_bid(auction, *, session: requests.Session):
    """Read the BOE bids tab and attach the public maximum bid when available."""
    current_bid = fetch_completed_current_bid(
        official_url=auction.official_url,
        lot_number=extract_lot_number(auction.external_id),
        session=session,
    )
    if current_bid is None:
        return auction
    return auction.model_copy(update={"current_bid": current_bid})


def fetch_completed_current_bid(
    *,
    official_url: str | None,
    lot_number: int | None,
    session: requests.Session,
):
    """Fetch the BOE `ver=5` view and extract the public final bid signal."""
    if not official_url:
        return None

    if lot_number is not None:
        lot_bids_url = build_detail_view_url(official_url, view=5, lot_number=lot_number)
        time.sleep(REQUEST_DELAY_SECONDS)
        try:
            response = session.get(lot_bids_url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            lot_current_bid = parse_detail_bids_page(response.text, lot_number=lot_number)
            if lot_current_bid is not None:
                return lot_current_bid
        except requests.RequestException:
            pass

    bids_url = build_detail_view_url(official_url, view=5)
    time.sleep(REQUEST_DELAY_SECONDS)
    try:
        response = session.get(bids_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return None

    if lot_number is not None:
        lot_bid_map = parse_detail_bids_table_page(response.text)
        if lot_number in lot_bid_map:
            return lot_bid_map[lot_number]

    return parse_detail_bids_page(response.text)


def extract_lot_number(external_id: str | None) -> int | None:
    """Infer the lot number from the synthetic lot external ID."""
    if external_id is None or "::lot:" not in external_id:
        return None

    fragment = external_id.split("::lot:", maxsplit=1)[1]
    if not fragment.isdigit():
        return None
    return int(fragment)


def filter_incremental_completed_listing_entries(
    listing_entries: list[dict[str, object]],
    *,
    processing_date: date,
    window_days: int,
) -> list[dict[str, object]]:
    """Keep only recent completed listings while preserving rows without reliable dates."""
    earliest_allowed_date = processing_date - timedelta(days=window_days)
    filtered_entries: list[dict[str, object]] = []

    for entry in listing_entries:
        item = entry["item"]
        closing_date = parse_iso_date(getattr(item, "closing_date", None))
        if closing_date is None or closing_date >= earliest_allowed_date:
            filtered_entries.append(entry)

    return filtered_entries


def should_early_stop_completed_listing_page(
    page_items: list[object],
    *,
    processing_date: date,
    window_days: int,
    full_refresh: bool,
) -> bool:
    """Stop only when one page is clearly older than the incremental overlap window."""
    if full_refresh or not page_items:
        return False

    earliest_allowed_date = processing_date - timedelta(days=window_days)
    parsed_closing_dates: list[date] = []

    for item in page_items:
        closing_date = parse_iso_date(getattr(item, "closing_date", None))
        if closing_date is None:
            return False
        parsed_closing_dates.append(closing_date)

    return all(closing_date < earliest_allowed_date for closing_date in parsed_closing_dates)


def fetch_completed_listing_pages_with_pagination(
    search_configs: list[SearchConfig],
    *,
    session: requests.Session,
    refresh_config: CompletedRefreshConfig,
    processing_date: date,
) -> list[dict[str, str]]:
    """Fetch completed listing pages and stop early when a full page is safely stale."""
    fetched_pages: list[dict[str, str]] = []

    for search_index, search_config in enumerate(search_configs, start=1):
        print(f"Fetching search {search_index}/{len(search_configs)}: {search_config.name}")
        current_url = search_config.url
        visited_urls: set[str] = set()

        for page_number in range(1, refresh_config.max_listing_pages + 1):
            if current_url in visited_urls:
                break
            visited_urls.add(current_url)

            time.sleep(REQUEST_DELAY_SECONDS)
            response = session.get(current_url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            fetched_pages.append(
                {
                    "search_name": search_config.name,
                    "search_url": search_config.url,
                    "page_url": current_url,
                    "page_number": str(page_number),
                    "html": response.text,
                }
            )
            print(f"  Listing page {page_number}: {current_url}")

            page_items = parse_listing_page(response.text)
            if should_early_stop_completed_listing_page(
                page_items,
                processing_date=processing_date,
                window_days=refresh_config.window_days,
                full_refresh=refresh_config.full_refresh,
            ):
                print(
                    "  [INFO] Early stop triggered for search "
                    f"{search_config.name}: page {page_number} is fully older than the incremental window."
                )
                break

            next_page_url = extract_next_listing_page_url(response.text, current_url)
            if next_page_url is None:
                break
            current_url = next_page_url

    return fetched_pages


def build_completed_search_report() -> dict[str, int]:
    """Build an empty metrics container for one completed search."""
    report = build_empty_search_report()
    report["rows_with_current_bid"] = 0
    return report


def parse_args() -> argparse.Namespace:
    """Parse CLI options for incremental vs full completed refresh."""
    parser = argparse.ArgumentParser(
        description="Load BOE completed auctions into SQLite.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Fetch the wider completed history instead of the recent incremental window.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=INCREMENTAL_WINDOW_DAYS,
        help=(
            "Incremental completed overlap window in days. "
            f"Default: {INCREMENTAL_WINDOW_DAYS}."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Override the number of completed listing pages fetched per search.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        full_refresh=args.full_refresh,
        window_days=args.window_days,
        max_listing_pages=args.max_pages,
    )
