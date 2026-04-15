"""Load BOE upcoming-opening auctions into a dedicated SQLite table."""

from __future__ import annotations

import sys
from pathlib import Path

import requests


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.normalize import normalize_auction
from monitor.storage import init_db, upsert_upcoming_auction
from scripts.load_sample_boe_data import (
    MAX_LISTING_PAGES,
    MAX_LISTING_ITEMS,
    REQUEST_DELAY_SECONDS,
    SearchConfig,
    build_empty_search_report,
    dedupe_listing_entries,
    expand_auction_from_detail,
    fetch_listing_pages_with_pagination,
    map_listing_entries_to_auctions,
    propagate_parent_postal_codes_in_entries,
)
from monitor.sources.boe import parse_listing_page


VALENCIA_UPCOMING_REAL_ESTATE_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=PU"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_UPCOMING_ALL_ASSETS_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=PU"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
UPCOMING_SEARCH_CONFIGS = [
    SearchConfig(name="valencia_upcoming_real_estate", url=VALENCIA_UPCOMING_REAL_ESTATE_URL),
    SearchConfig(name="valencia_upcoming_all_assets", url=VALENCIA_UPCOMING_ALL_ASSETS_URL),
]


def main() -> None:
    """Load upcoming-opening BOE auctions into the dedicated SQLite table."""
    init_db()
    per_search_report = {
        config.name: build_empty_search_report()
        for config in UPCOMING_SEARCH_CONFIGS
    }
    load_report = {
        "searches_processed": len(UPCOMING_SEARCH_CONFIGS),
        "listing_pages_processed": 0,
        "listing_items_raw": 0,
        "listing_items_unique": 0,
        "detail_auctions_expanded": 0,
        "lot_auctions_generated": 0,
        "auctions_saved": 0,
    }

    with requests.Session() as session:
        print(f"Processing {len(UPCOMING_SEARCH_CONFIGS)} BOE upcoming searches.")
        listing_pages = fetch_listing_pages_with_pagination(
            UPCOMING_SEARCH_CONFIGS,
            session=session,
            max_pages=MAX_LISTING_PAGES,
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

        unique_listing_entries = dedupe_listing_entries(listing_entries)[:MAX_LISTING_ITEMS]
        load_report["listing_items_unique"] = len(unique_listing_entries)
        for entry in unique_listing_entries:
            per_search_report[entry["search_name"]]["unique_items_contributed_after_dedupe"] += 1

        auction_entries = map_listing_entries_to_auctions(unique_listing_entries)
        expanded_auction_entries = []
        for index, auction_entry in enumerate(auction_entries, start=1):
            auction = auction_entry["auction"]
            search_name = auction_entry["search_name"]
            print(
                f"Processing upcoming detail {index}/{len(auction_entries)}: "
                f"{auction.external_id or auction.title}"
            )
            expanded_batch = expand_auction_from_detail(auction, session=session)
            load_report["detail_auctions_expanded"] += len(expanded_batch)
            per_search_report[search_name]["detail_auctions_expanded"] += len(expanded_batch)
            generated_lots = sum(
                1
                for item in expanded_batch
                if item.external_id is not None and "::lot:" in item.external_id
            )
            load_report["lot_auctions_generated"] += generated_lots
            per_search_report[search_name]["lot_auctions_generated"] += generated_lots
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
        upsert_upcoming_auction(entry["auction"])
        per_search_report[entry["search_name"]]["saved_to_sqlite"] += 1

    load_report["auctions_saved"] = len(stored_entries)

    print("BOE upcoming data load completed.")
    print(f"Searches processed: {load_report['searches_processed']}")
    print(f"Listing pages processed: {load_report['listing_pages_processed']}")
    print(f"Listing items raw: {load_report['listing_items_raw']}")
    print(f"Listing items unique: {load_report['listing_items_unique']}")
    print(f"Detail auctions expanded: {load_report['detail_auctions_expanded']}")
    print(f"Lot auctions generated: {load_report['lot_auctions_generated']}")
    print(f"Upcoming auctions saved to SQLite: {load_report['auctions_saved']}")
    print("Per-search contribution:")
    for search_name, report in per_search_report.items():
        print(
            f"  - {search_name}: raw_items_found={report['raw_items_found']}, "
            f"unique_items_contributed_after_dedupe={report['unique_items_contributed_after_dedupe']}, "
            f"detail_auctions_expanded={report['detail_auctions_expanded']}, "
            f"lot_auctions_generated={report['lot_auctions_generated']}, "
            f"saved_to_sqlite={report['saved_to_sqlite']}"
        )


if __name__ == "__main__":
    main()
