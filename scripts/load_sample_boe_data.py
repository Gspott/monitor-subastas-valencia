"""Load a small real BOE sample into the local SQLite database."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.models import Auction
from monitor.normalize import normalize_auction, normalize_postal_code
from monitor.sources.boe import (
    REQUEST_TIMEOUT_SECONDS,
    build_lot_detail_url,
    fetch_listing_pages,
    map_parsed_items_to_auctions,
    parse_detail_lot_general_page,
    parse_detail_page,
    parse_detail_lot_numbers_page,
    parse_detail_lots_page,
    parse_iso_date,
    parse_listing_page,
)
from monitor.storage import init_db, upsert_auction


VALENCIA_REAL_ESTATE_LISTING_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=EJ"
    "&campo%5B3%5D=BIEN.TIPO"
    "&dato%5B3%5D=I"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&campo%5B18%5D=SUBASTA.FECHA_INICIO"
    "&dato%5B18%5D%5B0%5D="
    "&dato%5B18%5D%5B1%5D="
    "&page_hits=40"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
VALENCIA_ALL_ASSETS_LISTING_URL = (
    "https://subastas.boe.es/subastas_ava.php"
    "?campo%5B2%5D=SUBASTA.ESTADO.CODIGO"
    "&dato%5B2%5D=EJ"
    "&campo%5B8%5D=BIEN.COD_PROVINCIA"
    "&dato%5B8%5D=46"
    "&page_hits=50"
    "&sort_field%5B0%5D=SUBASTA.FECHA_FIN"
    "&sort_order%5B0%5D=desc"
    "&accion=Buscar"
)
MAX_LISTING_PAGES = 5
MAX_LISTING_ITEMS = 300
REQUEST_DELAY_SECONDS = 0.5
USE_SAMPLE_SELECTION = False
TARGET_SAMPLE_COUNT = 120


@dataclass(frozen=True)
class SearchConfig:
    """Local configuration for one BOE search."""

    name: str
    url: str


SEARCH_CONFIGS = [
    SearchConfig(name="valencia_real_estate", url=VALENCIA_REAL_ESTATE_LISTING_URL),
    SearchConfig(name="valencia_all_assets", url=VALENCIA_ALL_ASSETS_LISTING_URL),
]


def main() -> None:
    """Load a wider BOE sample into the local SQLite database."""
    init_db()
    per_search_report = {
        config.name: build_empty_search_report()
        for config in SEARCH_CONFIGS
    }
    load_report = {
        "searches_processed": len(SEARCH_CONFIGS),
        "listing_pages_processed": 0,
        "listing_items_raw": 0,
        "listing_items_unique": 0,
        "detail_auctions_expanded": 0,
        "lot_auctions_generated": 0,
        "auctions_saved": 0,
        "per_search": per_search_report,
    }

    with requests.Session() as session:
        print(f"Processing {len(SEARCH_CONFIGS)} BOE searches.")
        listing_pages = fetch_listing_pages_with_pagination(
            SEARCH_CONFIGS,
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
        expanded_auction_entries: list[dict[str, object]] = []
        for index, auction_entry in enumerate(auction_entries, start=1):
            auction = auction_entry["auction"]
            search_name = auction_entry["search_name"]
            print(
                f"Processing detail {index}/{len(auction_entries)}: "
                f"{auction.external_id or auction.title}"
            )
            expanded_auction_batch = expand_auction_from_detail(auction, session=session)
            load_report["detail_auctions_expanded"] += len(expanded_auction_batch)
            per_search_report[search_name]["detail_auctions_expanded"] += len(expanded_auction_batch)
            load_report["lot_auctions_generated"] += sum(
                1
                for item in expanded_auction_batch
                if item.external_id is not None and "::lot:" in item.external_id
            )
            per_search_report[search_name]["lot_auctions_generated"] += sum(
                1
                for item in expanded_auction_batch
                if item.external_id is not None and "::lot:" in item.external_id
            )
            expanded_auction_entries.extend(
                {
                    "auction": item,
                    "search_name": search_name,
                }
                for item in expanded_auction_batch
            )

        selected_auction_entries = (
            select_sample_auction_entries(expanded_auction_entries, target_count=TARGET_SAMPLE_COUNT)
            if USE_SAMPLE_SELECTION
            else expanded_auction_entries
        )

    stored_auction_entries = [
        {
            "auction": normalize_auction(entry["auction"]),
            "search_name": entry["search_name"],
        }
        for entry in selected_auction_entries
    ]
    stored_auction_entries = propagate_parent_postal_codes_in_entries(stored_auction_entries)
    for entry in stored_auction_entries:
        auction = entry["auction"]
        upsert_auction(auction)
        per_search_report[entry["search_name"]]["saved_to_sqlite"] += 1
    load_report["auctions_saved"] = len(stored_auction_entries)

    print("BOE data load completed.")
    print(f"Searches processed: {load_report['searches_processed']}")
    print(f"Listing pages processed: {load_report['listing_pages_processed']}")
    print(f"Listing items raw: {load_report['listing_items_raw']}")
    print(f"Listing items unique: {load_report['listing_items_unique']}")
    print(f"Detail auctions expanded: {load_report['detail_auctions_expanded']}")
    print(f"Lot auctions generated: {load_report['lot_auctions_generated']}")
    print(f"Auctions saved to SQLite: {load_report['auctions_saved']}")
    print("Per-search contribution:")
    for search_name, report in per_search_report.items():
        print(
            f"  - {search_name}: raw_items_found={report['raw_items_found']}, "
            f"unique_items_contributed_after_dedupe={report['unique_items_contributed_after_dedupe']}, "
            f"detail_auctions_expanded={report['detail_auctions_expanded']}, "
            f"lot_auctions_generated={report['lot_auctions_generated']}, "
            f"saved_to_sqlite={report['saved_to_sqlite']}"
        )
    print("Saved auctions:")
    for entry in stored_auction_entries:
        auction = entry["auction"]
        print(f"  - {auction.external_id or '-'} | {auction.title}")


def expand_auction_from_detail(auction: Auction, *, session: requests.Session) -> list[Auction]:
    """Expand one listing auction into either one enriched auction or several lot auctions."""
    if not auction.official_url:
        return [auction]

    time.sleep(REQUEST_DELAY_SECONDS)
    try:
        response = session.get(auction.official_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        # Mantener el registro del listado si el detalle falla.
        return [auction]

    detail = parse_detail_page(response.text)
    detail_enriched_auction = apply_detail_to_auction(auction, detail)
    lot_auctions = build_lot_auctions(detail_enriched_auction, session=session)
    if lot_auctions:
        return lot_auctions
    if detail is None:
        return [auction]

    return [detail_enriched_auction]


def apply_detail_to_auction(auction: Auction, detail) -> Auction:
    """Apply parent-detail fields before deciding whether to persist parent or lots."""
    if detail is None:
        return auction

    return auction.model_copy(
        update={
            "title": detail.title or auction.title,
            "opening_date": parse_iso_date(detail.opening_date) or auction.opening_date,
            "closing_date": parse_iso_date(detail.closing_date) or auction.closing_date,
            "appraisal_value": detail.appraisal_value if detail.appraisal_value is not None else auction.appraisal_value,
            "starting_bid": detail.starting_bid if detail.starting_bid is not None else auction.starting_bid,
            "current_bid": detail.current_bid if detail.current_bid is not None else auction.current_bid,
            "deposit": detail.deposit if detail.deposit is not None else auction.deposit,
            "description": detail.description if detail.description is not None else auction.description,
            "occupancy_status": detail.occupancy_status if detail.occupancy_status is not None else auction.occupancy_status,
            "encumbrances_summary": (
                detail.encumbrances_summary
                if detail.encumbrances_summary is not None
                else auction.encumbrances_summary
            ),
        }
    )


def build_lot_auctions(auction: Auction, *, session: requests.Session) -> list[Auction]:
    """Build synthetic auction units from the BOE lot tab when it is present."""
    if not auction.official_url:
        return []

    lot_url = build_lot_detail_url(auction.official_url)
    try:
        response = session.get(lot_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return []

    lot_numbers = parse_detail_lot_numbers_page(response.text)
    parsed_lots = []

    if lot_numbers:
        for lot_number in lot_numbers:
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                lot_general_response = session.get(
                    build_lot_detail_url(auction.official_url, lot_number).replace("ver=3", "ver=1"),
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                lot_general_response.raise_for_status()
                time.sleep(REQUEST_DELAY_SECONDS)
                lot_response = session.get(
                    build_lot_detail_url(auction.official_url, lot_number),
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                lot_response.raise_for_status()
            except requests.RequestException:
                continue

            parsed_general_lot = parse_detail_lot_general_page(
                lot_general_response.text,
                lot_number=lot_number,
            )
            parsed_lot_pages = parse_detail_lots_page(lot_response.text)
            parsed_lot = next(
                (candidate for candidate in parsed_lot_pages if candidate.lot_number == lot_number),
                None,
            )
            merged_lot = merge_parsed_lot_data(parsed_lot, parsed_general_lot)
            if merged_lot is not None:
                parsed_lots.append(merged_lot)
    else:
        parsed_lots = parse_detail_lots_page(response.text)

    if not parsed_lots:
        return []

    lot_auctions: list[Auction] = []
    for parsed_lot in parsed_lots:
        lot_auctions.append(build_lot_auction(auction, parsed_lot))

    return lot_auctions


def build_lot_auction(auction: Auction, parsed_lot) -> Auction:
    """Build one lot auction while preserving parent-level dates and stable metadata."""
    return Auction(
        source=auction.source,
        external_id=f"{auction.external_id}::lot:{parsed_lot.lot_number}" if auction.external_id else None,
        title=f"{auction.title} - Lote {parsed_lot.lot_number}",
        province=parsed_lot.province or auction.province,
        municipality=parsed_lot.municipality or auction.municipality,
        postal_code=parsed_lot.postal_code or auction.postal_code,
        asset_class=parsed_lot.asset_class,
        asset_subclass=parsed_lot.asset_subclass,
        is_vehicle=auction.is_vehicle,
        official_status=auction.official_status,
        publication_date=auction.publication_date,
        opening_date=auction.opening_date,
        closing_date=auction.closing_date,
        appraisal_value=parsed_lot.appraisal_value,
        starting_bid=parsed_lot.starting_bid,
        current_bid=auction.current_bid,
        deposit=parsed_lot.deposit,
        score=None,
        occupancy_status=parsed_lot.occupancy_status,
        encumbrances_summary=parsed_lot.encumbrances_summary,
        description=parsed_lot.description,
        official_url=build_lot_detail_url(auction.official_url, parsed_lot.lot_number),
    )


def fetch_listing_pages_with_pagination(
    search_configs: list[SearchConfig],
    *,
    session: requests.Session,
    max_pages: int,
) -> list[dict[str, str]]:
    """Fetch several listing searches and follow the BOE next-page links when present."""
    fetched_pages: list[dict[str, str]] = []

    for search_index, search_config in enumerate(search_configs, start=1):
        print(f"Fetching search {search_index}/{len(search_configs)}: {search_config.name}")
        current_url = search_config.url
        visited_urls: set[str] = set()

        for page_number in range(1, max_pages + 1):
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

            next_page_url = extract_next_listing_page_url(response.text, current_url)
            if next_page_url is None:
                break
            current_url = next_page_url

    return fetched_pages


def extract_next_listing_page_url(html: str, current_url: str) -> str | None:
    """Extract the BOE next-page URL from the listing pagination controls."""
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.select(".paginar2 a"):
        if not isinstance(link, Tag):
            continue
        link_text = link.get_text(" ", strip=True)
        href = link.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        if "Pág. siguiente" in link_text:
            return urljoin(current_url, href)
    return None


def dedupe_listing_items(items):
    """Deduplicate parsed listing items before detail expansion."""
    deduped_items = []
    seen_keys: set[str] = set()

    for item in items:
        dedupe_key = item.external_id or item.official_url or item.title
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped_items.append(item)

    return deduped_items


def dedupe_listing_entries(listing_entries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Deduplicate listing entries while preserving the first contributing search."""
    deduped_entries = []
    seen_keys: set[str] = set()

    for entry in listing_entries:
        item = entry["item"]
        dedupe_key = item.external_id or item.official_url or item.title
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped_entries.append(entry)

    return deduped_entries


def extract_base_external_id(external_id: str | None) -> str | None:
    """Resolve the shared parent identifier for parent auctions and lot rows."""
    if external_id is None:
        return None
    return external_id.split("::lot:", maxsplit=1)[0]


def propagate_parent_postal_codes(auctions: list[Auction]) -> list[Auction]:
    """Propagate a postal code to the parent only when all useful lots agree."""
    valid_lot_postals_by_parent: dict[str, set[str]] = {}

    for auction in auctions:
        external_id = auction.external_id
        if external_id is None or "::lot:" not in external_id:
            continue

        parent_external_id = extract_base_external_id(external_id)
        postal_code = normalize_postal_code(auction.postal_code)
        if parent_external_id is None or postal_code is None:
            continue

        valid_lot_postals_by_parent.setdefault(parent_external_id, set()).add(postal_code)

    propagated_auctions: list[Auction] = []
    for auction in auctions:
        external_id = auction.external_id
        if external_id is None or "::lot:" in external_id:
            propagated_auctions.append(auction)
            continue

        if normalize_postal_code(auction.postal_code) is not None:
            propagated_auctions.append(auction)
            continue

        parent_external_id = extract_base_external_id(external_id)
        candidate_postal_codes = valid_lot_postals_by_parent.get(parent_external_id or "", set())
        if len(candidate_postal_codes) != 1:
            propagated_auctions.append(auction)
            continue

        propagated_auctions.append(
            auction.model_copy(update={"postal_code": next(iter(candidate_postal_codes))})
        )

    return propagated_auctions


def propagate_parent_postal_codes_in_entries(
    auction_entries: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Apply conservative parent postal-code propagation while preserving metadata."""
    propagated_auctions = propagate_parent_postal_codes(
        [entry["auction"] for entry in auction_entries]
    )
    return [
        {
            **entry,
            "auction": propagated_auction,
        }
        for entry, propagated_auction in zip(auction_entries, propagated_auctions, strict=True)
    ]


def map_listing_entries_to_auctions(listing_entries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Map deduplicated listing entries to auctions while keeping search origin."""
    auction_entries: list[dict[str, object]] = []

    for entry in listing_entries:
        auctions = map_parsed_items_to_auctions([entry["item"]])
        for auction in auctions:
            auction_entries.append(
                {
                    "auction": auction,
                    "search_name": entry["search_name"],
                }
            )

    return auction_entries


def build_empty_search_report() -> dict[str, int]:
    """Build an empty metrics container for one search."""
    return {
        "raw_items_found": 0,
        "unique_items_contributed_after_dedupe": 0,
        "detail_auctions_expanded": 0,
        "lot_auctions_generated": 0,
        "saved_to_sqlite": 0,
    }


def merge_parsed_lot_data(
    parsed_asset_lot,
    parsed_general_lot,
):
    """Merge lot asset metadata with lot auction amounts from the general tab."""
    if parsed_asset_lot is None:
        return parsed_general_lot
    if parsed_general_lot is None:
        return parsed_asset_lot

    return parsed_asset_lot.__class__(
        parent_external_id=parsed_asset_lot.parent_external_id or parsed_general_lot.parent_external_id,
        lot_number=parsed_asset_lot.lot_number,
        title=parsed_asset_lot.title or parsed_general_lot.title,
        description=parsed_asset_lot.description or parsed_general_lot.description,
        asset_class=parsed_asset_lot.asset_class,
        asset_subclass=parsed_asset_lot.asset_subclass,
        province=parsed_asset_lot.province or parsed_general_lot.province,
        municipality=parsed_asset_lot.municipality or parsed_general_lot.municipality,
        postal_code=parsed_asset_lot.postal_code or parsed_general_lot.postal_code,
        appraisal_value=(
            parsed_general_lot.appraisal_value
            if parsed_general_lot.appraisal_value is not None
            else parsed_asset_lot.appraisal_value
        ),
        starting_bid=(
            parsed_general_lot.starting_bid
            if parsed_general_lot.starting_bid is not None
            else parsed_asset_lot.starting_bid
        ),
        deposit=(
            parsed_general_lot.deposit
            if parsed_general_lot.deposit is not None
            else parsed_asset_lot.deposit
        ),
        occupancy_status=parsed_asset_lot.occupancy_status or parsed_general_lot.occupancy_status,
        encumbrances_summary=parsed_asset_lot.encumbrances_summary or parsed_general_lot.encumbrances_summary,
        official_url=parsed_asset_lot.official_url or parsed_general_lot.official_url,
    )


def select_sample_auctions(auctions: list[Auction], *, target_count: int) -> list[Auction]:
    """Pick a small, varied sample without introducing complex selection logic."""
    selected: list[Auction] = []
    seen_ids: set[str] = set()

    def add_first_matching(predicate) -> None:
        for auction in auctions:
            identity = auction.external_id or auction.title
            if identity in seen_ids:
                continue
            if predicate(auction):
                _add_auction_selection(selected, seen_ids, auctions, auction)
                return

    # Priorizar unos pocos perfiles utiles para revisar el scoring con datos reales.
    add_first_matching(lambda auction: "vivienda" in auction.title.casefold())
    add_first_matching(
        lambda auction: any(
            token in auction.title.casefold()
            for token in ("garaje", "plaza", "trastero")
        )
    )
    add_first_matching(
        lambda auction: auction.appraisal_value is None or auction.starting_bid is None or auction.deposit is None
    )
    add_first_matching(
        lambda auction: auction.description is not None and "lote" in auction.description.casefold()
    )

    for auction in auctions:
        if len(selected) >= target_count:
            break
        identity = auction.external_id or auction.title
        if identity in seen_ids:
            continue
        _add_auction_selection(selected, seen_ids, auctions, auction)

    # Mantener juntos los lotes de una misma subasta aunque la muestra final
    # crezca un poco respecto al objetivo nominal.
    return selected


def select_sample_auction_entries(
    auction_entries: list[dict[str, object]],
    *,
    target_count: int,
) -> list[dict[str, object]]:
    """Apply sample selection without losing the search attribution."""
    selected_auctions = select_sample_auctions(
        [entry["auction"] for entry in auction_entries],
        target_count=target_count,
    )
    selected_keys = {
        auction.external_id or auction.title
        for auction in selected_auctions
    }
    return [
        entry
        for entry in auction_entries
        if (entry["auction"].external_id or entry["auction"].title) in selected_keys
    ]


def _add_auction_selection(
    selected: list[Auction],
    seen_ids: set[str],
    auctions: list[Auction],
    auction: Auction,
) -> None:
    """Add one auction and keep the full lot family together when applicable."""
    identity = auction.external_id or auction.title
    if identity in seen_ids:
        return

    selected.append(auction)
    seen_ids.add(identity)

    parent_id = _extract_lot_parent_id(auction.external_id)
    if parent_id is None:
        return

    for related_auction in auctions:
        related_identity = related_auction.external_id or related_auction.title
        if related_identity in seen_ids:
            continue
        if _extract_lot_parent_id(related_auction.external_id) != parent_id:
            continue
        selected.append(related_auction)
        seen_ids.add(related_identity)


def _extract_lot_parent_id(external_id: str | None) -> str | None:
    """Return the parent auction identifier for synthetic lot rows."""
    if external_id is None or "::lot:" not in external_id:
        return None
    return external_id.split("::lot:", maxsplit=1)[0]


if __name__ == "__main__":
    main()
