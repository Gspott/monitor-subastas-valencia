"""Tests for the completed BOE loader helpers."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from decimal import Decimal

import requests

from monitor.models import Auction
from scripts.load_completed_boe_data import (
    INCREMENTAL_MAX_LISTING_PAGES,
    INCREMENTAL_WINDOW_DAYS,
    MAX_LISTING_PAGES,
    build_completed_refresh_config,
    build_completed_search_report,
    fetch_completed_current_bid,
    filter_incremental_completed_listing_entries,
    enrich_completed_auction_with_current_bid,
    extract_lot_number,
    should_early_stop_completed_listing_page,
)


def test_extract_lot_number_reads_synthetic_completed_lot_id() -> None:
    """Debe detectar el numero de lote desde el external_id sintetico."""
    assert extract_lot_number("SUB-COMPLETE-1::lot:3") == 3
    assert extract_lot_number("SUB-COMPLETE-1") is None


def test_build_completed_search_report_adds_current_bid_metric() -> None:
    """Debe incluir rows_with_current_bid en el reporte por búsqueda de completed."""
    report = build_completed_search_report()

    assert report["raw_items_found"] == 0
    assert report["unique_items_contributed_after_dedupe"] == 0
    assert report["detail_auctions_expanded"] == 0
    assert report["lot_auctions_generated"] == 0
    assert report["rows_with_current_bid"] == 0
    assert report["saved_to_sqlite"] == 0


def test_build_completed_refresh_config_uses_incremental_defaults() -> None:
    """Debe usar ventana y paginas cortas en modo incremental por defecto."""
    config = build_completed_refresh_config(full_refresh=False)

    assert config.full_refresh is False
    assert config.window_days == INCREMENTAL_WINDOW_DAYS
    assert config.max_listing_pages == INCREMENTAL_MAX_LISTING_PAGES


def test_build_completed_refresh_config_uses_historical_defaults_for_full_refresh() -> None:
    """Debe conservar el perfil historico al pedir full refresh."""
    config = build_completed_refresh_config(full_refresh=True)

    assert config.full_refresh is True
    assert config.max_listing_pages == MAX_LISTING_PAGES


def test_filter_incremental_completed_listing_entries_keeps_recent_and_undated_rows() -> None:
    """Debe quedarse solo con completed recientes y conservar filas sin fecha fiable."""
    recent_item = SimpleNamespace(closing_date="2026-04-10")
    stale_item = SimpleNamespace(closing_date="2026-03-10")
    undated_item = SimpleNamespace(closing_date=None)

    filtered_entries = filter_incremental_completed_listing_entries(
        [
            {"item": recent_item, "search_name": "recent"},
            {"item": stale_item, "search_name": "stale"},
            {"item": undated_item, "search_name": "undated"},
        ],
        processing_date=date(2026, 4, 15),
        window_days=21,
    )

    assert [entry["search_name"] for entry in filtered_entries] == ["recent", "undated"]


def test_should_early_stop_completed_listing_page_returns_true_for_fully_stale_page() -> None:
    """Debe cortar solo si toda la pagina queda claramente fuera de ventana."""
    should_stop = should_early_stop_completed_listing_page(
        [
            SimpleNamespace(closing_date="2026-03-10"),
            SimpleNamespace(closing_date="2026-03-15"),
        ],
        processing_date=date(2026, 4, 15),
        window_days=21,
        full_refresh=False,
    )

    assert should_stop is True


def test_should_early_stop_completed_listing_page_returns_false_for_mixed_page() -> None:
    """No debe cortar si la pagina mezcla entradas recientes y antiguas."""
    should_stop = should_early_stop_completed_listing_page(
        [
            SimpleNamespace(closing_date="2026-03-10"),
            SimpleNamespace(closing_date="2026-04-10"),
        ],
        processing_date=date(2026, 4, 15),
        window_days=21,
        full_refresh=False,
    )

    assert should_stop is False


def test_should_early_stop_completed_listing_page_returns_false_when_dates_are_missing() -> None:
    """No debe cortar si alguna fecha falta o no es fiable."""
    should_stop = should_early_stop_completed_listing_page(
        [
            SimpleNamespace(closing_date="2026-03-10"),
            SimpleNamespace(closing_date=None),
        ],
        processing_date=date(2026, 4, 15),
        window_days=21,
        full_refresh=False,
    )

    assert should_stop is False


def test_should_early_stop_completed_listing_page_returns_false_for_full_refresh() -> None:
    """El full refresh nunca debe activar early stop."""
    should_stop = should_early_stop_completed_listing_page(
        [
            SimpleNamespace(closing_date="2026-03-10"),
            SimpleNamespace(closing_date="2026-03-15"),
        ],
        processing_date=date(2026, 4, 15),
        window_days=21,
        full_refresh=True,
    )

    assert should_stop is False


def test_enrich_completed_auction_with_current_bid_keeps_lot_metadata(monkeypatch) -> None:
    """Debe adjuntar current_bid al lote completed sin perder el resto del registro."""
    auction = Auction(
        source="BOE",
        external_id="SUB-COMPLETE-1::lot:1",
        title="Lote completado",
        province="Valencia",
        municipality="Gandia",
        postal_code="46701",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="concluida en portal de subastas",
        publication_date=None,
        opening_date=date(2026, 3, 20),
        closing_date=date(2026, 4, 9),
        appraisal_value=Decimal("100000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=Decimal("4000.00"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.com/detailSubasta?idSub=1&ver=3&idLote=1",
    )

    monkeypatch.setattr(
        "scripts.load_completed_boe_data.fetch_completed_current_bid",
        lambda **_: Decimal("105000.00"),
    )

    enriched = enrich_completed_auction_with_current_bid(auction, session=requests.Session())

    assert enriched.current_bid == Decimal("105000.00")
    assert enriched.postal_code == "46701"
    assert enriched.opening_date == date(2026, 3, 20)


def test_fetch_completed_current_bid_falls_back_to_general_bids_table_for_lot(monkeypatch) -> None:
    """Debe leer la tabla general `Pujas máximas` si la vista del lote no trae bloque propio."""

    class DummyResponse:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, url: str, timeout: int):
            self.calls.append(url)
            if "ver=5&idLote=1" in url:
                return DummyResponse("<html><body><h4>Pujas</h4><p>Sin puja pública</p></body></html>")
            return DummyResponse(
                """
                <html><body>
                  <h4>Pujas máximas</h4>
                  <table>
                    <tr><th>Lote</th><th>Importe de la puja</th></tr>
                    <tr><td>1</td><td>71.159,88 €</td></tr>
                    <tr><td>2</td><td>Sin puja</td></tr>
                  </table>
                </body></html>
                """
            )

    monkeypatch.setattr("scripts.load_completed_boe_data.time.sleep", lambda _: None)

    current_bid = fetch_completed_current_bid(
        official_url="https://example.com/detalleSubasta.php?idSub=1&ver=3&idLote=1",
        lot_number=1,
        session=DummySession(),
    )

    assert current_bid == Decimal("71159.88")
