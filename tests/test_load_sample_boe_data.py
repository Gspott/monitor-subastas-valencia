"""Tests for the manual BOE sample loader helpers."""

from __future__ import annotations

from decimal import Decimal

from scripts.load_sample_boe_data import (
    apply_detail_to_auction,
    build_lot_auction,
    dedupe_listing_items,
    extract_next_listing_page_url,
    merge_parsed_lot_data,
    propagate_parent_postal_codes,
    select_sample_auctions,
)
from monitor.models import Auction
from monitor.sources.boe import ParsedBoeDetail, ParsedBoeItem, ParsedBoeLot, parse_iso_date


def test_select_sample_auctions_keeps_full_lot_family_together() -> None:
    """Debe conservar todos los lotes hermanos cuando uno entra en la muestra."""
    auctions = [
        _build_auction(
            external_id="SUB-BASE-001",
            title="Subasta SUB-BASE-001",
        ),
        _build_auction(
            external_id="SUB-LOTS-001::lot:1",
            title="Subasta SUB-LOTS-001 - Lote 1",
            description="Vivienda con varios lotes.",
        ),
        _build_auction(
            external_id="SUB-LOTS-001::lot:2",
            title="Subasta SUB-LOTS-001 - Lote 2",
            description="Vivienda con varios lotes.",
        ),
        _build_auction(
            external_id="SUB-LOTS-001::lot:3",
            title="Subasta SUB-LOTS-001 - Lote 3",
            description="Vivienda con varios lotes.",
        ),
    ]

    selected = select_sample_auctions(auctions, target_count=2)

    assert [auction.external_id for auction in selected] == [
        "SUB-LOTS-001::lot:1",
        "SUB-LOTS-001::lot:2",
        "SUB-LOTS-001::lot:3",
    ]


def test_merge_parsed_lot_data_prefers_numeric_amounts_from_general_tab() -> None:
    """Debe combinar la ficha del bien con los importes del tab general del lote."""
    asset_lot = ParsedBoeLot(
        parent_external_id="SUB-LOTS-001",
        lot_number=2,
        title="Bien 1 - Inmueble (Vivienda)",
        description="Vivienda del tipo B",
        asset_class="real_estate",
        asset_subclass="residential_property",
        municipality="Sollana",
        appraisal_value=None,
        starting_bid=None,
        deposit=None,
    )
    general_lot = ParsedBoeLot(
        parent_external_id="SUB-LOTS-001",
        lot_number=2,
        title="Lote 2",
        appraisal_value=Decimal("160000.00"),
        starting_bid=Decimal("128211.40"),
        deposit=Decimal("6410.57"),
    )

    merged_lot = merge_parsed_lot_data(asset_lot, general_lot)

    assert merged_lot is not None
    assert merged_lot.title == "Bien 1 - Inmueble (Vivienda)"
    assert merged_lot.starting_bid == Decimal("128211.40")
    assert merged_lot.appraisal_value == Decimal("160000.00")
    assert merged_lot.deposit == Decimal("6410.57")


def test_extract_next_listing_page_url_reads_boe_next_link() -> None:
    """Debe resolver la URL de la página siguiente desde la paginación del BOE."""
    html = """
    <html>
      <body>
        <div class="paginar2">
          <a href="subastas_ava.php?accion=Mas&id_busqueda=abc,-50-50">Pág. siguiente</a>
        </div>
      </body>
    </html>
    """

    next_url = extract_next_listing_page_url(
        html,
        "https://subastas.boe.es/subastas_ava.php?accion=Buscar",
    )

    assert next_url == "https://subastas.boe.es/subastas_ava.php?accion=Mas&id_busqueda=abc,-50-50"


def test_dedupe_listing_items_keeps_only_unique_external_ids() -> None:
    """Debe deduplicar items repetidos antes de expandir detalles."""
    items = [
        ParsedBoeItem(
            external_id="SUB-ONE",
            title="Auction one",
            province="Valencia",
            municipality="Valencia",
            asset_class="real_estate",
            asset_subclass="residential_property",
            official_status="Celebrandose",
            official_url="https://example.com/one",
        ),
        ParsedBoeItem(
            external_id="SUB-ONE",
            title="Auction one duplicate",
            province="Valencia",
            municipality="Valencia",
            asset_class="real_estate",
            asset_subclass="residential_property",
            official_status="Celebrandose",
            official_url="https://example.com/one-duplicate",
        ),
        ParsedBoeItem(
            external_id="SUB-TWO",
            title="Auction two",
            province="Valencia",
            municipality="Valencia",
            asset_class="real_estate",
            asset_subclass="garage",
            official_status="Celebrandose",
            official_url="https://example.com/two",
        ),
    ]

    deduped = dedupe_listing_items(items)

    assert [item.external_id for item in deduped] == ["SUB-ONE", "SUB-TWO"]


def test_propagate_parent_postal_codes_uses_consensus_from_lots() -> None:
    """Debe propagar el CP al padre cuando todos los lotes utiles coinciden."""
    parent = _build_auction(
        external_id="SUB-PARENT-001",
        title="Subasta SUB-PARENT-001",
    ).model_copy(update={"postal_code": None})
    lot_one = _build_auction(
        external_id="SUB-PARENT-001::lot:1",
        title="Subasta SUB-PARENT-001 - Lote 1",
    ).model_copy(update={"postal_code": "46001"})
    lot_two = _build_auction(
        external_id="SUB-PARENT-001::lot:2",
        title="Subasta SUB-PARENT-001 - Lote 2",
    ).model_copy(update={"postal_code": "46001"})

    propagated = propagate_parent_postal_codes([parent, lot_one, lot_two])

    assert propagated[0].postal_code == "46001"


def test_propagate_parent_postal_codes_keeps_parent_empty_when_lots_disagree() -> None:
    """No debe propagar nada si los lotes utiles apuntan a CP distintos."""
    parent = _build_auction(
        external_id="SUB-PARENT-002",
        title="Subasta SUB-PARENT-002",
    ).model_copy(update={"postal_code": None})
    lot_one = _build_auction(
        external_id="SUB-PARENT-002::lot:1",
        title="Subasta SUB-PARENT-002 - Lote 1",
    ).model_copy(update={"postal_code": "46001"})
    lot_two = _build_auction(
        external_id="SUB-PARENT-002::lot:2",
        title="Subasta SUB-PARENT-002 - Lote 2",
    ).model_copy(update={"postal_code": "46002"})

    propagated = propagate_parent_postal_codes([parent, lot_one, lot_two])

    assert propagated[0].postal_code is None


def test_propagate_parent_postal_codes_does_not_override_existing_parent_value() -> None:
    """Debe respetar el CP propio del padre aunque los lotes coincidan en otro."""
    parent = _build_auction(
        external_id="SUB-PARENT-003",
        title="Subasta SUB-PARENT-003",
    ).model_copy(update={"postal_code": "46100"})
    lot_one = _build_auction(
        external_id="SUB-PARENT-003::lot:1",
        title="Subasta SUB-PARENT-003 - Lote 1",
    ).model_copy(update={"postal_code": "46001"})
    lot_two = _build_auction(
        external_id="SUB-PARENT-003::lot:2",
        title="Subasta SUB-PARENT-003 - Lote 2",
    ).model_copy(update={"postal_code": "46001"})

    propagated = propagate_parent_postal_codes([parent, lot_one, lot_two])

    assert propagated[0].postal_code == "46100"


def test_propagate_parent_postal_codes_keeps_parent_empty_without_useful_lots() -> None:
    """Debe dejar el padre vacio si no hay lotes con CP valido."""
    parent = _build_auction(
        external_id="SUB-PARENT-004",
        title="Subasta SUB-PARENT-004",
    ).model_copy(update={"postal_code": None})
    lot_one = _build_auction(
        external_id="SUB-PARENT-004::lot:1",
        title="Subasta SUB-PARENT-004 - Lote 1",
    ).model_copy(update={"postal_code": None})
    lot_two = _build_auction(
        external_id="SUB-PARENT-004::lot:2",
        title="Subasta SUB-PARENT-004 - Lote 2",
    ).model_copy(update={"postal_code": "46-001"})

    propagated = propagate_parent_postal_codes([parent, lot_one, lot_two])

    assert propagated[0].postal_code is None


def test_apply_detail_to_auction_preserves_opening_date_for_completed_like_rows() -> None:
    """Debe trasladar la fecha de apertura parseada al auction enriquecido."""
    auction = _build_auction(
        external_id="SUB-COMPLETED-LOT-001",
        title="Subasta completada base",
    ).model_copy(update={"opening_date": None, "closing_date": None})
    detail = ParsedBoeDetail(
        external_id="SUB-COMPLETED-LOT-001",
        title="Subasta completada base",
        opening_date="2026-03-20",
        closing_date="2026-04-09",
        appraisal_value=Decimal("100000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=Decimal("4000.00"),
    )

    enriched = apply_detail_to_auction(auction, detail)

    assert enriched.opening_date == parse_iso_date("2026-03-20")
    assert enriched.closing_date == parse_iso_date("2026-04-09")


def test_build_lot_auction_keeps_parent_opening_date_and_lot_postal_code() -> None:
    """Debe conservar fecha de apertura del padre y CP del lote al crear un completed lot."""
    parent = _build_auction(
        external_id="SUB-COMPLETED-LOT-002",
        title="Subasta completada base",
    ).model_copy(
        update={
            "opening_date": parse_iso_date("2026-03-19"),
            "closing_date": parse_iso_date("2026-04-09"),
            "postal_code": None,
        }
    )
    parsed_lot = ParsedBoeLot(
        parent_external_id="SUB-COMPLETED-LOT-002",
        lot_number=1,
        title="Lote 1",
        description="Descripcion del lote",
        asset_class="real_estate",
        asset_subclass="residential_property",
        municipality="Sagunto",
        province="Valencia/València",
        postal_code="46520",
        appraisal_value=Decimal("90500.00"),
        starting_bid=Decimal("90500.00"),
        deposit=Decimal("4525.00"),
    )

    lot_auction = build_lot_auction(parent, parsed_lot)

    assert lot_auction.external_id == "SUB-COMPLETED-LOT-002::lot:1"
    assert lot_auction.opening_date == parse_iso_date("2026-03-19")
    assert lot_auction.postal_code == "46520"
    assert lot_auction.starting_bid == Decimal("90500.00")



def _build_auction(
    *,
    external_id: str,
    title: str,
    description: str | None = None,
) -> Auction:
    return Auction(
        source="BOE",
        external_id=external_id,
        title=title,
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="Celebrandose",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("100000"),
        starting_bid=Decimal("50000"),
        current_bid=None,
        deposit=Decimal("5000"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=description,
        official_url="https://example.com",
    )
