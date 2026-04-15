"""Tests for the business evaluation pipeline."""

from decimal import Decimal

from monitor.models import Auction
from monitor.pipeline.evaluate import (
    build_auction_lot_record,
    build_auction_record,
    evaluate_auction_or_lots,
    evaluate_opportunity,
    evaluate_parsed_auction,
)
from monitor.sources.boe import ParsedBoeLot


def test_build_auction_record_adapts_current_parser_output_without_inventing_fields() -> None:
    """Debe adaptar Auction a AuctionRecord sin forzar campos ausentes."""
    parsed_auction = Auction(
        source="BOE",
        external_id="SUB-7",
        title="Vivienda en Gandia",
        province="Valencia",
        municipality="Gandia",
        postal_code=None,
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("180000"),
        starting_bid=Decimal("90000"),
        current_bid=None,
        deposit=Decimal("9000"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description="Vivienda con terraza en zona urbana 46730",
        official_url=None,
    )

    record = build_auction_record(parsed_auction)

    assert record.auction_id == "SUB-7"
    assert record.source_url is None
    assert record.asset_type == "real_estate"
    assert record.asset_subtype == "residential_property"
    assert record.postal_code == "46730"
    assert record.address_text is None
    assert record.has_lots is None
    assert "Source URL is not available in the current parsed output" in record.parser_warnings


def test_evaluate_parsed_auction_returns_reviewable_explainable_output() -> None:
    """Debe evaluar una subasta parseada y devolver explicación trazable."""
    parsed_auction = Auction(
        source="BOE",
        external_id="SUB-8",
        title="Vivienda en Paterna",
        province="Valencia",
        municipality="Paterna",
        postal_code=None,
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("220000"),
        starting_bid=Decimal("100000"),
        current_bid=None,
        deposit=Decimal("7000"),
        score=None,
        occupancy_status=None,
        encumbrances_summary="Sin cargas conocidas",
        description="Vivienda con ascensor y distribución familiar en zona consolidada.",
        official_url="https://example.test/sub-8",
    )

    evaluation = evaluate_parsed_auction(parsed_auction)

    assert evaluation.category in {"review", "high_interest"}
    assert evaluation.score > 0
    assert "Location is inside the target area" in evaluation.positive_reasons
    assert evaluation.applied_filters == []


def test_evaluate_parsed_auction_does_not_discard_when_one_reference_price_exists() -> None:
    """Debe evaluar una subasta realista aunque falte opening_bid si hay valor de referencia."""
    parsed_auction = Auction(
        source="BOE",
        external_id="SUB-AT-2026-26R4686001049",
        title="Subasta SUB-AT-2026-26R4686001049",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        asset_class="real_estate",
        asset_subclass="garage",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("11996.15"),
        starting_bid=None,
        current_bid=None,
        deposit=Decimal("599.80"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.test/sub-real",
    )

    evaluation = evaluate_parsed_auction(parsed_auction)

    assert evaluation.category in {"review", "high_interest", "discard"}
    assert "Missing critical price or location data" not in evaluation.applied_filters
    assert "Record is too incomplete for confident review" not in evaluation.applied_filters
    assert evaluation.derivations.has_reference_price_data is True


def test_build_auction_lot_record_creates_evaluable_unit_for_one_lot() -> None:
    """Debe adaptar un lote a una unidad evaluable sin romper la ruta normal."""
    parent_auction = Auction(
        source="BOE",
        external_id="SUB-PARENT",
        title="Subasta SUB-PARENT",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=None,
        starting_bid=None,
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.test/sub-parent",
    )
    parsed_lot = ParsedBoeLot(
        parent_external_id="SUB-PARENT",
        lot_number=2,
        title="Bien 1 - Inmueble (Vivienda)",
        description="Vivienda del tipo B.",
        asset_class="real_estate",
        asset_subclass="residential_property",
        province="Valencia",
        municipality="Sollana",
        postal_code="46430",
        appraisal_value=Decimal("0.00"),
        starting_bid=Decimal("116757.60"),
        deposit=Decimal("5837.88"),
        occupancy_status="No consta",
        encumbrances_summary=None,
        official_url="https://example.test/sub-parent?lot=2",
    )

    record = build_auction_lot_record(parent_auction, parsed_lot)
    evaluation = evaluate_opportunity(record)

    assert record.auction_id == "SUB-PARENT::lot:2"
    assert record.lot_number == 2
    assert record.municipality == "Sollana"
    assert evaluation.derivations.opening_bid_ratio is None
    assert evaluation.applied_filters == []


def test_evaluate_auction_or_lots_returns_one_evaluation_per_lot() -> None:
    """Debe evaluar por lote cuando se proporcionan lotes parseados."""
    parent_auction = Auction(
        source="BOE",
        external_id="SUB-PARENT",
        title="Subasta SUB-PARENT",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=None,
        starting_bid=None,
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.test/sub-parent",
    )
    parsed_lots = [
        ParsedBoeLot(
            parent_external_id="SUB-PARENT",
            lot_number=1,
            title="Lote 1",
            description="Local comercial",
            asset_class="real_estate",
            asset_subclass="commercial_property",
            province="Valencia",
            municipality="Sollana",
            postal_code="46430",
            appraisal_value=Decimal("0.00"),
            starting_bid=Decimal("128211.40"),
            deposit=Decimal("6410.57"),
            occupancy_status="No consta",
            encumbrances_summary=None,
            official_url="https://example.test/sub-parent?lot=1",
        ),
        ParsedBoeLot(
            parent_external_id="SUB-PARENT",
            lot_number=2,
            title="Lote 2",
            description="Vivienda",
            asset_class="real_estate",
            asset_subclass="residential_property",
            province="Valencia",
            municipality="Sollana",
            postal_code="46430",
            appraisal_value=Decimal("0.00"),
            starting_bid=Decimal("116757.60"),
            deposit=Decimal("5837.88"),
            occupancy_status="No consta",
            encumbrances_summary=None,
            official_url="https://example.test/sub-parent?lot=2",
        ),
    ]

    evaluations = evaluate_auction_or_lots(parent_auction, parsed_lots)

    assert len(evaluations) == 2
    assert [evaluation.record.lot_number for evaluation in evaluations] == [1, 2]
