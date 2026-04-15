"""Tests for opportunity scoring."""

from decimal import Decimal

from monitor.domain.enrich import build_record_derivations
from monitor.domain.models import AuctionRecord
from monitor.domain.rules import evaluate_rules
from monitor.domain.scoring import build_evaluation


def test_build_evaluation_assigns_high_interest_with_explanations() -> None:
    """Debe asignar high_interest y separar razones positivas y warnings."""
    record = AuctionRecord(
        auction_id="SUB-5",
        source_url=None,
        title="Vivienda en Valencia",
        description="Vivienda amplia con información suficiente para revisión.",
        asset_type="real_estate",
        asset_subtype="residential_property",
        province="Valencia",
        municipality="Valencia",
        postal_code="46002",
        address_text=None,
        appraisal_value=Decimal("200000"),
        opening_bid=Decimal("70000"),
        deposit=Decimal("5000"),
        auction_date=None,
        has_lots=False,
        lot_count=1,
        charges_text="Sin cargas conocidas",
        occupancy_text=None,
        is_detail_complete=True,
        parser_warnings=["Source URL is not available in the current parsed output"],
    )

    derivations = build_record_derivations(record)
    rule_results = evaluate_rules(record, derivations)
    evaluation = build_evaluation(record, derivations, [], rule_results)

    assert evaluation.score == 100
    assert evaluation.category == "high_interest"
    assert "Opening bid still leaves room below appraisal value" in evaluation.positive_reasons
    assert "Source URL is not available in the current parsed output" in evaluation.warnings


def test_build_evaluation_discards_when_filters_exist() -> None:
    """Debe descartar directamente cuando hay filtros de exclusión."""
    record = AuctionRecord(
        auction_id="SUB-6",
        source_url=None,
        title="Activo incompleto",
        description=None,
        asset_type="other_non_vehicle_asset",
        asset_subtype="other_non_vehicle_asset",
        province=None,
        municipality=None,
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=False,
        parser_warnings=[],
    )

    derivations = build_record_derivations(record)
    evaluation = build_evaluation(
        record,
        derivations,
        ["Missing critical price or location data"],
        [],
    )

    assert evaluation.score == 0
    assert evaluation.category == "discard"
    assert evaluation.negative_reasons == ["Missing critical price or location data"]


def test_build_evaluation_creates_more_separated_review_scores() -> None:
    """Debe generar un score intermedio cuando hay incertidumbre y poco descuento."""
    record = AuctionRecord(
        auction_id="SUB-6B",
        source_url=None,
        title="Garaje en Valencia",
        description=None,
        asset_type="real_estate",
        asset_subtype="garage",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        address_text=None,
        appraisal_value=Decimal("12000.00"),
        opening_bid=Decimal("9000.00"),
        deposit=Decimal("600.00"),
        auction_date=None,
        has_lots=False,
        lot_count=1,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=False,
        parser_warnings=["Opening bid is not available in the current parsed output"],
    )

    derivations = build_record_derivations(record)
    rule_results = evaluate_rules(record, derivations)
    evaluation = build_evaluation(record, derivations, [], rule_results)

    assert evaluation.score < 70
    assert evaluation.category == "review"
    assert "Charges are unknown or unclear" in evaluation.negative_reasons
