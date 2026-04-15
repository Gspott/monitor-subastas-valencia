"""Tests for business rules."""

from decimal import Decimal

from monitor.domain.enrich import build_record_derivations
from monitor.domain.models import AuctionRecord
from monitor.domain.rules import evaluate_rules


def test_evaluate_rules_returns_structured_positive_and_negative_results() -> None:
    """Debe devolver reglas estructuradas y trazables."""
    record = AuctionRecord(
        auction_id="SUB-4",
        source_url=None,
        title="Vivienda en Torrent",
        description="Vivienda con buena descripción y sin estructura compleja.",
        asset_type="real_estate",
        asset_subtype="residential_property",
        province="Valencia",
        municipality="Torrent",
        postal_code=None,
        address_text=None,
        appraisal_value=Decimal("150000"),
        opening_bid=Decimal("60000"),
        deposit=Decimal("5000"),
        auction_date=None,
        has_lots=False,
        lot_count=1,
        charges_text="Sin cargas conocidas",
        occupancy_text=None,
        is_detail_complete=True,
        parser_warnings=[],
    )

    results = evaluate_rules(record, build_record_derivations(record))
    triggered_codes = {result.rule_code for result in results if result.triggered}

    assert len(results) == 12
    assert "opening_bid_reasonable" in triggered_codes
    assert "deposit_reasonable" in triggered_codes
    assert "is_property" in triggered_codes
    assert "is_residential_like" in triggered_codes
    assert "target_area" in triggered_codes
    assert "unknown_charges" not in triggered_codes
    assert "invalid_appraisal" not in triggered_codes


def test_evaluate_rules_penalizes_weak_price_unknown_charges_and_invalid_appraisal() -> None:
    """Debe penalizar con claridad los casos poco atractivos o poco fiables."""
    record = AuctionRecord(
        auction_id="SUB-4B",
        source_url=None,
        title="Activo dudoso",
        description="Activo con tasacion explicita poco fiable y cargas no claras.",
        asset_type="real_estate",
        asset_subtype="commercial_property",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        address_text=None,
        appraisal_value=Decimal("0.00"),
        opening_bid=Decimal("1000.00"),
        deposit=Decimal("150.00"),
        auction_date=None,
        has_lots=False,
        lot_count=1,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=True,
        parser_warnings=[],
    )

    results = evaluate_rules(record, build_record_derivations(record))
    deltas = {result.rule_code: result.score_delta for result in results if result.triggered}

    assert deltas["unknown_charges"] == -20
    assert deltas["invalid_appraisal"] == -15
