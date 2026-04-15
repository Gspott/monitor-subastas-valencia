"""Tests for record filters."""

from monitor.domain.enrich import build_record_derivations
from monitor.domain.filters import collect_filter_reasons, is_record_evaluable
from monitor.domain.models import AuctionRecord


def test_collect_filter_reasons_flags_incomplete_non_target_records() -> None:
    """Debe devolver razones claras cuando el registro no merece evaluación."""
    record = AuctionRecord(
        auction_id="SUB-3",
        source_url=None,
        title="Maquinaria industrial",
        description="",
        asset_type="other_non_vehicle_asset",
        asset_subtype="industrial_asset",
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
    reasons = collect_filter_reasons(record, derivations)

    assert "Missing critical price or location data" in reasons
    assert "Missing minimum usable location data" in reasons
    assert "Asset type is outside the current target" in reasons
    assert "Record is too incomplete for confident review" in reasons
    assert is_record_evaluable(record, derivations) is False


def test_collect_filter_reasons_keeps_special_lot_cases_evaluable() -> None:
    """Debe evitar descartar automaticamente los casos explicados por lotes."""
    record = AuctionRecord(
        auction_id="SUB-LOT-FILTER",
        source_url=None,
        title="Subasta con varios lotes",
        description="Ver valor de subasta en cada lote",
        asset_type="real_estate",
        asset_subtype="residential_property",
        province="Valencia",
        municipality="Valencia",
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
    reasons = collect_filter_reasons(record, derivations)

    assert "Missing critical price or location data" not in reasons
    assert "Record is too incomplete for confident review" not in reasons
    assert is_record_evaluable(record, derivations) is True
