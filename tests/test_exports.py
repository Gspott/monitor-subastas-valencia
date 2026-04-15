"""Tests for CSV export helpers."""

from decimal import Decimal
from pathlib import Path

from monitor.exports import export_all_active_valencia, export_auctions_to_csv
from monitor.models import Auction


def test_export_auctions_to_csv_orders_by_score_descending(tmp_path: Path) -> None:
    """Debe exportar ordenando por score descendente cuando exista."""
    low_score = Auction(
        source="BOE",
        external_id="SUB-LOW",
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("100000"),
        starting_bid=Decimal("90000"),
        current_bid=None,
        deposit=None,
        score=12,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    high_score = Auction(
        source="BOE",
        external_id="SUB-HIGH",
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("200000"),
        starting_bid=Decimal("70000"),
        current_bid=None,
        deposit=None,
        score=88,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    output_path = export_auctions_to_csv([low_score, high_score], tmp_path / "auctions.csv")
    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 3
    assert "SUB-HIGH" in lines[1]
    assert "SUB-LOW" in lines[2]


def test_export_all_active_valencia_filters_inactive_and_non_valencia(tmp_path: Path, monkeypatch) -> None:
    """Debe exportar solo subastas activas de Valencia."""
    monkeypatch.setattr("monitor.exports.ALL_ACTIVE_VALENCIA_EXPORT_PATH", tmp_path / "active.csv")

    active_valencia = Auction(
        source="BOE",
        external_id="SUB-ACTIVE",
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta con pujas",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("200000"),
        starting_bid=Decimal("70000"),
        current_bid=None,
        deposit=None,
        score=88,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    inactive_valencia = active_valencia.model_copy(update={"external_id": "SUB-CLOSED", "official_status": "cerrada"})
    active_other_province = active_valencia.model_copy(update={"external_id": "SUB-OTHER", "province": "Alicante"})

    output_path = export_all_active_valencia([active_valencia, inactive_valencia, active_other_province])
    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 2
    assert "SUB-ACTIVE" in lines[1]
