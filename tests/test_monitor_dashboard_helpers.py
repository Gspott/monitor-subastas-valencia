"""Tests for lightweight dashboard helper functions."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from monitor.domain.models import AuctionRecord, OpportunityEvaluation, RecordDerivations
from monitor.models import Auction
from scripts.monitor_dashboard import (
    average_optional,
    build_active_table_rows,
    build_completed_history_signals,
    build_completed_summary,
    build_completed_table_rows,
    build_group_summary_rows,
    build_history_confidence_label,
    build_historical_heat_label,
    compute_fraction,
    compute_ratio,
    filter_top_opportunity_rows,
    is_top_opportunity_row,
    resolve_active_history_signal,
    select_recent_completed_history_rows,
    sort_completed_table_rows,
)


def test_compute_ratio_returns_none_for_missing_or_zero_denominator() -> None:
    """Debe evitar ratios inválidos cuando faltan importes o el denominador es cero."""
    assert compute_ratio(numerator=Decimal("1000"), denominator=None) is None
    assert compute_ratio(numerator=Decimal("1000"), denominator=Decimal("0")) is None
    assert compute_ratio(numerator=None, denominator=Decimal("1000")) is None


def test_build_completed_table_rows_computes_final_bid_ratios() -> None:
    """Debe construir filas de completed con ratios finales derivados."""
    auction = Auction(
        source="BOE",
        external_id="SUB-COMPLETE-1::lot:2",
        title="Lote completado",
        province="Valencia",
        municipality="Gandia",
        postal_code="46701",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="concluida en portal de subastas",
        publication_date=None,
        opening_date=date(2026, 4, 1),
        closing_date=date(2026, 4, 10),
        appraisal_value=Decimal("100000"),
        starting_bid=Decimal("50000"),
        current_bid=Decimal("75000"),
        deposit=Decimal("5000"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.com/completed",
    )

    rows = build_completed_table_rows([auction])

    assert len(rows) == 1
    assert rows[0]["auction_lot_id"] == "SUB-COMPLETE-1::lot:2"
    assert rows[0]["lot_number"] == 2
    assert rows[0]["postal_code"] == "46701"
    assert rows[0]["current_bid"] == "75000"
    assert rows[0]["final_bid_ratio_vs_appraisal"] == "0.75"
    assert rows[0]["final_bid_ratio_vs_starting_bid"] == "1.50"


def test_build_active_table_rows_keeps_lot_location_instead_of_parent_location() -> None:
    """Debe mostrar la ubicacion del lote cuando el record final ya la trae resuelta."""
    evaluation = OpportunityEvaluation(
        record=AuctionRecord(
            auction_id="SUB-AT-2026-26R4686001035::lot:1",
            source_url="https://example.com/lot",
            lot_number=1,
            title="Lote en Daimus",
            description=None,
            asset_type="real_estate",
            asset_subtype="residential_property",
            province="Valencia/València",
            municipality="Daimus",
            postal_code="46710",
            appraisal_value=Decimal("100000"),
            opening_bid=Decimal("50000"),
            deposit=Decimal("5000"),
            auction_date=date(2026, 4, 15),
        ),
        derivations=RecordDerivations(
            opening_bid_ratio=Decimal("0.50"),
            deposit_ratio=Decimal("0.05"),
            has_reference_price_data=True,
            is_property=True,
            is_residential_like=True,
            is_in_target_area=True,
            has_minimum_location=True,
        ),
        score=80,
        category="high_interest",
        positive_reasons=["Good discount"],
        negative_reasons=["Unknown charges"],
        warnings=[],
    )

    rows = build_active_table_rows([evaluation], historical_signals={"postal_code": {}, "municipality": {}})

    assert rows[0]["auction_lot_id"] == "SUB-AT-2026-26R4686001035::lot:1"
    assert rows[0]["municipality"] == "Daimus"
    assert rows[0]["postal_code"] == "46710"
    assert rows[0]["location"] == "Daimus (46710)"


def test_build_completed_summary_ignores_invalid_ratios() -> None:
    """Debe excluir ratios inválidos de medias y máximos agregados."""
    rows = [
        {
            "_has_final_bid": True,
            "_final_bid_ratio_vs_starting_bid": Decimal("1.20"),
            "_final_bid_ratio_vs_appraisal": Decimal("0.80"),
        },
        {
            "_has_final_bid": True,
            "_final_bid_ratio_vs_starting_bid": None,
            "_final_bid_ratio_vs_appraisal": None,
        },
        {
            "_has_final_bid": False,
            "_final_bid_ratio_vs_starting_bid": Decimal("1.80"),
            "_final_bid_ratio_vs_appraisal": Decimal("0.95"),
        },
    ]

    summary = build_completed_summary(rows)

    assert summary["total_completed_rows"] == 3
    assert summary["rows_with_current_bid"] == 2
    assert summary["rows_without_current_bid"] == 1
    assert summary["no_bid_rate"] == "0.33"
    assert summary["rows_with_final_bid_ratio_vs_starting_bid"] == 2
    assert summary["rows_with_final_bid_ratio_vs_appraisal"] == 2
    assert summary["average_final_bid_ratio_vs_starting_bid"] == "1.50"
    assert summary["average_final_bid_ratio_vs_appraisal"] == "0.88"
    assert summary["max_final_bid_ratio_vs_starting_bid"] == "1.80"
    assert summary["max_final_bid_ratio_vs_appraisal"] == "0.95"


def test_build_group_summary_rows_uses_only_valid_starting_ratios() -> None:
    """Debe agrupar filas de municipality con conteo de pujas y no-pujas."""
    rows = [
        {
            "asset_type": "real_estate",
            "municipality": "Valencia",
            "postal_code": "46001",
            "_has_final_bid": True,
            "_final_bid_ratio_vs_starting_bid": Decimal("1.20"),
        },
        {
            "asset_type": "real_estate",
            "municipality": "Valencia",
            "postal_code": "46001",
            "_has_final_bid": False,
            "_final_bid_ratio_vs_starting_bid": Decimal("1.80"),
        },
        {
            "asset_type": "other",
            "municipality": "Gandia",
            "postal_code": "46701",
            "_has_final_bid": False,
            "_final_bid_ratio_vs_starting_bid": None,
        },
    ]

    grouped = build_group_summary_rows(rows, group_key="asset_type")

    assert grouped == [
        {
            "asset_type": "real_estate",
            "count": 2,
            "rows_with_bid": 1,
            "rows_without_bid": 1,
            "no_bid_rate": "0.50",
            "average_final_bid_ratio_vs_starting_bid": "1.50",
        },
        {
            "asset_type": "other",
            "count": 1,
            "rows_with_bid": 0,
            "rows_without_bid": 1,
            "no_bid_rate": "1.00",
            "average_final_bid_ratio_vs_starting_bid": "-",
        }
    ]


def test_average_optional_returns_none_for_empty_values() -> None:
    """Debe devolver None cuando no hay valores válidos."""
    assert average_optional([]) is None


def test_compute_fraction_returns_none_for_zero_denominator() -> None:
    """Debe devolver None cuando no hay filas sobre las que calcular tasa."""
    assert compute_fraction(1, 0) is None


def test_build_group_summary_rows_orders_municipality_by_no_bid_rate_then_count() -> None:
    """Debe priorizar municipios con mayor no_bid_rate y luego mayor volumen."""
    rows = [
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": True, "_final_bid_ratio_vs_starting_bid": Decimal("1.10")},
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
        {"municipality": "Gandia", "postal_code": "46701", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
        {"municipality": "Gandia", "postal_code": "46701", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
    ]

    grouped = build_group_summary_rows(rows, group_key="municipality")

    assert [row["municipality"] for row in grouped] == ["Gandia", "Valencia"]
    assert grouped[0]["no_bid_rate"] == "1.00"
    assert grouped[1]["no_bid_rate"] == "0.50"


def test_build_group_summary_rows_builds_postal_code_summary() -> None:
    """Debe generar resumen por codigo postal con pujas y tasa sin puja."""
    rows = [
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": True, "_final_bid_ratio_vs_starting_bid": Decimal("1.10")},
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
        {"municipality": "Torrent", "postal_code": "46900", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
    ]

    grouped = build_group_summary_rows(rows, group_key="postal_code")

    assert grouped[0]["postal_code"] == "46900"
    assert grouped[0]["rows_without_bid"] == 1
    assert grouped[0]["no_bid_rate"] == "1.00"
    assert grouped[1]["postal_code"] == "46001"
    assert grouped[1]["rows_with_bid"] == 1
    assert grouped[1]["rows_without_bid"] == 1


def test_sort_completed_table_rows_supports_postal_code() -> None:
    """Debe permitir ordenar completed por codigo postal sin romper la vista."""
    rows = [
        {"auction_lot_id": "SUB-2", "_sort_postal_code": "46980"},
        {"auction_lot_id": "SUB-1", "_sort_postal_code": "46001"},
        {"auction_lot_id": "SUB-3", "_sort_postal_code": ""},
    ]

    sorted_rows = sort_completed_table_rows(rows, sort_by="postal_code")

    assert [row["auction_lot_id"] for row in sorted_rows] == ["SUB-1", "SUB-2", "SUB-3"]


def test_build_completed_history_signals_respects_min_sample_size() -> None:
    """Debe ignorar grupos con muestra histórica insuficiente."""
    rows = [
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": True, "_final_bid_ratio_vs_starting_bid": Decimal("1.20")},
        {"municipality": "Valencia", "postal_code": "46001", "_has_final_bid": True, "_final_bid_ratio_vs_starting_bid": Decimal("1.30")},
        {"municipality": "Gandia", "postal_code": "46701", "_has_final_bid": True, "_final_bid_ratio_vs_starting_bid": Decimal("1.10")},
        {"municipality": "Gandia", "postal_code": "46701", "_has_final_bid": False, "_final_bid_ratio_vs_starting_bid": None},
    ]

    signals = build_completed_history_signals(rows, min_sample_size=3)

    assert "46001" in signals["postal_code"]
    assert "46701" not in signals["postal_code"]
    assert signals["postal_code"]["46001"]["heat_label"] == "mixed_market_low_confidence"
    assert signals["postal_code"]["46001"]["sample_size"] == 3
    assert signals["postal_code"]["46001"]["confidence_label"] == "low"


def test_resolve_active_history_signal_prefers_postal_code_then_falls_back_to_municipality() -> None:
    """Debe priorizar CP y usar municipio como fallback cuando no hay señal fina."""
    signals = {
        "postal_code": {
            "46001": {
                "sample_size": 4,
                "no_bid_rate": Decimal("0.20"),
                "avg_final_ratio_vs_starting_bid": Decimal("1.30"),
                "confidence_label": "low",
                "heat_label": "hot_market",
                "source_group": "postal_code",
            }
        },
        "municipality": {
            "Valencia": {
                "sample_size": 5,
                "no_bid_rate": Decimal("0.70"),
                "avg_final_ratio_vs_starting_bid": Decimal("1.05"),
                "confidence_label": "low",
                "heat_label": "cold_market",
                "source_group": "municipality",
            }
        },
    }

    postal_signal = resolve_active_history_signal(
        municipality="Valencia",
        postal_code="46001",
        historical_signals=signals,
    )
    municipality_signal = resolve_active_history_signal(
        municipality="Valencia",
        postal_code=None,
        historical_signals=signals,
    )

    assert postal_signal["heat_label"] == "hot_market"
    assert postal_signal["source_group"] == "postal_code"
    assert postal_signal["confidence_label"] == "low"
    assert municipality_signal["heat_label"] == "cold_market"
    assert municipality_signal["source_group"] == "municipality"
    assert municipality_signal["confidence_label"] == "low"


def test_resolve_active_history_signal_returns_unknown_without_enough_history() -> None:
    """Debe devolver unknown cuando no existe histórico suficiente."""
    signal = resolve_active_history_signal(
        municipality="Sueca",
        postal_code="46410",
        historical_signals={"postal_code": {}, "municipality": {}},
    )

    assert signal["heat_label"] == "unknown"
    assert signal["no_bid_rate"] is None


def test_build_historical_heat_label_classifies_cold_mixed_hot_and_unknown() -> None:
    """Debe clasificar el calor histórico con una heurística simple y legible."""
    assert build_historical_heat_label(
        no_bid_rate=Decimal("0.70"),
        avg_final_ratio_vs_starting_bid=Decimal("1.05"),
        sample_size=40,
    ) == "cold_market"
    assert build_historical_heat_label(
        no_bid_rate=Decimal("0.20"),
        avg_final_ratio_vs_starting_bid=Decimal("1.25"),
        sample_size=40,
    ) == "hot_market"
    assert build_historical_heat_label(
        no_bid_rate=Decimal("0.40"),
        avg_final_ratio_vs_starting_bid=Decimal("1.10"),
        sample_size=40,
    ) == "mixed_market"
    assert build_historical_heat_label(
        no_bid_rate=None,
        avg_final_ratio_vs_starting_bid=Decimal("1.10"),
        sample_size=40,
    ) == "unknown"


def test_build_history_confidence_label_uses_simple_sample_bands() -> None:
    """Debe asignar confidence con bandas simples y explícitas."""
    assert build_history_confidence_label(sample_size=0, min_sample_size=3) == "insufficient"
    assert build_history_confidence_label(sample_size=3, min_sample_size=3) == "low"
    assert build_history_confidence_label(sample_size=12, min_sample_size=3) == "medium"
    assert build_history_confidence_label(sample_size=35, min_sample_size=3) == "high"


def test_select_recent_completed_history_rows_keeps_only_recent_dated_rows() -> None:
    """Debe limitar el histórico a filas con closing_date válido y ordenar por recencia."""
    rows = [
        {"auction_lot_id": "SUB-1", "_has_closing_date": True, "_sort_closing_date": date(2026, 4, 10)},
        {"auction_lot_id": "SUB-2", "_has_closing_date": False, "_sort_closing_date": date.max},
        {"auction_lot_id": "SUB-3", "_has_closing_date": True, "_sort_closing_date": date(2026, 4, 12)},
        {"auction_lot_id": "SUB-4", "_has_closing_date": True, "_sort_closing_date": date(2026, 4, 11)},
    ]

    limited_rows = select_recent_completed_history_rows(rows, max_rows=2)

    assert [row["auction_lot_id"] for row in limited_rows] == ["SUB-3", "SUB-4"]


def test_is_top_opportunity_row_accepts_discounted_cold_market_rows() -> None:
    """Debe aceptar filas con descuento razonable y mercado historicamente frio."""
    row = {
        "has_price_data": "yes",
        "opening_bid_ratio": "0.80",
        "historical_confidence": "medium",
        "historical_heat_label": "cold_market",
    }

    assert is_top_opportunity_row(row) is True


def test_is_top_opportunity_row_accepts_relaxed_mixed_market_rows() -> None:
    """Debe aceptar mixed_market en el modo relajado actual."""
    row = {
        "has_price_data": "yes",
        "opening_bid_ratio": "0.95",
        "historical_confidence": "low",
        "historical_heat_label": "mixed_market_low_confidence",
    }

    assert is_top_opportunity_row(row) is True


def test_is_top_opportunity_row_rejects_missing_price_data_unknown_hot_or_weak_discount() -> None:
    """Debe excluir filas sin datos utiles, con unknown, hot_market o poco descuento."""
    assert is_top_opportunity_row(
        {
            "has_price_data": "no",
            "opening_bid_ratio": "0.40",
            "historical_confidence": "medium",
            "historical_heat_label": "cold_market",
        }
    ) is False
    assert is_top_opportunity_row(
        {
            "has_price_data": "yes",
            "opening_bid_ratio": "0.40",
            "historical_confidence": "insufficient",
            "historical_heat_label": "unknown",
        }
    ) is False
    assert is_top_opportunity_row(
        {
            "has_price_data": "yes",
            "opening_bid_ratio": "0.40",
            "historical_confidence": "high",
            "historical_heat_label": "hot_market",
        }
    ) is False
    assert is_top_opportunity_row(
        {
            "has_price_data": "yes",
            "opening_bid_ratio": "1.01",
            "historical_confidence": "high",
            "historical_heat_label": "cold_market_low_confidence",
        }
    ) is False


def test_filter_top_opportunity_rows_keeps_only_matching_rows() -> None:
    """Debe filtrar el subconjunto accionable según la heurística explícita."""
    rows = [
        {
            "auction_lot_id": "SUB-1",
            "has_price_data": "yes",
            "opening_bid_ratio": "0.60",
            "historical_confidence": "low",
            "historical_heat_label": "cold_market_low_confidence",
        },
        {
            "auction_lot_id": "SUB-2",
            "has_price_data": "yes",
            "opening_bid_ratio": "0.40",
            "historical_confidence": "high",
            "historical_heat_label": "hot_market",
        },
    ]

    filtered_rows = filter_top_opportunity_rows(rows)

    assert [row["auction_lot_id"] for row in filtered_rows] == ["SUB-1"]
