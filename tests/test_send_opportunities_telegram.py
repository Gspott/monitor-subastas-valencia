"""Tests for Telegram opportunity diffing helpers."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from monitor.domain.models import AuctionRecord, OpportunityEvaluation, RecordDerivations
from monitor.models import Auction
from scripts.send_opportunities_telegram import (
    TELEGRAM_TOP_ITEMS,
    build_clickable_source_link,
    build_telegram_historical_signals,
    collect_change_reasons,
    detect_relevant_updates,
    filter_telegram_candidate_evaluations,
    format_history_confidence_label,
    format_history_heat_label,
    format_location,
    format_relevant_updates_summary,
    format_relevant_update_lines,
    generate_mobile_report,
    read_bool_env,
)


def test_detect_relevant_updates_flags_new_opportunity() -> None:
    """Debe avisar cuando aparece una oportunidad no enviada antes."""
    evaluation = _build_evaluation(
        auction_id="SUB-NEW",
        score=72,
        category="review",
        opening_bid_ratio=Decimal("0.35"),
    )

    updates = detect_relevant_updates(
        [evaluation],
        previous_state={},
        ratio_alert_threshold=Decimal("0.20"),
        score_delta_alert_threshold=10,
        historical_signals={"postal_code": {}, "municipality": {}},
    )

    assert len(updates) == 1
    assert updates[0]["change_reasons"] == ["new opportunity"]


def test_collect_change_reasons_flags_material_score_increase() -> None:
    """Debe detectar una subida material de score."""
    reasons = collect_change_reasons(
        current_snapshot={
            "auction_lot_id": "SUB-SCORE",
            "score": 78,
            "category": "review",
            "opening_bid_ratio": Decimal("0.30"),
        },
        previous_snapshot={
            "auction_lot_id": "SUB-SCORE",
            "score": 66,
            "category": "review",
            "opening_bid_ratio": Decimal("0.30"),
        },
        ratio_alert_threshold=Decimal("0.20"),
        score_delta_alert_threshold=10,
    )

    assert reasons == ["score increased by 12"]


def test_collect_change_reasons_flags_promotion_to_high_interest() -> None:
    """Debe detectar el paso a high_interest."""
    reasons = collect_change_reasons(
        current_snapshot={
            "auction_lot_id": "SUB-HIGH",
            "score": 82,
            "category": "high_interest",
            "opening_bid_ratio": Decimal("0.28"),
        },
        previous_snapshot={
            "auction_lot_id": "SUB-HIGH",
            "score": 74,
            "category": "review",
            "opening_bid_ratio": Decimal("0.28"),
        },
        ratio_alert_threshold=Decimal("0.20"),
        score_delta_alert_threshold=10,
    )

    assert reasons == ["promoted to high_interest"]


def test_detect_relevant_updates_returns_empty_when_nothing_changes() -> None:
    """Debe quedarse vacío si no hay novedades relevantes."""
    evaluation = _build_evaluation(
        auction_id="SUB-STABLE",
        score=70,
        category="review",
        opening_bid_ratio=Decimal("0.32"),
    )

    updates = detect_relevant_updates(
        [evaluation],
        previous_state={
            "SUB-STABLE": {
                "auction_lot_id": "SUB-STABLE",
                "score": 70,
                "category": "review",
                "opening_bid_ratio": Decimal("0.32"),
            }
        },
        ratio_alert_threshold=Decimal("0.20"),
        score_delta_alert_threshold=10,
        historical_signals={"postal_code": {}, "municipality": {}},
    )

    assert updates == []


def test_format_relevant_updates_summary_is_mobile_friendly(tmp_path: Path) -> None:
    """Debe resumir pocas oportunidades con bloques cortos y enlace local al HTML."""
    evaluation = _build_evaluation(
        auction_id="SUB-MOBILE",
        score=82,
        category="high_interest",
        opening_bid_ratio=Decimal("0.18"),
    )
    updates = [
        {
            "auction_lot_id": "SUB-MOBILE",
            "evaluation": evaluation,
            "change_reasons": ["new opportunity"],
            "history_context": {
                "historical_heat_label": "cold_market",
                "historical_confidence": "medium",
                "historical_sample_size": 7,
            },
        }
    ]

    text = format_relevant_updates_summary(
        total_auctions=14,
        updates=updates,
        mobile_report_path=tmp_path / "mobile_report.html",
    )

    assert "Novedades relevantes: 1" in text
    assert "🔥 alta prioridad SUB-MOBILE" in text
    assert "📍 Valencia" in text
    assert "Puntuacion=82 | Ratio=0.18 | Apertura=50000 | Tasacion=100000" in text
    assert "✅ Punto fuerte: Good discount" in text
    assert "⚠️ Riesgo: Unknown charges" in text
    assert "🧊 Mercado historico: mercado frio" in text
    assert "📊 Confianza: media" in text
    assert "🧾 Muestra: 7" in text
    assert '🔗 <a href="https://example.com">Abrir ficha</a>' in text
    assert "HTML local:" in text


def test_build_clickable_source_link_returns_short_html_anchor() -> None:
    """Debe convertir la URL larga en un enlace corto clickable para Telegram."""
    link = build_clickable_source_link("https://example.com/path?a=1&b=2")

    assert link == '🔗 <a href="https://example.com/path?a=1&amp;b=2">Abrir ficha</a>'


def test_format_relevant_updates_summary_limits_items_to_telegram_top_items(tmp_path: Path) -> None:
    """Debe limitar el resumen textual al top configurado."""
    updates = [
        {
            "auction_lot_id": f"SUB-{index}",
            "evaluation": _build_evaluation(
                auction_id=f"SUB-{index}",
                score=80 - index,
                category="review",
                opening_bid_ratio=Decimal("0.25"),
            ),
            "change_reasons": ["new opportunity"],
        }
        for index in range(12)
    ]

    text = format_relevant_updates_summary(
        total_auctions=30,
        updates=updates,
        mobile_report_path=tmp_path / "mobile_report.html",
    )

    assert f"{TELEGRAM_TOP_ITEMS}. 👀 revisar SUB-9" in text
    assert "11. 👀 revisar SUB-10" not in text


def test_format_location_prefers_municipality_and_postal_code() -> None:
    """Debe priorizar localidad y codigo postal cuando existan."""
    assert format_location(
        municipality="Gandia",
        postal_code="46701",
        province="Valencia",
    ) == "Gandia (46701)"


def test_format_location_falls_back_to_province() -> None:
    """Debe usar provincia si no hay localidad."""
    assert format_location(
        municipality=None,
        postal_code=None,
        province="Valencia",
    ) == "Valencia"


def test_generate_mobile_report_creates_simple_html_cards(tmp_path: Path) -> None:
    """Debe generar un HTML local legible con tarjetas y enlace BOE."""
    evaluation = _build_evaluation(
        auction_id="SUB-HTML::lot:2",
        score=74,
        category="review",
        opening_bid_ratio=Decimal("0.25"),
    )
    output_path = generate_mobile_report([evaluation], output_path=tmp_path / "mobile_report.html")

    html_text = output_path.read_text(encoding="utf-8")

    assert output_path.exists()
    assert "SUB-HTML::lot:2" in html_text
    assert "Abrir BOE" in html_text
    assert "https://example.com" in html_text


def test_filter_telegram_candidate_evaluations_keeps_only_top_rows_in_top_mode(monkeypatch) -> None:
    """Debe quedarse solo con las oportunidades top cuando el modo top está activo."""
    top_evaluation = _build_evaluation(
        auction_id="SUB-TOP",
        score=81,
        category="high_interest",
        opening_bid_ratio=Decimal("0.60"),
    )
    top_evaluation.record.municipality = "Sueca"
    top_evaluation.record.postal_code = "46410"

    regular_evaluation = _build_evaluation(
        auction_id="SUB-REGULAR",
        score=77,
        category="review",
        opening_bid_ratio=Decimal("0.40"),
    )
    regular_evaluation.record.municipality = "Valencia"
    regular_evaluation.record.postal_code = "46001"

    monkeypatch.setattr(
        "scripts.send_opportunities_telegram.fetch_all_completed_auctions",
        lambda: _build_cold_market_completed_history(),
    )

    filtered = filter_telegram_candidate_evaluations(
        [top_evaluation, regular_evaluation],
        only_top_opportunities=True,
        historical_signals=_build_top_history_signals(),
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == ["SUB-TOP"]


def test_filter_telegram_candidate_evaluations_keeps_previous_behavior_when_top_mode_is_off() -> None:
    """Debe conservar el flujo actual si el modo top está desactivado."""
    evaluations = [
        _build_evaluation(
            auction_id="SUB-1",
            score=70,
            category="review",
            opening_bid_ratio=Decimal("0.35"),
        ),
        _build_evaluation(
            auction_id="SUB-2",
            score=75,
            category="high_interest",
            opening_bid_ratio=Decimal("0.25"),
        ),
    ]

    filtered = filter_telegram_candidate_evaluations(
        evaluations,
        only_top_opportunities=False,
        historical_signals={"postal_code": {}, "municipality": {}},
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == ["SUB-1", "SUB-2"]


def test_top_mode_filter_still_works_with_relevant_update_detection(monkeypatch) -> None:
    """Debe seguir detectando novedades solo dentro del subconjunto top."""
    top_evaluation = _build_evaluation(
        auction_id="SUB-TOP-ALERT",
        score=84,
        category="high_interest",
        opening_bid_ratio=Decimal("0.55"),
    )
    top_evaluation.record.municipality = "Sueca"
    top_evaluation.record.postal_code = "46410"

    non_top_evaluation = _build_evaluation(
        auction_id="SUB-NON-TOP-ALERT",
        score=79,
        category="review",
        opening_bid_ratio=Decimal("0.35"),
    )
    non_top_evaluation.record.municipality = "Madrid"
    non_top_evaluation.record.postal_code = "28001"

    monkeypatch.setattr(
        "scripts.send_opportunities_telegram.fetch_all_completed_auctions",
        lambda: _build_cold_market_completed_history(),
    )

    filtered = filter_telegram_candidate_evaluations(
        [top_evaluation, non_top_evaluation],
        only_top_opportunities=True,
        historical_signals=_build_top_history_signals(),
    )
    updates = detect_relevant_updates(
        filtered,
        previous_state={},
        ratio_alert_threshold=Decimal("0.20"),
        score_delta_alert_threshold=10,
        historical_signals=_build_top_history_signals(),
    )

    assert [update["auction_lot_id"] for update in updates] == ["SUB-TOP-ALERT"]


def test_format_relevant_update_lines_uses_lot_postal_code_and_history_fields() -> None:
    """Debe mostrar el CP del lote y las señales históricas visibles en Telegram."""
    evaluation = _build_evaluation(
        auction_id="SUB-AT-2026-26R4686001035",
        score=78,
        category="review",
        opening_bid_ratio=Decimal("0.62"),
    )
    evaluation.record.lot_number = 1
    evaluation.record.municipality = "Sagunt"
    evaluation.record.postal_code = "46500"

    lines = format_relevant_update_lines(
        index=1,
        update={
            "auction_lot_id": "SUB-AT-2026-26R4686001035::lot:1",
            "evaluation": evaluation,
            "change_reasons": ["new opportunity"],
            "history_context": {
                "historical_heat_label": "mixed_market_low_confidence",
                "historical_confidence": "low",
                "historical_sample_size": 4,
            },
        },
    )

    assert "1. 👀 revisar SUB-AT-2026-26R4686001035::lot:1" in lines[0]
    assert "📍 Sagunt (46500)" in lines[1]
    assert "🧊 Mercado historico: mercado mixto (confianza baja)" in lines[5]
    assert "📊 Confianza: baja" in lines[6]
    assert "🧾 Muestra: 4" in lines[7]


def test_history_label_formatters_translate_values_to_spanish() -> None:
    """Debe traducir heat y confidence a texto visible en castellano."""
    assert format_history_heat_label("cold_market") == "mercado frio"
    assert format_history_heat_label("unknown") == "sin historico fiable"
    assert format_history_confidence_label("high") == "alta"
    assert format_history_confidence_label("insufficient") == "insuficiente"


def test_read_bool_env_supports_explicit_true_values(monkeypatch) -> None:
    """Debe interpretar flags booleanas tipicas del entorno."""
    monkeypatch.setenv("TEST_BOOL_FLAG", "true")

    assert read_bool_env("TEST_BOOL_FLAG", default=False) is True


def test_read_bool_env_falls_back_to_default_for_missing_or_invalid_values(monkeypatch) -> None:
    """Debe mantener el valor por defecto cuando la variable falta o es invalida."""
    monkeypatch.delenv("TEST_BOOL_FLAG", raising=False)
    assert read_bool_env("TEST_BOOL_FLAG", default=True) is True

    monkeypatch.setenv("TEST_BOOL_FLAG", "maybe")
    assert read_bool_env("TEST_BOOL_FLAG", default=False) is False


def _build_evaluation(
    *,
    auction_id: str,
    score: int,
    category: str,
    opening_bid_ratio: Decimal | None,
) -> OpportunityEvaluation:
    return OpportunityEvaluation(
        record=AuctionRecord(
            auction_id=auction_id,
            source_url="https://example.com",
            lot_number=None,
            title="Auction title",
            description="Auction description",
            asset_type="real_estate",
            asset_subtype="residential_property",
            province="Valencia",
            municipality="Valencia",
            postal_code=None,
            appraisal_value=Decimal("100000"),
            opening_bid=Decimal("50000"),
            deposit=Decimal("5000"),
            auction_date=date(2026, 4, 10),
        ),
        derivations=RecordDerivations(
            opening_bid_ratio=opening_bid_ratio,
            deposit_ratio=Decimal("0.05"),
            has_reference_price_data=True,
            is_property=True,
            is_residential_like=True,
            is_in_target_area=True,
            has_minimum_location=True,
        ),
        score=score,
        category=category,
        positive_reasons=["Good discount"],
        negative_reasons=["Unknown charges"],
        warnings=[],
    )


def _build_cold_market_completed_history() -> list[Auction]:
    return [
        Auction(
            source="BOE",
            external_id="SUB-COMPLETE-1",
            title="Cold market 1",
            province="Valencia",
            municipality="Sueca",
            postal_code="46410",
            asset_class="real_estate",
            asset_subclass="residential_property",
            is_vehicle=False,
            official_status="concluida",
            publication_date=None,
            opening_date=date(2026, 1, 1),
            closing_date=date(2026, 1, 10),
            appraisal_value=Decimal("100000"),
            starting_bid=Decimal("50000"),
            current_bid=Decimal("52500"),
            deposit=Decimal("5000"),
            score=None,
            occupancy_status=None,
            encumbrances_summary=None,
            description=None,
            official_url="https://example.com/completed-1",
        ),
        Auction(
            source="BOE",
            external_id="SUB-COMPLETE-2",
            title="Cold market 2",
            province="Valencia",
            municipality="Sueca",
            postal_code="46410",
            asset_class="real_estate",
            asset_subclass="residential_property",
            is_vehicle=False,
            official_status="concluida",
            publication_date=None,
            opening_date=date(2026, 1, 11),
            closing_date=date(2026, 1, 20),
            appraisal_value=Decimal("90000"),
            starting_bid=Decimal("45000"),
            current_bid=None,
            deposit=Decimal("4500"),
            score=None,
            occupancy_status=None,
            encumbrances_summary=None,
            description=None,
            official_url="https://example.com/completed-2",
        ),
        Auction(
            source="BOE",
            external_id="SUB-COMPLETE-3",
            title="Cold market 3",
            province="Valencia",
            municipality="Sueca",
            postal_code="46410",
            asset_class="real_estate",
            asset_subclass="residential_property",
            is_vehicle=False,
            official_status="concluida",
            publication_date=None,
            opening_date=date(2026, 1, 21),
            closing_date=date(2026, 1, 30),
            appraisal_value=Decimal("95000"),
            starting_bid=Decimal("47000"),
            current_bid=None,
            deposit=Decimal("4700"),
            score=None,
            occupancy_status=None,
            encumbrances_summary=None,
            description=None,
            official_url="https://example.com/completed-3",
        ),
    ]


def _build_top_history_signals() -> dict[str, dict[str, dict[str, object]]]:
    completed_auctions = _build_cold_market_completed_history()
    return build_telegram_historical_signals_from_auctions(completed_auctions)


def build_telegram_historical_signals_from_auctions(
    auctions: list[Auction],
) -> dict[str, dict[str, dict[str, object]]]:
    """Debe reproducir el histórico reciente sin depender de SQLite real."""
    from monitor.opportunities.analysis import (
        build_completed_history_rows,
        build_completed_history_signals,
        select_recent_completed_history_rows,
    )

    rows = select_recent_completed_history_rows(build_completed_history_rows(auctions))
    return build_completed_history_signals(rows)
