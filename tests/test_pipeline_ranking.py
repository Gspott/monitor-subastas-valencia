"""Tests for ranking and export helpers."""

from __future__ import annotations

import csv
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from monitor.models import Auction
from monitor.pipeline.ranking import (
    export_opportunities_to_csv,
    export_opportunities_to_json,
    filter_actionable_opportunities,
    rank_opportunities,
    rank_and_filter_opportunities,
)


def test_rank_opportunities_orders_by_category_then_score_then_date() -> None:
    """Debe priorizar categoria, luego score y despues fecha."""
    auctions = [
        _build_auction(
            external_id="SUB-REVIEW",
            municipality="Paterna",
            appraisal_value=Decimal("200000"),
            starting_bid=Decimal("160000"),
            deposit=Decimal("25000"),
            description="Descripcion breve (2 lotes).",
            closing_date=date(2026, 4, 20),
            encumbrances_summary=None,
            title="Vivienda en Valencia (2 lotes)",
        ),
        _build_auction(
            external_id="SUB-HIGH-LATE",
            municipality="Valencia",
            appraisal_value=Decimal("250000"),
            starting_bid=Decimal("80000"),
            deposit=Decimal("5000"),
            description="Vivienda completa con buena relacion precio valor.",
            closing_date=date(2026, 4, 25),
        ),
        _build_auction(
            external_id="SUB-HIGH-EARLY",
            municipality="Torrent",
            appraisal_value=Decimal("250000"),
            starting_bid=Decimal("80000"),
            deposit=Decimal("5000"),
            description="Vivienda completa con buena relacion precio valor.",
            closing_date=date(2026, 4, 10),
        ),
        _build_auction(
            external_id="SUB-DISCARD",
            municipality="Valencia",
            appraisal_value=None,
            starting_bid=None,
            deposit=None,
            description=None,
            closing_date=None,
        ),
    ]

    evaluations = rank_opportunities(auctions)

    assert evaluations[0].category == "high_interest"
    assert evaluations[1].category == "high_interest"
    assert evaluations[2].category == "discard"
    assert evaluations[3].category == "discard"
    assert [evaluation.record.auction_id for evaluation in evaluations] == [
        "SUB-HIGH-EARLY",
        "SUB-HIGH-LATE",
        "SUB-REVIEW",
        "SUB-DISCARD",
    ]


def test_export_opportunities_to_csv_writes_expected_columns(tmp_path: Path) -> None:
    """Debe exportar un CSV legible con razones agregadas."""
    evaluations = rank_opportunities(
        [
            _build_auction(
                external_id="SUB-CSV",
                municipality="Valencia",
                appraisal_value=Decimal("200000"),
                starting_bid=Decimal("70000"),
                deposit=Decimal("5000"),
                description="Vivienda completa con informacion util para revisar.",
                closing_date=date(2026, 4, 15),
            )
        ]
    )

    output_path = export_opportunities_to_csv(evaluations, tmp_path / "opportunities.csv")

    with output_path.open("r", encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert len(rows) == 1
    assert rows[0]["auction_id"] == "SUB-CSV"
    assert rows[0]["category"] == "high_interest"
    assert rows[0]["score"] == "100"
    assert "Opening bid still leaves room below appraisal value" in rows[0]["positive_reasons"]


def test_export_opportunities_to_json_writes_readable_structure(tmp_path: Path) -> None:
    """Debe exportar un JSON claro con bloque de subasta y bloque de evaluacion."""
    evaluations = rank_opportunities(
        [
            _build_auction(
                external_id="SUB-JSON",
                municipality="Gandia",
                appraisal_value=Decimal("180000"),
                starting_bid=Decimal("90000"),
                deposit=Decimal("8000"),
                description="Vivienda para revisar con suficiente contexto.",
                closing_date=date(2026, 4, 18),
            )
        ]
    )

    output_path = export_opportunities_to_json(evaluations, tmp_path / "opportunities.json")
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(payload) == 1
    assert payload[0]["auction"]["auction_id"] == "SUB-JSON"
    assert payload[0]["evaluation"]["score"] > 0
    assert payload[0]["evaluation"]["category"] in {"review", "high_interest"}
    assert "positive" in payload[0]["evaluation"]["reasons"]


def test_rank_and_exports_handle_empty_lists(tmp_path: Path) -> None:
    """Debe comportarse bien cuando no hay subastas."""
    evaluations = rank_opportunities([])

    csv_path = export_opportunities_to_csv(evaluations, tmp_path / "empty.csv")
    json_path = export_opportunities_to_json(evaluations, tmp_path / "empty.json")

    assert evaluations == []
    assert csv_path.read_text(encoding="utf-8").splitlines() == [
        "auction_id,lot_number,title,municipality,asset_type,appraisal_value,opening_bid,opening_bid_ratio,score,category,positive_reasons,negative_reasons,warnings,source_url"
    ]
    assert json.loads(json_path.read_text(encoding="utf-8")) == []


def test_export_helpers_keep_missing_fields_empty() -> None:
    """Debe dejar vacios los campos ausentes en la exportacion."""
    evaluations = rank_opportunities(
        [
            _build_auction(
                external_id=None,
                municipality="Valencia",
                appraisal_value=None,
                starting_bid=None,
                deposit=None,
                description=None,
                closing_date=None,
            )
        ]
    )

    evaluation = evaluations[0]

    assert evaluation.record.auction_id is None
    assert evaluation.record.appraisal_value is None
    assert evaluation.record.opening_bid is None
    assert evaluation.category == "discard"


def test_export_helpers_include_lot_metadata_when_available(tmp_path: Path) -> None:
    """Debe exportar metadatos de lote cuando la unidad representa un lote."""
    evaluations = rank_opportunities(
        [
            _build_auction(
                external_id="SUB-LOT::lot:2",
                municipality="Sollana",
                appraisal_value=Decimal("0.00"),
                starting_bid=Decimal("116757.60"),
                deposit=Decimal("5837.88"),
                description="Vivienda del tipo B.",
                closing_date=date(2026, 4, 15),
                title="Subasta SUB-LOT - Lote 2",
            )
        ]
    )

    csv_path = export_opportunities_to_csv(evaluations, tmp_path / "lots.csv")
    rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8", newline="")))

    assert rows[0]["lot_number"] == "2"
    assert rows[0]["source_url"] == ""


def test_filter_actionable_opportunities_filters_by_categories() -> None:
    """Debe filtrar por categorias permitidas manteniendo el orden."""
    evaluations = _build_ranked_evaluations()

    filtered = filter_actionable_opportunities(
        evaluations,
        categories={"high_interest", "review"},
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == [
        "SUB-HIGH-EARLY",
        "SUB-HIGH-LATE",
    ]


def test_filter_actionable_opportunities_filters_by_min_score() -> None:
    """Debe aplicar score minimo de forma inclusiva."""
    evaluations = _build_ranked_evaluations()

    filtered = filter_actionable_opportunities(evaluations, min_score=80)

    assert [evaluation.record.auction_id for evaluation in filtered] == [
        "SUB-HIGH-EARLY",
        "SUB-HIGH-LATE",
    ]


def test_filter_actionable_opportunities_applies_top_n_after_filters() -> None:
    """Debe limitar el resultado una vez aplicados los filtros."""
    evaluations = _build_ranked_evaluations()

    filtered = filter_actionable_opportunities(
        evaluations,
        categories={"high_interest", "review"},
        top_n=2,
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == [
        "SUB-HIGH-EARLY",
        "SUB-HIGH-LATE",
    ]


def test_filter_actionable_opportunities_combines_filters_and_preserves_order() -> None:
    """Debe combinar filtros sin reordenar la lista de entrada."""
    evaluations = _build_ranked_evaluations()
    manual_order = [evaluations[2], evaluations[1], evaluations[0], evaluations[3]]

    filtered = filter_actionable_opportunities(
        manual_order,
        categories={"high_interest", "review"},
        min_score=60,
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == [
        "SUB-HIGH-LATE",
        "SUB-HIGH-EARLY",
    ]


def test_filter_actionable_opportunities_returns_copy_when_no_filters() -> None:
    """Debe devolver una copia equivalente si no se pasa ningun filtro."""
    evaluations = _build_ranked_evaluations()

    filtered = filter_actionable_opportunities(evaluations)

    assert filtered == evaluations
    assert filtered is not evaluations


def test_filter_actionable_opportunities_handles_empty_list() -> None:
    """Debe devolver lista vacia si no hay evaluaciones."""
    assert filter_actionable_opportunities([], categories={"high_interest"}, min_score=50, top_n=5) == []


def test_rank_and_filter_opportunities_reuses_full_flow() -> None:
    """Debe combinar ranking y seleccion en un helper sencillo."""
    auctions = _build_sample_auctions()

    filtered = rank_and_filter_opportunities(
        auctions,
        categories={"high_interest", "review"},
        min_score=80,
        top_n=1,
    )

    assert [evaluation.record.auction_id for evaluation in filtered] == ["SUB-HIGH-EARLY"]


def _build_auction(
    *,
    external_id: str | None,
    municipality: str,
    appraisal_value: Decimal | None,
    starting_bid: Decimal | None,
    deposit: Decimal | None,
    description: str | None,
    closing_date: date | None,
    encumbrances_summary: str | None = "Sin cargas conocidas",
    title: str = "Vivienda en Valencia",
) -> Auction:
    """Construir subastas simples para tests de pipeline."""
    return Auction(
        source="BOE",
        external_id=external_id,
        title=title,
        province="Valencia",
        municipality=municipality,
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=closing_date,
        appraisal_value=appraisal_value,
        starting_bid=starting_bid,
        current_bid=None,
        deposit=deposit,
        score=None,
        occupancy_status=None,
        encumbrances_summary=encumbrances_summary if appraisal_value is not None else None,
        description=description,
        official_url=None,
    )


def _build_sample_auctions() -> list[Auction]:
    """Construir una muestra consistente para ranking y filtrado."""
    return [
        _build_auction(
            external_id="SUB-REVIEW",
            municipality="Paterna",
            appraisal_value=Decimal("200000"),
            starting_bid=Decimal("160000"),
            deposit=Decimal("25000"),
            description="Descripcion breve (2 lotes).",
            closing_date=date(2026, 4, 20),
            encumbrances_summary=None,
            title="Vivienda en Valencia (2 lotes)",
        ),
        _build_auction(
            external_id="SUB-HIGH-LATE",
            municipality="Valencia",
            appraisal_value=Decimal("250000"),
            starting_bid=Decimal("80000"),
            deposit=Decimal("5000"),
            description="Vivienda completa con buena relacion precio valor.",
            closing_date=date(2026, 4, 25),
        ),
        _build_auction(
            external_id="SUB-HIGH-EARLY",
            municipality="Torrent",
            appraisal_value=Decimal("250000"),
            starting_bid=Decimal("80000"),
            deposit=Decimal("5000"),
            description="Vivienda completa con buena relacion precio valor.",
            closing_date=date(2026, 4, 10),
        ),
        _build_auction(
            external_id="SUB-DISCARD",
            municipality="Valencia",
            appraisal_value=None,
            starting_bid=None,
            deposit=None,
            description=None,
            closing_date=None,
        ),
    ]


def _build_ranked_evaluations():
    """Reutilizar un ranking estable para los tests de seleccion."""
    return rank_opportunities(_build_sample_auctions())
