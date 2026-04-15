"""Local smoke test for the current auction opportunity pipeline."""

from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path


# Permitir ejecutar el script desde la raiz del repositorio sin instalar nada extra.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.models import Auction
from monitor.pipeline.ranking import (
    export_opportunities_to_csv,
    export_opportunities_to_json,
    filter_actionable_opportunities,
    rank_opportunities,
)
from monitor.storage import fetch_all_auctions


TOP_N = 10
MIN_SCORE = 50
ALLOWED_CATEGORIES = {"high_interest", "review"}
OUTPUT_DIR = REPO_ROOT / "output"
CSV_OUTPUT_PATH = OUTPUT_DIR / "actionable_opportunities.csv"
JSON_OUTPUT_PATH = OUTPUT_DIR / "actionable_opportunities.json"


def main() -> None:
    """Run the current monitor pipeline and print a quick manual inspection summary."""
    auctions, source_label = load_smoke_auctions()
    evaluations = rank_opportunities(auctions)
    actionable = filter_actionable_opportunities(
        evaluations,
        categories=ALLOWED_CATEGORIES,
        min_score=MIN_SCORE,
        top_n=TOP_N,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = export_opportunities_to_csv(actionable, CSV_OUTPUT_PATH)
    json_path = export_opportunities_to_json(actionable, JSON_OUTPUT_PATH)

    print_summary(
        source_label=source_label,
        evaluations=evaluations,
        actionable=actionable,
        csv_path=csv_path,
        json_path=json_path,
    )


def load_smoke_auctions() -> tuple[list[Auction], str]:
    """Load a simple local auction sample without introducing new infrastructure."""
    try:
        real_auctions = fetch_all_auctions()
    except Exception as exc:
        # Mantener una salida robusta aunque la base local no este disponible.
        return build_example_auctions(), f"manual examples (database unavailable: {exc})"

    if real_auctions:
        return real_auctions, "local SQLite data via fetch_all_auctions()"

    return build_example_auctions(), "manual examples (database is empty)"


def build_example_auctions() -> list[Auction]:
    """Build a small in-script sample that exercises the current pipeline."""
    return [
        Auction(
            source="BOE",
            external_id="SMOKE-HIGH-001",
            title="Vivienda en Valencia",
            province="Valencia",
            municipality="Valencia",
            asset_class="real_estate",
            asset_subclass="residential_property",
            is_vehicle=False,
            official_status="abierta",
            publication_date=None,
            opening_date=None,
            closing_date=date(2026, 4, 25),
            appraisal_value=Decimal("220000"),
            starting_bid=Decimal("70000"),
            current_bid=None,
            deposit=Decimal("6000"),
            score=None,
            occupancy_status=None,
            encumbrances_summary="Sin cargas conocidas",
            description="Vivienda amplia con buena relacion entre puja inicial y valor de subasta.",
            official_url="https://example.test/smoke-high-001",
        ),
        Auction(
            source="BOE",
            external_id="SMOKE-REVIEW-001",
            title="Vivienda en Paterna (2 lotes)",
            province="Valencia",
            municipality="Paterna",
            asset_class="real_estate",
            asset_subclass="residential_property",
            is_vehicle=False,
            official_status="abierta",
            publication_date=None,
            opening_date=None,
            closing_date=date(2026, 4, 22),
            appraisal_value=Decimal("200000"),
            starting_bid=Decimal("160000"),
            current_bid=None,
            deposit=Decimal("25000"),
            score=None,
            occupancy_status=None,
            encumbrances_summary=None,
            description="Descripcion breve (2 lotes).",
            official_url="https://example.test/smoke-review-001",
        ),
        Auction(
            source="BOE",
            external_id="SMOKE-DISCARD-001",
            title="Activo con datos incompletos",
            province="Valencia",
            municipality="Valencia",
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
            official_url=None,
        ),
    ]


def print_summary(
    *,
    source_label: str,
    evaluations,
    actionable,
    csv_path: Path,
    json_path: Path,
) -> None:
    """Print a short human-readable summary of the current ranking output."""
    print("Smoke Test: monitor-subastas-valencia")
    print(f"Data source: {source_label}")
    print(f"Processed auctions: {len(evaluations)}")
    print(f"Actionable auctions: {len(actionable)}")
    print(f"Applied categories: {sorted(ALLOWED_CATEGORIES)}")
    print(f"Minimum score: {MIN_SCORE}")
    print(f"Top N limit: {TOP_N}")
    print(f"CSV export: {csv_path}")
    print(f"JSON export: {json_path}")
    print()
    print(f"Top actionable results (showing up to {min(TOP_N, len(actionable))}):")

    if not actionable:
        print("  No actionable opportunities matched the current filters.")
        return

    for index, evaluation in enumerate(actionable, start=1):
        positive_reason = evaluation.positive_reasons[0] if evaluation.positive_reasons else "-"
        negative_reason = evaluation.negative_reasons[0] if evaluation.negative_reasons else "-"
        warnings = " | ".join(evaluation.warnings) if evaluation.warnings else "-"
        municipality = evaluation.record.municipality or "-"

        print(
            f"{index:>2}. "
            f"{evaluation.record.auction_id or '-'} | "
            f"score={evaluation.score} | "
            f"{evaluation.category} | "
            f"{evaluation.record.title}"
        )
        print(f"    municipality: {municipality}")
        print(f"    top positive: {positive_reason}")
        print(f"    top negative: {negative_reason}")
        print(f"    warnings: {warnings}")


if __name__ == "__main__":
    main()
