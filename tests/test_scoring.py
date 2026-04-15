"""Tests for the auction scoring helpers."""

from decimal import Decimal

from monitor.models import Auction
from monitor.scoring import score_auction


def test_score_auction_rewards_discount_free_occupancy_and_location() -> None:
    """Debe asignar una puntuación alta a una oportunidad clara."""
    auction = Auction(
        source="BOE",
        external_id="SUB-100",
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
        current_bid=Decimal("80000"),
        deposit=Decimal("5000"),
        score=None,
        occupancy_status="libre",
        encumbrances_summary=None,
        description="Activo urbano sin ocupantes",
        official_url=None,
    )

    scored = score_auction(auction)

    assert scored.score == 95


def test_score_auction_penalizes_occupancy_risk_and_weak_discount() -> None:
    """Debe reflejar una oportunidad más floja cuando hay riesgo de ocupación."""
    auction = Auction(
        source="BOE",
        external_id="SUB-101",
        title="Local comercial",
        province="Valencia",
        municipality="Xativa",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("100000"),
        starting_bid=Decimal("82000"),
        current_bid=Decimal("90000"),
        deposit=Decimal("4000"),
        score=None,
        occupancy_status="ocupado",
        encumbrances_summary=None,
        description="Local con ocupación",
        official_url=None,
    )

    scored = score_auction(auction)

    assert scored.score == 6
