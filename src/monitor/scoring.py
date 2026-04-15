"""Opportunity scoring helpers for auctions."""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from .models import Auction


DISCOUNT_WEIGHT = 40
LOW_STARTING_BID_WEIGHT = 25
OCCUPANCY_WEIGHT = 20
LOCATION_WEIGHT = 15

HIGH_RELEVANCE_MUNICIPALITIES = {
    "Valencia",
    "Torrent",
    "Paterna",
    "Gandia",
    "Sagunto",
}

MEDIUM_RELEVANCE_MUNICIPALITIES = {
    "Mislata",
    "Burjassot",
    "Alaquas",
    "Alboraya",
}

OCCUPANCY_RISK_TERMS = (
    "ocupado",
    "ocupada",
    "ocupacion",
    "ocupación",
    "okupa",
    "arrendado",
    "inquilino",
    "sin posesion",
    "sin posesión",
)

FREE_OCCUPANCY_TERMS = (
    "libre",
    "desocupado",
    "desocupada",
    "vacío",
    "vacio",
    "sin ocupantes",
)


def score_auctions(auctions: Iterable[Auction]) -> list[Auction]:
    """Score a collection of auctions."""
    return [score_auction(auction) for auction in auctions]


def score_auction(auction: Auction) -> Auction:
    """Assign an opportunity score from 0 to 100."""
    score = 0
    score += _score_discount_vs_appraisal(auction)
    score += _score_starting_bid_vs_appraisal(auction)
    score += _score_occupancy(auction)
    score += _score_location(auction)

    # Limitar el resultado a un rango simple y estable.
    bounded_score = max(0, min(100, score))
    return auction.model_copy(update={"score": bounded_score})


def _score_discount_vs_appraisal(auction: Auction) -> int:
    """Reward auctions where current price looks discounted versus appraisal."""
    reference_value = auction.current_bid or auction.starting_bid
    appraisal_value = auction.appraisal_value

    if reference_value is None or appraisal_value is None or appraisal_value <= 0:
        return 0

    ratio = _safe_ratio(reference_value, appraisal_value)

    if ratio <= Decimal("0.40"):
        return 40
    if ratio <= Decimal("0.55"):
        return 32
    if ratio <= Decimal("0.70"):
        return 24
    if ratio <= Decimal("0.85"):
        return 12
    return 0


def _score_starting_bid_vs_appraisal(auction: Auction) -> int:
    """Reward low starting bids relative to appraisal value."""
    if auction.starting_bid is None or auction.appraisal_value is None or auction.appraisal_value <= 0:
        return 0

    ratio = _safe_ratio(auction.starting_bid, auction.appraisal_value)

    if ratio <= Decimal("0.30"):
        return 25
    if ratio <= Decimal("0.45"):
        return 20
    if ratio <= Decimal("0.60"):
        return 14
    if ratio <= Decimal("0.75"):
        return 8
    return 0


def _score_occupancy(auction: Auction) -> int:
    """Reward assets without occupancy risk signals."""
    occupancy_text = " ".join(
        value for value in (auction.occupancy_status, auction.description) if value
    ).casefold()

    if not occupancy_text:
        return 10

    if any(term in occupancy_text for term in OCCUPANCY_RISK_TERMS):
        return 0

    if any(term in occupancy_text for term in FREE_OCCUPANCY_TERMS):
        return 20

    return 10


def _score_location(auction: Auction) -> int:
    """Reward municipalities with higher local relevance within Valencia."""
    municipality = auction.municipality.strip()
    province = auction.province.strip()

    if province != "Valencia":
        return 0

    if municipality in HIGH_RELEVANCE_MUNICIPALITIES:
        return 15
    if municipality in MEDIUM_RELEVANCE_MUNICIPALITIES:
        return 10
    return 6


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    """Return a decimal ratio without hidden heuristics."""
    return numerator / denominator
