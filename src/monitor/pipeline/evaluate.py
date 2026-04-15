"""Pipeline helpers to adapt parsed auctions into business evaluations."""

from __future__ import annotations

from collections.abc import Iterable

from ..domain.enrich import build_record_derivations, infer_postal_code
from ..domain.filters import collect_filter_reasons
from ..domain.models import AuctionRecord, OpportunityEvaluation
from ..domain.rules import evaluate_rules
from ..domain.scoring import build_evaluation
from ..models import Auction
from ..sources.boe import ParsedBoeLot


def build_auction_record(parsed_auction: Auction) -> AuctionRecord:
    """Adapt the stable parser output into a business-oriented record."""
    parser_warnings: list[str] = []

    if parsed_auction.appraisal_value is None:
        parser_warnings.append("Appraisal value is not available in the current parsed output")
    if parsed_auction.starting_bid is None:
        parser_warnings.append("Opening bid is not available in the current parsed output")
    if parsed_auction.official_url is None:
        parser_warnings.append("Source URL is not available in the current parsed output")

    record = AuctionRecord(
        auction_id=parsed_auction.external_id,
        source_url=parsed_auction.official_url,
        lot_number=_infer_lot_number(parsed_auction.external_id),
        title=parsed_auction.title,
        description=parsed_auction.description,
        asset_type=parsed_auction.asset_class,
        asset_subtype=parsed_auction.asset_subclass,
        province=parsed_auction.province,
        municipality=parsed_auction.municipality,
        postal_code=parsed_auction.postal_code,
        address_text=None,
        appraisal_value=parsed_auction.appraisal_value,
        opening_bid=parsed_auction.starting_bid,
        deposit=parsed_auction.deposit,
        auction_date=parsed_auction.closing_date,
        has_lots=None,
        lot_count=None,
        charges_text=parsed_auction.encumbrances_summary,
        occupancy_text=parsed_auction.occupancy_status,
        is_detail_complete=_is_detail_complete(parsed_auction),
        parser_warnings=parser_warnings,
    )
    return record if record.postal_code is not None else record.model_copy(update={"postal_code": infer_postal_code(record)})


def build_auction_lot_record(parent_auction: Auction, parsed_lot: ParsedBoeLot) -> AuctionRecord:
    """Adapt one parsed lot into the same business record shape used elsewhere."""
    auction_id = (
        f"{parent_auction.external_id}::lot:{parsed_lot.lot_number}"
        if parent_auction.external_id is not None
        else None
    )
    record = AuctionRecord(
        auction_id=auction_id,
        source_url=parsed_lot.official_url or parent_auction.official_url,
        lot_number=parsed_lot.lot_number,
        title=parsed_lot.title,
        description=parsed_lot.description,
        asset_type=parsed_lot.asset_class,
        asset_subtype=parsed_lot.asset_subclass,
        province=parsed_lot.province or parent_auction.province,
        municipality=parsed_lot.municipality or parent_auction.municipality,
        postal_code=parsed_lot.postal_code,
        address_text=None,
        appraisal_value=parsed_lot.appraisal_value,
        opening_bid=parsed_lot.starting_bid,
        deposit=parsed_lot.deposit,
        auction_date=parent_auction.closing_date,
        has_lots=False,
        lot_count=1,
        charges_text=parsed_lot.encumbrances_summary,
        occupancy_text=parsed_lot.occupancy_status,
        is_detail_complete=all(
            value is not None
            for value in (parsed_lot.appraisal_value, parsed_lot.starting_bid, parsed_lot.deposit)
        ),
        parser_warnings=[],
    )
    return record if record.postal_code is not None else record.model_copy(update={"postal_code": infer_postal_code(record)})


def evaluate_opportunity(record: AuctionRecord) -> OpportunityEvaluation:
    """Evaluate one business record with filters, rules, and scoring."""
    derivations = build_record_derivations(record)
    applied_filters = collect_filter_reasons(record, derivations)
    rule_results = [] if applied_filters else evaluate_rules(record, derivations)
    return build_evaluation(record, derivations, applied_filters, rule_results)


def evaluate_parsed_auction(parsed_auction: Auction) -> OpportunityEvaluation:
    """Evaluate one parsed auction using the explicit adapter boundary."""
    return evaluate_opportunity(build_auction_record(parsed_auction))


def evaluate_parsed_lots(parent_auction: Auction, parsed_lots: Iterable[ParsedBoeLot]) -> list[OpportunityEvaluation]:
    """Evaluate parsed lots as separate opportunity units."""
    return [
        evaluate_opportunity(build_auction_lot_record(parent_auction, parsed_lot))
        for parsed_lot in parsed_lots
    ]


def evaluate_auction_or_lots(
    parsed_auction: Auction,
    parsed_lots: Iterable[ParsedBoeLot] | None = None,
) -> list[OpportunityEvaluation]:
    """Evaluate a whole auction or each lot, depending on what is available."""
    if parsed_lots is None:
        return [evaluate_parsed_auction(parsed_auction)]

    parsed_lot_list = list(parsed_lots)
    if not parsed_lot_list:
        return [evaluate_parsed_auction(parsed_auction)]

    return evaluate_parsed_lots(parsed_auction, parsed_lot_list)


def evaluate_parsed_auctions(parsed_auctions: Iterable[Auction]) -> list[OpportunityEvaluation]:
    """Evaluate a collection of parsed auctions and return them sorted by score."""
    evaluations = [evaluate_parsed_auction(parsed_auction) for parsed_auction in parsed_auctions]
    return sorted(evaluations, key=lambda evaluation: evaluation.score, reverse=True)


def _is_detail_complete(parsed_auction: Auction) -> bool:
    """Approximate detail completeness from currently available parsed fields."""
    return all(
        value is not None
        for value in (
            parsed_auction.appraisal_value,
            parsed_auction.starting_bid,
            parsed_auction.deposit,
        )
    )


def _infer_lot_number(external_id: str | None) -> int | None:
    """Infer lot number from the synthetic lot identity when available."""
    if external_id is None or "::lot:" not in external_id:
        return None
    try:
        return int(external_id.rsplit("::lot:", maxsplit=1)[-1])
    except ValueError:
        return None
