"""Simple Streamlit dashboard for exploring monitor results."""

from __future__ import annotations

import csv
import io
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path


# Permitir ejecutar la app desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    import streamlit as st
except ImportError as exc:  # pragma: no cover - flujo manual para la UI
    raise SystemExit(
        "Streamlit is not installed. Install it with: pip install streamlit"
    ) from exc

from monitor.pipeline.ranking import (
    filter_actionable_opportunities,
    rank_opportunities,
)
from monitor.opportunities.analysis import (
    DEFAULT_MAX_COMPLETED_HISTORY,
    DEFAULT_MIN_HISTORY_SAMPLE_SIZE,
    build_display_location,
    build_active_history_context as shared_build_active_history_context,
    build_completed_history_signals as shared_build_completed_history_signals,
    build_history_confidence_label as shared_build_history_confidence_label,
    build_history_signal_map as shared_build_history_signal_map,
    build_historical_heat_label as shared_build_historical_heat_label,
    filter_top_opportunity_rows as shared_filter_top_opportunity_rows,
    is_top_opportunity_row as shared_is_top_opportunity_row,
    parse_display_ratio as shared_parse_display_ratio,
    resolve_active_history_signal as shared_resolve_active_history_signal,
    select_recent_completed_history_rows as shared_select_recent_completed_history_rows,
)
from monitor.storage import (
    fetch_all_auctions,
    fetch_all_completed_auctions,
    fetch_all_upcoming_auctions,
)


DEFAULT_CATEGORIES = ["high_interest", "review"]
DEFAULT_TOP_N = 25
DEFAULT_MIN_SCORE = 50
DEFAULT_UPCOMING_TOP_N = 100
DEFAULT_COMPLETED_TOP_N = 100
MIN_HISTORY_SAMPLE_SIZE = DEFAULT_MIN_HISTORY_SAMPLE_SIZE
MAX_COMPLETED_HISTORY = DEFAULT_MAX_COMPLETED_HISTORY


def main() -> None:
    """Render a lightweight dashboard for manual result exploration."""
    st.set_page_config(page_title="Monitor Dashboard", layout="wide")
    st.title("Monitor Subastas Valencia")
    st.caption("Visual review of active auctions, upcoming watchlists, and completed-auction outcomes from the local SQLite database.")

    dataset = render_dataset_selector()
    if dataset == "active":
        render_active_dashboard()
        return
    if dataset == "upcoming":
        render_upcoming_dashboard()
        return

    render_completed_dashboard()


def render_dataset_selector() -> str:
    """Render the dataset selector while keeping the backing tables separate."""
    return st.radio(
        "Dataset",
        options=["active", "upcoming", "completed"],
        horizontal=True,
    )


def render_active_dashboard() -> None:
    """Render the current active-auction workflow without changing its core behavior."""
    auctions = fetch_all_auctions()
    completed_auctions = fetch_all_completed_auctions()
    evaluations = rank_opportunities(auctions)

    if not evaluations:
        st.warning("No active auctions found in the local SQLite database.")
        st.info("Load some active data first with `python scripts/load_sample_boe_data.py`.")
        return

    controls = render_active_controls()
    actionable = filter_actionable_opportunities(
        evaluations,
        categories=controls["selected_categories"],
        min_score=controls["min_score"],
    )
    completed_history_rows = select_recent_completed_history_rows(
        build_completed_table_rows(completed_auctions),
        max_rows=MAX_COMPLETED_HISTORY,
    )
    historical_signals = build_completed_history_signals(
        completed_history_rows
    )
    table_rows = build_active_table_rows(
        actionable,
        historical_signals=historical_signals,
    )
    filtered_rows = apply_active_dashboard_filters(
        table_rows,
        show_only_lots=controls["show_only_lots"],
        only_with_price_ratio=controls["only_with_price_ratio"],
        show_only_top_opportunities=controls["show_only_top_opportunities"],
    )
    sorted_rows = sort_active_table_rows(filtered_rows, sort_by=controls["sort_by"])
    limited_rows = sorted_rows[: controls["top_n"]]

    render_active_metrics(
        total_ranked=len(evaluations),
        total_actionable=len(actionable),
        total_displayed=len(limited_rows),
        total_top_opportunities=sum(1 for row in table_rows if row["_is_top_opportunity"]),
    )
    render_export_button(
        limited_rows,
        file_name="dashboard_active_filtered_opportunities.csv",
    )
    render_active_table(limited_rows, sort_by=controls["sort_by"])


def render_upcoming_dashboard() -> None:
    """Render an upcoming-opening watchlist backed by the dedicated SQLite table."""
    auctions = fetch_all_upcoming_auctions()
    if not auctions:
        st.warning("No upcoming auctions found in the local SQLite database.")
        st.info("Load upcoming data first with `python scripts/load_upcoming_boe_data.py`.")
        return

    controls = render_upcoming_controls()
    table_rows = build_upcoming_table_rows(auctions)
    filtered_rows = apply_upcoming_dashboard_filters(
        table_rows,
        only_with_price_ratio=controls["only_with_price_ratio"],
    )
    sorted_rows = sort_upcoming_table_rows(filtered_rows, sort_by=controls["sort_by"])
    limited_rows = sorted_rows[: controls["top_n"]]

    render_upcoming_metrics(
        total_upcoming=len(auctions),
        total_with_price_ratio=sum(1 for row in table_rows if row["_has_price_ratio"]),
        total_displayed=len(limited_rows),
    )
    render_export_button(
        limited_rows,
        file_name="dashboard_upcoming_watchlist.csv",
    )
    render_upcoming_table(limited_rows, sort_by=controls["sort_by"])


def render_completed_dashboard() -> None:
    """Render a completed-auctions analysis table backed by the dedicated SQLite table."""
    auctions = fetch_all_completed_auctions()
    if not auctions:
        st.warning("No completed auctions found in the local SQLite database.")
        st.info("Load completed data first with `python scripts/load_completed_boe_data.py`.")
        return

    controls = render_completed_controls()
    table_rows = build_completed_table_rows(auctions)
    filtered_rows = apply_completed_dashboard_filters(
        table_rows,
        only_with_final_bid=controls["only_with_final_bid"],
    )
    sorted_rows = sort_completed_table_rows(filtered_rows, sort_by=controls["sort_by"])
    limited_rows = sorted_rows[: controls["top_n"]]

    render_completed_metrics(
        total_completed=len(auctions),
        total_with_final_bid=sum(1 for row in table_rows if row["_has_final_bid"]),
        total_displayed=len(limited_rows),
    )
    render_completed_analytics(filtered_rows)
    render_export_button(
        limited_rows,
        file_name="dashboard_completed_analysis.csv",
    )
    render_completed_table(limited_rows, sort_by=controls["sort_by"])


def render_active_controls() -> dict[str, object]:
    """Render the top control bar for active auctions."""
    category_options = ["high_interest", "review", "discard"]
    col1, col2, col3 = st.columns([2, 2, 1])
    col4, col5, col6, col7 = st.columns([1, 1, 1, 1])

    with col1:
        min_score = st.slider("Minimum score", min_value=0, max_value=100, value=DEFAULT_MIN_SCORE)
    with col2:
        selected_categories = st.multiselect(
            "Categories",
            options=category_options,
            default=DEFAULT_CATEGORIES,
        )
    with col3:
        top_n = st.number_input(
            "Top N",
            min_value=1,
            max_value=500,
            value=DEFAULT_TOP_N,
            step=1,
        )
    with col4:
        show_only_lots = st.checkbox("Show only lots", value=False)
    with col5:
        only_with_price_ratio = st.checkbox("Only with price ratio", value=False)
    with col6:
        sort_by = st.selectbox(
            "Sort by",
            options=["score", "opening_bid_ratio", "auction"],
            index=0,
        )
    with col7:
        show_only_top_opportunities = st.checkbox("Show only top opportunities", value=False)

    return {
        "min_score": int(min_score),
        "selected_categories": list(selected_categories),
        "top_n": int(top_n),
        "show_only_lots": show_only_lots,
        "only_with_price_ratio": only_with_price_ratio,
        "sort_by": sort_by,
        "show_only_top_opportunities": show_only_top_opportunities,
    }


def render_upcoming_controls() -> dict[str, object]:
    """Render the top control bar for upcoming watchlist rows."""
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        only_with_price_ratio = st.checkbox("Only with price ratio", value=False)
    with col2:
        sort_by = st.selectbox(
            "Sort by",
            options=["opening_date", "opening_bid_ratio", "auction_id"],
            index=0,
        )
    with col3:
        top_n = st.number_input(
            "Top N",
            min_value=1,
            max_value=1000,
            value=DEFAULT_UPCOMING_TOP_N,
            step=1,
        )

    return {
        "only_with_price_ratio": only_with_price_ratio,
        "sort_by": sort_by,
        "top_n": int(top_n),
    }


def render_completed_controls() -> dict[str, object]:
    """Render the top control bar for completed-auction analysis rows."""
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        only_with_final_bid = st.checkbox("Only with final bid", value=False)
    with col2:
        sort_by = st.selectbox(
            "Sort by",
            options=[
                "closing_date",
                "final_bid_ratio_vs_appraisal",
                "final_bid_ratio_vs_starting_bid",
                "current_bid",
                "postal_code",
                "auction_id",
            ],
            index=0,
        )
    with col3:
        top_n = st.number_input(
            "Top N",
            min_value=1,
            max_value=1000,
            value=DEFAULT_COMPLETED_TOP_N,
            step=1,
        )

    return {
        "only_with_final_bid": only_with_final_bid,
        "sort_by": sort_by,
        "top_n": int(top_n),
    }


def render_active_metrics(
    *,
    total_ranked: int,
    total_actionable: int,
    total_displayed: int,
    total_top_opportunities: int,
) -> None:
    """Render compact summary metrics above the active results table."""
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total ranked auctions", total_ranked)
    col2.metric("Filtered actionable auctions", total_actionable)
    col3.metric("Rows displayed", total_displayed)
    col4.metric("Top opportunities in view", total_top_opportunities)


def render_upcoming_metrics(*, total_upcoming: int, total_with_price_ratio: int, total_displayed: int) -> None:
    """Render compact summary metrics above the upcoming watchlist table."""
    col1, col2, col3 = st.columns(3)
    col1.metric("Total upcoming auctions", total_upcoming)
    col2.metric("Rows with price ratio", total_with_price_ratio)
    col3.metric("Rows displayed", total_displayed)


def render_completed_metrics(*, total_completed: int, total_with_final_bid: int, total_displayed: int) -> None:
    """Render compact summary metrics above the completed analysis table."""
    col1, col2, col3 = st.columns(3)
    col1.metric("Total completed auctions", total_completed)
    col2.metric("Rows with final bid", total_with_final_bid)
    col3.metric("Rows displayed", total_displayed)


def render_completed_analytics(rows: list[dict[str, object]]) -> None:
    """Render a compact analytical summary for completed auctions."""
    summary = build_completed_summary(rows)

    st.subheader("Completed Analytics")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Completed rows in view", summary["total_completed_rows"])
    col2.metric("Rows with current bid", summary["rows_with_current_bid"])
    col3.metric("Rows without current bid", summary["rows_without_current_bid"])
    col4.metric("No-bid rate", summary["no_bid_rate"])

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Rows with valid final/starting ratio", summary["rows_with_final_bid_ratio_vs_starting_bid"])
    col6.metric("Avg final/starting ratio", summary["average_final_bid_ratio_vs_starting_bid"])
    col7.metric("Rows with valid final/appraisal ratio", summary["rows_with_final_bid_ratio_vs_appraisal"])
    col8.metric("Avg final/appraisal ratio", summary["average_final_bid_ratio_vs_appraisal"])

    col9, col10 = st.columns(2)
    col9.metric("Max final/starting ratio", summary["max_final_bid_ratio_vs_starting_bid"])
    col10.metric("Max final/appraisal ratio", summary["max_final_bid_ratio_vs_appraisal"])

    st.caption("Grouped summaries use only rows with a valid final bid ratio vs starting bid.")
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    with summary_col1:
        st.write("By asset type")
        st.dataframe(
            build_group_summary_rows(rows, group_key="asset_type"),
            use_container_width=True,
            hide_index=True,
        )
    with summary_col2:
        st.write("By municipality")
        st.dataframe(
            build_group_summary_rows(rows, group_key="municipality"),
            column_order=[
                "municipality",
                "count",
                "rows_with_bid",
                "rows_without_bid",
                "no_bid_rate",
                "average_final_bid_ratio_vs_starting_bid",
            ],
            use_container_width=True,
            hide_index=True,
        )
    with summary_col3:
        st.write("By postal code")
        st.dataframe(
            build_group_summary_rows(rows, group_key="postal_code"),
            column_order=[
                "postal_code",
                "count",
                "rows_with_bid",
                "rows_without_bid",
                "no_bid_rate",
                "average_final_bid_ratio_vs_starting_bid",
            ],
            use_container_width=True,
            hide_index=True,
        )


def render_export_button(rows: list[dict[str, object]], *, file_name: str) -> None:
    """Render a CSV export button for the currently filtered subset."""
    st.download_button(
        "Export filtered CSV",
        data=build_csv_bytes(rows),
        file_name=file_name,
        mime="text/csv",
    )


def render_active_table(rows: list[dict[str, object]], *, sort_by: str) -> None:
    """Render the active-auction table."""
    st.subheader("Ranked Opportunities")
    st.write(build_active_table_caption(row_count=len(rows), sort_by=sort_by))

    st.dataframe(
        [project_visible_row(row) for row in rows],
        use_container_width=True,
        hide_index=True,
        column_config={
            "auction_lot_id": st.column_config.TextColumn("Auction / Lot", width="medium"),
            "auction_id": st.column_config.TextColumn("Auction ID", width="medium"),
            "lot_number": st.column_config.NumberColumn("Lot", format="%d"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "municipality": st.column_config.TextColumn("Municipality"),
            "location": st.column_config.TextColumn("Location"),
            "postal_code": st.column_config.TextColumn("Postal Code"),
            "asset_type": st.column_config.TextColumn("Asset Type"),
            "appraisal_value": st.column_config.TextColumn("Appraisal Value"),
            "opening_bid": st.column_config.TextColumn("Opening Bid"),
            "opening_bid_ratio": st.column_config.TextColumn("Opening Bid Ratio"),
            "has_price_data": st.column_config.TextColumn("Has Price Data"),
            "score": st.column_config.NumberColumn("Score", format="%d"),
            "category": st.column_config.TextColumn("Category"),
            "historical_no_bid_rate": st.column_config.TextColumn("Historical No-bid Rate"),
            "historical_avg_final_ratio_vs_starting_bid": st.column_config.TextColumn("Historical Avg Final/Starting"),
            "historical_sample_size": st.column_config.NumberColumn("Historical Sample", format="%d"),
            "historical_confidence": st.column_config.TextColumn("Historical Confidence"),
            "historical_heat_label": st.column_config.TextColumn("Historical Heat"),
            "primary_positive_reason": st.column_config.TextColumn("Primary Positive Reason", width="large"),
            "primary_negative_reason": st.column_config.TextColumn("Primary Negative Reason", width="large"),
            "source_url": st.column_config.LinkColumn("Source URL", display_text="open"),
        },
    )


def render_upcoming_table(rows: list[dict[str, object]], *, sort_by: str) -> None:
    """Render the upcoming watchlist table."""
    st.subheader("Upcoming Watchlist")
    st.write(build_upcoming_table_caption(row_count=len(rows), sort_by=sort_by))

    st.dataframe(
        [project_visible_upcoming_row(row) for row in rows],
        use_container_width=True,
        hide_index=True,
        column_config={
            "auction_lot_id": st.column_config.TextColumn("Auction / Lot", width="medium"),
            "lot_number": st.column_config.NumberColumn("Lot", format="%d"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "municipality": st.column_config.TextColumn("Municipality"),
            "asset_type": st.column_config.TextColumn("Asset Type"),
            "official_status": st.column_config.TextColumn("Status"),
            "opening_date": st.column_config.TextColumn("Opening Date"),
            "closing_date": st.column_config.TextColumn("Closing Date"),
            "appraisal_value": st.column_config.TextColumn("Appraisal Value"),
            "opening_bid": st.column_config.TextColumn("Opening Bid"),
            "opening_bid_ratio": st.column_config.TextColumn("Opening Bid Ratio"),
            "has_price_data": st.column_config.TextColumn("Has Price Data"),
            "source_url": st.column_config.LinkColumn("Source URL", display_text="open"),
        },
    )


def render_completed_table(rows: list[dict[str, object]], *, sort_by: str) -> None:
    """Render the completed-auction analysis table."""
    st.subheader("Completed Auctions Analysis")
    st.write(build_completed_table_caption(row_count=len(rows), sort_by=sort_by))

    st.dataframe(
        [project_visible_completed_row(row) for row in rows],
        use_container_width=True,
        hide_index=True,
        column_config={
            "auction_lot_id": st.column_config.TextColumn("Auction / Lot", width="medium"),
            "lot_number": st.column_config.NumberColumn("Lot", format="%d"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "municipality": st.column_config.TextColumn("Municipality"),
            "postal_code": st.column_config.TextColumn("Postal Code"),
            "official_status": st.column_config.TextColumn("Status"),
            "opening_date": st.column_config.TextColumn("Opening Date"),
            "closing_date": st.column_config.TextColumn("Closing Date"),
            "opening_bid": st.column_config.TextColumn("Opening Bid"),
            "appraisal_value": st.column_config.TextColumn("Appraisal Value"),
            "current_bid": st.column_config.TextColumn("Current Bid"),
            "deposit": st.column_config.TextColumn("Deposit"),
            "final_bid_ratio_vs_appraisal": st.column_config.TextColumn("Final Bid / Appraisal"),
            "final_bid_ratio_vs_starting_bid": st.column_config.TextColumn("Final Bid / Starting"),
            "source_url": st.column_config.LinkColumn("Source URL", display_text="open"),
        },
    )


def build_active_table_rows(
    evaluations,
    *,
    historical_signals: dict[str, dict[str, dict[str, object]]] | None = None,
) -> list[dict[str, object]]:
    """Build table rows for the active dashboard without changing core models."""
    rows: list[dict[str, object]] = []
    historical_signals = historical_signals or {
        "municipality": {},
        "postal_code": {},
    }

    for evaluation in evaluations:
        opening_bid_ratio = evaluation.derivations.opening_bid_ratio
        has_price_data = (
            evaluation.record.opening_bid is not None
            and evaluation.record.opening_bid > 0
            and evaluation.record.appraisal_value is not None
            and evaluation.record.appraisal_value > 0
        )
        history_context = shared_build_active_history_context(
            municipality=evaluation.record.municipality,
            postal_code=evaluation.record.postal_code,
            opening_bid_ratio=opening_bid_ratio,
            has_price_data=has_price_data,
            historical_signals=historical_signals,
        )
        rows.append(
            {
                "auction_lot_id": build_auction_lot_id(
                    evaluation.record.auction_id,
                    evaluation.record.lot_number,
                ),
                "auction_id": evaluation.record.auction_id or "",
                "lot_number": evaluation.record.lot_number,
                "title": evaluation.record.title,
                "municipality": evaluation.record.municipality or "",
                "location": build_display_location(
                    municipality=evaluation.record.municipality,
                    postal_code=evaluation.record.postal_code,
                    province=evaluation.record.province,
                ),
                "postal_code": evaluation.record.postal_code or "",
                "asset_type": evaluation.record.asset_type,
                "appraisal_value": format_decimal(evaluation.record.appraisal_value),
                "opening_bid": format_decimal(evaluation.record.opening_bid),
                "opening_bid_ratio": format_ratio(opening_bid_ratio),
                "has_price_data": "yes" if has_price_data else "no",
                "score": evaluation.score,
                "category": evaluation.category,
                "primary_positive_reason": first_reason(evaluation.positive_reasons),
                "primary_negative_reason": first_reason(evaluation.negative_reasons),
                "historical_no_bid_rate": str(history_context["historical_no_bid_rate"]),
                "historical_avg_final_ratio_vs_starting_bid": str(
                    history_context["historical_avg_final_ratio_vs_starting_bid"]
                ),
                "historical_sample_size": history_context["historical_sample_size"],
                "historical_confidence": history_context["historical_confidence"],
                "historical_heat_label": history_context["historical_heat_label"],
                "source_url": evaluation.record.source_url or "",
                "_sort_opening_bid_ratio": float(opening_bid_ratio) if opening_bid_ratio is not None else None,
                "_has_price_ratio": opening_bid_ratio is not None,
                "_is_top_opportunity": shared_is_top_opportunity_row(history_context),
                "_auction_group_id": extract_parent_auction_id(evaluation.record.auction_id),
            }
        )

    return rows


def build_upcoming_table_rows(auctions) -> list[dict[str, object]]:
    """Build watchlist rows directly from upcoming auctions without mixing datasets."""
    rows: list[dict[str, object]] = []

    for auction in auctions:
        opening_bid_ratio = compute_opening_bid_ratio(
            opening_bid=auction.starting_bid,
            appraisal_value=auction.appraisal_value,
        )
        rows.append(
            {
                "auction_lot_id": build_auction_lot_id(auction.external_id, extract_lot_number(auction.external_id)),
                "auction_id": auction.external_id or "",
                "lot_number": extract_lot_number(auction.external_id),
                "title": auction.title,
                "municipality": auction.municipality or "",
                "postal_code": auction.postal_code or "",
                "asset_type": auction.asset_class or "",
                "official_status": auction.official_status or "",
                "opening_date": format_date(auction.opening_date),
                "closing_date": format_date(auction.closing_date),
                "appraisal_value": format_decimal(auction.appraisal_value),
                "opening_bid": format_decimal(auction.starting_bid),
                "opening_bid_ratio": format_ratio(opening_bid_ratio),
                "has_price_data": "yes" if has_price_data(auction.starting_bid, auction.appraisal_value) else "no",
                "source_url": auction.official_url or "",
                "_sort_opening_bid_ratio": float(opening_bid_ratio) if opening_bid_ratio is not None else None,
                "_has_price_ratio": opening_bid_ratio is not None,
                "_sort_opening_date": auction.opening_date or date.max,
                "_auction_group_id": extract_parent_auction_id(auction.external_id),
            }
        )

    return rows


def build_completed_table_rows(auctions) -> list[dict[str, object]]:
    """Build analysis rows directly from completed auctions without mixing datasets."""
    rows: list[dict[str, object]] = []

    for auction in auctions:
        lot_number = extract_lot_number(auction.external_id)
        final_bid_ratio_vs_appraisal = compute_ratio(
            numerator=auction.current_bid,
            denominator=auction.appraisal_value,
        )
        final_bid_ratio_vs_starting_bid = compute_ratio(
            numerator=auction.current_bid,
            denominator=auction.starting_bid,
        )
        rows.append(
            {
                "auction_lot_id": build_auction_lot_id(auction.external_id, lot_number),
                "auction_id": auction.external_id or "",
                "lot_number": lot_number,
                "title": auction.title,
                "municipality": auction.municipality or "",
                "postal_code": auction.postal_code or "",
                "asset_type": auction.asset_class or "",
                "official_status": auction.official_status or "",
                "opening_date": format_date(auction.opening_date),
                "closing_date": format_date(auction.closing_date),
                "opening_bid": format_decimal(auction.starting_bid),
                "appraisal_value": format_decimal(auction.appraisal_value),
                "current_bid": format_decimal(auction.current_bid),
                "deposit": format_decimal(auction.deposit),
                "final_bid_ratio_vs_appraisal": format_ratio(final_bid_ratio_vs_appraisal),
                "final_bid_ratio_vs_starting_bid": format_ratio(final_bid_ratio_vs_starting_bid),
                "source_url": auction.official_url or "",
                "_sort_closing_date": auction.closing_date or date.max,
                "_has_closing_date": auction.closing_date is not None,
                "_sort_current_bid": float(auction.current_bid) if auction.current_bid is not None else None,
                "_sort_postal_code": auction.postal_code or "",
                "_sort_final_bid_ratio_vs_appraisal": (
                    float(final_bid_ratio_vs_appraisal) if final_bid_ratio_vs_appraisal is not None else None
                ),
                "_sort_final_bid_ratio_vs_starting_bid": (
                    float(final_bid_ratio_vs_starting_bid) if final_bid_ratio_vs_starting_bid is not None else None
                ),
                "_has_final_bid": auction.current_bid is not None and auction.current_bid > 0,
                "_final_bid_ratio_vs_appraisal": final_bid_ratio_vs_appraisal,
                "_final_bid_ratio_vs_starting_bid": final_bid_ratio_vs_starting_bid,
                "_auction_group_id": extract_parent_auction_id(auction.external_id),
            }
        )

    return rows


def build_completed_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    """Build aggregate metrics for the completed dataset view."""
    rows_with_current_bid = sum(1 for row in rows if row["_has_final_bid"])
    total_rows = len(rows)
    valid_starting_ratios = [
        row["_final_bid_ratio_vs_starting_bid"]
        for row in rows
        if row["_final_bid_ratio_vs_starting_bid"] is not None
    ]
    valid_appraisal_ratios = [
        row["_final_bid_ratio_vs_appraisal"]
        for row in rows
        if row["_final_bid_ratio_vs_appraisal"] is not None
    ]

    return {
        "total_completed_rows": total_rows,
        "rows_with_current_bid": rows_with_current_bid,
        "rows_without_current_bid": total_rows - rows_with_current_bid,
        "no_bid_rate": format_ratio(compute_fraction(total_rows - rows_with_current_bid, total_rows)),
        "rows_with_final_bid_ratio_vs_starting_bid": len(valid_starting_ratios),
        "rows_with_final_bid_ratio_vs_appraisal": len(valid_appraisal_ratios),
        "average_final_bid_ratio_vs_starting_bid": format_ratio(average_optional(valid_starting_ratios)),
        "average_final_bid_ratio_vs_appraisal": format_ratio(average_optional(valid_appraisal_ratios)),
        "max_final_bid_ratio_vs_starting_bid": format_ratio(max_optional(valid_starting_ratios)),
        "max_final_bid_ratio_vs_appraisal": format_ratio(max_optional(valid_appraisal_ratios)),
    }


def build_completed_history_signals(
    rows: list[dict[str, object]],
    *,
    min_sample_size: int = MIN_HISTORY_SAMPLE_SIZE,
) -> dict[str, dict[str, dict[str, object]]]:
    """Build reusable historical signals from completed rows for active-auction context."""
    return shared_build_completed_history_signals(
        rows,
        min_sample_size=min_sample_size,
    )


def build_history_signal_map(
    rows: list[dict[str, object]],
    *,
    group_key: str,
    min_sample_size: int,
) -> dict[str, dict[str, object]]:
    """Build one grouped history map with conservative sample-size filtering."""
    return shared_build_history_signal_map(
        rows,
        group_key=group_key,
        min_sample_size=min_sample_size,
    )


def resolve_active_history_signal(
    *,
    municipality: str | None,
    postal_code: str | None,
    historical_signals: dict[str, dict[str, dict[str, object]]],
) -> dict[str, object]:
    """Resolve the best available historical signal for one active row."""
    return shared_resolve_active_history_signal(
        municipality=municipality,
        postal_code=postal_code,
        historical_signals=historical_signals,
    )


def build_historical_heat_label(
    *,
    no_bid_rate: Decimal | None,
    avg_final_ratio_vs_starting_bid: Decimal | None,
    sample_size: int,
    min_sample_size: int = MIN_HISTORY_SAMPLE_SIZE,
) -> str:
    """Build a lightweight market-heat label from completed-auction history."""
    return shared_build_historical_heat_label(
        no_bid_rate=no_bid_rate,
        avg_final_ratio_vs_starting_bid=avg_final_ratio_vs_starting_bid,
        sample_size=sample_size,
        min_sample_size=min_sample_size,
    )


def build_history_confidence_label(
    *,
    sample_size: int,
    min_sample_size: int = MIN_HISTORY_SAMPLE_SIZE,
) -> str:
    """Build a simple confidence label from historical sample size."""
    return shared_build_history_confidence_label(
        sample_size=sample_size,
        min_sample_size=min_sample_size,
    )


def select_recent_completed_history_rows(
    rows: list[dict[str, object]],
    *,
    max_rows: int = MAX_COMPLETED_HISTORY,
) -> list[dict[str, object]]:
    """Keep only recent completed rows with a valid closing date for history signals."""
    return shared_select_recent_completed_history_rows(
        rows,
        max_rows=max_rows,
    )


def build_group_summary_rows(rows: list[dict[str, object]], *, group_key: str) -> list[dict[str, object]]:
    """Build small grouped summaries for completed rows."""
    grouped: dict[str, dict[str, object]] = {}
    for row in rows:
        group_value = str(row.get(group_key) or "-")
        bucket = grouped.setdefault(
            group_value,
            {
                "count": 0,
                "rows_with_bid": 0,
                "rows_without_bid": 0,
                "ratios": [],
            },
        )
        bucket["count"] += 1
        if row["_has_final_bid"]:
            bucket["rows_with_bid"] += 1
        else:
            bucket["rows_without_bid"] += 1
        ratio = row["_final_bid_ratio_vs_starting_bid"]
        if ratio is not None:
            bucket["ratios"].append(ratio)

    summary_rows = [
        {
            group_key: group_value,
            "count": int(metrics["count"]),
            "rows_with_bid": int(metrics["rows_with_bid"]),
            "rows_without_bid": int(metrics["rows_without_bid"]),
            "no_bid_rate": format_ratio(
                compute_fraction(int(metrics["rows_without_bid"]), int(metrics["count"]))
            ),
            "average_final_bid_ratio_vs_starting_bid": format_ratio(
                average_optional(metrics["ratios"])
            ),
        }
        for group_value, metrics in grouped.items()
    ]
    if group_key in {"municipality", "postal_code"}:
        return sorted(
            summary_rows,
            key=lambda row: (
                -(float(row["no_bid_rate"]) if row["no_bid_rate"] != "-" else -1.0),
                -int(row["count"]),
                str(row[group_key]),
            ),
        )[:15]
    return sorted(summary_rows, key=lambda row: (-int(row["count"]), str(row[group_key])))[:10]


def compute_fraction(numerator: int, denominator: int) -> Decimal | None:
    """Compute a safe fraction for dashboard analytics."""
    if denominator <= 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def parse_display_ratio(value: str) -> Decimal | None:
    """Parse a compact dashboard ratio string back into a Decimal when possible."""
    return shared_parse_display_ratio(value)


def apply_active_dashboard_filters(
    rows: list[dict[str, object]],
    *,
    show_only_lots: bool,
    only_with_price_ratio: bool,
    show_only_top_opportunities: bool,
) -> list[dict[str, object]]:
    """Apply UI-only filters without touching the evaluation pipeline."""
    filtered_rows = rows
    if show_only_lots:
        filtered_rows = [row for row in filtered_rows if row["lot_number"] is not None]
    if only_with_price_ratio:
        filtered_rows = [row for row in filtered_rows if row["_has_price_ratio"]]
    if show_only_top_opportunities:
        filtered_rows = [row for row in filtered_rows if row["_is_top_opportunity"]]
    return filtered_rows


def apply_upcoming_dashboard_filters(
    rows: list[dict[str, object]],
    *,
    only_with_price_ratio: bool,
) -> list[dict[str, object]]:
    """Apply UI-only filters for the upcoming watchlist."""
    if only_with_price_ratio:
        return [row for row in rows if row["_has_price_ratio"]]
    return rows


def apply_completed_dashboard_filters(
    rows: list[dict[str, object]],
    *,
    only_with_final_bid: bool,
) -> list[dict[str, object]]:
    """Apply UI-only filters for the completed analysis view."""
    if only_with_final_bid:
        return [row for row in rows if row["_has_final_bid"]]
    return rows


def sort_active_table_rows(rows: list[dict[str, object]], *, sort_by: str) -> list[dict[str, object]]:
    """Sort active rows according to the selected dashboard view."""
    if sort_by == "opening_bid_ratio":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_opening_bid_ratio"] is None,
                row["_sort_opening_bid_ratio"] if row["_sort_opening_bid_ratio"] is not None else float("inf"),
                -int(row["score"]),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "auction":
        return sorted(
            rows,
            key=lambda row: (
                str(row["_auction_group_id"]),
                row["lot_number"] if row["lot_number"] is not None else -1,
                -int(row["score"]),
            ),
        )

    return sorted(
        rows,
        key=lambda row: (
            -int(row["score"]),
            row["_sort_opening_bid_ratio"] if row["_sort_opening_bid_ratio"] is not None else float("inf"),
            str(row["auction_lot_id"]),
        ),
    )


def sort_upcoming_table_rows(rows: list[dict[str, object]], *, sort_by: str) -> list[dict[str, object]]:
    """Sort upcoming rows according to the selected watchlist view."""
    if sort_by == "opening_bid_ratio":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_opening_bid_ratio"] is None,
                row["_sort_opening_bid_ratio"] if row["_sort_opening_bid_ratio"] is not None else float("inf"),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "auction_id":
        return sorted(
            rows,
            key=lambda row: (
                str(row["_auction_group_id"]),
                row["lot_number"] if row["lot_number"] is not None else -1,
                str(row["auction_lot_id"]),
            ),
        )

    return sorted(
        rows,
        key=lambda row: (
            row["_sort_opening_date"],
            row["_sort_opening_bid_ratio"] is None,
            row["_sort_opening_bid_ratio"] if row["_sort_opening_bid_ratio"] is not None else float("inf"),
            str(row["auction_lot_id"]),
        ),
    )


def sort_completed_table_rows(rows: list[dict[str, object]], *, sort_by: str) -> list[dict[str, object]]:
    """Sort completed rows according to the selected analysis view."""
    if sort_by == "final_bid_ratio_vs_appraisal":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_final_bid_ratio_vs_appraisal"] is None,
                row["_sort_final_bid_ratio_vs_appraisal"]
                if row["_sort_final_bid_ratio_vs_appraisal"] is not None else float("inf"),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "final_bid_ratio_vs_starting_bid":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_final_bid_ratio_vs_starting_bid"] is None,
                row["_sort_final_bid_ratio_vs_starting_bid"]
                if row["_sort_final_bid_ratio_vs_starting_bid"] is not None else float("inf"),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "current_bid":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_current_bid"] is None,
                -(row["_sort_current_bid"] if row["_sort_current_bid"] is not None else 0.0),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "postal_code":
        return sorted(
            rows,
            key=lambda row: (
                row["_sort_postal_code"] == "",
                str(row["_sort_postal_code"]),
                str(row["auction_lot_id"]),
            ),
        )

    if sort_by == "auction_id":
        return sorted(
            rows,
            key=lambda row: (
                str(row["_auction_group_id"]),
                row["lot_number"] if row["lot_number"] is not None else -1,
                str(row["auction_lot_id"]),
            ),
        )

    return sorted(
        rows,
        key=lambda row: (
            row["_sort_closing_date"],
            str(row["auction_lot_id"]),
        ),
    )


def project_visible_row(row: dict[str, object]) -> dict[str, object]:
    """Keep only the presentation columns visible in the active dataframe."""
    return {
        "auction_lot_id": row["auction_lot_id"],
        "auction_id": row["auction_id"],
        "lot_number": row["lot_number"],
        "title": row["title"],
        "municipality": row["municipality"],
        "location": row["location"],
        "postal_code": row["postal_code"],
        "asset_type": row["asset_type"],
        "appraisal_value": row["appraisal_value"],
        "opening_bid": row["opening_bid"],
        "opening_bid_ratio": row["opening_bid_ratio"],
        "has_price_data": row["has_price_data"],
        "score": row["score"],
        "category": row["category"],
        "historical_no_bid_rate": row["historical_no_bid_rate"],
        "historical_avg_final_ratio_vs_starting_bid": row["historical_avg_final_ratio_vs_starting_bid"],
        "historical_sample_size": row["historical_sample_size"],
        "historical_confidence": row["historical_confidence"],
        "historical_heat_label": row["historical_heat_label"],
        "primary_positive_reason": row["primary_positive_reason"],
        "primary_negative_reason": row["primary_negative_reason"],
        "source_url": row["source_url"],
    }


def project_visible_upcoming_row(row: dict[str, object]) -> dict[str, object]:
    """Keep only the presentation columns visible in the upcoming dataframe."""
    return {
        "auction_lot_id": row["auction_lot_id"],
        "lot_number": row["lot_number"],
        "title": row["title"],
        "municipality": row["municipality"],
        "asset_type": row["asset_type"],
        "official_status": row["official_status"],
        "opening_date": row["opening_date"],
        "closing_date": row["closing_date"],
        "appraisal_value": row["appraisal_value"],
        "opening_bid": row["opening_bid"],
        "opening_bid_ratio": row["opening_bid_ratio"],
        "has_price_data": row["has_price_data"],
        "source_url": row["source_url"],
    }


def project_visible_completed_row(row: dict[str, object]) -> dict[str, object]:
    """Keep only the presentation columns visible in the completed dataframe."""
    return {
        "auction_lot_id": row["auction_lot_id"],
        "lot_number": row["lot_number"],
        "title": row["title"],
        "municipality": row["municipality"],
        "postal_code": row["postal_code"],
        "official_status": row["official_status"],
        "opening_date": row["opening_date"],
        "closing_date": row["closing_date"],
        "opening_bid": row["opening_bid"],
        "appraisal_value": row["appraisal_value"],
        "current_bid": row["current_bid"],
        "deposit": row["deposit"],
        "final_bid_ratio_vs_appraisal": row["final_bid_ratio_vs_appraisal"],
        "final_bid_ratio_vs_starting_bid": row["final_bid_ratio_vs_starting_bid"],
        "source_url": row["source_url"],
    }


def is_top_opportunity_row(row: dict[str, object]) -> bool:
    """Apply a transparent top-opportunity heuristic on already built active rows."""
    return shared_is_top_opportunity_row(row)


def filter_top_opportunity_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Keep only rows that match the explicit top-opportunity heuristic."""
    return shared_filter_top_opportunity_rows(rows)


def build_auction_lot_id(auction_id: str | None, lot_number: int | None) -> str:
    """Build a compact identifier that makes lots easy to scan in the UI."""
    if auction_id is None:
        return ""
    if lot_number is None or auction_id.endswith(f"::lot:{lot_number}"):
        return auction_id
    return f"{auction_id}::lot:{lot_number}"


def extract_parent_auction_id(auction_id: str | None) -> str:
    """Extract the parent auction identifier for grouping purposes."""
    if auction_id is None:
        return ""
    if "::lot:" not in auction_id:
        return auction_id
    return auction_id.split("::lot:", maxsplit=1)[0]


def extract_lot_number(auction_id: str | None) -> int | None:
    """Extract the lot number from a combined auction identifier when present."""
    if auction_id is None or "::lot:" not in auction_id:
        return None

    lot_fragment = auction_id.split("::lot:", maxsplit=1)[1]
    if not lot_fragment.isdigit():
        return None

    return int(lot_fragment)


def build_csv_bytes(rows: list[dict[str, object]]) -> bytes:
    """Export the visible filtered subset to CSV directly from the dashboard view."""
    output = io.StringIO()
    fieldnames = list(rows[0].keys()) if rows else []
    fieldnames = [name for name in fieldnames if not name.startswith("_")]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: value for key, value in row.items() if not key.startswith("_")})
    return output.getvalue().encode("utf-8")


def build_active_table_caption(*, row_count: int, sort_by: str) -> str:
    """Build a short caption for the active table view."""
    if sort_by == "opening_bid_ratio":
        return f"Showing {row_count} rows sorted by opening bid ratio ascending."
    if sort_by == "auction":
        return f"Showing {row_count} rows grouped by auction and lot number."
    return f"Showing {row_count} rows sorted by score descending."


def build_upcoming_table_caption(*, row_count: int, sort_by: str) -> str:
    """Build a short caption for the upcoming watchlist view."""
    if sort_by == "opening_bid_ratio":
        return f"Showing {row_count} rows sorted by opening bid ratio ascending."
    if sort_by == "auction_id":
        return f"Showing {row_count} rows grouped by auction and lot number."
    return f"Showing {row_count} rows sorted by opening date ascending."


def build_completed_table_caption(*, row_count: int, sort_by: str) -> str:
    """Build a short caption for the completed analysis view."""
    if sort_by == "final_bid_ratio_vs_appraisal":
        return f"Showing {row_count} rows sorted by final bid ratio vs appraisal ascending."
    if sort_by == "final_bid_ratio_vs_starting_bid":
        return f"Showing {row_count} rows sorted by final bid ratio vs starting bid ascending."
    if sort_by == "current_bid":
        return f"Showing {row_count} rows sorted by current bid descending."
    if sort_by == "postal_code":
        return f"Showing {row_count} rows sorted by postal code ascending."
    if sort_by == "auction_id":
        return f"Showing {row_count} rows grouped by auction and lot number."
    return f"Showing {row_count} rows sorted by closing date ascending."


def first_reason(reasons: list[str]) -> str:
    """Return the first available reason or an empty string."""
    return reasons[0] if reasons else ""


def format_decimal(value) -> str:
    """Format decimal-like values for presentation."""
    if value is None:
        return "-"
    return format(value, "f")


def format_ratio(value) -> str:
    """Format price ratios in a compact way for fast visual comparison."""
    if value is None:
        return "-"
    return f"{float(value):.2f}"


def format_date(value: date | None) -> str:
    """Format optional dates in a consistent dashboard-friendly way."""
    if value is None:
        return "-"
    return value.isoformat()


def has_price_data(opening_bid, appraisal_value) -> bool:
    """Check whether a row has enough positive price data for ratios."""
    return (
        opening_bid is not None
        and opening_bid > 0
        and appraisal_value is not None
        and appraisal_value > 0
    )


def compute_opening_bid_ratio(*, opening_bid, appraisal_value):
    """Compute a comparable opening-bid ratio for watchlist rows."""
    if not has_price_data(opening_bid, appraisal_value):
        return None
    return opening_bid / appraisal_value


def compute_ratio(*, numerator, denominator):
    """Compute a safe ratio for completed-auction analysis."""
    if numerator is None or numerator <= 0:
        return None
    if denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def average_optional(values) -> object | None:
    """Compute an average when at least one valid value exists."""
    sequence = list(values)
    if not sequence:
        return None
    return sum(sequence) / len(sequence)


def max_optional(values) -> object | None:
    """Compute a max when at least one valid value exists."""
    sequence = list(values)
    if not sequence:
        return None
    return max(sequence)


if __name__ == "__main__":
    main()
