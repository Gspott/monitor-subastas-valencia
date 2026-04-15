"""Main entry point for the monitor application."""

import argparse
import logging

from .audit import (
    audit_dedupe_collisions,
    export_audit_result_to_csv,
    export_detailed_audit_result_to_csv,
)
from .backfill import backfill_official_status
from .dedupe import build_dedupe_key, dedupe_auctions
from .exports import export_all_active_valencia, export_new_auctions
from .normalize import normalize_auctions
from .scoring import score_auctions
from .sources.boe import run_boe_source
from .storage import fetch_active_valencia_auctions, fetch_all_auctions, init_db, upsert_auction


logger = logging.getLogger(__name__)


def main() -> None:
    """Initialize the database, run BOE ingestion, and persist auctions."""
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.backfill_status:
        result = backfill_official_status(dry_run=not args.apply)
        print(result.to_console_text())
        logger.info(
            "Backfill summary: scanned=%s changed=%s dry_run=%s",
            result.total_rows_scanned,
            result.total_rows_changed,
            result.dry_run,
        )
        return

    if args.audit_dedupe:
        result = audit_dedupe_collisions()
        print(result.to_console_text())
        if args.export_audit:
            audit_export_path = export_audit_result_to_csv(result)
            logger.info("Exported dedupe audit to %s.", audit_export_path)
        if args.export_audit_detailed:
            detailed_audit_export_path = export_detailed_audit_result_to_csv(result)
            logger.info("Exported detailed dedupe audit to %s.", detailed_audit_export_path)
        return

    # Inicializar la base de datos solo antes del pipeline de ingesta normal.
    init_db()
    logger.info("Database initialized successfully.")

    existing_identity_keys = collect_existing_identity_keys()

    fetched_auctions = run_boe_source()
    normalized_auctions = normalize_auctions(fetched_auctions)
    deduped_auctions = dedupe_auctions(normalized_auctions)
    scored_auctions = score_auctions(deduped_auctions)
    new_auctions = [
        auction for auction in scored_auctions
        if build_dedupe_key(auction) not in existing_identity_keys
    ]

    for auction in scored_auctions:
        upsert_auction(auction)

    if args.export:
        new_export_path = export_new_auctions(new_auctions)
        active_auctions = fetch_active_valencia_auctions()
        active_export_path = export_all_active_valencia(active_auctions)
        logger.info("Exported current run auctions to %s.", new_export_path)
        logger.info("Exported active Valencia auctions to %s.", active_export_path)

    logger.info(
        "Stored %s BOE auctions in SQLite after normalization, deduplication, and scoring.",
        len(scored_auctions),
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the monitor entry point."""
    parser = argparse.ArgumentParser(description="Monitor Valencia auctions from official sources.")
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export processed results to CSV files in the data directory.",
    )
    parser.add_argument(
        "--backfill-status",
        action="store_true",
        help="Run an optional backfill that normalizes stored official_status values.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes for commands that default to dry-run mode.",
    )
    parser.add_argument(
        "--audit-dedupe",
        action="store_true",
        help="Run an optional read-only audit for suspicious historical dedupe collisions.",
    )
    parser.add_argument(
        "--export-audit",
        action="store_true",
        help="Export dedupe audit examples to CSV when running --audit-dedupe.",
    )
    parser.add_argument(
        "--export-audit-detailed",
        action="store_true",
        help="Export one CSV row per suspicious record when running --audit-dedupe.",
    )
    return parser.parse_args()


def collect_existing_identity_keys() -> set[str]:
    """Collect existing dedupe identities before the current ingestion run."""
    return {
        identity_key
        for auction in fetch_all_auctions()
        for identity_key in [build_dedupe_key(auction)]
        if identity_key is not None
    }


if __name__ == "__main__":
    main()
