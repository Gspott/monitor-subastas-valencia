"""Backfill municipality values in SQLite from postal code."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.backfill import (
    audit_postal_code_municipality_variants,
    audit_municipality_backfill,
    backfill_municipality_from_postal_code,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the municipality backfill."""
    parser = argparse.ArgumentParser(
        description="Backfill municipality values using the shared postal-code normalization.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the municipality changes without writing to SQLite (default).",
    )
    mode_group.add_argument(
        "--apply",
        action="store_true",
        help="Apply the municipality changes to SQLite.",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Show a grouped audit of all potential changes without writing to SQLite.",
    )
    parser.add_argument(
        "--table",
        choices=["active", "upcoming", "completed", "all"],
        default="all",
        help="Limit the backfill to one dataset table or run across all tables.",
    )
    parser.add_argument(
        "--postal-code",
        help="Filter audit output to one exact five-digit postal code.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the municipality backfill in dry-run or apply mode."""
    args = parse_args()
    if args.postal_code and not args.audit:
        raise SystemExit("--postal-code requires --audit.")

    if args.audit:
        if args.postal_code:
            result = audit_postal_code_municipality_variants(
                postal_code=args.postal_code,
                table=args.table,
            )
            print(result.to_console_text())
            return
        result = audit_municipality_backfill(table=args.table)
        print(result.to_console_text())
        return

    result = backfill_municipality_from_postal_code(
        dry_run=not args.apply,
        table=args.table,
    )
    print(result.to_console_text())


if __name__ == "__main__":
    main()
