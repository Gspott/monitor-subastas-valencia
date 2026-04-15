"""Run the full local monitor cycle for active, upcoming, and completed datasets."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from scripts import (
    load_completed_boe_data,
    load_sample_boe_data,
    load_upcoming_boe_data,
    send_opportunities_telegram,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the full monitor cycle."""
    parser = argparse.ArgumentParser(
        description="Run the full local monitor cycle."
    )
    parser.add_argument(
        "--completed-full-refresh",
        action="store_true",
        help="Force a full historical refresh for completed auctions.",
    )
    return parser.parse_args()


def main(*, completed_full_refresh: bool = False) -> None:
    """Run the three BOE refresh steps and then send Telegram alerts."""
    started_at = datetime.now().isoformat(timespec="seconds")
    print(f"[START] Full monitor cycle started at {started_at}")

    run_cycle_step(
        label="active",
        step_main=load_sample_boe_data.main,
        start_message="[STEP] Refreshing active auctions...",
        done_message="[INFO] Active auctions refresh completed.",
    )
    run_cycle_step(
        label="upcoming",
        step_main=load_upcoming_boe_data.main,
        start_message="[STEP] Refreshing upcoming auctions...",
        done_message="[INFO] Upcoming auctions refresh completed.",
    )
    run_cycle_step(
        label="completed",
        step_main=lambda: load_completed_boe_data.main(
            full_refresh=completed_full_refresh,
        ),
        start_message="[STEP] Refreshing completed auctions...",
        done_message="[INFO] Completed auctions refresh completed.",
    )

    try:
        print("[STEP] Sending Telegram top opportunities...")
        send_opportunities_telegram.main()
        print("[INFO] Telegram step completed.")
    except Exception as exc:
        print(f"[ERROR] Telegram alerting failed: {exc}")
        print("[END] Full monitor cycle finished with Telegram error.")
        raise

    finished_at = datetime.now().isoformat(timespec="seconds")
    print(f"[END] Full monitor cycle finished at {finished_at}")


def run_cycle_step(
    *,
    label: str,
    step_main,
    start_message: str,
    done_message: str,
) -> None:
    """Run one cycle step and stop the whole flow if it fails."""
    print(start_message)
    try:
        step_main()
    except Exception as exc:
        print(f"[ERROR] {label} refresh failed: {exc}")
        print("[END] Full monitor cycle stopped before Telegram.")
        raise
    print(done_message)


if __name__ == "__main__":
    args = parse_args()
    main(completed_full_refresh=args.completed_full_refresh)
