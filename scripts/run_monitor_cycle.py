"""Run one full local monitor cycle."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.pipeline.ranking import rank_and_filter_opportunities
from monitor.storage import fetch_all_auctions
from scripts import load_sample_boe_data, send_opportunities_telegram


DEFAULT_CATEGORIES = {"high_interest", "review"}
MIN_SCORE = 60
TOP_N = 20


def main() -> None:
    """Run data refresh, evaluation snapshot, and Telegram alerting in order."""
    started_at = datetime.now().isoformat(timespec="seconds")
    print(f"[START] Monitor cycle started at {started_at}")

    before_count = len(fetch_all_auctions())
    print(f"[STEP] Existing auctions in SQLite before refresh: {before_count}")

    try:
        print("[STEP] Refreshing local BOE sample data...")
        load_sample_boe_data.main()
    except Exception as exc:
        print(f"[ERROR] Data load failed: {exc}")
        print("[END] Monitor cycle stopped before Telegram.")
        raise

    after_auctions = fetch_all_auctions()
    after_count = len(after_auctions)
    filtered_evaluations = rank_and_filter_opportunities(
        after_auctions,
        categories=DEFAULT_CATEGORIES,
        min_score=MIN_SCORE,
        top_n=TOP_N,
    )

    print("[STEP] Data refresh completed.")
    print(f"[INFO] Auctions in SQLite after refresh: {after_count}")
    print(f"[INFO] Filtered actionable opportunities: {len(filtered_evaluations)}")

    try:
        print("[STEP] Sending Telegram alerts for relevant changes...")
        send_opportunities_telegram.main()
    except Exception as exc:
        print(f"[ERROR] Telegram alerting failed: {exc}")
        print("[END] Monitor cycle finished with alerting error.")
        raise

    finished_at = datetime.now().isoformat(timespec="seconds")
    print("[INFO] Telegram step completed.")
    print(f"[END] Monitor cycle finished at {finished_at}")


if __name__ == "__main__":
    main()
