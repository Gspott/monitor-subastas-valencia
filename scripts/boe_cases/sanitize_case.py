from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parents[2]
CASES_ROOT = ROOT_DIR / "tests" / "fixtures" / "boe" / "cases"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.boe_cases.sanitizer import sanitize_boe_html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sanitize raw HTML for one BOE case.")
    parser.add_argument("--case-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    case_dir = CASES_ROOT / args.case_id
    if not case_dir.is_dir():
        raise SystemExit(f"Case directory not found: {case_dir}. Run create_case.py first.")

    manifest_path = case_dir / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"Manifest not found: {manifest_path}. Run create_case.py first.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("fixture_status") != "sanitized_from_raw":
        raise SystemExit("sanitize_case.py only supports cases with fixture_status='sanitized_from_raw'.")

    raw_file = manifest.get("raw_file")
    if not isinstance(raw_file, str) or not raw_file:
        raise SystemExit("Manifest is missing a valid raw_file entry.")

    raw_path = (case_dir / raw_file).resolve()
    if not raw_path.is_file():
        raise SystemExit(f"Raw HTML not found: {raw_path}")

    raw_html = raw_path.read_text(encoding="utf-8")
    sanitized_html = sanitize_boe_html(raw_html)

    sanitized_path = case_dir / "sanitized.html"
    sanitized_path.write_text(sanitized_html, encoding="utf-8")

    print(f"Sanitized case written to: {sanitized_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
