from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_ROOT = ROOT_DIR / "tests" / "fixtures" / "boe" / "raw"
CASES_ROOT = ROOT_DIR / "tests" / "fixtures" / "boe" / "cases"
VALID_PAGE_KINDS = {"listing", "detail"}


def build_manifest(case_id: str, page_kind: str, origin_url: str, notes: str | None) -> dict[str, object]:
    manifest: dict[str, object] = {
        "schema_version": 1,
        "case_id": case_id,
        "source": "boe",
        "page_kind": page_kind,
        "fixture_status": "sanitized_from_raw",
        "origin_url": origin_url,
        "capture_date": date.today().isoformat(),
        "raw_file": f"../../raw/{case_id}/raw.html",
        "sanitized_file": "sanitized.html",
        "expected_file": "expected.json",
        "sanitizer_version": 1,
    }
    if notes:
        manifest["notes"] = notes
    return manifest


def build_expected_skeleton(page_kind: str) -> dict[str, object]:
    if page_kind == "listing":
        return {"items": []}
    return {"detail": {}}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a BOE sanitized_from_raw case scaffold.")
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--page-kind", required=True, choices=sorted(VALID_PAGE_KINDS))
    parser.add_argument("--origin-url", required=True)
    parser.add_argument("--notes")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_path = RAW_ROOT / args.case_id / "raw.html"
    if not raw_path.is_file():
        raise SystemExit(f"Raw HTML not found: {raw_path}")

    case_dir = CASES_ROOT / args.case_id
    manifest_path = case_dir / "manifest.json"
    expected_path = case_dir / "expected.json"

    if case_dir.exists() and not args.force:
        raise SystemExit(
            f"Case directory already exists: {case_dir}. Use --force to overwrite scaffold files."
        )

    case_dir.mkdir(parents=True, exist_ok=True)

    manifest = build_manifest(args.case_id, args.page_kind, args.origin_url, args.notes)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    if args.force or not expected_path.exists():
        expected = build_expected_skeleton(args.page_kind)
        expected_path.write_text(json.dumps(expected, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Created case scaffold: {case_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
