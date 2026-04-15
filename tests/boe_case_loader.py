"""Helpers for BOE parser case fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BOE_CASES_DIR = Path(__file__).parent / "fixtures" / "boe" / "cases"


def get_boe_case_dir(case_id: str) -> Path:
    """Return the directory for one BOE case fixture."""
    return BOE_CASES_DIR / case_id


def list_boe_case_dirs() -> list[Path]:
    """Return all BOE case directories sorted by name."""
    return sorted(path for path in BOE_CASES_DIR.iterdir() if path.is_dir())


def load_boe_case(case_id: str) -> dict[str, Any]:
    """Load one BOE case fixture and its metadata."""
    case_dir = get_boe_case_dir(case_id)
    manifest = json.loads((case_dir / "manifest.json").read_text(encoding="utf-8"))
    expected = json.loads((case_dir / "expected.json").read_text(encoding="utf-8"))
    html = (case_dir / "sanitized.html").read_text(encoding="utf-8")

    return {
        "case_dir": case_dir,
        "manifest": manifest,
        "expected": expected,
        "html": html,
    }
