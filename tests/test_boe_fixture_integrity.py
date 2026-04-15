"""Integrity checks for BOE case fixtures."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from tests.boe_case_loader import list_boe_case_dirs


VALID_FIXTURE_STATUSES = {
    "legacy_sanitized",
    "sanitized_from_raw",
}
VALID_PAGE_KINDS = {"listing", "detail"}
RAW_ROOT = Path(__file__).parent / "fixtures" / "boe" / "raw"


def test_boe_case_directories_are_present() -> None:
    """Debe existir al menos un caso BOE formalizado."""
    assert list_boe_case_dirs()


def test_boe_cases_have_required_files_and_metadata() -> None:
    """Debe validar la estructura mínima y el contrato del corpus BOE."""
    for case_dir in list_boe_case_dirs():
        manifest_path = case_dir / "manifest.json"
        expected_path = case_dir / "expected.json"
        sanitized_path = case_dir / "sanitized.html"

        assert manifest_path.is_file()
        assert expected_path.is_file()
        assert sanitized_path.is_file()
        assert not (case_dir / "raw.html").exists()

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = json.loads(expected_path.read_text(encoding="utf-8"))

        assert manifest["schema_version"] == 1
        assert manifest["case_id"] == case_dir.name
        assert manifest["source"] == "boe"
        assert manifest["page_kind"] in VALID_PAGE_KINDS
        assert manifest["fixture_status"] in VALID_FIXTURE_STATUSES
        assert manifest["sanitized_file"] == "sanitized.html"
        assert manifest["expected_file"] == "expected.json"

        if manifest["fixture_status"] == "legacy_sanitized":
            assert manifest["origin_url"] is None
            assert "raw_file" not in manifest
            assert "capture_date" not in manifest
            assert "sanitizer_version" not in manifest
        else:
            assert isinstance(manifest["origin_url"], str)
            assert manifest["origin_url"].strip()
            assert isinstance(manifest["raw_file"], str)
            assert manifest["raw_file"].strip()
            assert manifest["sanitizer_version"] == 1
            capture_date = manifest["capture_date"]
            assert isinstance(capture_date, str)
            assert date.fromisoformat(capture_date).isoformat() == capture_date

            raw_path = (case_dir / manifest["raw_file"]).resolve()
            assert raw_path.is_file()
            assert RAW_ROOT.resolve() in raw_path.parents
            assert case_dir.resolve() not in raw_path.parents

        assert sanitized_path.read_text(encoding="utf-8").strip()

        if manifest["page_kind"] == "listing":
            assert isinstance(expected["items"], list)
            assert expected["items"]
            seen_external_ids: set[str] = set()
            for item in expected["items"]:
                assert isinstance(item, dict)
                assert item["external_id"]
                assert item["external_id"] not in seen_external_ids
                seen_external_ids.add(item["external_id"])
            if "item_count" in expected:
                assert isinstance(expected["item_count"], int)
                assert expected["item_count"] >= 0
        else:
            assert isinstance(expected["detail"], dict)
            assert expected["detail"]
