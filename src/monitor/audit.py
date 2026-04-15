"""Utilities for optional read-only data audits."""

from __future__ import annotations

import csv
import logging
import sqlite3
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

from .config import DATA_DIR, DATABASE_PATH


logger = logging.getLogger(__name__)

AUDIT_EXAMPLE_LIMIT = 5
AUDIT_EXPORT_PATH = DATA_DIR / "dedupe_audit.csv"
AUDIT_DETAILED_EXPORT_PATH = DATA_DIR / "dedupe_audit_detailed.csv"


@dataclass(slots=True)
class AuditRecord:
    """One stored auction row used in the collision audit."""

    row_id: int
    source: str
    external_id: str | None
    title: str
    municipality: str
    asset_class: str
    appraisal_value: Decimal | None
    official_url: str | None
    publication_date: date | None
    opening_date: date | None
    closing_date: date | None
    official_status: str


@dataclass(slots=True)
class AuditExample:
    """One suspicious dedupe group example."""

    base_key: str
    group_size: int
    conflict_reasons: list[str]
    row_ids: list[int]
    titles: list[str]


@dataclass(slots=True)
class AuditGroup:
    """One suspicious dedupe group with its full row-level detail."""

    base_key: str
    conflict_reasons: list[str]
    records: list[AuditRecord]


@dataclass(slots=True)
class AuditResult:
    """Summary of a dedupe collision audit."""

    total_rows_scanned: int = 0
    total_groups_scanned: int = 0
    suspicious_groups_count: int = 0
    suspicious_records_count: int = 0
    examples: list[AuditExample] = field(default_factory=list)
    suspicious_groups: list[AuditGroup] = field(default_factory=list)

    def to_console_text(self) -> str:
        """Render a compact console summary for manual review."""
        displayed_examples = self.examples[:AUDIT_EXAMPLE_LIMIT]
        remaining_examples = max(0, self.suspicious_groups_count - len(displayed_examples))

        lines = [
            "dedupe collision audit summary",
            f"- total rows scanned: {self.total_rows_scanned}",
            f"- total groups scanned: {self.total_groups_scanned}",
            f"- suspicious groups count: {self.suspicious_groups_count}",
            f"- suspicious records count: {self.suspicious_records_count}",
            f"- examples included: {len(displayed_examples)}",
        ]

        if displayed_examples:
            lines.append("- examples:")
            for example in displayed_examples:
                lines.append(
                    f"  base {example.base_key} | size={example.group_size} | reasons={', '.join(example.conflict_reasons)}"
                )
                lines.append(f"    row_ids={example.row_ids}")
                lines.append(f"    titles={example.titles}")
            if remaining_examples > 0:
                lines.append(f"  ... and {remaining_examples} more suspicious groups")

        return "\n".join(lines)


def audit_dedupe_collisions() -> AuditResult:
    """Audit the SQLite database for suspicious historical dedupe collisions."""
    with sqlite3.connect(DATABASE_PATH) as conn:
        rows = conn.execute("""
            SELECT
                id,
                source,
                external_id,
                title,
                municipality,
                asset_class,
                appraisal_value,
                official_url,
                publication_date,
                opening_date,
                closing_date,
                official_status
            FROM auctions
        """).fetchall()

    result = AuditResult(total_rows_scanned=len(rows))
    records = [_row_to_audit_record(row) for row in rows]
    grouped_records = _group_records_by_fallback_base(records)
    result.total_groups_scanned = len(grouped_records)

    for base_key, group_records in grouped_records.items():
        if len(group_records) <= 1:
            continue

        conflict_reasons = _detect_group_conflicts(group_records)
        if not conflict_reasons:
            continue

        result.suspicious_groups_count += 1
        result.suspicious_records_count += len(group_records)
        result.suspicious_groups.append(
            AuditGroup(
                base_key=base_key,
                conflict_reasons=conflict_reasons,
                records=group_records,
            )
        )
        if len(result.examples) < AUDIT_EXAMPLE_LIMIT:
            result.examples.append(
                AuditExample(
                    base_key=base_key,
                    group_size=len(group_records),
                    conflict_reasons=conflict_reasons,
                    row_ids=[record.row_id for record in group_records],
                    titles=[record.title for record in group_records[:3]],
                )
            )

    logger.info(
        "Dedupe audit completed. rows=%s groups=%s suspicious_groups=%s suspicious_records=%s",
        result.total_rows_scanned,
        result.total_groups_scanned,
        result.suspicious_groups_count,
        result.suspicious_records_count,
    )
    return result


def export_audit_result_to_csv(result: AuditResult, output_path: Path = AUDIT_EXPORT_PATH) -> Path:
    """Export suspicious dedupe groups to CSV for manual review."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "base_key",
                "group_size",
                "conflict_reasons",
                "row_ids",
                "titles",
            ],
        )
        writer.writeheader()
        for example in result.examples:
            writer.writerow(
                {
                    "base_key": example.base_key,
                    "group_size": example.group_size,
                    "conflict_reasons": ", ".join(example.conflict_reasons),
                    "row_ids": ", ".join(str(row_id) for row_id in example.row_ids),
                    "titles": " | ".join(example.titles),
                }
            )

    return output_path


def export_detailed_audit_result_to_csv(
    result: AuditResult,
    output_path: Path = AUDIT_DETAILED_EXPORT_PATH,
) -> Path:
    """Export suspicious dedupe groups with one CSV row per suspicious record."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "group_key",
                "suspicion_reason",
                "row_id",
                "source",
                "external_id",
                "title",
                "municipality",
                "asset_class",
                "appraisal_value",
                "official_url",
                "publication_date",
                "opening_date",
                "closing_date",
                "official_status",
            ],
        )
        writer.writeheader()
        for group in result.suspicious_groups:
            for record in group.records:
                writer.writerow(
                    {
                        "group_key": group.base_key,
                        "suspicion_reason": ", ".join(group.conflict_reasons),
                        "row_id": record.row_id,
                        "source": record.source,
                        "external_id": record.external_id,
                        "title": record.title,
                        "municipality": record.municipality,
                        "asset_class": record.asset_class,
                        "appraisal_value": str(record.appraisal_value) if record.appraisal_value is not None else None,
                        "official_url": record.official_url,
                        "publication_date": record.publication_date.isoformat() if record.publication_date else None,
                        "opening_date": record.opening_date.isoformat() if record.opening_date else None,
                        "closing_date": record.closing_date.isoformat() if record.closing_date else None,
                        "official_status": record.official_status,
                    }
                )

    return output_path


def _row_to_audit_record(row: tuple) -> AuditRecord:
    """Convert a database row into an audit record."""
    return AuditRecord(
        row_id=row[0],
        source=row[1],
        external_id=row[2],
        title=row[3],
        municipality=row[4],
        asset_class=row[5],
        appraisal_value=_parse_decimal(row[6]),
        official_url=_normalize_optional_text(row[7]),
        publication_date=_parse_date(row[8]),
        opening_date=_parse_date(row[9]),
        closing_date=_parse_date(row[10]),
        official_status=row[11],
    )


def _group_records_by_fallback_base(records: list[AuditRecord]) -> dict[str, list[AuditRecord]]:
    """Group rows by the minimum historical fallback dedupe base."""
    grouped: dict[str, list[AuditRecord]] = {}

    for record in records:
        if not record.municipality.strip():
            continue
        if not record.asset_class.strip():
            continue
        if record.appraisal_value is None:
            continue

        base_key = "|".join(
            (
                record.municipality.casefold(),
                record.asset_class.casefold(),
                str(record.appraisal_value),
            )
        )
        grouped.setdefault(base_key, []).append(record)

    return grouped


def _detect_group_conflicts(records: list[AuditRecord]) -> list[str]:
    """Detect suspicious signal conflicts inside one fallback group."""
    conflict_reasons: list[str] = []

    if _has_multiple_distinct_values(record.official_url for record in records):
        conflict_reasons.append("official_url mismatch")
    if _has_multiple_distinct_values(
        record.publication_date.isoformat() if record.publication_date else None
        for record in records
    ):
        conflict_reasons.append("publication_date mismatch")
    if _has_multiple_distinct_values(
        record.opening_date.isoformat() if record.opening_date else None
        for record in records
    ):
        conflict_reasons.append("opening_date mismatch")
    if _has_multiple_distinct_values(
        record.closing_date.isoformat() if record.closing_date else None
        for record in records
    ):
        conflict_reasons.append("closing_date mismatch")
    if _has_multiple_distinct_values(_normalize_title(record.title) for record in records):
        # Heurística simple: si los títulos normalizados difieren, tratarlos como
        # señal de sospecha y dejar la revisión al usuario.
        conflict_reasons.append("title mismatch")

    return conflict_reasons


def _has_multiple_distinct_values(values) -> bool:
    """Return whether more than one distinct non-empty value exists."""
    distinct_values = {value for value in values if value not in (None, "")}
    return len(distinct_values) > 1


def _normalize_title(value: str) -> str:
    """Normalize titles for a simple collision audit heuristic."""
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(without_marks.casefold().split())


def _normalize_optional_text(value: str | None) -> str | None:
    """Normalize optional text values used by the audit."""
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def _parse_date(value: str | None) -> date | None:
    """Parse ISO date values stored in SQLite."""
    if value is None:
        return None

    return date.fromisoformat(value)


def _parse_decimal(value: str | None) -> Decimal | None:
    """Parse decimal values stored as text in SQLite."""
    if value is None:
        return None

    return Decimal(value)
