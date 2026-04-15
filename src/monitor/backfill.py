"""Utilities for safe optional data backfills."""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal

from .config import DATABASE_PATH
from .location import (
    classify_46730_municipality_variant,
    normalize_municipality_with_postal_code,
    resolve_municipality_from_postal_code,
)
from .normalize import normalize_postal_code
from .status import normalize_status
from .storage import ACTIVE_AUCTIONS_TABLE, COMPLETED_AUCTIONS_TABLE, UPCOMING_AUCTIONS_TABLE


logger = logging.getLogger(__name__)

EXAMPLE_LIMIT = 10
CONSOLE_EXAMPLE_LIMIT = 5
AUDIT_EXAMPLE_LIMIT = 3
BackfillTable = Literal["active", "upcoming", "completed", "all"]
TABLE_NAME_BY_SELECTOR = {
    "active": ACTIVE_AUCTIONS_TABLE,
    "upcoming": UPCOMING_AUCTIONS_TABLE,
    "completed": COMPLETED_AUCTIONS_TABLE,
}


@dataclass(slots=True)
class BackfillExample:
    """One example of a changed status during backfill."""

    row_id: int
    before: str
    after: str


@dataclass(slots=True)
class BackfillResult:
    """Summary of an official_status backfill run."""

    total_rows_scanned: int = 0
    total_rows_changed: int = 0
    dry_run: bool = True
    examples: list[BackfillExample] = field(default_factory=list)

    def to_console_text(self) -> str:
        """Render a compact console summary for manual review."""
        mode_label = "dry-run" if self.dry_run else "apply"
        displayed_examples = self.examples[:CONSOLE_EXAMPLE_LIMIT]
        remaining_examples = 0
        if len(self.examples) > CONSOLE_EXAMPLE_LIMIT:
            remaining_examples = max(0, self.total_rows_changed - CONSOLE_EXAMPLE_LIMIT)
        lines = [
            f"official_status backfill summary ({mode_label})",
            f"- total rows scanned: {self.total_rows_scanned}",
            f"- total rows changed: {self.total_rows_changed}",
            f"- examples included: {len(displayed_examples)}",
        ]

        if displayed_examples:
            lines.append("- examples:")
            for example in displayed_examples:
                lines.append(
                    f"  row {example.row_id}: {example.before!r} -> {example.after!r}"
                )
            if remaining_examples > 0:
                lines.append(f"  ... and {remaining_examples} more changes")

        return "\n".join(lines)


@dataclass(slots=True)
class MunicipalityBackfillExample:
    """One example of a municipality change during backfill."""

    table: str
    row_id: int
    postal_code: str
    before: str
    after: str


@dataclass(slots=True)
class MunicipalityBackfillChange:
    """One potential municipality change detected during scan."""

    table: str
    row_id: int
    postal_code: str
    municipality_old: str
    municipality_new: str


@dataclass(slots=True)
class MunicipalityBackfillResult:
    """Summary of a municipality-from-postal-code backfill run."""

    rows_examined: int = 0
    rows_with_valid_postal_code: int = 0
    rows_with_supported_mapping: int = 0
    rows_changed: int = 0
    dry_run: bool = True
    table: BackfillTable = "all"
    examples: list[MunicipalityBackfillExample] = field(default_factory=list)

    def to_console_text(self) -> str:
        """Render a compact console summary for manual review."""
        mode_label = "dry-run" if self.dry_run else "apply"
        displayed_examples = self.examples[:CONSOLE_EXAMPLE_LIMIT]
        remaining_examples = max(0, self.rows_changed - len(displayed_examples))
        lines = [
            f"municipality backfill summary ({mode_label})",
            f"- table selection: {self.table}",
            f"- rows examined: {self.rows_examined}",
            f"- rows with valid postal code: {self.rows_with_valid_postal_code}",
            f"- rows with supported mapping: {self.rows_with_supported_mapping}",
            f"- rows changed: {self.rows_changed}",
            f"- examples included: {len(displayed_examples)}",
        ]

        if displayed_examples:
            lines.append("- examples:")
            for example in displayed_examples:
                lines.append(
                    f"  {example.table} row {example.row_id}: "
                    f"{example.postal_code} | {example.before!r} -> {example.after!r}"
                )
            if remaining_examples > 0:
                lines.append(f"  ... and {remaining_examples} more changes")

        return "\n".join(lines)


@dataclass(slots=True)
class MunicipalityAuditGroup:
    """One grouped municipality transformation for audit review."""

    municipality_old: str
    municipality_new: str
    count: int
    examples: list[MunicipalityBackfillChange] = field(default_factory=list)


@dataclass(slots=True)
class MunicipalityAuditResult:
    """Grouped audit view of municipality changes before applying a backfill."""

    rows_examined: int = 0
    rows_with_valid_postal_code: int = 0
    rows_with_supported_mapping: int = 0
    rows_changed: int = 0
    table: BackfillTable = "all"
    changes: list[MunicipalityBackfillChange] = field(default_factory=list)
    groups: list[MunicipalityAuditGroup] = field(default_factory=list)

    def to_console_text(self) -> str:
        """Render grouped audit output ordered by descending frequency."""
        lines = [
            "municipality backfill audit",
            f"- table selection: {self.table}",
            f"- rows examined: {self.rows_examined}",
            f"- rows with valid postal code: {self.rows_with_valid_postal_code}",
            f"- rows with supported mapping: {self.rows_with_supported_mapping}",
            f"- rows changed: {self.rows_changed}",
            f"- transformation groups: {len(self.groups)}",
        ]

        if self.groups:
            lines.append("- grouped transformations:")
            for group in self.groups:
                lines.append(
                    f"  {group.municipality_old!r} -> {group.municipality_new!r}: {group.count}"
                )
                for example in group.examples[:AUDIT_EXAMPLE_LIMIT]:
                    lines.append(
                        f"    {example.table} row {example.row_id} | "
                        f"{example.postal_code} | {example.municipality_old!r} -> {example.municipality_new!r}"
                    )

        return "\n".join(lines)


@dataclass(slots=True)
class PostalCodeMunicipalityVariant:
    """One municipality variant currently present for a postal code."""

    municipality: str
    count: int
    examples: list[tuple[str, int]] = field(default_factory=list)


@dataclass(slots=True)
class PostalCodeAuditResult:
    """Audit view of the current municipality variants for one postal code."""

    postal_code: str = ""
    table: BackfillTable = "all"
    total_rows: int = 0
    rows_with_municipality: int = 0
    empty_municipality_rows: int = 0
    generic_municipality_rows: int = 0
    inconsistent_municipality_rows: int = 0
    variants: list[PostalCodeMunicipalityVariant] = field(default_factory=list)

    def to_console_text(self) -> str:
        """Render a compact postal-code-focused municipality audit."""
        lines = [
            f"postal_code audit: {self.postal_code}",
            f"- table selection: {self.table}",
            f"- total rows: {self.total_rows}",
            f"- with municipality: {self.rows_with_municipality}",
            f"- empty municipality: {self.empty_municipality_rows}",
            f"- generic municipality: {self.generic_municipality_rows}",
            f"- inconsistent municipality: {self.inconsistent_municipality_rows}",
            "- variants:",
        ]

        for variant in self.variants:
            label = variant.municipality if variant.municipality else ""
            lines.append(f"  {label!r}: {variant.count}")

        return "\n".join(lines)


def backfill_official_status(dry_run: bool = True) -> BackfillResult:
    """Backfill official_status values in the existing SQLite database."""
    result = BackfillResult(dry_run=dry_run)

    with sqlite3.connect(DATABASE_PATH) as conn:
        rows = conn.execute("""
            SELECT id, official_status
            FROM auctions
        """).fetchall()
        result.total_rows_scanned = len(rows)

        updates: list[tuple[str, int]] = []
        for row_id, current_status in rows:
            if current_status is None:
                continue

            normalized_status = normalize_status(current_status)
            if normalized_status is None or normalized_status == current_status:
                continue

            updates.append((normalized_status, row_id))
            if len(result.examples) < EXAMPLE_LIMIT:
                result.examples.append(
                    BackfillExample(
                        row_id=row_id,
                        before=current_status,
                        after=normalized_status,
                    )
                )

        result.total_rows_changed = len(updates)

        if not dry_run and updates:
            # Tocar solo official_status para que el backfill sea acotado y seguro.
            conn.executemany(
                """
                UPDATE auctions
                SET official_status = ?
                WHERE id = ?
                """,
                updates,
            )
            conn.commit()

    logger.info(
        "Backfill official_status completed. dry_run=%s scanned=%s changed=%s",
        result.dry_run,
        result.total_rows_scanned,
        result.total_rows_changed,
    )
    for example in result.examples:
        logger.info(
            "Backfill example row_id=%s before=%r after=%r",
            example.row_id,
            example.before,
            example.after,
        )

    return result


def backfill_municipality_from_postal_code(
    *,
    dry_run: bool = True,
    table: BackfillTable = "all",
) -> MunicipalityBackfillResult:
    """Backfill municipality values in SQLite using the shared postal-code logic."""
    result = MunicipalityBackfillResult(dry_run=dry_run, table=table)
    scan = _scan_municipality_backfill_changes(table)
    result.rows_examined = scan.rows_examined
    result.rows_with_valid_postal_code = scan.rows_with_valid_postal_code
    result.rows_with_supported_mapping = scan.rows_with_supported_mapping
    result.rows_changed = len(scan.changes)
    for change in scan.changes[:EXAMPLE_LIMIT]:
        result.examples.append(
            MunicipalityBackfillExample(
                table=change.table,
                row_id=change.row_id,
                postal_code=change.postal_code,
                before=change.municipality_old,
                after=change.municipality_new,
            )
        )

    if not dry_run and scan.changes:
        with sqlite3.connect(DATABASE_PATH) as conn:
            updates_by_table: dict[str, list[tuple[str, int]]] = defaultdict(list)
            for change in scan.changes:
                updates_by_table[change.table].append((change.municipality_new, change.row_id))

            for table_name, updates in updates_by_table.items():
                # Tocar solo municipality para mantener el backfill acotado y seguro.
                conn.executemany(
                    f"""
                    UPDATE {table_name}
                    SET municipality = ?
                    WHERE id = ?
                    """,
                    updates,
                )
            conn.commit()

    logger.info(
        "Backfill municipality completed. dry_run=%s table=%s examined=%s valid_postal=%s supported=%s changed=%s",
        result.dry_run,
        result.table,
        result.rows_examined,
        result.rows_with_valid_postal_code,
        result.rows_with_supported_mapping,
        result.rows_changed,
    )
    for example in result.examples:
        logger.info(
            "Municipality backfill example table=%s row_id=%s postal_code=%s before=%r after=%r",
            example.table,
            example.row_id,
            example.postal_code,
            example.before,
            example.after,
        )

    return result


def audit_municipality_backfill(
    *,
    table: BackfillTable = "all",
) -> MunicipalityAuditResult:
    """Audit all potential municipality changes without writing to SQLite."""
    scan = _scan_municipality_backfill_changes(table)
    grouped_changes = group_municipality_backfill_changes(scan.changes)
    return MunicipalityAuditResult(
        rows_examined=scan.rows_examined,
        rows_with_valid_postal_code=scan.rows_with_valid_postal_code,
        rows_with_supported_mapping=scan.rows_with_supported_mapping,
        rows_changed=len(scan.changes),
        table=table,
        changes=scan.changes,
        groups=grouped_changes,
    )


def audit_postal_code_municipality_variants(
    *,
    postal_code: str,
    table: BackfillTable = "all",
) -> PostalCodeAuditResult:
    """Audit current municipality variants for one postal code without writing to SQLite."""
    normalized_postal_code = normalize_postal_code(postal_code)
    if normalized_postal_code is None:
        raise ValueError("Postal code audit requires one valid five-digit postal code.")

    selected_tables = _select_backfill_tables(table)
    rows: list[tuple[str, int, str]] = []

    with sqlite3.connect(DATABASE_PATH) as conn:
        for table_name in selected_tables:
            table_rows = conn.execute(
                f"""
                SELECT id, municipality
                FROM {table_name}
                WHERE postal_code = ?
                """,
                (normalized_postal_code,),
            ).fetchall()
            rows.extend(
                (table_name, row_id, municipality or "")
                for row_id, municipality in table_rows
            )

    grouped_variants = group_postal_code_municipality_variants(rows)
    canonical_municipality = resolve_municipality_from_postal_code(normalized_postal_code)
    generic_rows = 0
    inconsistent_rows = 0
    if normalized_postal_code == "46730":
        for variant in grouped_variants:
            classification = classify_46730_municipality_variant(variant.municipality)
            if classification == "generic":
                generic_rows += variant.count
            elif classification == "other":
                inconsistent_rows += variant.count
    else:
        for variant in grouped_variants:
            if not variant.municipality:
                continue
            if canonical_municipality is not None and variant.municipality == canonical_municipality:
                generic_rows += variant.count
            else:
                inconsistent_rows += variant.count

    return PostalCodeAuditResult(
        postal_code=normalized_postal_code,
        table=table,
        total_rows=len(rows),
        rows_with_municipality=sum(1 for _, _, municipality in rows if municipality.strip()),
        empty_municipality_rows=sum(1 for _, _, municipality in rows if not municipality.strip()),
        generic_municipality_rows=generic_rows,
        inconsistent_municipality_rows=inconsistent_rows,
        variants=grouped_variants,
    )


def group_municipality_backfill_changes(
    changes: list[MunicipalityBackfillChange],
) -> list[MunicipalityAuditGroup]:
    """Group municipality changes by old and new value, ordered by frequency."""
    buckets: dict[tuple[str, str], list[MunicipalityBackfillChange]] = defaultdict(list)
    for change in changes:
        buckets[(change.municipality_old, change.municipality_new)].append(change)

    groups: list[MunicipalityAuditGroup] = []
    for (municipality_old, municipality_new), bucket in buckets.items():
        ordered_examples = sorted(bucket, key=lambda item: (item.table, item.row_id))
        groups.append(
            MunicipalityAuditGroup(
                municipality_old=municipality_old,
                municipality_new=municipality_new,
                count=len(bucket),
                examples=ordered_examples[:AUDIT_EXAMPLE_LIMIT],
            )
        )

    return sorted(
        groups,
        key=lambda group: (-group.count, group.municipality_old, group.municipality_new),
    )


def group_postal_code_municipality_variants(
    rows: list[tuple[str, int, str]],
) -> list[PostalCodeMunicipalityVariant]:
    """Group current municipality values for one postal code by frequency."""
    buckets: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for table_name, row_id, municipality in rows:
        buckets[municipality].append((table_name, row_id))

    variants: list[PostalCodeMunicipalityVariant] = []
    for municipality, bucket in buckets.items():
        ordered_examples = sorted(bucket, key=lambda item: (item[0], item[1]))
        variants.append(
            PostalCodeMunicipalityVariant(
                municipality=municipality,
                count=len(bucket),
                examples=ordered_examples[:AUDIT_EXAMPLE_LIMIT],
            )
        )

    return sorted(
        variants,
        key=lambda variant: (-variant.count, variant.municipality),
    )


def _select_backfill_tables(table: BackfillTable) -> list[str]:
    """Resolve a CLI table selector to concrete SQLite table names."""
    if table == "all":
        return list(TABLE_NAME_BY_SELECTOR.values())
    return [TABLE_NAME_BY_SELECTOR[table]]


@dataclass(slots=True)
class _MunicipalityBackfillScan:
    """Internal scan result reused by backfill and audit modes."""

    rows_examined: int = 0
    rows_with_valid_postal_code: int = 0
    rows_with_supported_mapping: int = 0
    changes: list[MunicipalityBackfillChange] = field(default_factory=list)


def _scan_municipality_backfill_changes(table: BackfillTable) -> _MunicipalityBackfillScan:
    """Collect all municipality changes implied by the shared postal-code logic."""
    scan = _MunicipalityBackfillScan()
    selected_tables = _select_backfill_tables(table)

    with sqlite3.connect(DATABASE_PATH) as conn:
        for table_name in selected_tables:
            rows = conn.execute(f"""
                SELECT id, municipality, postal_code
                FROM {table_name}
            """).fetchall()
            scan.rows_examined += len(rows)

            for row_id, current_municipality, current_postal_code in rows:
                valid_postal_code = normalize_postal_code(current_postal_code)
                if valid_postal_code is None:
                    continue
                scan.rows_with_valid_postal_code += 1

                if resolve_municipality_from_postal_code(valid_postal_code) is None:
                    continue
                scan.rows_with_supported_mapping += 1

                current_text = current_municipality or ""
                normalized_municipality = normalize_municipality_with_postal_code(
                    current_text,
                    postal_code=valid_postal_code,
                )
                if normalized_municipality == current_text:
                    continue

                scan.changes.append(
                    MunicipalityBackfillChange(
                        table=table_name,
                        row_id=row_id,
                        postal_code=valid_postal_code,
                        municipality_old=current_text,
                        municipality_new=normalized_municipality,
                    )
                )

    return scan
