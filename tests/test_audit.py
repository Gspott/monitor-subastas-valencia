"""Tests for optional SQLite audits."""

import sqlite3
from pathlib import Path

import monitor.audit as audit
import monitor.storage as storage


def test_audit_dedupe_collisions_ignores_groups_without_conflict(tmp_path, monkeypatch) -> None:
    """No debe marcar grupos sin señales contradictorias."""
    database_path = tmp_path / "audit_no_conflict.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(audit, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "boe::one", "BOE", "ONE", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                ),
                (
                    "boe::two", "BOE", "TWO", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                ),
            ],
        )
        conn.commit()

    result = audit.audit_dedupe_collisions()

    assert result.total_rows_scanned == 2
    assert result.total_groups_scanned == 1
    assert result.suspicious_groups_count == 0
    assert result.suspicious_records_count == 0


def test_audit_dedupe_collisions_detects_official_url_conflicts(tmp_path, monkeypatch) -> None:
    """Debe detectar grupos con URLs oficiales distintas."""
    database_path = tmp_path / "audit_url_conflict.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(audit, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, official_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "boe::one", "BOE", "ONE", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                    "https://example.test/item-1",
                ),
                (
                    "boe::two", "BOE", "TWO", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                    "https://example.test/item-2",
                ),
            ],
        )
        conn.commit()

    result = audit.audit_dedupe_collisions()

    assert result.suspicious_groups_count == 1
    assert result.suspicious_records_count == 2
    assert "official_url mismatch" in result.examples[0].conflict_reasons


def test_audit_dedupe_collisions_detects_date_conflicts(tmp_path, monkeypatch) -> None:
    """Debe detectar grupos con fechas distintas."""
    database_path = tmp_path / "audit_date_conflict.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(audit, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, publication_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "boe::one", "BOE", "ONE", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00", "2026-04-01",
                ),
                (
                    "boe::two", "BOE", "TWO", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00", "2026-04-03",
                ),
            ],
        )
        conn.commit()

    result = audit.audit_dedupe_collisions()

    assert result.suspicious_groups_count == 1
    assert "publication_date mismatch" in result.examples[0].conflict_reasons


def test_audit_dedupe_collisions_detects_title_conflicts(tmp_path, monkeypatch) -> None:
    """Debe detectar grupos con títulos claramente distintos."""
    database_path = tmp_path / "audit_title_conflict.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(audit, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.executemany(
            """
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "boe::one", "BOE", "ONE", "Local comercial", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                ),
                (
                    "boe::two", "BOE", "TWO", "Solar urbano", "Valencia", "Gandia",
                    "real_estate", "commercial_property", 0, "abierta", "99000.00",
                ),
            ],
        )
        conn.commit()

    result = audit.audit_dedupe_collisions()

    assert result.suspicious_groups_count == 1
    assert "title mismatch" in result.examples[0].conflict_reasons


def test_audit_dedupe_collisions_does_not_modify_database(tmp_path, monkeypatch) -> None:
    """La auditoría debe ser estrictamente de solo lectura."""
    database_path = tmp_path / "audit_read_only.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(audit, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, official_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "boe::one", "BOE", "ONE", "Local comercial", "Valencia", "Gandia",
            "real_estate", "commercial_property", 0, "abierta", "99000.00",
            "https://example.test/item-1",
        ))
        conn.commit()

    before = sqlite3.connect(database_path).execute(
        "SELECT official_status, official_url FROM auctions WHERE id = 1"
    ).fetchone()
    result = audit.audit_dedupe_collisions()
    after = sqlite3.connect(database_path).execute(
        "SELECT official_status, official_url FROM auctions WHERE id = 1"
    ).fetchone()

    assert result.total_rows_scanned == 1
    assert before == after


def test_audit_result_to_console_text_is_compact_and_clear() -> None:
    """Debe renderizar un resumen legible para revisión manual."""
    result = audit.AuditResult(
        total_rows_scanned=20,
        total_groups_scanned=4,
        suspicious_groups_count=2,
        suspicious_records_count=5,
        examples=[
            audit.AuditExample(
                base_key="gandia|real_estate|99000.00",
                group_size=2,
                conflict_reasons=["official_url mismatch", "title mismatch"],
                row_ids=[1, 2],
                titles=["Local comercial", "Solar urbano"],
            )
        ],
    )

    text = result.to_console_text()

    assert "dedupe collision audit summary" in text
    assert "- total rows scanned: 20" in text
    assert "- total groups scanned: 4" in text
    assert "- suspicious groups count: 2" in text
    assert "- suspicious records count: 5" in text
    assert "official_url mismatch, title mismatch" in text


def test_export_detailed_audit_result_to_csv_writes_one_row_per_suspicious_record(tmp_path: Path) -> None:
    """Debe exportar una fila por cada registro sospechoso."""
    result = audit.AuditResult(
        total_rows_scanned=4,
        total_groups_scanned=2,
        suspicious_groups_count=1,
        suspicious_records_count=2,
        suspicious_groups=[
            audit.AuditGroup(
                base_key="gandia|real_estate|99000.00",
                conflict_reasons=["official_url mismatch"],
                records=[
                    audit.AuditRecord(
                        row_id=1,
                        source="BOE",
                        external_id="ONE",
                        title="Local comercial",
                        municipality="Gandia",
                        asset_class="real_estate",
                        appraisal_value=audit.Decimal("99000.00"),
                        official_url="https://example.test/item-1",
                        publication_date=None,
                        opening_date=None,
                        closing_date=None,
                        official_status="abierta",
                    ),
                    audit.AuditRecord(
                        row_id=2,
                        source="BOE",
                        external_id="TWO",
                        title="Solar urbano",
                        municipality="Gandia",
                        asset_class="real_estate",
                        appraisal_value=audit.Decimal("99000.00"),
                        official_url="https://example.test/item-2",
                        publication_date=None,
                        opening_date=None,
                        closing_date=None,
                        official_status="abierta",
                    ),
                ],
            )
        ],
    )

    output_path = audit.export_detailed_audit_result_to_csv(result, tmp_path / "audit_detailed.csv")
    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 3
    assert "group_key,suspicion_reason,row_id,source,external_id,title" in lines[0]
    assert "ONE" in lines[1]
    assert "TWO" in lines[2]


def test_export_detailed_audit_result_to_csv_skips_non_suspicious_groups(tmp_path: Path) -> None:
    """No debe exportar filas cuando no hay grupos sospechosos."""
    result = audit.AuditResult(
        total_rows_scanned=2,
        total_groups_scanned=1,
        suspicious_groups_count=0,
        suspicious_records_count=0,
        suspicious_groups=[],
    )

    output_path = audit.export_detailed_audit_result_to_csv(result, tmp_path / "audit_detailed_empty.csv")
    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
