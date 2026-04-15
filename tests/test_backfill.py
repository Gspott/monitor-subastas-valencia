"""Tests for optional SQLite backfills."""

import sqlite3

import monitor.backfill as backfill
import monitor.storage as storage
from monitor.backfill import (
    BackfillExample,
    BackfillResult,
    MunicipalityAuditResult,
    MunicipalityBackfillExample,
    MunicipalityBackfillChange,
    MunicipalityBackfillResult,
    PostalCodeAuditResult,
    group_municipality_backfill_changes,
    group_postal_code_municipality_variants,
)


def test_backfill_official_status_dry_run_reports_changes_without_writing(tmp_path, monkeypatch) -> None:
    """Debe detectar cambios sin escribir cuando dry_run=True."""
    database_path = tmp_path / "backfill_dry_run.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "boe::legacy-1", "BOE", "LEGACY-1", "Activo histórico", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "Celebrándose con pujas",
        ))
        conn.commit()

    result = backfill.backfill_official_status(dry_run=True)

    assert result.total_rows_scanned == 1
    assert result.total_rows_changed == 1
    assert result.dry_run is True
    assert result.examples[0].before == "Celebrándose con pujas"
    assert result.examples[0].after == "abierta con pujas"

    with sqlite3.connect(database_path) as conn:
        stored_status = conn.execute("SELECT official_status FROM auctions WHERE id = 1").fetchone()[0]

    assert stored_status == "Celebrándose con pujas"


def test_backfill_official_status_apply_writes_changes_only_to_status(tmp_path, monkeypatch) -> None:
    """Debe actualizar solo official_status cuando se aplica el backfill."""
    database_path = tmp_path / "backfill_apply.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status, score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "boe::legacy-2", "BOE", "LEGACY-2", "Activo histórico", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "En tramitación", 55,
        ))
        conn.commit()

    result = backfill.backfill_official_status(dry_run=False)

    assert result.total_rows_scanned == 1
    assert result.total_rows_changed == 1
    assert result.dry_run is False

    with sqlite3.connect(database_path) as conn:
        row = conn.execute("SELECT official_status, score, title FROM auctions WHERE id = 1").fetchone()

    assert row[0] == "en tramitacion"
    assert row[1] == 55
    assert row[2] == "Activo histórico"


def test_backfill_official_status_normalizes_unknown_status_prudently(tmp_path, monkeypatch) -> None:
    """Debe normalizar también estados desconocidos sin reclasificarlos a la fuerza."""
    database_path = tmp_path / "backfill_unknown.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            INSERT INTO auctions (
                dedupe_key, source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "boe::legacy-3", "BOE", "LEGACY-3", "Activo histórico", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "Pendiente de revisión",
        ))
        conn.commit()

    result = backfill.backfill_official_status(dry_run=False)

    assert result.total_rows_changed == 1
    assert result.examples[0].after == "pendiente de revision"

    with sqlite3.connect(database_path) as conn:
        stored_status = conn.execute("SELECT official_status FROM auctions WHERE id = 1").fetchone()[0]

    assert stored_status == "pendiente de revision"


def test_backfill_official_status_ignores_null_status_values(tmp_path, monkeypatch) -> None:
    """No debe tocar filas con official_status nulo."""
    database_path = tmp_path / "backfill_null.db"
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            CREATE TABLE auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                official_status TEXT
            )
        """)
        conn.execute("""
            INSERT INTO auctions (official_status) VALUES (?)
        """, (None,))
        conn.commit()

    result = backfill.backfill_official_status(dry_run=False)

    assert result.total_rows_scanned == 1
    assert result.total_rows_changed == 0


def test_backfill_result_to_console_text_shows_all_examples_when_below_limit() -> None:
    """Debe mostrar todos los ejemplos cuando no se supera el límite visual."""
    result = BackfillResult(
        total_rows_scanned=12,
        total_rows_changed=3,
        dry_run=True,
        examples=[
            BackfillExample(row_id=1, before="Celebrándose", after="abierta"),
            BackfillExample(row_id=2, before="En tramitación", after="en tramitacion"),
        ],
    )

    text = result.to_console_text()

    assert "official_status backfill summary (dry-run)" in text
    assert "- total rows scanned: 12" in text
    assert "- total rows changed: 3" in text
    assert "- examples included: 2" in text
    assert "row 1: 'Celebrándose' -> 'abierta'" in text
    assert "row 2: 'En tramitación' -> 'en tramitacion'" in text
    assert "... and" not in text


def test_backfill_result_to_console_text_keeps_all_examples_at_limit() -> None:
    """Debe mantener visibles todos los ejemplos si coinciden con el límite."""
    result = BackfillResult(
        total_rows_scanned=20,
        total_rows_changed=5,
        dry_run=False,
        examples=[
            BackfillExample(row_id=1, before="A", after="a"),
            BackfillExample(row_id=2, before="B", after="b"),
            BackfillExample(row_id=3, before="C", after="c"),
            BackfillExample(row_id=4, before="D", after="d"),
            BackfillExample(row_id=5, before="E", after="e"),
        ],
    )

    text = result.to_console_text()

    assert "official_status backfill summary (apply)" in text
    assert "- examples included: 5" in text
    assert "row 5: 'E' -> 'e'" in text
    assert "... and" not in text


def test_backfill_result_to_console_text_adds_more_changes_line_above_limit() -> None:
    """Debe resumir el exceso de cambios cuando hay demasiados ejemplos."""
    result = BackfillResult(
        total_rows_scanned=30,
        total_rows_changed=8,
        dry_run=True,
        examples=[
            BackfillExample(row_id=1, before="A", after="a"),
            BackfillExample(row_id=2, before="B", after="b"),
            BackfillExample(row_id=3, before="C", after="c"),
            BackfillExample(row_id=4, before="D", after="d"),
            BackfillExample(row_id=5, before="E", after="e"),
            BackfillExample(row_id=6, before="F", after="f"),
            BackfillExample(row_id=7, before="G", after="g"),
        ],
    )

    text = result.to_console_text()

    assert "- examples included: 5" in text
    assert "row 5: 'E' -> 'e'" in text
    assert "row 6: 'F' -> 'f'" not in text
    assert "  ... and 3 more changes" in text


def test_backfill_municipality_does_not_change_rows_without_valid_postal_code(tmp_path, monkeypatch) -> None:
    """No debe tocar filas sin CP válido."""
    database_path = tmp_path / "backfill_municipality_invalid_postal.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-invalid-postal",
        external_id="MUNI-INVALID-POSTAL",
        municipality="Valencia",
        postal_code="46-710",
    )

    result = backfill.backfill_municipality_from_postal_code(
        dry_run=True,
        table="active",
    )

    assert result.rows_examined == 1
    assert result.rows_with_valid_postal_code == 0
    assert result.rows_with_supported_mapping == 0
    assert result.rows_changed == 0


def test_backfill_municipality_does_not_change_rows_with_unsupported_postal_code(tmp_path, monkeypatch) -> None:
    """No debe tocar filas si el CP válido no tiene mapeo conocido."""
    database_path = tmp_path / "backfill_municipality_unsupported_postal.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-unsupported-postal",
        external_id="MUNI-UNSUPPORTED-POSTAL",
        municipality="Valencia",
        postal_code="46123",
    )

    result = backfill.backfill_municipality_from_postal_code(
        dry_run=True,
        table="active",
    )

    assert result.rows_examined == 1
    assert result.rows_with_valid_postal_code == 1
    assert result.rows_with_supported_mapping == 0
    assert result.rows_changed == 0


def test_backfill_municipality_corrects_conflicting_value_when_mapping_exists(tmp_path, monkeypatch) -> None:
    """Debe corregir municipality cuando contradice un CP soportado."""
    database_path = tmp_path / "backfill_municipality_conflict.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.COMPLETED_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-conflict",
        external_id="MUNI-CONFLICT",
        municipality="Valencia",
        postal_code="46710",
    )

    result = backfill.backfill_municipality_from_postal_code(
        dry_run=False,
        table="completed",
    )

    assert result.rows_examined == 1
    assert result.rows_with_valid_postal_code == 1
    assert result.rows_with_supported_mapping == 1
    assert result.rows_changed == 1
    assert result.examples[0].postal_code == "46710"
    assert result.examples[0].before == "Valencia"
    assert result.examples[0].after == "Daimus"

    with sqlite3.connect(database_path) as conn:
        row = conn.execute(
            f"SELECT municipality, title, official_status FROM {storage.COMPLETED_AUCTIONS_TABLE} WHERE id = 1"
        ).fetchone()

    assert row[0] == "Daimus"
    assert row[1] == "Activo histórico"
    assert row[2] == "abierta"


def test_backfill_municipality_removes_accents_when_canonical_value_is_equivalent(tmp_path, monkeypatch) -> None:
    """Debe proponer la forma sin acento cuando solo cambia el naming canónico."""
    database_path = tmp_path / "backfill_municipality_accents.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-accent",
        external_id="MUNI-ACCENT",
        municipality="Algemesí",
        postal_code="46680",
    )

    result = backfill.backfill_municipality_from_postal_code(
        dry_run=True,
        table="active",
    )

    assert result.rows_changed == 1
    assert result.examples[0].before == "Algemesí"
    assert result.examples[0].after == "Algemesi"


def test_backfill_municipality_fills_empty_value_when_mapping_exists(tmp_path, monkeypatch) -> None:
    """Debe rellenar municipality vacío cuando el CP permite resolverlo."""
    database_path = tmp_path / "backfill_municipality_fill_empty.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.UPCOMING_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-empty",
        external_id="MUNI-EMPTY",
        municipality="   ",
        postal_code="46430",
    )

    result = backfill.backfill_municipality_from_postal_code(
        dry_run=False,
        table="upcoming",
    )

    assert result.rows_changed == 1
    assert result.examples[0].after == "Sollana"

    with sqlite3.connect(database_path) as conn:
        stored_municipality = conn.execute(
            f"SELECT municipality FROM {storage.UPCOMING_AUCTIONS_TABLE} WHERE id = 1"
        ).fetchone()[0]

    assert stored_municipality == "Sollana"


def test_backfill_municipality_is_idempotent_on_second_run(tmp_path, monkeypatch) -> None:
    """Debe quedar sin cambios en una segunda ejecución sobre la misma base."""
    database_path = tmp_path / "backfill_municipality_idempotent.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::municipality-idempotent",
        external_id="MUNI-IDEMPOTENT",
        municipality="Valencia",
        postal_code="46710",
    )

    first_result = backfill.backfill_municipality_from_postal_code(
        dry_run=False,
        table="active",
    )
    second_result = backfill.backfill_municipality_from_postal_code(
        dry_run=False,
        table="active",
    )

    assert first_result.rows_changed == 1
    assert second_result.rows_examined == 1
    assert second_result.rows_with_valid_postal_code == 1
    assert second_result.rows_with_supported_mapping == 1
    assert second_result.rows_changed == 0


def test_municipality_backfill_result_to_console_text_lists_examples() -> None:
    """Debe mostrar métricas y ejemplos del backfill de municipality."""
    result = MunicipalityBackfillResult(
        rows_examined=9,
        rows_with_valid_postal_code=7,
        rows_with_supported_mapping=4,
        rows_changed=2,
        dry_run=True,
        table="all",
        examples=[
            MunicipalityBackfillExample(
                table="auctions",
                row_id=3,
                postal_code="46710",
                before="Valencia",
                after="Daimus",
            )
        ],
    )

    text = result.to_console_text()

    assert "municipality backfill summary (dry-run)" in text
    assert "- rows examined: 9" in text
    assert "- rows with valid postal code: 7" in text
    assert "- rows with supported mapping: 4" in text
    assert "- rows changed: 2" in text
    assert "auctions row 3: 46710 | 'Valencia' -> 'Daimus'" in text


def test_group_municipality_backfill_changes_counts_transformations() -> None:
    """Debe agrupar cambios por transformación y ordenarlos por frecuencia."""
    groups = group_municipality_backfill_changes(
        [
            MunicipalityBackfillChange(
                table="auctions",
                row_id=1,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
            MunicipalityBackfillChange(
                table="completed_auctions",
                row_id=2,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
            MunicipalityBackfillChange(
                table="auctions",
                row_id=3,
                postal_code="46680",
                municipality_old="Algemesí",
                municipality_new="Algemesi",
            ),
        ]
    )

    assert len(groups) == 2
    assert groups[0].municipality_old == "Puerto De Sagunto"
    assert groups[0].municipality_new == "Puerto de Sagunto"
    assert groups[0].count == 2
    assert groups[1].municipality_old == "Algemesí"
    assert groups[1].count == 1


def test_group_municipality_backfill_changes_keeps_up_to_three_examples_per_group() -> None:
    """Debe conservar solo 2-3 ejemplos por grupo para salida de auditoría."""
    groups = group_municipality_backfill_changes(
        [
            MunicipalityBackfillChange(
                table="auctions",
                row_id=4,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
            MunicipalityBackfillChange(
                table="auctions",
                row_id=2,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
            MunicipalityBackfillChange(
                table="completed_auctions",
                row_id=9,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
            MunicipalityBackfillChange(
                table="upcoming_auctions",
                row_id=1,
                postal_code="46520",
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
            ),
        ]
    )

    assert len(groups[0].examples) == 3
    assert (groups[0].examples[0].table, groups[0].examples[0].row_id) == ("auctions", 2)


def test_audit_municipality_backfill_reports_grouped_changes_without_writing(tmp_path, monkeypatch) -> None:
    """Debe auditar cambios sin escribir en SQLite."""
    database_path = tmp_path / "audit_municipality.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::audit-1",
        external_id="AUDIT-1",
        municipality="Puerto De Sagunto",
        postal_code="46520",
    )
    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::audit-2",
        external_id="AUDIT-2",
        municipality="Puerto De Sagunto",
        postal_code="46520",
    )
    _insert_location_row(
        database_path,
        storage.COMPLETED_AUCTIONS_TABLE,
        dedupe_key="boe::audit-3",
        external_id="AUDIT-3",
        municipality="Algemesí",
        postal_code="46680",
    )

    result = backfill.audit_municipality_backfill(table="all")

    assert result.rows_examined == 3
    assert result.rows_with_valid_postal_code == 3
    assert result.rows_with_supported_mapping == 3
    assert result.rows_changed == 3
    assert len(result.changes) == 3
    assert result.groups[0].municipality_old == "Puerto De Sagunto"
    assert result.groups[0].municipality_new == "Puerto de Sagunto"
    assert result.groups[0].count == 2
    assert result.groups[1].municipality_old == "Algemesí"
    assert result.groups[1].municipality_new == "Algemesi"

    with sqlite3.connect(database_path) as conn:
        stored_rows = conn.execute(
            f"""
            SELECT municipality FROM {storage.ACTIVE_AUCTIONS_TABLE}
            UNION ALL
            SELECT municipality FROM {storage.COMPLETED_AUCTIONS_TABLE}
            """
        ).fetchall()

    assert ("Puerto De Sagunto",) in stored_rows
    assert ("Algemesí",) in stored_rows


def test_municipality_audit_result_to_console_text_shows_group_counts_and_examples() -> None:
    """Debe mostrar grupos ordenados y ejemplos por transformación."""
    result = MunicipalityAuditResult(
        rows_examined=12,
        rows_with_valid_postal_code=10,
        rows_with_supported_mapping=6,
        rows_changed=4,
        table="all",
        changes=[],
        groups=[
            backfill.MunicipalityAuditGroup(
                municipality_old="Puerto De Sagunto",
                municipality_new="Puerto de Sagunto",
                count=3,
                examples=[
                    MunicipalityBackfillChange(
                        table="auctions",
                        row_id=31,
                        postal_code="46520",
                        municipality_old="Puerto De Sagunto",
                        municipality_new="Puerto de Sagunto",
                    ),
                    MunicipalityBackfillChange(
                        table="completed_auctions",
                        row_id=55,
                        postal_code="46520",
                        municipality_old="Puerto De Sagunto",
                        municipality_new="Puerto de Sagunto",
                    ),
                ],
            )
        ],
    )

    text = result.to_console_text()

    assert "municipality backfill audit" in text
    assert "- rows changed: 4" in text
    assert "'Puerto De Sagunto' -> 'Puerto de Sagunto': 3" in text
    assert "auctions row 31 | 46520 | 'Puerto De Sagunto' -> 'Puerto de Sagunto'" in text


def test_group_postal_code_municipality_variants_counts_current_values() -> None:
    """Debe agrupar municipios actuales por frecuencia para un CP concreto."""
    variants = group_postal_code_municipality_variants(
        [
            ("auctions", 1, "Playa de Gandia"),
            ("completed_auctions", 3, "Grao de Gandia"),
            ("auctions", 2, "Playa de Gandia"),
            ("upcoming_auctions", 4, ""),
        ]
    )

    assert len(variants) == 3
    assert variants[0].municipality == "Playa de Gandia"
    assert variants[0].count == 2
    assert variants[1].municipality == ""
    assert variants[1].count == 1
    assert variants[2].municipality == "Grao de Gandia"
    assert variants[2].count == 1


def test_audit_postal_code_municipality_variants_filters_specific_postal_code(tmp_path, monkeypatch) -> None:
    """Debe auditar solo las filas del CP solicitado."""
    database_path = tmp_path / "postal_code_audit_filter.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-a",
        external_id="PC-46730-A",
        municipality="Playa de Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.COMPLETED_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-b",
        external_id="PC-46730-B",
        municipality="Grao de Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.UPCOMING_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46701-a",
        external_id="PC-46701-A",
        municipality="Gandia",
        postal_code="46701",
    )

    result = backfill.audit_postal_code_municipality_variants(
        postal_code="46730",
        table="all",
    )

    assert result.postal_code == "46730"
    assert result.total_rows == 2
    assert result.rows_with_municipality == 2
    assert result.empty_municipality_rows == 0
    assert [variant.municipality for variant in result.variants] == [
        "Grao de Gandia",
        "Playa de Gandia",
    ]


def test_audit_postal_code_municipality_variants_counts_variants_and_preserves_db(tmp_path, monkeypatch) -> None:
    """Debe contar variantes reales sin modificar la base de datos."""
    database_path = tmp_path / "postal_code_audit_counts.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    monkeypatch.setattr(backfill, "DATABASE_PATH", database_path)
    storage.init_db()

    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-1",
        external_id="PC-46730-1",
        municipality="Playa de Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.ACTIVE_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-2",
        external_id="PC-46730-2",
        municipality="Playa de Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.COMPLETED_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-3",
        external_id="PC-46730-3",
        municipality="Grao de Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.UPCOMING_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-4",
        external_id="PC-46730-4",
        municipality="Gandia",
        postal_code="46730",
    )
    _insert_location_row(
        database_path,
        storage.UPCOMING_AUCTIONS_TABLE,
        dedupe_key="boe::pc-46730-5",
        external_id="PC-46730-5",
        municipality="",
        postal_code="46730",
    )

    result = backfill.audit_postal_code_municipality_variants(
        postal_code="46730",
        table="all",
    )

    assert result.total_rows == 5
    assert result.rows_with_municipality == 4
    assert result.empty_municipality_rows == 1
    assert result.generic_municipality_rows == 1
    assert result.inconsistent_municipality_rows == 0
    assert [(variant.municipality, variant.count) for variant in result.variants] == [
        ("Playa de Gandia", 2),
        ("", 1),
        ("Gandia", 1),
        ("Grao de Gandia", 1),
    ]

    with sqlite3.connect(database_path) as conn:
        stored_rows = conn.execute(
            f"""
            SELECT municipality FROM {storage.ACTIVE_AUCTIONS_TABLE}
            UNION ALL
            SELECT municipality FROM {storage.COMPLETED_AUCTIONS_TABLE}
            UNION ALL
            SELECT municipality FROM {storage.UPCOMING_AUCTIONS_TABLE}
            """
        ).fetchall()

    assert ("Playa de Gandia",) in stored_rows
    assert ("Grao de Gandia",) in stored_rows
    assert ("Gandia",) in stored_rows
    assert ("",) in stored_rows


def test_postal_code_audit_result_to_console_text_lists_variant_counts() -> None:
    """Debe mostrar métricas y variantes por frecuencia para un CP concreto."""
    result = PostalCodeAuditResult(
        postal_code="46730",
        table="all",
        total_rows=42,
        rows_with_municipality=38,
        empty_municipality_rows=4,
        generic_municipality_rows=6,
        inconsistent_municipality_rows=32,
        variants=[
            backfill.PostalCodeMunicipalityVariant(municipality="Playa de Gandia", count=18),
            backfill.PostalCodeMunicipalityVariant(municipality="Grao de Gandia", count=12),
            backfill.PostalCodeMunicipalityVariant(municipality="Gandia", count=6),
            backfill.PostalCodeMunicipalityVariant(municipality="", count=4),
        ],
    )

    text = result.to_console_text()

    assert "postal_code audit: 46730" in text
    assert "- total rows: 42" in text
    assert "- with municipality: 38" in text
    assert "- empty municipality: 4" in text
    assert "'Playa de Gandia': 18" in text
    assert "'Grao de Gandia': 12" in text
    assert "'Gandia': 6" in text


def _insert_location_row(
    database_path,
    table_name: str,
    *,
    dedupe_key: str,
    external_id: str,
    municipality: str,
    postal_code: str | None,
) -> None:
    """Insert one minimal row for municipality backfill tests."""
    with sqlite3.connect(database_path) as conn:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                dedupe_key, source, external_id, title, province, municipality, postal_code,
                asset_class, asset_subclass, is_vehicle, official_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                "BOE",
                external_id,
                "Activo histórico",
                "Valencia",
                municipality,
                postal_code,
                "real_estate",
                "residential_property",
                0,
                "abierta",
            ),
        )
        conn.commit()
