"""Tests for SQLite storage behavior."""

import sqlite3
from datetime import date
from decimal import Decimal

import monitor.storage as storage
from monitor.models import Auction


def test_upsert_auction_uses_persisted_dedupe_key_when_external_id_is_missing(tmp_path, monkeypatch) -> None:
    """Debe evitar duplicados en SQLite aunque falte external_id."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "auctions.db")
    storage.init_db()

    base_payload = {
        "source": "BOE",
        "external_id": None,
        "title": "Local comercial",
        "province": "Valencia",
        "municipality": "Gandia",
        "asset_class": "real_estate",
        "asset_subclass": "commercial_property",
        "is_vehicle": False,
        "official_status": "abierta",
        "publication_date": None,
        "opening_date": None,
        "closing_date": None,
        "appraisal_value": Decimal("99000.00"),
        "current_bid": None,
        "deposit": None,
        "score": 25,
        "occupancy_status": None,
        "encumbrances_summary": None,
        "description": None,
        "official_url": None,
    }

    storage.upsert_auction(Auction(**base_payload, starting_bid=Decimal("80000.00")))
    storage.upsert_auction(Auction(**base_payload, starting_bid=Decimal("75000.00")))

    stored_auctions = storage.fetch_all_auctions()

    assert len(stored_auctions) == 1
    assert stored_auctions[0].starting_bid == Decimal("75000.00")


def test_init_db_migrates_old_schema_without_dropping_timestamps(tmp_path, monkeypatch) -> None:
    """Debe migrar una tabla antigua sin perder id ni timestamps."""
    database_path = tmp_path / "legacy.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            CREATE TABLE auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                title TEXT NOT NULL,
                province TEXT NOT NULL,
                municipality TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                asset_subclass TEXT NOT NULL,
                is_vehicle BOOLEAN NOT NULL DEFAULT 0,
                official_status TEXT NOT NULL,
                publication_date TEXT,
                opening_date TEXT,
                closing_date TEXT,
                appraisal_value TEXT,
                starting_bid TEXT,
                current_bid TEXT,
                deposit TEXT,
                occupancy_status TEXT,
                encumbrances_summary TEXT,
                description TEXT,
                official_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, external_id)
            )
        """)
        conn.execute("""
            INSERT INTO auctions (
                source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, starting_bid, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "BOE", "LEGACY-1", "Vivienda en Valencia", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "abierta",
            "150000.00", "120000.00", "2026-04-01 10:00:00", "2026-04-02 11:00:00",
        ))
        conn.commit()

    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        row = conn.execute("""
            SELECT id, dedupe_key, score, created_at, updated_at
            FROM auctions
            WHERE external_id = 'LEGACY-1'
        """).fetchone()

        assert row[0] == 1
        assert row[1] == "boe::legacy-1"
        assert row[2] is None
        assert row[3] == "2026-04-01 10:00:00"
        assert row[4] == "2026-04-02 11:00:00"


def test_init_db_resolves_duplicate_dedupe_keys_by_keeping_most_recent(tmp_path, monkeypatch, caplog) -> None:
    """Debe resolver conflictos de dedupe_key antes del índice único."""
    database_path = tmp_path / "conflicts.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            CREATE TABLE auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                title TEXT NOT NULL,
                province TEXT NOT NULL,
                municipality TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                asset_subclass TEXT NOT NULL,
                is_vehicle BOOLEAN NOT NULL DEFAULT 0,
                official_status TEXT NOT NULL,
                publication_date TEXT,
                opening_date TEXT,
                closing_date TEXT,
                appraisal_value TEXT,
                starting_bid TEXT,
                current_bid TEXT,
                deposit TEXT,
                dedupe_key TEXT,
                score INTEGER,
                occupancy_status TEXT,
                encumbrances_summary TEXT,
                description TEXT,
                official_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO auctions (
                source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, starting_bid, dedupe_key, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "BOE", "A-1", "Activo antiguo", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "abierta",
            "150000.00", "120000.00", "duplicate-key", "2026-04-01 10:00:00", "2026-04-02 10:00:00",
        ))
        conn.execute("""
            INSERT INTO auctions (
                source, external_id, title, province, municipality,
                asset_class, asset_subclass, is_vehicle, official_status,
                appraisal_value, starting_bid, dedupe_key, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "BOE", "A-2", "Activo reciente", "Valencia", "Valencia",
            "real_estate", "residential_property", 0, "abierta",
            "150000.00", "110000.00", "duplicate-key", "2026-04-03 10:00:00", "2026-04-04 10:00:00",
        ))
        conn.commit()

    caplog.set_level("WARNING")
    storage.init_db()

    with sqlite3.connect(database_path) as conn:
        rows = conn.execute("""
            SELECT external_id, title, starting_bid
            FROM auctions
            WHERE dedupe_key = 'duplicate-key'
        """).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "A-2"
    assert rows[0][1] == "Activo reciente"
    assert rows[0][2] == "110000.00"
    assert "Resolved dedupe_key conflict" in caplog.text


def test_upsert_auction_preserves_created_at_and_refreshes_updated_at(tmp_path, monkeypatch) -> None:
    """Debe mantener created_at y actualizar updated_at en un upsert."""
    database_path = tmp_path / "timestamps.db"
    monkeypatch.setattr(storage, "DATABASE_PATH", database_path)
    storage.init_db()

    auction = Auction(
        source="BOE",
        external_id="SUB-200",
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("200000.00"),
        starting_bid=Decimal("120000.00"),
        current_bid=None,
        deposit=None,
        score=50,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    storage.upsert_auction(auction)

    with sqlite3.connect(database_path) as conn:
        conn.execute("""
            UPDATE auctions
            SET created_at = '2026-04-01 10:00:00',
                updated_at = '2026-04-01 10:00:00'
            WHERE external_id = 'SUB-200'
        """)
        conn.commit()

    storage.upsert_auction(auction.model_copy(update={"starting_bid": Decimal("110000.00")}))

    with sqlite3.connect(database_path) as conn:
        row = conn.execute("""
            SELECT created_at, updated_at, starting_bid
            FROM auctions
            WHERE external_id = 'SUB-200'
        """).fetchone()

    assert row[0] == "2026-04-01 10:00:00"
    assert row[1] != "2026-04-01 10:00:00"
    assert row[2] == "110000.00"


def test_upsert_auction_persists_postal_code_when_available(tmp_path, monkeypatch) -> None:
    """Debe conservar el codigo postal cuando ya viene de una extraccion fiable."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "postal.db")
    storage.init_db()

    auction = Auction(
        source="BOE",
        external_id="SUB-POSTAL-1",
        title="Lote con codigo postal",
        province="Valencia",
        municipality="Chiva",
        postal_code="46370",
        asset_class="real_estate",
        asset_subclass="land",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=None,
        starting_bid=None,
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description="CL/ PUERTO DE CONTRERAS . 46370 - CHIVA (VALENCIA)",
        official_url="https://example.com/postal",
    )

    storage.upsert_auction(auction)
    stored = storage.fetch_all_auctions()

    assert len(stored) == 1
    assert stored[0].postal_code == "46370"


def test_init_db_creates_separate_upcoming_table(tmp_path, monkeypatch) -> None:
    """Debe crear una tabla separada para próximas aperturas."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "upcoming.db")

    storage.init_db()

    with sqlite3.connect(storage.DATABASE_PATH) as conn:
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert "auctions" in table_names
    assert "upcoming_auctions" in table_names
    assert "completed_auctions" in table_names


def test_upcoming_auctions_are_persisted_separately_from_active_auctions(tmp_path, monkeypatch) -> None:
    """Debe guardar upcoming sin contaminar la tabla principal de activas."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "separate.db")
    storage.init_db()

    active_auction = Auction(
        source="BOE",
        external_id="SUB-ACTIVE-1",
        title="Subasta activa",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("120000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=Decimal("4000.00"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.com/active",
    )
    upcoming_auction = active_auction.model_copy(
        update={
            "external_id": "SUB-UPCOMING-1",
            "title": "Subasta próxima apertura",
            "official_status": "prox. apertura",
            "official_url": "https://example.com/upcoming",
        }
    )

    storage.upsert_auction(active_auction)
    storage.upsert_upcoming_auction(upcoming_auction)

    stored_active = storage.fetch_all_auctions()
    stored_upcoming = storage.fetch_all_upcoming_auctions()

    assert [auction.external_id for auction in stored_active] == ["SUB-ACTIVE-1"]
    assert [auction.external_id for auction in stored_upcoming] == ["SUB-UPCOMING-1"]


def test_completed_auctions_are_persisted_separately_from_active_and_upcoming(tmp_path, monkeypatch) -> None:
    """Debe guardar completed sin contaminar activas ni próximas aperturas."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "completed.db")
    storage.init_db()

    base_auction = Auction(
        source="BOE",
        external_id="SUB-ACTIVE-2",
        title="Subasta activa",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("180000.00"),
        starting_bid=Decimal("120000.00"),
        current_bid=Decimal("130000.00"),
        deposit=Decimal("6000.00"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.com/active-2",
    )
    upcoming_auction = base_auction.model_copy(
        update={
            "external_id": "SUB-UPCOMING-2",
            "title": "Subasta próxima apertura",
            "official_status": "prox. apertura",
            "official_url": "https://example.com/upcoming-2",
        }
    )
    completed_auction = base_auction.model_copy(
        update={
            "external_id": "SUB-COMPLETED-1",
            "title": "Subasta completada",
            "official_status": "concluida en portal de subastas",
            "official_url": "https://example.com/completed-1",
        }
    )

    storage.upsert_auction(base_auction)
    storage.upsert_upcoming_auction(upcoming_auction)
    storage.upsert_completed_auction(completed_auction)

    stored_active = storage.fetch_all_auctions()
    stored_upcoming = storage.fetch_all_upcoming_auctions()
    stored_completed = storage.fetch_all_completed_auctions()

    assert [auction.external_id for auction in stored_active] == ["SUB-ACTIVE-2"]
    assert [auction.external_id for auction in stored_upcoming] == ["SUB-UPCOMING-2"]
    assert [auction.external_id for auction in stored_completed] == ["SUB-COMPLETED-1"]


def test_upsert_completed_auction_persists_opening_date_postal_code_and_current_bid(tmp_path, monkeypatch) -> None:
    """Debe conservar opening_date, postal_code y current_bid en la tabla de completed."""
    monkeypatch.setattr(storage, "DATABASE_PATH", tmp_path / "completed_fields.db")
    storage.init_db()

    completed_auction = Auction(
        source="BOE",
        external_id="SUB-COMPLETED-POSTAL-1::lot:1",
        title="Lote completado",
        province="Valencia",
        municipality="Sagunto",
        postal_code="46520",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="concluida en portal de subastas",
        publication_date=None,
        opening_date=date(2026, 3, 20),
        closing_date=date(2026, 4, 9),
        appraisal_value=Decimal("90500.00"),
        starting_bid=Decimal("90500.00"),
        current_bid=Decimal("102500.00"),
        deposit=Decimal("4525.00"),
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.com/completed-lot",
    )

    storage.upsert_completed_auction(completed_auction)
    stored_completed = storage.fetch_all_completed_auctions()

    assert len(stored_completed) == 1
    assert stored_completed[0].opening_date == date(2026, 3, 20)
    assert stored_completed[0].postal_code == "46520"
    assert stored_completed[0].current_bid == Decimal("102500.00")
