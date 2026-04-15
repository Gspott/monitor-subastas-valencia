"""Database storage module for auction data using SQLite."""

import logging
import sqlite3
from datetime import date
from decimal import Decimal

from .config import DATABASE_PATH
from .dedupe import build_dedupe_key
from .models import Auction
from .status import is_active_status


TABLE_COLUMNS = [
    "dedupe_key",
    "source",
    "external_id",
    "title",
    "province",
    "municipality",
    "postal_code",
    "asset_class",
    "asset_subclass",
    "is_vehicle",
    "official_status",
    "publication_date",
    "opening_date",
    "closing_date",
    "appraisal_value",
    "starting_bid",
    "current_bid",
    "deposit",
    "score",
    "occupancy_status",
    "encumbrances_summary",
    "description",
    "official_url",
]
MISSING_EXTERNAL_ID_PREFIX = "__missing_external_id__::"
ACTIVE_AUCTIONS_TABLE = "auctions"
UPCOMING_AUCTIONS_TABLE = "upcoming_auctions"
COMPLETED_AUCTIONS_TABLE = "completed_auctions"


logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    return sqlite3.connect(DATABASE_PATH)


def init_db() -> None:
    """Initialize the database tables used by the monitor."""
    with get_connection() as conn:
        conn.execute(_create_auctions_table_sql(ACTIVE_AUCTIONS_TABLE))
        conn.execute(_create_auctions_table_sql(UPCOMING_AUCTIONS_TABLE))
        conn.execute(_create_auctions_table_sql(COMPLETED_AUCTIONS_TABLE))
        _ensure_compatible_schema(conn, ACTIVE_AUCTIONS_TABLE)
        _ensure_compatible_schema(conn, UPCOMING_AUCTIONS_TABLE)
        _ensure_compatible_schema(conn, COMPLETED_AUCTIONS_TABLE)
        _create_indexes(conn)
        conn.commit()


def upsert_auction(auction: Auction) -> None:
    """Insert or update an auction in the database."""
    _upsert_auction_in_table(auction, ACTIVE_AUCTIONS_TABLE)


def upsert_upcoming_auction(auction: Auction) -> None:
    """Insert or update an upcoming auction in the dedicated table."""
    _upsert_auction_in_table(auction, UPCOMING_AUCTIONS_TABLE)


def upsert_completed_auction(auction: Auction) -> None:
    """Insert or update a completed auction in the dedicated table."""
    _upsert_auction_in_table(auction, COMPLETED_AUCTIONS_TABLE)


def _upsert_auction_in_table(auction: Auction, table_name: str) -> None:
    """Insert or update an auction in the requested table."""
    with get_connection() as conn:
        dedupe_key = build_dedupe_key(auction)
        if dedupe_key is None:
            raise ValueError("Auction cannot be stored without a dedupe identity.")
        external_id_value = _serialize_external_id_for_storage(conn, auction, dedupe_key, table_name)

        # Convert dates to ISO format strings
        pub_date = auction.publication_date.isoformat() if auction.publication_date else None
        open_date = auction.opening_date.isoformat() if auction.opening_date else None
        close_date = auction.closing_date.isoformat() if auction.closing_date else None

        # Convert decimals to strings
        appraisal = str(auction.appraisal_value) if auction.appraisal_value is not None else None
        starting = str(auction.starting_bid) if auction.starting_bid is not None else None
        current = str(auction.current_bid) if auction.current_bid is not None else None
        dep = str(auction.deposit) if auction.deposit is not None else None

        conn.execute(f"""
            INSERT INTO {table_name} (
                dedupe_key, source, external_id, title, province, municipality, postal_code,
                asset_class, asset_subclass, is_vehicle, official_status,
                publication_date, opening_date, closing_date,
                appraisal_value, starting_bid, current_bid, deposit, score,
                occupancy_status, encumbrances_summary, description, official_url,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                title = excluded.title,
                province = excluded.province,
                municipality = excluded.municipality,
                postal_code = excluded.postal_code,
                asset_class = excluded.asset_class,
                asset_subclass = excluded.asset_subclass,
                is_vehicle = excluded.is_vehicle,
                official_status = excluded.official_status,
                publication_date = excluded.publication_date,
                opening_date = excluded.opening_date,
                closing_date = excluded.closing_date,
                appraisal_value = excluded.appraisal_value,
                starting_bid = excluded.starting_bid,
                current_bid = excluded.current_bid,
                deposit = excluded.deposit,
                score = excluded.score,
                external_id = COALESCE(excluded.external_id, {table_name}.external_id),
                occupancy_status = excluded.occupancy_status,
                encumbrances_summary = excluded.encumbrances_summary,
                description = excluded.description,
                official_url = excluded.official_url,
                updated_at = CURRENT_TIMESTAMP
        """, (
            str(dedupe_key), auction.source, external_id_value, auction.title, auction.province, auction.municipality,
            auction.postal_code,
            auction.asset_class, auction.asset_subclass, auction.is_vehicle, auction.official_status,
            pub_date, open_date, close_date,
            appraisal, starting, current, dep, auction.score,
            auction.occupancy_status, auction.encumbrances_summary, auction.description, auction.official_url
        ))
        conn.commit()


def fetch_active_valencia_auctions() -> list[Auction]:
    """Fetch active auctions for Valencia from the database."""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT
                source,
                external_id,
                title,
                province,
                municipality,
                postal_code,
                asset_class,
                asset_subclass,
                is_vehicle,
                official_status,
                publication_date,
                opening_date,
                closing_date,
                appraisal_value,
                starting_bid,
                current_bid,
                deposit,
                score,
                occupancy_status,
                encumbrances_summary,
                description,
                official_url
            FROM {ACTIVE_AUCTIONS_TABLE}
            WHERE province = ?
              AND is_vehicle = 0
        """, ("Valencia",)).fetchall()

    auctions = [_row_to_auction(row) for row in rows]
    return [auction for auction in auctions if is_active_status(auction.official_status)]


def fetch_all_auctions() -> list[Auction]:
    """Fetch all auctions currently stored in the database."""
    return _fetch_all_from_table(ACTIVE_AUCTIONS_TABLE)


def fetch_all_upcoming_auctions() -> list[Auction]:
    """Fetch all upcoming auctions currently stored in the database."""
    return _fetch_all_from_table(UPCOMING_AUCTIONS_TABLE)


def fetch_all_completed_auctions() -> list[Auction]:
    """Fetch all completed auctions currently stored in the database."""
    return _fetch_all_from_table(COMPLETED_AUCTIONS_TABLE)


def _fetch_all_from_table(table_name: str) -> list[Auction]:
    """Fetch all auctions currently stored in one database table."""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT
                source,
                external_id,
                title,
                province,
                municipality,
                postal_code,
                asset_class,
                asset_subclass,
                is_vehicle,
                official_status,
                publication_date,
                opening_date,
                closing_date,
                appraisal_value,
                starting_bid,
                current_bid,
                deposit,
                score,
                occupancy_status,
                encumbrances_summary,
                description,
                official_url
            FROM {table_name}
        """).fetchall()

    return [_row_to_auction(row) for row in rows]


def _ensure_compatible_schema(conn: sqlite3.Connection, table_name: str) -> None:
    """Ensure one SQLite table matches the current Auction model."""
    column_map = _get_column_map(conn, table_name)
    if not column_map:
        return

    _add_missing_columns(conn, table_name, column_map)
    _backfill_timestamp_columns(conn, table_name)
    _backfill_dedupe_keys(conn, table_name)
    _resolve_dedupe_key_conflicts(conn, table_name)


def _create_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes that support the main read patterns."""
    _create_unique_dedupe_key_index(conn, ACTIVE_AUCTIONS_TABLE)
    _create_unique_dedupe_key_index(conn, UPCOMING_AUCTIONS_TABLE)
    _create_unique_dedupe_key_index(conn, COMPLETED_AUCTIONS_TABLE)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{ACTIVE_AUCTIONS_TABLE}_active_valencia
        ON {ACTIVE_AUCTIONS_TABLE} (province, official_status, is_vehicle)
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{UPCOMING_AUCTIONS_TABLE}_province_status
        ON {UPCOMING_AUCTIONS_TABLE} (province, official_status, is_vehicle)
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{COMPLETED_AUCTIONS_TABLE}_province_status
        ON {COMPLETED_AUCTIONS_TABLE} (province, official_status, is_vehicle)
    """)


def _row_to_auction(row: sqlite3.Row | tuple) -> Auction:
    """Convert a database row into an Auction object."""
    return Auction(
        source=row[0],
        external_id=_deserialize_external_id(row[1]),
        title=row[2],
        province=row[3],
        municipality=row[4],
        postal_code=row[5],
        asset_class=row[6],
        asset_subclass=row[7],
        is_vehicle=bool(row[8]),
        official_status=row[9],
        publication_date=_parse_date(row[10]),
        opening_date=_parse_date(row[11]),
        closing_date=_parse_date(row[12]),
        appraisal_value=_parse_decimal(row[13]),
        starting_bid=_parse_decimal(row[14]),
        current_bid=_parse_decimal(row[15]),
        deposit=_parse_decimal(row[16]),
        score=row[17],
        occupancy_status=row[18],
        encumbrances_summary=row[19],
        description=row[20],
        official_url=row[21],
    )


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


def _create_auctions_table_sql(table_name: str) -> str:
    """Build the CREATE TABLE statement for auctions."""
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedupe_key TEXT,
            source TEXT NOT NULL,
            external_id TEXT,
            title TEXT NOT NULL,
            province TEXT NOT NULL,
            municipality TEXT NOT NULL,
            postal_code TEXT,
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
            score INTEGER,
            occupancy_status TEXT,
            encumbrances_summary TEXT,
            description TEXT,
            official_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, external_id)
        )
    """


def _get_column_map(conn: sqlite3.Connection, table_name: str) -> dict[str, tuple]:
    """Read the current column metadata for one auction table."""
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {column[1]: column for column in columns}


def _add_missing_columns(conn: sqlite3.Connection, table_name: str, column_map: dict[str, tuple]) -> None:
    """Add missing columns incrementally without rebuilding the table."""
    missing_columns = {
        "dedupe_key": "TEXT",
        "score": "INTEGER",
        "postal_code": "TEXT",
    }

    for column_name, column_definition in missing_columns.items():
        if column_name in column_map:
            continue

        # Añadir columnas de forma incremental para no tocar los datos existentes.
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")


def _backfill_timestamp_columns(conn: sqlite3.Connection, table_name: str) -> None:
    """Backfill timestamp columns if any existing rows have null values."""
    conn.execute(f"""
        UPDATE {table_name}
        SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)
        WHERE created_at IS NULL
    """)
    conn.execute(f"""
        UPDATE {table_name}
        SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
        WHERE updated_at IS NULL
    """)


def _backfill_dedupe_keys(conn: sqlite3.Connection, table_name: str) -> None:
    """Populate missing dedupe_key values for existing rows."""
    rows = conn.execute(f"""
        SELECT
            rowid,
            source,
            external_id,
            title,
            province,
            municipality,
            postal_code,
            asset_class,
            asset_subclass,
            is_vehicle,
            official_status,
            publication_date,
            opening_date,
            closing_date,
            appraisal_value,
            starting_bid,
            current_bid,
            deposit,
            score,
            occupancy_status,
            encumbrances_summary,
            description,
            official_url
        FROM {table_name}
        WHERE dedupe_key IS NULL OR TRIM(dedupe_key) = ''
    """).fetchall()

    for row in rows:
        auction = _row_to_auction(row[1:])
        dedupe_key = build_dedupe_key(auction)
        if dedupe_key is None:
            logger.warning("Skipping dedupe_key backfill for rowid=%s due to missing identity.", row[0])
            continue

        conn.execute(
            f"UPDATE {table_name} SET dedupe_key = ? WHERE rowid = ?",
            (dedupe_key, row[0]),
        )


def _resolve_dedupe_key_conflicts(conn: sqlite3.Connection, table_name: str) -> None:
    """Resolve duplicate dedupe_key values before creating the unique index."""
    duplicate_groups = conn.execute(f"""
        SELECT dedupe_key, COUNT(*)
        FROM {table_name}
        WHERE dedupe_key IS NOT NULL AND TRIM(dedupe_key) != ''
        GROUP BY dedupe_key
        HAVING COUNT(*) > 1
    """).fetchall()

    for dedupe_key, _count in duplicate_groups:
        rows = conn.execute(f"""
            SELECT
                rowid,
                updated_at,
                created_at
            FROM {table_name}
            WHERE dedupe_key = ?
            ORDER BY
                COALESCE(updated_at, created_at, '') DESC,
                COALESCE(created_at, '') DESC,
                rowid DESC
        """, (dedupe_key,)).fetchall()

        keeper_rowid = rows[0][0]
        duplicate_rowids = [row[0] for row in rows[1:]]

        logger.warning(
            "Resolved dedupe_key conflict for %s by keeping rowid=%s and deleting rowids=%s.",
            dedupe_key,
            keeper_rowid,
            duplicate_rowids,
        )

        # Conservar el registro más reciente y eliminar duplicados antiguos.
        conn.executemany(
            f"DELETE FROM {table_name} WHERE rowid = ?",
            [(rowid,) for rowid in duplicate_rowids],
        )


def _create_unique_dedupe_key_index(conn: sqlite3.Connection, table_name: str) -> None:
    """Create the unique index for dedupe_key after conflicts are resolved."""
    conn.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_dedupe_key
        ON {table_name} (dedupe_key)
    """)


def _serialize_external_id_for_storage(
    conn: sqlite3.Connection,
    auction: Auction,
    dedupe_key: str,
    table_name: str,
) -> str | None:
    """Serialize external_id safely for both new and legacy schemas."""
    if auction.external_id is not None:
        return auction.external_id

    if _external_id_is_not_null(conn, table_name):
        # Mantener compatibilidad con esquemas antiguos sin reconstruir la tabla.
        return f"{MISSING_EXTERNAL_ID_PREFIX}{dedupe_key}"

    return None


def _deserialize_external_id(value: str | None) -> str | None:
    """Convert legacy placeholder external IDs back to None."""
    if value is None:
        return None

    if value.startswith(MISSING_EXTERNAL_ID_PREFIX):
        return None

    return value


def _external_id_is_not_null(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check whether one table still enforces external_id NOT NULL."""
    column_map = _get_column_map(conn, table_name)
    external_id_metadata = column_map.get("external_id")
    if external_id_metadata is None:
        return False

    return bool(external_id_metadata[3])
