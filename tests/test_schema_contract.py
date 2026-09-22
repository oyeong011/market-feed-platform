from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from mdfeed.storage.catalog import (
    APPLICATION_TABLES,
    RECEIPT_TABLES,
    SQLITE_RECEIPT_TABLES,
    catalog_table,
)

ROOT = Path(__file__).resolve().parents[1]
SQLITE_SCHEMA = ROOT / "src" / "mdfeed" / "storage" / "schema_sqlite.sql"
POSTGRES_SCHEMA = ROOT / "src" / "mdfeed" / "storage" / "schema.sql"


def _sqlite_columns(schema: Path, table: str, tmp_path: Path) -> list[str]:
    path = tmp_path / "schema.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(schema.read_text(encoding="utf-8"))
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    finally:
        conn.close()
    return [str(row[1]) for row in rows]


def _postgres_table_body(schema_text: str, table: str) -> str:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS {re.escape(table)} \((.*?)\);",
        schema_text,
        re.IGNORECASE | re.DOTALL,
    )
    assert match is not None, f"{table} table is missing"
    return match.group(1)


def _postgres_columns(schema_text: str, table: str) -> list[str]:
    columns: list[str] = []
    for raw_line in _postgres_table_body(schema_text, table).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        first = line.split(maxsplit=1)[0].strip('"')
        if first.upper() in {"PRIMARY", "CONSTRAINT", "CHECK", "UNIQUE", "FOREIGN"}:
            continue
        columns.append(first)
    return columns


def test_catalog_lists_the_eight_application_tables_in_order() -> None:
    assert APPLICATION_TABLES == (
        "instruments",
        "trades",
        "book_top",
        "bars_1m",
        "signals",
        "feed_stats",
        "latest",
        "quality_events",
    )


def test_schema_parity_matches_catalog_columns(tmp_path: Path) -> None:
    pg_schema = POSTGRES_SCHEMA.read_text(encoding="utf-8")
    for table_name in APPLICATION_TABLES:
        table = catalog_table(table_name)
        assert _sqlite_columns(SQLITE_SCHEMA, table_name, tmp_path) == table.sqlite_columns
        assert _postgres_columns(pg_schema, table_name) == table.postgres_columns


def test_receipt_and_gap_tables_have_expected_scope(tmp_path: Path) -> None:
    sqlite_conn = sqlite3.connect(tmp_path / "receipt.db")
    try:
        sqlite_conn.executescript(SQLITE_SCHEMA.read_text(encoding="utf-8"))
        sqlite_tables = {
            str(row[0])
            for row in sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        sqlite_conn.close()
    pg_schema = POSTGRES_SCHEMA.read_text(encoding="utf-8")

    assert SQLITE_RECEIPT_TABLES <= sqlite_tables
    assert RECEIPT_TABLES <= set(re.findall(r"CREATE TABLE IF NOT EXISTS ([a-z_]+)", pg_schema))
    assert "first_source_rowid" in _postgres_columns(pg_schema, "migration_checkpoints")
    assert "chunk_row_count" in _postgres_columns(pg_schema, "migration_checkpoints")
    assert "required_scope_json" in _postgres_columns(pg_schema, "data_gaps")


def test_no_automatic_retention_or_database_timezone_mutation() -> None:
    pg_schema = POSTGRES_SCHEMA.read_text(encoding="utf-8").lower()
    assert "add_retention_policy" not in pg_schema
    assert "alter database" not in pg_schema


def test_catalog_timestamp_and_numeric_type_contracts() -> None:
    money_like = {
        "price",
        "qty",
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "spread_bp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "notional",
        "vwap",
        "strength",
        "ref_price",
        "value",
    }
    for table_name in APPLICATION_TABLES:
        table = catalog_table(table_name)
        for column in table.timestamp_columns:
            mapping = table.column(column)
            assert mapping.sqlite_type in {"INTEGER", "TEXT"}
            assert mapping.time_encoding == "epoch_us <-> aware UTC datetime"
        for column in table.columns:
            if column.name in money_like:
                assert column.sqlite_type == "REAL"
                assert column.postgres_type == "DOUBLE PRECISION"
