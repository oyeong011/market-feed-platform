from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from mdfeed.storage.catalog import CATALOG, TableCatalog

from .time import parse_aware_text
from .types import MigrationPreflightError, SqlRow, SqlValue, StorageDigest, TableDigest

KNOWN_VENUES = frozenset({"UPBIT", "BINANCE", "KIS", "KRX", "KRX-IDX", "RATE"})


def open_sqlite_source(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise MigrationPreflightError(f"SQLite source does not exist: {path}")
    if path.with_name(f"{path.name}-wal").exists() and path.with_name(f"{path.name}-wal").stat().st_size:
        raise MigrationPreflightError("SQLite source has an active WAL; checkpoint it before immutable access")
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise MigrationPreflightError("SQLite source could not be opened read-only") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise MigrationPreflightError("SQLite source is not a readable database") from exc
    return conn


def canonical_sqlite_digest(path: Path) -> StorageDigest:
    conn = open_sqlite_source(path)
    try:
        _preflight_values(conn)
        tables = tuple(_sqlite_table_digest(conn, table) for table in CATALOG)
    finally:
        conn.close()
    fingerprint = _fingerprint(tables)
    return StorageDigest(fingerprint=fingerprint, tables=tables)


def plan_sqlite_source(path: Path) -> StorageDigest:
    return canonical_sqlite_digest(path)


def read_sqlite_chunk(
    conn: sqlite3.Connection,
    table: TableCatalog,
    after_rowid: int,
    limit: int,
) -> list[tuple[int, SqlRow]]:
    sql = (
        f"SELECT rowid, {', '.join(table.sqlite_columns)} "
        f"FROM {table.name} WHERE rowid > ? ORDER BY rowid LIMIT ?"
    )
    rows = conn.execute(sql, (after_rowid, limit)).fetchall()
    return [
        (int(row["rowid"]), _normalize_sqlite_row(table, row))
        for row in rows
    ]


def read_sqlite_rowid_range(
    conn: sqlite3.Connection,
    table: TableCatalog,
    first_rowid: int,
    last_rowid: int,
) -> list[tuple[int, SqlRow]]:
    sql = (
        f"SELECT rowid, {', '.join(table.sqlite_columns)} "
        f"FROM {table.name} WHERE rowid BETWEEN ? AND ? ORDER BY rowid"
    )
    rows = conn.execute(sql, (first_rowid, last_rowid)).fetchall()
    return [
        (int(row["rowid"]), _normalize_sqlite_row(table, row))
        for row in rows
    ]


def chunk_content_hash(chunk: list[tuple[int, SqlRow]]) -> str:
    hasher = hashlib.sha256()
    for rowid, row in chunk:
        hasher.update(json.dumps((rowid, row), separators=(",", ":"), ensure_ascii=True).encode())
        hasher.update(b"\n")
    return hasher.hexdigest()


def _sqlite_table_digest(conn: sqlite3.Connection, table: TableCatalog) -> TableDigest:
    _assert_columns(conn, table)
    cur = conn.execute(
        f"SELECT {', '.join(table.sqlite_columns)} FROM {table.name} "
        f"ORDER BY {_order_columns(table)}, rowid"
    )
    return _digest_rows(table, cur)


def _assert_columns(conn: sqlite3.Connection, table: TableCatalog) -> None:
    found = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table.name})")]
    if found != table.sqlite_columns:
        raise MigrationPreflightError(f"SQLite schema mismatch for {table.name}")


def _normalize_sqlite_row(table: TableCatalog, row: sqlite3.Row) -> SqlRow:
    values: list[SqlValue] = []
    for column in table.columns:
        raw = row[column.name]
        if raw is None:
            values.append(None)
        elif column.time_encoding is not None and table.name == "instruments":
            if not isinstance(raw, str):
                raise MigrationPreflightError("instrument timestamp must be timezone text or NULL")
            values.append(parse_aware_text(raw))
        elif table.name == "instruments" and column.name == "active":
            active = int(raw)
            if active not in (0, 1):
                raise MigrationPreflightError("instruments.active must be 0 or 1")
            values.append(active)
        else:
            values.append(raw)
    return tuple(values)


def _digest_rows(table: TableCatalog, cur: sqlite3.Cursor) -> TableDigest:
    hasher = hashlib.sha256()
    null_counts = {column.name: 0 for column in table.columns if column.nullable}
    min_times: dict[str, int | None] = {column: None for column in table.timestamp_columns}
    max_times: dict[str, int | None] = {column: None for column in table.timestamp_columns}
    row_count = 0
    while True:
        batch = cur.fetchmany(10_000)
        if not batch:
            break
        for raw_row in batch:
            row = _normalize_sqlite_row(table, raw_row)
            row_count += 1
            hasher.update(json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode())
            hasher.update(b"\n")
            for index, column in enumerate(table.columns):
                value = row[index]
                if column.nullable and value is None:
                    null_counts[column.name] += 1
                if column.name in min_times and value is not None:
                    time_value = int(value)
                    current_min = min_times[column.name]
                    current_max = max_times[column.name]
                    min_times[column.name] = time_value if current_min is None else min(current_min, time_value)
                    max_times[column.name] = time_value if current_max is None else max(current_max, time_value)
    return TableDigest(
        name=table.name,
        row_count=row_count,
        null_counts=null_counts,
        min_times=min_times,
        max_times=max_times,
        content_hash=hasher.hexdigest(),
    )


def _fingerprint(tables: tuple[TableDigest, ...]) -> str:
    payload = {table.name: table.to_json() for table in tables}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _order_columns(table: TableCatalog) -> str:
    ordered = list(table.verification_order)
    for column in table.sqlite_columns:
        if column not in ordered:
            ordered.append(column)
    return ", ".join(ordered)


def _preflight_values(conn: sqlite3.Connection) -> None:
    for table in CATALOG:
        rowid = conn.execute(
            f"SELECT rowid FROM {table.name} WHERE rowid <= 0 LIMIT 1"
        ).fetchone()
        if rowid is not None:
            raise MigrationPreflightError(f"{table.name} contains rowid <= 0")
        if "venue" not in table.sqlite_columns:
            continue
        rows = conn.execute(
            f"SELECT DISTINCT venue FROM {table.name} WHERE venue IS NOT NULL"
        ).fetchall()
        unknown = sorted(str(row[0]) for row in rows if str(row[0]) not in KNOWN_VENUES)
        if unknown:
            raise MigrationPreflightError(
                f"unknown venue in {table.name}: {', '.join(unknown)}"
            )
