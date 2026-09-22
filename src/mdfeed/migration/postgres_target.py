from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

from mdfeed.config import Config
from mdfeed.storage.catalog import CATALOG, TableCatalog
from mdfeed.storage.db import StorageConfigurationError, validate_storage_config

from .time import datetime_to_epoch_us, epoch_us_to_datetime
from .types import (
    MigrationPreflightError,
    PgRow,
    PgValue,
    SqlRow,
    SqlValue,
    StorageDigest,
    TableDigest,
)


def connect_postgres(dsn: str):
    cfg = Config()
    cfg.storage_backend = "postgres"
    cfg.storage_profile = os.getenv("MDFEED_STORAGE_PROFILE", "production")
    cfg.pg_dsn = dsn
    try:
        validate_storage_config(cfg)
    except StorageConfigurationError as exc:
        raise MigrationPreflightError(str(exc)) from exc
    try:
        import psycopg2
    except ImportError as exc:
        raise MigrationPreflightError("psycopg2 is required for PostgreSQL migration") from exc
    return psycopg2.connect(dsn)


def init_target_schema(dsn: str) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    conn = connect_postgres(dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(schema_path.read_text(encoding="utf-8"))
    finally:
        conn.close()


def canonical_postgres_digest(dsn: str) -> StorageDigest:
    conn = connect_postgres(dsn)
    try:
        return canonical_postgres_digest_from_connection(conn)
    finally:
        conn.close()


def canonical_postgres_digest_from_connection(conn) -> StorageDigest:
    tables = tuple(_postgres_table_digest(conn, table) for table in CATALOG)
    fingerprint = _fingerprint(tables)
    return StorageDigest(fingerprint=fingerprint, tables=tables)


def assert_empty_or_matching(conn, run_id: str, source_fingerprint: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='migration_runs')"
        )
        has_runs = bool(cur.fetchone()[0])
        if not has_runs:
            return
        cur.execute(
            "SELECT source_fingerprint FROM migration_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
        if row is not None and row[0] != source_fingerprint:
            raise MigrationPreflightError("migration run source fingerprint changed")
        if row is not None:
            return
        for table in CATALOG:
            cur.execute(f"SELECT COUNT(*) FROM {table.name}")
            if int(cur.fetchone()[0]) > 0:
                raise MigrationPreflightError("target is not empty and run does not match")


def insert_chunk(conn, table: TableCatalog, rows: list[SqlRow]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(table.columns))
    sql = (
        f"INSERT INTO {table.name} ({', '.join(table.postgres_columns)}) "
        f"VALUES ({placeholders})"
    )
    converted = [_postgres_row(table, row) for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, converted)


def _postgres_table_digest(conn, table: TableCatalog) -> TableDigest:
    with conn.cursor(name=f"mdfeed_digest_{table.name}") as cur:
        cur.itersize = 10_000
        cur.execute(
            f"SELECT {', '.join(table.postgres_columns)} FROM {table.name} "
            f"ORDER BY {_order_columns(table)}"
        )
        return _digest_rows(table, cur)


def _postgres_row(table: TableCatalog, row: SqlRow) -> PgRow:
    values: list[PgValue] = []
    for index, column in enumerate(table.columns):
        value = row[index]
        if value is None:
            values.append(None)
        elif table.name == "instruments" and column.name == "active":
            values.append(int(value) == 1)
        elif column.time_encoding is not None:
            values.append(epoch_us_to_datetime(int(value)))
        else:
            values.append(value)
    return tuple(values)


def _normalize_postgres_row(table: TableCatalog, row: PgRow) -> SqlRow:
    values: list[SqlValue] = []
    for index, column in enumerate(table.columns):
        value = row[index]
        if value is None:
            values.append(None)
        elif column.time_encoding is not None:
            if not isinstance(value, dt.datetime):
                raise MigrationPreflightError("PostgreSQL timestamp column is not a datetime")
            values.append(datetime_to_epoch_us(value))
        elif column.name == "active":
            values.append(1 if value else 0)
        elif isinstance(value, dt.datetime):
            raise MigrationPreflightError("PostgreSQL datetime value appeared in a non-time column")
        else:
            values.append(value)
    return tuple(values)


def _digest_rows(table: TableCatalog, cur) -> TableDigest:
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
            row = _normalize_postgres_row(table, tuple(raw_row))
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
    for column in table.postgres_columns:
        if column not in ordered:
            ordered.append(column)
    return ", ".join(ordered)
