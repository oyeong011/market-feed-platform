from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from mdfeed.storage.catalog import CATALOG

from .postgres_target import (
    assert_empty_or_matching,
    canonical_postgres_digest,
    connect_postgres,
    init_target_schema,
    insert_chunk,
)
from .sqlite_source import (
    canonical_sqlite_digest,
    chunk_content_hash,
    open_sqlite_source,
    read_sqlite_chunk,
    read_sqlite_rowid_range,
)
from .types import MigrationPreflightError, MigrationReceipt, StorageDigest


def run_migration(
    source: Path,
    target_dsn: str,
    run_id: str,
    *,
    chunk_size: int = 50_000,
    init_target: bool = False,
) -> MigrationReceipt:
    source_digest = canonical_sqlite_digest(source)
    if init_target:
        init_target_schema(target_dsn)
    conn = connect_postgres(target_dsn)
    source_conn = open_sqlite_source(source)
    try:
        _acquire_target_lock(conn)
        assert_empty_or_matching(conn, run_id, source_digest.fingerprint)
        _validate_checkpoints(conn, source_conn, run_id)
        _ensure_run(conn, run_id, source_digest.fingerprint)
        for table in CATALOG:
            _migrate_table(conn, source_conn, run_id, source_digest, table.name, chunk_size)
        final_source_digest = canonical_sqlite_digest(source)
        if final_source_digest.fingerprint != source_digest.fingerprint:
            raise RuntimeError("migration source changed during copy")
        target_digest = canonical_postgres_digest(target_dsn)
        if source_digest.fingerprint != target_digest.fingerprint:
            raise RuntimeError("migration verify failed: source and target digests differ")
        _complete_run(conn, run_id)
    finally:
        _release_target_lock(conn)
        source_conn.close()
        conn.close()
    return MigrationReceipt(
        run_id=run_id,
        source_fingerprint=source_digest.fingerprint,
        completed=True,
        source=source_digest,
        target=target_digest,
    )


def migration_status(source: Path, target_dsn: str, run_id: str) -> MigrationReceipt:
    source_digest = canonical_sqlite_digest(source)
    target_digest = canonical_postgres_digest(target_dsn)
    conn = connect_postgres(target_dsn)
    try:
        state = _run_state(conn, run_id, source_digest.fingerprint)
    finally:
        conn.close()
    completed = state == "COMPLETED" and source_digest.fingerprint == target_digest.fingerprint
    return MigrationReceipt(
        run_id=run_id,
        source_fingerprint=source_digest.fingerprint,
        completed=completed,
        source=source_digest,
        target=target_digest,
    )


def verify_migration(source: Path, target_dsn: str, run_id: str) -> MigrationReceipt:
    status = migration_status(source, target_dsn, run_id)
    if not status.completed:
        raise RuntimeError("migration verification failed")
    return status


def _ensure_run(conn, run_id: str, source_fingerprint: str) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO migration_runs "
            "(run_id, source_fingerprint, state, started_at, updated_at) "
            "VALUES (%s, %s, 'RUNNING', now(), now()) "
            "ON CONFLICT (run_id) DO UPDATE SET updated_at = now()",
            (run_id, source_fingerprint),
        )


def _validate_checkpoints(
    conn,
    source_conn: sqlite3.Connection,
    run_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, last_source_rowid, row_count, chunk_hash "
            ", first_source_rowid, chunk_row_count "
            "FROM migration_checkpoints WHERE run_id = %s",
            (run_id,),
        )
        rows = cur.fetchall()
    for row in rows:
        table_name, last_rowid, row_count, chunk_hash, first_rowid, chunk_row_count = row
        expected = _checkpoint_hash_from_source(
            source_conn,
            str(table_name),
            int(first_rowid),
            int(last_rowid),
            int(chunk_row_count),
        )
        if chunk_hash != expected:
            raise RuntimeError("checkpoint tamper detected")
        if _target_count(conn, str(table_name)) != int(row_count):
            raise RuntimeError("checkpoint target row count mismatch")


def _migrate_table(
    conn,
    source_conn: sqlite3.Connection,
    run_id: str,
    source_digest: StorageDigest,
    table_name: str,
    chunk_size: int,
) -> None:
    table = next(table for table in CATALOG if table.name == table_name)
    while True:
        after_rowid = _checkpoint_rowid(conn, run_id, table.name)
        chunk = read_sqlite_chunk(source_conn, table, after_rowid, chunk_size)
        if not chunk:
            return
        rows = [row for _rowid, row in chunk]
        first_rowid = chunk[0][0]
        last_rowid = chunk[-1][0]
        row_count = _checkpoint_count(conn, run_id, table.name) + len(rows)
        chunk_hash = chunk_content_hash(chunk)
        with conn:
            insert_chunk(conn, table, rows)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO migration_checkpoints "
                    "(run_id, table_name, first_source_rowid, last_source_rowid, "
                    "chunk_row_count, row_count, chunk_hash, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, now()) "
                    "ON CONFLICT (run_id, table_name) DO UPDATE SET "
                    "first_source_rowid=EXCLUDED.first_source_rowid, "
                    "last_source_rowid=EXCLUDED.last_source_rowid, "
                    "chunk_row_count=EXCLUDED.chunk_row_count, "
                    "row_count=EXCLUDED.row_count, chunk_hash=EXCLUDED.chunk_hash, "
                    "updated_at=EXCLUDED.updated_at",
                    (
                        run_id,
                        table.name,
                        first_rowid,
                        last_rowid,
                        len(rows),
                        row_count,
                        chunk_hash,
                    ),
                )
        if row_count > source_digest.table(table.name).row_count:
            raise MigrationPreflightError("checkpoint row count exceeds source count")


def _checkpoint_rowid(conn, run_id: str, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_source_rowid FROM migration_checkpoints "
            "WHERE run_id = %s AND table_name = %s",
            (run_id, table_name),
        )
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _checkpoint_count(conn, run_id: str, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT row_count FROM migration_checkpoints "
            "WHERE run_id = %s AND table_name = %s",
            (run_id, table_name),
        )
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _complete_run(conn, run_id: str) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE migration_runs SET state='COMPLETED', completed_at=now(), "
            "updated_at=now() WHERE run_id=%s",
            (run_id,),
        )


def _checkpoint_hash_from_source(
    source_conn: sqlite3.Connection,
    table_name: str,
    first_rowid: int,
    last_rowid: int,
    chunk_row_count: int,
) -> str:
    table = next(table for table in CATALOG if table.name == table_name)
    chunk = read_sqlite_rowid_range(source_conn, table, first_rowid, last_rowid)
    if len(chunk) != chunk_row_count:
        raise RuntimeError("checkpoint source row count mismatch")
    return chunk_content_hash(chunk)


def _target_count(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = cur.fetchone()
    return int(row[0])


def _run_state(conn, run_id: str, source_fingerprint: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state, source_fingerprint FROM migration_runs WHERE run_id = %s",
            (run_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    if row[1] != source_fingerprint:
        raise MigrationPreflightError("migration run source fingerprint changed")
    return str(row[0])


def _acquire_target_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext('mdfeed:migration-target'))")
        row = cur.fetchone()
    if row is None or not row[0]:
        raise RuntimeError("another migration is already running for this target")


def _release_target_lock(conn) -> None:
    if conn.closed:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(hashtext('mdfeed:migration-target'))")


def receipt_json(receipt: MigrationReceipt) -> str:
    return json.dumps(receipt.to_json(), ensure_ascii=False, sort_keys=True)
