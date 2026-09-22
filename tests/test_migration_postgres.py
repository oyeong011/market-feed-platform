from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from mdfeed.migration import (
    canonical_postgres_digest,
    canonical_postgres_digest_from_connection,
    run_migration,
)
from mdfeed.migration.postgres_target import connect_postgres
from mdfeed.migration.types import MigrationPreflightError
from mdfeed.storage.catalog import APPLICATION_TABLES

ROOT = Path(__file__).resolve().parents[1]
SQLITE_SCHEMA = ROOT / "src" / "mdfeed" / "storage" / "schema_sqlite.sql"


def _psycopg2_module():
    return pytest.importorskip("psycopg2")


def _admin_dsn() -> str:
    from conftest import pg_test_runtime

    return str(pg_test_runtime()["dsn"])


def _database_dsn(admin_dsn: str, database: str) -> str:
    base, _, _old_database = admin_dsn.rpartition("/")
    return f"{base}/{database}"


@pytest.fixture
def target_dsn(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "test")
    psycopg2 = _psycopg2_module()
    admin_dsn = _admin_dsn()
    database = f"mf_lane_b_{uuid4().hex}"
    conn = psycopg2.connect(admin_dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{database}"')
    finally:
        conn.close()
    try:
        yield _database_dsn(admin_dsn, database)
    finally:
        cleanup = psycopg2.connect(admin_dsn)
        cleanup.autocommit = True
        try:
            with cleanup.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        finally:
            cleanup.close()


def _create_source(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SQLITE_SCHEMA.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO instruments "
            "(venue,symbol,base,quote,active,first_seen,last_seen) "
            "VALUES ('UPBIT','KRW-BTC','BTC','KRW',1,NULL,NULL)"
        )
        conn.execute(
            "INSERT INTO trades "
            "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (1700000000000000,'UPBIT','KRW-BTC',100.5,2.25,1,"
            "1700000000000100,100,42)"
        )
        conn.execute(
            "INSERT INTO trades "
            "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (1700000000000000,'UPBIT','KRW-BTC',100.5,2.25,1,"
            "1700000000000100,100,42)"
        )
        conn.execute(
            "INSERT INTO latest "
            "(venue,symbol,ts,price,qty,side,latency_us) "
            "VALUES ('UPBIT','KRW-BTC',1700000000000000,100.5,2.25,1,100)"
        )
        conn.commit()
    finally:
        conn.close()


def _add_trades(path: Path, count: int) -> None:
    conn = sqlite3.connect(path)
    try:
        for index in range(count):
            conn.execute(
                "INSERT INTO trades "
                "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                "VALUES (?, 'UPBIT','KRW-BTC',100.5,2.25,1,?,100,?)",
                (
                    1700000000001000 + index,
                    1700000000001100 + index,
                    1000 + index,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def test_run_migration_copies_all_tables_and_preserves_duplicates(
    tmp_path: Path, target_dsn: str
) -> None:
    source = tmp_path / "source.db"
    _create_source(source)

    receipt = run_migration(source, target_dsn, "pytest-run", chunk_size=1, init_target=True)

    target = canonical_postgres_digest(target_dsn)
    assert receipt.completed
    assert target.table("trades").row_count == 2
    assert target.table("instruments").null_counts["first_seen"] == 1
    assert {table.name for table in target.tables} == set(APPLICATION_TABLES)


def test_completed_multichunk_run_resumes_with_changed_chunk_size(
    tmp_path: Path, target_dsn: str
) -> None:
    source = tmp_path / "source.db"
    _create_source(source)
    _add_trades(source, 21)

    first = run_migration(source, target_dsn, "multi-run", chunk_size=7, init_target=True)
    second = run_migration(source, target_dsn, "multi-run", chunk_size=5, init_target=False)

    assert first.completed
    assert second.completed
    assert second.target is not None
    assert second.target.table("trades").row_count == 23


def test_resume_rejects_checkpoint_tamper(tmp_path: Path, target_dsn: str) -> None:
    psycopg2 = _psycopg2_module()
    source = tmp_path / "source.db"
    _create_source(source)
    run_migration(source, target_dsn, "tamper-run", chunk_size=1, init_target=True)

    conn = psycopg2.connect(target_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE migration_checkpoints SET chunk_hash = 'tampered' "
                "WHERE run_id = 'tamper-run' AND table_name = 'trades'"
            )
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="checkpoint"):
        run_migration(source, target_dsn, "tamper-run", chunk_size=1, init_target=False)


def test_target_wide_lock_refuses_concurrent_migration(
    tmp_path: Path, target_dsn: str
) -> None:
    psycopg2 = _psycopg2_module()
    source = tmp_path / "source.db"
    _create_source(source)
    from mdfeed.migration.postgres_target import init_target_schema

    init_target_schema(target_dsn)
    blocker = psycopg2.connect(target_dsn)
    try:
        with blocker.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext('mdfeed:migration-target'))")
        with pytest.raises(RuntimeError, match="another migration"):
            run_migration(source, target_dsn, "blocked-run", chunk_size=1)
    finally:
        with blocker.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext('mdfeed:migration-target'))")
        blocker.close()


def test_verify_requires_known_completed_run(tmp_path: Path, target_dsn: str) -> None:
    from mdfeed.migration import verify_migration

    source = tmp_path / "source.db"
    _create_source(source)
    run_migration(source, target_dsn, "actual-run", chunk_size=1, init_target=True)

    with pytest.raises(RuntimeError, match="verification failed"):
        verify_migration(source, target_dsn, "unknown-run")


def test_cli_run_status_verify_with_target_dsn_env(
    tmp_path: Path, target_dsn: str
) -> None:
    source = tmp_path / "source.db"
    _create_source(source)
    env = dict(os.environ)
    env["MDFEED_MIGRATION_TARGET_DSN"] = target_dsn
    env["MDFEED_STORAGE_PROFILE"] = "test"

    run_cmd = [
        sys.executable,
        "-m",
        "mdfeed.migration",
        "run",
        "--source",
        str(source),
        "--target-dsn-env",
        "MDFEED_MIGRATION_TARGET_DSN",
        "--run-id",
        "cli-run",
        "--chunk-size",
        "1",
        "--init-target",
    ]
    run_proc = subprocess.run(run_cmd, env=env, text=True, capture_output=True, check=False)
    assert run_proc.returncode == 0, run_proc.stderr
    assert "postgresql://" not in run_proc.stdout

    for command in ("status", "verify"):
        cmd = [
            sys.executable,
            "-m",
            "mdfeed.migration",
            command,
            "--source",
            str(source),
            "--target-dsn-env",
            "MDFEED_MIGRATION_TARGET_DSN",
            "--run-id",
            "cli-run",
        ]
        proc = subprocess.run(cmd, env=env, text=True, capture_output=True, check=False)
        assert proc.returncode == 0, proc.stderr
        payload = json.loads(proc.stdout)
        assert payload["run_id"] == "cli-run"
        assert payload["tables"]["trades"]["row_count"] == 2


def test_connection_scoped_digest_keeps_caller_connection_open(
    tmp_path: Path, target_dsn: str
) -> None:
    psycopg2 = _psycopg2_module()
    source = tmp_path / "source.db"
    _create_source(source)
    run_migration(source, target_dsn, "connection-digest-run", chunk_size=1, init_target=True)
    conn = psycopg2.connect(target_dsn)
    try:
        digest = canonical_postgres_digest_from_connection(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            one = cur.fetchone()[0]
    finally:
        conn.close()

    assert digest.table("trades").row_count == 2
    assert one == 1


def test_production_sslmode_disable_refused_before_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg2 = _psycopg2_module()
    called = False

    def fail_connect(_dsn: str) -> None:
        nonlocal called
        called = True
        raise AssertionError("connect must not run before storage policy validation")

    monkeypatch.delenv("MDFEED_STORAGE_PROFILE", raising=False)
    monkeypatch.setattr(psycopg2, "connect", fail_connect)

    with pytest.raises(MigrationPreflightError) as exc_info:
        connect_postgres(
            "postgresql://user:never-print-this@127.0.0.1:55439/mdfeed"
            "?sslmode=disable&sslrootcert=/tmp/ca.pem"
        )

    message = str(exc_info.value)
    assert called is False
    assert "sslmode=verify-full" in message
    assert "never-print-this" not in message
    assert "postgresql://" not in message


def test_production_missing_ca_refused_before_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg2 = _psycopg2_module()
    called = False

    def fail_connect(_dsn: str) -> None:
        nonlocal called
        called = True
        raise AssertionError("connect must not run before storage policy validation")

    monkeypatch.delenv("MDFEED_STORAGE_PROFILE", raising=False)
    monkeypatch.setattr(psycopg2, "connect", fail_connect)

    with pytest.raises(MigrationPreflightError) as exc_info:
        connect_postgres(
            "postgresql://user:never-print-this@127.0.0.1:55439/mdfeed"
            "?sslmode=verify-full"
        )

    message = str(exc_info.value)
    assert called is False
    assert "sslrootcert" in message
    assert "never-print-this" not in message
    assert "postgresql://" not in message
