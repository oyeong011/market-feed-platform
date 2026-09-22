from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
from uuid import uuid4

import pytest

from mdfeed import backup, gaps
from mdfeed.gap_repository import record_trusted_coverage, record_trusted_receipt
from mdfeed.migration import canonical_postgres_digest, run_migration
from mdfeed.storage.catalog import APPLICATION_TABLES
from mdfeed.storage.db import PostgresStorage

ROOT = pathlib.Path(__file__).resolve().parents[1]
SQLITE_SCHEMA = ROOT / "src" / "mdfeed" / "storage" / "schema_sqlite.sql"


def _psycopg2():
    return pytest.importorskip("psycopg2")


def _admin_dsn() -> str:
    from conftest import pg_test_runtime

    return str(pg_test_runtime()["dsn"])


def _db_dsn(admin_dsn: str, database: str) -> str:
    base, _, _old_database = admin_dsn.rpartition("/")
    return f"{base}/{database}"


def _user_dsn(dsn: str, user: str) -> str:
    prefix, marker, rest = dsn.partition("://")
    host = rest.split("@", 1)[-1]
    return f"{prefix}{marker}{user}@{host}"


@pytest.fixture
def target_dsn() -> str:
    psycopg2 = _psycopg2()
    admin_dsn = _admin_dsn()
    database = f"mf_lane_d_{uuid4().hex}"
    admin = psycopg2.connect(admin_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{database}"')
        yield _db_dsn(admin_dsn, database)
    finally:
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        admin.close()


def _create_source(path: pathlib.Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SQLITE_SCHEMA.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO instruments (venue,symbol,base,quote,active,first_seen,last_seen) "
            "VALUES ('UPBIT','KRW-BTC','BTC','KRW',1,NULL,NULL)"
        )
        conn.execute(
            "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (1767225600000000,'UPBIT','KRW-BTC',100.5,2.25,1,1767225600000100,100,42)"
        )
        conn.execute(
            "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
            "VALUES (1767225600000000,'UPBIT','KRW-BTC',100.0,1.0,101.0,1.1,99.5)"
        )
        conn.execute(
            "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,notional,vwap,tick_count) "
            "VALUES (1767225600000000,'UPBIT','KRW-BTC',100,101,99,100.5,2.25,226.125,100.5,1)"
        )
        conn.execute(
            "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
            "VALUES (1767225600000000,'UPBIT','KRW-BTC','synthetic',1,0.5,100.5)"
        )
        conn.execute(
            "INSERT INTO feed_stats (ts,service,venue,ticks,latency_p50_us,latency_p99_us,gaps,drops,subscribers) "
            "VALUES (1767225600000000,'feedd','UPBIT',1,100,200,0,0,1)"
        )
        conn.execute(
            "INSERT INTO latest (venue,symbol,ts,price,qty,side,latency_us) "
            "VALUES ('UPBIT','KRW-BTC',1767225600000000,100.5,2.25,1,100)"
        )
        conn.execute(
            "INSERT INTO quality_events (ts,check_name,severity,venue,symbol,detail,value) "
            "VALUES (1767225600000000,'synthetic','info','UPBIT','KRW-BTC','ok',1.0)"
        )
        conn.commit()
    finally:
        conn.close()


def _remote_helper(tmp_path: pathlib.Path) -> tuple[str, str]:
    script = tmp_path / "remote.py"
    script.write_text(
        "import pathlib, shutil, sys\n"
        "root = pathlib.Path(sys.argv[2])\n"
        "root.mkdir(parents=True, exist_ok=True)\n"
        "if sys.argv[1] == 'push':\n"
        "    target = root / sys.argv[4]\n"
        "    target.parent.mkdir(parents=True, exist_ok=True)\n"
        "    shutil.copyfile(sys.argv[3], target)\n"
        "elif sys.argv[1] == 'fetch':\n"
        "    shutil.copyfile(root / sys.argv[3], sys.argv[4])\n",
        encoding="utf-8",
    )
    remote = tmp_path / "remote"
    return (
        f"{sys.executable} {script} push {remote} {{source}} {{object}}",
        f"{sys.executable} {script} fetch {remote} {{object}} {{destination}}",
    )


def test_synthetic_storage_recovery_e2e_uses_persistent_pg_receipts(target_dsn: str, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "test")
    source = tmp_path / "source.db"
    _create_source(source)
    receipt = run_migration(source, target_dsn, "lane-d-run", chunk_size=2, init_target=True)
    rerun = run_migration(source, target_dsn, "lane-d-run", chunk_size=3, init_target=False)
    target = canonical_postgres_digest(target_dsn)
    assert receipt.source.fingerprint == target.fingerprint == rerun.target.fingerprint
    assert {table.name for table in target.tables} == set(APPLICATION_TABLES)
    assert all(target.table(table).row_count == 1 for table in APPLICATION_TABLES)

    push, fetch = _remote_helper(tmp_path)
    monkeypatch.setenv("DATABASE_URL", target_dsn)
    monkeypatch.setenv("MDFEED_MAINTENANCE_DSN", _admin_dsn())
    dump, manifest = backup.create_backup(
        dsn_env="DATABASE_URL",
        output_dir=str(tmp_path / "backup"),
        object_id="backup/lane-d.dump",
        push_command=push,
        fetch_command=fetch,
        receipt_dir=str(tmp_path / "receipts"),
        timeout_s=30,
    )
    restore = backup.restore_remote_drill(
        manifest=str(manifest),
        object_id="backup/lane-d.dump",
        fetch_command=fetch,
        timeout_s=30,
    )
    manifest_json = json.loads(manifest.read_text(encoding="utf-8"))
    coverage_id = "coverage-lane-d"
    assert restore.status == "restored"
    assert set(manifest_json["table_digests"]) == set(APPLICATION_TABLES)

    storage = PostgresStorage(target_dsn)
    try:
        repo = gaps.open_repository(storage=storage)
        synthetic = gaps.GapRecord(
            id="synthetic-storage-gap",
            started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            ended_at=None,
            state=gaps.GapState.OPEN,
            required_scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), tuple(APPLICATION_TABLES)),
            reason="synthetic outage",
        )
        repo.save(gaps.close_gap(synthetic, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC)))
        record_trusted_coverage(
            storage,
            coverage_id=coverage_id,
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), tuple(APPLICATION_TABLES)),
            source_fingerprint=receipt.source.fingerprint,
            table_counts=manifest_json["table_counts"],
            producer="pytest-authority",
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        record_trusted_receipt(
            storage,
            gap_id="synthetic-storage-gap",
            receipt_id="receipt-lane-d",
            actual_reconciliation_id="lane-d-run",
            authoritative_coverage_id=coverage_id,
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        with pytest.raises(gaps.GapTransitionError):
            record_trusted_receipt(
                storage,
                gap_id="synthetic-storage-gap",
                receipt_id="missing-coverage-registry",
                actual_reconciliation_id="lane-d-run",
                authoritative_coverage_id="missing-coverage",
                verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
            )
        with pytest.raises(gaps.GapTransitionError):
            record_trusted_receipt(
                storage,
                gap_id="synthetic-storage-gap",
                receipt_id="missing-migration-registry",
                actual_reconciliation_id="missing-run",
                authoritative_coverage_id=coverage_id,
                verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
            )
        record_trusted_coverage(
            storage,
            coverage_id="different-fingerprint-coverage",
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), tuple(APPLICATION_TABLES)),
            source_fingerprint="different-fingerprint",
            table_counts=manifest_json["table_counts"],
            producer="pytest-authority",
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        with pytest.raises(gaps.GapTransitionError):
            record_trusted_receipt(
                storage, gap_id="synthetic-storage-gap", receipt_id="different-coverage",
                actual_reconciliation_id="lane-d-run",
                authoritative_coverage_id="different-fingerprint-coverage",
                verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
            )
        p = storage.placeholder
        storage.execute(
            "INSERT INTO migration_runs "
            "(run_id,source_fingerprint,state,started_at,completed_at,updated_at) "
            f"VALUES ({p},{p},'COMPLETED',{p},{p},{p})",
            ("stale-run", "stale-fingerprint", "2026-01-01T00:00:00Z",
             "2026-01-01T00:00:01Z", "2026-01-01T00:00:01Z"),
        )
        record_trusted_coverage(
            storage, coverage_id="stale-coverage",
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), tuple(APPLICATION_TABLES)),
            source_fingerprint="stale-fingerprint", table_counts=manifest_json["table_counts"],
            producer="pytest-authority", verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        with pytest.raises(gaps.GapTransitionError):
            record_trusted_receipt(
                storage, gap_id="synthetic-storage-gap", receipt_id="stale-run-proof",
                actual_reconciliation_id="stale-run", authoritative_coverage_id="stale-coverage",
                verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
            )
        partial = gaps.GapRecord(
            id="partial-storage-gap",
            started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            ended_at=None,
            state=gaps.GapState.OPEN,
            required_scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            reason="partial synthetic outage",
        )
        repo.save(gaps.close_gap(partial, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC)))
        record_trusted_coverage(
            storage, coverage_id="partial-coverage",
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            source_fingerprint=receipt.source.fingerprint, table_counts={"trades": 1},
            producer="pytest-authority", verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        storage.execute("UPDATE authoritative_gap_coverages SET status='FAILED' WHERE coverage_id='partial-coverage'")
        tampered = gaps.GapRecord(
            id="tampered-storage-gap",
            started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            ended_at=None,
            state=gaps.GapState.OPEN,
            required_scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            reason="tampered synthetic outage",
        )
        repo.save(gaps.close_gap(tampered, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC)))
        record_trusted_coverage(
            storage, coverage_id="tampered-coverage",
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            source_fingerprint=receipt.source.fingerprint, table_counts={"trades": 1},
            producer="pytest-authority", verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        record_trusted_receipt(
            storage, gap_id="tampered-storage-gap", receipt_id="tampered-receipt",
            actual_reconciliation_id="lane-d-run", authoritative_coverage_id="tampered-coverage",
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        storage.execute("UPDATE gap_recovery_receipts SET evidence_hash='tampered' WHERE receipt_id='tampered-receipt'")
    finally:
        storage.close()

    psycopg2 = _psycopg2()
    role = f"mf_lane_d_no_proof_{uuid4().hex}"
    admin = psycopg2.connect(_admin_dsn())
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE ROLE "{role}" LOGIN')
            cur.execute(f'GRANT CONNECT ON DATABASE "{target_dsn.rpartition("/")[2]}" TO "{role}"')
        restricted = psycopg2.connect(_user_dsn(target_dsn, role))
        try:
            with pytest.raises(psycopg2.Error):
                with restricted, restricted.cursor() as cur:
                    cur.execute(
                        "INSERT INTO authoritative_gap_coverages "
                        "(coverage_id,covered_start,covered_end,scope_hash,scope_json,"
                        "source_fingerprint,table_counts_json,evidence_hash,producer,verified_at,status) "
                        "VALUES ('forged','2026-01-01','2026-01-01','x','{}','x','{}','x','bad','2026-01-01','VERIFIED')"
                    )
        finally:
            restricted.close()
    finally:
        with admin.cursor() as cur:
            cur.execute(f'REVOKE CONNECT ON DATABASE "{target_dsn.rpartition("/")[2]}" FROM "{role}"')
            cur.execute(f'DROP ROLE IF EXISTS "{role}"')
        admin.close()

    env = os.environ.copy()
    env.update({
        "PYTHONPATH": "src",
        "MDFEED_STORAGE_BACKEND": "postgres",
        "MDFEED_STORAGE_PROFILE": "test",
        "DATABASE_URL": target_dsn,
    })
    cmd = [
        sys.executable,
        "-m",
        "mdfeed.cli",
        "gap",
        "verify-recovery",
        "--id",
        "synthetic-storage-gap",
        "--receipt-id",
        "receipt-lane-d",
    ]
    ok = subprocess.run(cmd, env=env, text=True, capture_output=True, check=False, timeout=20)
    assert ok.returncode == 0, ok.stderr + ok.stdout
    recovered = json.loads(ok.stdout)
    assert recovered["state"] == "RECOVERED"

    for gap_id, receipt_id in (
        ("synthetic-storage-gap", "missing-receipt"),
        ("partial-storage-gap", "partial-receipt"),
        ("tampered-storage-gap", "tampered-receipt"),
    ):
        forged = subprocess.run(
            [sys.executable, "-m", "mdfeed.cli", "gap", "verify-recovery",
             "--id", gap_id, "--receipt-id", receipt_id],
            env=env, text=True, capture_output=True, check=False, timeout=20,
        )
        assert forged.returncode == 2
        assert json.loads(forged.stdout)["error"] == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"

    status = subprocess.run(
        [sys.executable, "-m", "mdfeed.cli", "gap", "status"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )
    payload = json.loads(status.stdout)
    states = {item["id"]: item["state"] for item in payload["items"]}
    assert states["synthetic-storage-gap"] == "RECOVERED"
    assert states["collection-stop-20260908"] == "OPEN"
    assert status.returncode == 1
