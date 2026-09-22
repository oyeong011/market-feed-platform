from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from mdfeed.migration import (
    MigrationPreflightError,
    canonical_sqlite_digest,
    open_sqlite_source,
    plan_sqlite_source,
)
from mdfeed.storage.catalog import APPLICATION_TABLES

ROOT = Path(__file__).resolve().parents[1]
SQLITE_SCHEMA = ROOT / "src" / "mdfeed" / "storage" / "schema_sqlite.sql"


def create_source(path: Path) -> None:
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
            "VALUES (1700000000000000,'UPBIT','KRW-BTC',1.25,0.5,1,"
            "1700000000000010,10,1)"
        )
        conn.execute(
            "INSERT INTO trades "
            "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (1700000000000000,'UPBIT','KRW-BTC',1.25,0.5,1,"
            "1700000000000010,10,1)"
        )
        conn.execute(
            "INSERT INTO book_top "
            "(ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
            "VALUES (1700000000000000,'UPBIT','KRW-BTC',1.2,2.0,1.3,3.0,7.5)"
        )
        conn.execute(
            "INSERT INTO bars_1m "
            "(bucket,venue,symbol,open,high,low,close,volume,notional,vwap,tick_count) "
            "VALUES (1699999980000000,'UPBIT','KRW-BTC',1.0,2.0,0.5,1.5,"
            "10.0,15.0,1.5,2)"
        )
        conn.execute(
            "INSERT INTO signals "
            "(ts,venue,symbol,strategy,action,strength,ref_price) "
            "VALUES (1700000001000000,'UPBIT','KRW-BTC','sma',1,0.75,1.25)"
        )
        conn.execute(
            "INSERT INTO feed_stats "
            "(ts,service,venue,ticks,latency_p50_us,latency_p99_us,gaps,drops,subscribers) "
            "VALUES (1700000002000000,'writer','UPBIT',2,10,20,0,0,1)"
        )
        conn.execute(
            "INSERT INTO latest "
            "(venue,symbol,ts,price,qty,side,latency_us) "
            "VALUES ('UPBIT','KRW-BTC',1700000000000000,1.25,0.5,1,10)"
        )
        conn.execute(
            "INSERT INTO quality_events "
            "(ts,check_name,severity,venue,symbol,detail,value) "
            "VALUES (1700000003000000,'price_jump','WARNING','UPBIT',"
            "'KRW-BTC','synthetic',1.25)"
        )
        conn.commit()
    finally:
        conn.close()


def test_plan_reads_source_without_schema_mutation(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    before = source.stat()

    plan = plan_sqlite_source(source)

    after = source.stat()
    assert before.st_size == after.st_size
    assert before.st_mtime_ns == after.st_mtime_ns
    assert {row.table for row in plan.tables} == set(APPLICATION_TABLES)
    assert plan.tables_by_name["trades"].row_count == 2
    assert plan.tables_by_name["instruments"].null_counts["first_seen"] == 1
    assert plan.tables_by_name["instruments"].null_counts["last_seen"] == 1


def test_open_sqlite_source_rejects_active_wal(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    wal = source.with_name(f"{source.name}-wal")
    wal.write_bytes(b"active wal")

    with pytest.raises(MigrationPreflightError, match="WAL"):
        open_sqlite_source(source)


def test_plan_rejects_ambiguous_timezone_text(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    conn = sqlite3.connect(source)
    try:
        conn.execute(
            "UPDATE instruments SET first_seen='2026-09-09 12:00:00' "
            "WHERE venue='UPBIT'"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(MigrationPreflightError, match="timezone"):
        plan_sqlite_source(source)


def test_plan_rejects_unknown_venues_before_writes(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    conn = sqlite3.connect(source)
    try:
        conn.execute(
            "INSERT INTO trades "
            "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (1700000000000001,'UNKNOWN','X',1.0,1.0,1,"
            "1700000000000002,1,2)"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(MigrationPreflightError, match="unknown venue"):
        plan_sqlite_source(source)


def test_plan_accepts_existing_builtin_venues(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    conn = sqlite3.connect(source)
    try:
        for venue in ("KRX", "KRX-IDX", "RATE"):
            conn.execute(
                "INSERT INTO trades "
                "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                "VALUES (1700000000000001,?, 'X',1.0,1.0,1,"
                "1700000000000002,1,2)",
                (venue,),
            )
        conn.commit()
    finally:
        conn.close()

    digest = plan_sqlite_source(source)

    assert digest.table("trades").row_count == 5


def test_plan_rejects_non_boolean_active_before_writes(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    conn = sqlite3.connect(source)
    try:
        conn.execute("UPDATE instruments SET active=2 WHERE venue='UPBIT'")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(MigrationPreflightError, match="active"):
        plan_sqlite_source(source)


def test_plan_rejects_non_positive_rowid_before_writes(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    conn = sqlite3.connect(source)
    try:
        conn.execute(
            "INSERT INTO trades "
            "(rowid,ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (0,1700000000000001,'UPBIT','KRW-BTC',1.0,1.0,1,"
            "1700000000000002,1,2)"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(MigrationPreflightError, match="rowid"):
        plan_sqlite_source(source)


def test_canonical_digest_preserves_duplicate_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)

    digest = canonical_sqlite_digest(source)

    assert digest.table("trades").row_count == 2
    assert digest.table("trades").content_hash
    assert digest.table("instruments").null_counts["first_seen"] == 1


def test_cli_plan_uses_env_name_without_raw_dsn(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    create_source(source)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src")  # 서브프로세스는 conftest 경로를 모른다
    env["MDFEED_MIGRATION_TARGET_DSN"] = "postgresql://user:secret@127.0.0.1/db"
    cmd = [
        sys.executable,
        "-m",
        "mdfeed.migration",
        "plan",
        "--source",
        str(source),
        "--target-dsn-env",
        "MDFEED_MIGRATION_TARGET_DSN",
    ]

    proc = subprocess.run(cmd, env=env, text=True, capture_output=True, check=False)

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["source"]["tables"]["trades"]["row_count"] == 2
    assert "secret" not in proc.stdout
    assert "postgresql://" not in proc.stdout
