import datetime as dt
import os
import subprocess
import sys

import pytest

from mdfeed import backup
from mdfeed.migration.postgres_target import init_target_schema


def _runtime():
    from conftest import pg_test_runtime

    return pg_test_runtime()


def _psycopg2():
    return pytest.importorskip("psycopg2")


def _db_dsn(base_dsn, database):
    return backup._with_database(base_dsn, database)


@pytest.fixture
def pg_source_db(monkeypatch):
    runtime = _runtime()
    psycopg2 = _psycopg2()
    # 예전엔 os.environ 에 직접 넣고 finally 에서 되돌렸는데, 그 사이의
    # psycopg2.connect 가 실패하면 finally 에 못 가서 이후 모든 테스트에
    # STORAGE_PROFILE=test 가 새어 나갔다 (test_storage 3건 순서 의존 실패).
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "test")
    admin_dsn = runtime["dsn"]
    database = "mdfeed_backup_src_" + str(os.getpid())
    admin = psycopg2.connect(admin_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
            cur.execute(f'CREATE DATABASE "{database}"')
        dsn = _db_dsn(admin_dsn, database)
        init_target_schema(dsn)
        conn = psycopg2.connect(dsn)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO instruments "
                    "(venue,symbol,base,quote,active,first_seen,last_seen) "
                    "VALUES ('UPBIT','KRW-BTC','BTC','KRW',TRUE,NULL,NULL)"
                )
                cur.execute(
                    "INSERT INTO trades "
                    "(ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                    "VALUES "
                    "('2026-01-01T00:00:00Z','UPBIT','KRW-BTC',1.0,2.0,1,"
                    "'2026-01-01T00:00:01Z',100,1),"
                    "('2026-01-01T00:00:02Z','UPBIT','KRW-BTC',2.0,3.0,2,"
                    "'2026-01-01T00:00:03Z',200,2)"
                )
                cur.execute(
                    "INSERT INTO book_top "
                    "(ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
                    "VALUES ('2026-01-01T00:00:00Z','UPBIT','KRW-BTC',1.0,2.0,3.0,4.0,5.0)"
                )
                cur.execute(
                    "INSERT INTO latest "
                    "(venue,symbol,ts,price,qty,side,latency_us) "
                    "VALUES ('UPBIT','KRW-BTC','2026-01-01T00:00:02Z',2.0,3.0,2,200)"
                )
                cur.execute(
                    "INSERT INTO quality_events "
                    "(ts,check_name,severity,venue,symbol,detail,value) "
                    "VALUES ('2026-01-01T00:00:00Z','freshness','info','UPBIT','KRW-BTC','ok',1.0)"
                )
            with conn.cursor() as cur:
                cur.execute("SELECT oid FROM pg_database WHERE datname = current_database()")
                oid = cur.fetchone()[0]
        finally:
            conn.close()
        yield admin_dsn, dsn, database, oid
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        admin.close()


def _remote_helper(tmp_path):
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


def test_postgres_backup_restore_drill_success(pg_source_db, tmp_path, monkeypatch):
    admin_dsn, source_dsn, database, oid = pg_source_db
    push, fetch = _remote_helper(tmp_path)
    monkeypatch.setenv("DATABASE_URL", source_dsn)
    monkeypatch.setenv("MDFEED_MAINTENANCE_DSN", admin_dsn)

    dump, manifest = backup.create_backup(
        dsn_env="DATABASE_URL",
        output_dir=str(tmp_path / "backup"),
        object_id="backup/synthetic.dump",
        push_command=push,
        fetch_command=fetch,
        receipt_dir=str(tmp_path / "receipts"),
        timeout_s=20,
    )
    drill = backup.restore_drill(str(dump), str(manifest), timeout_s=20)

    assert drill.status == "restored"
    assert drill.cleanup == "dropped"
    assert drill.tables["trades"] == 2
    psycopg2 = _psycopg2()
    conn = psycopg2.connect(source_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT oid FROM pg_database WHERE datname = %s", (database,))
            assert cur.fetchone()[0] == oid
            cur.execute("SELECT COUNT(*) FROM trades")
            assert cur.fetchone()[0] == 2
    finally:
        conn.close()


def test_postgres_backup_restore_drill_uses_fetched_remote_artifact(pg_source_db, tmp_path, monkeypatch):
    admin_dsn, source_dsn, _database, _oid = pg_source_db
    push, fetch = _remote_helper(tmp_path)
    monkeypatch.setenv("DATABASE_URL", source_dsn)
    monkeypatch.setenv("MDFEED_MAINTENANCE_DSN", admin_dsn)
    _dump, manifest = backup.create_backup(
        dsn_env="DATABASE_URL",
        output_dir=str(tmp_path / "backup"),
        object_id="backup/remote-restore.dump",
        push_command=push,
        fetch_command=fetch,
        receipt_dir=str(tmp_path / "receipts"),
        timeout_s=20,
    )

    drill = backup.restore_remote_drill(
        manifest=str(manifest),
        object_id="backup/remote-restore.dump",
        fetch_command=fetch,
        timeout_s=20,
    )

    assert drill.status == "restored"
    assert drill.cleanup == "dropped"
    assert drill.tables["quality_events"] == 1


def test_postgres_backup_restore_cli_missing_admin(pg_source_db, tmp_path, monkeypatch):
    _admin_dsn, source_dsn, _database, _oid = pg_source_db
    monkeypatch.setenv("DATABASE_URL", source_dsn)
    dump, manifest = backup.create_backup(
        dsn_env="DATABASE_URL",
        output_dir=str(tmp_path),
        timeout_s=20,
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    env.pop("MDFEED_MAINTENANCE_DSN", None)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mdfeed.backup",
            "restore-drill",
            "--artifact",
            str(dump),
            "--manifest",
            str(manifest),
        ],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 1
    assert "MDFEED_MAINTENANCE_DSN" in result.stderr


def test_postgres_retention_source_day_proof_uses_timestamp_bounds(pg_source_db):
    from mdfeed.retention import _source_day_proof
    from mdfeed.storage.db import PostgresStorage

    _admin_dsn, source_dsn, _database, _oid = pg_source_db
    store = PostgresStorage(source_dsn)
    try:
        rows, digest = _source_day_proof(store, "trades", dt.date(2026, 1, 1))
    finally:
        store.close()

    assert rows == 2
    assert len(digest) == 64
