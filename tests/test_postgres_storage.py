import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest

# 드라이버가 없는 러너(기본 CI 파이썬 잡)에서는 수집 단계에서 스킵. 있으면 그대로 진행.
psycopg2 = pytest.importorskip("psycopg2")
import psycopg2.sql  # noqa: E402

from mdfeed import archive as ar
from mdfeed.config import Config
from mdfeed.services.writer import Writer
from mdfeed.storage.db import (
    PG_RUNTIME_TABLE_PRIVILEGES,
    PG_RUNTIME_VIEW_PRIVILEGES,
    PostgresStorage,
    StorageConfigurationError,
    make_storage_batch,
    open_storage,
)

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DAY = dt.date(2026, 8, 30)


@pytest.fixture
def pg_dsn():
    from conftest import pg_test_runtime

    data = pg_test_runtime()
    admin_dsn = data["dsn"]
    db_name = f"lane_a_{int(time.time())}_{os.getpid()}"
    createdb = data["binaries"].get("createdb", "createdb")
    dropdb = data["binaries"].get("dropdb", "dropdb")
    env = {**os.environ, "PGPASSWORD": ""}
    subprocess.run([createdb, "--maintenance-db", admin_dsn, db_name], check=True, env=env)
    dsn = admin_dsn.rsplit("/", 1)[0] + f"/{db_name}"
    try:
        yield dsn
    finally:
        subprocess.run([dropdb, "--if-exists", "--maintenance-db", admin_dsn, db_name], check=False, env=env)


@pytest.fixture
def pg_store(pg_dsn):
    store = PostgresStorage(pg_dsn)
    store.ensure_schema()
    try:
        yield store
    finally:
        store.close()


def _role_dsn(pg_dsn: str, role: str) -> str:
    parsed = urlsplit(pg_dsn)
    return urlunsplit(
        (parsed.scheme, f"{role}@{parsed.hostname}:{parsed.port}", parsed.path, "", "")
    )


def _grant_relation_privileges(
    cur,
    role: str,
    relation_privileges: dict[str, tuple[str, ...]],
) -> None:
    for relation, privileges in relation_privileges.items():
        privilege_sql = psycopg2.sql.SQL(", ").join(
            psycopg2.sql.SQL(privilege) for privilege in privileges
        )
        relation_sql = psycopg2.sql.SQL("{}.{}").format(
            psycopg2.sql.Identifier("public"),
            psycopg2.sql.Identifier(relation),
        )
        cur.execute(
            psycopg2.sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                privilege_sql,
                relation_sql,
                psycopg2.sql.Identifier(role),
            )
        )


def _create_restricted_role(
    pg_dsn: str,
    role: str,
    table_privileges: dict[str, tuple[str, ...]],
    view_privileges: dict[str, tuple[str, ...]],
) -> None:
    admin = psycopg2.connect(pg_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(
                psycopg2.sql.SQL("CREATE ROLE {} LOGIN").format(
                    psycopg2.sql.Identifier(role)
                )
            )
            cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            cur.execute(
                psycopg2.sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(
                    psycopg2.sql.Identifier(role)
                )
            )
            cur.execute(
                psycopg2.sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    psycopg2.sql.Identifier(role)
                )
            )
            _grant_relation_privileges(cur, role, table_privileges)
            _grant_relation_privileges(cur, role, view_privileges)
    finally:
        admin.close()


def _drop_restricted_role(pg_dsn: str, role: str) -> None:
    admin = psycopg2.connect(pg_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(
                psycopg2.sql.SQL(
                    "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {}"
                ).format(psycopg2.sql.Identifier(role))
            )
            cur.execute(
                psycopg2.sql.SQL("REVOKE USAGE ON SCHEMA public FROM {}").format(
                    psycopg2.sql.Identifier(role)
                )
            )
            cur.execute(
                psycopg2.sql.SQL("DROP ROLE IF EXISTS {}").format(
                    psycopg2.sql.Identifier(role)
                )
            )
    finally:
        admin.close()


@pytest.fixture
def restricted_pg_store(pg_dsn):
    role = f"lane_a_runtime_{int(time.time())}_{os.getpid()}"
    store = PostgresStorage(pg_dsn)
    store.ensure_schema()
    store.close()
    _create_restricted_role(
        pg_dsn,
        role,
        dict(PG_RUNTIME_TABLE_PRIVILEGES),
        dict(PG_RUNTIME_VIEW_PRIVILEGES),
    )
    restricted = PostgresStorage(_role_dsn(pg_dsn, role))
    try:
        yield restricted
    finally:
        restricted.close()
        _drop_restricted_role(pg_dsn, role)


def _remote_helper(tmp_path: Path) -> tuple[str, str]:
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
        "    shutil.copyfile(root / sys.argv[3], sys.argv[4])\n"
        "else:\n"
        "    raise SystemExit(2)\n",
        encoding="utf-8",
    )
    remote = tmp_path / "remote"
    push = f"{sys.executable} {script} push {remote} {{source}} {{object}}"
    fetch = f"{sys.executable} {script} fetch {remote} {{object}} {{destination}}"
    return push, fetch


def _insert_archive_day(store: PostgresStorage) -> None:
    lo, _hi = ar.day_bounds_us(ARCHIVE_DAY)
    store.insert_trades([
        (lo + 1_000, "UPBIT", "KRW-BTC", 1.0, 2.0, 1, lo + 1_500, 500, 1),
        (lo + 2_000, "UPBIT", "KRW-ETH", 3.0, 4.0, 2, lo + 2_500, 500, 2),
    ])
    store.insert_book([
        (lo + 1_000, "UPBIT", "KRW-BTC", 0.9, 1.0, 1.1, 1.0, 20.0),
    ])


def test_test_profile_allows_loopback_without_tls(pg_dsn):
    cfg = Config()
    cfg.storage_profile = "test"
    cfg.pg_dsn = pg_dsn
    store = open_storage(cfg)
    try:
        assert store.kind == "postgres"
    finally:
        store.close()


def test_production_profile_rejects_missing_tls_before_connect(pg_dsn):
    cfg = Config()
    cfg.pg_dsn = pg_dsn

    with pytest.raises(StorageConfigurationError, match="sslmode=verify-full"):
        open_storage(cfg)


def test_postgres_stream_uses_dedicated_read_transaction(pg_store):
    t = 1_700_000_000_000_001
    pg_store.insert_trades([(t, "UPBIT", "KRW-BTC", 1.0, 1.0, 1, t, 7, 1)])

    rows = list(pg_store.stream("SELECT venue, symbol FROM trades ORDER BY ts", chunk=1))

    assert rows == [[("UPBIT", "KRW-BTC")]]


def test_postgres_delete_older_than_maps_epoch_us_to_timestamptz(pg_store):
    old = 1_700_000_000_000_001
    new = old + 2_000_000
    pg_store.insert_trades([
        (old, "UPBIT", "OLD", 1.0, 1.0, 1, old, 7, 1),
        (new, "UPBIT", "NEW", 1.0, 1.0, 1, new, 7, 2),
    ])

    assert pg_store.delete_older_than("trades", "ts", old + 1_000_000, 100) == 1

    rows = pg_store.query("SELECT symbol FROM trades ORDER BY symbol")
    assert rows == [{"symbol": "NEW"}]


def test_postgres_trades_and_latest_are_atomic(pg_store):
    pg_store.execute(
        "ALTER TABLE latest ADD CONSTRAINT latest_atomicity_probe "
        "CHECK (symbol <> 'ATOMIC-BROKEN')"
    )
    t = 1_700_000_000_000_001

    with pytest.raises(psycopg2.Error):
        pg_store.insert_trades([(t, "UPBIT", "ATOMIC-BROKEN", 1.0, 1.0, 1, t, 7, 1)])

    assert pg_store.query("SELECT COUNT(*) AS n FROM trades")[0]["n"] == 0


def test_postgres_batch_receipt_commits_with_rows(pg_store):
    t = 1_700_000_000_000_001
    batch = make_storage_batch(
        trades=[(t, "UPBIT", "KRW-BTC", 1.0, 1.0, 1, t, 7, 1)],
        books=[],
        signals=[],
        bars=[],
    )

    receipt = pg_store.write_batch(batch)

    assert receipt.batch_id == batch.batch_id
    assert pg_store.query(
        "SELECT receipt_id, batch_hash, row_count FROM ingest_batch_receipts"
    ) == [
        {
            "receipt_id": f"{batch.batch_id}:trades",
            "batch_hash": batch.digest,
            "row_count": 1,
        }
    ]


def test_postgres_batch_id_is_distinct_from_digest_for_identical_content(pg_store):
    t = 1_700_000_000_000_001
    rows = [(t, "UPBIT", "KRW-BTC", 1.0, 1.0, 1, t, 7, 1)]

    first = make_storage_batch(trades=rows, books=[], signals=[], bars=[])
    second = make_storage_batch(trades=rows, books=[], signals=[], bars=[])

    assert first.batch_id != second.batch_id
    assert first.digest == second.digest


def test_postgres_replaying_same_batch_receipt_does_not_duplicate_rows(pg_store):
    t = 1_700_000_000_000_001
    batch = make_storage_batch(
        trades=[(t, "UPBIT", "KRW-BTC", 1.0, 1.0, 1, t, 7, 1)],
        books=[],
        signals=[],
        bars=[],
    )

    pg_store.write_batch(batch)
    receipt = pg_store.write_batch(batch)

    assert receipt.batch_id == batch.batch_id
    assert pg_store.query("SELECT COUNT(*) AS n FROM trades") == [{"n": 1}]


def test_postgres_runtime_schema_validation_accepts_exact_runtime_role(
    restricted_pg_store,
):
    t = 1_700_000_000_000_001
    restricted_pg_store.validate_runtime_schema()

    receipt = restricted_pg_store.write_batch(
        make_storage_batch(
            trades=[(t, "UPBIT", "KRW-BTC", 1.0, 1.0, 1, t, 7, 1)],
            books=[],
            signals=[],
            bars=[],
        )
    )

    assert receipt.rows_written == 1
    assert restricted_pg_store.query("SELECT COUNT(*) AS n FROM trades") == [{"n": 1}]
    assert restricted_pg_store.query("SELECT COUNT(*) AS n FROM migration_runs") == [
        {"n": 0}
    ]
    with pytest.raises(psycopg2.Error):
        restricted_pg_store.execute(
            "INSERT INTO migration_runs(run_id, source_fingerprint, state) "
            "VALUES ('forbidden', 'synthetic', 'failed')"
        )
    with pytest.raises(psycopg2.Error):
        restricted_pg_store.execute("CREATE TABLE runtime_role_forbidden (id INTEGER)")


def test_postgres_runtime_schema_validation_refuses_select_only_role(pg_dsn):
    role = f"lane_a_readonly_{int(time.time())}_{os.getpid()}"
    store = PostgresStorage(pg_dsn)
    store.ensure_schema()
    store.close()
    table_privileges = {table: ("SELECT",) for table in PG_RUNTIME_TABLE_PRIVILEGES}
    _create_restricted_role(
        pg_dsn,
        role,
        table_privileges,
        dict(PG_RUNTIME_VIEW_PRIVILEGES),
    )
    restricted = PostgresStorage(_role_dsn(pg_dsn, role))
    try:
        with pytest.raises(StorageConfigurationError, match="trades:INSERT"):
            restricted.validate_runtime_schema()
    finally:
        restricted.close()
        _drop_restricted_role(pg_dsn, role)


def test_postgres_runtime_schema_validation_refuses_missing_required_table(pg_dsn):
    store = PostgresStorage(pg_dsn)
    store.ensure_schema()
    try:
        store.execute("DROP TABLE signals")

        with pytest.raises(StorageConfigurationError, match="signals"):
            store.validate_runtime_schema()
    finally:
        store.close()


def test_writer_archive_cycle_uses_remote_receipts_and_retries_local_export(
    pg_store: PostgresStorage,
    tmp_path: Path,
):
    _insert_archive_day(pg_store)
    cfg = Config()
    cfg.storage_backend = "postgres"
    cfg.storage_profile = "test"
    cfg.archive_dir = str(tmp_path / "archive")
    cfg.archive_lag_s = 0
    cfg.retention_days = 1
    push, fetch = _remote_helper(tmp_path)
    writer = Writer(cfg)
    writer.storage = pg_store
    writer.cfg.archive_push_command = push
    writer.cfg.archive_fetch_command = f"{sys.executable} -c 'raise SystemExit(3)' {{object}} {{destination}}"

    first = writer._archive_once()

    assert first.archived == []
    assert len(first.failed) == 2
    assert not list(Path(cfg.archive_dir).glob("archive_*.json"))

    writer.cfg.archive_fetch_command = fetch
    second = writer._archive_once()

    assert second.failed == []
    assert sorted(second.archived) == [
        f"book_top/{ARCHIVE_DAY}",
        f"trades/{ARCHIVE_DAY}",
    ]
    assert second.rows == 3
    receipts = sorted(Path(cfg.archive_dir).glob("archive_*.json"))
    assert len(receipts) == 2
    assert writer._archive_floor_us() == ar.day_bounds_us(ARCHIVE_DAY + dt.timedelta(days=1))[0]
    assert writer._archive_once().archived == []


def test_writer_prune_blocks_when_remote_proof_required_without_archive_config(
    pg_store: PostgresStorage,
):
    _insert_archive_day(pg_store)
    cfg = Config()
    cfg.storage_backend = "postgres"
    cfg.storage_profile = "production"
    cfg.retention_days = 1
    cfg.archive_dir = ""
    cfg.archive_push_command = ""
    cfg.archive_fetch_command = ""
    writer = Writer(cfg)
    writer.storage = pg_store

    result = writer._prune_locked(
        lambda storage, retention_days, *, guard, budget_s, floor_us, remote_receipt_dir: {
            "floor_us": floor_us,
            "remote_receipt_dir": remote_receipt_dir,
        }
    )

    assert result["floor_us"] == 0
    assert result["remote_receipt_dir"] is None
