#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from mdfeed import gaps
from mdfeed.gap_repository import record_trusted_coverage, record_trusted_receipt
from mdfeed.migration import canonical_postgres_digest
from mdfeed.migration.postgres_target import init_target_schema
from mdfeed.storage.db import PostgresStorage, SQLiteStorage


def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def http_json(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
        return json.loads(response.read())


def cli(env: dict[str, str], args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "mdfeed.cli", *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=20,
    )


def wait_http(port: int) -> None:
    for _ in range(100):
        try:
            http_json(port, "/healthz")
            return
        except (OSError, TimeoutError, urllib.error.URLError):
            time.sleep(0.05)
    raise RuntimeError("REST API did not become ready")


def start_rest(env: dict[str, str]) -> tuple[subprocess.Popen[str], int]:
    port = free_port()
    child_env = dict(env)
    child_env["MDFEED_HTTP_HOST"] = "127.0.0.1"
    child_env["MDFEED_HTTP_PORT"] = str(port)
    proc = subprocess.Popen(
        [sys.executable, "-m", "mdfeed.services.rest_api"],
        env=child_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_http(port)
    except Exception:
        proc.terminate()
        proc.wait(timeout=10)
        raise
    return proc, port


def sqlite_mode(tmp: str, env: dict[str, str]) -> dict:
    db_path = os.path.join(tmp, "api.db")
    state_file = os.path.join(tmp, "gaps.json")
    SQLiteStorage(db_path).ensure_schema()
    cli_env = dict(env)
    cli_env.update({
        "MDFEED_STORAGE_BACKEND": "sqlite",
        "MDFEED_STORAGE_PROFILE": "test",
        "MDFEED_SQLITE_PATH": db_path,
    })
    status = cli(cli_env, ["gap", "status", "--state-file", state_file])
    bad = _malformed_recovery(cli_env, state_file)
    proc, port = start_rest(cli_env)
    try:
        return _payload(status, bad, http_json(port, "/api/v1/gaps"), http_json(port, "/healthz"), proc.poll() is None)
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def pg_mode(runtime_path: str, env: dict[str, str]) -> dict:
    import psycopg2

    runtime = json.loads(Path(runtime_path).read_text(encoding="utf-8"))
    admin_dsn = str(runtime["dsn"])
    database = f"mf_lane_d_controls_{uuid4().hex}"
    admin = psycopg2.connect(admin_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{database}"')
        dsn = _db_dsn(admin_dsn, database)
        os.environ["MDFEED_STORAGE_PROFILE"] = "test"
        init_target_schema(dsn)
        _seed_synthetic_gap(dsn)
        cli_env = dict(env)
        cli_env.update({
            "MDFEED_STORAGE_BACKEND": "postgres",
            "MDFEED_STORAGE_PROFILE": "test",
            "DATABASE_URL": dsn,
        })
        close = cli(cli_env, ["gap", "close", "--id", "control-synthetic-gap", "--ended-at", "2026-01-01T00:01:00Z"])
        close_json = json.loads(close.stdout)
        if close.returncode != 1 or close_json.get("state") != gaps.GapState.ENDED_UNRECOVERED.value:
            raise RuntimeError(f"gap close did not persist closed-unrecovered state: {close.stdout}{close.stderr}")
        _record_control_receipt(dsn)
        recovered = cli(cli_env, ["gap", "verify-recovery", "--id", "control-synthetic-gap", "--receipt-id", "control-receipt"])
        missing = cli(cli_env, ["gap", "verify-recovery", "--id", "control-synthetic-gap", "--receipt-id", "missing-receipt"])
        bad = _malformed_recovery(cli_env, None)
        proc, port = start_rest(cli_env)
        try:
            payload = _payload(recovered, bad, http_json(port, "/api/v1/gaps"), http_json(port, "/healthz"), proc.poll() is None)
            payload["pg"] = {
                "close_exit": close.returncode,
                "close_json": close_json,
                "missing_receipt_exit": missing.returncode,
            }
            payload["pg"]["missing_receipt_json"] = json.loads(missing.stdout)
            return payload
        finally:
            proc.terminate()
            proc.wait(timeout=10)
    finally:
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        admin.close()


def _db_dsn(admin_dsn: str, database: str) -> str:
    base, _, _old = admin_dsn.rpartition("/")
    return f"{base}/{database}"


def _seed_synthetic_gap(dsn: str) -> None:
    storage = PostgresStorage(dsn)
    try:
        repo = gaps.open_repository(storage=storage)
        repo.save(gaps.GapRecord(
            "control-synthetic-gap",
            dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            None,
            gaps.GapState.OPEN,
            gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            "synthetic control gap",
        ))
    finally:
        storage.close()


def _record_control_receipt(dsn: str) -> None:
    digest = canonical_postgres_digest(dsn)
    storage = PostgresStorage(dsn)
    try:
        p = storage.placeholder
        storage.execute(
            "INSERT INTO migration_runs "
            "(run_id,source_fingerprint,state,started_at,completed_at,updated_at) "
            f"VALUES ({p},{p},'COMPLETED',{p},{p},{p})",
            (
                "control-reconciliation-run", digest.fingerprint,
                "2026-01-01T00:01:00Z", "2026-01-01T00:01:30Z", "2026-01-01T00:01:30Z",
            ),
        )
        record_trusted_coverage(
            storage,
            coverage_id="control-coverage-snapshot",
            covered_start=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
            covered_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC),
            scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
            source_fingerprint=digest.fingerprint,
            table_counts={"trades": digest.table("trades").row_count},
            producer="control-test-authority",
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
        record_trusted_receipt(
            storage,
            gap_id="control-synthetic-gap",
            receipt_id="control-receipt",
            actual_reconciliation_id="control-reconciliation-run",
            authoritative_coverage_id="control-coverage-snapshot",
            verified_at=dt.datetime(2026, 1, 1, 0, 2, tzinfo=gaps.UTC),
        )
    finally:
        storage.close()


def _malformed_recovery(env: dict[str, str], state_file: str | None) -> subprocess.CompletedProcess[str]:
    args = ["gap", "verify-recovery", "--id", "collection-stop-20260908", "--receipt", "/dev/null"]
    if state_file is not None:
        args.extend(["--state-file", state_file])
    return cli(env, args)


def _payload(status, bad, gaps_json: dict, health: dict, api_alive: bool) -> dict:
    return {
        "cli_exit": status.returncode,
        "cli_json": json.loads(status.stdout),
        "http_json": gaps_json,
        "health_json": health,
        "malformed_recovery_exit": bad.returncode,
        "malformed_recovery_json": json.loads(bad.stdout),
        "cleanup": {"api_process_alive_before_stop": api_alive, "temp_dir": "removed"},
    }


def main() -> int:
    parser = argparse.ArgumentParser("verify-storage-control-surfaces")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--pg-runtime")
    args = parser.parse_args()
    env = dict(os.environ)
    env["PYTHONPATH"] = env.get("PYTHONPATH", "src")
    with tempfile.TemporaryDirectory(prefix="mdfeed-control-") as tmp:
        payload = pg_mode(args.pg_runtime, env) if args.pg_runtime else sqlite_mode(tmp, env)
        os.makedirs(os.path.dirname(args.evidence) or ".", exist_ok=True)
        Path(args.evidence).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
