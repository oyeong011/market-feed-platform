from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import pathlib
import secrets
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Any

from .archive import _redact, _run_transfer, _sha256_file
from .config import Config
from .migration.time import datetime_to_epoch_us
from .migration.types import SqlRow, SqlValue, StorageDigest, TableDigest
from .storage.catalog import APPLICATION_TABLES, CATALOG, TableCatalog
from .storage.db import StorageConfigurationError, validate_storage_config

BackupJson = dict[str, Any]
BackupReceipt = dict[str, Any]
PgEnv = dict[str, str]
RowValue = str | int | float | bool | None | dt.datetime
DbRow = tuple[RowValue, ...]
DbCursor = Any
SqlModule = Any

APP_TABLES = tuple(APPLICATION_TABLES)
DEFAULT_PG_BIN = "/opt/homebrew/opt/postgresql@16/bin"
FETCH_CHUNK = 50_000


@dataclass(frozen=True, slots=True)
class VerifiedBackupFetch:
    artifact: str
    manifest: str
    receipt: BackupReceipt


@dataclass(frozen=True, slots=True)
class RestoreDrillResult:
    status: str
    database: str
    tables: dict[str, int]
    cleanup: str

    def to_json(self) -> BackupJson:
        return {
            "status": self.status,
            "database": self.database,
            "tables": self.tables,
            "cleanup": self.cleanup,
        }


class BackupError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason: str
        self.reason = reason

    def __str__(self) -> str:
        return self.reason


@dataclass(frozen=True, slots=True)
class BackupManifest:
    object_id: str
    artifact: str
    artifact_sha256: str
    artifact_bytes: int
    snapshot_id: str
    table_counts: dict[str, int]
    table_digests: dict[str, str]
    created_at: str
    format: str = "pg_dump custom"

    def to_json(self) -> BackupJson:
        return {
            "object_id": self.object_id,
            "artifact": self.artifact,
            "artifact_sha256": self.artifact_sha256,
            "artifact_bytes": self.artifact_bytes,
            "snapshot_id": self.snapshot_id,
            "table_counts": self.table_counts,
            "table_digests": self.table_digests,
            "created_at": self.created_at,
            "format": self.format,
        }


def write_backup_manifest(
    artifact: os.PathLike[str] | str,
    manifest_path: os.PathLike[str] | str,
    *,
    object_id: str,
    snapshot_id: str,
    table_counts: dict[str, int],
    table_digests: dict[str, str],
) -> pathlib.Path:
    artifact_path = pathlib.Path(artifact)
    manifest = BackupManifest(
        object_id=object_id,
        artifact=artifact_path.name,
        artifact_sha256=_sha256_file(str(artifact_path)),
        artifact_bytes=artifact_path.stat().st_size,
        snapshot_id=snapshot_id,
        table_counts=table_counts,
        table_digests=table_digests,
        created_at=dt.datetime.now(dt.timezone.utc).isoformat(),
    )
    target = pathlib.Path(manifest_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".partial")
    tmp.write_text(json.dumps(manifest.to_json(), sort_keys=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(target)
    return target


def _read_json(path: str) -> BackupJson:
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError) as exc:
        raise BackupError(f"cannot read backup manifest: {_redact(str(exc))}") from exc
    if not isinstance(raw, dict):
        raise BackupError("backup manifest is not an object")
    return {str(key): value for key, value in raw.items()}


def _write_receipt(receipt: BackupReceipt, receipt_dir: str, object_id: str) -> None:
    os.makedirs(receipt_dir, exist_ok=True)
    target = pathlib.Path(receipt_dir) / f"{object_id.replace('/', '_')}.json"
    fd, tmp = tempfile.mkstemp(prefix=target.name, suffix=".partial", dir=receipt_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(receipt, fh, sort_keys=True, indent=2)
            fh.write("\n")
        os.replace(tmp, target)
    finally:
        with contextlib.suppress(OSError):
            os.remove(tmp)


def verify_remote_backup(
    *,
    artifact: str,
    manifest: str,
    object_id: str,
    push_command: str,
    fetch_command: str,
    receipt_dir: str,
    timeout_s: float = 1800.0,
    push_first: bool = True,
) -> BackupReceipt:
    expected = _read_json(manifest)
    expected_artifact_sha = str(expected.get("artifact_sha256", ""))
    expected_artifact_bytes = expected.get("artifact_bytes")
    artifact_exists = os.path.exists(artifact)
    artifact_sha = _sha256_file(artifact) if artifact_exists else expected_artifact_sha
    manifest_sha = _sha256_file(manifest)
    if push_first and not artifact_exists:
        raise BackupError("local artifact is required before remote push")
    if artifact_exists and expected_artifact_sha != artifact_sha:
        raise BackupError("local artifact digest does not match manifest")
    if artifact_exists and expected_artifact_bytes != os.path.getsize(artifact):
        raise BackupError("local artifact size does not match manifest")
    manifest_object = f"{object_id}.json"
    try:
        if push_first:
            _run_transfer(push_command, source=artifact, object_id=object_id, timeout_s=timeout_s)
            _run_transfer(push_command, source=manifest, object_id=manifest_object, timeout_s=timeout_s)
        with tempfile.TemporaryDirectory(prefix="mdfeed-backup-fetch-") as tmpdir:
            fetched_artifact = artifact if not artifact_exists and not push_first else os.path.join(
                tmpdir, pathlib.Path(artifact).name)
            fetched_manifest = fetched_artifact + ".json"
            pathlib.Path(fetched_artifact).parent.mkdir(parents=True, exist_ok=True)
            _run_transfer(fetch_command, object_id=object_id, destination=fetched_artifact, timeout_s=timeout_s)
            _run_transfer(fetch_command, object_id=manifest_object, destination=fetched_manifest, timeout_s=timeout_s)
            remote_manifest_sha = _sha256_file(fetched_manifest)
            if remote_manifest_sha != manifest_sha:
                raise BackupError("remote manifest digest does not match trusted manifest")
            remote_artifact_sha = _sha256_file(fetched_artifact)
            remote_artifact_bytes = os.path.getsize(fetched_artifact)
            if remote_artifact_sha != expected_artifact_sha:
                raise BackupError("remote artifact digest does not match trusted manifest")
            if remote_artifact_bytes != expected_artifact_bytes:
                raise BackupError("remote artifact size does not match trusted manifest")
    except BackupError:
        raise
    except Exception as exc:
        raise BackupError(_redact(str(exc))) from exc
    receipt = {
        "schema_version": 1,
        "status": "verified",
        "object_id": object_id,
        "artifact_sha256": artifact_sha,
        "remote_artifact_sha256": expected_artifact_sha,
        "artifact_bytes": int(expected_artifact_bytes or 0),
        "remote_artifact_bytes": int(expected_artifact_bytes or 0),
        "manifest_sha256": manifest_sha,
        "remote_manifest_sha256": manifest_sha,
        "verified_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    _write_receipt(receipt, receipt_dir, object_id)
    return receipt


def _pg_bin(name: str) -> str:
    path = pathlib.Path(DEFAULT_PG_BIN) / name
    return str(path) if path.exists() else name


def _dsn_from_env(env_name: str) -> str:
    dsn = os.getenv(env_name)
    if not dsn:
        raise BackupError(f"{env_name} is required")
    return dsn


def _psycopg2_extensions():
    try:
        import psycopg2.extensions
    except ImportError as exc:
        raise BackupError("psycopg2 is required for PostgreSQL backup") from exc
    return psycopg2.extensions


def _run_pg(argv: list[str], *, dsn: str, timeout_s: float,
            dsn_env: str = "DATABASE_URL") -> None:
    env = os.environ.copy()
    env.update(_pg_env_from_dsn(dsn, dsn_env=dsn_env))
    try:
        result = subprocess.run(argv, env=env, capture_output=True,
                                timeout=timeout_s, check=False)
    except OSError as exc:
        raise BackupError(f"PostgreSQL tool failed: {type(exc).__name__}: {_redact(str(exc))}") from exc
    except subprocess.TimeoutExpired as exc:
        raise BackupError(f"PostgreSQL tool timed out after {timeout_s:g}s: {_redact(str(exc))}") from exc
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace")
        raise BackupError(f"PostgreSQL tool exited {result.returncode}: {_redact(stderr)}")


def _validate_postgres_dsn(dsn: str, dsn_env: str) -> None:
    cfg = Config()
    cfg.storage_backend = "postgres"
    cfg.storage_profile = os.getenv("MDFEED_STORAGE_PROFILE", cfg.storage_profile)
    cfg.pg_dsn = dsn
    try:
        validate_storage_config(cfg)
    except StorageConfigurationError as exc:
        raise BackupError(f"{dsn_env} failed PostgreSQL storage validation: {_redact(str(exc))}") from exc


def _connect(dsn: str, *, dsn_env: str = "DATABASE_URL"):
    _validate_postgres_dsn(dsn, dsn_env)
    try:
        import psycopg2
        import psycopg2.extras
        import psycopg2.sql
    except ImportError as exc:
        raise BackupError("psycopg2 is required for PostgreSQL backup") from exc
    return psycopg2, psycopg2.extras, psycopg2.sql, psycopg2.connect(dsn)


def _pg_env_from_dsn(dsn: str, *, dsn_env: str = "DATABASE_URL") -> dict[str, str]:
    _validate_postgres_dsn(dsn, dsn_env)
    extensions = _psycopg2_extensions()
    try:
        parts = extensions.parse_dsn(dsn)
    except Exception as exc:
        raise BackupError(f"{dsn_env} could not be parsed for PostgreSQL tools") from exc
    mapping = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
    }
    return {env_key: str(parts[key]) for key, env_key in mapping.items() if parts.get(key)}


def _table_exists(cur: DbCursor, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cur.fetchone()
    return bool(row and row[0])


def _snapshot_digest(conn, sqlmod: SqlModule) -> StorageDigest:
    tables = tuple(_stream_table_digest(conn, sqlmod, table) for table in CATALOG)
    payload = {table.name: table.to_json() for table in tables}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    import hashlib
    return StorageDigest(fingerprint=hashlib.sha256(encoded).hexdigest(), tables=tables)


def _stream_table_digest(conn, sqlmod: SqlModule, table: TableCatalog) -> TableDigest:
    import hashlib
    with conn.cursor() as check:
        if not _table_exists(check, table.name):
            raise BackupError(f"required application table is missing: {table.name}")
    hasher = hashlib.sha256()
    row_count = 0
    null_counts = {column.name: 0 for column in table.columns if column.nullable}
    min_times: dict[str, int | None] = {column: None for column in table.timestamp_columns}
    max_times: dict[str, int | None] = {column: None for column in table.timestamp_columns}
    query = sqlmod.SQL("SELECT {} FROM {} ORDER BY {}").format(
        sqlmod.SQL(", ").join(sqlmod.Identifier(column) for column in table.postgres_columns),
        sqlmod.Identifier(table.name),
        sqlmod.SQL(", ").join(sqlmod.Identifier(column) for column in _order_columns(table)),
    )
    cursor_name = f"mdfeed_backup_digest_{table.name}"
    with conn.cursor(name=cursor_name) as cur:
        cur.itersize = FETCH_CHUNK
        cur.execute(query)
        while True:
            rows = cur.fetchmany(FETCH_CHUNK)
            if not rows:
                break
            for row in rows:
                normalized = _normalize_postgres_row(table, tuple(row))
                hasher.update(json.dumps(normalized, separators=(",", ":"), ensure_ascii=True).encode())
                hasher.update(b"\n")
                row_count += 1
                for index, column in enumerate(table.columns):
                    value = normalized[index]
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


def _order_columns(table: TableCatalog) -> tuple[str, ...]:
    ordered = list(table.verification_order)
    for column in table.postgres_columns:
        if column not in ordered:
            ordered.append(column)
    return tuple(ordered)


def _normalize_postgres_row(table: TableCatalog, row: DbRow) -> SqlRow:
    values: list[SqlValue] = []
    for index, column in enumerate(table.columns):
        value = row[index]
        if value is None:
            values.append(None)
        elif column.time_encoding is not None:
            if not isinstance(value, dt.datetime):
                raise BackupError("PostgreSQL timestamp column is not a datetime")
            values.append(datetime_to_epoch_us(value))
        elif column.name == "active":
            values.append(1 if value else 0)
        elif isinstance(value, dt.datetime):
            raise BackupError("PostgreSQL datetime value appeared in a non-time column")
        else:
            values.append(value)
    return tuple(values)


def create_backup(
    *,
    dsn_env: str,
    output_dir: str,
    object_id: str | None = None,
    push_command: str = "",
    fetch_command: str = "",
    receipt_dir: str | None = None,
    timeout_s: float = 1800.0,
) -> tuple[pathlib.Path, pathlib.Path]:
    dsn = _dsn_from_env(dsn_env)
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    backup_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    dump = pathlib.Path(output_dir) / f"mdfeed-{backup_id}-{secrets.token_hex(4)}.dump"
    partial_dump = dump.with_suffix(dump.suffix + ".partial")
    object_name = object_id or f"backups/{dump.name}"
    conn = None
    try:
        psycopg2, _extras, sqlmod, conn = _connect(dsn, dsn_env=dsn_env)
        conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ, readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_export_snapshot()")
            snapshot_row = cur.fetchone()
            if snapshot_row is None:
                raise BackupError("PostgreSQL did not export a backup snapshot")
            snapshot_id = str(snapshot_row[0])
            digest = _snapshot_digest(conn, sqlmod)
            counts = {table.name: table.row_count for table in digest.tables}
            digests = {table.name: table.content_hash for table in digest.tables}
            _run_pg(
                [_pg_bin("pg_dump"), "--format=custom", "--file", str(partial_dump), "--snapshot", snapshot_id],
                dsn=dsn,
                timeout_s=timeout_s,
                dsn_env=dsn_env,
            )
            partial_dump.replace(dump)
    except Exception as exc:
        if isinstance(exc, BackupError):
            raise
        raise BackupError(f"backup create failed for {dsn_env}: {_redact(str(exc))}") from exc
    finally:
        with contextlib.suppress(OSError):
            partial_dump.unlink()
        if conn is not None:
            conn.close()
    manifest = write_backup_manifest(
        dump,
        str(dump) + ".json",
        object_id=object_name,
        snapshot_id=snapshot_id,
        table_counts=counts,
        table_digests=digests,
    )
    if push_command or fetch_command:
        if receipt_dir is None:
            receipt_dir = output_dir
        verify_remote_backup(
            artifact=str(dump),
            manifest=str(manifest),
            object_id=object_name,
            push_command=push_command,
            fetch_command=fetch_command,
            receipt_dir=receipt_dir,
            timeout_s=timeout_s,
        )
    return dump, manifest


def _with_database(dsn: str, database: str) -> str:
    extensions = _psycopg2_extensions()
    parts = extensions.parse_dsn(dsn)
    parts["dbname"] = database
    return extensions.make_dsn(**parts)


def _database_name(dsn: str) -> str:
    if not dsn:
        return ""
    extensions = _psycopg2_extensions()
    return str(extensions.parse_dsn(dsn).get("dbname", ""))


def restore_drill(
    artifact: str,
    manifest: str,
    *,
    maintenance_dsn_env: str = "MDFEED_MAINTENANCE_DSN",
    timeout_s: float = 1800.0,
) -> RestoreDrillResult:
    maintenance_dsn = _dsn_from_env(maintenance_dsn_env)
    data = _read_json(manifest)
    if _sha256_file(artifact) != data.get("artifact_sha256"):
        raise BackupError("restore artifact digest does not match trusted manifest")
    source_db = _database_name(os.getenv("DATABASE_URL", ""))
    database = "mdfeed_restore_test_" + secrets.token_hex(8)
    if database in {source_db, "postgres", "template0", "template1"}:
        raise BackupError("restore target database name is not safe")
    _psycopg2, _extras, sqlmod, conn = _connect(maintenance_dsn, dsn_env=maintenance_dsn_env)
    created = False
    target_dsn = _with_database(maintenance_dsn, database)
    result: RestoreDrillResult | None = None
    error: BaseException | None = None
    try:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT oid FROM pg_database WHERE datname=%s", (database,))
            if cur.fetchone() is not None:
                raise BackupError("restore target database already exists")
            cur.execute(sqlmod.SQL("CREATE DATABASE {}").format(sqlmod.Identifier(database)))
            created = True
        _run_pg([_pg_bin("pg_restore"), "--dbname", database, "--no-owner", artifact],
                dsn=target_dsn, timeout_s=timeout_s, dsn_env=maintenance_dsn_env)
        restored = _reconcile_restored(target_dsn, data)
        result = RestoreDrillResult(
            status="restored",
            database=database,
            tables=restored,
            cleanup="pending",
        )
    except BackupError:
        error = sys.exc_info()[1]
        raise
    except Exception as exc:
        error = exc
        text = _redact(str(exc))
        if "permission denied" in text.lower() or "createdb" in text.lower():
            raise BackupError(f"{maintenance_dsn_env} lacks CREATEDB authority") from exc
        raise BackupError(f"restore drill failed: {text}") from exc
    finally:
        cleanup_error = None
        if created:
            try:
                with conn.cursor() as cur:
                    cur.execute(sqlmod.SQL("DROP DATABASE {} WITH (FORCE)").format(sqlmod.Identifier(database)))
            except _psycopg2.Error as exc:
                cleanup_error = exc
        conn.close()
        if cleanup_error is not None and error is None:
            raise BackupError(f"restore cleanup failed: {_redact(str(cleanup_error))}") from cleanup_error
    if result is None:
        raise BackupError("restore drill did not produce a result")
    return RestoreDrillResult(
        status=result.status,
        database=result.database,
        tables=result.tables,
        cleanup="dropped",
    )


def restore_remote_drill(
    *,
    manifest: str,
    object_id: str,
    fetch_command: str,
    maintenance_dsn_env: str = "MDFEED_MAINTENANCE_DSN",
    timeout_s: float = 1800.0,
) -> RestoreDrillResult:
    with tempfile.TemporaryDirectory(prefix="mdfeed-backup-restore-fetch-") as tmpdir:
        artifact_path = os.path.join(tmpdir, pathlib.Path(object_id).name)
        manifest_path = artifact_path + ".json"
        receipt = verify_remote_backup(
            artifact=artifact_path,
            manifest=manifest,
            object_id=object_id,
            push_command="",
            fetch_command=fetch_command,
            receipt_dir=tmpdir,
            timeout_s=timeout_s,
            push_first=False,
        )
        _run_transfer(fetch_command, object_id=f"{object_id}.json", destination=manifest_path, timeout_s=timeout_s)
        if receipt["remote_manifest_sha256"] != _sha256_file(manifest_path):
            raise BackupError("fetched restore manifest is not bound to receipt")
        return restore_drill(
            artifact_path,
            manifest_path,
            maintenance_dsn_env=maintenance_dsn_env,
            timeout_s=timeout_s,
        )


def _reconcile_restored(dsn: str, manifest: BackupJson) -> dict[str, int]:
    _psycopg2, _extras, sqlmod, conn = _connect(dsn, dsn_env="restore target")
    try:
        digest = _snapshot_digest(conn, sqlmod)
        counts = {table.name: table.row_count for table in digest.tables}
        digests = {table.name: table.content_hash for table in digest.tables}
    finally:
        conn.close()
    expected_counts = manifest.get("table_counts", {})
    expected_digests = manifest.get("table_digests", {})
    if not isinstance(expected_counts, dict) or not isinstance(expected_digests, dict):
        raise BackupError("backup manifest missing table digests")
    for table in APP_TABLES:
        if counts.get(table) != expected_counts.get(table):
            raise BackupError(f"restored {table} count does not match manifest")
        if digests.get(table) != expected_digests.get(table):
            raise BackupError(f"restored {table} digest does not match manifest")
    return counts


def _status(receipt_dir: str) -> int:
    receipts = sorted(pathlib.Path(receipt_dir).glob("*.json"))
    print(json.dumps({"receipt_dir": receipt_dir, "receipts": len(receipts)}, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mdfeed.backup")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--dsn-env", default="DATABASE_URL")
    create.add_argument("--output-dir", required=True)
    create.add_argument("--object-id")
    create.add_argument("--push-command", default=os.getenv("MDFEED_BACKUP_PUSH_COMMAND", ""))
    create.add_argument("--fetch-command", default=os.getenv("MDFEED_BACKUP_FETCH_COMMAND", ""))
    create.add_argument("--receipt-dir")
    create.add_argument("--timeout-s", type=float, default=1800.0)
    verify = sub.add_parser("verify-remote")
    verify.add_argument("--artifact", required=True)
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--object-id", required=True)
    verify.add_argument("--fetch-command", default=os.getenv("MDFEED_BACKUP_FETCH_COMMAND", ""))
    verify.add_argument("--receipt-dir", required=True)
    verify.add_argument("--timeout-s", type=float, default=1800.0)
    drill = sub.add_parser("restore-drill")
    drill.add_argument("--artifact", required=True)
    drill.add_argument("--manifest", required=True)
    drill.add_argument("--object-id")
    drill.add_argument("--fetch-command", default=os.getenv("MDFEED_BACKUP_FETCH_COMMAND", ""))
    drill.add_argument("--maintenance-dsn-env", default="MDFEED_MAINTENANCE_DSN")
    drill.add_argument("--timeout-s", type=float, default=1800.0)
    status = sub.add_parser("status")
    status.add_argument("--receipt-dir", required=True)
    args = parser.parse_args(argv)
    try:
        match args.command:
            case "create":
                dump, manifest = create_backup(
                    dsn_env=args.dsn_env,
                    output_dir=args.output_dir,
                    object_id=args.object_id,
                    push_command=args.push_command,
                    fetch_command=args.fetch_command,
                    receipt_dir=args.receipt_dir,
                    timeout_s=args.timeout_s,
                )
                print(json.dumps({"artifact": str(dump), "manifest": str(manifest)}, sort_keys=True))
            case "verify-remote":
                receipt = verify_remote_backup(
                    artifact=args.artifact,
                    manifest=args.manifest,
                    object_id=args.object_id,
                    push_command="",
                    fetch_command=args.fetch_command,
                    receipt_dir=args.receipt_dir,
                    timeout_s=args.timeout_s,
                    push_first=False,
                )
                print(json.dumps(receipt, sort_keys=True))
            case "restore-drill":
                if args.object_id and args.fetch_command:
                    restore_result = restore_remote_drill(
                        manifest=args.manifest,
                        object_id=args.object_id,
                        fetch_command=args.fetch_command,
                        maintenance_dsn_env=args.maintenance_dsn_env,
                        timeout_s=args.timeout_s,
                    )
                else:
                    restore_result = restore_drill(
                        args.artifact,
                        args.manifest,
                        maintenance_dsn_env=args.maintenance_dsn_env,
                        timeout_s=args.timeout_s,
                    )
                print(json.dumps(restore_result.to_json(), sort_keys=True))
            case "status":
                return _status(args.receipt_dir)
            case _:
                parser.error("unknown command")
    except BackupError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
