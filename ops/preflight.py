#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

STALE_BACKUP_SECONDS = 26 * 60 * 60
STALE_RESTORE_SECONDS = 8 * 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class Check:
    code: str
    level: str
    message: str


@dataclass(frozen=True, slots=True)
class PreflightResult:
    status: str
    runtime: str
    exit_code: int
    summary: str
    checks: list[Check]
    metrics: dict[str, float]


def load_env(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _sanitize(value: str) -> str:
    out = value
    parsed = urlsplit(out)
    if parsed.password:
        out = out.replace(parsed.password, "***")
    for marker in ("password=", "PGPASSWORD=", "passfile=", "token=", "secret="):
        idx = out.lower().find(marker.lower())
        if idx < 0:
            continue
        start = idx + len(marker)
        end = len(out)
        for sep in (" ", "&", "\n", "\t"):
            pos = out.find(sep, start)
            if pos >= 0:
                end = min(end, pos)
        out = out[:start] + "***" + out[end:]
    return out


def _dsn_parts(dsn: str) -> dict[str, str]:
    if "://" in dsn:
        parsed = urlsplit(dsn)
        parts = {key: value[-1] for key, value in parse_qs(parsed.query).items() if value}
        if parsed.hostname:
            parts["host"] = parsed.hostname
        if parsed.username:
            parts["user"] = parsed.username
        if parsed.path and parsed.path != "/":
            parts["dbname"] = parsed.path.lstrip("/")
        return parts
    parts: dict[str, str] = {}
    for item in shlex.split(dsn):
        key, sep, value = item.partition("=")
        if sep:
            parts[key] = value
    return parts


def _check_transfer_template(
    checks: list[Check],
    env: dict[str, str],
    key: str,
    required: set[str],
) -> None:
    template = env.get(key, "")
    missing_label = key.removeprefix("MDFEED_")
    template_label = missing_label.removesuffix("_COMMAND")
    if not template:
        checks.append(Check(f"{missing_label}_MISSING", "error", f"{key} is required"))
        return
    try:
        argv = shlex.split(template)
    except ValueError as exc:
        checks.append(Check(f"{template_label}_TEMPLATE_UNSAFE", "error", f"{key} cannot be parsed: {exc}"))
        return
    present = {name for name in ("source", "object", "destination") if "{" + name + "}" in template}
    missing = sorted(required - present)
    extra_source = "source" in present and "source" not in required
    extra_destination = "destination" in present and "destination" not in required
    if missing or extra_source or extra_destination:
        checks.append(
            Check(
                f"{template_label}_TEMPLATE_UNSAFE",
                "error",
                f"{key} must use placeholders {sorted(required)} with shell=False",
            )
        )
        return
    if not argv or shutil.which(argv[0]) is None:
        checks.append(Check(f"{template_label}_COMMAND_UNAVAILABLE", "warn", f"{key} executable is unavailable"))


def _parse_verified_at(data: dict[str, str]) -> datetime | None:
    raw = data.get("verified_at") or data.get("created_at")
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _receipt_age_seconds(verified_at: datetime) -> float:
    return max(0.0, (datetime.now(timezone.utc) - verified_at).total_seconds())


def _check_receipt_file(
    checks: list[Check],
    metrics: dict[str, float],
    env: dict[str, str],
    key: str,
    code: str,
    metric_name: str,
) -> None:
    path = env.get(key, "")
    if not path:
        checks.append(Check(f"{code}_MISSING", "warn", f"{key} is required for verified readiness"))
        metrics[metric_name] = -1.0
        return
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        checks.append(Check(f"{code}_UNREADABLE", "warn", f"{key} is not present"))
        return
    except (json.JSONDecodeError, OSError) as exc:
        checks.append(Check(f"{code}_UNREADABLE", "warn", f"{key} is not usable: {_sanitize(str(exc))}"))
        return
    if not isinstance(data, dict):
        checks.append(Check(f"{code}_UNREADABLE", "warn", f"{key} must contain a JSON object"))
        return
    status = str(data.get("status", "")).lower()
    if status not in {"verified", "restored"}:
        checks.append(Check(f"{code}_NOT_VERIFIED", "warn", f"{key} has no verified/restored status"))
        return
    verified_at = _parse_verified_at({str(k): str(v) for k, v in data.items()})
    if verified_at is None:
        checks.append(Check(f"{code}_AGE_UNKNOWN", "warn", f"{key} has no parseable verification time"))
        metrics[metric_name] = -1.0
        return
    metrics[metric_name] = _receipt_age_seconds(verified_at)


def _check_gap_file(checks: list[Check], metrics: dict[str, float], env: dict[str, str]) -> None:
    path = env.get("MDFEED_GAP_STATE_FILE", "")
    if not path:
        checks.append(Check("GAP_STATE_MISSING", "warn", "MDFEED_GAP_STATE_FILE is required"))
        return
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        checks.append(Check("GAP_STATE_UNAVAILABLE", "warn", "gap state file is not present yet"))
        return
    except (json.JSONDecodeError, OSError) as exc:
        checks.append(Check("GAP_STATE_UNAVAILABLE", "warn", f"gap state cannot be read: {_sanitize(str(exc))}"))
        return
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        checks.append(Check("GAP_STATE_UNAVAILABLE", "warn", "gap state JSON must expose items"))
        return
    open_count = sum(1 for item in items if isinstance(item, dict) and item.get("state") != "RECOVERED")
    if open_count:
        checks.append(Check("OPEN_DATA_GAPS", "warn", f"{open_count} unrecovered data gap(s) remain"))
    metrics["mdfeed_deployment_gaps_open"] = float(open_count)


def _check_postgres_runtime(checks: list[Check], metrics: dict[str, float], dsn: str) -> bool:
    try:
        import psycopg2
    except ImportError:
        checks.append(Check("PSYCOPG2_MISSING", "error", "psycopg2 is required for PostgreSQL runtime checks"))
        return False
    try:
        conn = psycopg2.connect(dsn, connect_timeout=2)
    except psycopg2.Error as exc:
        detail = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        checks.append(Check("POSTGRES_RUNTIME_NOT_VERIFIED", "warn", _sanitize(detail)))
        return False
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            size_row = cur.fetchone()
            if size_row is None:
                checks.append(Check("POSTGRES_SIZE_UNKNOWN", "warn", "database size could not be measured"))
                return True
            size = size_row[0]
            if int(size) <= 0:
                checks.append(Check("POSTGRES_SIZE_UNKNOWN", "warn", "database size could not be measured"))
            cur.execute(
                "SELECT tablename FROM pg_catalog.pg_tables "
                "WHERE schemaname='public' AND tablename IN "
                "('instruments','trades','book_top','bars_1m','signals','feed_stats','latest','quality_events')"
            )
            found = {row[0] for row in cur.fetchall()}
            required = {"instruments", "trades", "book_top", "bars_1m", "signals", "feed_stats", "latest", "quality_events"}
            missing = sorted(required - found)
            if missing:
                checks.append(Check("POSTGRES_SCHEMA_INCOMPLETE", "error", f"missing tables: {', '.join(missing)}"))
            for table, privilege in (
                ("trades", "SELECT,INSERT,UPDATE,DELETE"),
                ("book_top", "SELECT,INSERT,UPDATE,DELETE"),
                ("bars_1m", "SELECT,INSERT,UPDATE,DELETE"),
                ("signals", "SELECT,INSERT,UPDATE,DELETE"),
                ("feed_stats", "SELECT,INSERT,UPDATE,DELETE"),
                ("latest", "SELECT,INSERT,UPDATE,DELETE"),
                ("quality_events", "SELECT,INSERT,UPDATE,DELETE"),
                ("ingest_batch_receipts", "SELECT,INSERT"),
                ("data_gaps", "SELECT,INSERT,UPDATE"),
            ):
                cur.execute("SELECT has_table_privilege(current_user, %s, %s)", (table, privilege))
                privilege_row = cur.fetchone()
                if privilege_row is None or not privilege_row[0]:
                    checks.append(Check("POSTGRES_ROLE_PRIVILEGE_MISSING", "error", f"runtime role lacks {privilege} on {table}"))
            for table in ("backup_restore_receipts", "archive_remote_receipts", "gap_recovery_receipts"):
                cur.execute("SELECT has_table_privilege(current_user, %s, 'INSERT')", (table,))
                proof_row = cur.fetchone()
                if proof_row and proof_row[0]:
                    checks.append(Check("POSTGRES_RUNTIME_PROOF_WRITE_PRIVILEGE", "error", f"runtime role can write proof table {table}"))
            cur.execute("SELECT COUNT(*) FROM data_gaps WHERE state <> 'RECOVERED'")
            gap_row = cur.fetchone()
            open_gaps = int(gap_row[0]) if gap_row is not None else 0
            metrics["mdfeed_deployment_gaps_open"] = float(open_gaps)
            if open_gaps:
                checks.append(Check("OPEN_DATA_GAPS", "warn", f"{open_gaps} unrecovered data gap(s) remain"))
            cur.execute("SELECT EXTRACT(EPOCH FROM now() - MAX(verified_at)) FROM archive_remote_receipts WHERE status = 'VERIFIED'")
            backup_age = cur.fetchone()
            if backup_age is None or backup_age[0] is None:
                checks.append(Check("BACKUP_RECEIPT_MISSING", "warn", "no verified remote backup receipt in PostgreSQL"))
            else:
                metrics["mdfeed_backup_last_verified_age_seconds"] = float(backup_age[0])
                if metrics["mdfeed_backup_last_verified_age_seconds"] > STALE_BACKUP_SECONDS:
                    checks.append(Check("BACKUP_RECEIPT_STALE", "warn", "latest verified remote backup receipt is stale"))
            cur.execute("SELECT EXTRACT(EPOCH FROM now() - MAX(verified_at)) FROM backup_restore_receipts WHERE status = 'VERIFIED'")
            restore_age = cur.fetchone()
            if restore_age is None or restore_age[0] is None:
                checks.append(Check("RESTORE_DRILL_MISSING", "warn", "no verified restore drill receipt in PostgreSQL"))
            else:
                metrics["mdfeed_restore_drill_last_verified_age_seconds"] = float(restore_age[0])
                if metrics["mdfeed_restore_drill_last_verified_age_seconds"] > STALE_RESTORE_SECONDS:
                    checks.append(Check("RESTORE_DRILL_STALE", "warn", "latest restore drill receipt is stale"))
    finally:
        conn.close()
    return True


def run(config_path: str) -> PreflightResult:
    env = load_env(config_path)
    checks: list[Check] = []
    dsn = env.get("DATABASE_URL", "")
    profile = env.get("MDFEED_STORAGE_PROFILE", "production")
    retention_days = env.get("MDFEED_RETENTION_DAYS", "0")
    runtime = "config-only"
    metrics = {
        "mdfeed_storage_backend_mismatch": 0.0,
        "mdfeed_storage_db_unavailable": 0.0,
        "mdfeed_writer_pending_rows": -1.0,
        "mdfeed_backup_last_verified_age_seconds": -1.0,
        "mdfeed_restore_drill_last_verified_age_seconds": -1.0,
        "mdfeed_retention_remote_coverage_blocked": 0.0,
        "mdfeed_deployment_gaps_open": -1.0,
    }

    if env.get("MDFEED_STORAGE_BACKEND", "postgres") != "postgres":
        checks.append(Check("STORAGE_BACKEND_MISMATCH", "error", "deployment storage backend must be postgres"))
        metrics["mdfeed_storage_backend_mismatch"] = 1.0
    if not dsn:
        checks.append(Check("DATABASE_URL_MISSING", "error", "DATABASE_URL is required"))
    else:
        parts = _dsn_parts(dsn)
        if parts.get("sslmode") != "verify-full":
            checks.append(Check("POSTGRES_TLS_VERIFY_FULL_MISSING", "error", "PostgreSQL must use sslmode=verify-full"))
        ca = parts.get("sslrootcert", "")
        if not ca:
            checks.append(Check("POSTGRES_CA_MISSING", "error", "PostgreSQL CA root certificate is required"))
        elif not Path(ca).is_file():
            checks.append(Check("POSTGRES_CA_UNREADABLE", "error", "PostgreSQL CA root certificate is not readable"))
        if parts.get("user") != "mdfeed_runtime":
            checks.append(Check("POSTGRES_RUNTIME_ROLE_UNEXPECTED", "warn", "runtime DSN should use mdfeed_runtime"))
    if profile == "test":
        checks.append(Check("SYNTHETIC_TEST_PROFILE", "warn", "synthetic test profile is not production-ready"))
        checks.append(Check("POSTGRES_RUNTIME_NOT_VERIFIED", "warn", "runtime connection is skipped for synthetic test profile"))
    elif profile != "production":
        checks.append(Check("STORAGE_PROFILE_UNSAFE", "error", "MDFEED_STORAGE_PROFILE must be production or test"))

    _check_transfer_template(checks, env, "MDFEED_BACKUP_PUSH_COMMAND", {"source", "object"})
    _check_transfer_template(checks, env, "MDFEED_BACKUP_FETCH_COMMAND", {"object", "destination"})

    if retention_days != "0":
        checks.append(Check("RETENTION_NOT_DISABLED", "error", "retention must remain disabled before restore proof"))
    if not env.get("MDFEED_MAINTENANCE_DSN_ENV"):
        checks.append(Check("MAINTENANCE_DSN_ENV_MISSING", "warn", "restore drills require a maintenance DSN env var name"))
    if not env.get("MDFEED_EXPECTED_CAPACITY_BYTES"):
        checks.append(Check("DATABASE_CAPACITY_UNKNOWN", "warn", "set MDFEED_EXPECTED_CAPACITY_BYTES for explicit capacity proof"))
    if shutil.which("pg_dump") is None:
        checks.append(Check("PG_DUMP_MISSING", "warn", "pg_dump is not installed"))
    if shutil.which("pg_restore") is None:
        checks.append(Check("PG_RESTORE_MISSING", "warn", "pg_restore is not installed"))

    if dsn and not any(check.level == "error" for check in checks) and profile != "test":
        runtime = "verified" if _check_postgres_runtime(checks, metrics, dsn) else "config-only"
    else:
        _check_receipt_file(
            checks,
            metrics,
            env,
            "MDFEED_BACKUP_RECEIPT_FILE",
            "BACKUP_RECEIPT",
            "mdfeed_backup_last_verified_age_seconds",
        )
        _check_receipt_file(
            checks,
            metrics,
            env,
            "MDFEED_RESTORE_RECEIPT_FILE",
            "RESTORE_DRILL",
            "mdfeed_restore_drill_last_verified_age_seconds",
        )
        _check_gap_file(checks, metrics, env)
    db_codes = {"DATABASE_URL_MISSING", "PSYCOPG2_MISSING", "POSTGRES_RUNTIME_NOT_VERIFIED", "POSTGRES_SCHEMA_INCOMPLETE"}
    if any(check.code in db_codes for check in checks):
        metrics["mdfeed_storage_db_unavailable"] = 1.0
    if any(check.code.startswith("BACKUP_") or check.code.startswith("RESTORE_DRILL_") for check in checks):
        metrics["mdfeed_retention_remote_coverage_blocked"] = 1.0

    has_error = any(check.level == "error" for check in checks)
    has_warn = any(check.level == "warn" for check in checks)
    if has_error:
        status = "unsafe"
        exit_code = 2
        summary = "deployment is unsafe; fix error checks before starting services"
    elif has_warn:
        status = "incomplete"
        exit_code = 1
        summary = "configuration is syntactically useful but not production-ready"
    else:
        status = "verified"
        exit_code = 0
        summary = "runtime, schema, backup, restore, and gap checks are verified"
    return PreflightResult(status, runtime, exit_code, summary, checks, metrics)


def _prometheus(result: PreflightResult) -> str:
    lines = []
    for name, value in sorted(result.metrics.items()):
        lines.append(f"{name} {value:g}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser("mdfeed-preflight")
    _ = parser.add_argument("--config", required=True)
    _ = parser.add_argument("--json", action="store_true")
    _ = parser.add_argument("--prometheus", action="store_true")
    args = parser.parse_args()
    result = run(args.config)
    payload = {
        "status": result.status,
        "runtime": result.runtime,
        "exit_code": result.exit_code,
        "summary": result.summary,
        "checks": [asdict(check) for check in result.checks],
    }
    if args.prometheus:
        print(_prometheus(result), end="")
    elif args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        for check in result.checks:
            print(f"{check.level.upper()} {check.code}: {check.message}")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
