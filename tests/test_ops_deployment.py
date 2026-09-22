import json
import os
import signal
import subprocess

import pytest
import sys
import time
import urllib.request
from pathlib import Path


def test_preflight_blocks_missing_external_storage_without_secret_leak():
    result = subprocess.run(
        [
            sys.executable,
            "ops/preflight.py",
            "--config",
            "ops/mdfeed.env.example",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "never-print-this" not in result.stdout
    body = json.loads(result.stdout)
    codes = {item["code"] for item in body["checks"]}
    assert "DATABASE_URL_MISSING" in codes
    assert "BACKUP_FETCH_COMMAND_MISSING" in codes


def test_preflight_reports_test_fixture_as_incomplete_not_production_ready(tmp_path):
    cfg = tmp_path / "secure.env"
    ca = tmp_path / "ca.crt"
    ca.write_text("synthetic CA\n", encoding="utf-8")
    cfg.write_text(
        "\n".join([
            f"DATABASE_URL=postgresql://mdfeed_runtime@127.0.0.1:1/mdfeed?sslmode=verify-full&sslrootcert={ca}",
            "MDFEED_STORAGE_PROFILE=test",
            "MDFEED_BACKUP_PUSH_COMMAND=cp {source} {object}",
            "MDFEED_BACKUP_FETCH_COMMAND=cp {object} {destination}",
            "MDFEED_RETENTION_DAYS=0",
        ]),
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "ops/preflight.py", "--config", str(cfg), "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    body = json.loads(result.stdout)
    assert result.returncode == 1
    assert body["status"] == "incomplete"
    assert body["runtime"] == "config-only"
    assert "not production-ready" in body["summary"]
    codes = {item["code"] for item in body["checks"]}
    assert "SYNTHETIC_TEST_PROFILE" in codes
    assert "POSTGRES_RUNTIME_NOT_VERIFIED" in codes


def test_preflight_blocks_bad_tls_and_remote_templates_without_io(tmp_path):
    cfg = tmp_path / "bad.env"
    remote = tmp_path / "remote"
    cfg.write_text(
        "\n".join([
            "DATABASE_URL=postgresql://mdfeed_runtime:never-print-this@db.example/mdfeed?sslmode=require",
            f"MDFEED_BACKUP_PUSH_COMMAND=cp {{source}} {remote}/backup.dump",
            f"MDFEED_BACKUP_FETCH_COMMAND=cp {remote}/backup.dump {{destination}}",
            "MDFEED_RETENTION_DAYS=7",
        ]),
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "ops/preflight.py", "--config", str(cfg), "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    body = json.loads(result.stdout)
    assert result.returncode == 2
    assert body["status"] == "unsafe"
    assert "never-print-this" not in result.stdout
    assert not remote.exists()
    codes = {item["code"] for item in body["checks"]}
    assert "POSTGRES_TLS_VERIFY_FULL_MISSING" in codes
    assert "RETENTION_NOT_DISABLED" in codes
    assert "BACKUP_PUSH_TEMPLATE_UNSAFE" in codes
    assert "BACKUP_FETCH_TEMPLATE_UNSAFE" in codes


def test_preflight_config_only_complete_but_unverified_is_incomplete(tmp_path):
    pytest.importorskip("psycopg2")   # 드라이버 부재는 preflight 가 unsafe(2) 로 보는 게 맞다 — 이 시험의 대상은 아니다
    cfg = tmp_path / "prod.env"
    ca = tmp_path / "ca.crt"
    backup = tmp_path / "backup.json"
    restore = tmp_path / "restore.json"
    gap = tmp_path / "gaps.json"
    ca.write_text("synthetic CA\n", encoding="utf-8")
    backup.write_text(json.dumps({"status": "VERIFIED", "verified_at": "2026-09-09T00:00:00Z"}), encoding="utf-8")
    restore.write_text(json.dumps({"status": "restored", "verified_at": "2026-09-09T00:00:00Z"}), encoding="utf-8")
    gap.write_text(json.dumps({"items": []}), encoding="utf-8")
    cfg.write_text(
        "\n".join([
            f"DATABASE_URL=postgresql://mdfeed_runtime@127.0.0.1:1/mdfeed?sslmode=verify-full&sslrootcert={ca}",
            "MDFEED_MAINTENANCE_DSN_ENV=MDFEED_MAINTENANCE_DSN",
            "MDFEED_BACKUP_PUSH_COMMAND=cp {source} {object}",
            "MDFEED_BACKUP_FETCH_COMMAND=cp {object} {destination}",
            f"MDFEED_BACKUP_RECEIPT_FILE={backup}",
            f"MDFEED_RESTORE_RECEIPT_FILE={restore}",
            f"MDFEED_GAP_STATE_FILE={gap}",
            "MDFEED_RETENTION_DAYS=0",
            "MDFEED_EXPECTED_CAPACITY_BYTES=1000000",
        ]),
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "ops/preflight.py", "--config", str(cfg), "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    body = json.loads(result.stdout)
    assert result.returncode == 1
    assert body["status"] == "incomplete"
    assert body["runtime"] == "config-only"
    codes = {item["code"] for item in body["checks"]}
    assert "POSTGRES_RUNTIME_NOT_VERIFIED" in codes
    assert "DATABASE_CAPACITY_UNKNOWN" not in codes


def test_compose_files_do_not_publish_database_or_default_passwords():
    paths = ["docker-compose.yml", "docker-compose.server.yml", "docker-compose.observability.yml"]
    text = "\n".join(Path(path).read_text(encoding="utf-8") for path in paths)
    assert "mdfeed:mdfeed" not in text
    assert "5432:5432" not in text
    assert "GF_AUTH_ANONYMOUS_ENABLED: \"true\"" not in text
    assert "admin / admin" not in text
    assert "postgres_ca.crt:/run/secrets/postgres_ca.crt:ro" in text


def test_server_profile_has_bootstrap_roles_and_maintenance_timers():
    bootstrap = Path("ops/bootstrap_roles.sql").read_text(encoding="utf-8")
    assert "CREATE ROLE mdfeed_runtime LOGIN" in bootstrap
    assert "CREATE ROLE mdfeed_backup LOGIN" in bootstrap
    assert "CREATE ROLE mdfeed_maintenance LOGIN CREATEDB" in bootstrap
    assert "IF NOT EXISTS" in bootstrap
    assert "PASSWORD" not in bootstrap
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES" not in bootstrap
    assert "GRANT CREATE ON SCHEMA public TO mdfeed_runtime" not in bootstrap
    assert "ON trades, book_top, bars_1m, signals, feed_stats, latest, quality_events" in bootstrap
    assert "ON ingest_batch_receipts TO mdfeed_runtime" in bootstrap
    assert "ON backup_restore_receipts, archive_remote_receipts" in bootstrap

    backup_service = Path("ops/systemd/mdfeed-backup.service").read_text(encoding="utf-8")
    restore_service = Path("ops/systemd/mdfeed-restore-drill.service").read_text(encoding="utf-8")
    assert "python3 ops/backup_create.py" in backup_service
    assert "ops/backup_restore_drill.py" in restore_service
    assert "--backup-dir /var/lib/mdfeed/backups" in restore_service
    assert "--receipt-dir /var/lib/mdfeed/backup/receipts" in restore_service
    for text in (backup_service, restore_service):
        assert "NoNewPrivileges=true" in text
        assert "ProtectSystem=strict" in text
        assert "ReadWritePaths=/var/lib/mdfeed /var/log/mdfeed" in text


def test_backup_restore_runner_requires_real_artifact_and_manifest(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "ops/backup_restore_drill.py",
            "--backup-dir",
            str(tmp_path),
            "--receipt-dir",
            str(tmp_path / "receipts"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "no backup manifest" in result.stderr
    assert "--artifact" not in result.stderr


def test_backup_create_runner_requires_env_defaults(tmp_path):
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    for key in (
        "MDFEED_BACKUP_DIR",
        "MDFEED_BACKUP_RECEIPT_DIR",
        "MDFEED_BACKUP_DATABASE_URL",
        "MDFEED_BACKUP_PUSH_COMMAND",
        "MDFEED_BACKUP_FETCH_COMMAND",
    ):
        env.pop(key, None)
    result = subprocess.run(
        [sys.executable, "ops/backup_create.py"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 2
    assert "MDFEED_BACKUP_DIR is required" in result.stderr


def test_ops_install_creates_role_users_and_owned_directories():
    text = Path("ops/ops.sh").read_text(encoding="utf-8")
    assert "useradd --system --no-create-home --shell /usr/sbin/nologin mdfeed-backup" in text
    assert "useradd --system --no-create-home --shell /usr/sbin/nologin mdfeed-maintenance" in text
    assert "install -d -o mdfeed-backup -g mdfeed /var/lib/mdfeed/backups" in text
    assert "install -d -o mdfeed-maintenance -g mdfeed /var/lib/mdfeed/restore-drills" in text
    assert "mdfeed-backup.timer" in text
    assert "mdfeed-restore-drill.timer" in text


def test_alerts_cover_storage_deployment_metrics():
    alerts = Path("ops/observability/alerts.yml").read_text(encoding="utf-8")
    metrics = Path("scripts/verify_alerts.py").read_text(encoding="utf-8")
    for metric in (
        "mdfeed_storage_backend_mismatch",
        "mdfeed_storage_db_unavailable",
        "mdfeed_writer_pending_rows",
        "mdfeed_backup_last_verified_age_seconds",
        "mdfeed_restore_drill_last_verified_age_seconds",
        "mdfeed_retention_remote_coverage_blocked",
        "mdfeed_deployment_gaps_open",
    ):
        assert metric in alerts
        assert metric in metrics


def test_prometheus_scrapes_private_preflight_monitor():
    prometheus = Path("ops/observability/prometheus.yml").read_text(encoding="utf-8")
    assert "host.docker.internal:9120" in prometheus
    unit = Path("ops/systemd/mdfeed-preflight-monitor.service").read_text(encoding="utf-8")
    assert "127.0.0.1" in unit
    assert "ops/preflight_monitor.py" in unit


def test_preflight_monitor_serves_actual_metrics(tmp_path):
    cfg = tmp_path / "bad.env"
    cfg.write_text("MDFEED_STORAGE_BACKEND=sqlite\n", encoding="utf-8")
    port = str(19000 + os.getpid() % 1000)
    proc = subprocess.Popen(
        [
            sys.executable,
            "ops/preflight_monitor.py",
            "--config",
            str(cfg),
            "--host",
            "127.0.0.1",
            "--port",
            port,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(50):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=0.2) as response:
                    text = response.read().decode("utf-8")
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise AssertionError("monitor did not start")
        assert "mdfeed_storage_backend_mismatch 1" in text
        assert "mdfeed_storage_db_unavailable 1" in text
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def test_preflight_produces_alert_metrics(tmp_path):
    cfg = tmp_path / "bad.env"
    cfg.write_text("MDFEED_STORAGE_BACKEND=sqlite\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "ops/preflight.py", "--config", str(cfg), "--prometheus"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "mdfeed_storage_backend_mismatch 1" in result.stdout
    assert "mdfeed_storage_db_unavailable 1" in result.stdout
    assert "mdfeed_retention_remote_coverage_blocked 1" in result.stdout


def test_up_refuses_missing_storage_before_spawning_live_children():
    env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": "src",
    }
    result = subprocess.run(
        [sys.executable, "-m", "mdfeed.cli", "up"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )
    assert result.returncode == 2
    assert "STORAGE_PREFLIGHT_FAILED" in result.stderr
    assert "[supervisor]" not in result.stdout


def test_delegated_cli_help_names_real_subcommands():
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    for command, expected in (("migrate", "plan, run, status, verify"), ("backup", "restore-drill")):
        result = subprocess.run(
            [sys.executable, "-m", "mdfeed.cli", command, "--help"],
            check=False, capture_output=True, text=True, env=env, timeout=10,
        )
        assert result.returncode == 0
        assert expected in result.stdout
        assert "forwarded" not in result.stdout
