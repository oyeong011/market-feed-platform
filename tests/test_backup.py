import json
import os
import subprocess
import sys

import pytest

from mdfeed import backup


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
        "    shutil.copyfile(root / sys.argv[3], sys.argv[4])\n"
        "else:\n"
        "    raise SystemExit(2)\n",
        encoding="utf-8",
    )
    remote = tmp_path / "remote"
    return (
        f"{sys.executable} {script} push {remote} {{source}} {{object}}",
        f"{sys.executable} {script} fetch {remote} {{object}} {{destination}}",
    )


def test_backup_remote_roundtrip_fetches_manifest_and_artifact(tmp_path):
    artifact = tmp_path / "db.dump"
    artifact.write_bytes(b"pg custom dump bytes")
    manifest = backup.write_backup_manifest(
        artifact,
        tmp_path / "db.dump.json",
        object_id="backup/test.dump",
        snapshot_id="snapshot-1",
        table_counts={"trades": 2, "book_top": 1},
        table_digests={"trades": "aa", "book_top": "bb"},
    )
    push, fetch = _remote_helper(tmp_path)

    receipt = backup.verify_remote_backup(
        artifact=str(artifact),
        manifest=str(manifest),
        object_id="backup/test.dump",
        push_command=push,
        fetch_command=fetch,
        receipt_dir=str(tmp_path / "receipts"),
        timeout_s=5,
    )

    assert receipt["status"] == "verified"
    assert receipt["artifact_sha256"] == receipt["remote_artifact_sha256"]
    assert receipt["manifest_sha256"] == receipt["remote_manifest_sha256"]


def test_backup_remote_tamper_is_rejected(tmp_path):
    artifact = tmp_path / "db.dump"
    artifact.write_bytes(b"pg custom dump bytes")
    manifest = backup.write_backup_manifest(
        artifact,
        tmp_path / "db.dump.json",
        object_id="backup/tamper.dump",
        snapshot_id="snapshot-1",
        table_counts={"trades": 2},
        table_digests={"trades": "aa"},
    )
    push, fetch = _remote_helper(tmp_path)
    backup._run_transfer(push, source=str(artifact), object_id="backup/tamper.dump", timeout_s=5)
    backup._run_transfer(push, source=str(manifest), object_id="backup/tamper.dump.json", timeout_s=5)
    (tmp_path / "remote" / "backup" / "tamper.dump").write_bytes(b"bad")

    with pytest.raises(backup.BackupError):
        backup.verify_remote_backup(
            artifact=str(artifact),
            manifest=str(manifest),
            object_id="backup/tamper.dump",
            push_command="",
            fetch_command=fetch,
            receipt_dir=str(tmp_path / "receipts"),
            timeout_s=5,
            push_first=False,
        )
    assert not list((tmp_path / "receipts").glob("*.json"))


def test_backup_cli_uses_env_name_and_redacts_dsn(tmp_path):
    pytest.importorskip("psycopg2")   # 드라이버가 없으면 DSN 검증 전에 끝나 이 시험의 대상 경로에 못 간다
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    env["SECRET_DSN"] = "postgresql://user:never-print-this@127.0.0.1:1/db"
    env["MDFEED_STORAGE_PROFILE"] = "test"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mdfeed.backup",
            "create",
            "--dsn-env",
            "SECRET_DSN",
            "--output-dir",
            str(tmp_path),
        ],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode != 0
    assert "never-print-this" not in result.stderr
    assert "SECRET_DSN" in result.stderr


def test_restore_drill_refuses_missing_maintenance_dsn(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"object_id": "backup/x.dump"}), encoding="utf-8")
    artifact = tmp_path / "x.dump"
    artifact.write_bytes(b"x")
    monkeypatch.delenv("MDFEED_MAINTENANCE_DSN", raising=False)

    with pytest.raises(backup.BackupError, match="MDFEED_MAINTENANCE_DSN"):
        backup.restore_drill(str(artifact), str(manifest), maintenance_dsn_env="MDFEED_MAINTENANCE_DSN")


def test_backup_create_uses_storage_tls_policy(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:never-print-this@example.com/db?sslmode=require")
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "production")

    with pytest.raises(backup.BackupError, match="sslmode=verify-full") as excinfo:
        backup.create_backup(dsn_env="DATABASE_URL", output_dir=str(tmp_path), timeout_s=1)

    assert "never-print-this" not in str(excinfo.value)


def test_backup_default_names_are_unique_and_published_atomically(monkeypatch, tmp_path):
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql):
            self.sql = sql

        def fetchone(self):
            return ("snapshot",)

    class _Conn:
        def set_session(self, **kwargs):
            self.session = kwargs

        def cursor(self):
            return _Cursor()

        def close(self):
            self.closed = True

    class _Extensions:
        ISOLATION_LEVEL_REPEATABLE_READ = 2

    class _Psycopg2:
        extensions = _Extensions()

    def fake_connect(dsn, *, dsn_env="DATABASE_URL"):
        return _Psycopg2(), object(), object(), _Conn()

    def fake_snapshot_digest(conn, sqlmod):
        table = backup.TableDigest(
            name="trades", row_count=0, null_counts={}, min_times={},
            max_times={}, content_hash="0" * 64)
        return backup.StorageDigest(fingerprint="0" * 64, tables=(table,))

    def fake_run_pg(argv, *, dsn, timeout_s, dsn_env="DATABASE_URL"):
        path = backup.pathlib.Path(argv[argv.index("--file") + 1])
        path.write_bytes(b"dump")

    monkeypatch.setenv("DATABASE_URL", "postgresql://user@127.0.0.1/db")
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "test")
    monkeypatch.setattr(backup, "_connect", fake_connect)
    monkeypatch.setattr(backup, "_snapshot_digest", fake_snapshot_digest)
    monkeypatch.setattr(backup, "_run_pg", fake_run_pg)

    first, first_manifest = backup.create_backup(dsn_env="DATABASE_URL", output_dir=str(tmp_path))
    second, second_manifest = backup.create_backup(dsn_env="DATABASE_URL", output_dir=str(tmp_path))

    assert first != second
    assert first.exists() and second.exists()
    assert not list(tmp_path.glob("*.partial"))
    assert json.loads(first_manifest.read_text(encoding="utf-8"))["object_id"] != json.loads(
        second_manifest.read_text(encoding="utf-8"))["object_id"]
