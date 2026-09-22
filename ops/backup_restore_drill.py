#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def _latest_manifest(backup_dir: Path) -> Path | None:
    manifests = sorted(backup_dir.glob("*.dump.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1] if manifests else None


def _read_manifest(path: Path) -> dict[str, str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("backup manifest is not an object")
    return {str(key): str(value) for key, value in raw.items()}


def main() -> int:
    parser = argparse.ArgumentParser("mdfeed-restore-drill-runner")
    _ = parser.add_argument("--backup-dir", required=True)
    _ = parser.add_argument("--receipt-dir", required=True)
    _ = parser.add_argument("--maintenance-dsn-env", default="MDFEED_MAINTENANCE_DSN")
    _ = parser.add_argument("--timeout-s", default="1800")
    args = parser.parse_args()

    backup_dir = Path(args.backup_dir)
    manifest = _latest_manifest(backup_dir)
    if manifest is None:
        print(f"no backup manifest found in {backup_dir}", file=sys.stderr)
        return 1
    try:
        data = _read_manifest(manifest)
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        print(f"cannot read backup manifest: {exc}", file=sys.stderr)
        return 1
    artifact = backup_dir / data.get("artifact", "")
    if not artifact.is_file():
        print(f"backup artifact missing for manifest {manifest.name}", file=sys.stderr)
        return 1

    command = [
        sys.executable,
        "-m",
        "mdfeed.backup",
        "restore-drill",
        "--artifact",
        str(artifact),
        "--manifest",
        str(manifest),
        "--maintenance-dsn-env",
        str(args.maintenance_dsn_env),
        "--timeout-s",
        str(args.timeout_s),
    ]
    object_id = data.get("object_id")
    fetch_command = os.getenv("MDFEED_BACKUP_FETCH_COMMAND", "")
    if object_id and fetch_command:
        command.extend(["--object-id", object_id, "--fetch-command", fetch_command])
    result = subprocess.run(command, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
