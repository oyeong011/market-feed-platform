#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

from mdfeed import backup


def _required_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        print(f"{name} is required", file=sys.stderr)
        raise SystemExit(2)
    return value


def main() -> int:
    output_dir = _required_env("MDFEED_BACKUP_DIR")
    receipt_dir = _required_env("MDFEED_BACKUP_RECEIPT_DIR")
    _ = _required_env("MDFEED_BACKUP_DATABASE_URL")
    _ = _required_env("MDFEED_BACKUP_PUSH_COMMAND")
    _ = _required_env("MDFEED_BACKUP_FETCH_COMMAND")
    return backup.main([
        "create",
        "--dsn-env",
        "MDFEED_BACKUP_DATABASE_URL",
        "--output-dir",
        output_dir,
        "--receipt-dir",
        receipt_dir,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
