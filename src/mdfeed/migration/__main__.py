from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .engine import migration_status, receipt_json, run_migration, verify_migration
from .sqlite_source import plan_sqlite_source
from .types import MigrationPreflightError


def _target_dsn(env_name: str) -> str:
    dsn = os.getenv(env_name, "")
    if not dsn:
        raise MigrationPreflightError(f"target DSN environment variable is empty: {env_name}")
    return dsn


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source", required=True)
    parser.add_argument("--target-dsn-env", default="MDFEED_MIGRATION_TARGET_DSN")
    parser.add_argument("--run-id", default="manual-run")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("python -m mdfeed.migration")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    _add_common(plan)
    run = sub.add_parser("run")
    _add_common(run)
    run.add_argument("--chunk-size", type=int, default=50_000)
    run.add_argument("--init-target", action="store_true")
    status = sub.add_parser("status")
    _add_common(status)
    verify = sub.add_parser("verify")
    _add_common(verify)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        source = Path(args.source)
        dsn = _target_dsn(args.target_dsn_env)
        match args.command:
            case "plan":
                print(receipt_json_placeholder(args.run_id, plan_sqlite_source(source)))
            case "run":
                print(
                    receipt_json(
                        run_migration(
                            source,
                            dsn,
                            args.run_id,
                            chunk_size=args.chunk_size,
                            init_target=args.init_target,
                        )
                    )
                )
            case "status":
                print(receipt_json(migration_status(source, dsn, args.run_id)))
            case "verify":
                print(receipt_json(verify_migration(source, dsn, args.run_id)))
            case unreachable:
                raise AssertionError(f"unknown command: {unreachable}")
    except MigrationPreflightError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def receipt_json_placeholder(run_id: str, source_digest) -> str:
    from .types import MigrationReceipt

    return receipt_json(
        MigrationReceipt(
            run_id=run_id,
            source_fingerprint=source_digest.fingerprint,
            completed=False,
            source=source_digest,
            target=None,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
