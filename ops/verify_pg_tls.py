#!/usr/bin/env python3
"""DATABASE_URL 로 **실제로 붙어서** TLS 와 역할을 확인한다.

이 스크립트가 있는 이유. compose 의 DSN 은 `sslmode=verify-full` 이었는데
서버에 TLS 설정이 없고 역할도 만들어지지 않아서 **붙을 수 없는 문자열**이었다.
그런데 `docker compose config -q` 는 통과하고, 시험은 "CA 를 마운트한다고 적혀
있는가" 만 봤다. 적혀 있었다. 한 번도 붙어 본 적이 없었다(결함 48).

설정을 문자열로 검사하는 것과 붙여 보는 것은 다르다. 이건 후자다.

    python ops/verify_pg_tls.py
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL 이 없다", file=sys.stderr)
        return 2
    # 비밀은 찍지 않는다. sslmode 와 인증서 경로만 보여 준다.
    shown = dsn.split("?", 1)[-1] if "?" in dsn else "(질의 인자 없음)"
    print(f"접속 시도 — {shown}")

    try:
        import psycopg2
    except ImportError:
        print("psycopg2 가 없다 — 이미지에 드라이버가 안 들어갔다", file=sys.stderr)
        return 2

    expect_user = os.environ.get("EXPECT_PG_USER", "mdfeed_runtime")
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user, "
                    "(SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), "
                    "(SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid())")
        user, ssl, version = cur.fetchone()
        print(f"접속 성공 · user={user} · ssl={ssl} · {version}")
        problems = []
        if user != expect_user:
            problems.append(f"접속 역할이 {user} 다 (기대: {expect_user})")
        if not ssl:
            problems.append("TLS 없이 붙었다 — sslmode=verify-full 이 실제로는 안 먹고 있다")

        # 역할이 있다는 것만으로는 부족하다. 권한이 의도대로인지 한 줄 본다.
        cur.execute("SELECT has_table_privilege(current_user, 'trades', 'INSERT'), "
                    "has_schema_privilege(current_user, 'public', 'CREATE')")
        can_insert, can_create = cur.fetchone()
        print(f"권한 · trades INSERT={can_insert} · public CREATE={can_create}")
        if not can_insert:
            problems.append("trades 에 INSERT 를 못 한다 — 수집이 적재를 못 한다")
        if can_create:
            problems.append("public 에 CREATE 를 할 수 있다 — 런타임 역할이 너무 넓다")

    for p in problems:
        print(f"::error::{p}" if "GITHUB_ACTIONS" in os.environ else f"실패: {p}",
              file=sys.stderr)
    if problems:
        return 1
    print("TLS · 역할 · 권한 확인 완료")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
