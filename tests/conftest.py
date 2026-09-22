import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "quant"))

# 테스트는 사용자 환경변수의 영향을 받으면 안 된다
for k in list(os.environ):
    if k.startswith("MDFEED_"):
        del os.environ[k]


PG_RUNTIME_EVIDENCE = ROOT / ".omo" / "evidence" / "pg-test-runtime.json"


def pg_test_runtime() -> dict:
    """PostgreSQL 시험 런타임을 찾아 서버가 실제로 살아 있을 때만 돌려준다.

    출처는 두 가지다. 우선순위 순으로:
    1. ``TEST_POSTGRES_DSN`` 환경변수 — CI 의 postgres 서비스 컨테이너용.
       예전 CI 는 이 변수를 넘기기만 하고 읽는 테스트가 없어서, 증거 파일이 없는
       러너에서는 전부 스킵되고 초록불만 떴다 (2026-09-21 확인).
    2. ``.omo/evidence/pg-test-runtime.json`` — 로컬 일회용 클러스터의 증거 파일.
       클러스터를 내린 뒤에도 파일은 남으므로, 파일만 믿고 진행하면 픽스처
       setup 에서 연결 오류 25건이 난다 (2026-09-21 실측).

    어느 쪽이든 호스트:포트에 TCP 로 닿는지 확인하고 아니면 스킵한다.
    """
    import json
    import socket
    from urllib.parse import urlsplit

    import pytest

    env_dsn = os.environ.get("TEST_POSTGRES_DSN", "").strip()
    if env_dsn:
        parts = urlsplit(env_dsn)
        data = {
            "source": "TEST_POSTGRES_DSN",
            "dsn": env_dsn,
            "host": parts.hostname or "127.0.0.1",
            "port": parts.port or 5432,
            "binaries": {},
        }
    elif PG_RUNTIME_EVIDENCE.exists():
        data = json.loads(PG_RUNTIME_EVIDENCE.read_text(encoding="utf-8"))
        data.setdefault("source", str(PG_RUNTIME_EVIDENCE))
        data.setdefault("binaries", {})
    else:
        pytest.skip("PostgreSQL test runtime: TEST_POSTGRES_DSN unset and evidence file missing")
    host = str(data.get("host") or "127.0.0.1")
    port = int(data.get("port") or 5432)
    try:
        with socket.create_connection((host, port), timeout=1.0):
            pass
    except OSError as exc:
        pytest.skip(f"PostgreSQL test runtime at {host}:{port} is not reachable ({exc.__class__.__name__})")
    return data
