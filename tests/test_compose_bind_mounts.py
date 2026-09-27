"""compose 가 읽는 호스트 경로가 **실제로 있는가.**

`docker compose config -q` 는 문법만 본다. 없는 경로를 바인드 마운트해도 통과한다.
그런데 도커는 없는 경로를 만나면 그 자리에 **디렉터리를 만든다.** 비밀번호 파일을
기대하는 프로세스는 디렉터리를 읽고, 스택은 안 뜬다.

실제로 그랬다(결함 48). compose 가 비밀 파일 4개를 읽는데 저장소에 하나도 없고,
만드는 절차도 없고, `.gitignore` 에도 없었다(즉 누가 만들면 커밋됐다). CI 는
`docker compose config -q` 로 초록불이었고, `tests/test_ops_deployment.py` 는
**"CA 를 마운트한다고 적혀 있는가"** 만 봤다. 적혀 있었다. 파일이 없었다.

이 시험은 도커가 필요 없다. 경로 목록과 파일 시스템만 본다.

  1. compose 가 읽는 호스트 경로는 저장소에 있거나, 생성기가 만든다고 선언한 것이어야 한다
  2. 생성기가 만든다고 한 경로는 `.gitignore` 에 있어야 한다 (비밀이 커밋되는 길을 막는다)
  3. 생성기 목록에 있는데 아무 compose 도 안 읽으면 그것도 어긋남이다
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker-compose.yml", "docker-compose.observability.yml",
           "docker-compose.server.yml"]
GENERATOR = ROOT / "ops" / "gen_dev_secrets.sh"


def _bind_sources() -> dict[str, list[str]]:
    """compose 가 읽는 호스트 상대경로 → 그 경로를 읽는 파일들."""
    out: dict[str, list[str]] = {}
    for name in COMPOSE:
        path = ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            m = re.match(r"-\s+(\./[^:\s]+):", stripped)
            if m:
                out.setdefault(m.group(1)[2:], []).append(name)
    return out


def _generated() -> set[str]:
    """생성기가 만든다고 선언한 경로. 목록이 곧 범위다."""
    result = subprocess.run(["bash", str(GENERATOR), "--list"],
                            capture_output=True, text=True, cwd=ROOT, check=True)
    return {ln.strip() for ln in result.stdout.splitlines() if ln.strip()}


def test_generator_declares_its_paths():
    assert GENERATOR.exists(), "비밀 파일을 만드는 절차가 저장소에 있어야 한다"
    paths = _generated()
    assert paths, "--list 가 아무것도 내지 않는다 — 목록 파싱이 깨졌다"
    for rel in paths:
        assert rel.startswith("ops/secrets/"), f"비밀은 ops/secrets/ 안에 둔다: {rel}"


def test_every_compose_bind_source_exists_or_is_generated():
    generated = _generated()
    missing: list[str] = []
    for rel, files in sorted(_bind_sources().items()):
        if (ROOT / rel).exists() or rel in generated:
            continue
        missing.append(f"{rel} ← {', '.join(files)}")
    assert not missing, (
        "compose 가 읽는데 저장소에도 없고 생성기도 안 만든다. 도커는 그 자리에 "
        "디렉터리를 만들고 스택은 안 뜬다:\n  " + "\n  ".join(missing))


def test_generated_secrets_are_gitignored():
    """생성기가 만드는 파일이 커밋되면 자격증명이 저장소에 들어간다."""
    leaky: list[str] = []
    for rel in sorted(_generated()):
        probe = subprocess.run(["git", "check-ignore", "-q", rel],
                               cwd=ROOT, capture_output=True)
        if probe.returncode != 0:
            leaky.append(rel)
    assert not leaky, f".gitignore 에 없다 — 만들면 커밋된다: {leaky}"


def test_generated_secrets_are_not_already_tracked():
    """과거에 실수로 커밋된 게 남아 있지 않은가. gitignore 는 이미 추적 중인 건 못 막는다."""
    tracked = subprocess.run(["git", "ls-files"] + sorted(_generated()),
                             cwd=ROOT, capture_output=True, text=True)
    assert not tracked.stdout.strip(), (
        f"비밀 파일이 git 에 추적되고 있다: {tracked.stdout.split()}")


def test_generator_makes_nothing_nobody_reads():
    """읽는 곳이 없는 비밀을 만들면, 지우지도 못하고 왜 있는지도 모르게 된다."""
    read = set(_bind_sources())
    orphan = sorted(_generated() - read)
    assert not orphan, (
        f"생성기가 만드는데 어느 compose 도 읽지 않는다: {orphan}")


def test_postgres_tls_chain_is_complete():
    """`sslmode=verify-full` 은 셋이 다 있어야 성립한다.

    예전에는 DSN 만 `verify-full` 이고 서버 쪽에 TLS 설정이 **아예 없었다.** 그러면
    클라이언트가 TLS 를 요구하는데 서버가 제공하지 않아 접속이 전부 떨어진다.
    """
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "sslmode=verify-full" in text
    # 서버가 TLS 를 켜고, 인증서·키·CA 를 지정하는가
    for need in ("ssl=on", "ssl_cert_file=", "ssl_key_file=", "ssl_ca_file="):
        assert need in text, f"DSN 은 verify-full 인데 서버 설정에 {need} 가 없다"
    # 역할이 있어야 붙는다. bootstrap_roles.sql 이 initdb 경로에 들어가는가
    assert "/docker-entrypoint-initdb.d/bootstrap_roles.sql" in text, (
        "bootstrap_roles.sql 이 initdb 에 없으면 mdfeed_runtime 이 만들어지지 않는다")
    assert (ROOT / "ops" / "bootstrap_roles.sql").exists()


def test_postgres_healthcheck_proves_more_than_the_port():
    """`pg_isready` 만으로는 뒤따르는 서비스가 붙을 수 있는지 모른다.

    역할이 없으면 포트는 열려 있고 접속은 떨어진다. 그러면 depends_on 이 healthy 를
    보고 서비스를 띄우고, 그 서비스가 접속 실패로 죽는다 — 원인이 postgres 인데
    빨간 줄은 다른 데서 뜬다.
    """
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    block = text[text.index("  postgres:"):]
    block = block[:block.index("\n  feedd:")]
    assert "pg_roles" in block and "mdfeed_runtime" in block, (
        "postgres healthcheck 가 역할 존재까지 확인해야 한다")
