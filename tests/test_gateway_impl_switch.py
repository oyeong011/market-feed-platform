"""MDFEED_GATEWAY_IMPL 로 배포 게이트웨이를 C++ 구현으로 바꿔 끼울 수 있는가.

이 스위치의 핵심은 **조용히 폴백하지 않는 것**이다. C++ 로 띄우라고 했는데 바이너리가 없으면
파이썬으로 돌아가는 대신 실패해야 한다. 안 그러면 "C++ 로 배포 중"이라고 믿는 채로 파이썬이 도는
상태가 된다 — 이 저장소가 반복해서 겪은 유형(선언과 실제가 다른 것)이다.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from mdfeed.cli import CPP_IMPLS, SERVICES, active_services, service_command

ROOT = Path(__file__).resolve().parents[1]


def test_default_is_python_module(monkeypatch):
    monkeypatch.delenv("MDFEED_GATEWAY_IMPL", raising=False)
    assert service_command("tcp-gateway", "mdfeed.services.tcp_gateway") == [sys.executable, "-m", "mdfeed.services.tcp_gateway"]


def test_cpp_selects_the_binary(monkeypatch):
    monkeypatch.setenv("MDFEED_GATEWAY_IMPL", "cpp")
    binary = ROOT / CPP_IMPLS["tcp-gateway"]
    if not binary.exists():
        pytest.skip("C++ 바이너리가 없다 (make cpp)")
    assert service_command("tcp-gateway", "mdfeed.services.tcp_gateway") == [str(binary)]


def test_cpp_does_not_silently_fall_back(monkeypatch, tmp_path):
    """바이너리가 없으면 파이썬으로 돌아가지 말고 실패해야 한다."""
    monkeypatch.setenv("MDFEED_GATEWAY_IMPL", "cpp")
    monkeypatch.setitem(CPP_IMPLS, "tcp-gateway", "cpp/build/does-not-exist")
    with pytest.raises(SystemExit) as e:
        service_command("tcp-gateway", "mdfeed.services.tcp_gateway")
    assert "make cpp" in str(e.value)


def test_unknown_impl_is_rejected(monkeypatch):
    monkeypatch.setenv("MDFEED_GATEWAY_IMPL", "rust")
    with pytest.raises(SystemExit):
        service_command("tcp-gateway", "mdfeed.services.tcp_gateway")


def test_other_services_are_unaffected(monkeypatch):
    """스위치는 게이트웨이에만 적용된다. 수집·적재·전략은 파이썬 그대로다."""
    monkeypatch.setenv("MDFEED_GATEWAY_IMPL", "cpp")
    for name, module in (("feedd", "mdfeed.services.feedd"), ("writer", "mdfeed.services.writer")):
        assert service_command(name, module) == [sys.executable, "-m", module]


def test_ops_script_finds_the_cpp_process_pattern():
    """상태판이 C++ 게이트웨이를 못 찾으면 '떠 있는데 안 보인다' 가 된다."""
    out = subprocess.run(["bash", "-c", f'cd {ROOT} && MDFEED_GATEWAY_IMPL=cpp bash -c \'source ops/ops.sh >/dev/null 2>&1 || true; module_of tcp-gateway\''],
                         capture_output=True, text=True)
    # ops.sh 는 source 시 부작용이 있을 수 있어 함수만 추출해 평가한다
    script = (ROOT / "ops/ops.sh").read_text()
    start = script.index("module_of() {")
    end = script.index("\n}\n", start) + 3
    fn = script[start:end]
    for impl, want in (("cpp", "cpp/build/tcp_gateway"), ("python", "mdfeed.services.tcp_gateway")):
        r = subprocess.run(["bash", "-c", f"{fn}\nMDFEED_GATEWAY_IMPL={impl} module_of tcp-gateway"], capture_output=True, text=True)
        assert r.stdout.strip() == want, (impl, r.stdout, r.stderr, out.stdout)


def test_mcast_publisher_is_opt_in(monkeypatch):
    """멀티캐스트는 망 설정이 맞아야 돈다. 안 맞으면 조용히 아무도 못 받으므로 켜는 건 명시적 결정이어야 한다."""
    monkeypatch.delenv("MDFEED_MCAST_ENABLED", raising=False)
    assert [s[0] for s in active_services()] == [s[0] for s in SERVICES]
    monkeypatch.setenv("MDFEED_MCAST_ENABLED", "1")
    assert "mcast-publisher" in [s[0] for s in active_services()]


def test_mcast_publisher_always_uses_the_binary(monkeypatch):
    """구현이 C++ 하나뿐이다. MDFEED_GATEWAY_IMPL 과 무관하게 바이너리로 뜬다."""
    binary = ROOT / CPP_IMPLS["mcast-publisher"]
    if not binary.exists():
        pytest.skip("C++ 바이너리가 없다 (make cpp)")
    for impl in ("python", "cpp"):
        monkeypatch.setenv("MDFEED_GATEWAY_IMPL", impl)
        assert service_command("mcast-publisher", None) == [str(binary)]


def test_healthcheck_counts_mcast_only_when_enabled():
    """안 켠 구성에서 '응답 없음' WARN 을 내면 사람이 알람을 무시하게 된다."""
    import subprocess
    code = ("import os,sys; sys.path.insert(0,'src'); "
            "import importlib.util as u; "
            "spec=u.spec_from_file_location('hc','ops/healthcheck.py'); m=u.module_from_spec(spec); spec.loader.exec_module(m); "
            "print([s[0] for s in m.SERVICES])")
    for env_val, expect in (("", False), ("1", True)):
        e = {**os.environ, "MDFEED_MCAST_ENABLED": env_val}
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT, env=e).stdout
        assert ("mcast-publisher" in out) is expect, (env_val, out)
