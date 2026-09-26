"""안전망들이 **같은 서비스 목록**을 보고 있는가.

이 저장소에는 서비스를 대상으로 하는 안전망이 여럿이다.
감독기(cli.SERVICES) · 상태판(ops.sh) · 헬스체크(ops/healthcheck.py) · 장시간 감시(bench/soak.py) ·
지표 수집(prometheus.yml) · 장애 주입(ops/chaos.sh) · **운영 배포(systemd 유닛·타깃)**.

**각 안전망은 자기 목록에 있는 것만 본다. 목록이 곧 범위다.**
새 서비스를 붙이면서 목록 하나를 빠뜨리면 그 서비스만 조용히 밖에 남는다. 2026-09-24~26 한 주에
같은 유형이 세 번 나왔다.

  결함 38  C++ 서비스가 자원 누수 감시 밖 (resources 를 안 내서 0MB/0fd 로 읽힘)
  결함 42  장애 주입이 C++ 서비스를 프로세스로 못 찾아 건드리지도 못함
  결함 43  quality 서비스에 systemd 유닛이 아예 없었다 — 개발에서는 돌고 운영에서는 안 돌았다
  그리고 알람 쪽도 같은 방향으로 한 번 (새 지표를 보는 알람이 하나도 없었음)

개별로 고치면 네 번째가 온다. 목록이 어긋나면 여기서 실패하게 한다.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# 서비스 → 관리 포트. 이 표가 기준이다. 새 서비스를 붙이면 여기부터 고치게 된다.
EXPECTED = {
    "feedd": 9100,
    "tcp-gateway": 9111,
    "ws-gateway": 9102,
    "rest-api": 9103,
    "writer": 9104,
    "strategy": 9105,
    "quality": 9106,
}
# 선택 서비스: 켠 구성에서만 대상이 된다. 안 켠 곳에서 빨간 줄을 내면 안 되므로
# "있어야 한다"가 아니라 "켜면 모든 안전망이 함께 본다"를 요구한다.
OPTIONAL = {"mcast-publisher": 9132}


def _text(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _block(text: str, start: str, chars: int = 900) -> str:
    i = text.index(start)
    return text[i:i + chars]


def test_supervisor_and_healthcheck_agree():
    """감독기가 띄우는 것과 헬스체크가 세는 것이 같아야 한다."""
    cli = _block(_text("src/mdfeed/cli.py"), "SERVICES = [")
    hc = _block(_text("ops/healthcheck.py"), "SERVICES = [")
    for name, port in EXPECTED.items():
        assert f'"{name}"' in cli, f"감독기 목록에 {name} 없음"
        assert f'"{name}"' in hc, f"헬스체크 목록에 {name} 없음"
        assert str(port) in cli and str(port) in hc, f"{name} 관리 포트 {port} 불일치"


def test_soak_watches_every_service():
    """장시간 감시가 한 서비스라도 빠뜨리면 그 서비스의 누수는 영원히 안 보인다(결함 38)."""
    soak = _block(_text("bench/soak.py"), "SERVICES = [", 1200)
    for name, port in {**EXPECTED, **OPTIONAL}.items():
        assert f'"{name}"' in soak, f"soak 목록에 {name} 없음 — 누수 감시 밖이다"
        assert str(port) in soak, f"{name} 포트 {port} 가 soak 목록과 다르다"


def test_prometheus_scrapes_every_service():
    prom = _text("ops/observability/prometheus.yml")
    for name, port in {**EXPECTED, **OPTIONAL}.items():
        assert str(port) in prom, f"prometheus 타깃에 {name}(:{port}) 없음"


def test_ops_status_knows_every_service():
    ops = _text("ops/ops.sh")
    admin = _block(ops, "admin_port() {", 600)
    for name, port in {**EXPECTED, **OPTIONAL}.items():
        assert name in admin and str(port) in admin, f"ops.sh 상태판이 {name} 를 모른다"


def test_chaos_can_actually_find_every_process():
    """장애 주입이 프로세스를 못 찾으면 그 서비스의 복구 경로는 확인된 적이 없다(결함 42).

    구현이 바뀌면 명령줄도 바뀐다. 파이썬 모듈 경로로만 찾으면 C++ 서비스를 못 흔든다.
    """
    chaos = _text("ops/chaos.sh")
    assert "proc_pattern()" in chaos, "구현별 프로세스 패턴이 없다"
    pattern_block = _block(chaos, "proc_pattern() {", 700)
    assert "cpp/build/tcp_gateway" in pattern_block, "C++ 게이트웨이를 못 찾는다"
    assert "cpp/build/mcast_publisher" in pattern_block, "멀티캐스트 발행자를 못 찾는다"
    restart = _block(chaos, "chaos_restart_all() {", 800)
    for name in ("tcp_gateway", "ws_gateway", "rest_api", "writer", "strategy", "quality"):
        assert name in restart, f"재기동 시나리오에 {name} 없음"
    assert "mcast_publisher" in restart, "멀티캐스트 발행자가 재기동 시나리오에 없다"


def test_optional_service_is_opt_in_everywhere():
    """선택 서비스는 **모든 안전망에서 같은 방식으로** 선택적이어야 한다.

    한 곳에서만 필수면 안 켠 구성이 계속 빨갛고, 한 곳에서만 빠지면 켠 구성이 감시 밖이 된다.
    """
    flag = "MDFEED_MCAST_ENABLED"
    for rel in ("src/mdfeed/cli.py", "ops/healthcheck.py", "ops/ops.sh"):
        assert flag in _text(rel), f"{rel} 가 선택 서비스 플래그를 모른다"
    # soak·prometheus·chaos 는 "떠 있으면 본다" 방식이라 플래그가 필요 없다.
    # 대신 대상 목록에는 반드시 들어 있어야 한다(위 시험들이 본다).


def test_every_cpp_service_reports_resources():
    """자원을 안 내는 서비스는 감시 목록에 있어도 감시 밖이다(결함 38의 본질)."""
    for rel in ("cpp/src/tcp_gateway.cpp", "cpp/src/mcast_publisher.cpp"):
        src = _text(rel)
        assert "proc_stat_json()" in src, f"{rel} 가 resources 를 안 낸다"
        assert "process_rss_bytes" in src and "process_fd_open" in src, f"{rel} 가 자원 지표를 안 낸다"


def test_every_service_has_a_systemd_unit():
    """운영 배포도 안전망이다. 유닛이 없으면 그 서비스는 systemd 에서 **안 돈다**(결함 43).

    quality 가 그랬다. 감독기(`make up`)는 띄우는데 유닛이 없어 systemd 배포에서는 안 떴다.
    그 서비스가 내는 지표에는 알람이 걸려 있으니, 운영에서 그 알람은 영원히 안 울린다.
    """
    units = {p.stem.replace("mdfeed-", "") for p in (ROOT / "ops/systemd").glob("mdfeed-*.service")}
    for name in EXPECTED:
        assert name in units, f"{name} 에 systemd 유닛이 없다 — 운영 배포에서 안 돈다"
    for name in OPTIONAL:
        assert name in units, f"선택 서비스 {name} 도 유닛은 있어야 한다 (켜는 건 따로)"


def test_target_pulls_in_every_required_service():
    """유닛이 있어도 타깃이 안 당기면 `systemctl start mdfeed.target` 으로는 안 뜬다."""
    target = _text("ops/systemd/mdfeed.target")
    for name in EXPECTED:
        assert f"mdfeed-{name}.service" in target, f"타깃이 {name} 를 안 당긴다"
    # 선택 서비스는 일부러 뺀다. 그 사실이 주석으로 적혀 있어야 다음 사람이 실수로 넣지 않는다.
    for name in OPTIONAL:
        assert f"mdfeed-{name}.service" not in re.sub(r"#.*", "", target), \
            f"선택 서비스 {name} 가 타깃에 들어 있다 — 안 켠 구성에서 실패한다"


def test_install_enables_every_required_service():
    """유닛과 타깃이 맞아도 enable 목록에서 빠지면 재부팅 후 안 뜬다."""
    ops = _text("ops/ops.sh")
    enable = _block(ops, "systemctl enable mdfeed.target", 400)
    for name in EXPECTED:
        assert f"mdfeed-{name}" in enable, f"install 이 {name} 를 enable 하지 않는다"
