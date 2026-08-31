"""운영 기록의 환산이 맞는가 — 틀린 환산은 없는 경보를 만든다.

실측(2026-08-31): 게이트웨이 하나만 4분 전에 재기동했는데, 스택 전체의
**최소** 가동시간으로 나누는 바람에 멀쩡한 수집기가 전부 폭주로 찍혔다.

    upbit  재접속 7   →  실제 2.6회/시간   보고 70회/시간
    kis_rest 재접속 11 → 실제 4.1회/시간   보고 110회/시간

임계는 10회/시간이다. 없는 사건으로 경보를 내면, 진짜일 때 아무도 안 본다.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import ops_report                                    # noqa: E402


def _up(venue: str, reconnects: int, shard_uptime_h: float) -> dict:
    return {"venue": venue, "messages": 1000, "reconnects": reconnects,
            "errors": reconnects, "last_msg_age_s": 0.0, "stale": False,
            "expects_data": True, "shard_uptime_h": shard_uptime_h}


def _report(ups: list[dict]) -> dict:
    return {
        "captured_at": "2026-08-31T12:00:00+09:00", "date": "2026-08-31",
        "services_up": 8, "services_expected": 8, "unhealthy": [],
        "uptime_h": 0.07,                            # 방금 재기동한 서비스가 있다
        "throughput": {"feedd_seq": 1, "rows_written": 1, "bars_written": 1,
                       "signals": 0},
        "integrity": {"gap_count": 0, "lost_messages": 0, "duplicate_count": 0,
                      "bus_dropped": 0},
        "quality": {"checked": 1, "critical": 0, "critical_rate_pct": 0.0},
        "upstreams": ups, "inactive": [],
    }


def test_시간당_환산은_그_어댑터의_프로세스_가동시간을_쓴다():
    """게이트웨이를 재기동했다고 수집기가 폭주로 찍히면 안 된다."""
    body = ops_report.render(_report([_up("upbit", 7, 2.68)]))
    assert "재접속이 잦은 경로" not in body, (
        "2.6회/시간인데 폭주로 판정했다 — 스택 최소 가동시간으로 나누고 있다")


def test_진짜_폭주는_그대로_잡는다():
    body = ops_report.render(_report([_up("kis", 100, 2.0)]))   # 50회/시간
    assert "재접속이 잦은 경로" in body
    assert "50.0회" in body


def test_가동_10분_미만이면_판정을_미룬다():
    """근거가 얇을 때 경보를 내는 건 근거 없이 내는 것과 같다."""
    body = ops_report.render(_report([_up("upbit", 3, 0.05)]))  # 60회/시간이지만 3분
    assert "재접속이 잦은 경로" not in body


def test_임계_바로_위아래를_가른다():
    assert "재접속이 잦은 경로" not in ops_report.render(
        _report([_up("a", 10, 1.0)]))                # 정확히 10회/시간
    assert "재접속이 잦은 경로" in ops_report.render(
        _report([_up("a", 11, 1.0)]))                # 11회/시간
