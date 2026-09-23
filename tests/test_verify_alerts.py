"""알람 검사기가 양방향 사각을 다 보는가.

이 저장소는 "알람은 있는데 지표가 없다"(결함 23)를 한 번 겪었다. 그 반대인
"지표는 내는데 아무 알람도 안 본다"도 같은 종류의 사각이다. 그리고 검사기 자신이
여러 줄로 쓴 식의 둘째 줄을 안 읽고 있었다 — 검사기가 눈을 감으면 아무것도 못 잡는다.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/verify_alerts.py"


def run(rules: Path) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(SCRIPT), "--rules", str(rules)],
                          capture_output=True, text=True, cwd=ROOT)


def test_repository_rules_pass_both_directions():
    r = run(ROOT / "ops/observability/alerts.yml")
    assert r.returncode == 0, r.stdout[-2000:]
    assert "감시 사각이 없습니다" in r.stdout


def test_metric_on_a_continuation_line_is_checked(tmp_path):
    """여러 줄 식의 둘째 줄에만 있는 지표도 참조로 세야 한다. 안 그러면 없어져도 안 잡힌다."""
    rules = tmp_path / "a.yml"
    rules.write_text((ROOT / "ops/observability/alerts.yml").read_text(encoding="utf-8")
                     .replace("or increase(mdfeed_mcast_injected_duplicates_total[5m]) > 0",
                              "or increase(mdfeed_metric_that_does_not_exist[5m]) > 0", 1),
                     encoding="utf-8")
    r = run(rules)
    assert r.returncode == 1
    assert "mdfeed_metric_that_does_not_exist" in r.stdout
    assert "영원히 안 울린다" in r.stdout


def test_exposed_metric_with_no_alert_fails(tmp_path):
    """지표를 새로 내면서 '볼지 말지' 결정을 건너뛰지 못하게 한다."""
    rules = tmp_path / "b.yml"
    rules.write_text(
        "groups:\n  - name: tiny\n    rules:\n"
        "      - alert: OnlyOne\n        expr: mdfeed_upstream_stale > 0\n"
        "        labels: { severity: warning }\n", encoding="utf-8")
    r = run(rules)
    assert r.returncode == 1
    assert "무관심" in r.stdout and "NO_ALERT_BY_DESIGN" in r.stdout


def test_by_design_metrics_are_not_reported_as_unwatched():
    """대시보드·진단용 지표까지 알람을 강요하면 목록이 소음이 된다."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("va", SCRIPT)
    va = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(va)
    assert "mdfeed_uptime_seconds" in va.NO_ALERT_BY_DESIGN
    # 설계상 무알람 목록과 조건부 목록이 겹치면 판정이 모호해진다
    assert not (va.NO_ALERT_BY_DESIGN & set(va.CONDITIONAL))
    assert not (va.NO_ALERT_BY_DESIGN & va.MCAST_METRICS)
