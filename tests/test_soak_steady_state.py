"""누수 판정이 **시동과 정상 상태를 구분하는가.**

`bench/soak.py` 는 관측 창 하나의 기울기를 헤드라인으로 썼다. 그 숫자는 시동과 정상
상태를 섞는다. 2026-09-29 주간 실행(150분)의 원시 표본을 30분 창으로 끊어 보고 알았다.

    feedd            8.38 → 8.96 → 0.52 → 0.00 → 0.00  MB/h
    mcast-publisher  7.10 → 6.47 → 1.38 → 1.18 → 0.91
    writer           3.88 → 1.81 → 0.77 → 0.47 → 0.44

전부 60분쯤에 정착한다. 그런데 보고서에는 feedd 가 **+3.20MB/h(임계 5의 64%)** 로
적혀 있었다 — 마지막 90분 동안 정확히 0.00 인 서비스다. 앞 5분만 빼는 워밍업 제외로는
부족했다. 정착에 60분이 걸리는데 5분을 뺀 셈이다.

순위도 뒤집혀 있었다. feedd(3.20)가 mcast-publisher(2.78)보다 나쁘게 보이는데, 끝까지
오르고 있는 쪽은 mcast-publisher 다. **판정에 쓰는 값이 서비스 서열을 반대로 매겼다.**

이 시험은 두 축으로 본다.

1. 합성 시계열로 "시동 후 평탄" 과 "끝까지 샌다" 가 **다르게 판정되는지**
2. 실제 기록(`tests/data/soak-linux-20260929-trimmed.json`)으로 그때의 수치가
   재현되는지. 하네스가 원시 표본을 남기기 때문에 과거 실행을 다시 판정할 수 있다 —
   그게 결함 39 에서 넣은 장치이고, 여기서 처음 값을 한다.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).parent / "data" / "soak-linux-20260929-trimmed.json"


def _soak():
    spec = importlib.util.spec_from_file_location("soak", ROOT / "bench" / "soak.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _rows(samples: list[tuple[float, float]]) -> list:
    """(분, RSS MB) → 하네스가 쓰는 행 모양 (t초, rss, fd, frames)."""
    return [(m * 60.0, rss, 20, int(m * 60 * 18)) for m, rss in samples]


def _slopes(mod, rows):
    """하네스와 같은 방식으로 전체·정상상태 기울기를 낸다."""
    t0 = rows[0][0]
    judged = [r for r in rows if (r[0] - t0) / 60.0 >= mod.WARMUP_MINUTES] or rows
    jxs = [(r[0] - judged[0][0]) / 3600.0 for r in judged]
    full = mod.slope(jxs, [r[1] for r in judged])
    tail = judged[-max(2, int(len(judged) * mod.STEADY_FRACTION)):]
    sxs = [(r[0] - tail[0][0]) / 3600.0 for r in tail]
    steady = mod.slope(sxs, [r[1] for r in tail])
    minutes = (tail[-1][0] - tail[0][0]) / 60.0
    return full, steady, minutes


# ── 1. 합성: 두 모양이 구분되는가 ────────────────────────────────────────
def test_warmup_then_flat_is_not_a_leak():
    """캐시를 채우고 평탄해지는 것은 누수가 아니다. 전체 기울기로는 누수로 읽힌다."""
    mod = _soak()
    # 0~60분에 30MB 오르고 그 뒤 150분까지 완전히 평탄
    samples = [(m, 30.0 + 0.5 * m) for m in range(0, 61, 3)]
    samples += [(m, 60.0) for m in range(63, 151, 3)]
    full, steady, minutes = _slopes(mod, _rows(samples))

    assert full > mod.RSS_GROWTH_LIMIT_MB_H, (
        f"전체 기울기가 {full:.2f}MB/h — 이 모양이 예전 판정에서 누수로 읽혔다는 전제가 깨졌다")
    assert abs(steady) < 0.1, f"정상 상태는 평탄해야 한다: {steady:.2f}MB/h"
    assert minutes >= mod.MIN_MINUTES_FOR_STEADY


def test_steady_leak_is_still_caught():
    """시동이 없어도 끝까지 새면 잡아야 한다. 정상 상태로 옮긴 판정이 느슨해지지 않았는가."""
    mod = _soak()
    samples = [(m, 30.0 + 0.15 * m) for m in range(0, 151, 3)]   # 9MB/h 로 계속
    full, steady, minutes = _slopes(mod, _rows(samples))
    assert steady > mod.RSS_GROWTH_LIMIT_MB_H, f"정상 상태 누수를 놓쳤다: {steady:.2f}MB/h"
    delta = 30.0 + 0.15 * 150 - (30.0 + 0.15 * (150 - minutes))
    assert delta >= mod.RSS_STEADY_ABSOLUTE_MIN_MB, (
        f"절대 하한 {mod.RSS_STEADY_ABSOLUTE_MIN_MB}MB 가 너무 높아 이 누수가 걸러진다 (+{delta:.1f}MB)")


def test_flat_then_leaking_is_caught_where_the_old_metric_diluted_it():
    """앞이 평탄하고 뒤에서 새기 시작하면, 전체 기울기는 희석된다.

    예전 판정은 창 하나의 기울기였으므로 뒤쪽 누수를 앞쪽 평탄 구간이 반으로 깎았다.
    정상 상태 창은 그 희석을 받지 않는다.
    """
    mod = _soak()
    samples = [(m, 30.0) for m in range(0, 101, 3)]                      # 100분 평탄
    samples += [(m, 30.0 + 0.12 * (m - 100)) for m in range(103, 151, 3)]  # 7.2MB/h
    full, steady, _ = _slopes(mod, _rows(samples))
    assert steady > mod.RSS_GROWTH_LIMIT_MB_H, f"뒤쪽 누수를 놓쳤다: {steady:.2f}MB/h"
    assert full < steady, (
        f"전체 기울기({full:.2f})가 정상 상태({steady:.2f})보다 작아야 한다 — 희석의 증거")


def test_short_steady_window_withholds_judgement():
    """정상 상태 창이 짧으면 판정하지 않고 그렇게 적는다 (결함 41 과 같은 태도)."""
    mod = _soak()
    samples = [(m, 30.0 + 0.1 * m) for m in range(0, 21, 1)]   # 20분
    _, _, minutes = _slopes(mod, _rows(samples))
    assert minutes < mod.MIN_MINUTES_FOR_STEADY, (
        f"20분 관측의 마지막 1/3 은 {minutes:.1f}분 — 판정 유보 대상이어야 한다")


# ── 2. 실제 기록으로 재현 ────────────────────────────────────────────────
def test_recorded_run_reproduces_the_misleading_headline():
    """**결함 49 의 근거.** 그때 보고된 숫자와 정상 상태가 얼마나 다른지 고정한다."""
    mod = _soak()
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    reported = data["reported_at_the_time"]
    by_name = {s["service"]: s for s in data["services"]}

    feedd = _rows([(r["t"] / 60.0, r["rss_mb"]) for r in by_name["feedd"]["rows"]])
    full, steady, minutes = _slopes(mod, feedd)

    assert reported["feedd"] > 3.0, "그때 보고된 feedd 기울기가 3MB/h 를 넘었다는 기록"
    assert abs(steady) < 0.2, (
        f"feedd 는 마지막 {minutes:.0f}분 동안 평탄했다. 그런데 보고서는 "
        f"+{reported['feedd']}MB/h 였다. 정상 상태 측정값: {steady:.2f}MB/h")
    assert full > steady + 2.0, "전체 기울기가 정상 상태를 크게 과장했다는 사실을 고정한다"


def test_recorded_run_ranking_was_inverted():
    """보고된 숫자는 서비스 서열을 반대로 매겼다."""
    mod = _soak()
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    reported = data["reported_at_the_time"]
    steady = {}
    for s in data["services"]:
        rows = _rows([(r["t"] / 60.0, r["rss_mb"]) for r in s["rows"]])
        steady[s["service"]] = _slopes(mod, rows)[1]

    assert reported["feedd"] > reported["mcast-publisher"], (
        "보고서에서는 feedd 가 더 나쁘게 보였다")
    assert steady["mcast-publisher"] > steady["feedd"], (
        f"정상 상태에서는 mcast-publisher 가 더 나쁘다 "
        f"(feedd {steady['feedd']:.2f} · mcast {steady['mcast-publisher']:.2f}) — "
        "즉 보고된 값이 서열을 뒤집고 있었다")


def test_every_recorded_service_still_passes_on_steady_state():
    """고친 판정으로 과거 실행을 다시 봐도 통과해야 한다. 아니면 둘 중 하나가 틀렸다."""
    mod = _soak()
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for s in data["services"]:
        rows = _rows([(r["t"] / 60.0, r["rss_mb"]) for r in s["rows"]])
        _, st, minutes = _slopes(mod, rows)
        assert st <= mod.RSS_GROWTH_LIMIT_MB_H, (
            f"{s['service']} 가 정상 상태 +{st:.2f}MB/h — 새 판정으로는 실패한다. "
            "과거 실행이 실제로 샜거나, 임계가 틀렸다")


def test_fixture_keeps_its_provenance():
    """출처 없는 측정값은 근거가 아니다."""
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert data.get("source_run_id"), "어느 실행에서 나온 값인지 적혀 있어야 한다"
    assert "150" in str(data.get("duration_minutes")) or data["duration_minutes"] >= 140
    assert data["_note"].strip()


# ── 3. 상수들이 서로 어긋나지 않는가 ────────────────────────────────────
def test_reachability_math_matches_the_constants():
    """판정 창과 노이즈 하한은 **함께** 바뀌어야 한다.

    창을 좁히면서 하한을 그대로 두면 짧은 창에서 하한을 못 넘어 판정이 영원히 유보된다
    (결함 41 이 그것이었다). 반대로 하한만 낮추면 노이즈가 누수로 읽힌다.
    그래서 "이 설정으로 임계짜리 누수를 잡으려면 몇 분이 필요한가" 를 계산해 고정한다.
    """
    mod = _soak()
    need_steady = max(
        mod.RSS_STEADY_ABSOLUTE_MIN_MB / mod.RSS_GROWTH_LIMIT_MB_H * 60.0,
        mod.MIN_MINUTES_FOR_STEADY)
    need_total = need_steady / mod.STEADY_FRACTION + mod.WARMUP_MINUTES
    assert need_total < 120, (
        f"이 설정으로는 전체 {need_total:.0f}분이 필요하다. 예전(125분)보다 나빠졌다 — "
        "창을 좁혔으면 하한도 함께 내려야 한다")
    assert need_steady >= mod.MIN_MINUTES_FOR_STEADY, (
        "노이즈 하한이 요구하는 창이 최소 창보다 짧다 — 둘 중 하나가 무의미하다")


def test_weekly_run_is_long_enough_to_judge():
    """**주간 실행이 판정 가능한 길이인가.**

    상수를 고치면서 워크플로의 `--minutes` 를 안 보면, 매주 도는 실행이 조용히
    "판정 보류" 로 바뀐다. 초록불은 그대로다 — 이 저장소가 반복해서 만난 모양이다.
    """
    mod = _soak()
    workflow = (ROOT / ".github" / "workflows" / "soak.yml").read_text(encoding="utf-8")
    import re
    m = re.search(r"--minutes \"\$\{\{ github\.event\.inputs\.minutes \|\| '(\d+)' \}\}\"",
                  workflow)
    assert m, "soak.yml 에서 기본 관측 시간을 못 읽었다 — 정규식이 낡았다"
    minutes = float(m.group(1))

    steady = (minutes - mod.WARMUP_MINUTES) * mod.STEADY_FRACTION
    need = max(mod.RSS_STEADY_ABSOLUTE_MIN_MB / mod.RSS_GROWTH_LIMIT_MB_H * 60.0,
               mod.MIN_MINUTES_FOR_STEADY)
    assert steady >= need, (
        f"주간 실행 {minutes:.0f}분의 판정 창은 {steady:.0f}분인데 "
        f"{need:.0f}분이 필요하다 — 매주 '판정 보류' 로 돌고 있다")
    assert minutes >= mod.MIN_MINUTES_FOR_SLOPE
