"""품질 검사를 **실제 시장 데이터**로 회귀 검증한다.

두 방향을 같이 본다.

1. **오탐** — 정상 데이터에 대고 얼마나 울리는가
2. **미탐** — 진짜 결함을 넣으면 잡는가

한쪽만 보면 안 된다. 오탐만 줄이면 아무 데도 안 울리는 검사가 되고,
미탐만 보면 조용한 시장을 고장이라 부르는 검사가 된다.
실제로 2026-08-31 에 `stale_value` 가 후자였다 — 9,700건이 전부 오탐이었다.

왜 합성이 아니라 실데이터인가
-----------------------------
사람이 만든 틱은 "같은 가격 스무 번 연속" 같은 실제 미시구조를 재현하지
않는다. EURUSDT 의 동일가 구간, BTTCUSDT 의 호가 단위, KRX 비유동 종목의
분 단위 공백은 **실제 시장에만 있다.** 그게 오탐을 만든 원인이었다.

코퍼스 갱신: `python scripts/make_quality_corpus.py`
"""
import gzip
import json
from pathlib import Path

import pytest

from mdfeed.quality import SEV_CRITICAL, SEV_WARNING, QualityMonitor

CORPUS = Path(__file__).resolve().parents[1] / "data" / "quality_corpus.jsonl.gz"

# 실측 베이스라인(2026-08-31). 이 위로 늘면 오탐이 늘어난 것이다.
MAX_EVENTS_PER_10K = 3.0


def load():
    if not CORPUS.exists():
        pytest.skip(f"코퍼스 없음: {CORPUS}")
    with gzip.open(CORPUS, "rt", encoding="utf-8") as fh:
        return [json.loads(x) for x in fh]


def run(rows, monitor=None):
    """코퍼스를 검사기에 흘린다. 벽시계는 체결시각으로 고정해 결정론을 지킨다."""
    m = monitor or QualityMonitor()
    clock = [0.0]
    m.stale._now = lambda: clock[0]
    for r in rows:
        clock[0] = r["ts"] / 1e6
        ts = r["ts"] * 1000
        if r["k"] == "t":
            m.on_trade(r["v"], r["s"], r["p"], ts)
        elif r["k"] == "q":
            m.on_quote(r["v"], r["s"], r["b"], r["a"], ts)
        else:
            m.on_bar(r["v"], r["s"], r["o"], r["h"], r["l"], r["c"], ts)
    return m


# ── 1. 오탐 ────────────────────────────────────────────────────────────────

def test_정상_시장_데이터에_거의_안_울린다():
    rows = load()
    m = run(rows)
    rep = m.report()
    per_10k = rep["critical"] + rep["warning"]
    rate = per_10k / (len(rows) / 10_000)
    assert rate <= MAX_EVENTS_PER_10K, (
        f"실데이터 {len(rows):,}건에 {per_10k}건 울렸다 "
        f"({rate:.2f}/만건) — 오탐이 늘었다: {rep['by_check']}")


def test_정상_데이터에_CRITICAL_은_없다():
    """CRITICAL 은 '이 값을 쓰면 안 된다'는 뜻이다. 정상 데이터에 나오면 안 된다."""
    rep = run(load()).report()
    assert rep["critical"] == 0, f"정상 데이터에 CRITICAL {rep['critical']}건: {rep['by_check']}"


def test_조용한_시장을_상류_고장이라_안_한다():
    """이게 9,700건 오탐의 정체였다."""
    rep = run(load()).report()
    assert rep["by_check"].get(f"stale_value:{SEV_WARNING}", 0) == 0


# ── 2. 미탐 ────────────────────────────────────────────────────────────────
# 한 번도 안 울리는 검사는 없는 것과 구분되지 않는다.

def test_상류가_같은_기록을_반복하면_잡는다():
    rows = load()
    victim = next(r for r in rows if r["k"] == "t")
    frozen = [dict(victim, ts=victim["ts"] + i * 10_000_000) for i in range(40)]
    # 체결시각(내용)은 그대로 두고 도착만 이어진다 = 상류가 같은 레코드를 반복
    for f in frozen:
        f["_frozen_ts"] = victim["ts"]
    m = QualityMonitor()
    clock = [0.0]
    m.stale._now = lambda: clock[0]
    fired = []
    for f in frozen:
        clock[0] = f["ts"] / 1e6
        fired += m.on_trade(f["v"], f["s"], f["p"], victim["ts"] * 1000)
    assert any(e.check == "stale_value" for e in fired), "얼어붙은 상류를 못 잡는다"


def test_한_틱_급등을_잡는다():
    rows = load()
    t = next(r for r in rows if r["k"] == "t")
    m = QualityMonitor()
    m.stale._now = lambda: 0.0
    m.on_trade(t["v"], t["s"], 100.0, t["ts"] * 1000)
    ev = m.on_trade(t["v"], t["s"], 150.0, t["ts"] * 1000 + 1_000_000_000)
    assert any(e.check == "price_jump" and e.severity == SEV_CRITICAL for e in ev)


def test_역전된_호가를_잡는다():
    m = QualityMonitor()
    ev = m.on_quote("TEST", "S", bid=101.0, ask=100.0, ts_ns=1)   # 매수 > 매도
    assert any(e.check == "quote_sanity" for e in ev), "역전 호가를 그냥 통과시킨다"


def test_깨진_봉을_잡는다():
    m = QualityMonitor()
    ev = m.on_bar("TEST", "S", o=100.0, h=90.0, l=110.0, c=100.0, ts_ns=1)  # 고가<저가
    assert any(e.check == "bar_integrity" and e.severity == SEV_CRITICAL for e in ev)


def test_모든_검사가_적어도_한_번은_울린다():
    """검사기 목록과 실제로 울릴 수 있는 검사가 어긋나면 안 된다.

    사건을 만들어도 안 울리는 검사가 있으면, 그건 배포돼 있는 게 아니다.
    """
    m = QualityMonitor()
    m.stale._now = lambda: 0.0
    seen = set()
    seen.update(e.check for e in m.on_quote("T", "S", 101.0, 100.0, 1))
    seen.update(e.check for e in m.on_bar("T", "S", 100.0, 90.0, 110.0, 100.0, 1))
    m.on_trade("T", "S", 100.0, 1)
    seen.update(e.check for e in m.on_trade("T", "S", 150.0, 1_000_000_000))
    assert {"quote_sanity", "bar_integrity", "price_jump"} <= seen, seen
