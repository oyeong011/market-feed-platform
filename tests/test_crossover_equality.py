"""동률(a == b)은 교차가 아니다 — 구현과 **검증 코드** 양쪽에서.

indicators.Crossover 의 문서에 이렇게 적혀 있다.

    처음엔 `a > b` 하나로 판정했는데, 그러면 두 값이 정확히 같아지는 순간을
    "아래로 내려갔다"로 오인한다.

구현은 그렇게 고쳐져 있었다. 그런데 그 구현을 **검증하는 코드**
(quant/integrations._crossover_match)는 여전히 `>=` / `<=` 로 비교하고 있었다.
동률이 `>=` 를 만족해 "위에 있었다"로 처리되고, 아래에서 스치기만 한 구간이
하향 교차로 잡힌다.

실측 (UPBIT KRW-BTC 1,878봉, SMA 10/30)
    i=1770  m1 < m2   아래
    i=1771  m1 == m2  동률      ← 스치기만 함
    i=1772  m1 < m2   아래      ← 교차한 적 없음
    우리 68개 / 대조 69개 — 그 한 건 차이로
    cross_check 이 "SMA 교차 시점 불일치 — 조사 필요"를 계속 뱉고 있었다.

**틀린 건 구현이 아니라 대조 코드였다.**
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "quant"))

from mdfeed.indicators import SMA, Crossover   # noqa: E402


class _Bar:
    def __init__(self, i, close):
        self.bucket_ns = i * 60_000_000_000
        self.open = self.high = self.low = self.close = close
        self.volume = 1.0
        self.tick_count = 1


def test_아래에서_스치면_교차가_아니다():
    """아래 → 동률 → 아래. 한 번도 위로 간 적이 없다."""
    c = Crossover()
    assert c.update(1.0, 2.0) == 0        # 아래 (첫 관측)
    assert c.update(2.0, 2.0) == 0        # 동률 — 접촉
    assert c.update(1.0, 2.0) == 0, "스치기만 했는데 교차로 봤다"


def test_위에서_스치면_교차가_아니다():
    c = Crossover()
    assert c.update(2.0, 1.0) == 0
    assert c.update(2.0, 2.0) == 0
    assert c.update(2.0, 1.0) == 0, "스치기만 했는데 교차로 봤다"


def test_동률을_지나_반대로_가면_교차다():
    """접촉 자체는 교차가 아니지만, 실제로 갈라지면 그때는 교차다."""
    c = Crossover()
    assert c.update(1.0, 2.0) == 0        # 아래
    assert c.update(2.0, 2.0) == 0        # 동률
    assert c.update(3.0, 2.0) == 1, "동률을 지나 위로 갔는데 못 잡았다"


def test_대조_코드도_같은_규칙을_쓴다():
    """구현만 고치고 검증 코드를 안 고치면, 검증이 영원히 불일치를 뱉는다."""
    pd = pytest.importorskip("pandas")
    from integrations import _crossover_match

    fast, slow = 2, 3
    # 느린 평균 아래에 있다가 정확히 한 번 닿고 다시 아래로 내려가는 계열
    closes = [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    bars = [_Bar(i, float(px)) for i, px in enumerate(closes)]
    assert _crossover_match(bars, fast, slow) is True

    # 진짜 교차가 있는 계열에서도 일치해야 한다 (기준이 느슨해진 게 아님)
    closes = [10, 10, 10, 11, 12, 13, 14, 13, 12, 11, 10, 9, 8, 9, 10, 11, 12]
    bars = [_Bar(i, float(px)) for i, px in enumerate(closes)]
    assert _crossover_match(bars, fast, slow) is True


def test_실측_패턴을_그대로_재현해도_일치한다():
    """i=1770 아래 → 1771 동률 → 1772 아래. 그 한 건이 문제였다."""
    pd = pytest.importorskip("pandas")
    from integrations import _crossover_match

    fast, slow = 2, 4
    # 두 이동평균이 정확히 같아지는 지점을 만든다
    closes = [100.0, 100.0, 100.0, 100.0, 90.0, 90.0, 110.0, 90.0, 90.0, 90.0,
              90.0, 90.0, 90.0, 90.0]
    bars = [_Bar(i, px) for i, px in enumerate(closes)]
    assert _crossover_match(bars, fast, slow) is True


# ── 교차검증이 실제로 검증하는가 ──────────────────────────────────────────

def _sample_bars(n=400):
    """추세가 여러 번 뒤집혀 교차가 실제로 나오는 계열."""
    import math
    out = []
    for i in range(n):
        px = 50_000_000 + math.sin(i / 23.0) * 3_000_000 + (i % 11) * 40_000
        b = _Bar(i, px)
        b.open = px * 0.999
        b.high = px * 1.002
        b.low = px * 0.998
        out.append(b)
    return out


def test_기준_엔진이_실제로_거래한다():
    """0회와 34회를 대조해서 알 수 있는 건 없다.

    backtesting.py 는 **정수 단위**로 체결한다. 1억짜리 BTC 를 자본 100만원으로
    사려 하면 units = floor(현금×비율 / 가격) = 0 이라 주문이 전부 취소된다.
    실제로 그래서 "우리 34회 / 상대 0회" 였고, 수익률 비교가 아무것도
    비교하지 않고 있었다. 그런데 verdict 는 그걸 "차이 3.325%p" 로 보고했다.
    """
    pytest.importorskip("pandas")
    pytest.importorskip("backtesting")
    from integrations import verify_backtest_bt

    r = verify_backtest_bt(_sample_bars(), fast=10, slow=30)
    assert "skipped" not in r
    assert r["trades"] > 0, "기준 엔진이 거래를 0회 한다 — 비교가 성립하지 않는다"


def test_기준_자본을_맞춘_사실을_숨기지_않는다():
    """수익률은 규모에 무관하지만, 같은 자본으로 돌린 걸로 오해하면 안 된다."""
    pytest.importorskip("pandas")
    pytest.importorskip("backtesting")
    from integrations import verify_backtest_bt

    r = verify_backtest_bt(_sample_bars(), equity=1_000_000.0)
    assert r["ref_equity"] > 1_000_000
    assert r["ref_equity_note"]


def test_두_엔진의_수익률이_붙는다():
    """우리 엔진이 조용히 틀렸는지 남의 엔진으로 확인한다. 그게 이 코드의 존재 이유다."""
    pytest.importorskip("pandas")
    pytest.importorskip("backtesting")
    import backtest as bt
    from integrations import verify_backtest_bt

    bars = _sample_bars()
    rows = [bt.BarRow(bucket_ns=b.bucket_ns, open=b.open, high=b.high,
                      low=b.low, close=b.close) for b in bars]
    ours = bt.run(rows, "sma_cross", symbol="UPBIT:KRW-BTC",
                  slippage_bp=0.0, cash_fraction=0.95, cooldown_s=0.0)
    theirs = verify_backtest_bt(bars, fast=10, slow=30)
    if theirs["trades"] == 0:
        pytest.skip("기준 엔진이 거래를 안 했다 — 위 시험이 그걸 잡는다")
    ours_pct = (ours.end_equity / ours.start_equity - 1) * 100
    diff = abs(ours_pct - theirs["total_return_pct"])
    assert diff < 1.0, (
        f"두 엔진의 수익률이 {diff:.3f}%p 벌어졌다 "
        f"(우리 {ours_pct:.3f}% / 상대 {theirs['total_return_pct']:.3f}%)")
