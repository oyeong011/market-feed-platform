"""실시간 엔진과 백테스트가 정말 **같은 결과**를 내는가.

이 프로젝트는 두 곳에서 그렇게 주장한다.

    indicators.py — "실시간 엔진과 백테스트가 같은 코드를 쓴다"
    strategies.py — "같은 클래스를 실시간 루프에 꽂든 과거 봉 리스트에
                     꽂든 결과가 동일하다"

지표와 전략 클래스가 같은 건 맞다. 그런데 **시스템의 출력**이 같은지는
아무도 확인한 적이 없다. 전략이 같은 판단을 내려도 그 뒤에 붙은 것이
다르면 백테스트 성과는 배포된 시스템의 성과가 아니다.

여기서 그 차이를 고정한다.
"""
import time

import pytest

from mdfeed.config import Config
from mdfeed.models import Bar, Trade
from mdfeed.services.strategy import StrategyEngine
from mdfeed.strategies import HOLD, REGISTRY, SignalGate

BASE_NS = 1_700_000_000_000_000_000
IV_NS = 60 * 1_000_000_000


def make_bars(n: int = 120):
    """추세가 뒤집히는 결정론적 봉. 시그널이 실제로 나오게 만든다."""
    import math
    out = []
    for i in range(n):
        px = 100.0 + math.sin(i / 7.0) * 18.0 + (i % 5) * 0.4
        b = Bar("TEST", "SYNTH", BASE_NS + i * IV_NS, 60)
        for k, p in enumerate((px, px * 1.01, px * 0.99, px)):
            b.update(Trade("TEST", "SYNTH", b.bucket_ns + k, b.bucket_ns + k,
                           p, 1.0, 1))
        out.append(b)
    return out


def _backtest(strategy: str, bars, cooldown_s: float = 0.0):
    """백테스트 경로 — 전략 + 같은 SignalGate. quant/backtest.run 과 같은 규칙."""
    strat = REGISTRY[strategy]()
    gate = SignalGate(cooldown_s)
    out = []
    for b in bars:
        a = strat.on_bar(b)
        if a != HOLD and gate.allow("TEST:SYNTH", strategy, b.bucket_ns):
            out.append((b.bucket_ns, a))
    return out


def _live(strategy: str, bars, cooldown_s: float):
    """실시간 경로 — 진짜 StrategyEngine 의 봉 마감 처리를 쓴다."""
    cfg = Config()
    cfg.strategies = [strategy]
    cfg.signal_cooldown_s = cooldown_s
    eng = StrategyEngine(cfg)
    emitted = []
    eng._emit = lambda bar, name, action: emitted.append((bar.bucket_ns, action))
    for b in bars:
        eng._close_bar(f"{b.venue}:{b.symbol}", b)
    return emitted


@pytest.mark.parametrize("strategy", sorted(REGISTRY))
def test_쿨다운이_없으면_두_경로가_완전히_같다(strategy):
    """전략·지표가 공유된다는 주장의 실제 확인."""
    bars = make_bars()
    assert _live(strategy, bars, cooldown_s=0.0) == _backtest(strategy, bars), (
        f"{strategy}: 쿨다운을 껐는데도 실시간과 백테스트가 다르다")


@pytest.mark.parametrize("strategy", sorted(REGISTRY))
@pytest.mark.parametrize("cooldown", [0.0, 30.0, 180.0, 3600.0])
def test_쿨다운이_켜져_있어도_두_경로가_같다(strategy, cooldown):
    """억제 규칙까지 같은 구현을 써야 백테스트가 배포된 시스템을 설명한다.

    예전엔 쿨다운이 실시간에만 있었다. 지표·전략 클래스를 공유해 놓고
    그 뒤에 붙은 규칙이 갈리면 공유한 의미가 없다.
    """
    bars = make_bars()
    assert _live(strategy, bars, cooldown) == _backtest(strategy, bars, cooldown)


@pytest.mark.parametrize("strategy", sorted(REGISTRY))
def test_봉_간격보다_긴_쿨다운은_실제로_억제한다(strategy):
    """게이트가 아무것도 안 하면 있으나 마나다. 양쪽에서 똑같이 줄어야 한다."""
    bars = make_bars()
    none_ = _backtest(strategy, bars, 0.0)
    if len(none_) < 2:
        pytest.skip(f"{strategy}: 이 봉에서 시그널이 부족해 비교할 게 없다")
    # 창 전체보다 긴 쿨다운 → 첫 시그널만 남아야 한다.
    # 고정 숫자를 쓰면 봉 생성이 바뀔 때 조용히 억제가 0이 된다.
    window_s = (bars[-1].bucket_ns - bars[0].bucket_ns) / 1e9 + 60
    long_ = _backtest(strategy, bars, window_s)
    assert len(long_) == 1, f"창 전체보다 긴 쿨다운인데 {len(long_)}건 나왔다"
    assert _live(strategy, bars, window_s) == long_


@pytest.mark.parametrize("strategy", sorted(REGISTRY))
def test_재생_속도가_결과를_바꾸지_않는다(strategy):
    """쿨다운을 벽시계로 재면 같은 테이프를 다시 흘려도 결과가 달라진다.

    평시에는 봉 간격(60초)이 쿨다운(30초)보다 길어 거의 안 걸리지만,
    재생이나 밀린 구간을 따라잡을 때는 봉이 몇 ms 간격으로 닫혀 전부
    억제됐다. **정확히 그때가 백테스트와 대조하는 순간이다.**

    이제 시장 시각으로 재므로 얼마나 빨리 흘리든 같은 결과가 나온다.
    """
    bars = make_bars()
    fast = _live(strategy, bars, 30.0)
    time.sleep(0.15)                              # 벽시계를 실제로 흘려 본다
    slow = []
    cfg = Config()
    cfg.strategies = [strategy]
    cfg.signal_cooldown_s = 30.0
    eng = StrategyEngine(cfg)
    eng._emit = lambda bar, name, action: slow.append((bar.bucket_ns, action))
    for i, b in enumerate(bars):
        eng._close_bar(f"{b.venue}:{b.symbol}", b)
        if i % 20 == 0:
            time.sleep(0.05)                      # 느리게 흘린다
    assert fast == slow, "재생 속도에 따라 시그널이 달라진다 — 벽시계로 재고 있다"


def test_백테스트_실행기도_같은_게이트를_쓴다():
    """헬퍼만 맞추고 진짜 실행기가 안 쓰면 소용없다."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "quant"))
    import backtest as bt

    bars = make_bars()
    rows = [bt.BarRow(bucket_ns=b.bucket_ns, open=b.open, high=b.high,
                      low=b.low, close=b.close, volume=b.volume,
                      tick_count=b.tick_count) for b in bars]
    window_s = (rows[-1].bucket_ns - rows[0].bucket_ns) / 1e9 + 60
    loose = bt.run(rows, "sma_cross", symbol="SYNTH", cooldown_s=0.0)
    tight = bt.run(rows, "sma_cross", symbol="SYNTH", cooldown_s=window_s)
    assert loose.suppressed == 0
    assert tight.suppressed > 0, "실행기가 게이트를 안 쓰고 있다"
    assert len(tight.fills) <= len(loose.fills)


def test_백테스트_봉이_실시간_봉의_필드를_다_갖지는_않는다():
    """BarRow 와 models.Bar 는 **관례로만** 맞춰져 있다.

    전략이 venue·symbol·interval_s·notional 중 하나라도 쓰면 백테스트에서만
    터진다. 지금 전략들은 안 쓰지만, 막는 장치가 없다는 사실을 못 박아 둔다.
    """
    import dataclasses
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "quant"))
    from backtest import BarRow

    live = {f.name for f in dataclasses.fields(Bar)}
    bt = {f.name for f in dataclasses.fields(BarRow)}
    missing = live - bt
    assert missing == {"venue", "symbol", "interval_s", "notional"}, (
        f"필드 구성이 바뀌었다. 전략이 {missing} 를 쓰면 백테스트에서만 터진다")


@pytest.mark.parametrize("strategy", sorted(REGISTRY))
def test_전략은_백테스트_봉만으로_판단한다(strategy):
    """실시간에만 있는 필드를 전략이 몰래 쓰고 있지 않은지 본다."""
    import dataclasses
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "quant"))
    from backtest import BarRow

    strat = REGISTRY[strategy]()
    for i in range(60):
        b = make_bars(1)[0]
        row = BarRow(bucket_ns=BASE_NS + i * IV_NS, open=b.open, high=b.high,
                     low=b.low, close=b.close + i * 0.3, volume=b.volume,
                     tick_count=b.tick_count)
        strat.on_bar(row)          # AttributeError 가 나면 실시간 전용 필드를 쓴 것


def test_봉_간격보다_짧은_쿨다운은_기동_때_밝힌다(caplog):
    """켜 뒀다고 믿는 장치가 실은 꺼져 있으면 안 된다.

    기본값이 정확히 그 상태다 — SIGNAL_COOLDOWN_S=30, BAR_INTERVAL_S=60.
    시장 시각으로 재므로 두 시그널은 항상 60초 이상 떨어져 있고,
    30초 쿨다운은 한 번도 발동하지 않는다. 실측: 1,878봉 백테스트에서
    suppressed_signals=0.
    """
    cfg = Config()
    cfg.signal_cooldown_s = 30.0
    cfg.bar_interval_s = 60
    with caplog.at_level("WARNING"):
        StrategyEngine(cfg)
    assert "발동하지 않는다" in caplog.text

    caplog.clear()
    cfg.signal_cooldown_s = 300.0
    with caplog.at_level("WARNING"):
        StrategyEngine(cfg)
    assert "발동하지 않는다" not in caplog.text
