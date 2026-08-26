"""백테스트 엔진 — 정직성 장치가 실제로 동작하는지."""
import math

import pytest

import backtest as bt


def bars(closes, opens=None):
    opens = opens or closes
    return [bt.BarRow(i * 60_000_000_000, o, max(o, c), min(o, c), c, 1.0, 1)
            for i, (o, c) in enumerate(zip(opens, closes))]


def test_flat_market_loses_only_costs():
    """가격이 안 움직이면 수익은 정확히 비용만큼 마이너스여야 한다."""
    b = bars([100.0] * 100)
    r = bt.run(b, "sma_cross", fee=0.001, slippage_bp=0)
    assert r.total_return <= 0
    assert r.trades == 0 or r.total_return == pytest.approx(0.0, abs=0.01)


def test_no_lookahead_signal_fills_at_next_bar_open():
    """t 봉 종가로 판단하고 t+1 봉 시가로 체결해야 한다.

    같은 봉 종가로 체결하면 '종가를 보고 종가에 산' 셈이 되어 성과가 부풀려진다.
    """
    closes = [100.0] * 40 + [200.0] * 40          # 40번째에서 급등
    opens = [100.0] * 41 + [200.0] * 39
    b = bars(closes, opens)
    r = bt.run(b, "sma_cross", fee=0.0, slippage_bp=0)
    buys = [f for f in r.fills if f.side == "BUY"]
    if buys:
        idx = next(i for i, x in enumerate(b) if x.bucket_ns == buys[0].ts_ns)
        # 체결가는 그 봉의 시가여야 한다 (종가가 아니라)
        assert buys[0].price == pytest.approx(b[idx].open)


def test_fees_and_slippage_reduce_return():
    closes = [100 + math.sin(i / 5) * 10 for i in range(300)]
    b = bars(closes)
    free = bt.run(b, "sma_cross", fee=0.0, slippage_bp=0)
    costly = bt.run(b, "sma_cross", fee=0.002, slippage_bp=20)
    assert costly.total_return < free.total_return


def test_position_is_closed_at_end():
    """미청산 포지션을 남기면 최종 자산이 부풀려진다."""
    b = bars([100 + i for i in range(200)])
    r = bt.run(b, "sma_cross")
    buys = sum(1 for f in r.fills if f.side == "BUY")
    sells = sum(1 for f in r.fills if f.side == "SELL")
    assert buys == sells


def test_buy_and_hold_baseline():
    b = bars([100.0] * 1 + [110.0])
    eq = bt.buy_and_hold(b, 1_000_000, fee=0.0)
    assert eq == pytest.approx(1_100_000, rel=1e-9)


def test_max_drawdown_is_negative_or_zero():
    b = bars([100, 120, 90, 130, 80, 140])
    r = bt.run(b, "sma_cross")
    assert r.max_drawdown() <= 0.0


def test_summary_has_all_metrics():
    b = bars([100 + math.sin(i / 7) * 8 for i in range(400)])
    s = bt.run(b, "rsi_revert").summary(60)
    for k in ("total_return_pct", "max_drawdown_pct", "sharpe",
              "win_rate_pct", "trades", "bars"):
        assert k in s


def test_unknown_strategy_raises():
    with pytest.raises(ValueError):
        bt.run(bars([1.0, 2.0]), "does_not_exist")


def test_empty_and_tiny_inputs_do_not_crash():
    assert bt.run([], "sma_cross").total_return == 0.0
    assert bt.buy_and_hold([], 100.0) == 100.0
