"""증분 지표 — 널리 쓰이는 정의와 수치가 맞는지 대조한다.

실시간 엔진과 백테스트가 이 클래스들을 공유하므로, 여기가 틀리면 두 곳이 함께 틀린다.
"""
import math

import pytest

from mdfeed.indicators import (ATR, EMA, MACD, RSI, SMA, Bollinger, Crossover,
                               RollingStd)

# Wilder(1978) 예제로 널리 인용되는 종가 시퀀스
WILDER = [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08,
          45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64]


class TestSMA:
    def test_value(self):
        s = SMA(5)
        for x in [1, 2, 3, 4, 5]:
            s.update(x)
        assert s.value == pytest.approx(3.0)

    def test_not_ready_before_window_fills(self):
        s = SMA(5)
        for x in [1, 2, 3, 4]:
            assert s.update(x) is None
        assert not s.ready

    def test_window_slides(self):
        s = SMA(3)
        for x in range(1, 11):
            s.update(x)
        assert s.value == pytest.approx((8 + 9 + 10) / 3)

    def test_rolling_sum_matches_naive_after_many_updates(self):
        """합을 굴리는 최적화가 누적오차로 어긋나지 않는지."""
        s = SMA(50)
        vals = [1e6 + i * 0.001 for i in range(5000)]
        for v in vals:
            s.update(v)
        assert s.value == pytest.approx(sum(vals[-50:]) / 50, rel=1e-12)


class TestEMA:
    def test_seeded_with_sma(self):
        e = EMA(3)
        out = [e.update(x) for x in [1, 2, 3, 4, 5]]
        assert out[:2] == [None, None]
        assert out[2] == pytest.approx(2.0)           # SMA(1,2,3)
        assert out[3] == pytest.approx(2.0 + 0.5 * (4 - 2.0))

    def test_converges_to_constant(self):
        e = EMA(10)
        for _ in range(500):
            e.update(42.0)
        assert e.value == pytest.approx(42.0, abs=1e-9)


class TestRSI:
    def test_matches_wilder_reference(self):
        """Wilder 원 정의(평활 1/n)와 소수 둘째 자리까지 일치해야 한다."""
        r = RSI(14)
        got = [v for v in (r.update(c) for c in WILDER) if v is not None]
        expected = [70.46, 66.25, 66.48, 69.35, 66.29]
        assert [round(v, 2) for v in got[:5]] == expected

    def test_all_gains_gives_100(self):
        r = RSI(14)
        for i in range(40):
            r.update(100 + i)
        assert r.value == pytest.approx(100.0)

    def test_all_losses_gives_0(self):
        r = RSI(14)
        for i in range(40):
            r.update(100 - i)
        assert r.value == pytest.approx(0.0)

    def test_bounded_0_100(self):
        import random
        rnd = random.Random(7)
        r = RSI(14)
        px = 100.0
        for _ in range(2000):
            px *= 1 + rnd.gauss(0, 0.01)
            v = r.update(px)
            if v is not None:
                assert 0.0 <= v <= 100.0


class TestRollingStd:
    def test_matches_sample_stdev(self):
        import statistics
        vals = [2, 4, 4, 4, 5, 5, 7, 9]
        s = RollingStd(len(vals))
        for v in vals:
            s.update(v)
        assert s.value == pytest.approx(statistics.stdev(vals))

    def test_constant_series_is_zero(self):
        s = RollingStd(10)
        for _ in range(10):
            s.update(5.0)
        assert s.value == pytest.approx(0.0, abs=1e-12)

    def test_no_negative_variance_from_float_error(self):
        """제곱합 방식은 분산이 음수로 새는 고전적 함정이 있다."""
        s = RollingStd(20)
        for _ in range(100):
            s.update(1e9 + 1e-6)
        assert s.value is not None and s.value >= 0.0


class TestBollinger:
    def test_bands_straddle_middle(self):
        b = Bollinger(20, 2.0)
        for i in range(40):
            b.update(100 + (i % 5))
        mid, up, lo = b.value
        assert lo < mid < up
        assert (up - mid) == pytest.approx(mid - lo)


class TestMACD:
    def test_histogram_is_macd_minus_signal(self):
        m = MACD()
        out = None
        for i in range(200):
            out = m.update(100 + math.sin(i / 10) * 5)
        macd, sig, hist = out
        assert hist == pytest.approx(macd - sig)


class TestATR:
    def test_true_range_uses_prev_close_gaps(self):
        a = ATR(3)
        a.update(10, 9, 9.5)
        a.update(11, 10.5, 10.8)      # 갭 상승: TR = high - prev_close = 1.5
        v = a.update(12, 11, 11.5)
        assert v is not None and v > 0


class TestCrossover:
    def test_detects_edges_only(self):
        c = Crossover()
        seq = [(1, 2), (3, 2), (4, 2), (1, 2), (0, 2)]
        assert [c.update(a, b) for a, b in seq] == [0, 1, 0, -1, 0]

    def test_first_observation_never_signals(self):
        assert Crossover().update(10, 1) == 0
