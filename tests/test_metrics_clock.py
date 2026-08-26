"""지연 히스토그램과 시계 오프셋 추정."""
import pytest

from mdfeed.clock import ClockMonitor, SkewEstimator
from mdfeed.metrics import Histogram, Registry


class TestHistogram:
    def test_percentiles_track_tail_not_mean(self):
        """평균에 묻히는 꼬리를 상위 분위수가 잡아내야 한다.

        꼬리 비율이 정확히 1%면 p99 는 경계에 걸린다. 2% 를 써서 p99 가
        확실히 꼬리 안으로 들어가게 한다.
        """
        h = Histogram("t")
        for _ in range(9800):
            h.record(1000.0)
        for _ in range(200):
            h.record(500_000.0)
        s = h.snapshot()
        assert s["p50_us"] == pytest.approx(1000, rel=0.15)
        assert s["p99_us"] > 100_000
        assert s["mean_us"] < 20_000          # 평균은 거의 안 움직인다

    def test_exactly_1pct_tail_sits_at_p99_boundary(self):
        """꼬리가 정확히 1%면 p99 는 본체, p99.9 가 꼬리를 잡는다.
        분위수 해석을 헷갈리지 않기 위해 명시적으로 고정해 둔다."""
        h = Histogram("t")
        for _ in range(9900):
            h.record(1000.0)
        for _ in range(100):
            h.record(500_000.0)
        s = h.snapshot()
        assert s["p99_us"] < 2_000
        assert s["p999_us"] > 100_000

    def test_percentile_never_exceeds_max(self):
        h = Histogram("t")
        for v in (10.0, 20.0, 30.0):
            h.record(v)
        assert h.percentile(99.9) <= h.snapshot()["max_us"]

    def test_relative_error_within_bucket_resolution(self):
        h = Histogram("t")
        for _ in range(1000):
            h.record(12345.0)
        assert h.percentile(50) == pytest.approx(12345.0, rel=0.13)

    def test_empty_histogram_is_safe(self):
        s = Histogram("t").snapshot()
        assert s["count"] == 0 and s["p99_us"] == 0.0

    def test_negative_latency_clamped(self):
        """시계 역전으로 음수가 들어와도 히스토그램이 깨지면 안 된다."""
        h = Histogram("t")
        h.record(-5000.0)
        assert h.snapshot()["count"] == 1


class TestRegistry:
    def test_prometheus_exposition_format(self):
        r = Registry("svc")
        r.counter("ticks_total", 5, venue="UPBIT")
        r.gauge("subscribers", 3)
        r.observe("lat", 100.0)
        text = r.prometheus()
        assert 'mdfeed_ticks_total{service="svc",venue="UPBIT"} 5' in text
        assert 'mdfeed_subscribers{service="svc"} 3' in text
        assert 'quantile="p99"' in text
        assert text.endswith("\n")

    def test_labels_are_independent_series(self):
        r = Registry("svc")
        r.counter("t", 1, venue="A")
        r.counter("t", 2, venue="B")
        snap = r.snapshot()["counters"]
        assert snap['t{venue="A"}'] == 1 and snap['t{venue="B"}'] == 2


class TestSkewEstimator:
    def test_negative_offset_is_corrected_to_nonnegative(self):
        """거래소 시계가 앞서면 원시 지연이 음수로 나온다.
        보정 후에는 절대 음수가 나오면 안 된다."""
        e = SkewEstimator()
        raws = [-13000.0, -12000.0, -9000.0, -13500.0, 5000.0]
        out = [e.observe(v) for v in raws]
        assert all(v >= 0 for v in out)
        assert e.offset_us == pytest.approx(-13500.0)
        assert e.suspicious is True

    def test_relative_ordering_preserved(self):
        e = SkewEstimator()
        for v in (-10_000.0, -9_000.0, -5_000.0):
            e.observe(v)
        # 보정은 상수를 빼는 것이므로 상대 크기가 유지된다
        assert e.observe(-9_000.0) < e.observe(-5_000.0)

    def test_healthy_clock_not_flagged(self):
        e = SkewEstimator()
        for v in (200.0, 350.0, 180.0):
            e.observe(v)
        assert e.suspicious is False
        assert e.offset_us == pytest.approx(180.0)

    def test_monitor_tracks_venues_separately(self):
        m = ClockMonitor()
        m.observe("BINANCE", -13_000.0)
        m.observe("UPBIT", 30_000.0)
        rep = m.report()
        assert rep["BINANCE"]["local_clock_behind"] is True
        assert rep["UPBIT"]["local_clock_behind"] is False
        assert m.any_suspicious() is True
