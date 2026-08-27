"""지연시간 히스토그램 · 카운터 · Prometheus 노출.

운영에서 평균 지연시간은 거의 쓸모가 없다. 마켓데이터에서 문제가 되는 건
"가끔 200ms 튀는 구간"이고, 그건 평균에 묻힌다. 그래서 p50/p95/p99/p99.9를
본다.

전체 표본을 들고 있으면 메모리가 무한히 자라므로 로그 스케일 버킷
히스토그램(HDR 히스토그램 아이디어의 축소판)을 쓴다. 버킷당 상대오차는
1/BUCKETS_PER_DECADE 수준으로 묶이고, 메모리는 고정된다.
"""

from __future__ import annotations

import math
import threading
import time
from typing import Iterable

BUCKETS_PER_DECADE = 20          # 데케이드(10배)당 20칸 → 상대오차 약 12%
MIN_US = 1.0                     # 1µs 미만은 첫 버킷으로
DECADES = 8                      # 1µs ~ 100s
NUM_BUCKETS = BUCKETS_PER_DECADE * DECADES + 2


class Histogram:
    """로그 버킷 히스토그램. 고정 메모리(약 162칸), O(1) 기록."""

    __slots__ = ("name", "_buckets", "_count", "_sum", "_min", "_max", "_lock")

    def __init__(self, name: str):
        self.name = name
        self._buckets = [0] * NUM_BUCKETS
        self._count = 0
        self._sum = 0.0
        self._min = float("inf")
        self._max = 0.0
        self._lock = threading.Lock()

    @staticmethod
    def _index(us: float) -> int:
        if us <= MIN_US:
            return 0
        i = int(math.log10(us / MIN_US) * BUCKETS_PER_DECADE) + 1
        return min(i, NUM_BUCKETS - 1)

    @staticmethod
    def _value(index: int) -> float:
        if index == 0:
            return MIN_US
        return MIN_US * (10 ** ((index - 0.5) / BUCKETS_PER_DECADE))

    def record(self, us: float) -> None:
        if us < 0:                      # 거래소 시계가 앞선 경우(clock skew)
            us = 0.0
        with self._lock:
            self._buckets[self._index(us)] += 1
            self._count += 1
            self._sum += us
            if us < self._min:
                self._min = us
            if us > self._max:
                self._max = us

    def record_many(self, values: Iterable[float]) -> None:
        for v in values:
            self.record(v)

    def percentile(self, q: float) -> float:
        """q는 0~100. 표본이 없으면 0."""
        with self._lock:
            if self._count == 0:
                return 0.0
            target = self._count * q / 100.0
            cum = 0
            for i, c in enumerate(self._buckets):
                cum += c
                if cum >= target:
                    # 버킷 대표값이 실측 최대를 넘지 않도록 클램프
                    # (로그 버킷은 위쪽으로 최대 12% 과대추정한다)
                    return min(self._value(i), self._max)
            return self._max

    def snapshot(self) -> dict:
        with self._lock:
            count, total, mn, mx = self._count, self._sum, self._min, self._max
        return {
            "count": count,
            "mean_us": round(total / count, 1) if count else 0.0,
            "min_us": round(mn, 1) if count else 0.0,
            "p50_us": round(self.percentile(50), 1),
            "p95_us": round(self.percentile(95), 1),
            "p99_us": round(self.percentile(99), 1),
            "p999_us": round(self.percentile(99.9), 1),
            "max_us": round(mx, 1),
        }

    def reset(self) -> None:
        with self._lock:
            self._buckets = [0] * NUM_BUCKETS
            self._count = 0
            self._sum = 0.0
            self._min = float("inf")
            self._max = 0.0


class Registry:
    """프로세스 하나의 지표 모음. /metrics 와 /healthz 가 이걸 읽는다."""

    def __init__(self, service: str):
        self.service = service
        self.started_at = time.time()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._hists: dict[str, Histogram] = {}
        self._lock = threading.Lock()

    def declare_counters(self, *names: str, **labeled) -> None:
        """카운터를 0으로 미리 만들어 둔다.

        **초기화하지 않으면 첫 사건이 날 때까지 지표 자체가 존재하지 않는다.**
        Prometheus 는 없는 지표에 대해 오류를 내지 않고 조용히 no data 를 준다.
        그래서 `increase(mdfeed_dropped_total[5m]) > 1000` 같은 알람이
        **영원히 울리지 않는다.** 설정은 완벽해 보이고 아무도 이상을 못 느낀다.

        드롭이 한 번도 없는 것과 드롭 지표가 없는 것은 다르다. 전자는 좋은 소식이고
        후자는 계측이 안 되고 있다는 뜻인데, 초기화를 안 하면 둘이 구분되지 않는다.

        `scripts/verify_alerts.py` 가 이 누락을 잡아 준다.
        """
        for n in names:
            with self._lock:
                self._counters.setdefault(n, 0.0)
        for n, label_sets in labeled.items():
            for labels in label_sets:
                key = _key(n, labels)
                with self._lock:
                    self._counters.setdefault(key, 0.0)

    def counter(self, name: str, value: float = 1.0, **labels) -> None:
        key = _key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, name: str, value: float, **labels) -> None:
        with self._lock:
            self._gauges[_key(name, labels)] = value

    def histogram(self, name: str) -> Histogram:
        with self._lock:
            h = self._hists.get(name)
            if h is None:
                h = self._hists[name] = Histogram(name)
            return h

    def observe(self, name: str, us: float) -> None:
        self.histogram(name).record(us)

    @property
    def uptime_s(self) -> float:
        return time.time() - self.started_at

    def rate(self, counter_name: str) -> float:
        """기동 이후 평균 초당 처리량."""
        up = self.uptime_s
        return self._counters.get(counter_name, 0.0) / up if up > 0 else 0.0

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            names = list(self._hists)
        return {
            "service": self.service,
            "uptime_s": round(self.uptime_s, 1),
            "counters": counters,
            "gauges": gauges,
            "histograms": {n: self._hists[n].snapshot() for n in names},
        }

    def prometheus(self) -> str:
        """Prometheus text exposition format v0.0.4."""
        lines = [
            f'mdfeed_uptime_seconds{{service="{self.service}"}} {self.uptime_s:.1f}',
        ]
        for key, v in sorted(self._counters.items()):
            lines.append(f"{_prom(key, self.service)} {v:g}")
        for key, v in sorted(self._gauges.items()):
            lines.append(f"{_prom(key, self.service)} {v:g}")
        for name in sorted(self._hists):
            snap = self._hists[name].snapshot()
            for q in ("p50", "p95", "p99", "p999"):
                lines.append(
                    f'mdfeed_{name}_microseconds{{service="{self.service}",quantile="{q}"}} '
                    f'{snap[q + "_us"]:g}'
                )
            lines.append(
                f'mdfeed_{name}_count{{service="{self.service}"}} {snap["count"]:g}'
            )
        return "\n".join(lines) + "\n"


def _key(name: str, labels: dict) -> str:
    if not labels:
        return name
    inner = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{inner}}}"


def _prom(key: str, service: str) -> str:
    if "{" in key:
        name, rest = key.split("{", 1)
        return f'mdfeed_{name}{{service="{service}",{rest}'
    return f'mdfeed_{key}{{service="{service}"}}'
