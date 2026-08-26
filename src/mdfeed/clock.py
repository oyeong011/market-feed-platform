"""거래소별 시계 오프셋 추정 — 지연시간 지표를 믿을 수 있게 만드는 장치.

문제
----
지연시간을 `수신시각(우리 시계) - 체결시각(거래소 시계)` 로 재면, 두 시계가
어긋난 만큼이 그대로 지연시간에 섞인다. 실제로 이 프로젝트를 바이낸스에 붙였을 때
**-11ms** 가 나왔다. 음수 지연은 물리적으로 불가능하니 로컬 시계가 약 11ms 뒤처져
있다는 뜻이다. 이 상태로 p99 를 보고하면 전부 거짓말이다.

해결: 최소값 필터 (NTP 가 쓰는 것과 같은 아이디어)
--------------------------------------------------
관측값 = 진짜 편도지연 + 시계오프셋 이고, 진짜 편도지연은 항상 0 이상이다.
따라서 충분히 많은 표본의 **최솟값**이 시계오프셋의 추정치가 된다
(최소한 한 건은 큐잉 없이 거의 즉시 도착했다고 보는 가정).

    offset ≈ min(관측 지연)
    보정 지연 = 관측 지연 - offset      → 항상 0 이상, 상대 비교가 유효해진다

슬라이딩 윈도우로 최솟값을 갱신하는 이유는 시계 오프셋이 NTP 보정·서버 이전으로
시간에 따라 변하기 때문이다. 고정 최솟값을 쓰면 한 번 튄 이상치에 영원히 묶인다.

주의: 이건 **편도 지연의 절대값**을 주지 않는다. 왕복 측정 없이 편도를 정확히
아는 방법은 없다(PTP 하드웨어 타임스탬프가 필요하다). 여기서 얻는 것은
"기준선 대비 얼마나 튀었는가"이고, 운영 알람에는 그걸로 충분하다.
"""

from __future__ import annotations

import time


class SkewEstimator:
    """venue 하나의 시계 오프셋을 슬라이딩 최솟값으로 추정."""

    __slots__ = ("_buckets", "_bucket_s", "_n", "_cur_idx", "_cur_start", "samples")

    def __init__(self, window_s: float = 300.0, buckets: int = 10):
        self._n = buckets
        self._bucket_s = window_s / buckets
        self._buckets: list[float | None] = [None] * buckets
        self._cur_idx = 0
        self._cur_start = time.time()
        self.samples = 0

    def _roll(self, now: float) -> None:
        elapsed = now - self._cur_start
        if elapsed < self._bucket_s:
            return
        steps = min(int(elapsed / self._bucket_s), self._n)
        for _ in range(steps):
            self._cur_idx = (self._cur_idx + 1) % self._n
            self._buckets[self._cur_idx] = None   # 가장 오래된 버킷을 비운다
        self._cur_start += steps * self._bucket_s

    def observe(self, raw_us: float, now: float | None = None) -> float:
        """관측 지연을 넣고 보정된 지연을 돌려준다."""
        now = now or time.time()
        self._roll(now)
        b = self._buckets[self._cur_idx]
        if b is None or raw_us < b:
            self._buckets[self._cur_idx] = raw_us
        self.samples += 1
        return max(0.0, raw_us - self.offset_us)

    @property
    def offset_us(self) -> float:
        vals = [v for v in self._buckets if v is not None]
        return min(vals) if vals else 0.0

    @property
    def suspicious(self) -> bool:
        """오프셋이 음수로 크면 로컬 시계가 뒤처진 것. NTP 점검이 필요하다."""
        return self.offset_us < -1000.0           # 1ms 이상 뒤처짐


class ClockMonitor:
    """venue 여러 개를 묶어 관리하고 /healthz 에 노출한다."""

    def __init__(self, window_s: float = 300.0):
        self.window_s = window_s
        self._est: dict[str, SkewEstimator] = {}

    def observe(self, venue: str, raw_us: float) -> float:
        est = self._est.get(venue)
        if est is None:
            est = self._est[venue] = SkewEstimator(self.window_s)
        return est.observe(raw_us)

    def report(self) -> dict:
        return {
            v: {
                "offset_us": round(e.offset_us, 1),
                "samples": e.samples,
                "local_clock_behind": e.suspicious,
            }
            for v, e in sorted(self._est.items())
        }

    def any_suspicious(self) -> bool:
        return any(e.suspicious for e in self._est.values())
