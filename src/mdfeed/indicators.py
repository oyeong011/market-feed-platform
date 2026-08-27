"""증분(streaming) 기술지표 — 실시간 엔진과 백테스트가 **같은 코드**를 쓴다.

왜 증분인가
-----------
1. 실시간 엔진이 틱마다 pandas 로 전체 시리즈를 다시 계산하면 O(N) × 틱 수가 되어
   금방 못 따라간다. 여기 있는 것들은 전부 갱신당 O(1)이다.
2. 더 중요한 이유: **백테스트와 실서비스의 로직 불일치를 구조적으로 막는다.**
   백테스트는 pandas 로, 실시간은 손으로 짠 루프로 계산하면 미묘하게 달라지고,
   그 차이는 실거래에서만 드러난다. 같은 클래스를 양쪽이 쓰면 애초에 갈릴 수 없다.

미래 참조(look-ahead) 방지
--------------------------
모든 지표는 `update(x)` 가 **x 를 포함한 시점까지의 값**을 돌려준다. 시그널을
만들 때는 "직전 봉까지의 지표"로 판단하고 "현재 봉 종가"로 체결하는 규칙을
백테스트 쪽에서 강제한다 (quant/backtest.py 참고).

pandas-ta / TA-Lib 호환성
-------------------------
SMA·EMA·RSI(Wilder)·MACD·볼린저는 그 라이브러리들과 같은 정의를 따랐다.
tests/test_indicators.py 에서 수치를 직접 대조한다.
"""

from __future__ import annotations

import math
from collections import deque


class SMA:
    """단순이동평균. deque 합을 굴려 O(1)."""

    __slots__ = ("n", "_q", "_sum")

    def __init__(self, n: int):
        self.n = n
        self._q: deque[float] = deque(maxlen=n)
        self._sum = 0.0

    def update(self, x: float) -> float | None:
        if len(self._q) == self.n:
            self._sum -= self._q[0]
        self._q.append(x)
        self._sum += x
        return self.value

    @property
    def ready(self) -> bool:
        return len(self._q) == self.n

    @property
    def value(self) -> float | None:
        return self._sum / self.n if self.ready else None


class EMA:
    """지수이동평균. 첫 값은 SMA 로 seeding 한다 (TA-Lib 관행)."""

    __slots__ = ("n", "k", "_v", "_seed", "_count")

    def __init__(self, n: int):
        self.n = n
        self.k = 2.0 / (n + 1.0)
        self._v: float | None = None
        self._seed = 0.0
        self._count = 0

    def update(self, x: float) -> float | None:
        self._count += 1
        if self._v is None:
            self._seed += x
            if self._count == self.n:
                self._v = self._seed / self.n
            return self._v
        self._v += self.k * (x - self._v)
        return self._v

    @property
    def ready(self) -> bool:
        return self._v is not None

    @property
    def value(self) -> float | None:
        return self._v


class RSI:
    """Wilder RSI. 평활계수 1/n 을 쓰는 원본 정의를 따른다(EMA 2/(n+1)가 아니다)."""

    __slots__ = ("n", "_prev", "_avg_gain", "_avg_loss", "_count", "_gain_sum", "_loss_sum")

    def __init__(self, n: int = 14):
        self.n = n
        self._prev: float | None = None
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._count = 0
        self._gain_sum = 0.0
        self._loss_sum = 0.0

    def update(self, x: float) -> float | None:
        if self._prev is None:
            self._prev = x
            return None
        change = x - self._prev
        self._prev = x
        gain, loss = max(change, 0.0), max(-change, 0.0)

        if self._avg_gain is None:
            self._count += 1
            self._gain_sum += gain
            self._loss_sum += loss
            if self._count < self.n:
                return None
            self._avg_gain = self._gain_sum / self.n
            self._avg_loss = self._loss_sum / self.n
        else:
            self._avg_gain = (self._avg_gain * (self.n - 1) + gain) / self.n
            self._avg_loss = (self._avg_loss * (self.n - 1) + loss) / self.n
        return self.value

    @property
    def ready(self) -> bool:
        return self._avg_gain is not None

    @property
    def value(self) -> float | None:
        if self._avg_gain is None:
            return None
        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


class RollingStd:
    """표본표준편차. Welford 대신 창 전체를 들고 재계산하는 대신,
    합/제곱합을 굴려 O(1)로 계산한다. 부동소수 누적오차를 줄이려고
    창이 다 찰 때마다 주기적으로 재계산한다."""

    __slots__ = ("n", "_q", "_sum", "_sq", "_since_recalc")

    def __init__(self, n: int):
        self.n = n
        self._q: deque[float] = deque(maxlen=n)
        self._sum = 0.0
        self._sq = 0.0
        self._since_recalc = 0

    def update(self, x: float) -> float | None:
        if len(self._q) == self.n:
            old = self._q[0]
            self._sum -= old
            self._sq -= old * old
        self._q.append(x)
        self._sum += x
        self._sq += x * x
        self._since_recalc += 1
        if self._since_recalc >= 10_000:          # 누적오차 리셋
            self._sum = math.fsum(self._q)
            self._sq = math.fsum(v * v for v in self._q)
            self._since_recalc = 0
        return self.value

    @property
    def ready(self) -> bool:
        return len(self._q) == self.n

    @property
    def value(self) -> float | None:
        if not self.ready:
            return None
        mean = self._sum / self.n
        var = max(self._sq / self.n - mean * mean, 0.0)
        # 표본분산으로 보정 (pandas .std() 기본값과 맞춘다)
        var *= self.n / (self.n - 1) if self.n > 1 else 1.0
        return math.sqrt(var)


class Bollinger:
    """볼린저 밴드. (중심선, 상단, 하단, %B)"""

    __slots__ = ("n", "k", "_sma", "_std")

    def __init__(self, n: int = 20, k: float = 2.0):
        self.n, self.k = n, k
        self._sma = SMA(n)
        self._std = RollingStd(n)

    def update(self, x: float):
        self._sma.update(x)
        self._std.update(x)
        return self.value

    @property
    def ready(self) -> bool:
        return self._sma.ready and self._std.ready

    @property
    def value(self):
        if not self.ready:
            return None
        m, s = self._sma.value, self._std.value
        up, lo = m + self.k * s, m - self.k * s
        return m, up, lo


class MACD:
    """MACD(12,26,9). (macd, signal, histogram)"""

    __slots__ = ("_fast", "_slow", "_sig")

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self._fast, self._slow, self._sig = EMA(fast), EMA(slow), EMA(signal)

    def update(self, x: float):
        f, s = self._fast.update(x), self._slow.update(x)
        if f is None or s is None:
            return None
        macd = f - s
        sig = self._sig.update(macd)
        if sig is None:
            return None
        return macd, sig, macd - sig

    @property
    def ready(self) -> bool:
        return self._sig.ready


class ATR:
    """Average True Range (Wilder). 변동성 기반 손절폭 산정에 쓴다."""

    __slots__ = ("n", "_prev_close", "_avg", "_sum", "_count")

    def __init__(self, n: int = 14):
        self.n = n
        self._prev_close: float | None = None
        self._avg: float | None = None
        self._sum = 0.0
        self._count = 0

    def update(self, high: float, low: float, close: float) -> float | None:
        if self._prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self._prev_close), abs(low - self._prev_close))
        self._prev_close = close
        if self._avg is None:
            self._count += 1
            self._sum += tr
            if self._count < self.n:
                return None
            self._avg = self._sum / self.n
        else:
            self._avg = (self._avg * (self.n - 1) + tr) / self.n
        return self._avg

    @property
    def ready(self) -> bool:
        return self._avg is not None

    @property
    def value(self) -> float | None:
        return self._avg


class Crossover:
    """두 계열의 교차 감지. 상향돌파 +1, 하향돌파 -1, 그 외 0.

    '지금 a > b' 가 아니라 '직전엔 아니었는데 지금 그렇다' 를 봐야 한다.
    전자로 짜면 조건이 유지되는 내내 매 틱 시그널이 쏟아진다.

    **동률(a == b)은 교차가 아니다.**
    처음엔 `a > b` 하나로 판정했는데, 그러면 두 값이 정확히 같아지는 순간을
    "아래로 내려갔다"로 오인한다. 실제로 삼성전자 10초봉에서 교차 지점이
    pandas 계산과 3곳이나 1봉씩 어긋났다 — 국내 주식은 호가 단위가 커서
    두 이동평균이 정확히 일치하는 일이 생기지만, 크립토 float 에서는 거의
    일어나지 않아 오랫동안 드러나지 않았다.

    동률일 때는 판정을 보류하고 직전 상태를 유지한다. 값이 실제로 한쪽으로
    갈라진 뒤에야 교차로 인정한다.
    """

    __slots__ = ("_prev",)

    def __init__(self):
        self._prev: bool | None = None

    def update(self, a: float, b: float) -> int:
        if a == b:
            return 0            # 접촉은 교차가 아니다. 상태를 그대로 둔다
        cur = a > b
        prev, self._prev = self._prev, cur
        if prev is None or prev == cur:
            return 0
        return 1 if cur else -1
