"""전략 정의 — 실시간 엔진과 백테스트가 공유하는 단일 구현.

각 전략은 심볼 하나당 인스턴스 하나를 갖고, 봉이 닫힐 때마다 `on_bar(bar)` 를
받아 -1(매도) / 0(관망) / +1(매수) 을 돌려준다. 상태는 전부 인스턴스 안에 있으므로
같은 클래스를 실시간 루프에 꽂든 과거 봉 리스트에 꽂든 결과가 동일하다.

의도적으로 단순하게 둔 이유
---------------------------
이 프로젝트가 증명하려는 것은 "알파를 찾았다"가 아니라 **피드 데이터를 실시간
지표 → 시그널 → 저장 → 백테스트로 흘려보내는 파이프라인이 일관되게 동작한다**는
것이다. 전략이 화려할수록 그 검증은 오히려 흐려진다.
백테스트 결과는 수수료·슬리피지를 포함해 그대로 보고하며, 손실이 나면 손실로 적는다.
"""

from __future__ import annotations

from .indicators import ATR, RSI, SMA, Bollinger, Crossover

BUY, SELL, HOLD = 1, -1, 0


class SignalGate:
    """같은 (종목, 전략)의 시그널을 최소 간격 이상 벌린다.

    **시장 시각(봉의 버킷)으로 잰다. 벽시계가 아니다.**

    벽시계로 재면 같은 테이프를 다시 흘려도 결과가 달라진다. 평시에는
    봉 간격(60초)이 쿨다운(30초)보다 길어 거의 안 걸리지만, 재생이나
    밀린 구간을 따라잡을 때는 봉이 몇 ms 간격으로 닫혀 **전부 억제된다.**
    정확히 그때가 백테스트와 실시간을 비교하는 순간이다.

    이 게이트를 실시간 엔진과 백테스트가 **같이 쓴다.** 예전엔 실시간에만
    있어서, 백테스트가 보고하는 성과가 배포된 시스템의 성과가 아니었다.
    지표·전략 클래스를 공유해 놓고 그 뒤에 붙은 규칙이 갈리면 공유한 의미가 없다.
    """

    def __init__(self, cooldown_s: float = 0.0):
        self.cooldown_s = float(cooldown_s)
        self.suppressed = 0
        self._last: dict[tuple, int] = {}

    def allow(self, key: str, strategy: str, bar_ts_ns: int) -> bool:
        if self.cooldown_s <= 0:
            return True
        ck = (key, strategy)
        prev = self._last.get(ck)
        if prev is not None and (bar_ts_ns - prev) < self.cooldown_s * 1e9:
            self.suppressed += 1
            return False
        self._last[ck] = bar_ts_ns
        return True


class Strategy:
    name = "base"

    def on_bar(self, bar) -> int:
        raise NotImplementedError

    def state(self) -> dict:
        return {}

    @property
    def ready(self) -> bool:
        return True


class SMACross(Strategy):
    """단기/장기 이동평균 교차 — 추세추종의 표준 baseline."""

    name = "sma_cross"

    def __init__(self, fast: int = 10, slow: int = 30):
        self.fast_n, self.slow_n = fast, slow
        self._fast, self._slow = SMA(fast), SMA(slow)
        self._cross = Crossover()

    def on_bar(self, bar) -> int:
        f = self._fast.update(bar.close)
        s = self._slow.update(bar.close)
        if f is None or s is None:
            return HOLD
        c = self._cross.update(f, s)
        return BUY if c > 0 else (SELL if c < 0 else HOLD)

    @property
    def ready(self) -> bool:
        return self._slow.ready

    def state(self) -> dict:
        return {"fast": self._fast.value, "slow": self._slow.value,
                "params": {"fast": self.fast_n, "slow": self.slow_n}}


class RSIRevert(Strategy):
    """RSI 평균회귀 — 과매도에서 매수, 과매수에서 매도.

    임계선을 '넘은 상태'가 아니라 '넘어갔다가 되돌아오는 순간'에 반응한다.
    전자는 추세장에서 계속 물리는 전형적인 실패 패턴이다.
    """

    name = "rsi_revert"

    def __init__(self, n: int = 14, low: float = 30.0, high: float = 70.0):
        self.n, self.low, self.high = n, low, high
        self._rsi = RSI(n)
        self._was_low = False
        self._was_high = False

    def on_bar(self, bar) -> int:
        v = self._rsi.update(bar.close)
        if v is None:
            return HOLD
        sig = HOLD
        if self._was_low and v > self.low:
            sig = BUY                     # 과매도에서 빠져나오는 순간
        elif self._was_high and v < self.high:
            sig = SELL                    # 과매수에서 꺾이는 순간
        self._was_low = v <= self.low
        self._was_high = v >= self.high
        return sig

    @property
    def ready(self) -> bool:
        return self._rsi.ready

    def state(self) -> dict:
        return {"rsi": self._rsi.value,
                "params": {"n": self.n, "low": self.low, "high": self.high}}


class BollingerBreak(Strategy):
    """볼린저 밴드 돌파 — 변동성 확장 구간의 추세 진입."""

    name = "bb_break"

    def __init__(self, n: int = 20, k: float = 2.0):
        self.n, self.k = n, k
        self._bb = Bollinger(n, k)
        self._inside = True

    def on_bar(self, bar) -> int:
        v = self._bb.update(bar.close)
        if v is None:
            return HOLD
        _mid, up, lo = v
        sig = HOLD
        if self._inside and bar.close > up:
            sig = BUY
        elif self._inside and bar.close < lo:
            sig = SELL
        self._inside = lo <= bar.close <= up
        return sig

    @property
    def ready(self) -> bool:
        return self._bb.ready

    def state(self) -> dict:
        v = self._bb.value
        return {"mid": v[0] if v else None, "upper": v[1] if v else None,
                "lower": v[2] if v else None, "params": {"n": self.n, "k": self.k}}


REGISTRY = {
    "sma_cross": SMACross,
    "rsi_revert": RSIRevert,
    "bb_break": BollingerBreak,
}


def build(names, **kwargs) -> list[Strategy]:
    out = []
    for n in names:
        cls = REGISTRY.get(n.strip())
        if cls:
            out.append(cls())
    return out
