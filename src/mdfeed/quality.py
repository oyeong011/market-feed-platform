"""데이터 품질 검사 — 이 프로젝트의 첫 번째 원칙을 실제로 집행하는 곳.

README 첫 줄에 이렇게 적어 두었다.

    틀린 값을 조용히 배포하는 것이 값이 없는 것보다 나쁘다.
    아무 알람도 안 울린다.

그런데 그걸 **탐지하는 장치가 없었다.** 원칙만 있고 집행이 없으면 원칙이 아니다.
이 모듈이 그 집행부다.

값이 없는 것은 금방 안다 — 그래프가 멈추고, 헬스체크가 빨개지고, 전화가 온다.
값이 틀린 것은 아무 일도 일어나지 않는다. 그래서 **틀림을 능동적으로 찾아야 한다.**

검사 항목과 근거
----------------
| 검사 | 무엇이 틀렸다는 신호인가 |
| --- | --- |
| 가격 점프 | 파싱 오프셋이 밀렸거나 소수점을 잘못 읽었다. 실제 급등락과 구분이 필요하다 |
| 크로스된 호가 | 매수호가 > 매도호가. 정상 시장에서 지속될 수 없다. 필드가 뒤바뀐 신호 |
| 광폭 스프레드 | 유동성 고갈이거나, 한쪽 호가만 갱신되고 있다 |
| 정체 | 같은 값이 계속 온다. 업스트림이 캐시를 주거나 우리가 갱신을 놓치고 있다 |
| OHLC 위반 | low > open 등. 집계 로직 버그. 하류가 전부 오염된다 |
| 교차 시장 괴리 | 같은 자산이 두 거래소에서 크게 벌어짐. 한쪽이 틀렸거나 실제 차익 기회 |

모든 검사는 **참을 거짓이라 하지 않는 쪽**으로 기운다. 오탐이 잦으면 사람이
알람을 무시하게 되고, 그러면 진짜가 왔을 때도 무시한다.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, asdict

# 심각도. CRITICAL 은 "이 값을 쓰면 안 된다", WARNING 은 "봐야 한다".
SEV_WARNING = "WARNING"
SEV_CRITICAL = "CRITICAL"


@dataclass(slots=True)
class QualityEvent:
    ts_ns: int
    check: str
    severity: str
    venue: str
    symbol: str
    detail: str
    value: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


class PriceJumpCheck:
    """직전 체결 대비 급변 탐지.

    단순히 "X% 넘으면 이상"으로 하면 실제 급등락에서 오탐이 쏟아진다.
    그래서 두 가지를 함께 본다.

    1. **절대 임계** — 한 틱에 이만큼 움직이는 건 정상 시장에서 드물다
    2. **최근 변동성 대비** — 평소 0.01% 씩 움직이던 종목이 5% 뛰면 이상하지만,
       평소 3% 씩 흔들리던 종목의 5% 는 그냥 변동성이다

    2번이 오탐을 크게 줄인다. 종목마다 정상 범위가 다르기 때문이다.
    """

    name = "price_jump"

    def __init__(self, abs_pct: float = 10.0, sigma: float = 8.0, window: int = 64,
                 max_gap_s: float = 300.0):
        self.abs_pct = abs_pct
        self.sigma = sigma
        self.window = window
        # 두 틱이 이만큼 떨어져 있으면 "한 틱에" 라고 부를 수 없다. 아래 참고.
        # 60초로 뒀다가 300초로 올렸다 — KRX 의 거래가 뜸한 종목은 장중에도
        # 몇 분씩 체결이 없다. 너무 짧게 잡으면 **정작 큰 움직임이 중요한
        # 비유동 종목에서만** 판정이 꺼진다.
        self.max_gap_s = max_gap_s
        self._last: dict[str, tuple[float, int]] = {}     # 가격, 시각
        self._moves: dict[str, deque] = {}
        self.ref_resets = 0

    def check(self, venue: str, symbol: str, price: float, ts_ns: int) -> QualityEvent | None:
        key = f"{venue}:{symbol}"
        prev = self._last.get(key)
        self._last[key] = (price, ts_ns)
        if prev is None or prev[0] <= 0 or price <= 0:
            return None

        prev_px, prev_ts = prev
        # 경과 시간을 안 보면 "한 틱에 X% 이동"이 사실은 "11시간 만의 첫 틱"일 수 있다.
        #
        # 실측(2026-08-29): upbit 이 11.2시간 멎었다가 돌아온 직후 CRITICAL 2건이
        # 났다. 데이터에는 아무 문제가 없었다 — 기준가가 11시간 전 값이었다.
        # 이 검사는 ts_ns 를 받아 놓고 간격에 쓰지 않아, **스스로 이름 붙인 조건을
        # 확인하지 않고 있었다.**
        #
        # 크립토는 몇 초만 비어도 이상하고, KRX 는 밤새 비는 게 정상이다.
        # 어느 쪽이든 그 간격을 건너뛴 두 값은 "한 틱"이 아니므로 비교하지 않는다.
        # 조용히 넘기면 안 되니 횟수를 세어 지표로 낸다 — 이 값이 튀는 것은
        # 데이터 이상이 아니라 **수집이 끊겼다**는 신호다.
        gap_s = abs(ts_ns - prev_ts) / 1e9
        if gap_s > self.max_gap_s:
            self.ref_resets += 1
            self._moves.pop(key, None)          # 변동성 문맥도 같이 낡았다
            pct = abs(price - prev_px) / prev_px * 100.0
            if pct >= self.abs_pct:
                # 조용히 버리지 않는다. 간격이 벌어졌다고 큰 움직임을 아예
                # 안 보면, 거래가 뜸한 종목의 상한가가 통째로 안 보인다.
                # 다만 "한 틱에"가 아니므로 CRITICAL 은 아니다 — 봐야 하는 값이지
                # 쓰면 안 되는 값이 아니다.
                return QualityEvent(ts_ns, self.name, SEV_WARNING, venue, symbol,
                                    f"{gap_s:,.0f}초 만의 첫 틱에서 {pct:.2f}% 이동 "
                                    f"({prev_px:,.4g} → {price:,.4g}) — "
                                    f"기준가가 낡아 한 틱 판정은 하지 않는다", pct)
            return None

        pct = abs(price - prev_px) / prev_px * 100.0
        moves = self._moves.setdefault(key, deque(maxlen=self.window))

        typical = None
        if len(moves) >= 16:
            ordered = sorted(moves)
            typical = ordered[len(ordered) // 2]      # 중앙값. 평균은 이상치에 끌린다

        moves.append(pct)

        if pct >= self.abs_pct:
            return QualityEvent(ts_ns, self.name, SEV_CRITICAL, venue, symbol,
                                f"한 틱에 {pct:.2f}% 이동 "
                                f"({prev_px:,.4g} → {price:,.4g}, "
                                f"{(ts_ns - prev_ts) / 1e9:.1f}초 간격)", pct)
        if typical and typical > 0 and pct > max(typical * self.sigma, 0.5):
            return QualityEvent(ts_ns, self.name, SEV_WARNING, venue, symbol,
                                f"{pct:.3f}% 이동 — 평소 중앙값 {typical:.3f}% 의 "
                                f"{pct / typical:.0f}배", pct)
        return None


class QuoteSanityCheck:
    """호가 정합성.

    크로스된 호가(매수 > 매도)는 정상 시장에서 지속될 수 없다. 순간적으로
    관측될 수는 있으나, 그보다는 **필드를 뒤바꿔 읽었을 가능성**이 훨씬 높다.
    실제로 이 프로젝트에서 KIS 체결구분을 문서대로 읽었다가 방향이 전부 뒤집혔던
    것과 같은 종류의 오류다.
    """

    name = "quote_sanity"

    def __init__(self, max_spread_bp: float = 1000.0):
        self.max_spread_bp = max_spread_bp

    def check(self, venue: str, symbol: str, bid: float, ask: float,
              ts_ns: int) -> QualityEvent | None:
        if bid <= 0 or ask <= 0:
            return None
        if bid > ask:
            return QualityEvent(ts_ns, self.name, SEV_CRITICAL, venue, symbol,
                                f"크로스된 호가 — 매수 {bid:,.4g} > 매도 {ask:,.4g}. "
                                f"필드가 뒤바뀌었을 가능성",
                                (bid - ask) / ask * 10_000)
        mid = (bid + ask) / 2
        spread_bp = (ask - bid) / mid * 10_000
        if spread_bp > self.max_spread_bp:
            return QualityEvent(ts_ns, self.name, SEV_WARNING, venue, symbol,
                                f"스프레드 {spread_bp:,.0f}bp — 유동성 고갈이거나 "
                                f"한쪽 호가만 갱신 중", spread_bp)
        return None


class StaleValueCheck:
    """상류가 **같은 기록을 반복**하고 있는 상태.

    업스트림이 캐시된 값을 주거나, 우리가 갱신을 놓치고 있다는 신호다.
    **연결은 살아 있고 메시지도 오는데 내용이 안 바뀐다** — 정체 감지가 못 잡는
    종류의 정지다.

    가격만 보면 안 된다 (2026-08-31 실측으로 고침)
    ----------------------------------------------
    예전엔 "같은 가격이 20건 이상 2분 넘게" 로 판정했다. 그 결과 9,700건이
    쌓였는데, 상위가 EURUSDT · BTTCUSDT 처럼 **원래 가격이 잘 안 움직이는**
    종목이었다. 실제로 EURUSDT 의 최장 동일가 구간을 뜯어 보니
    20건이 8.1초에 걸쳐 있고 **체결시각 10개 · 수량 12개가 서로 달랐다.**
    진짜로 별개의 체결이 같은 가격에 난 것이다 — 상류는 멀쩡했다.

    조용한 시장을 상류 고장이라고 부르면, 정작 진짜 고장 때 아무도 안 본다.

    그래서 판정 기준을 **기록의 동일성**으로 바꿨다. 가격뿐 아니라
    체결시각까지 같아야 한다. 서로 다른 체결이 우연히 같은 가격에 나는 건
    정상이지만, **같은 시각·같은 가격이 스무 번 반복되는 건** 상류가 같은
    레코드를 다시 주고 있다는 뜻이다.

    기록이 얼어 있으면 그 시각으로는 경과를 잴 수 없으므로 벽시계를 쓴다.
    (재생·백테스트에서 결정론을 지키려고 now_fn 을 주입할 수 있게 뒀다.)

    호가는 원래 잘 안 바뀌므로 체결가에만 적용한다.
    """

    name = "stale_value"

    def __init__(self, after_s: float = 120.0, min_updates: int = 20,
                 now_fn=None):
        self.after_s = after_s
        self.min_updates = min_updates
        self._now = now_fn or time.time
        self._state: dict[str, tuple] = {}      # (가격, 체결시각), 최초 벽시계, 횟수
        self._fired: set[str] = set()

    def check(self, venue: str, symbol: str, price: float,
              ts_ns: int) -> QualityEvent | None:
        key = f"{venue}:{symbol}"
        ident = (price, ts_ns)                  # 가격만이 아니라 **기록**이다
        now = self._now()
        prev = self._state.get(key)
        if prev is None or prev[0] != ident:
            self._state[key] = (ident, now, 1)
            self._fired.discard(key)
            return None
        _same, since, count = prev
        self._state[key] = (ident, since, count + 1)
        held = now - since
        if held >= self.after_s and count + 1 >= self.min_updates and key not in self._fired:
            self._fired.add(key)                       # 한 번만 알린다
            return QualityEvent(ts_ns, self.name, SEV_WARNING, venue, symbol,
                                f"{held:.0f}초 동안 {count + 1}건이 모두 같은 기록 "
                                f"(가격 {price:,.4g} · 체결시각 동일) — 업스트림이 "
                                f"같은 레코드를 반복하고 있다", held)
        return None


class BarIntegrityCheck:
    """OHLC 관계 위반. 집계 로직 버그이고, 하류가 전부 오염된다."""

    name = "bar_integrity"

    def check(self, venue: str, symbol: str, o: float, h: float, l: float,
              c: float, ts_ns: int) -> QualityEvent | None:
        bad = []
        if l > o:
            bad.append(f"저가 {l:,.4g} > 시가 {o:,.4g}")
        if l > c:
            bad.append(f"저가 {l:,.4g} > 종가 {c:,.4g}")
        if h < o:
            bad.append(f"고가 {h:,.4g} < 시가 {o:,.4g}")
        if h < c:
            bad.append(f"고가 {h:,.4g} < 종가 {c:,.4g}")
        if h < l:
            bad.append(f"고가 {h:,.4g} < 저가 {l:,.4g}")
        if not bad:
            return None
        return QualityEvent(ts_ns, self.name, SEV_CRITICAL, venue, symbol,
                            "OHLC 관계 위반: " + ", ".join(bad))


class CrossVenueCheck:
    """같은 자산이 두 거래소에서 얼마나 벌어져 있는가.

    업비트는 원화, 바이낸스는 달러로 같은 코인을 거래한다. 두 가격의 비율이
    곧 **암묵 환율**이다. 여러 코인에서 뽑은 암묵 환율은 서로 가까워야 한다 —
    벌어지면 둘 중 하나가 틀렸거나 실제 차익 기회다.

    이게 유용한 이유는 **외부 환율 소스 없이 자체 정합성을 검사**할 수 있다는 점이다.
    BTC 로 뽑은 환율과 ETH 로 뽑은 환율이 5% 벌어졌다면, 둘 중 한 종목의 가격이
    이상하다는 뜻이다. 어느 쪽인지는 세 번째 코인이 알려준다.

    부수적으로 이 값 자체가 상품이 된다 — 국내 시장의 프리미엄 지표다.
    """

    name = "cross_venue"

    # (자산, 원화 심볼, 달러 심볼)
    PAIRS = [
        ("BTC", "UPBIT:KRW-BTC", "BINANCE:BTCUSDT"),
        ("ETH", "UPBIT:KRW-ETH", "BINANCE:ETHUSDT"),
        ("SOL", "UPBIT:KRW-SOL", "BINANCE:SOLUSDT"),
        ("XRP", "UPBIT:KRW-XRP", "BINANCE:XRPUSDT"),
    ]

    def __init__(self, divergence_pct: float = 3.0, min_assets: int = 3):
        self.divergence_pct = divergence_pct
        self.min_assets = min_assets
        self.prices: dict[str, float] = {}
        self.implied: dict[str, float] = {}
        # None 으로 두어야 첫 알람이 쿨다운에 잡아먹히지 않는다.
        # 0.0 으로 두면 (now - 0) < 60 인 구간에서 첫 발화가 통째로 사라진다.
        self._last_fire: float | None = None

    def observe(self, venue: str, symbol: str, price: float) -> None:
        self.prices[f"{venue}:{symbol}"] = price

    def implied_fx(self) -> dict[str, float]:
        """자산별 암묵 환율 (KRW per USD)."""
        out = {}
        for asset, krw_sym, usd_sym in self.PAIRS:
            k, u = self.prices.get(krw_sym), self.prices.get(usd_sym)
            if k and u and u > 0:
                out[asset] = k / u
        self.implied = out
        return out

    def check(self, ts_ns: int) -> QualityEvent | None:
        fx = self.implied_fx()
        if len(fx) < self.min_assets:
            return None
        vals = sorted(fx.values())
        mid = vals[len(vals) // 2]
        if mid <= 0:
            return None
        worst_asset, worst_dev = None, 0.0
        for asset, v in fx.items():
            dev = abs(v - mid) / mid * 100.0
            if dev > worst_dev:
                worst_asset, worst_dev = asset, dev
        if worst_dev < self.divergence_pct:
            return None
        now = ts_ns / 1e9
        if self._last_fire is not None and now - self._last_fire < 60:
            return None                              # 알람 폭주 방지
        self._last_fire = now
        detail = " · ".join(f"{a} {v:,.0f}" for a, v in sorted(fx.items()))
        return QualityEvent(ts_ns, self.name, SEV_WARNING, "CROSS", worst_asset or "?",
                            f"암묵 환율이 자산마다 {worst_dev:.2f}% 벌어짐 "
                            f"(중앙값 {mid:,.0f} KRW/USD) — {detail}", worst_dev)


class QualityMonitor:
    """검사기들을 묶어 돌리고 결과를 모은다."""

    def __init__(self, cfg=None):
        g = lambda n, d: float(getattr(cfg, n, d)) if cfg else d      # noqa: E731
        self.jump = PriceJumpCheck(g("qc_jump_abs_pct", 10.0), g("qc_jump_sigma", 8.0),
                                   max_gap_s=g("qc_jump_max_gap_s", 300.0))
        self.quote = QuoteSanityCheck(g("qc_max_spread_bp", 1000.0))
        self.stale = StaleValueCheck(g("qc_stale_after_s", 120.0))
        self.bar = BarIntegrityCheck()
        self.cross = CrossVenueCheck(g("qc_divergence_pct", 3.0))
        self.counts: dict[str, int] = {}
        self.recent: list[dict] = []
        self.checked = 0

    def _record(self, ev: QualityEvent | None) -> QualityEvent | None:
        if ev is None:
            return None
        k = f"{ev.check}:{ev.severity}"
        self.counts[k] = self.counts.get(k, 0) + 1
        self.recent.append(ev.to_dict())
        del self.recent[:-100]
        return ev

    def on_trade(self, venue: str, symbol: str, price: float, ts_ns: int):
        self.checked += 1
        self.cross.observe(venue, symbol, price)
        out = []
        for ev in (self._record(self.jump.check(venue, symbol, price, ts_ns)),
                   self._record(self.stale.check(venue, symbol, price, ts_ns)),
                   self._record(self.cross.check(ts_ns))):
            if ev:
                out.append(ev)
        return out

    def on_quote(self, venue: str, symbol: str, bid: float, ask: float, ts_ns: int):
        self.checked += 1
        ev = self._record(self.quote.check(venue, symbol, bid, ask, ts_ns))
        return [ev] if ev else []

    def on_bar(self, venue: str, symbol: str, o: float, h: float, l: float,
               c: float, ts_ns: int):
        ev = self._record(self.bar.check(venue, symbol, o, h, l, c, ts_ns))
        return [ev] if ev else []

    def report(self) -> dict:
        crit = sum(v for k, v in self.counts.items() if k.endswith(SEV_CRITICAL))
        warn = sum(v for k, v in self.counts.items() if k.endswith(SEV_WARNING))
        return {
            "checked": self.checked,
            "critical": crit,
            "warning": warn,
            "by_check": dict(sorted(self.counts.items())),
            "implied_fx": {k: round(v, 1) for k, v in self.cross.implied.items()},
            # 시세 기준가를 버린 횟수. 이 값이 튀면 데이터 이상이 아니라
            # 수집이 끊겼다는 신호다 — 검사 결과와 다른 축이라 따로 낸다.
            "price_ref_resets": self.jump.ref_resets,
            "recent": list(reversed(self.recent[-20:])),
        }
