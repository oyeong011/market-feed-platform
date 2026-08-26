"""정규화 마켓데이터 스키마 + 고정폭 바이너리 직렬화.

FEED 서비스의 첫 번째 책임은 "거래소마다 제각각인 원본 메시지를 하나의
스키마로 정규화" 하는 것이다. 이 모듈이 그 단일 진실 원천(SSOT)이며,
수집기·게이트웨이·적재기·전략엔진이 모두 여기 정의된 구조체만 주고받는다.

설계 판단
---------
* 시각은 전부 epoch nanosecond(uint64). float 초 단위는 1e-7 수준에서
  정밀도가 깨져 지연시간 p99 측정이 무의미해진다.
* 가격/수량은 float64. 암호화폐는 8자리 소수가 흔해 int64 tick 표현으로
  바꾸려면 거래소별 tick size 테이블이 필요한데, 이 프로젝트 범위에서는
  과설계라고 판단했다. (DESIGN.md "정밀도" 절에 근거 기록)
* 페이로드는 고정폭 struct. 파싱에 분기가 없어 hot path에서 예측 가능한
  비용을 갖고, 공유메모리 링버퍼에 그대로 얹을 수 있다.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass, asdict
from typing import ClassVar

# ── 메시지 종류 ────────────────────────────────────────────────────────────
MSG_HEARTBEAT = 1
MSG_TRADE = 2
MSG_BOOK = 3
MSG_SIGNAL = 4
MSG_SNAPSHOT = 5
MSG_SUBSCRIBE = 6
MSG_ACK = 7

MSG_NAMES = {
    MSG_HEARTBEAT: "HEARTBEAT",
    MSG_TRADE: "TRADE",
    MSG_BOOK: "BOOK",
    MSG_SIGNAL: "SIGNAL",
    MSG_SNAPSHOT: "SNAPSHOT",
    MSG_SUBSCRIBE: "SUBSCRIBE",
    MSG_ACK: "ACK",
}

# ── 체결 방향 ──────────────────────────────────────────────────────────────
SIDE_UNKNOWN = 0
SIDE_BUY = 1
SIDE_SELL = 2
SIDE_NAMES = {SIDE_UNKNOWN: "UNKNOWN", SIDE_BUY: "BUY", SIDE_SELL: "SELL"}


def now_ns() -> int:
    """벽시계 기준 epoch nanosecond.

    지연시간 계산은 거래소가 찍은 ts_event(벽시계)와 비교해야 하므로
    monotonic이 아니라 time.time_ns()를 쓴다.
    """
    return time.time_ns()


def _fix(s: str, n: int) -> bytes:
    """문자열을 n바이트 고정폭으로. 초과분은 자른다(정규화 심볼은 16자 이내)."""
    return s.encode("ascii", "ignore")[:n].ljust(n, b"\x00")


def _unfix(b: bytes) -> str:
    return b.rstrip(b"\x00").decode("ascii", "ignore")


@dataclass(slots=True)
class Trade:
    """체결(틱). FEED의 기본 단위."""

    venue: str          # 거래소 코드 (UPBIT / BINANCE / KIS)
    symbol: str         # 정규화 심볼 (BTC-KRW, BTCUSDT, 005930)
    ts_event_ns: int    # 거래소가 찍은 체결 시각
    ts_recv_ns: int     # 우리 프로세스가 소켓에서 읽어낸 시각
    price: float
    qty: float
    side: int = SIDE_UNKNOWN

    # 16 + 8 + 8 + 8 + 8 + 8 + 1 + 7패딩 = 64B (캐시라인 정렬)
    FMT: ClassVar[str] = "!16s8sQQddB7x"
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self) -> bytes:
        return struct.pack(
            self.FMT, _fix(self.symbol, 16), _fix(self.venue, 8),
            self.ts_event_ns, self.ts_recv_ns, self.price, self.qty, self.side,
        )

    @classmethod
    def unpack(cls, buf: bytes) -> "Trade":
        sym, ven, te, tr, px, qty, side = struct.unpack(cls.FMT, buf[: cls.SIZE])
        return cls(_unfix(ven), _unfix(sym), te, tr, px, qty, side)

    @property
    def latency_us(self) -> float:
        """거래소 체결 시각 → 수신 시각 지연(마이크로초)."""
        return (self.ts_recv_ns - self.ts_event_ns) / 1_000.0

    @property
    def notional(self) -> float:
        return self.price * self.qty

    def to_dict(self) -> dict:
        d = asdict(self)
        d["side_name"] = SIDE_NAMES.get(self.side, "UNKNOWN")
        d["latency_us"] = round(self.latency_us, 1)
        return d


@dataclass(slots=True)
class BookTop:
    """호가 최우선(BBO). 전체 depth는 이 프로젝트 범위 밖."""

    venue: str
    symbol: str
    ts_event_ns: int
    ts_recv_ns: int
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float

    FMT: ClassVar[str] = "!16s8sQQdddd"
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self) -> bytes:
        return struct.pack(
            self.FMT, _fix(self.symbol, 16), _fix(self.venue, 8),
            self.ts_event_ns, self.ts_recv_ns,
            self.bid, self.bid_qty, self.ask, self.ask_qty,
        )

    @classmethod
    def unpack(cls, buf: bytes) -> "BookTop":
        sym, ven, te, tr, b, bq, a, aq = struct.unpack(cls.FMT, buf[: cls.SIZE])
        return cls(_unfix(ven), _unfix(sym), te, tr, b, bq, a, aq)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0 if self.bid and self.ask else 0.0

    @property
    def spread_bp(self) -> float:
        """스프레드(basis point). 유동성 품질 지표이자 데이터 이상 탐지에 쓴다."""
        m = self.mid
        return ((self.ask - self.bid) / m * 10_000.0) if m else 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["mid"] = self.mid
        d["spread_bp"] = round(self.spread_bp, 2)
        return d


@dataclass(slots=True)
class Signal:
    """전략엔진이 피드 위에 얹어 발행하는 매매 신호."""

    venue: str
    symbol: str
    ts_ns: int
    strategy: str
    action: int      # 1 매수, -1 매도(=2로 인코딩), 0 관망
    strength: float  # 0.0 ~ 1.0
    ref_price: float

    FMT: ClassVar[str] = "!16s8s16sQbdd7x"
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self) -> bytes:
        return struct.pack(
            self.FMT, _fix(self.symbol, 16), _fix(self.venue, 8),
            _fix(self.strategy, 16), self.ts_ns,
            self.action, self.strength, self.ref_price,
        )

    @classmethod
    def unpack(cls, buf: bytes) -> "Signal":
        sym, ven, strat, ts, act, stg, px = struct.unpack(cls.FMT, buf[: cls.SIZE])
        return cls(_unfix(ven), _unfix(sym), ts, _unfix(strat), act, stg, px)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["action_name"] = {1: "BUY", -1: "SELL", 0: "HOLD"}.get(self.action, "HOLD")
        return d


@dataclass(slots=True)
class Bar:
    """OHLCV 집계 바. writer가 틱을 접어 만들고 DB/백테스트가 소비한다."""

    venue: str
    symbol: str
    bucket_ns: int      # 버킷 시작 시각
    interval_s: int
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    notional: float = 0.0
    tick_count: int = 0

    def update(self, t: Trade) -> None:
        if self.tick_count == 0:
            self.open = self.high = self.low = t.price
        self.high = max(self.high, t.price)
        self.low = min(self.low, t.price)
        self.close = t.price
        self.volume += t.qty
        self.notional += t.notional
        self.tick_count += 1

    @property
    def vwap(self) -> float:
        return self.notional / self.volume if self.volume else self.close

    def to_dict(self) -> dict:
        d = asdict(self)
        d["vwap"] = self.vwap
        return d


PAYLOAD_TYPES = {MSG_TRADE: Trade, MSG_BOOK: BookTop, MSG_SIGNAL: Signal}
