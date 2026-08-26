"""백테스트 엔진 — 수집한 피드 데이터로 전략을 검증한다.

실시간 엔진과 **같은 전략 클래스**(src/mdfeed/strategies.py)를 쓴다. 백테스트만
따로 구현하면 두 코드가 미묘하게 갈리고, 그 차이는 실거래에서만 드러난다.

정직하게 만들려고 넣은 장치들
-----------------------------
1. **미래 참조 차단**: t 시점 봉이 닫힌 뒤 나온 시그널은 **t+1 봉의 시가**로 체결한다.
   같은 봉 종가로 체결하면 "종가를 보고 종가에 산" 것이 되어 성과가 부풀려진다.
   백테스트가 실전보다 좋게 나오는 가장 흔한 원인이다.
2. **수수료 + 슬리피지**: 왕복 비용을 빼지 않은 수익률은 의미가 없다. 기본값은
   업비트 원화마켓 수수료(0.05%)와 슬리피지 5bp.
3. **전량 매수/매도만**: 부분 체결·레버리지·증거금을 모델링하지 않는다. 못 하는 걸
   한 척하느니 범위를 좁히고 그 사실을 적는다.
4. **결과는 있는 그대로**: 손실이 나면 손실로 보고한다. 파라미터를 성과가 나올
   때까지 돌려 고르는 것(과최적화)이 백테스트를 쓸모없게 만드는 주범이다.

성과 지표
---------
총수익률 · CAGR · 최대낙폭(MDD) · 샤프 · 승률 · 손익비 · 거래횟수 ·
Buy&Hold 대비 초과수익. 샤프는 봉 주기에 맞춰 연율화한다.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdfeed.strategies import BUY, HOLD, SELL, REGISTRY  # noqa: E402

FEE_RATE = 0.0005          # 편도 0.05%
SLIPPAGE_BP = 5.0          # 5bp


@dataclass
class BarRow:
    """백테스트용 최소 봉. models.Bar 와 필드 이름을 맞춰 전략이 그대로 먹는다."""
    bucket_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    tick_count: int = 0


@dataclass
class Fill:
    ts_ns: int
    side: str
    price: float
    qty: float
    fee: float
    equity: float


@dataclass
class Result:
    strategy: str
    symbol: str
    bars: int
    start_equity: float
    end_equity: float
    fills: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)

    @property
    def total_return(self) -> float:
        return self.end_equity / self.start_equity - 1.0

    @property
    def trades(self) -> int:
        return len(self.fills) // 2

    def max_drawdown(self) -> float:
        peak, mdd = -math.inf, 0.0
        for _ts, eq in self.equity_curve:
            peak = max(peak, eq)
            if peak > 0:
                mdd = min(mdd, eq / peak - 1.0)
        return mdd

    def sharpe(self, bar_seconds: int = 60) -> float:
        """봉 수익률 기준 연율화 샤프. 무위험수익률 0 가정."""
        eq = [e for _t, e in self.equity_curve]
        if len(eq) < 3:
            return 0.0
        rets = [eq[i] / eq[i - 1] - 1.0 for i in range(1, len(eq)) if eq[i - 1] > 0]
        if len(rets) < 2:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        sd = math.sqrt(var)
        if sd == 0:
            return 0.0
        periods_per_year = 365 * 24 * 3600 / bar_seconds     # 암호화폐는 24/7
        return mean / sd * math.sqrt(periods_per_year)

    def win_rate(self) -> float:
        wins = total = 0
        entry = None
        for f in self.fills:
            if f.side == "BUY":
                entry = f
            elif entry is not None:
                total += 1
                if f.price > entry.price:
                    wins += 1
                entry = None
        return wins / total if total else 0.0

    def profit_factor(self) -> float:
        gains = losses = 0.0
        entry = None
        for f in self.fills:
            if f.side == "BUY":
                entry = f
            elif entry is not None:
                pnl = (f.price - entry.price) * f.qty
                if pnl >= 0:
                    gains += pnl
                else:
                    losses -= pnl
                entry = None
        return gains / losses if losses > 0 else (math.inf if gains > 0 else 0.0)

    def summary(self, bar_seconds: int = 60) -> dict:
        return {
            "strategy": self.strategy,
            "symbol": self.symbol,
            "bars": self.bars,
            "trades": self.trades,
            "total_return_pct": round(self.total_return * 100, 3),
            "max_drawdown_pct": round(self.max_drawdown() * 100, 3),
            "sharpe": round(self.sharpe(bar_seconds), 3),
            "win_rate_pct": round(self.win_rate() * 100, 1),
            "profit_factor": (round(self.profit_factor(), 3)
                              if math.isfinite(self.profit_factor()) else None),
            "start_equity": round(self.start_equity, 2),
            "end_equity": round(self.end_equity, 2),
        }


def run(bars: list[BarRow], strategy_name: str, symbol: str = "",
        equity: float = 1_000_000.0, fee: float = FEE_RATE,
        slippage_bp: float = SLIPPAGE_BP, **params) -> Result:
    """봉 리스트에 전략 하나를 돌린다.

    체결 규칙: t 봉 종가로 판단 → **t+1 봉 시가**로 체결.
    """
    cls = REGISTRY.get(strategy_name)
    if cls is None:
        raise ValueError(f"알 수 없는 전략: {strategy_name}")
    strat = cls(**params) if params else cls()

    res = Result(strategy_name, symbol, len(bars), equity, equity)
    cash, position = equity, 0.0
    pending = HOLD
    slip = slippage_bp / 10_000.0

    for i, bar in enumerate(bars):
        # ── 직전 봉에서 나온 시그널을 이번 봉 시가로 체결 ────────────────
        if pending == BUY and position == 0.0:
            px = bar.open * (1 + slip)              # 매수는 불리한 쪽으로 밀린다
            qty = cash / (px * (1 + fee))
            cost = qty * px
            f = cost * fee
            cash -= cost + f
            position = qty
            res.fills.append(Fill(bar.bucket_ns, "BUY", px, qty, f, cash + position * px))
        elif pending == SELL and position > 0.0:
            px = bar.open * (1 - slip)
            proceeds = position * px
            f = proceeds * fee
            cash += proceeds - f
            res.fills.append(Fill(bar.bucket_ns, "SELL", px, position, f, cash))
            position = 0.0
        pending = HOLD

        # ── 이번 봉으로 지표 갱신 → 다음 봉에 체결할 시그널 ──────────────
        sig = strat.on_bar(bar)
        if sig != HOLD and i < len(bars) - 1:
            pending = sig

        res.equity_curve.append((bar.bucket_ns, cash + position * bar.close))

    # 마지막 봉 종가로 청산해 성과를 확정한다
    if position > 0.0:
        px = bars[-1].close * (1 - slip)
        proceeds = position * px
        f = proceeds * fee
        cash += proceeds - f
        res.fills.append(Fill(bars[-1].bucket_ns, "SELL", px, position, f, cash))
        position = 0.0
    res.end_equity = cash
    return res


def buy_and_hold(bars: list[BarRow], equity: float = 1_000_000.0,
                 fee: float = FEE_RATE) -> float:
    """비교 기준선. 전략이 이걸 못 이기면 존재 이유가 없다."""
    if len(bars) < 2:
        return equity
    qty = equity / (bars[0].open * (1 + fee))
    return qty * bars[-1].close * (1 - fee)


def load_bars_from_db(db_path: str, venue: str, symbol: str,
                      limit: int = 100_000) -> list[BarRow]:
    """writer 가 적재한 1분봉을 그대로 읽는다. 별도 데이터 준비 과정이 없다."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT bucket, open, high, low, close, volume, tick_count FROM bars_1m "
        "WHERE venue=? AND symbol=? ORDER BY bucket ASC LIMIT ?",
        (venue, symbol, limit)).fetchall()
    conn.close()
    return [BarRow(r["bucket"] * 1000, r["open"], r["high"], r["low"],
                   r["close"], r["volume"], r["tick_count"]) for r in rows]


def load_bars_from_replay(path: str, venue: str, symbol: str,
                          interval_s: int = 60) -> list[BarRow]:
    """녹화 파일에서 직접 봉을 만든다 (DB 없이도 백테스트가 돈다)."""
    from mdfeed.models import MSG_TRADE, Bar, Trade
    from mdfeed.protocol import FrameParser

    parser = FrameParser()
    bars: dict[int, Bar] = {}
    iv_ns = interval_s * 1_000_000_000
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            for f in parser.feed(chunk):
                if f.msg_type != MSG_TRADE or len(f.payload) < Trade.SIZE:
                    continue
                t = Trade.unpack(f.payload)
                if t.venue != venue or t.symbol != symbol:
                    continue
                b = (t.ts_event_ns // iv_ns) * iv_ns
                bar = bars.get(b)
                if bar is None:
                    bar = bars[b] = Bar(venue, symbol, b, interval_s)
                bar.update(t)
    return [BarRow(b.bucket_ns, b.open, b.high, b.low, b.close, b.volume, b.tick_count)
            for b in sorted(bars.values(), key=lambda x: x.bucket_ns)]
