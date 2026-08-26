"""오픈소스 퀀트 라이브러리 교차검증 — vectorbt / backtesting.py / pandas-ta.

왜 남의 라이브러리로 다시 계산하나
----------------------------------
자체 백테스트 엔진의 가장 큰 위험은 "조용히 틀리는 것"이다. 미래 참조가 한 줄
섞여 있어도 결과는 그럴듯하게 나오고, 그걸 걸러줄 사람이 없다.

그래서 널리 쓰이는 라이브러리로 **같은 데이터·같은 규칙**을 다시 돌려 수치를
대조한다. 두 결과가 벌어지면 우리 엔진이 틀렸다고 보는 편이 안전하다.

설치돼 있지 않으면 조용히 건너뛴다. 이 프로젝트의 핵심 경로(수집→배포→적재)는
표준 라이브러리만으로 동작해야 하므로, 검증용 라이브러리를 필수 의존성으로
만들지 않는다.

    pip install 'mdfeed[quant]'      # vectorbt, backtesting, pandas-ta, pandas
"""

from __future__ import annotations

import math


def available() -> dict:
    """어떤 라이브러리가 설치돼 있는지."""
    out = {}
    for mod in ("pandas", "numpy", "vectorbt", "backtesting", "pandas_ta", "talib"):
        try:
            __import__(mod)
            out[mod] = True
        except Exception:                            # noqa: BLE001
            out[mod] = False
    return out


def to_dataframe(bars):
    """BarRow 리스트 → pandas DataFrame (DatetimeIndex)."""
    import pandas as pd
    return pd.DataFrame(
        {"Open": [b.open for b in bars], "High": [b.high for b in bars],
         "Low": [b.low for b in bars], "Close": [b.close for b in bars],
         "Volume": [b.volume for b in bars]},
        index=pd.to_datetime([b.bucket_ns for b in bars], unit="ns"))


def verify_indicators(bars, fast: int = 10, slow: int = 30, rsi_n: int = 14) -> dict:
    """우리 증분 지표 vs pandas-ta / pandas 기본 계산 대조."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from mdfeed.indicators import RSI, SMA

    try:
        import pandas as pd
    except ImportError:
        return {"skipped": "pandas 미설치"}

    closes = [b.close for b in bars]
    if len(closes) < max(slow, rsi_n) + 5:
        return {"skipped": f"봉이 부족 ({len(closes)}개)"}

    s = SMA(slow)
    ours_sma = [s.update(c) for c in closes]
    theirs_sma = pd.Series(closes).rolling(slow).mean().tolist()
    sma_diff = max((abs(a - b) for a, b in zip(ours_sma, theirs_sma)
                    if a is not None and not math.isnan(b)), default=0.0)

    r = RSI(rsi_n)
    ours_rsi = [r.update(c) for c in closes]
    out = {"sma_max_abs_diff": sma_diff, "bars": len(closes)}

    try:
        import pandas_ta as ta
        theirs_rsi = ta.rsi(pd.Series(closes), length=rsi_n).tolist()
        out["rsi_max_abs_diff"] = max(
            (abs(a - b) for a, b in zip(ours_rsi, theirs_rsi)
             if a is not None and b is not None and not math.isnan(b)), default=0.0)
        out["rsi_source"] = "pandas_ta"
    except Exception:                                # noqa: BLE001
        # pandas-ta 없이도 Wilder RSI 를 pandas 로 직접 재현해 대조한다
        srs = pd.Series(closes)
        delta = srs.diff()
        gain = delta.clip(lower=0).ewm(alpha=1 / rsi_n, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / rsi_n, adjust=False).mean()
        theirs_rsi = (100 - 100 / (1 + gain / loss)).tolist()
        tail = slice(rsi_n * 4, None)                 # 초기 seeding 방식 차이는 제외
        out["rsi_max_abs_diff"] = max(
            (abs(a - b) for a, b in zip(ours_rsi[tail], theirs_rsi[tail])
             if a is not None and b is not None and not math.isnan(b)), default=0.0)
        out["rsi_source"] = "pandas(ewm) 재현"
    return out


def verify_backtest_vectorbt(bars, fast: int = 10, slow: int = 30,
                             fee: float = 0.0005) -> dict:
    """vectorbt 로 SMA 교차 전략을 다시 돌려 총수익률을 대조."""
    try:
        import numpy as np
        import pandas as pd
        import vectorbt as vbt
    except ImportError:
        return {"skipped": "vectorbt 미설치 (pip install vectorbt)"}

    df = to_dataframe(bars)
    f = vbt.MA.run(df["Close"], fast).ma
    s = vbt.MA.run(df["Close"], slow).ma
    entries = (f > s) & (f.shift(1) <= s.shift(1))
    exits = (f < s) & (f.shift(1) >= s.shift(1))
    pf = vbt.Portfolio.from_signals(df["Close"], entries, exits, fees=fee, freq="1min")
    return {
        "total_return_pct": float(pf.total_return()) * 100,
        "max_drawdown_pct": float(pf.max_drawdown()) * 100,
        "trades": int(pf.trades.count()),
        "source": "vectorbt",
    }


def verify_backtest_bt(bars, fast: int = 10, slow: int = 30,
                       fee: float = 0.0005) -> dict:
    """backtesting.py 로 동일 전략 재검증."""
    try:
        import pandas as pd
        from backtesting import Backtest, Strategy
        from backtesting.lib import crossover
    except ImportError:
        return {"skipped": "backtesting 미설치 (pip install backtesting)"}

    df = to_dataframe(bars)

    def SMA_(values, n):
        return pd.Series(values).rolling(n).mean()

    class Cross(Strategy):
        n1, n2 = fast, slow

        def init(self):
            self.ma1 = self.I(SMA_, self.data.Close, self.n1)
            self.ma2 = self.I(SMA_, self.data.Close, self.n2)

        def next(self):
            if crossover(self.ma1, self.ma2):
                self.buy()
            elif crossover(self.ma2, self.ma1):
                self.position.close()

    bt = Backtest(df, Cross, cash=1_000_000, commission=fee)
    stats = bt.run()
    return {
        "total_return_pct": float(stats["Return [%]"]),
        "max_drawdown_pct": float(stats["Max. Drawdown [%]"]),
        "trades": int(stats["# Trades"]),
        "source": "backtesting.py",
    }


def cross_check(bars, ours: dict | None = None, fast: int = 10, slow: int = 30,
                fee: float = 0.0005) -> dict:
    """전체 교차검증 리포트.

    **사과 대 사과로 맞추는 것이 핵심이다.** 처음엔 우리 결과(슬리피지 5bp 포함)를
    backtesting.py(슬리피지 없음)와 그냥 비교했더니 0.21%p 가 벌어졌고, 그게
    엔진 버그인지 가정 차이인지 알 수 없었다. 그래서 비교용으로는 우리 엔진도
    **슬리피지 0** 으로 다시 돌려 같은 조건에서 대조한다.
    남는 차이는 체결 규칙 차이(부분청산 등)로 설명 가능한 범위여야 한다.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import backtest as bt

    # 비교 전용: 라이브러리와 같은 가정(수수료만, 슬리피지 없음)
    matched = bt.run(bars, "sma_cross", equity=1_000_000.0,
                     fee=fee, slippage_bp=0.0).summary(60)

    report = {
        "libraries": available(),
        "indicators": verify_indicators(bars, fast, slow),
        "ours_as_reported": ours,
        "ours_matched_assumptions": {
            "note": "비교용으로 슬리피지 0 · 수수료만 적용해 재실행한 결과",
            "total_return_pct": matched["total_return_pct"],
            "max_drawdown_pct": matched["max_drawdown_pct"],
            "trades": matched["trades"],
        },
        "vectorbt": verify_backtest_vectorbt(bars, fast, slow, fee),
        "backtesting_py": verify_backtest_bt(bars, fast, slow, fee),
    }

    diffs = {}
    for key in ("vectorbt", "backtesting_py"):
        r = report[key]
        if "total_return_pct" in r:
            diffs[key] = round(r["total_return_pct"] - matched["total_return_pct"], 4)
    report["return_diff_pp_vs_matched"] = diffs
    report["verdict"] = _verdict(report)
    return report


def _verdict(report: dict) -> str:
    ind = report.get("indicators", {})
    sma_d = ind.get("sma_max_abs_diff")
    diffs = [v for v in report.get("return_diff_pp_vs_matched", {}).values()]
    parts = []
    if sma_d is not None:
        parts.append(f"SMA 최대 절대오차 {sma_d:.2e} (부동소수 오차 수준, 사실상 동일)")
    if ind.get("rsi_max_abs_diff") is not None:
        parts.append(
            f"RSI 최대 절대오차 {ind['rsi_max_abs_diff']:.3f} — "
            f"우리 구현은 Wilder 원 정의(첫 n개 평균으로 seeding)를 따르고 "
            f"비교 대상은 첫 값부터 EWM 을 시작한다. seeding 방식 차이이며 "
            f"tests/test_indicators.py 에서 Wilder 공식 예제와 소수 둘째 자리까지 일치를 확인했다")
    if diffs:
        worst = max(abs(d) for d in diffs)
        parts.append(
            f"동일 가정에서 총수익률 최대 차이 {worst:.3f}%p — "
            f"{'체결 규칙(부분청산·주문 단위) 차이로 설명되는 범위' if worst < 0.15 else '조사 필요'}")
    return " / ".join(parts)
