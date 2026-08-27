"""백테스트 실행기 — DB 또는 녹화 파일에서 봉을 읽어 전 전략을 돌리고 JSON 을 낸다.

산출 JSON 은 GitHub Pages 대시보드가 그대로 읽는다. 결과를 손으로 옮겨 적지 않는
것이 중요하다 — 사람이 옮기는 순간 문서와 실제가 갈라진다.

    python quant/run_backtest.py --source replay --symbol BINANCE:BTCUSDT
    python quant/run_backtest.py --source db --symbol UPBIT:KRW-BTC --out docs/data/backtest.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import backtest as bt                                            # noqa: E402
from mdfeed.strategies import REGISTRY                           # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser("run_backtest")
    ap.add_argument("--source", choices=["db", "replay"], default="replay")
    ap.add_argument("--db", default="data/mdfeed.db")
    ap.add_argument("--replay", default="data/replay/sample.mdf")
    ap.add_argument("--symbol", default="BINANCE:BTCUSDT", help="VENUE:SYMBOL")
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--equity", type=float, default=1_000_000)
    ap.add_argument("--fee", type=float, default=bt.FEE_RATE)
    ap.add_argument("--slippage-bp", type=float, default=bt.SLIPPAGE_BP)
    ap.add_argument("--strategies", nargs="*", default=list(REGISTRY))
    ap.add_argument("--cross-check", action="store_true",
                    help="vectorbt / backtesting.py 로 교차검증")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    venue, _, symbol = args.symbol.partition(":")
    if args.source == "db":
        bars = bt.load_bars_from_db(args.db, venue, symbol)
        src = f"{args.db} (bars_1m)"
    else:
        bars = bt.load_bars_from_replay(args.replay, venue, symbol, args.interval)
        src = f"{args.replay} (녹화 리플레이)"

    if len(bars) < 5:
        print(f"봉이 부족합니다 ({len(bars)}개). 먼저 데이터를 수집하세요:\n"
              f"  make record   # 또는  make up 으로 스택을 띄워 두기")
        return 1

    span_min = (bars[-1].bucket_ns - bars[0].bucket_ns) / 6e10
    print(f"데이터: {src}")
    print(f"대상  : {args.symbol}  |  {len(bars)}봉 ({args.interval}초봉, "
          f"약 {span_min:.0f}분 구간)  |  틱 {sum(b.tick_count for b in bars):,}건")
    print(f"비용  : 수수료 {args.fee*100:.3f}% (편도) + 슬리피지 {args.slippage_bp:.1f}bp\n")

    bh_equity = bt.buy_and_hold(bars, args.equity, args.fee)
    bh_ret = (bh_equity / args.equity - 1) * 100

    print(f"{'전략':<14} {'거래':>5} {'수익률':>9} {'MDD':>8} {'샤프':>8} "
          f"{'승률':>7} {'손익비':>7} {'B&H대비':>9}")
    print("-" * 76)
    results = []
    for name in args.strategies:
        if name not in REGISTRY:
            continue
        r = bt.run(bars, name, args.symbol, args.equity, args.fee, args.slippage_bp)
        s = r.summary(args.interval)
        s["vs_buy_hold_pp"] = round(s["total_return_pct"] - bh_ret, 3)
        results.append(s)
        pf = f"{s['profit_factor']:.2f}" if s["profit_factor"] is not None else "  -"
        print(f"{name:<14} {s['trades']:>5} {s['total_return_pct']:>8.3f}% "
              f"{s['max_drawdown_pct']:>7.2f}% {s['sharpe']:>8.2f} "
              f"{s['win_rate_pct']:>6.1f}% {pf:>7} {s['vs_buy_hold_pp']:>8.3f}p")
    print("-" * 76)
    print(f"{'buy & hold':<14} {1:>5} {bh_ret:>8.3f}%")

    payload = {
        "symbol": args.symbol, "source": src, "bars": len(bars),
        "interval_s": args.interval, "span_minutes": round(span_min, 1),
        "ticks": sum(b.tick_count for b in bars),
        "fee_rate": args.fee, "slippage_bp": args.slippage_bp,
        "buy_hold_return_pct": round(bh_ret, 3),
        "results": results,
        "first_bar_ns": bars[0].bucket_ns, "last_bar_ns": bars[-1].bucket_ns,
    }

    if args.cross_check:
        import integrations
        best = max(results, key=lambda r: r["total_return_pct"]) if results else {}
        print("\n=== 오픈소스 라이브러리 교차검증 ===")
        cc = integrations.cross_check(bars, best, symbol=args.symbol, equity=args.equity)
        payload["cross_check"] = cc
        print(json.dumps(cc, ensure_ascii=False, indent=1, default=str))

    print("\n주의: 짧은 구간·단일 종목 결과는 통계적 유의성이 없습니다. "
          "이 수치는 파이프라인이 끝까지 동작한다는 증거이지 전략의 우열이 아닙니다.")

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
        print(f"\n결과 저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
