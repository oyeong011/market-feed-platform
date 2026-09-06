"""라이브가 실제로 내보낸 시그널의 모의 체결 장부.

이건 백테스트가 아니다
----------------------
`quant/backtest.py` 는 전략 코드를 **과거 봉에 다시 먹여** 시그널을 재생성한다.
깨끗한 데이터, 빠짐없는 봉, 지연 없는 판단이 전제다.

이 파일은 다르다. **실시간 엔진이 그때 실제로 내보낸 시그널**(`signals` 테이블)을
그대로 가져와 체결시킨다. 그 시그널들은 실전 조건을 겪은 것들이다 — 네트워크 지연,
빠진 봉, 그리고 2026-08-28~09-02 에 네 번 난 업스트림 중단.

둘을 대조하면 **실전 조건이 전략을 얼마나 갉아먹었는지**가 나온다. 백테스트만
보고 "이 전략은 된다"고 말하는 것이 실패하는 지점이 여기다.

체결 규칙
---------
1. **미래 참조 차단.** 시그널은 봉이 닫힌 뒤에 나온다. 그 봉 종가로 체결하면
   "종가를 보고 종가에 산" 것이 된다. **다음 봉의 시가**로만 체결한다.
   시그널에 실린 `ref_price` 는 절대 체결가로 쓰지 않는다 — 그게 바로 그 종가다.
2. **다음 봉이 없으면 체결 못 한다.** 피드가 멎었거나 종목 거래가 끊긴 경우다.
   이걸 조용히 버리면 안 된다. **체결 못 한 시그널 수가 이 장부의 핵심 지표**다.
3. **롱만.** 공매도·레버리지·증거금을 모델링하지 않는다. 매수 시그널로 진입하고
   매도 시그널로 청산한다. 포지션이 있는데 또 매수하면 무시, 없는데 매도해도 무시.
4. **왕복 비용을 뺀다.** 수수료 편도 0.05% + 슬리피지 5bp, 진입·청산 양쪽.
5. **종목당 같은 금액.** 종목마다 자본을 나누면 무엇이 성과를 냈는지 흐려진다.
   모든 거래에 같은 명목금액을 넣고 수익률만 본다. **수익률을 곱하지 않는다** —
   거래가 시간상 겹치므로 순차 복리는 성립하지 않는다.

무엇을 보고하나
---------------
전략별로 거래 수·승률·평균 수익률·누적·최대낙폭, 그리고 **체결 못 한 시그널 수**.
손실이 나면 손실로 보고한다.
"""
from __future__ import annotations

import argparse
import array
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

FEE_RATE = 0.0005            # 편도 0.05%
SLIPPAGE_BP = 5.0            # 5bp
BAR_US = 60 * 1_000_000      # 1분봉
# 시그널 뒤 이 시간 안에 봉이 없으면 체결 실패로 센다. 넉넉히 잡되 무한은 아니다 —
# 하루 뒤에 체결하는 건 그 전략이 의도한 거래가 아니다.
FILL_WINDOW_US = 15 * BAR_US
BUY, SELL = 1, -1

# 매매 대상이 아닌 거래소. 지수는 살 수 없다.
#
# 처음엔 이걸 안 걸렀다. 그랬더니 `KRX-IDX` 가 단일 거래 **+712.7%** 를 냈고
# 총손익의 대부분을 차지했다. 파고들었더니 08-28 오후 4시간 동안 지수 이름이
# 잘려 빈 문자열이 됐던 구간이 있었고(de3c261 에서 고침), 코스닥(830)과
# 코스피(6,874)가 같은 종목으로 뭉쳐 있었다. 장부는 그걸 "830에 사서
# 6,874에 팔았다"로 읽었다.
#
# 데이터 결함은 이미 고쳐진 것이었지만, **애초에 지수를 매매 대상에 넣은 게
# 잘못**이다. 코스피를 살 수는 없다. 결함이 없었어도 이 수치는 무의미했다.
NON_TRADABLE_VENUES = {"KRX-IDX"}


class Bars:
    """(venue, symbol) → 시각·시가 두 배열. dict of dict 보다 훨씬 작다."""

    def __init__(self):
        self.ts: dict[tuple, array.array] = {}
        self.op: dict[tuple, array.array] = {}

    def load(self, conn, keys: set) -> int:
        n = 0
        cur = conn.execute(
            "SELECT venue, symbol, bucket, open FROM bars_1m ORDER BY venue, symbol, bucket")
        for venue, symbol, bucket, op in cur:
            k = (venue, symbol)
            if k not in keys:
                continue
            if k not in self.ts:
                self.ts[k] = array.array("q")
                self.op[k] = array.array("d")
            self.ts[k].append(int(bucket))
            self.op[k].append(float(op if op is not None else 0.0))
            n += 1
        return n

    def next_open(self, key: tuple, after_us: int):
        """after_us **이후에 시작하는** 첫 봉의 시가. 없으면 None.

        같은 봉으로 체결하면 미래 참조가 된다. 그래서 `>` 가 아니라
        그 봉의 다음 경계부터 찾는다.
        """
        ts = self.ts.get(key)
        if not ts:
            return None
        import bisect
        i = bisect.bisect_right(ts, after_us)
        if i >= len(ts):
            return None
        if ts[i] - after_us > FILL_WINDOW_US:
            return None                      # 너무 멀다 — 피드가 비었던 구간
        px = self.op[key][i]
        return (ts[i], px) if px > 0 else None


def run(db_path: str, fee: float = FEE_RATE, slip_bp: float = SLIPPAGE_BP,
        limit: int = 0) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60.0)
    conn.execute("PRAGMA query_only=ON")

    sql = "SELECT ts, venue, symbol, strategy, action, ref_price FROM signals ORDER BY ts"
    if limit:
        sql += f" LIMIT {int(limit)}"
    sigs = conn.execute(sql).fetchall()
    keys = {(s[1], s[2]) for s in sigs
            if s[1] not in NON_TRADABLE_VENUES and s[2] and s[2].strip()}
    print(f"[paper] 시그널 {len(sigs):,}건 · 종목 {len(keys):,}개 — 봉 적재 중...",
          flush=True)
    bars = Bars()
    t0 = time.time()
    nbars = bars.load(conn, keys)
    print(f"[paper] 봉 {nbars:,}개 적재 ({time.time()-t0:.1f}s)", flush=True)
    conn.close()

    slip = slip_bp / 10_000.0
    # (venue, symbol, strategy) → 진입 정보
    open_pos: dict[tuple, tuple] = {}
    trades: dict[str, list] = defaultdict(list)
    unfilled: dict[str, int] = defaultdict(int)
    ignored: dict[str, int] = defaultdict(int)
    ref_gap: list[float] = []                # ref_price 대비 실제 체결가 차이

    skipped_venue = 0
    skipped_blank = 0
    for ts, venue, symbol, strat, action, ref in sigs:
        if venue in NON_TRADABLE_VENUES:
            skipped_venue += 1
            continue
        if not symbol or not symbol.strip():
            # 빈 종목명은 서로 다른 것이 뭉쳐 있다는 뜻이다. 값을 믿을 수 없다.
            skipped_blank += 1
            continue
        key = (venue, symbol)
        pkey = (venue, symbol, strat)
        held = pkey in open_pos
        if (action == BUY and held) or (action == SELL and not held):
            ignored[strat] += 1              # 방향이 안 맞는 시그널
            continue

        got = bars.next_open(key, ts)
        if got is None:
            unfilled[strat] += 1             # ← 피드가 비어 체결 못 함
            continue
        fill_ts, px = got
        if ref:
            ref_gap.append((px - ref) / ref)

        if action == BUY:
            entry = px * (1 + slip) * (1 + fee)
            open_pos[pkey] = (fill_ts, entry)
        else:
            entry_ts, entry = open_pos.pop(pkey)
            exit_px = px * (1 - slip) * (1 - fee)
            trades[strat].append({
                "venue": venue, "symbol": symbol,
                "entry_ts": entry_ts, "exit_ts": fill_ts,
                "entry": entry, "exit": exit_px,
                "ret": exit_px / entry - 1.0,
                "hold_min": (fill_ts - entry_ts) / 1e6 / 60,
            })

    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "fee_rate": fee, "slippage_bp": slip_bp,
        "signals_total": len(sigs),
        "note": "라이브 시그널의 모의 체결. 다음 봉 시가 체결, 롱 전용, 왕복 비용 반영.",
        "strategies": {},
    }
    for strat, tl in sorted(trades.items()):
        rets = [t["ret"] for t in tl]
        wins = [r for r in rets if r > 0]
        # 거래마다 **같은 명목금액 1단위**를 넣는다. 곱하지 않는다.
        #
        # 처음엔 수익률을 순차로 곱했는데 그건 틀렸다. 894개 거래가 시간상
        # 겹치는데 한 계좌를 차례로 복리 굴린 것처럼 계산한 것이다 —
        # 같은 순간에 자본을 여러 번 전액 투입할 수는 없다.
        # 겹치는 거래에 복리를 먹이면 변동성 끌림이 과장돼 누적이 실제와
        # 무관해진다(실측: 평균 +0.731%인데 누적 −40%가 나왔다).
        #
        # 명목 고정이면 손익은 그냥 더하면 되고, 낙폭도 같은 단위로 읽힌다.
        by_exit = sorted(tl, key=lambda t: t["exit_ts"])
        cum, peak, mdd = 0.0, 0.0, 0.0
        for t in by_exit:
            cum += t["ret"]
            peak = max(peak, cum)
            mdd = max(mdd, peak - cum)      # 명목 단위 낙폭
        out["strategies"][strat] = {
            "trades": len(tl),
            "win_rate": len(wins) / len(rets) if rets else 0.0,
            "avg_ret": sum(rets) / len(rets) if rets else 0.0,
            "best": max(rets) if rets else 0.0,
            "worst": min(rets) if rets else 0.0,
            # 거래당 1단위 명목 기준 총손익. 예: 0.35 = 명목의 35%.
            "pnl_units": cum,
            "mdd_units": mdd,
            "avg_hold_min": sum(t["hold_min"] for t in tl) / len(tl) if tl else 0.0,
            "unfilled_signals": unfilled.get(strat, 0),
            "ignored_signals": ignored.get(strat, 0),
            "open_at_end": sum(1 for k in open_pos if k[2] == strat),
        }
    out["excluded"] = {
        "non_tradable_venue": skipped_venue,
        "blank_symbol": skipped_blank,
        "why": "지수는 매매 대상이 아니고, 빈 종목명은 서로 다른 것이 뭉친 것이다",
    }
    tot_unfilled = sum(unfilled.values())
    out["unfilled_total"] = tot_unfilled
    out["unfilled_pct"] = tot_unfilled / len(sigs) if sigs else 0.0
    if ref_gap:
        ref_gap.sort()
        out["ref_vs_fill"] = {
            "n": len(ref_gap),
            "median_bp": ref_gap[len(ref_gap) // 2] * 10_000,
            "p95_bp": ref_gap[int(len(ref_gap) * 0.95)] * 10_000,
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser("paper_ledger",
                                 description="라이브 시그널 모의 체결 장부")
    ap.add_argument("--db", default="data/mdfeed.db")
    ap.add_argument("--out", default="docs/data/paper-ledger.json")
    ap.add_argument("--limit", type=int, default=0, help="시그널 수 제한 (시험용)")
    a = ap.parse_args()

    r = run(a.db, limit=a.limit)

    print()
    print(f"{'전략':<12} {'거래':>7} {'승률':>7} {'거래당평균':>10} "
          f"{'총손익':>9} {'최대낙폭':>9} {'보유(분)':>8} {'미체결':>7}")
    print("─" * 84)
    for s, d in r["strategies"].items():
        print(f"{s:<12} {d['trades']:>7,} {d['win_rate']*100:>6.1f}% "
              f"{d['avg_ret']*100:>+9.3f}% {d['pnl_units']:>+8.1f}배 "
              f"{d['mdd_units']:>8.1f}배 {d['avg_hold_min']:>8.1f} "
              f"{d['unfilled_signals']:>7,}")
    print("─" * 84)
    print("총손익·낙폭 단위는 **거래당 명목금액의 배수**다. 수익률이 아니다 —")
    print("거래가 시간상 겹치므로 한 계좌의 수익률로 환산할 수 없다.")
    print("거래당 100만원씩 넣을 수 있었다면 "
          + " · ".join(f"{k} {v['pnl_units']*100:+,.0f}만원"
                       for k, v in r["strategies"].items()))
    ex = r["excluded"]
    print(f"시그널 {r['signals_total']:,}건 중 "
          f"제외 {ex['non_tradable_venue'] + ex['blank_symbol']:,}건"
          f"(지수 {ex['non_tradable_venue']:,} · 빈 종목명 {ex['blank_symbol']:,}) · "
          f"체결 못 함 {r['unfilled_total']:,}건 ({r['unfilled_pct']*100:.1f}%)")
    if "ref_vs_fill" in r:
        g = r["ref_vs_fill"]
        print(f"시그널 시점 가격 대비 실제 체결가: 중앙값 {g['median_bp']:+.1f}bp · "
              f"p95 {g['p95_bp']:+.1f}bp")

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(r, fh, ensure_ascii=False, indent=2)
    print(f"\n→ {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
