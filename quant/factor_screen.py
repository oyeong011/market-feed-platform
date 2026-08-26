"""팩터 스크리너 — 기존 financial-database(487,434건)를 이 플랫폼의 참조 데이터 평면으로 붙인다.

MDFeed 의 두 데이터 평면
------------------------
    실시간 평면 : 거래소 WS → feedd → 버스 → 1분봉        (밀리초 단위, 휘발성)
    참조 평면   : SEC EDGAR / OpenDART → 재무제표 487k건   (분기 단위, 영속)

두 평면을 잇는 조인 키는 **DART stock_code = KIS 종목코드**다. 삼성전자는
참조 평면에서 `005930`, 실시간 평면에서 `KIS:005930` 이다. 크립토 심볼은 대응하는
재무제표가 없으므로 조인 대상이 아니다 — 억지로 붙이지 않는다.

원본 데이터 형식
----------------
long/EAV 형식이다: (ticker, fiscal_year, statement_type, field_name, value).
컬럼이 늘어도 스키마를 안 바꿔도 된다는 장점이 있지만, 팩터 계산은 항목 간
연산이라 wide 로 피벗해야 한다. pandas 없이 표준 csv 모듈로 스트리밍 처리한다
(파일이 25MB라 전부 메모리에 올려도 되지만, 수집이 계속되면 GB 단위가 된다).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.request
from collections import defaultdict

RAW_BASE = "https://raw.githubusercontent.com/oyeong011/financial-database/main/data"


def fetch_if_missing(market: str, cache_dir: str = "data/reference") -> str:
    """로컬에 없으면 기존 레포에서 받아 캐시한다."""
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{market}_financial_data.csv")
    if os.path.exists(path) and os.path.getsize(path) > 1024:
        return path
    url = f"{RAW_BASE}/{market}/financial_data.csv"
    print(f"[factor] 내려받는 중: {url}")
    urllib.request.urlretrieve(url, path)
    print(f"[factor] 저장: {path} ({os.path.getsize(path)/1e6:.1f}MB)")
    return path


def load_wide(path: str, key_col: str) -> dict:
    """long → wide 피벗. {(key, year): {field: value}}"""
    out: dict = defaultdict(dict)
    names: dict = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            key = (row.get(key_col) or "").strip()
            year = row.get("fiscal_year")
            if not key or not year:
                continue
            try:
                val = float(row["value"])
            except (TypeError, ValueError):
                continue
            out[(key, int(year))][row["field_name"]] = val
            names.setdefault(key, row.get("company_name", ""))
    return out, names


def _safe_div(a, b):
    try:
        return a / b if a is not None and b else None
    except (TypeError, ZeroDivisionError):
        return None


def compute_factors(wide: dict, names: dict, min_revenue: float = 0.0) -> list[dict]:
    """항목 간 연산으로 팩터를 만든다. 결측은 None 으로 남기고 절대 0으로 채우지 않는다.

    결측을 0으로 채우면 '부채가 없는 회사'와 '부채 데이터가 없는 회사'가 같아진다.
    스크리닝 결과가 조용히 오염되는 전형적 경로다.
    """
    # 종목별 연도 정렬 (YoY 성장률용)
    by_key: dict = defaultdict(dict)
    for (key, year), fields in wide.items():
        by_key[key][year] = fields

    rows = []
    for key, years in by_key.items():
        ys = sorted(years)
        if not ys:
            continue
        y = ys[-1]
        f = years[y]
        prev = years.get(y - 1, {})

        revenue = f.get("revenue")
        if revenue is None or revenue < min_revenue:
            continue

        op = f.get("operating_income")
        net = f.get("net_income")
        assets = f.get("total_assets")
        equity = f.get("total_equity")
        liab = f.get("total_liabilities")
        ocf = f.get("operating_cash_flow")
        capex = f.get("capex")
        cash = f.get("cash_and_equivalents")

        fcf = (ocf - abs(capex)) if (ocf is not None and capex is not None) else None
        rows.append({
            "key": key,
            "name": names.get(key, ""),
            "fiscal_year": y,
            "revenue": revenue,
            "operating_margin": _safe_div(op, revenue),
            "net_margin": _safe_div(net, revenue),
            "roa": _safe_div(net, assets),
            "roe": _safe_div(net, equity),
            "debt_to_equity": _safe_div(liab, equity),
            "revenue_growth": _safe_div(
                revenue - prev.get("revenue"), prev.get("revenue"))
            if prev.get("revenue") else None,
            "fcf": fcf,
            "fcf_margin": _safe_div(fcf, revenue),
            "cash_ratio": _safe_div(cash, assets),
        })
    return rows


def screen(rows: list[dict], top: int = 20) -> list[dict]:
    """복합 점수로 순위를 매긴다.

    각 팩터를 백분위로 바꿔 합산한다. 원값을 그대로 더하면 단위가 큰 팩터
    (매출액)가 점수를 독식한다. 결측이 있는 종목은 그 팩터에서 중앙값(0.5)을
    받아 과도한 불이익도 이득도 없게 한다.
    """
    factors = [("operating_margin", 1), ("roe", 1), ("revenue_growth", 1),
               ("fcf_margin", 1), ("debt_to_equity", -1)]
    n = len(rows)
    if n == 0:
        return []

    pct: dict = {}
    for name, _sign in factors:
        vals = sorted((r[name], i) for i, r in enumerate(rows) if r[name] is not None)
        p = {}
        for rank, (_v, i) in enumerate(vals):
            p[i] = rank / max(len(vals) - 1, 1)
        pct[name] = p

    for i, r in enumerate(rows):
        score = 0.0
        for name, sign in factors:
            v = pct[name].get(i, 0.5)
            score += v if sign > 0 else (1.0 - v)
        r["score"] = round(score / len(factors), 4)

    return sorted(rows, key=lambda r: r["score"], reverse=True)[:top]


def join_with_feed(rows: list[dict], db_path: str, venue: str = "KIS") -> list[dict]:
    """참조 평면 ↔ 실시간 평면 조인 (DART stock_code = KIS 종목코드).

    피드에 없는 종목은 last/거래량이 None 으로 남는다. 이 프로젝트의 기본 설정은
    크립토 어댑터라 KIS 데이터가 없으면 전부 None 이고, 그게 정상이다.
    """
    import sqlite3
    if not os.path.exists(db_path):
        return rows
    conn = sqlite3.connect(db_path)
    live = {}
    for sym, close, vol in conn.execute(
            "SELECT symbol, close, SUM(volume) FROM bars_1m WHERE venue=? "
            "GROUP BY symbol", (venue,)):
        live[sym] = (close, vol)
    conn.close()
    for r in rows:
        px, vol = live.get(r["key"], (None, None))
        r["live_price"] = px
        r["live_volume"] = vol
        r["feed_linked"] = px is not None
    return rows


def main() -> int:
    ap = argparse.ArgumentParser("factor_screen",
                                 description="financial-database 팩터 스크리너")
    ap.add_argument("--market", choices=["dart", "sec"], default="dart")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--min-revenue", type=float, default=1e11,
                    help="최소 매출 (DART 원, SEC 달러)")
    ap.add_argument("--db", default="data/mdfeed.db")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    path = fetch_if_missing(args.market)
    key_col = "stock_code" if args.market == "dart" else "ticker"
    wide, names = load_wide(path, key_col)
    print(f"[factor] {args.market.upper()} 로드: {len(wide):,} (종목,연도) 조합")

    rows = compute_factors(wide, names, args.min_revenue)
    print(f"[factor] 팩터 계산 완료: {len(rows):,} 종목 (매출 하한 통과)")

    ranked = screen(rows, args.top)
    ranked = join_with_feed(ranked, args.db)

    print(f"\n{'#':>3} {'종목':<10} {'이름':<24} {'점수':>6} {'영업이익률':>9} "
          f"{'ROE':>8} {'매출성장':>9} {'피드연결':>7}")
    print("-" * 88)
    for i, r in enumerate(ranked, 1):
        def pc(v):
            return f"{v*100:>8.1f}%" if v is not None else "       -"
        print(f"{i:>3} {r['key']:<10} {r['name'][:22]:<24} {r['score']:>6.3f} "
              f"{pc(r['operating_margin'])} {pc(r['roe'])} {pc(r['revenue_growth'])} "
              f"{'O' if r.get('feed_linked') else '-':>7}")

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump({"market": args.market, "universe": len(rows),
                       "items": ranked}, fh, ensure_ascii=False, indent=1)
        print(f"\n[factor] 결과 저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
