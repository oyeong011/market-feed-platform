#!/usr/bin/env python3
"""암호화폐 종목 유니버스 내려받기 — 업비트 KRW 마켓 · 바이낸스 USDT 페어.

KRX 는 REST 폴링이라 종목을 늘리면 데이터가 늘지 않고 회전 주기만 길어진다.
암호화폐는 웹소켓 구독이라 종목을 늘리면 실제로 유량이 늘어난다.
그래서 부하 특성이 완전히 다르고, 유니버스도 따로 관리한다.

KRX 마스터와 같은 형식(market,code,name)으로 떨궈 로딩 코드를 공유한다.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import urllib.request

UPBIT_MARKETS = "https://api.upbit.com/v1/market/all"
BINANCE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
UA = {"User-Agent": "mdfeed/1.0 (universe fetcher)"}


def _get(url: str) -> dict | list:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def upbit_krw() -> list[tuple[str, str]]:
    """업비트 원화 마켓. BTC/USDT 마켓은 뺀다 — 같은 코인이 중복 등록된다."""
    rows = []
    for m in _get(UPBIT_MARKETS):
        code = m.get("market", "")
        if not code.startswith("KRW-"):
            continue
        rows.append((code, m.get("korean_name") or code))
    return sorted(rows)


def binance_usdt() -> list[tuple[str, str]]:
    """바이낸스 USDT 현물 페어. 거래정지(TRADING 아님) 종목은 뺀다 —
    구독은 받아주지만 체결이 영원히 안 와서 정체 감시가 계속 울린다."""
    rows = []
    for s in _get(BINANCE_INFO).get("symbols", []):
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        if not s.get("isSpotTradingAllowed"):
            continue
        rows.append((s["symbol"].lower(), s.get("baseAsset", "")))
    return sorted(rows)


def main() -> int:
    ap = argparse.ArgumentParser("fetch_crypto_symbols")
    ap.add_argument("--out", default="data/reference/crypto_symbols.csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    sources = [("UPBIT", upbit_krw), ("BINANCE", binance_usdt)]
    counts = {}
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["market", "code", "name"])
        for market, fn in sources:
            try:
                rows = fn()
            except Exception as e:                        # noqa: BLE001
                # 한쪽이 막혀도 다른 쪽은 받아 둔다. 유니버스가 통째로
                # 비면 어댑터가 enabled()=False 로 조용히 꺼진다.
                print(f"  {market}: 실패 — {type(e).__name__}: {e}")
                counts[market] = 0
                continue
            for code, name in rows:
                w.writerow([market, code, name])
            counts[market] = len(rows)
            print(f"  {market}: {len(rows):,}종목")

    total = sum(counts.values())
    print(f"{args.out} — 합계 {total:,}종목")
    return 0 if total else 1


if __name__ == "__main__":
    raise SystemExit(main())
