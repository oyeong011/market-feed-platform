#!/usr/bin/env python3
"""품질 검사 회귀용 실데이터 코퍼스를 뽑는다.

왜 합성이 아니라 실데이터인가
-----------------------------
2026-08-31 에 `stale_value` 가 조용한 시장을 상류 고장이라 부르고 있는 걸
발견했다. 합성 데이터로는 절대 못 잡았을 결함이다 — 사람이 만든 틱은
"같은 가격이 스무 번 연속" 같은 실제 미시구조를 재현하지 않는다.
EURUSDT 의 20건 동일가 구간, BTTCUSDT 의 호가 단위 문제, KRX 비유동
종목의 분 단위 공백은 **실제 시장에만 있다.**

그래서 오탐 회귀는 실제 체결로 잡는다. 이 스크립트가 그 표본을 만든다.
공개 시장가격이라 저장소에 넣어도 문제 없다(자격증명 없음).

    python scripts/make_quality_corpus.py
"""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "quality_corpus.jsonl.gz")
DB = os.path.join(ROOT, "data", "mdfeed.db")

N_TRADES, N_BOOKS, N_BARS = 12000, 4000, 4000


def main() -> int:
    if not os.path.exists(DB):
        print(f"DB 없음: {DB}", file=sys.stderr)
        return 1
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=30)
    c.row_factory = sqlite3.Row

    rows = []
    for r in c.execute("SELECT ts,venue,symbol,price FROM trades "
                       f"ORDER BY ts DESC LIMIT {N_TRADES}"):
        rows.append({"k": "t", "ts": r["ts"], "v": r["venue"],
                     "s": r["symbol"], "p": r["price"]})
    for r in c.execute("SELECT ts,venue,symbol,bid,ask FROM book_top "
                       f"ORDER BY ts DESC LIMIT {N_BOOKS}"):
        rows.append({"k": "q", "ts": r["ts"], "v": r["venue"], "s": r["symbol"],
                     "b": r["bid"], "a": r["ask"]})
    for r in c.execute("SELECT bucket,venue,symbol,open,high,low,close FROM bars_1m "
                       f"ORDER BY bucket DESC LIMIT {N_BARS}"):
        rows.append({"k": "b", "ts": r["bucket"], "v": r["venue"], "s": r["symbol"],
                     "o": r["open"], "h": r["high"], "l": r["low"], "c": r["close"]})
    rows.sort(key=lambda x: x["ts"])

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    kinds = {}
    for r in rows:
        kinds[r["k"]] = kinds.get(r["k"], 0) + 1
    print(f"{OUT}  {os.path.getsize(OUT) / 1024:.0f}KB  "
          f"체결 {kinds.get('t', 0):,} · 호가 {kinds.get('q', 0):,} · 봉 {kinds.get('b', 0):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
