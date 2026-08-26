#!/usr/bin/env python3
"""대시보드 정적 모드용 스냅샷 생성.

GitHub Pages 에는 살아있는 서비스가 없으므로, 녹화 파일의 마지막 상태를 뽑아
정적 스냅샷으로 만든다. 실행 중인 feedd 가 있으면 그쪽을 우선 쓴다.

    python scripts/capture_snapshot.py --out docs/data/snapshot.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdfeed.clock import ClockMonitor                         # noqa: E402
from mdfeed.models import MSG_BOOK, MSG_TRADE, BookTop, Trade  # noqa: E402
from mdfeed.protocol import FrameParser                        # noqa: E402


def from_live(port: int) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/snapshot", timeout=3) as r:
            d = json.loads(r.read())
        return {"source": "live feedd", "items": d["items"]}
    except Exception:                                # noqa: BLE001
        return None


def from_replay(path: str) -> dict:
    clock = ClockMonitor()
    snap: dict[str, dict] = {}
    parser = FrameParser()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            for f in parser.feed(chunk):
                if f.msg_type == MSG_TRADE and len(f.payload) >= Trade.SIZE:
                    t = Trade.unpack(f.payload)
                    key = f"{t.venue}:{t.symbol}"
                    cur = snap.setdefault(key, {"venue": t.venue, "symbol": t.symbol,
                                                "trades": 0, "volume": 0.0})
                    cur["last"] = t.price
                    cur["qty"] = t.qty
                    cur["side"] = t.side
                    cur["trades"] += 1
                    cur["volume"] = round(cur["volume"] + t.qty, 8)
                    cur["latency_us"] = round(clock.observe(t.venue, t.latency_us), 1)
                elif f.msg_type == MSG_BOOK and len(f.payload) >= BookTop.SIZE:
                    b = BookTop.unpack(f.payload)
                    key = f"{b.venue}:{b.symbol}"
                    cur = snap.setdefault(key, {"venue": b.venue, "symbol": b.symbol,
                                                "trades": 0, "volume": 0.0})
                    cur.update(bid=b.bid, ask=b.ask, mid=b.mid,
                               spread_bp=round(b.spread_bp, 3))
    return {"source": f"녹화 리플레이 ({os.path.basename(path)})",
            "items": sorted(snap.values(), key=lambda x: (x["venue"], x["symbol"]))}


def main() -> int:
    ap = argparse.ArgumentParser("capture_snapshot")
    ap.add_argument("--replay", default="data/replay/sample.mdf")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--out", default="docs/data/snapshot.json")
    args = ap.parse_args()

    d = from_live(args.port)
    if d is None:
        if not os.path.exists(args.replay):
            print(f"실행 중인 feedd 도 없고 녹화 파일도 없습니다: {args.replay}")
            return 1
        d = from_replay(args.replay)

    d["captured_at"] = time.strftime("%Y-%m-%d %H:%M:%S KST")
    d["count"] = len(d["items"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(d, fh, ensure_ascii=False, indent=1)
    print(f"스냅샷 {d['count']}종목 저장 ({d['source']}) → {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
