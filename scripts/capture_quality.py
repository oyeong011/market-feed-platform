#!/usr/bin/env python3
"""품질 검사 스냅샷 — 대시보드가 읽을 JSON.

실행 중인 quality 프로세스에서 가져오고, 없으면 녹화 파일을 검사기에 통과시켜
같은 결과를 만든다. 공개 대시보드에는 살아있는 서비스가 없으므로 후자가 기본이다.
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

from mdfeed.models import MSG_BOOK, MSG_TRADE, BookTop, Trade   # noqa: E402
from mdfeed.protocol import FrameParser                          # noqa: E402
from mdfeed.quality import QualityMonitor                        # noqa: E402


def from_live(port: int):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/events", timeout=3) as r:
            d = json.loads(r.read())
        d["source"] = "live quality service"
        return d
    except Exception:                                # noqa: BLE001
        return None


def from_replay(path: str):
    m = QualityMonitor()
    p = FrameParser()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            for f in p.feed(chunk):
                if f.msg_type == MSG_TRADE and len(f.payload) >= Trade.SIZE:
                    t = Trade.unpack(f.payload)
                    m.on_trade(t.venue, t.symbol, t.price, t.ts_event_ns)
                elif f.msg_type == MSG_BOOK and len(f.payload) >= BookTop.SIZE:
                    b = BookTop.unpack(f.payload)
                    m.on_quote(b.venue, b.symbol, b.bid, b.ask, b.ts_event_ns)
    d = m.report()
    d["source"] = f"녹화 리플레이 ({os.path.basename(path)})"
    return d


def main() -> int:
    ap = argparse.ArgumentParser("capture_quality")
    ap.add_argument("--replay", default="data/replay/sample.mdf")
    ap.add_argument("--port", type=int, default=9106)
    ap.add_argument("--out", default="docs/data/quality.json")
    args = ap.parse_args()

    d = from_live(args.port)
    if d is None:
        if not os.path.exists(args.replay):
            print(f"녹화 파일 없음: {args.replay}")
            return 1
        d = from_replay(args.replay)
    d["captured_at"] = time.strftime("%Y-%m-%d %H:%M:%S KST")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(d, fh, ensure_ascii=False, indent=1)
    print(f"검사 {d['checked']:,}건 / CRITICAL {d['critical']} / WARNING {d['warning']} "
          f"({d['source']}) → {args.out}")
    if d.get("implied_fx"):
        print("  암묵 환율:", {k: round(v, 1) for k, v in d["implied_fx"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
