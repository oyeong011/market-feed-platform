#!/usr/bin/env python3
"""수신기에 알람이 **실제로 도착했는지** 기다려 확인한다.

설정에 Alertmanager 가 적혀 있는 것과 알람이 사람에게 닿는 것은 다르다. 이 저장소는
규칙 48개를 들고 있으면서 Alertmanager 가 없었고, 그 상태로 한 달을 보냈다(결함 51).
그래서 CI 가 **규칙 평가 → 라우팅 → 수신**을 실제로 통과시킨다.

    python scripts/wait_for_alert.py --alertname MarketDataGapOpen \
        --status firing --receiver critical --timeout 180
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def fetch(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser("wait_for_alert")
    ap.add_argument("--sink", default="http://127.0.0.1:9140")
    ap.add_argument("--alertname", required=True)
    ap.add_argument("--status", default="firing", choices=["firing", "resolved"])
    ap.add_argument("--receiver", help="이 receiver 로 라우팅됐는지도 본다")
    ap.add_argument("--timeout", type=float, default=180.0)
    args = ap.parse_args()

    url = f"{args.sink}/alerts?alertname={args.alertname}&status={args.status}"
    deadline = time.time() + args.timeout
    last = 0
    while time.time() < deadline:
        data = fetch(url)
        if data is None:
            print("수신기에 아직 못 물어봤다", flush=True)
        else:
            items = data.get("items", [])
            if len(items) != last:
                last = len(items)
                print(f"{args.alertname} {args.status} {last}건", flush=True)
            if items:
                hit = items[-1]
                print(json.dumps(hit, ensure_ascii=False, indent=1), flush=True)
                if args.receiver and hit.get("receiver") != args.receiver:
                    print(f"도착은 했는데 receiver 가 {hit.get('receiver')!r} 다 "
                          f"(기대: {args.receiver!r}) — severity 라우팅이 안 먹었다",
                          file=sys.stderr)
                    return 1
                # 알람이 왜 울렸고 무엇을 하라는지 함께 와야 한다. 둘 중 하나가 비면
                # 받은 사람이 판단할 수 없다 — 규칙 파일이 선언한 규약이다.
                missing = [k for k in ("summary", "action") if not hit.get(k)]
                if missing:
                    print(f"도착했지만 {', '.join(missing)} 가 비어 있다 — "
                          "받은 사람이 무엇을 할지 모른다", file=sys.stderr)
                    return 1
                print(f"\n도착 확인: {args.alertname} {args.status} "
                      f"· receiver={hit.get('receiver')} · severity={hit.get('severity')}")
                return 0
        time.sleep(3)

    print(f"{args.timeout:.0f}초 안에 {args.alertname} {args.status} 가 "
          "수신기에 도착하지 않았다", file=sys.stderr)
    data = fetch(f"{args.sink}/alerts")
    if data is not None:
        print(f"수신기가 받은 전체: {data.get('count')}건 — "
              f"{[i['alertname'] for i in data.get('items', [])][:10]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
