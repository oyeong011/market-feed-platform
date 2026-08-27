#!/usr/bin/env python3
"""알람 규칙이 참조하는 지표가 실제로 존재하는지 검증한다.

**존재하지 않는 지표를 참조하는 알람은 영원히 울리지 않는다.**
그런데 Prometheus 는 아무 오류도 내지 않는다 — 그냥 조용히 no data 다.
설정 파일은 문법적으로 완벽하고, 대시보드에도 규칙이 보이고, 아무도 이상을 못 느낀다.
이 프로젝트가 계속 마주친 "조용히 틀린" 실패의 한 종류다.

그래서 규칙에서 지표 이름을 뽑아 실행 중인 서비스의 /metrics 와 대조한다.

    python scripts/verify_alerts.py
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request

PORTS = [9100, 9200, 9111, 9102, 9103, 9104, 9105, 9106]
METRIC_RE = re.compile(r"\b(mdfeed_[a-z0-9_]+)")
EXPR_RE = re.compile(r"^\s*expr:\s*(.+)$")


def scrape(port: int) -> set[str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=4) as r:
            text = r.read().decode()
    except Exception:                                # noqa: BLE001
        return set()
    out = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line.split("{")[0].split(" ")[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser("verify_alerts")
    ap.add_argument("--rules", default="ops/observability/alerts.yml")
    args = ap.parse_args()

    exposed = set()
    live_ports = []
    for p in PORTS:
        m = scrape(p)
        if m:
            live_ports.append(p)
            exposed |= m
    if not exposed:
        print("실행 중인 서비스가 없습니다. 스택을 먼저 띄우세요: make up-shards")
        return 2
    print(f"수집: {len(live_ports)}개 포트에서 지표 {len(exposed)}종\n")

    referenced: dict[str, list[str]] = {}
    alert = None
    for line in open(args.rules, encoding="utf-8"):
        a = re.match(r"^\s*- alert:\s*(\S+)", line)
        if a:
            alert = a.group(1)
        e = EXPR_RE.match(line)
        if e and alert:
            for m in METRIC_RE.findall(e.group(1)):
                referenced.setdefault(m, []).append(alert)

    # up 은 Prometheus 가 만드는 지표라 우리 /metrics 에 없다
    missing = {m: a for m, a in referenced.items() if m not in exposed}
    ok = {m: a for m, a in referenced.items() if m in exposed}

    print(f"{'지표':<46} {'상태':<8} 참조 알람")
    print("-" * 90)
    for m, alerts in sorted(ok.items()):
        print(f"{m:<46} {'OK':<8} {', '.join(alerts)}")
    for m, alerts in sorted(missing.items()):
        print(f"{m:<46} {'없음':<8} {', '.join(alerts)}   ← 이 알람은 영원히 안 울린다")
    print("-" * 90)
    print(f"참조 {len(referenced)}종 · 존재 {len(ok)} · 누락 {len(missing)}")
    if missing:
        print("\n누락된 지표를 노출하거나 규칙을 고치세요.")
        return 1
    print("모든 알람이 실재하는 지표를 참조합니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
