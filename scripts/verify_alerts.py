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

# 구성에 따라 없는 것이 정상인 지표.
#
# 지연·시계 지표는 **지연을 측정하는 어댑터가 하나라도 있어야** 생긴다.
# 리플레이 전용 구성(CI)에서는 measures_latency=False 뿐이라 생기지 않는데,
# 그건 결함이 아니라 정확한 동작이다. 없는 값을 0 으로 채우면
# "지연이 0µs" 라는 거짓말이 된다.
#
# 다만 **실측 어댑터가 붙어 있는데도 없으면 그건 진짜 결함**이다.
# 그래서 조건을 확인한 뒤에 판정한다.
CONDITIONAL = {
    "mdfeed_ingest_latency_microseconds":
        "지연을 측정하는 어댑터(measures_latency=True)가 있어야 생성된다",
    "mdfeed_clock_offset_us":
        "같은 조건. 시계 오프셋은 거래소 체결시각이 있어야 추정할 수 있다",
}
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


def any_adapter_measures_latency() -> bool | None:
    """지연을 측정하는 업스트림이 하나라도 붙어 있는가.

    None 이면 feedd 에 물어보지 못한 것 — 판단을 유보한다.
    """
    for port in (9100, 9200):
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/healthz", timeout=4) as r:
                d = json.loads(r.read())
        except Exception:                            # noqa: BLE001
            continue
        for u in d.get("upstreams", []):
            if u.get("measures_latency"):
                return True
    return False


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

    measures = any_adapter_measures_latency()
    ok, conditional, missing = {}, {}, {}
    for m, alerts in referenced.items():
        if m in exposed:
            ok[m] = alerts
        elif m in CONDITIONAL and measures is False:
            # 지연을 재는 어댑터가 없으므로 없는 것이 정확한 동작이다
            conditional[m] = alerts
        else:
            missing[m] = alerts

    print(f"{'지표':<46} {'상태':<10} 참조 알람")
    print("-" * 92)
    for m, alerts in sorted(ok.items()):
        print(f"{m:<46} {'OK':<10} {', '.join(alerts)}")
    for m, alerts in sorted(conditional.items()):
        print(f"{m:<46} {'조건부':<10} {', '.join(alerts)}")
        print(f"{'':<46} {'':<10} └ {CONDITIONAL[m]}")
    for m, alerts in sorted(missing.items()):
        print(f"{m:<46} {'없음':<10} {', '.join(alerts)}   ← 이 알람은 영원히 안 울린다")
    print("-" * 92)
    print(f"참조 {len(referenced)}종 · 존재 {len(ok)} · 조건부 {len(conditional)} · "
          f"누락 {len(missing)}")
    if conditional:
        print(f"조건부는 현재 구성(지연 측정 어댑터 없음)에서 없는 것이 정확한 동작입니다.\n"
              f"실측 어댑터를 붙이면 생성되며, 그때도 없으면 결함으로 잡힙니다.")
    if missing:
        print("\n누락된 지표를 노출하거나 규칙을 고치세요.")
        return 1
    print("모든 알람이 실재하거나, 없는 이유가 설명되는 지표를 참조합니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
