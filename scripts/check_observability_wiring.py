#!/usr/bin/env python3
"""대시보드가 **실제로 화면에 뜰 배선인가.**

패널을 아무리 잘 만들어도 프로비저닝이 조용히 건너뛰면 아무것도 안 보인다.
Grafana 는 그때 오류를 내지 않는다 — 폴더가 비어 있을 뿐이다.

보는 것 셋.

1. 데이터소스에 `isDefault: true` 가 있는가. 패널에 `datasource` 를 안 적으면
   기본 데이터소스로 가는데, 기본이 없으면 패널 전부가 "datasource not found" 다.
2. 프로비저닝이 보는 경로에 compose 가 대시보드를 **실제로 마운트하는가.**
   한쪽 경로만 바꾸면 조용히 어긋난다.
3. Prometheus 가 읽는 규칙 파일 경로가 compose 마운트 지점과 같은가.
   다르면 알람이 하나도 안 등록되는데, 어디에도 빨간 줄이 안 뜬다.

    python scripts/check_observability_wiring.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OBS = ROOT / "ops" / "observability"
COMPOSE = ROOT / "docker-compose.observability.yml"


def main() -> int:
    problems: list[str] = []

    datasources = OBS / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    dashboards = OBS / "grafana" / "provisioning" / "dashboards" / "default.yml"
    for path in (datasources, dashboards, COMPOSE, OBS / "prometheus.yml"):
        if not path.exists():
            problems.append(f"{path.relative_to(ROOT)} 가 없다")
    if problems:
        for p in problems:
            print(f"::error::{p}" if "GITHUB_ACTIONS" in os.environ else p)
        return 1

    ds = datasources.read_text(encoding="utf-8")
    dash = dashboards.read_text(encoding="utf-8")
    compose = COMPOSE.read_text(encoding="utf-8")
    prom = (OBS / "prometheus.yml").read_text(encoding="utf-8")

    if "isDefault: true" not in ds:
        problems.append("데이터소스에 isDefault: true 가 없다 — "
                        "datasource 를 안 적은 패널이 전부 'datasource not found' 가 된다")

    m = re.search(r"^\s*path:\s*(\S+)", dash, re.M)
    if not m:
        problems.append("대시보드 프로비저닝에 path 가 없다")
    else:
        mount = m.group(1)
        if f":{mount}:ro" not in compose:
            problems.append(
                f"프로비저닝은 {mount} 를 보는데 compose 가 거기에 마운트하지 않는다 "
                "— 폴더가 비고 패널이 하나도 안 뜬다")

    for m in re.finditer(r"^\s*-\s*(/etc/prometheus/\S+)", prom, re.M):
        target = m.group(1)
        if f":{target}:ro" not in compose:
            problems.append(
                f"prometheus.yml 이 {target} 를 읽는데 compose 가 거기에 마운트하지 않는다")

    for p in problems:
        print(f"::error::{p}" if "GITHUB_ACTIONS" in os.environ else p)
    if problems:
        return 1
    print("관측 배선 검증 통과 (기본 데이터소스 · 대시보드 마운트 · 규칙 파일 마운트)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
