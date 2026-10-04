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
4. **알람이 사람에게 닿는 경로가 끊기지 않았는가.** `alerting.alertmanagers` 가
   실재하는 서비스를 가리키는지, Alertmanager 의 webhook 이 실재하는 서비스를
   가리키는지, 그리고 **규칙이 쓰는 severity 마다 라우트가 있는지**를 본다.
   severity 에 오타가 나면 그 알람은 조용히 default 로 떨어진다 — 전달은 되지만
   critical 의 짧은 대기·짧은 반복 간격을 못 받는다. 규칙 48개가 Alertmanager
   자체 없이 firing 만 되고 있던 것이 결함 51 이다.

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
RULES = ROOT / "ops" / "observability" / "alerts.yml"
ALERTMANAGER = OBS / "alertmanager.yml"


def _compose_services(compose: str) -> set[str]:
    """compose 의 서비스 이름. 컨테이너 네트워크에서 이 이름이 호스트명이 된다."""
    out: set[str] = set()
    in_services = False
    for line in compose.splitlines():
        if re.match(r"^services:\s*$", line):
            in_services = True
            continue
        if in_services and re.match(r"^[a-zA-Z]", line):
            break
        m = re.match(r"^  ([a-z][a-z0-9_-]*):\s*$", line)
        if in_services and m:
            out.add(m.group(1))
    return out


def _delivery_problems(prom: str, compose: str) -> list[str]:
    """**알람이 사람에게 닿는 경로가 끊기지 않았는가.**

    규칙이 실재하는 지표를 보고, 패널도 있고, 그런데 Alertmanager 가 없으면 알람은
    Prometheus 안에서 firing 이 되고 거기서 끝난다. 이 저장소가 그 상태였다(결함 51).
    """
    problems: list[str] = []
    services = _compose_services(compose)

    if not ALERTMANAGER.exists():
        return ["ops/observability/alertmanager.yml 이 없다 — 알람이 갈 곳이 없다"]
    am = ALERTMANAGER.read_text(encoding="utf-8")

    # ① Prometheus → Alertmanager
    targets = re.findall(r'alertmanagers:.*?targets:\s*\[([^\]]*)\]', prom, re.S)
    if not re.search(r"^alerting:", prom, re.M):
        problems.append("prometheus.yml 에 alerting 절이 없다 — 규칙이 울려도 "
                        "Prometheus 안에서 끝난다")
    for group in targets:
        for host in re.findall(r'"([^":]+):\d+"', group):
            if host not in services:
                problems.append(
                    f"prometheus.yml 이 알람을 {host} 로 보내는데 compose 에 그 서비스가 없다")

    # ② Alertmanager → 수신기
    hooks = re.findall(r'url:\s*https?://([^:/\s]+)', am)
    if not hooks:
        problems.append("alertmanager.yml 에 receiver 목적지가 없다 — 받는 곳이 없다")
    for host in set(hooks):
        if host not in services and "." not in host:
            problems.append(
                f"alertmanager.yml 이 {host} 로 보내는데 compose 에 그 서비스가 없다")

    # ③ 규칙이 쓰는 severity 마다 라우트가 있는가.
    #    오타가 나면 그 알람은 조용히 default 로 떨어지고, critical 의 짧은 대기·짧은
    #    반복 간격을 못 받는다. 목록이 곧 범위다.
    used = set(re.findall(r"severity:\s*(\w+)", RULES.read_text(encoding="utf-8")))
    routed = set(re.findall(r'severity\s*=\s*"(\w+)"', am))
    for sev in sorted(used - routed):
        problems.append(
            f'규칙이 severity="{sev}" 를 쓰는데 alertmanager.yml 에 그 라우트가 없다 '
            "— 그 알람은 default 로 떨어지고 지정한 대기·반복 간격을 못 받는다")
    for sev in sorted(routed - used):
        problems.append(
            f'alertmanager.yml 이 severity="{sev}" 를 라우팅하는데 그 severity 를 '
            "쓰는 규칙이 없다 — 낡은 라우트다")

    # ④ 억제 규칙이 실재하는 알람을 가리키는가. 없는 알람을 억제원으로 두면
    #    억제가 영원히 안 걸리고, 그 사실이 어디에도 안 남는다.
    names = set(re.findall(r"-\s*alert:\s*(\S+)", RULES.read_text(encoding="utf-8")))
    for ref in re.findall(r'alertname\s*=\s*"([A-Za-z]+)"', am):
        if ref not in names:
            problems.append(f'alertmanager.yml 의 억제 규칙이 없는 알람 "{ref}" 를 가리킨다')
    for group in re.findall(r'alertname\s*=~\s*"([^"]+)"', am):
        for ref in group.split("|"):
            if ref and ref not in names:
                problems.append(
                    f'alertmanager.yml 의 억제 규칙이 없는 알람 "{ref}" 를 가리킨다')
    return problems


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

    problems += _delivery_problems(prom, compose)

    for p in problems:
        print(f"::error::{p}" if "GITHUB_ACTIONS" in os.environ else p)
    if problems:
        return 1
    print("관측 배선 검증 통과 (기본 데이터소스 · 대시보드 마운트 · 규칙 파일 마운트)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
