#!/usr/bin/env python3
"""Grafana 대시보드가 프로비저닝될 수 있는 모양인지 검사한다.

지표 이름이 맞는지는 `scripts/verify_alerts.py` 가 본다. 여기서는 **Grafana 가 이 파일을
읽고 패널을 그릴 수 있는가**만 본다. 둘은 다른 실패다.

`uid` 가 없으면 프로비저닝이 조용히 건너뛴다. 패널 `id` 가 겹치면 Grafana 가 하나만 남긴다.
`targets` 나 `expr` 이 비면 패널은 나타나지만 영원히 빈 그래프다 — 그리고 빈 그래프는
"값이 0" 과 구분되지 않는다. 이 저장소가 계속 마주친 "조용히 틀린" 실패다.

    python scripts/check_dashboard.py [경로…]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DEFAULT = "ops/observability/grafana/dashboards/mdfeed.json"


def check(path: Path) -> list[str]:
    problems: list[str] = []
    try:
        dash = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [f"{path}: JSON 이 아니다 — {e}"]

    for key in ("uid", "title"):
        if not dash.get(key):
            problems.append(f"{path}: {key} 가 없다 (Grafana 가 프로비저닝을 건너뛴다)")

    panels = dash.get("panels")
    if not panels:
        return problems + [f"{path}: 패널이 없다"]

    seen: dict[int, str] = {}
    for panel in panels:
        title = panel.get("title") or "(제목 없음)"
        pid = panel.get("id")
        if pid is None:
            problems.append(f"{path}: '{title}' 에 id 가 없다")
        elif pid in seen:
            problems.append(
                f"{path}: 패널 id {pid} 중복 — '{seen[pid]}' 와 '{title}' "
                f"(Grafana 는 하나만 남긴다)")
        else:
            seen[pid] = title

        if not panel.get("gridPos"):
            problems.append(f"{path}: '{title}' 에 gridPos 가 없다")
        targets = panel.get("targets")
        if not targets:
            problems.append(f"{path}: '{title}' 에 질의가 없다 — 빈 패널이 된다")
            continue
        for i, target in enumerate(targets):
            if not target.get("expr"):
                problems.append(f"{path}: '{title}' 의 타깃 {i} 에 expr 이 없다")
            if not target.get("refId"):
                problems.append(f"{path}: '{title}' 의 타깃 {i} 에 refId 가 없다")
    return problems


def main(argv: list[str]) -> int:
    paths = [Path(p) for p in (argv or [DEFAULT])]
    problems: list[str] = []
    for path in paths:
        if not path.exists():
            problems.append(f"{path}: 없는 파일")
            continue
        problems += check(path)
    for p in problems:
        print(f"::error::{p}" if "GITHUB_ACTIONS" in __import__("os").environ else p)
    if problems:
        return 1
    total = sum(len(json.loads(p.read_text(encoding="utf-8"))["panels"]) for p in paths)
    print(f"대시보드 {len(paths)}개 · 패널 {total}개 검증 통과")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
