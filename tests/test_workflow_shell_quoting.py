"""워크플로의 `run:` 이 **의도한 명령 그대로 실행되는가.**

YAML 평문 스칼라(`run: cmd \\` 처럼 키 뒤에 바로 이어 쓴 것)는 줄바꿈을 공백으로
접는다. 그러면 줄 끝의 역슬래시가 **글자 그대로** 남아 셸이 그것을 인자로 받는다.

    run: python3 x.py --a \\
           --b 1

    → bash 가 받는 것: python3 x.py --a \\ --b 1
    → error: unrecognized arguments:  --b

2026-10-04 에 알람 전달 잡이 이걸로 떨어졌다. YAML 은 유효하고, 워크플로는 돌고,
명령만 다르게 실행된다 — 이 저장소가 반복해서 만난 "조용히 틀린" 모양이다.
블록 스칼라(`run: |`)를 쓰면 줄바꿈이 보존돼 역슬래시가 제 역할을 한다.

PyYAML 을 쓰지 않는다 — 핵심 경로 의존성 0 을 시험에서도 지킨다. `run:` 값이
블록 스칼라인지는 그 줄만 보면 알 수 있다.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))

RUN_RE = re.compile(r"^(\s*)(?:-\s+)?run:(.*)$")


def _folded_runs(text: str) -> list[tuple[int, str]]:
    """평문 스칼라로 쓴 `run:` 중, 여러 줄로 이어진 것을 찾는다."""
    lines = text.splitlines()
    out: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        m = RUN_RE.match(line)
        if not m:
            continue
        indent, value = m.group(1), m.group(2).strip()
        if not value or value.startswith(("|", ">")):
            continue                      # 블록 스칼라 — 줄바꿈이 보존된다
        # 다음 줄이 더 깊게 들여써져 있으면 평문 스칼라가 접히고 있다
        for nxt in lines[i + 1:]:
            if not nxt.strip():
                break
            nxt_indent = len(nxt) - len(nxt.lstrip())
            if nxt_indent > len(indent):
                out.append((i + 1, line.strip()))
            break
    return out


def test_no_multiline_plain_scalar_run_steps():
    """여러 줄 명령은 블록 스칼라로 쓴다. 평문 스칼라는 역슬래시를 글자로 남긴다."""
    assert WORKFLOWS, "워크플로 파일을 하나도 못 찾았다 — 경로가 바뀌었다"
    bad: list[str] = []
    for wf in WORKFLOWS:
        for lineno, snippet in _folded_runs(wf.read_text(encoding="utf-8")):
            bad.append(f"{wf.name}:{lineno}  {snippet[:70]}")
    assert not bad, (
        "`run:` 뒤에 바로 이어 쓴 여러 줄 명령이 있다. YAML 이 줄바꿈을 공백으로 접어 "
        "역슬래시가 글자로 남는다 — `run: |` 로 바꾸세요:\n  " + "\n  ".join(bad))


def test_the_detector_actually_detects():
    """**통과만 확인하면 결함 45 를 또 만든다.** 잡는지 직접 본다."""
    broken = (
        "jobs:\n"
        "  x:\n"
        "    steps:\n"
        "      - name: 나쁜 예\n"
        "        run: python3 x.py --a \\\n"
        "               --b 1\n"
    )
    assert _folded_runs(broken), "접힌 평문 스칼라를 못 잡는다"

    ok = (
        "jobs:\n"
        "  x:\n"
        "    steps:\n"
        "      - name: 좋은 예\n"
        "        run: |\n"
        "          python3 x.py --a \\\n"
        "            --b 1\n"
        "      - name: 한 줄\n"
        "        run: echo hi\n"
    )
    assert not _folded_runs(ok), f"블록 스칼라를 잘못 잡는다: {_folded_runs(ok)}"


def test_every_workflow_run_step_is_parseable_shell():
    """블록 스칼라 안의 명령이 셸 문법으로 성립하는가.

    역슬래시 문제와 별개로, 따옴표가 안 닫힌 명령은 실행 시점에 처음 터진다.
    `bash -n` 으로 미리 읽어 본다.
    """
    import subprocess
    problems: list[str] = []
    for wf in WORKFLOWS:
        text = wf.read_text(encoding="utf-8")
        lines = text.splitlines()
        for i, line in enumerate(lines):
            m = RUN_RE.match(line)
            if not m or not m.group(2).strip().startswith("|"):
                continue
            indent = len(m.group(1))
            body = []
            for nxt in lines[i + 1:]:
                if nxt.strip() and (len(nxt) - len(nxt.lstrip())) <= indent:
                    break
                body.append(nxt[indent + 2:] if len(nxt) > indent + 2 else "")
            script = "\n".join(body)
            # ${{ }} 는 셸이 아니라 Actions 가 치환한다. 셸 검사 전에 치워 둔다.
            script = re.sub(r"\$\{\{[^}]*\}\}", "ACTIONS_EXPR", script)
            r = subprocess.run(["bash", "-n"], input=script, text=True,
                               capture_output=True)
            if r.returncode != 0:
                problems.append(f"{wf.name}:{i + 1}  {r.stderr.strip().splitlines()[0]}")
    assert not problems, "셸 문법이 안 맞는 run 블록:\n  " + "\n  ".join(problems)
