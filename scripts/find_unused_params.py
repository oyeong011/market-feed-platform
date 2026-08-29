#!/usr/bin/env python3
"""받아 놓고 안 쓰는 인자를 찾는다.

왜 이걸 도구로 만드나
---------------------
2026-08-29 하루에 같은 모양의 결함이 세 번 나왔다.

* 감시자가 어댑터의 장 시간 판정 훅을 **안 거치고** 유휴를 다시 계산했다
  → 휴장 중 시간당 18회 재접속
* 게이트웨이가 큐 깊이만 보고 **transport 버퍼를 안 봤다**
  → 65KB 가 밀려 있는데 지표는 0
* `PriceJumpCheck.check` 가 `ts_ns` 를 **인자로 받아 놓고 간격에 안 썼다**
  → "한 틱에 30% 이동"이 사실은 11시간 만의 첫 틱

셋 다 "있는데 안 쓴다"다. 마지막 것은 기계가 찾을 수 있는 형태였다.
인자를 받는다는 건 그 값이 판단에 필요하다는 선언이고, 안 쓰면
**선언과 구현이 어긋난 채로 조용히 통과한다.**

한계
----
안 쓰는 인자가 전부 결함은 아니다. 인터페이스를 맞추려고 받는 경우
(공통 시그니처, 콜백 규약)가 정상적으로 존재한다. 그래서 이 도구는
**판정하지 않고 목록만 낸다.** `_` 로 시작하는 이름은 "일부러 안 쓴다"는
표시로 보고 건너뛴다 — 의도를 이름으로 적으면 여기서 빠진다.

    python scripts/find_unused_params.py           # 목록
    python scripts/find_unused_params.py --check   # 하나라도 있으면 exit 1
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

SKIP_NAMES = {"self", "cls"}


def _params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.arg]:
    a = fn.args
    return [*a.posonlyargs, *a.args, *a.kwonlyargs]


def _used_names(fn) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Name):
            out.add(node.id)
        elif isinstance(node, ast.Attribute):
            pass                       # Name 방문에서 잡힌다
    return out


def _is_stub(fn) -> bool:
    """`...` / `pass` / `raise NotImplementedError` 만 있는 추상 메서드는 대상이 아니다."""
    body = [n for n in fn.body if not isinstance(n, ast.Expr)
            or not isinstance(n.value, ast.Constant)]
    if not body:
        return True
    return all(
        isinstance(n, ast.Pass)
        or (isinstance(n, ast.Raise) and "NotImplementedError" in ast.dump(n))
        for n in body)


def scan(path: Path) -> list[tuple[int, str, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if _is_stub(node):
            continue
        used = _used_names(node)
        for p in _params(node):
            if p.arg in SKIP_NAMES or p.arg.startswith("_"):
                continue
            if p.arg not in used:
                found.append((p.lineno, node.name, p.arg))
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="하나라도 있으면 exit 1")
    args = ap.parse_args()

    total = 0
    for f in sorted(SRC.rglob("*.py")):
        hits = scan(f)
        if not hits:
            continue
        rel = f.relative_to(ROOT)
        for line, fn, param in hits:
            print(f"{rel}:{line}  {fn}() 가 {param!r} 를 받고 안 쓴다")
            total += 1

    print(f"\n받아 놓고 안 쓰는 인자 {total}개")
    if total:
        print("전부 결함은 아니다 — 인터페이스를 맞추려 받는 경우가 있다.")
        print("의도한 것이면 이름 앞에 _ 를 붙여 표시한다.")
    return 1 if (args.check and total) else 0


if __name__ == "__main__":
    raise SystemExit(main())
