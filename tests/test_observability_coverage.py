"""관측 안전망 세 방향이 서로 어긋나지 않는가.

`scripts/verify_alerts.py` 는 세 방향을 본다.

1. 알람 → 지표   없는 지표를 참조하는 알람은 영원히 안 울린다 (결함 23)
2. 지표 → 알람   내기만 하고 아무도 안 보는 지표 (결함 33)
3. 알람 → 대시보드  울렸는데 볼 그래프가 없는 알람 (결함 45와 함께 추가)

그 검사기는 스택이 떠 있어야 완전히 돈다. 이 시험은 스택 없이 **선언만으로** 판정할 수
있는 부분을 CI 기본 경로에 고정한다.

무엇보다 **2번이 실질적으로 비어 있던 사건**을 다시 만들지 않게 한다(결함 45).
역방향 판정을 `DECLARED_OFFLINE` 으로 하는데, 그 목록에는 알람이 참조하는 43종만 들어
있었다. 그러니 "참조되지 않은 지표"가 나올 수 없었고 결과는 항상 "사각 없음" 이었다 —
검사기가 자기 입력을 자기 답으로 쓰고 있었다. 실측으로 목록을 맞추자 무감시 지표가
40종이었고, 그 안에 **적재 못 한 행을 세는 카운터**가 있었다.

그래서 이 파일은 "검사가 통과했다"가 아니라 **"검사에 판정할 재료가 있다"**도 요구한다.
"""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RULES = ROOT / "ops" / "observability" / "alerts.yml"
DASHBOARD = ROOT / "ops" / "observability" / "grafana" / "dashboards" / "mdfeed.json"


def _verifier():
    spec = importlib.util.spec_from_file_location(
        "verify_alerts", ROOT / "scripts" / "verify_alerts.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _alerts() -> dict[str, dict]:
    """알람 이름 → {"severity", "metrics"}. 검사기와 같은 방식으로 읽는다."""
    out: dict[str, dict] = {}
    alert, in_expr = None, False
    for line in RULES.read_text(encoding="utf-8").splitlines(keepends=True):
        a = re.match(r"^\s*- alert:\s*(\S+)", line)
        if a:
            alert, in_expr = a.group(1), False
            out[alert] = {"severity": None, "metrics": set()}
        if alert is None:
            continue
        sev = re.search(r"severity:\s*(\w+)", line)
        if sev:
            out[alert]["severity"] = sev.group(1)
        e = re.match(r"^\s*expr:\s*(.+)$", line)
        body = None
        if e:
            in_expr, body = True, e.group(1)
        elif in_expr:
            if re.match(r"^\s*(-\s|[a-z_]+:)", line):
                in_expr = False
            else:
                body = line
        if body:
            out[alert]["metrics"] |= set(re.findall(r"\bmdfeed_[a-z0-9_]+", body))
            if re.search(r"\bup\{", body):
                out[alert]["metrics"].add("up")
    return out


def _panel_metrics() -> set[str]:
    dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    found: set[str] = set()
    for panel in dash.get("panels", []):
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            found |= set(re.findall(r"\bmdfeed_[a-z0-9_]+", expr))
            if re.search(r"\bup\{", expr):
                found.add("up")
    return found


# ── 1. 알람 → 지표 ───────────────────────────────────────────────────────
def test_every_alert_references_a_declared_metric():
    """선언 목록에 없는 지표를 참조하는 알람은 영원히 안 울린다."""
    declared = _verifier().DECLARED_OFFLINE
    missing = {a: sorted(v["metrics"] - declared)
               for a, v in _alerts().items() if v["metrics"] - declared}
    assert not missing, f"선언되지 않은 지표를 보는 알람: {missing}"


def test_every_alert_actually_references_a_metric():
    """지표를 하나도 참조하지 않는 알람은 무엇을 보는지 알 수 없다."""
    empty = sorted(a for a, v in _alerts().items() if not v["metrics"])
    assert not empty, f"참조 지표가 없는 알람: {empty}"


def test_every_alert_declares_severity():
    """심각도가 없으면 패널 요구(critical) 대상인지 판정할 수 없다."""
    nosev = sorted(a for a, v in _alerts().items() if not v["severity"])
    assert not nosev, f"severity 라벨이 없는 알람: {nosev}"


# ── 2. 지표 → 알람 ───────────────────────────────────────────────────────
def test_every_declared_metric_is_either_alerted_or_exempted():
    """내겠다고 선언한 지표는 알람이 보거나, 안 보는 이유가 적혀 있어야 한다."""
    mod = _verifier()
    referenced: set[str] = set()
    for v in _alerts().values():
        referenced |= v["metrics"]
    unwatched = sorted(m for m in mod.DECLARED_OFFLINE
                       if m.startswith("mdfeed_")
                       and m not in referenced
                       and m not in mod.NO_ALERT_BY_DESIGN)
    assert not unwatched, (
        f"이 지표를 보는 알람이 없고 면제 이유도 없다: {unwatched}")


def test_reverse_check_has_something_to_judge():
    """**결함 45 재발 방지.**

    역방향 검사의 판정 재료는 `DECLARED_OFFLINE - 알람이_참조하는_지표` 다. 그 차집합이
    비면 검사는 항상 통과한다 — 통과했다는 사실이 아무것도 뜻하지 않는다. 실제로 그랬다.

    그래서 "면제 목록에 적은 지표는 반드시 선언 목록에도 있어야 한다"를 요구한다.
    선언에 없으면 역방향 검사는 그 지표를 아예 후보로 삼지 않으므로, 면제를 적은 행위
    자체가 무의미해진다. 그리고 그때 검사가 보는 범위가 조용히 줄어든다.
    """
    mod = _verifier()
    orphan = sorted(m for m in mod.NO_ALERT_BY_DESIGN if m not in mod.DECLARED_OFFLINE)
    assert not orphan, (
        "면제했는데 선언 목록에 없다 — 역방향 검사가 이 지표를 후보로 보지 않는다: "
        f"{orphan}")

    referenced: set[str] = set()
    for v in _alerts().values():
        referenced |= v["metrics"]
    judged = {m for m in mod.DECLARED_OFFLINE
              if m.startswith("mdfeed_") and m not in referenced}
    assert len(judged) >= 20, (
        f"역방향 검사가 판정하는 지표가 {len(judged)}종뿐이다. 선언 목록이 "
        "'알람이 참조하는 것' 으로 좁아지면 이 검사는 통과가 보장된다(결함 45)")


# ── 3. 알람 → 대시보드 ───────────────────────────────────────────────────
def test_every_critical_alert_has_a_dashboard_panel():
    """새벽에 호출된 사람이 Grafana 를 열었을 때 볼 것이 있어야 한다."""
    mod = _verifier()
    panels = _panel_metrics()
    blind = {a: sorted(v["metrics"]) for a, v in _alerts().items()
             if v["severity"] == "critical" and not (v["metrics"] & panels)
             and a not in mod.NO_PANEL_BY_DESIGN}
    assert not blind, f"울려도 볼 그래프가 없는 critical 알람: {blind}"


def test_no_dashboard_panel_points_at_an_undeclared_metric():
    """죽은 지표를 가리키는 패널은 빈 그래프로 남고, '정상 0' 과 구분되지 않는다."""
    declared = _verifier().DECLARED_OFFLINE
    dead = sorted(_panel_metrics() - declared)
    assert not dead, f"선언되지 않은 지표를 가리키는 패널: {dead}"


def test_panel_exemptions_name_a_real_alert():
    """없는 알람을 면제해 두면, 진짜 알람이 생겼을 때 조용히 면제된다."""
    mod = _verifier()
    names = set(_alerts())
    stale = sorted(a for a in mod.NO_PANEL_BY_DESIGN if a not in names)
    assert not stale, f"존재하지 않는 알람을 면제하고 있다: {stale}"


# ── 선언 목록이 실제로 내는 것을 따라가는가 ──────────────────────────────
#
# 위 세 검사의 기준은 전부 `DECLARED_OFFLINE` 이다. 그 목록이 실제로 내는 지표를 안 따라가면
# 세 검사가 동시에 눈을 감는다. 결함 45 가 바로 그것이었다 — 목록에 알람이 참조하는 43종만
# 들어 있어서, 실측 82종 중 40종이 어느 검사에도 걸리지 않았다. 그 안에 적재 못 한 행을
# 세는 카운터가 있었다.
#
# 검사기는 스택을 긁어 이걸 잡지만, 스택이 떠 있어야 한다. 여기서는 **소스의 호출 지점**에서
# 이름을 뽑아 같은 비교를 한다. 지표는 `registry.counter("name", ...)` 처럼 literal 로 쓰이므로
# 정적으로 읽을 수 있다. 런타임 프리픽스(`mdfeed_`)만 붙여 준다.
CALL_RE = re.compile(
    r"registry\.(?:counter|gauge|declare_counters)\(\s*((?:\"[a-z0-9_]+\"\s*,?\s*)+)")
CPP_RE = re.compile(r'\bline\("([a-z0-9_]+)"')

# 호출 지점 이름이 그대로 지표명이 아닌 것. 히스토그램은 `_microseconds`/`_count` 로 펼쳐지고,
# 일부는 라벨만 바꿔 같은 이름으로 나간다.
NOT_A_BARE_METRIC = {
    "mdfeed_ingest_latency",           # → _microseconds · _count 로 펼쳐진다
}


def _callsite_metrics() -> set[str]:
    found: set[str] = set()
    for path in list((ROOT / "src").rglob("*.py")) + [ROOT / "ops" / "preflight_monitor.py"]:
        if "__pycache__" in str(path):
            continue
        text = path.read_text(encoding="utf-8")
        for group in CALL_RE.findall(text):
            found |= {f"mdfeed_{n}" for n in re.findall(r'"([a-z0-9_]+)"', group)}
    for path in (ROOT / "cpp" / "src").rglob("*.cpp"):
        found |= {f"mdfeed_{n}" for n in CPP_RE.findall(path.read_text(encoding="utf-8"))}
    return found - NOT_A_BARE_METRIC


def test_callsite_metrics_are_all_declared():
    """**결함 45 본체.**

    코드가 내는 지표가 선언 목록에 없으면 세 검사 전부가 그 지표를 못 본다.
    새 지표를 추가하면서 "알람을 붙일지" 결정을 건너뛸 수 없게 한다.
    """
    declared = _verifier().DECLARED_OFFLINE
    undeclared = sorted(_callsite_metrics() - declared)
    assert not undeclared, (
        f"코드가 내는데 DECLARED_OFFLINE 에 없다 ({len(undeclared)}종): {undeclared}\n"
        "선언에 넣고, 알람을 붙이거나 NO_ALERT_BY_DESIGN 에 이유를 적으세요.")


def test_callsite_scan_actually_finds_metrics():
    """스캔이 0종을 찾아도 위 시험은 통과한다. 그 통과는 아무것도 뜻하지 않는다."""
    found = _callsite_metrics()
    assert len(found) >= 70, f"호출 지점 스캔이 {len(found)}종만 찾았다 — 정규식이 낡았다"


# ── 알람 규칙이 스스로 선언한 규약을 지키는가 ────────────────────────────
#
# `ops/observability/alerts.yml` 머리에 이렇게 적혀 있다.
#
#   "**울리면 사람이 할 일이 있어야 한다.** 보고 넘길 알람은 알람이 아니라 대시보드
#    항목이다. 그래서 각 규칙에 무엇을 하라는 문장을 붙였다."
#
# 실제로는 48건 중 한 건에 `action` 이 없었다. 규약을 글로만 적으면 이렇게 된다.
# PyYAML 을 쓰지 않는다 — 핵심 경로 의존성 0 을 시험에서도 지킨다. 진짜 파싱은
# CI 의 promtool 이 한다.
def test_every_alert_says_what_to_do():
    text = RULES.read_text(encoding="utf-8")
    blocks = re.split(r"^(?=[ \t]*- alert:)", text, flags=re.M)[1:]
    assert len(blocks) >= 40, f"규칙을 {len(blocks)}건만 읽었다 — 정규식이 낡았다"
    missing = []
    for block in blocks:
        name = re.match(r"[ \t]*- alert:[ \t]*(\S+)", block).group(1)
        need = [k for k in ("summary", "action") if f"{k}:" not in block]
        if "for:" not in block:
            need.append("for")
        if need:
            missing.append((name, need))
    assert not missing, (
        "알람 규칙 파일은 '각 규칙에 무엇을 하라는 문장을 붙였다' 고 선언한다. "
        f"안 지킨 규칙: {missing}")


def test_no_duplicate_keys_in_alert_rules():
    """**중복 키 하나가 알람 48개를 동시에 죽인다.**

    Prometheus(Go yaml)는 같은 매핑에 같은 키가 두 번 나오면 **그 파일 전체**를 거부한다.
    규칙이 하나도 안 등록되고, 어디에도 빨간 줄이 안 뜬다.

    실제로 그랬다. `ArchiveStalled` 에 `action` 이 두 개 있었다 — 하나는
    `RetentionNotRunning` 의 문장이 잘못 붙은 것이었다. 2026-08-28 에 들어와 한 달간
    **모든 알람이 죽어 있었다**(결함 47).

    왜 안 걸렸나. `verify_alerts.py` 는 지표 이름을 정규식으로만 읽는다 — 파일이 파싱되는지
    안 본다. PyYAML 로 읽어도 안 걸린다. **PyYAML 은 중복 키를 조용히 덮어쓴다** (그래서
    '`action` 이 없는 규칙' 이라는 증상만 보이고 원인은 안 보였다). 진짜 파서를 붙인
    CI 의 `promtool check rules` 첫 실행이 잡았다 — 결함 44 와 같은 구조다.

    여기서는 의존성 없이 같은 것을 본다. 들여쓰기 깊이로 블록을 나누고 키 중복을 센다.
    """
    dupes: list[str] = []
    stack: list[tuple[int, set[str]]] = []
    for lineno, raw in enumerate(RULES.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip())
        key = re.match(r"(?:-\s+)?([a-z_]+):(?:\s|$)", raw.strip())
        if raw.lstrip().startswith("- "):
            # 새 리스트 항목은 새 매핑이다
            while stack and stack[-1][0] >= indent:
                stack.pop()
            stack.append((indent + 2, set()))
            if key:
                stack[-1][1].add(key.group(1))
            continue
        if not key:
            continue                              # 여러 줄로 이어지는 값
        while stack and stack[-1][0] > indent:
            stack.pop()
        if not stack or stack[-1][0] < indent:
            stack.append((indent, set()))
        seen = stack[-1][1]
        if key.group(1) in seen:
            dupes.append(f"{RULES.name}:{lineno} '{key.group(1)}' 가 같은 블록에 두 번")
        seen.add(key.group(1))
    assert not dupes, (
        "중복 키가 있으면 Prometheus 는 규칙 파일 전체를 거부한다 — 알람이 전멸한다:\n  "
        + "\n  ".join(dupes))
