#!/usr/bin/env python3
"""외부 감시용 헬스체크 — Nagios 규약 종료코드(0 OK / 1 WARN / 2 CRIT).

용도
----
* Docker HEALTHCHECK
* cron 기반 감시 (`*/1 * * * * /opt/mdfeed/ops/healthcheck.py --quiet || alert`)
* 로드밸런서 헬스 프로브
* watchdog.sh 의 판정 근거

각 서비스의 /healthz 만 보는 게 아니라 **피드 서비스 고유의 품질 지표**까지 본다.
프로세스가 살아있고 포트가 열려 있어도, 틱이 안 들어오면 그건 장애다.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

OK, WARN, CRIT = 0, 1, 2
LEVEL_NAME = {OK: "OK", WARN: "WARN", CRIT: "CRIT"}

SERVICES = [
    ("feedd", 9100, True),          # True = 필수. 죽으면 CRIT
    ("tcp-gateway", 9111, True),
    ("ws-gateway", 9102, False),
    ("rest-api", 9103, False),
    ("writer", 9104, True),
    ("strategy", 9105, False),
]

# 피드 품질 임계치
MAX_TICK_AGE_S = 120.0          # 이보다 오래 틱이 없으면 CRIT (하트비트는 별개)
MAX_GAP_RATIO = 0.001           # 유실률 0.1% 초과면 WARN
MAX_CLOCK_SKEW_MS = 100.0       # 시계가 이만큼 어긋나면 지연 지표를 못 믿는다


def fetch(port: int, path: str = "/healthz", timeout: float = 3.0):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except Exception:                            # noqa: BLE001
            return None
    except Exception:                                # noqa: BLE001
        return None


def check() -> tuple[int, list[str]]:
    worst = OK
    msgs: list[str] = []

    def bump(level: int, msg: str) -> None:
        nonlocal worst
        worst = max(worst, level)
        msgs.append(f"[{LEVEL_NAME[level]}] {msg}")

    for name, port, required in SERVICES:
        body = fetch(port)
        if body is None:
            bump(CRIT if required else WARN, f"{name}: 응답 없음 (:{port})")
            continue
        if not body.get("healthy", False):
            bump(CRIT if required else WARN,
                 f"{name}: unhealthy — {body.get('reason') or body.get('clock_warning') or '사유 미상'}")
        else:
            msgs.append(f"[OK] {name}: 정상")

    # ── 피드 고유 품질 ────────────────────────────────────────────────────
    feed = fetch(9100)
    if feed:
        for up in feed.get("upstreams", []):
            age = up.get("last_msg_age_s")
            if age is None:
                bump(WARN, f"업스트림 {up['venue']}: 아직 데이터 없음")
            elif age > MAX_TICK_AGE_S:
                bump(CRIT, f"업스트림 {up['venue']}: {age:.0f}초째 무데이터 "
                           f"(임계 {MAX_TICK_AGE_S:.0f}s) — 세션이 half-open 일 수 있음")
            if up.get("reconnects", 0) > 20:
                bump(WARN, f"업스트림 {up['venue']}: 재접속 {up['reconnects']}회 — 회선/거래소 점검")
        for venue, c in (feed.get("clock") or {}).items():
            skew_ms = abs(c.get("offset_us", 0)) / 1000.0
            if skew_ms > MAX_CLOCK_SKEW_MS:
                bump(WARN, f"시계 오프셋 {venue}: {skew_ms:.0f}ms — "
                           f"NTP 동기화 확인 (지연 지표 신뢰도 저하)")

    w = fetch(9104)
    if w:
        seq = w.get("sequence") or {}
        lost = seq.get("lost_messages", 0)
        total = max(w.get("frames_in", 0), 1)
        ratio = lost / total
        if ratio > MAX_GAP_RATIO:
            bump(WARN, f"시퀀스 유실률 {ratio*100:.3f}% ({lost:,}/{total:,}) — "
                       f"버스 백프레셔 또는 소비자 지연")
        if w.get("db_errors", 0) > 0:
            bump(WARN, f"DB 오류 {w['db_errors']}건 — 저장소 점검")
        if w.get("pending_rows", 0) > 50_000:
            bump(WARN, f"적재 대기 {w['pending_rows']:,}행 — DB 쓰기가 못 따라감")

    return worst, msgs


def main() -> int:
    ap = argparse.ArgumentParser("healthcheck")
    ap.add_argument("--quiet", action="store_true", help="문제가 있을 때만 출력")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    status, msgs = check()
    if args.json:
        print(json.dumps({"status": ["OK", "WARN", "CRIT"][status],
                          "exit_code": status, "messages": msgs}, ensure_ascii=False))
    elif not (args.quiet and status == OK):
        for m in msgs:
            print(m)
        print(f"\n전체 판정: {['OK', 'WARNING', 'CRITICAL'][status]}")
    return status


if __name__ == "__main__":
    sys.exit(main())
