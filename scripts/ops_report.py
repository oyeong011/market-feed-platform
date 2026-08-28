#!/usr/bin/env python3
"""운영 스냅샷을 남긴다.

financial-database 에서 배운 것을 그대로 옮겼다. 거기서는 매일 03:00 에
DB 실측값으로 README 를 다시 만들어 커밋한다. 그 덕에 "매일 돈다"가
확인 가능한 문장이 됐고, 빠진 날 하루도 커밋 이력으로 알았다.

여기서도 같은 걸 한다. 헬스 엔드포인트에서 실측값을 받아 하루치 기록을
남긴다. "돌아갑니다"는 확인할 수 없지만 "8/28 12.3시간 · 재시작 0 · 570만건"은
확인할 수 있다.

  python3 scripts/ops_report.py              # 사람이 읽는 표
  python3 scripts/ops_report.py --json out.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
# 9101 은 tcp-gateway 의 MDFP 데이터 포트다. HTTP 가 아니라서 여기 없다.
# 처음에 넣었다가 헬스 조회에 바이너리 프레임이 돌아왔다.
# 샤딩하면 feedd 가 venue 그룹마다 하나씩 뜬다(9100 crypto / 9200 krx).
# 처음엔 9100 만 봤다가 KRX 샤드를 통째로 놓쳤다. 단일 노드를 가정한 리포트는
# 샤딩된 배포에서 조용히 절반만 보고한다.
# 9111 은 tcp_gateway 의 admin 이다. 데이터 포트(9101)와 번호가 떨어져 있어
# 목록에서 빠져 있었고, 그래서 리포트는 8개 중 7개만 보고 "전부 정상"이라 했다.
PORTS = {
    9100: "feedd", 9200: "feedd:krx", 9102: "ws-gateway", 9103: "rest-api",
    9104: "writer", 9105: "strategy", 9106: "quality", 9111: "tcp-gateway",
}


def probe(port: int, timeout: float = 3.0) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        # 조회 실패는 그 자체가 정보다. 어떤 이유든 "응답 없음"으로 기록한다.
        # HTTP 가 아닌 포트를 찌르면 BadStatusLine 이 나온다.
        return None


def collect() -> dict:
    now = datetime.now(KST)
    services, up = {}, []
    for port, name in PORTS.items():
        h = probe(port)
        if h is None:
            continue
        services[name] = h
        up.append(h.get("uptime_s", 0.0))

    feedd = services.get("feedd", {})
    writer = services.get("writer", {})
    quality = services.get("quality", {})

    # 유실은 writer 가 버스별로 센다. 합쳐서 본다.
    seq = writer.get("sequence", {}) or {}
    lost = sum(v.get("lost_messages", 0) for v in seq.values())
    gaps = sum(v.get("gap_count", 0) for v in seq.values())
    dups = sum(v.get("duplicate_count", 0) for v in seq.values())

    # 업스트림 — 정체와 재접속은 따로 본다.
    # 재접속 0 이면서 정체면 재접속이 안 도는 것이다. 이게 실제로 있었다.
    ups = []
    feedds = [(n, h) for n, h in services.items() if n.startswith("feedd")]
    for shard, h in feedds:
        for u in h.get("upstreams", []) or []:
            u = dict(u, venue=f"{u.get('venue')}({shard.split(':')[-1]})"
                     if len(feedds) > 1 else u.get("venue"))
            ups.append({
                "venue": u.get("venue"),
                "messages": u.get("messages", 0),
                "reconnects": u.get("reconnects", 0),
                "errors": u.get("errors", 0),
                "last_msg_age_s": u.get("last_msg_age_s"),
                "stale": u.get("stale", False),
                # 휴장 중이라 조용한 것인지, 죽어서 조용한 것인지 구분한다.
                "expects_data": u.get("expects_data", True),
            })

    # 꺼진 업스트림은 리포트에서 제일 먼저 보여야 한다. 실제로 KRX 샤드가
    # 자격증명 미설정으로 통째로 꺼져 있었는데, 활성만 찍던 리포트에는
    # "이상 없음"으로 보였다. 안 보이는 것과 없는 것은 다르다.
    inactive = []
    for n, h in services.items():
        for u in h.get("inactive_upstreams", []) or []:
            inactive.append({"shard": n, "venue": u.get("venue"),
                             "reason": u.get("reason")})

    checked = quality.get("checked", 0)
    critical = quality.get("critical", 0)

    return {
        "captured_at": now.isoformat(timespec="seconds"),
        "date": now.date().isoformat(),
        "services_up": len(services),
        "services_expected": len(PORTS),
        "unhealthy": [n for n, h in services.items() if h.get("healthy") is False],
        "uptime_h": round(min(up) / 3600, 2) if up else 0.0,
        "throughput": {
            "feedd_seq": sum(h.get("seq", 0) for n, h in services.items()
                             if n.startswith("feedd")),
            "rows_written": writer.get("rows_written", 0),
            "bars_written": writer.get("bars_written", 0),
            "signals": services.get("strategy", {}).get("signals_emitted", 0),
        },
        "integrity": {
            "gap_count": gaps, "lost_messages": lost, "duplicate_count": dups,
            "bus_dropped": sum(h.get("bus_dropped", 0) for n, h in services.items()
                               if n.startswith("feedd")),
        },
        "quality": {
            "checked": checked, "critical": critical,
            "warning": quality.get("warning", 0),
            "critical_rate_pct": round(critical / checked * 100, 4) if checked else 0.0,
            "by_check": quality.get("by_check", {}),
        },
        "upstreams": ups,
        "inactive_upstreams": inactive,
        "memory_mb": {
            n: (h.get("resources") or {}).get("rss_mb")
            for n, h in services.items() if (h.get("resources") or {}).get("rss_mb")
        },
    }


def render(r: dict) -> str:
    L = [f"# 운영 기록 {r['date']}", "",
         f"수집 시각 {r['captured_at']}", ""]
    L += ["| 항목 | 값 |", "|---|---|",
          f"| 연속 가동 | **{r['uptime_h']}시간** |",
          f"| 서비스 | {r['services_up']} / {r['services_expected']} |"]
    if r["unhealthy"]:
        L.append(f"| 비정상 | **{', '.join(r['unhealthy'])}** |")
    t, i, q = r["throughput"], r["integrity"], r["quality"]
    L += [f"| 누적 메시지 | {t['feedd_seq']:,} |",
          f"| 적재 행 | {t['rows_written']:,} |",
          f"| 생성 봉 | {t['bars_written']:,} |",
          f"| 시퀀스 갭 | {i['gap_count']} (유실 {i['lost_messages']:,}) |",
          f"| 버스 드롭 | {i['bus_dropped']:,} |",
          f"| 품질 검사 | {q['checked']:,}건 · CRITICAL {q['critical']:,} ({q['critical_rate_pct']}%) |",
          ""]
    L += ["| 거래소 | 메시지 | 재접속 | 마지막 수신 | 데이터 기대 | 정체 |",
          "|---|---|---|---|---|---|"]
    for u in r["upstreams"]:
        age = u["last_msg_age_s"]
        mark = " ⚠" if u["stale"] else ""
        exp = "개장" if u.get("expects_data", True) else "휴장"
        L.append(f"| {u['venue']} | {u['messages']:,} | {u['reconnects']} | "
                 f"{age if age is None else f'{age:.0f}s'} | {exp} | {u['stale']}{mark} |")
    if r.get("inactive_upstreams"):
        L += ["", "## 꺼져 있는 수집 경로", "",
              "| 샤드 | 경로 | 이유 |", "|---|---|---|"]
        for u in r["inactive_upstreams"]:
            L.append(f"| {u['shard']} | {u['venue']} | {u['reason']} |")
        L += ["", "> 꺼진 경로는 조용하다. 활성만 세면 이상 없음으로 보인다."]
    # 정체를 못 잡는 것과 반대 방향의 실패: 멀쩡한데 계속 끊는 경우.
    # 휴장 중 워치독이 "조용함"을 고장으로 읽어 30초마다 재접속한 적이 있다.
    hours = max(r["uptime_h"], 0.1)
    storm = [u for u in r["upstreams"] if u["reconnects"] / hours > 10]
    if storm:
        L += ["", "## 재접속이 잦은 경로", "", "| 거래소 | 재접속 | 시간당 |",
              "|---|---|---|"]
        for u in storm:
            L.append(f"| {u['venue']} | {u['reconnects']} | "
                     f"{u['reconnects'] / hours:.0f}회 |")
        L += ["", "> 시간당 10회를 넘으면 재접속이 복구가 아니라 증상이다. "
              "브로커 쪽에서 키 단위로 차단될 수 있다."]
    if any(u["stale"] and u["reconnects"] == 0 for u in r["upstreams"]):
        L += ["", "> 정체인데 재접속이 0 이면 재접속 경로가 안 도는 것이다. "
              "소켓 recv 타임아웃만으로는 이 상태를 못 잡는다."]
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="JSON 을 이 경로에 쓴다")
    ap.add_argument("--out", help="마크다운을 이 경로에 쓴다")
    a = ap.parse_args()

    r = collect()
    if r["services_up"] == 0:
        print("서비스가 하나도 응답하지 않는다. 스택이 떠 있는지 확인할 것.", file=sys.stderr)
        return 1

    md = render(r)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(r, f, ensure_ascii=False, indent=2)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(md + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
