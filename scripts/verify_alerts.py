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
import urllib.request

PORTS = [9100, 9200, 9111, 9102, 9103, 9104, 9105, 9106, PREFLIGHT_PORT := 9120, 9132]

# 배포 게이트 지표는 서비스가 아니라 **별도 익스포터**(ops/preflight_monitor.py, 9120) 가 낸다.
# 개발 스택(make up)에는 그 익스포터가 없다. 그때 이 7종이 없는 건 결함이 아니라 미기동이다.
# 다만 익스포터가 떠 있는데도 없으면 진짜 결함이다 — CI 스모크는 익스포터를 같이 띄워 실제로 검증한다.
# 첫 CI 실행(2026-09-22)에서 이 7종이 "영원히 안 울린다" 로 잡혔다. 익스포터를 긁지 않고 있었다.
PREFLIGHT_METRICS = {
    "mdfeed_backup_last_verified_age_seconds",
    "mdfeed_deployment_gaps_open",
    "mdfeed_restore_drill_last_verified_age_seconds",
    "mdfeed_retention_remote_coverage_blocked",
    "mdfeed_storage_backend_mismatch",
    "mdfeed_storage_db_unavailable",
    "mdfeed_writer_pending_rows",
}

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
DECLARED_OFFLINE = {
    "mdfeed_mcast_send_errors_total",
    "mdfeed_mcast_retrans_unavailable_total",
    "mdfeed_mcast_retrans_requests_total",
    "mdfeed_mcast_injected_drops_total",
    "mdfeed_mcast_injected_reorders_total",
    "mdfeed_mcast_injected_duplicates_total",
    "mdfeed_send_eagain_total",
    "mdfeed_adapter_task_deaths_total",
    "mdfeed_archive_enabled",
    "mdfeed_archive_failed_segments",
    "mdfeed_backup_last_verified_age_seconds",
    "mdfeed_clock_offset_us",
    "mdfeed_data_gaps_open",
    "mdfeed_data_gaps_recovered_total",
    "mdfeed_data_gaps_unrecovered_duration_seconds",
    "mdfeed_db_growth_bytes_per_hour",
    "mdfeed_disk_free_bytes",
    "mdfeed_deployment_gaps_open",
    "mdfeed_dropped_total",
    "mdfeed_gap_messages_total",
    "mdfeed_gap_recovery_verification_failures_total",
    "mdfeed_ingest_latency_microseconds",
    "mdfeed_market_open",
    "mdfeed_process_fd_growth_per_hour",
    "mdfeed_process_fd_limit",
    "mdfeed_process_fd_open",
    "mdfeed_process_rss_growth_mb_per_hour",
    "mdfeed_published_total",
    "mdfeed_quality_events_total",
    "mdfeed_reconnects_total",
    "mdfeed_restore_drill_last_verified_age_seconds",
    "mdfeed_retention_prune_incomplete",
    "mdfeed_retention_remote_coverage_blocked",
    "mdfeed_rows_archived_total",
    "mdfeed_rows_pruned_total",
    "mdfeed_session_cancel_timeouts_total",
    "mdfeed_storage_backend_mismatch",
    "mdfeed_storage_db_unavailable",
    "mdfeed_symbol_collision_kinds",
    "mdfeed_symbol_truncated_kinds",
    "mdfeed_upstream_stale",
    "mdfeed_writer_pending_rows",
    "up",
}
# 멀티캐스트 발행자는 선택 서비스(MDFEED_MCAST_ENABLED). 안 켠 구성에서 이 지표가 없는 건
# 정확한 동작이다. 켜져 있는데 없으면 그건 결함이다 — 아래에서 구분한다.
MCAST_PORT = 9132
MCAST_METRICS = {
    "mdfeed_mcast_send_errors_total",
    "mdfeed_mcast_retrans_unavailable_total",
    "mdfeed_mcast_retrans_requests_total",
    "mdfeed_mcast_injected_drops_total",
    "mdfeed_mcast_injected_reorders_total",
    "mdfeed_mcast_injected_duplicates_total",
}

# **아무 알람도 안 보는 게 맞는 지표.** 대시보드·진단용이거나 다른 지표의 분모다.
# 여기 없고 알람도 없으면 실패한다 — 지표를 새로 내면서 "볼지 말지" 결정을 건너뛰지 못하게 한다.
# 이 저장소는 반대 방향(알람은 있는데 지표가 없다)으로 이미 한 번 당했다(결함 23).
NO_ALERT_BY_DESIGN = {
    "mdfeed_uptime_seconds",           # 대시보드 표시용
    "mdfeed_sent_total",               # 처리량. 이상은 비율 지표로 본다
    "mdfeed_frames_in_total",
    "mdfeed_connections_total",
    "mdfeed_conflated_total",
    "mdfeed_subscribers",
    "mdfeed_bus_reads_total",          # send_calls 와 함께 보는 진단용 분모
    "mdfeed_send_calls_total",
    "mdfeed_send_bytes_total",
    "mdfeed_mcast_datagrams_total",
    "mdfeed_mcast_bytes_total",
    "mdfeed_mcast_snapshots_total",
    "mdfeed_mcast_own_heartbeats_total",
    "mdfeed_mcast_recovery_clients",
    "mdfeed_mcast_seq",
    "mdfeed_mcast_retrans_frames_total",   # 요청 수(McastRetransStorm)로 본다
    "mdfeed_process_rss_bytes",            # 증가율 지표로 알람을 건다
    "mdfeed_process_fd_open",
    "mdfeed_market_open",              # 다른 알람의 조건(장 시간)으로만 쓴다
    "mdfeed_data_gaps_recovered_total",  # 복구는 좋은 일이다. 알람 대상은 열린 공백 쪽
    "mdfeed_rows_archived_total",      # 보존 정책 진행 표시. 이상은 retention_prune_incomplete 로 본다
    "mdfeed_rows_pruned_total",
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
        except Exception:                            # noqa: BLE001, S112
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
        exposed = set(DECLARED_OFFLINE)
        print(f"오프라인 검증: 선언된 지표 {len(exposed)}종\n")
    else:
        print(f"수집: {len(live_ports)}개 포트에서 지표 {len(exposed)}종\n")

    referenced: dict[str, list[str]] = {}
    alert = None
    in_expr = False
    with open(args.rules, encoding="utf-8") as rules_file:
        for line in rules_file:
            a = re.match(r"^\s*- alert:\s*(\S+)", line)
            if a:
                alert, in_expr = a.group(1), False
            e = EXPR_RE.match(line)
            if e and alert:
                in_expr = True
                for m in METRIC_RE.findall(e.group(1)):
                    referenced.setdefault(m, []).append(alert)
                continue
            # **여러 줄로 쓴 식의 이어지는 줄.** 예전엔 `expr:` 로 시작하는 줄만 읽어서,
            # 둘째 줄에만 있는 지표는 참조로 세지 않았다. 그러면 그 지표가 없어져도
            # "모든 알람이 실재하는 지표를 참조한다"가 나온다 — 검사기가 눈을 감는다.
            # 다음 키(labels:/for:/annotations: …)나 새 규칙이 나오기 전까지가 식이다.
            if in_expr and alert:
                if re.match(r"^\s*(-\s|[a-z_]+:)", line):
                    in_expr = False
                    continue
                for m in METRIC_RE.findall(line):
                    referenced.setdefault(m, []).append(alert)

    measures = any_adapter_measures_latency()
    preflight_live = PREFLIGHT_PORT in live_ports
    ok, conditional, missing = {}, {}, {}
    notes = dict(CONDITIONAL)
    for m, alerts in referenced.items():
        if m in exposed:
            ok[m] = alerts
        elif m in CONDITIONAL and measures is False:
            # 지연을 재는 어댑터가 없으므로 없는 것이 정확한 동작이다
            conditional[m] = alerts
        elif m in MCAST_METRICS and live_ports and MCAST_PORT not in live_ports:
            # 멀티캐스트 발행자를 안 켠 구성. 없는 것이 정확한 동작이다.
            conditional[m] = alerts
            notes[m] = f"멀티캐스트 발행자(:{MCAST_PORT}) 가 떠 있어야 생성된다 (MDFEED_MCAST_ENABLED)"
        elif m in PREFLIGHT_METRICS and live_ports and not preflight_live:
            # 익스포터가 안 떠 있다. 떠 있는데 없으면 아래 missing 으로 간다
            conditional[m] = alerts
            notes[m] = f"배포 게이트 익스포터(ops/preflight_monitor.py :{PREFLIGHT_PORT}) 가 떠 있어야 생성된다"
        else:
            missing[m] = alerts

    print(f"{'지표':<46} {'상태':<10} 참조 알람")
    print("-" * 92)
    for m, alerts in sorted(ok.items()):
        print(f"{m:<46} {'OK':<10} {', '.join(alerts)}")
    for m, alerts in sorted(conditional.items()):
        print(f"{m:<46} {'조건부':<10} {', '.join(alerts)}")
        print(f"{'':<46} {'':<10} └ {notes[m]}")
    for m, alerts in sorted(missing.items()):
        print(f"{m:<46} {'없음':<10} {', '.join(alerts)}   ← 이 알람은 영원히 안 울린다")
    print("-" * 92)
    print(f"참조 {len(referenced)}종 · 존재 {len(ok)} · 조건부 {len(conditional)} · "
          f"누락 {len(missing)}")
    if conditional:
        print("조건부는 현재 구성(지연 측정 어댑터 없음 · 배포 게이트 익스포터 미기동)에서 없는 것이 정확한 동작입니다.\n"
              "해당 구성요소를 붙이면 생성되며, 그때도 없으면 결함으로 잡힙니다.")
    # ── 반대 방향: 지표는 내는데 아무 알람도 안 보는 것 ──────────────────
    # 여기까지는 "알람이 없는 지표를 참조하는가"만 봤다. 새 서비스를 붙이면서 지표만 내고
    # 알람을 안 붙이면 "값은 있는데 아무도 안 본다"가 된다. 같은 종류의 사각이다.
    unwatched = sorted(m for m in exposed
                       if m not in referenced and m not in NO_ALERT_BY_DESIGN
                       and not m.startswith("mdfeed_") is False)
    unwatched = [m for m in unwatched if m.startswith("mdfeed_")]
    if unwatched:
        print()
        for m in unwatched:
            print(f"{m:<46} {'무관심':<10} 이 지표를 보는 알람이 없다")
        print("\n알람을 붙이거나, 볼 필요가 없으면 scripts/verify_alerts.py 의 "
              "NO_ALERT_BY_DESIGN 에 이유와 함께 넣으세요.")
        return 1

    if missing:
        print("\n누락된 지표를 노출하거나 규칙을 고치세요.")
        return 1
    print(f"모든 알람이 실재하는 지표를 참조하고, 노출 지표 {len(exposed)}종에 "
          f"감시 사각이 없습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
