#!/usr/bin/env python3
"""알람 규칙이 참조하는 지표가 실제로 존재하는지 검증한다.

**존재하지 않는 지표를 참조하는 알람은 영원히 울리지 않는다.**
그런데 Prometheus 는 아무 오류도 내지 않는다 — 그냥 조용히 no data 다.
설정 파일은 문법적으로 완벽하고, 대시보드에도 규칙이 보이고, 아무도 이상을 못 느낀다.
이 프로젝트가 계속 마주친 "조용히 틀린" 실패의 한 종류다.

그래서 규칙에서 지표 이름을 뽑아 실행 중인 서비스의 /metrics 와 대조한다.

세 방향을 본다. 하나라도 빠지면 사각이 남는다.

1. 알람 → 지표: 없는 지표를 참조하는 알람은 영원히 안 울린다 (결함 23)
2. 지표 → 알람: 내기만 하고 아무도 안 보는 지표 (결함 33)
3. 알람 → 대시보드: **울렸는데 볼 그래프가 없는 알람.** 새벽에 호출된 사람이
   Grafana 를 열면 빈 화면이다. 그리고 반대로, 죽은 지표를 가리키는 패널은
   빈 그래프로 남는데 아무도 그게 고장인지 원래 0 인지 모른다.

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
    "mdfeed_send_eagain_total":
        "C++ 배포 게이트웨이에서만 생성된다 (파이썬 판은 asyncio 가 쓰기를 대신해 EAGAIN 을 셀 자리가 없다)",
    "mdfeed_fanout_delay_spread":
        "C++ 배포 게이트웨이에서만 생성된다 (구독자별 전송 지연을 재는 계측이 거기에만 있다)",
    "mdfeed_ingest_latency_microseconds":
        "지연을 측정하는 어댑터(measures_latency=True)가 있어야 생성된다",
    "mdfeed_clock_offset_us":
        "같은 조건. 시계 오프셋은 거래소 체결시각이 있어야 추정할 수 있다",
    "mdfeed_market_open":
        "국내 장 시간을 아는 어댑터(KRX/KIS)가 있어야 생성된다. 리플레이 전용 구성에는 없다",
    "mdfeed_rest_rate_limited_total":
        "REST 로 폴링하는 어댑터(KIS)가 붙어 있어야 생성된다. 웹소켓·리플레이 구성에는 없다",
}
DECLARED_OFFLINE = {
    "mdfeed_adapter_errors_total",
    "mdfeed_adapter_silent_exits_total",
    "mdfeed_adapter_task_deaths_total",
    "mdfeed_archive_enabled",
    "mdfeed_archive_failed_segments",
    "mdfeed_backup_last_verified_age_seconds",
    "mdfeed_bars_closed_total",
    "mdfeed_bars_written_total",
    "mdfeed_bus_drops_total",
    "mdfeed_bus_reads_total",
    "mdfeed_clock_offset_us",
    "mdfeed_coalesced_batches_total",
    "mdfeed_conflated_total",
    "mdfeed_connections_total",
    "mdfeed_counts_scan_seconds",
    "mdfeed_data_gaps_open",
    "mdfeed_data_gaps_recovered_total",
    "mdfeed_data_gaps_unrecovered_duration_seconds",
    "mdfeed_db_bytes",
    "mdfeed_db_errors_total",
    "mdfeed_db_growth_bytes_per_hour",
    "mdfeed_db_reclaimable_bytes",
    "mdfeed_deployment_gaps_open",
    "mdfeed_disk_free_bytes",
    "mdfeed_disk_hours_until_full",
    "mdfeed_dropped_total",
    "mdfeed_entitlement_denied_total",
    "mdfeed_fanout_delay_max_us",
    "mdfeed_fanout_delay_spread",
    "mdfeed_frames_in_total",
    "mdfeed_gap_messages_total",
    "mdfeed_gap_recovery_verification_failures_total",
    "mdfeed_http_errors_total",
    "mdfeed_http_requests_total",
    "mdfeed_ingest_latency_microseconds",
    "mdfeed_market_open",
    "mdfeed_max_backlog",
    "mdfeed_max_wire_bytes",
    "mdfeed_mcast_bytes_total",
    "mdfeed_mcast_datagrams_total",
    "mdfeed_mcast_injected_drops_total",
    "mdfeed_mcast_injected_duplicates_total",
    "mdfeed_mcast_injected_reorders_total",
    "mdfeed_mcast_own_heartbeats_total",
    "mdfeed_mcast_recovery_clients",
    "mdfeed_mcast_retrans_frames_total",
    "mdfeed_mcast_retrans_requests_total",
    "mdfeed_mcast_retrans_unavailable_total",
    "mdfeed_mcast_send_errors_total",
    "mdfeed_mcast_seq",
    "mdfeed_mcast_snapshots_total",
    "mdfeed_price_ref_resets",
    "mdfeed_process_fd_growth_per_hour",
    "mdfeed_process_fd_limit",
    "mdfeed_process_fd_open",
    "mdfeed_process_rss_bytes",
    "mdfeed_process_rss_growth_mb_per_hour",
    "mdfeed_process_threads",
    "mdfeed_published_total",
    "mdfeed_quality_events_total",
    "mdfeed_reconnects_total",
    "mdfeed_rest_rate_limited_total",
    "mdfeed_restore_drill_last_verified_age_seconds",
    "mdfeed_resyncs_total",
    "mdfeed_retention_prune_incomplete",
    "mdfeed_retention_remote_coverage_blocked",
    "mdfeed_ring_oversize_total",
    "mdfeed_ring_write_seq",
    "mdfeed_rows_archived_total",
    "mdfeed_rows_dropped_total",
    "mdfeed_rows_pruned_total",
    "mdfeed_rows_written_total",
    "mdfeed_send_bytes_total",
    "mdfeed_send_calls_total",
    "mdfeed_send_eagain_total",
    "mdfeed_sent_total",
    "mdfeed_seq",
    "mdfeed_session_cancel_timeouts_total",
    "mdfeed_signals_suppressed_total",
    "mdfeed_signals_total",
    "mdfeed_snapshot_msgs_total",
    "mdfeed_stale_restarts_total",
    "mdfeed_storage_backend_mismatch",
    "mdfeed_storage_db_unavailable",
    "mdfeed_subscribers",
    "mdfeed_symbol_collision_kinds",
    "mdfeed_symbol_truncated_kinds",
    "mdfeed_symbol_truncated_total",
    "mdfeed_symbols_tracked",
    "mdfeed_task_restarts_total",
    "mdfeed_ticks_total",
    "mdfeed_upstream_stale",
    "mdfeed_uptime_seconds",
    "mdfeed_writer_pending_rows",
    "mdfeed_ws_clients",
    "up",
}
# 멀티캐스트 발행자는 선택 서비스(MDFEED_MCAST_ENABLED). 안 켠 구성에서 이 지표가 없는 건
# 정확한 동작이다. 켜져 있는데 없으면 그건 결함이다 — 아래에서 구분한다.
MCAST_PORT = 9132
GATEWAY_PORT = 9111

# C++ 배포 게이트웨이만 내는 지표. 파이썬 판은 asyncio 가 쓰기를 대신하므로 EAGAIN 을 셀 자리가
# 아예 없다 — 같은 값을 억지로 만들면 "0 이니까 괜찮다" 는 거짓말이 된다. 구현이 다르면 낼 수
# 있는 것도 다르고, 그 사실을 분류로 적는다.
CPP_GATEWAY_METRICS = {"mdfeed_send_eagain_total", "mdfeed_fanout_delay_spread", "mdfeed_fanout_delay_max_us"}
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
    "mdfeed_fanout_delay_max_us",          # 절대값은 부하에 따라 변한다. 판정은 비(spread)로 한다
    "mdfeed_process_rss_bytes",            # 증가율 지표로 알람을 건다
    "mdfeed_process_fd_open",
    "mdfeed_data_gaps_recovered_total",  # 복구는 좋은 일이다. 알람 대상은 열린 공백 쪽
    "mdfeed_rows_archived_total",      # 보존 정책 진행 표시. 이상은 retention_prune_incomplete 로 본다
    "mdfeed_rows_pruned_total",
    "mdfeed_ticks_total",              # venue 별 유입. 멈춤은 upstream_stale 로 본다
    "mdfeed_signals_total",            # 전략 신호 발생 수. 장 상황에 따라 0 이 정상이다
    "mdfeed_price_ref_resets",         # 급등락 기준가 재설정. 품질 판정은 quality_events_total 로 본다
    "mdfeed_disk_hours_until_full",    # DiskFillingSoon 이 같은 값을 식으로 본다. 여기선 표시용
    "mdfeed_ws_clients",               # 구독자 수. 이상은 dropped_total 로 본다
    # ── 아래는 선언 목록을 실측에 맞추면서 처음 판정 대상이 된 것들(결함 45) ──
    "mdfeed_adapter_silent_exits_total",  # adapter_task_deaths_total 과 같은 자리에서 오른다. 알람은 그쪽
    "mdfeed_stale_restarts_total",     # 정체를 보고 되살린 횟수. 정체 자체는 upstream_stale 로 본다
    "mdfeed_resyncs_total",            # 접속·컨플레이션 때 정상적으로 오른다. 뒤처짐은 dropped_total
    "mdfeed_symbol_truncated_total",   # 절단 건수. 판정은 종류 수(symbol_truncated_kinds)로 한다
    "mdfeed_signals_suppressed_total", # 전략이 일부러 억제한 신호. 억제가 설계다
    "mdfeed_bars_closed_total",        # 처리량. 멈춤은 발행 처리량 알람으로 본다
    "mdfeed_bars_written_total",
    "mdfeed_rows_written_total",
    "mdfeed_snapshot_msgs_total",
    "mdfeed_http_requests_total",
    "mdfeed_coalesced_batches_total",  # 배치 묶음 수. 효율 진단용
    "mdfeed_symbols_tracked",          # 추적 종목 수. 구성에 따라 다르다
    "mdfeed_process_threads",          # 진단용. 누수는 fd·rss 증가율로 본다
    "mdfeed_db_bytes",                 # 절대 크기보다 증가율(db_growth_bytes_per_hour)로 본다
    "mdfeed_db_reclaimable_bytes",     # VACUUM 여지. retention_prune_incomplete 로 본다
    "mdfeed_counts_scan_seconds",      # 통계 스캔 소요. 요청 경로에 없으므로 알람 대상이 아니다
    "mdfeed_max_backlog",              # 구독자 최대 적체. 판정은 드롭(dropped_total)으로 한다
    "mdfeed_max_wire_bytes",           # 최대 프레임 크기. 링 초과는 ring_oversize_total 로 본다
    "mdfeed_seq",                      # 발행 시퀀스. 단조 증가값이라 임계가 없다
    "mdfeed_ring_write_seq",
}

# **critical 알람인데 대시보드 패널을 두지 않는 것.** 기본은 "두라"다 — 새벽 3시에
# 호출된 사람이 그래프 없이 판단할 수는 없다. 여기 넣으려면 왜 볼 게 없는지 적어야 한다.
NO_PANEL_BY_DESIGN: dict[str, str] = {}

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


def adapter_names() -> set[str] | None:
    """붙어 있는 어댑터 이름. 못 물어보면 None(판단 유보).

    지표가 어댑터 종류에 딸린 경우(REST 폴링만 레이트리밋을 맞는다) 이걸로 판정한다.
    "없다"를 결함으로 볼지 정상으로 볼지는 구성이 정한다.
    """
    names: set[str] = set()
    asked = False
    for port in (9100, 9200):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=4) as r:
                d = json.loads(r.read())
        except Exception:                            # noqa: BLE001, S112
            continue
        asked = True
        for t in d.get("tasks", []):
            n = str(t.get("name", ""))
            if n.startswith("adapter:"):
                names.add(n.split(":", 1)[1].lower())
    return names if asked else None


REST_POLLING_METRICS = {"mdfeed_rest_rate_limited_total"}
REST_POLLING_ADAPTERS = {"kis", "kis_rest"}


def gateway_impl() -> str | None:
    """배포 게이트웨이가 파이썬 판인지 C++ 판인지. 못 물어보면 None(판단 유보)."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{GATEWAY_PORT}/healthz", timeout=4) as r:
            return str(json.loads(r.read()).get("impl", "python"))
    except Exception:                                # noqa: BLE001, S112
        return None


def dashboard_metrics(path: str) -> dict[str, list[str]]:
    """대시보드 패널이 참조하는 지표 → 그 지표를 쓰는 패널 제목들.

    패널 식(expr)만 본다. 제목·설명에 지표 이름을 적어 둔 것은 참조가 아니다.
    """
    with open(path, encoding="utf-8") as fh:
        dash = json.load(fh)
    out: dict[str, list[str]] = {}
    for panel in dash.get("panels", []):
        title = panel.get("title", "(제목 없음)")
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            for m in METRIC_RE.findall(expr) + (["up"] if re.search(r"\bup\{", expr) else []):
                out.setdefault(m, []).append(title)
    return out


def main() -> int:
    ap = argparse.ArgumentParser("verify_alerts")
    ap.add_argument("--rules", default="ops/observability/alerts.yml")
    ap.add_argument("--dashboard",
                    default="ops/observability/grafana/dashboards/mdfeed.json")
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
    alert_metrics: dict[str, set[str]] = {}
    severity: dict[str, str] = {}
    alert = None
    in_expr = False
    with open(args.rules, encoding="utf-8") as rules_file:
        for line in rules_file:
            a = re.match(r"^\s*- alert:\s*(\S+)", line)
            if a:
                alert, in_expr = a.group(1), False
                alert_metrics.setdefault(alert, set())
            sev = re.search(r"severity:\s*(\w+)", line)
            if sev and alert:
                severity[alert] = sev.group(1)
            e = EXPR_RE.match(line)
            if e and alert:
                in_expr = True
                for m in METRIC_RE.findall(e.group(1)):
                    referenced.setdefault(m, []).append(alert)
                    alert_metrics[alert].add(m)
                if re.search(r"\bup\{", e.group(1)):
                    alert_metrics[alert].add("up")
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
                    alert_metrics[alert].add(m)

    measures = any_adapter_measures_latency()
    adapters = adapter_names()
    preflight_live = PREFLIGHT_PORT in live_ports
    impl = gateway_impl()
    ok, conditional, missing = {}, {}, {}
    notes = dict(CONDITIONAL)
    for m, alerts in referenced.items():
        if m in exposed:
            ok[m] = alerts
        elif m in CONDITIONAL and measures is False:
            # 지연을 재는 어댑터가 없으므로 없는 것이 정확한 동작이다
            conditional[m] = alerts
        elif (m in REST_POLLING_METRICS and adapters is not None
              and not (adapters & REST_POLLING_ADAPTERS)):
            # REST 로 폴링하는 어댑터가 없다. 레이트리밋을 맞을 자리가 없으므로
            # 이 지표가 없는 것이 정확한 동작이다.
            conditional[m] = alerts
            notes[m] = ("REST 폴링 어댑터(KIS)가 붙어 있어야 생성된다 "
                        f"(현재 어댑터: {', '.join(sorted(adapters)) or '없음'})")
        elif m in CPP_GATEWAY_METRICS and impl is not None and impl != "c++":
            # 파이썬 게이트웨이가 도는 중. 이 지표가 없는 것이 정확한 동작이다.
            conditional[m] = alerts
            notes[m] = f"C++ 배포 게이트웨이에서만 생성된다 (현재 impl={impl})"
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
    failed = bool(missing)
    # ── 반대 방향: 지표는 내는데 아무 알람도 안 보는 것 ──────────────────
    # 여기까지는 "알람이 없는 지표를 참조하는가"만 봤다(결함 23). 그 반대인 "지표만 내고
    # 아무도 안 본다"도 같은 사각이다.
    #
    # **판정은 실서비스 스크레이프가 아니라 선언 목록(DECLARED_OFFLINE)으로 한다.**
    # 살아 있는 지표 집합은 어떤 서비스가 떠 있고 어떤 어댑터가 붙었느냐에 따라 달라진다.
    # 그걸로 판정하면 같은 코드가 구성에 따라 통과하기도 실패하기도 한다 — 그런 검사는
    # 검사가 아니다. 선언 목록은 "우리가 내겠다고 한 것"이고 결정을 내릴 자리다.
    declared_unwatched = sorted(m for m in DECLARED_OFFLINE
                                if m.startswith("mdfeed_")
                                and m not in referenced and m not in NO_ALERT_BY_DESIGN)
    if declared_unwatched:
        print()
        for m in declared_unwatched:
            print(f"{m:<46} {'무관심':<10} 이 지표를 보는 알람이 없다")
        print("\n알람을 붙이거나, 볼 필요가 없으면 scripts/verify_alerts.py 의 "
              "NO_ALERT_BY_DESIGN 에 이유와 함께 넣으세요.")
        failed = True

    # 살아 있는데 선언에 없는 지표는 **실패다.**
    #
    # 예전에는 참고로만 찍었다. 이유는 "어댑터 하나를 켜고 끄는 것만으로 빌드가 빨개진다"
    # 였는데, 그 판단이 위 역방향 검사를 무력화했다. 선언 목록에 알람이 참조하는 43종만
    # 들어 있었고 역방향 판정을 그 목록으로 했으니, 결과는 항상 "사각 없음" 이었다 —
    # 검사기가 자기 입력을 자기 답으로 쓰고 있었다(결함 45). 실측으로 맞춰 보니
    # 무감시 지표가 40종이었고 그중에 **버려진 행을 세는 카운터**가 있었다.
    #
    # 구성에 따라 지표가 늘어나는 건 사실이지만, 늘어난 지표가 선언에 없다는 건 누군가
    # 지표를 내면서 "볼지 말지" 를 결정하지 않았다는 뜻이다. 그 결정을 미루게 하는 검사는
    # 검사가 아니다. 켜고 끄는 것만으로 새 지표가 나온다면 그 지표도 선언하면 된다.
    if live_ports:
        undeclared = sorted(m for m in exposed if m.startswith("mdfeed_") and m not in DECLARED_OFFLINE)
        if undeclared:
            print()
            for m in undeclared:
                print(f"{m:<46} {'미선언':<10} 내고 있는데 선언 목록에 없다")
            print(f"\n{len(undeclared)}종을 DECLARED_OFFLINE 에 넣고, 알람을 붙이거나 "
                  f"NO_ALERT_BY_DESIGN 에 이유를 적으세요.")
            failed = True

    # ── 세 번째 방향: 알람은 울리는데 볼 그래프가 없다 ──────────────────
    # 위 두 검사는 "알람이 울릴 수 있는가" 까지만 본다. 울린 다음이 남는다.
    # critical 알람에 패널이 없으면 호출받은 사람은 Grafana 를 열고 아무것도 못 본다.
    # 반대로 죽은 지표를 가리키는 패널은 빈 그래프로 남는데, 그게 고장인지 원래 0 인지
    # 구분할 방법이 없다 — 이쪽이 더 위험하다. 없는 것보다 틀린 게 나쁘다.
    panels = dashboard_metrics(args.dashboard)
    known = exposed | DECLARED_OFFLINE
    dead = sorted(m for m in panels if m not in known)
    if dead:
        print()
        for m in dead:
            print(f"{m:<46} {'죽은패널':<10} {', '.join(panels[m])}   ← 이 패널은 영원히 빈다")
        print("\n지표를 노출하거나 패널을 지우세요. 빈 그래프는 '정상 0' 과 구분되지 않습니다.")
        failed = True

    blind = {a: sorted(ms) for a, ms in alert_metrics.items()
             if severity.get(a) == "critical" and ms and not (ms & set(panels))
             and a not in NO_PANEL_BY_DESIGN}
    if blind:
        print()
        for a, ms in sorted(blind.items()):
            print(f"{a:<46} {'무패널':<10} {', '.join(ms)}   ← 울려도 볼 그래프가 없다")
        print("\ncritical 알람에는 패널을 두세요. 볼 게 없는 이유가 있으면 "
              "NO_PANEL_BY_DESIGN 에 적으세요.")
        failed = True

    # warning 은 실패로 두지 않는다. 한 건 한 건 패널을 요구하면 대시보드가 알람 목록이
    # 되고, 그러면 아무도 안 본다. 다만 목록은 보여 준다 — 판단할 사람은 사람이다.
    warn_blind = sorted(a for a, ms in alert_metrics.items()
                        if severity.get(a) != "critical" and ms and not (ms & set(panels)))
    if warn_blind:
        print(f"\n참고: 패널 없는 warning 알람 {len(warn_blind)}건 — "
              f"{', '.join(warn_blind[:8])}{' …' if len(warn_blind) > 8 else ''}")

    if missing:
        print("\n누락된 지표를 노출하거나 규칙을 고치세요.")
    if failed:
        return 1
    crit = sum(1 for a in alert_metrics if severity.get(a) == "critical")
    print(f"모든 알람이 실재하는 지표를 참조하고, 노출 지표 {len(exposed)}종에 "
          f"감시 사각이 없습니다.")
    print(f"대시보드 패널 {len(panels)}종을 보고, critical 알람 {crit}건 모두 "
          f"볼 그래프가 있습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
