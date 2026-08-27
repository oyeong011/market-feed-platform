#!/usr/bin/env bash
# MDFeed 운영 스크립트 — 리눅스 서버에서 서비스·프로세스·로그를 점검한다.
#
#   ./ops/ops.sh status      전체 상태 (프로세스 + HTTP 헬스 + 포트)
#   ./ops/ops.sh health      상세 헬스 JSON
#   ./ops/ops.sh ports       리스닝 포트 확인
#   ./ops/ops.sh logs [svc]  로그 tail (systemd면 journalctl)
#   ./ops/ops.sh top         프로세스 자원 사용량
#   ./ops/ops.sh conns       현재 구독자 연결
#   ./ops/ops.sh diag        장애 진단 원스톱 (RUNBOOK 1차 조치)
#   ./ops/ops.sh install     systemd 설치 (root 필요)
#
# systemd 가 있으면 systemctl 을, 없으면(맥/컨테이너) PID 파일을 근거로 동작한다.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

RUN_DIR="${MDFEED_RUN_DIR:-/tmp/mdfeed}"
SERVICES="feedd tcp-gateway ws-gateway rest-api writer strategy quality"

# 연관배열(declare -A)은 bash 4+ 전용이라 macOS 기본 bash 3.2 에서 깨진다.
# 개발 노트북과 리눅스 서버에서 같은 스크립트를 쓰려고 case 로 조회한다.
admin_port() {
  case "$1" in
    feedd) echo 9100 ;; tcp-gateway) echo 9111 ;; ws-gateway) echo 9102 ;;
    rest-api) echo 9103 ;; writer) echo 9104 ;; strategy) echo 9105 ;; quality) echo 9106 ;;
  esac
}
module_of() {
  case "$1" in
    feedd) echo mdfeed.services.feedd ;;
    tcp-gateway) echo mdfeed.services.tcp_gateway ;;
    ws-gateway) echo mdfeed.services.ws_gateway ;;
    rest-api) echo mdfeed.services.rest_api ;;
    writer) echo mdfeed.services.writer ;;
    strategy) echo mdfeed.services.strategy ;;
    quality) echo mdfeed.services.quality ;;
  esac
}

G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; D=$'\e[2m'; N=$'\e[0m'
hr() { local i=0; while [ $i -lt "${1:-80}" ]; do printf '─'; i=$((i+1)); done; echo; }
has_systemd() { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }

pid_of() {
  local svc=$1
  if has_systemd; then
    systemctl show -p MainPID --value "mdfeed-$svc.service" 2>/dev/null | grep -v '^0$'
  else
    # PID 파일이 있으면 살아있는지 확인하고, 없으면 명령줄로 찾는다
    local f="$RUN_DIR/$svc.pid"
    [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null && cat "$f" && return 0
    pgrep -f "$(module_of "$svc")" 2>/dev/null | head -1
  fi
}

http_json() { curl -s --max-time 3 "http://127.0.0.1:$1$2" 2>/dev/null; }

cmd_status() {
  printf "%-14s %-8s %-9s %-9s %s\n" "SERVICE" "PID" "PROCESS" "HTTP" "요약"
  hr 92
  local bad=0
  for svc in $SERVICES; do
    local pid proc http summary body port
    port=$(admin_port "$svc")
    pid=$(pid_of "$svc")
    if [ -n "$pid" ]; then proc="${G}RUNNING${N}"; else proc="${R}DOWN${N}"; bad=$((bad+1)); fi
    body=$(http_json "$port" /healthz)
    if [ -z "$body" ]; then
      http="${R}NO-RESP${N}"; summary=""
      [ -n "$pid" ] && bad=$((bad+1))
    elif echo "$body" | grep -q '"healthy": *true'; then
      http="${G}HEALTHY${N}"
      summary=$(echo "$body" | python3 -c '
import json,sys
d=json.load(sys.stdin)
keys=["frames_in","rows_written","signals_emitted","subscribers","ws_clients","seq","symbols"]
print(" ".join(f"{k}={d[k]}" for k in keys if k in d)[:44])' 2>/dev/null)
    else
      http="${Y}UNHEALTHY${N}"; bad=$((bad+1))
      summary=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("reason") or d.get("clock_warning") or "")' 2>/dev/null)
    fi
    printf "%-14s %-8s %-18s %-18s %s\n" "$svc" "${pid:--}" "$proc" "$http" "$summary"
  done
  hr 92
  if [ "$bad" -eq 0 ]; then echo "${G}전체 정상${N}"; else echo "${R}이상 항목 $bad 건 — ./ops/ops.sh diag 로 진단${N}"; fi
  return $(( bad > 0 ))
}

cmd_ports() {
  echo "── 리스닝 포트 ──"
  for p in 9100 9101 9102 9103 9104 9105 9111; do
    local who
    if command -v ss >/dev/null 2>&1; then who=$(ss -ltnp 2>/dev/null | grep ":$p ")
    else who=$(lsof -nP -iTCP:"$p" -sTCP:LISTEN 2>/dev/null | tail -1); fi
    if [ -n "$who" ]; then printf "  %-6s ${G}LISTEN${N}  %s\n" "$p" "$(echo "$who" | awk '{print $1, $2}')"
    else printf "  %-6s ${D}closed${N}\n" "$p"; fi
  done
  echo "── UDS 버스 소켓 ──"
  for s in "$RUN_DIR/bus.sock" "$RUN_DIR/signals.sock"; do
    if [ -S "$s" ]; then printf "  ${G}OK${N}     %s  (%s)\n" "$s" "$(ls -l "$s" | awk '{print $1, $3":"$4}')"
    else printf "  ${R}없음${N}   %s\n" "$s"; fi
  done
}

cmd_health() {
  local target="${1:-}"
  for svc in $SERVICES; do
    [ -n "$target" ] && [ "$svc" != "$target" ] && continue
    echo "═══ $svc (:$(admin_port "$svc")) ═══"
    http_json "$(admin_port "$svc")" /healthz | python3 -m json.tool 2>/dev/null || echo "  응답 없음"
  done
}

cmd_logs() {
  local svc="${1:-}"
  if has_systemd; then
    if [ -n "$svc" ]; then journalctl -u "mdfeed-$svc" -n 100 -f --no-pager
    else journalctl -u 'mdfeed-*' -n 100 -f --no-pager; fi
  else
    echo "systemd 없음 → 프로세스 stdout 을 확인하세요 (make up 실행 터미널)"
    [ -f /tmp/stack.log ] && tail -50 /tmp/stack.log
  fi
}

cmd_top() {
  echo "── 프로세스 자원 ──"
  printf "%-14s %-8s %-7s %-7s %-8s %s\n" "SERVICE" "PID" "%CPU" "%MEM" "RSS(MB)" "ELAPSED"
  for svc in $SERVICES; do
    local pid; pid=$(pid_of "$svc"); [ -z "$pid" ] && continue
    ps -p "$pid" -o pid=,pcpu=,pmem=,rss=,etime= 2>/dev/null | \
      awk -v s="$svc" '{printf "%-14s %-8s %-7s %-7s %-8.1f %s\n", s, $1, $2, $3, $4/1024, $5}'
  done
  echo ""
  echo "── 파일 디스크립터 (배포단은 구독자 수만큼 소켓을 연다) ──"
  for svc in feedd tcp-gateway ws-gateway; do
    local pid; pid=$(pid_of "$svc"); [ -z "$pid" ] && continue
    local n; n=$(lsof -p "$pid" 2>/dev/null | wc -l | tr -d ' ')
    printf "  %-14s fd=%s\n" "$svc" "$n"
  done
}

cmd_conns() {
  echo "── TCP 구독자 (:9101) ──"
  http_json 9111 /subscribers | python3 -m json.tool 2>/dev/null || echo "  게이트웨이 응답 없음"
  echo "── WebSocket 구독자 (:9102) ──"
  http_json 9102 /api/clients | python3 -m json.tool 2>/dev/null || echo "  게이트웨이 응답 없음"
}

cmd_diag() {
  echo "════════ MDFeed 장애 진단 ════════"
  echo "시각: $(date '+%F %T %Z')   호스트: $(hostname)"
  echo ""
  cmd_status; echo ""
  cmd_ports;  echo ""

  echo "── 업스트림 상태 ──"
  http_json 9100 /healthz | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: print("  feedd 응답 없음 — 수집이 멈춘 상태"); raise SystemExit
for u in d.get("upstreams", []):
    mark = "STALE" if u["stale"] else "OK"
    print(f"  [{mark:5}] {u[\"venue\"]:<9} 메시지 {u[\"messages\"]:>8,}  재접속 {u[\"reconnects\"]}  "
          f"오류 {u[\"errors\"]}  마지막 {u[\"last_msg_age_s\"]}s 전")
for x in d.get("inactive_upstreams", []):
    print(f"  [OFF  ] {x[\"venue\"]:<9} {x[\"reason\"]}")
if d.get("clock_warning"): print(f"  ! {d[\"clock_warning\"]}")
for v,c in (d.get("clock") or {}).items():
    print(f"  시계 {v:<9} 오프셋 {c[\"offset_us\"]/1000:>8.1f}ms  표본 {c[\"samples\"]:,}")
' 2>/dev/null
  echo ""
  echo "── 시퀀스 무결성 (writer 기준) ──"
  http_json 9104 /healthz | python3 -c '
import json,sys
d=json.load(sys.stdin); s=d.get("sequence",{})
print(f"  갭 {s.get(\"gap_count\",0)}회 / 유실 {s.get(\"lost_messages\",0)}건 / 중복 {s.get(\"duplicate_count\",0)}건")
print(f"  적재 {d.get(\"rows_written\",0):,}행 / {d.get(\"bars_written\",0):,}봉  대기 {d.get(\"pending_rows\",0)}행  DB오류 {d.get(\"db_errors\",0)}")
' 2>/dev/null || echo "  writer 응답 없음"
  echo ""
  echo "── 디스크 / 메모리 ──"
  df -h . 2>/dev/null | tail -1 | awk '{print "  디스크: 사용 "$3" / 전체 "$2" ("$5")"}'
  if command -v free >/dev/null 2>&1; then free -h | awk 'NR==2{print "  메모리: 사용 "$3" / 전체 "$2}'; fi
  echo ""
  echo "── 시각 동기화 (지연 지표의 신뢰성 근거) ──"
  if command -v timedatectl >/dev/null 2>&1; then timedatectl show -p NTPSynchronized -p TimeUSec 2>/dev/null | sed 's/^/  /'
  elif command -v sntp >/dev/null 2>&1; then sntp -t 2 time.apple.com 2>/dev/null | tail -1 | sed 's/^/  /'
  else echo "  (시각 동기화 도구 없음)"; fi
  echo ""
  echo "다음 조치는 RUNBOOK.md 의 증상별 절차를 따르세요."
}

cmd_install() {
  [ "$(id -u)" -ne 0 ] && { echo "root 권한이 필요합니다: sudo $0 install"; exit 1; }
  id mdfeed >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin mdfeed
  install -d -o mdfeed -g mdfeed /opt/mdfeed /var/lib/mdfeed /var/log/mdfeed /run/mdfeed /etc/mdfeed
  cp -r src quant docs "$0" /opt/mdfeed/ 2>/dev/null
  [ -f /etc/mdfeed/mdfeed.env ] || install -m 640 -o root -g mdfeed ops/mdfeed.env.example /etc/mdfeed/mdfeed.env
  cp ops/systemd/*.service ops/systemd/*.target /etc/systemd/system/
  cp ops/logrotate.conf /etc/logrotate.d/mdfeed
  # /run 은 재부팅 시 사라지므로 tmpfiles 로 매번 만든다
  echo "d /run/mdfeed 0755 mdfeed mdfeed -" > /etc/tmpfiles.d/mdfeed.conf
  systemd-tmpfiles --create /etc/tmpfiles.d/mdfeed.conf
  systemctl daemon-reload
  systemctl enable mdfeed.target mdfeed-feedd mdfeed-tcp-gateway mdfeed-ws-gateway \
                   mdfeed-rest-api mdfeed-writer mdfeed-strategy
  echo "설치 완료. 기동: systemctl start mdfeed.target"
}

case "${1:-status}" in
  status)  cmd_status ;;
  ports)   cmd_ports ;;
  health)  cmd_health "${2:-}" ;;
  logs)    cmd_logs "${2:-}" ;;
  top)     cmd_top ;;
  conns)   cmd_conns ;;
  diag)    cmd_diag ;;
  install) cmd_install ;;
  *) sed -n '2,20p' "$0"; exit 1 ;;
esac
