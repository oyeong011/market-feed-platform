#!/usr/bin/env bash
# 장애 주입 — 복구 경로가 실제로 도는지 확인한다.
#
# 이 프로젝트에는 복구 장치가 여럿 있다. 지수 백오프 재접속, CRC 재동기화,
# 느린 구독자 격리, DB 폴백, 샤드 격리. 그런데 **평소에는 하나도 안 돌아간다.**
# 61분을 돌려도 재접속 0회였다. 즉 그 코드들이 맞는지 확인된 적이 없다.
#
# 안 돌아본 복구 경로는 복구 경로가 아니다. 그래서 일부러 부순다.
#
#   ./ops/chaos.sh              전체 시나리오
#   ./ops/chaos.sh bus          버스 끊기만
#
# 각 시나리오는 [주입 → 관찰 → 복구 확인] 순서로 돌고, 복구를 못 하면 실패한다.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; B=$'\e[1m'; N=$'\e[0m'
FAILED=0
pass() { echo "  ${G}복구 확인${N} $*"; }
fail() { echo "  ${R}복구 실패${N} $*"; FAILED=1; }
info() { echo "  ${Y}·${N} $*"; }

health() { curl -s --max-time 3 "http://127.0.0.1:$1/healthz" 2>/dev/null; }
jqv() { python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
    for k in '$1'.split('.'): d=d[k]
    print(d)
except Exception: print('')" 2>/dev/null; }

require_stack() {
  health 9100 >/dev/null || { echo "스택이 없습니다. make up-shards 로 먼저 띄우세요"; exit 2; }
}

# ── 1. feedd 를 죽인다 — 소비자가 재접속하는가 ───────────────────────────
chaos_feedd() {
  echo "${B}[1] feedd:crypto SIGKILL — 소비자 자동 재접속${N}"
  local before_wr before_rows pid after_wr after_rows
  before_wr=$(health 9104 | jqv "frames_in")
  pid=$(lsof -nP -iTCP:9100 -sTCP:LISTEN 2>/dev/null | tail -1 | awk '{print $2}')
  [ -z "$pid" ] && { fail "feedd pid 를 못 찾음"; return; }
  info "feedd pid=$pid 종료. writer 수신 $before_wr 건"
  kill -9 "$pid" 2>/dev/null
  sleep 20
  after_wr=$(health 9104 | jqv "frames_in")
  local wr_pid_alive
  wr_pid_alive=$(health 9104 | jqv "service")
  if [ -z "$after_wr" ]; then
    fail "writer 가 함께 죽었다 (Wants= 대신 Requires= 를 쓰고 있지 않은지 확인)"
  elif [ "$after_wr" -gt "$before_wr" ]; then
    pass "writer 생존 + 수신 재개 ($before_wr → $after_wr)"
  else
    fail "writer 는 살았으나 수신이 재개되지 않음 ($before_wr → $after_wr)"
  fi
}

# ── 2. 버스 소켓을 지운다 — 재생성되는가 ─────────────────────────────────
chaos_bus() {
  echo "${B}[2] 버스 소켓 삭제 — 발행자가 다시 만드는가${N}"
  local sock="${MDFEED_RUN_DIR:-/tmp/mdfeed}/bus-crypto.sock"
  [ -S "$sock" ] || { info "소켓 없음: $sock (건너뜀)"; return; }
  info "삭제: $sock"
  rm -f "$sock"
  sleep 18
  if [ -S "$sock" ]; then
    pass "소켓 재생성됨 (feedd 재시작 경로)"
  else
    # feedd 가 안 죽었다면 소켓은 안 돌아온다 — 그것도 사실이므로 그대로 적는다
    info "소켓이 돌아오지 않음. 발행자는 살아 있으나 새 구독자가 못 붙는 상태"
    info "→ 현재 설계의 한계. 소켓 감시 후 재바인드가 필요하다"
  fi
}

# ── 3. 오염된 프레임을 밀어넣는다 — CRC 재동기화가 도는가 ────────────────
#
# 첫 판은 주입 조건이 안 맞아 오염을 거의 안 넣고 "재동기화 0" 으로 끝났다.
# 실패가 아니라 **테스트가 안 돌았던 것**이다. 그런 결과를 통과로 세면
# 검증 자체가 거짓말이 된다. 주입을 확정적으로 하고 결과를 판정하게 고쳤다.
chaos_corrupt() {
  echo "${B}[3] TCP 스트림에 쓰레기 주입 — CRC 재동기화${N}"
  local out
  out=$(PYTHONPATH=src python3 - <<'PYEOF'
import socket, sys, time
sys.path.insert(0, "src")
from mdfeed.protocol import FrameParser

try:
    s = socket.create_connection(("127.0.0.1", 9101), timeout=5)
except Exception as e:
    print(f"CONNECT_FAIL {e}"); raise SystemExit

s.settimeout(3.0)
p = FrameParser()
good = injected = 0
t0 = time.time()
while time.time() - t0 < 20 and injected < 3:
    try:
        chunk = s.recv(65536)
    except socket.timeout:
        continue
    if not chunk:
        break
    # 받은 덩어리 중간을 확실히 오염시킨다. 프레임 경계와 무관한 위치에
    # 쓰레기를 끼워 넣어야 파서가 재동기화를 해야만 살아남는다.
    if len(chunk) > 40 and good >= 3:
        mid = len(chunk) // 2
        chunk = chunk[:mid] + b"\x00\xff\xfe\xfdGARBAGE" + chunk[mid:]
        injected += 1
    for _ in p.feed(chunk):
        good += 1

# 오염 이후에도 계속 받는지 확인 (재동기화가 됐다면 계속 온다)
after = 0
t1 = time.time()
while time.time() - t1 < 6:
    try:
        chunk = s.recv(65536)
    except socket.timeout:
        continue
    if not chunk:
        break
    for _ in p.feed(chunk):
        after += 1
s.close()
print(f"RESULT injected={injected} frames={good} after={after} "
      f"crc={p.crc_error_count} resync={p.resync_count}")
PYEOF
)
  echo "  $out" | sed 's/^  RESULT/  /'
  local injected resync after
  injected=$(echo "$out" | grep -oE 'injected=[0-9]+' | cut -d= -f2)
  resync=$(echo "$out" | grep -oE 'resync=[0-9]+' | cut -d= -f2)
  after=$(echo "$out" | grep -oE 'after=[0-9]+' | cut -d= -f2)
  if [ -z "${injected:-}" ] || [ "${injected:-0}" -eq 0 ]; then
    fail "오염을 주입하지 못했다 — 테스트가 돌지 않은 것이지 통과가 아니다"
  elif [ "${resync:-0}" -eq 0 ]; then
    fail "오염 ${injected}회를 넣었는데 재동기화가 0회. CRC 검사가 동작하지 않는다"
  elif [ "${after:-0}" -eq 0 ]; then
    fail "재동기화는 됐으나 이후 프레임이 오지 않는다 — 세션이 멎었다"
  else
    pass "오염 ${injected}회 → 재동기화 ${resync}회 후 ${after}건 계속 수신"
  fi
}

# ── 4. 느린 구독자 — 백프레셔가 도는가 ───────────────────────────────────
chaos_slow_subscriber() {
  echo "${B}[4] 읽지 않는 구독자 — 백프레셔·격리${N}"
  local before after
  before=$(curl -s --max-time 3 localhost:9111/healthz | jqv "total_dropped")
  PYTHONPATH=src python3 - <<'PYEOF'
import socket, time
# 붙기만 하고 한 바이트도 읽지 않는다. 게이트웨이 큐가 차야 한다.
try:
    s = socket.create_connection(("127.0.0.1", 9101), timeout=5)
except Exception as e:
    print(f"  접속 실패: {e}"); raise SystemExit
print("  읽지 않는 구독자 접속. 25초 유지")
time.sleep(25)
s.close()
print("  구독자 종료")
PYEOF
  sleep 3
  after=$(curl -s --max-time 3 localhost:9111/healthz | jqv "total_dropped")
  local alive
  alive=$(curl -s --max-time 3 localhost:9111/healthz | jqv "healthy")
  if [ "$alive" = "True" ]; then
    pass "느린 구독자 붙은 동안에도 게이트웨이 정상 (드롭 $before → $after)"
  else
    fail "느린 구독자 하나에 게이트웨이가 무너짐"
  fi
}

# ── 5. DB 를 뺏는다 — 폴백이 도는가 ──────────────────────────────────────
chaos_db() {
  echo "${B}[5] DB 파일 잠금 — 적재 실패 시 서비스가 죽는가${N}"
  local db="data/mdfeed.db"
  [ -f "$db" ] || { info "SQLite 파일 없음 (건너뜀)"; return; }
  local before after healthy
  before=$(health 9104 | jqv "rows_written")
  chmod 000 "$db" 2>/dev/null
  info "DB 읽기·쓰기 권한 제거. 20초 관찰"
  sleep 20
  healthy=$(health 9104 | jqv "service")
  chmod 644 "$db" 2>/dev/null
  sleep 15
  after=$(health 9104 | jqv "rows_written")
  if [ -z "$healthy" ]; then
    fail "DB 접근 불가로 writer 프로세스가 죽었다"
  elif [ -n "$after" ] && [ "$after" -ge "$before" ]; then
    pass "writer 생존 · 권한 복구 후 적재 재개 ($before → $after)"
  else
    fail "writer 는 살았으나 적재가 재개되지 않음"
  fi
}

require_stack
echo "장애 주입 시작 — $(date '+%H:%M:%S')"
echo "복구 장치는 평소에 안 돌아간다. 일부러 부숴야 맞는지 알 수 있다."
echo ""

# ── 서비스를 하나씩 죽였다 살린다 ─────────────────────────────────────────
# 2026-08-28 ~ 09-02 에 **같은 가족의 사고가 네 번** 났다. 증상은 매번 같았고
# (업스트림이 조용히 멎고 헬스는 정상) 원인은 매번 달랐다.
#   08-28 태스크 사망이 안 보임 · 08-31 정리가 복구 차단
#   09-01 감독이 조용히 이탈   · 09-02 소비자를 GC 가 수거
#
# 네 건 다 시험으로는 안 잡혔다. **프로세스 경계에서 났기 때문이다.**
# 여기서 각 서비스를 실제로 죽이고, 정해진 시간 안에 돌아오는지 본다.
# 이 시나리오가 있었으면 네 건 중 셋은 자동으로 잡혔다.

RECOVER_S=${RECOVER_S:-45}

chaos_service_kill() {
  local name="$1" port="$2"
  echo "${B}[서비스 재기동] $name (:$port)${N}"
  local pid before
  pid=$(pgrep -f "mdfeed.services.$name" | head -1)
  [ -z "$pid" ] && { fail "$name 프로세스를 못 찾음"; return; }
  before=$(health "$port" | jqv uptime_s)
  info "pid $pid 를 SIGKILL 한다 (가동 ${before%.*}초)"
  kill -9 "$pid" 2>/dev/null

  local t0 waited=0
  t0=$(date +%s)
  while [ "$waited" -lt "$RECOVER_S" ]; do
    sleep 2
    waited=$(( $(date +%s) - t0 ))
    local up
    up=$(health "$port" | jqv uptime_s)
    if [ -n "$up" ] && [ "${up%.*}" -lt "${before%.*}" ] 2>/dev/null; then
      pass "$name 이 ${waited}초 만에 돌아왔다 (가동 ${up%.*}초)"
      return
    fi
  done
  fail "$name 이 ${RECOVER_S}초 안에 안 돌아왔다 — 감독이 없거나 죽었다"
}

chaos_restart_all() {
  echo "${B}[전 서비스 재기동 복구]${N}"
  for pair in "tcp_gateway 9111" "ws_gateway 9102" "rest_api 9103" \
              "writer 9104" "strategy 9105" "quality 9106"; do
    chaos_service_kill $pair
  done
}

# ── 감독이 실제로 되살리는가 (프로세스 안) ────────────────────────────────
chaos_task_supervision() {
  echo "${B}[태스크 감독] 되살린 흔적이 지표에 남는가${N}"
  local ok=0
  for pair in "9100 feedd" "9104 writer" "9106 quality" "9111 tcp-gateway"; do
    set -- $pair
    local body
    body=$(health "$1")
    [ -z "$body" ] && { fail "$2 무응답"; continue; }
    local n
    n=$(echo "$body" | python3 -c "import json,sys
try:
    d=json.load(sys.stdin); print(len(d.get('tasks',[])))
except Exception: print(0)")
    if [ "${n:-0}" -gt 0 ]; then
      info "$2 감독 태스크 ${n}개"
      ok=$((ok+1))
    else
      fail "$2 가 태스크 상태를 안 낸다 — 감독을 안 거치고 있다"
    fi
  done
  [ "$ok" -ge 3 ] && pass "감독 계약이 서비스에 걸려 있다"
}

case "${1:-all}" in
  feedd)   chaos_feedd ;;
  bus)     chaos_bus ;;
  corrupt) chaos_corrupt ;;
  slow)    chaos_slow_subscriber ;;
  db)      chaos_db ;;
  restart) chaos_restart_all ;;
  tasks)   chaos_task_supervision ;;
  all)
    chaos_corrupt; echo
    chaos_slow_subscriber; echo
    chaos_db; echo
    chaos_feedd; echo
    chaos_task_supervision; echo
    chaos_restart_all; echo
    ;;
  *) echo "사용법: $0 {all|feedd|bus|corrupt|slow|db|restart|tasks}"; exit 1 ;;
esac
echo ""
[ "$FAILED" = "0" ] && echo "${G}모든 복구 경로 확인${N}" || echo "${R}복구 실패 항목 있음${N}"
exit "$FAILED"
