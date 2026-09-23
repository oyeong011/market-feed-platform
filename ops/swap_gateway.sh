#!/usr/bin/env bash
# 배포 게이트웨이 무중단 교체 — 새 프로세스를 같은 포트에 띄우고, 옛 프로세스를 드레인시킨다.
#
#   ops/swap_gateway.sh [교체할 바이너리 경로]
#
# 원리(cpp/src/tcp_gateway.cpp 머리말 참고):
#   1. 새 프로세스가 SO_REUSEPORT 로 같은 포트에 함께 바인드한다
#   2. 옛 프로세스에 SIGTERM → 리스너만 닫고, 기존 구독자에게 MDFEED_DRAIN_SECONDS 동안 계속 배포
#   3. 그동안 새 접속은 전부 새 프로세스로 간다 → connect() 실패 없음
#
# 이 스크립트는 교체 **중에 접속을 계속 두드려** 실패 수를 센다. 0 이 아니면 실패로 끝낸다 —
# "무중단"이라고 말만 하고 확인하지 않으면 그건 주장이지 사실이 아니다.
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-./cpp/build/tcp_gateway}
PORT=${MDFEED_TCP_PORT:-9101}
HOST=${MDFEED_TCP_HOST:-127.0.0.1}
DRAIN=${MDFEED_DRAIN_SECONDS:-5}

[ -x "$BIN" ] || { echo "실행 파일이 없습니다: $BIN (make cpp)"; exit 1; }

old_pid=$(pgrep -f "$(basename "$BIN")" | head -1)
[ -n "$old_pid" ] || { echo "돌고 있는 게이트웨이를 못 찾았습니다"; exit 1; }
echo "옛 프로세스 pid=$old_pid · 포트 $HOST:$PORT · 드레인 ${DRAIN}초"

# 교체 내내 접속을 두드린다
probe_out=$(mktemp)
(
  ok=0; fail=0
  end=$(( $(date +%s) + DRAIN + 8 ))
  while [ "$(date +%s)" -lt "$end" ]; do
    if MDFEED_PROBE=1 python3 - "$HOST" "$PORT" <<'PY' 2>/dev/null
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1); s.close()
PY
    then ok=$((ok+1)); else fail=$((fail+1)); fi
    sleep 0.05
  done
  echo "$ok $fail" > "$probe_out"
) & probe_pid=$!

sleep 1
MDFEED_DRAIN_SECONDS=$DRAIN "$BIN" & new_pid=$!
sleep 1
kill -0 "$new_pid" 2>/dev/null || { echo "새 프로세스가 뜨지 못했습니다 (SO_REUSEPORT 가 꺼져 있진 않은지 확인)"; kill $probe_pid 2>/dev/null; exit 1; }
echo "새 프로세스 pid=$new_pid — 같은 포트에 합류"

kill -TERM "$old_pid"
echo "옛 프로세스에 SIGTERM — 드레인 시작"
for _ in $(seq 1 $((DRAIN + 10))); do kill -0 "$old_pid" 2>/dev/null || break; sleep 1; done
kill -0 "$old_pid" 2>/dev/null && { echo "옛 프로세스가 드레인 기한 안에 안 끝났습니다"; exit 1; }
echo "옛 프로세스 종료 완료"

wait $probe_pid 2>/dev/null
read -r ok fail < "$probe_out"; rm -f "$probe_out"
echo "교체 중 접속 시도: 성공 $ok · 실패 $fail"
[ "${fail:-1}" -eq 0 ] || { echo "::실패:: 교체 중 접속이 끊겼습니다"; exit 1; }
echo "무중단 교체 확인 — 새 pid=$new_pid"
