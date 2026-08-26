#!/usr/bin/env bash
# MDFeed 워치독 — systemd 가 못 잡는 "살아있지만 일을 안 하는" 상태를 처리한다.
#
# systemd 의 Restart=on-failure 는 **프로세스가 죽어야** 동작한다. 그런데 피드
# 서비스의 실제 장애는 대부분 프로세스가 멀쩡한 채로 일어난다:
#   * TCP half-open — 소켓은 ESTABLISHED 인데 데이터가 안 온다
#   * 거래소가 구독을 조용히 해지 (에러 없이 스트림만 멎음)
#   * 이벤트 루프가 블로킹 호출에 물려 정지
# 이런 상태는 /healthz 로만 탐지된다. 그래서 헬스 판정 → 재시작 경로를 따로 둔다.
#
#   cron:  */1 * * * * /opt/mdfeed/ops/watchdog.sh >> /var/log/mdfeed/watchdog.log 2>&1
#   또는:  systemd timer (mdfeed-watchdog.timer)

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

STATE_DIR="${MDFEED_RUN_DIR:-/tmp/mdfeed}"
STATE="$STATE_DIR/watchdog.state"
MAX_RESTARTS_PER_HOUR=4          # 이 이상은 재시작으로 못 고치는 문제다
mkdir -p "$STATE_DIR"

log() { echo "$(date '+%F %T') [watchdog] $*"; }

restart_count() {
  local now cutoff
  now=$(date +%s); cutoff=$((now - 3600))
  [ -f "$STATE" ] || return 0
  awk -v c="$cutoff" '$1 > c' "$STATE" | wc -l | tr -d ' '
}

record_restart() {
  local now; now=$(date +%s)
  echo "$now $1" >> "$STATE"
  # 1시간 지난 기록은 버린다
  awk -v c="$((now - 3600))" '$1 > c' "$STATE" > "$STATE.tmp" && mv "$STATE.tmp" "$STATE"
}

python3 ops/healthcheck.py --quiet
rc=$?

if [ "$rc" -eq 0 ]; then
  exit 0
fi

log "헬스체크 실패 (exit=$rc)"
python3 ops/healthcheck.py | sed 's/^/  /'

if [ "$rc" -lt 2 ]; then
  log "WARN 수준 — 재시작하지 않고 기록만 남김"
  exit 0
fi

n=$(restart_count)
if [ "$n" -ge "$MAX_RESTARTS_PER_HOUR" ]; then
  log "최근 1시간 재시작 $n회 — 임계치($MAX_RESTARTS_PER_HOUR) 도달."
  log "재시작으로 해결되지 않는 문제다. 사람이 개입해야 한다 (RUNBOOK.md 참고)."
  # 알림 훅: 환경변수로 지정된 명령을 부른다 (슬랙/PagerDuty 등)
  [ -n "${MDFEED_ALERT_CMD:-}" ] && eval "$MDFEED_ALERT_CMD" \
    "'MDFeed: 재시작 임계 도달, 수동 개입 필요'"
  exit 2
fi

if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  log "CRIT — mdfeed.target 재시작 (최근 1시간 ${n}회)"
  systemctl restart mdfeed.target
else
  log "CRIT — systemd 없음. 수동 재시작 필요: make down && make up"
fi
record_restart "restart"
sleep 15
python3 ops/healthcheck.py --quiet && log "복구 확인" || log "재시작 후에도 비정상"
