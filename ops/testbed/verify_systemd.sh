#!/usr/bin/env bash
# systemd 유닛을 실제 리눅스에서 검증한다.
#
# 유닛 파일을 "썼다"와 "돌려봤다"는 다르다. Restart 정책, TimeoutStopSec,
# ProtectSystem=strict, 그리고 무엇보다 **의존 관계가 재시작을 어떻게 전파하는지**는
# 실제 systemd 아래에서만 드러난다. 이 스크립트로 Requires= 가 소비자까지
# 끌고 내려가는 문제를 찾았다.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
C=mdfeed-systemd-verify
G=$'\e[32m'; R=$'\e[31m'; N=$'\e[0m'
pass() { echo "  ${G}PASS${N} $*"; }
fail() { echo "  ${R}FAIL${N} $*"; FAILED=1; }
FAILED=0

command -v docker >/dev/null || { echo "Docker 가 필요합니다"; exit 1; }
echo "── systemd 테스트베드 준비 ──"
docker build -q -f ops/testbed/Dockerfile.systemd -t mdfeed-systemd-test . >/dev/null
docker rm -f "$C" >/dev/null 2>&1
docker run -d --name "$C" --privileged --cgroupns=host \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw -v "$PWD:/src:ro" mdfeed-systemd-test >/dev/null
for _ in $(seq 1 30); do
  [ "$(docker exec "$C" systemctl is-system-running 2>/dev/null)" != "" ] && break
  sleep 1
done
docker exec "$C" bash -c "cp -r /src /work && cd /work && rm -rf .venv .git data/mdfeed.db*" >/dev/null 2>&1

echo "── 설치 ──"
docker exec -w /work "$C" bash ops/ops.sh install >/dev/null 2>&1 \
  && pass "ops.sh install" || fail "ops.sh install"
docker exec "$C" sed -i 's|^MDFEED_ADAPTERS=.*|MDFEED_ADAPTERS=upbit,binance|' /etc/mdfeed/mdfeed.env

echo "── 기동 ──"
docker exec "$C" systemctl start mdfeed.target
sleep 25
n=$(docker exec "$C" systemctl --no-pager --plain list-units 'mdfeed-*.service' 2>/dev/null | grep -c "active running")
[ "$n" -eq 6 ] && pass "6개 서비스 active (실제: $n)" || fail "서비스 기동 (active: $n/6)"

echo "── 보안 강화 적용 ──"
for k in "ProtectSystem=strict" "NoNewPrivileges=yes" "User=mdfeed" "LimitNOFILE=65536"; do
  key=${k%%=*}; want=${k##*=}
  got=$(docker exec "$C" systemctl show mdfeed-feedd -p "$key" --value 2>/dev/null)
  [ "$got" = "$want" ] && pass "$key=$got" || fail "$key: 기대 $want, 실제 $got"
done

echo "── SIGKILL 후 재시작, 소비자는 생존 ──"
before_feedd=$(docker exec "$C" systemctl show mdfeed-feedd -p MainPID --value)
before_wr=$(docker exec "$C" systemctl show mdfeed-writer -p MainPID --value)
docker exec "$C" kill -9 "$before_feedd" 2>/dev/null
sleep 14
after_feedd=$(docker exec "$C" systemctl show mdfeed-feedd -p MainPID --value)
after_wr=$(docker exec "$C" systemctl show mdfeed-writer -p MainPID --value)
[ "$before_feedd" != "$after_feedd" ] && pass "feedd 재시작 ($before_feedd → $after_feedd)" \
  || fail "feedd 재시작 안 됨"
[ "$before_wr" = "$after_wr" ] && pass "writer 생존 (PID $after_wr) — Wants= 로 재시작 미전파" \
  || fail "writer 가 함께 재시작됨 ($before_wr → $after_wr). Requires= 를 쓰고 있지 않은지 확인"

echo "── 자동 재접속 ──"
sleep 8
docker exec "$C" journalctl -u mdfeed-writer -n 40 --no-pager -o cat 2>/dev/null \
  | grep -q "bus subscriber connected" && pass "버스 재접속 로그 확인" || fail "재접속 로그 없음"

echo "── 우아한 종료 ──"
rows_before=$(docker exec "$C" python3 -c "
import sqlite3;print(sqlite3.connect('/var/lib/mdfeed/mdfeed.db').execute('SELECT COUNT(*) FROM trades').fetchone()[0])" 2>/dev/null || echo 0)
t0=$(date +%s); docker exec "$C" systemctl stop mdfeed.target; t1=$(date +%s)
[ $((t1-t0)) -lt 30 ] && pass "정지 $((t1-t0))초 (TimeoutStopSec=30 이내)" || fail "정지에 $((t1-t0))초"
rows_after=$(docker exec "$C" python3 -c "
import sqlite3;print(sqlite3.connect('/var/lib/mdfeed/mdfeed.db').execute('SELECT COUNT(*) FROM trades').fetchone()[0])" 2>/dev/null || echo 0)
[ "$rows_after" -ge "$rows_before" ] && pass "종료 flush ($rows_before → $rows_after 행)" || fail "flush 실패"

docker rm -f "$C" >/dev/null 2>&1
echo ""
[ "$FAILED" = "0" ] && echo "${G}systemd 검증 통과${N}" || echo "${R}실패 항목 있음${N}"
exit "$FAILED"
