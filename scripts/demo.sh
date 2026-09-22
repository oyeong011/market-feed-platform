#!/usr/bin/env bash
# MDFeed 원클릭 데모 — 스택을 띄우고 무엇을 보면 되는지 안내한다.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

G=$'\e[32m'; B=$'\e[1m'; C=$'\e[36m'; N=$'\e[0m'

echo "${B}MDFeed 데모${N}"
echo "───────────────────────────────────────────────"

if [ ! -f data/replay/sample.mdf ]; then
  echo "오프라인 데모 녹화 파일이 없습니다: data/replay/sample.mdf"
  echo "라이브 녹화는 'make record' 로 명시적으로 실행하세요."
  exit 1
fi

if [ -n "${MDFEED_DEMO_DIR:-}" ]; then
  DEMO_DIR="$MDFEED_DEMO_DIR"
  CLEAN_DEMO_DIR=0
else
  DEMO_DIR="$(mktemp -d /tmp/mdfeed-demo.XXXXXX)"
  CLEAN_DEMO_DIR=1
fi
mkdir -p "$DEMO_DIR/run"
export MDFEED_ADAPTERS=replay
export MDFEED_REPLAY_LOOP="${MDFEED_REPLAY_LOOP:-1}"
export MDFEED_STORAGE_BACKEND=sqlite
export MDFEED_STORAGE_PROFILE=test
export MDFEED_SQLITE_PATH="$DEMO_DIR/mdfeed.db"
export MDFEED_RUN_DIR="$DEMO_DIR/run"
export MDFEED_BUS_PATH="$DEMO_DIR/run/bus.sock"
export MDFEED_SIGNAL_BUS_PATH="$DEMO_DIR/run/signals.sock"

echo "모드: replay + synthetic SQLite ($MDFEED_SQLITE_PATH)"

pkill -f "mdfeed.cli up" 2>/dev/null; pkill -f "mdfeed.services" 2>/dev/null; sleep 1

PYTHONPATH=src python3 -m mdfeed.cli up > /tmp/mdfeed-demo.log 2>&1 &
SUP=$!
trap 'echo ""; echo "종료 중..."; kill $SUP 2>/dev/null; pkill -f "mdfeed.services" 2>/dev/null; if [ "$CLEAN_DEMO_DIR" = 1 ]; then rm -rf "$DEMO_DIR"; fi; exit 0' INT TERM

echo -n "기동 대기"
for _ in $(seq 1 20); do
  if curl -s --max-time 1 http://127.0.0.1:9100/readyz 2>/dev/null | grep -q '"ready": true'; then break; fi
  echo -n "."; sleep 1
done
echo " ${G}준비 완료${N}"
echo ""

bash ops/ops.sh status
echo ""
echo "${B}볼 곳${N}"
echo "  ${C}대시보드${N}      http://localhost:9102/            브라우저에서 실시간 시세"
echo "  ${C}헬스${N}          curl localhost:9100/healthz | python3 -m json.tool"
echo "  ${C}Prometheus${N}    curl localhost:9100/metrics"
echo "  ${C}스냅샷${N}        curl localhost:9100/snapshot | python3 -m json.tool"
echo "  ${C}REST 조회${N}     curl 'localhost:9103/api/v1/bars?venue=UPBIT&symbol=KRW-BTC'"
echo "  ${C}TCP 구독${N}      make client"
echo "  ${C}진단${N}          make diag"
echo ""
echo "Ctrl-C 로 종료합니다. 스택 로그: /tmp/mdfeed-demo.log"
wait $SUP
