#!/usr/bin/env bash
# bash 로 쓴다 — 리눅스 CI 러너에는 zsh 가 없다 (첫 실행에서 'required file not found' 로 죽었다).
# 같은 feedd(리플레이 40배속) 아래에서 파이썬/C++ 게이트웨이를 번갈아 같은 부하 시험에 건다.
# 결과: docs/data/load_gateway_{python,cpp}.json + docs/data/gateway_compare.json
set -u
cd "$(dirname "$0")/.."
RUN=${MDFEED_RUN_DIR:-/tmp/mdfcmp}; rm -rf "$RUN"; mkdir -p "$RUN"
export MDFEED_RUN_DIR=$RUN MDFEED_BUS_PATH=$RUN/bus.sock MDFEED_ADAPTERS=replay MDFEED_REPLAY_FILE=data/replay/sample.mdf
export MDFEED_REPLAY_LOOP=1 MDFEED_REPLAY_SPEED=${REPLAY_SPEED:-40} MDFEED_RING_ENABLED=0 MDFEED_STORAGE_PROFILE=test MDFEED_STORAGE_BACKEND=sqlite
export MDFEED_SQLITE_PATH=$RUN/x.db MDFEED_HTTP_HOST=127.0.0.1 MDFEED_TCP_HOST=127.0.0.1 MDFEED_FEEDD_ADMIN_PORT=29100
export MDFEED_TCP_PORT=29101 MDFEED_TCP_ADMIN_PORT=29111 PYTHONPATH=src
SUBS=${SUBS:-"10 50 100 200"}; SECONDS_PER=${SECONDS_PER:-8}; PROCS=${PROCS:-8}
CLIENT=${CLIENT:-python}          # python = bench/load_test.py (스레드/프로세스) · cpp = cpp/build/load_client (단일 스레드 poll)
GATEWAYS=${GATEWAYS:-"python cpp"}
SUFFIX=""; [ "$CLIENT" = "cpp" ] && SUFFIX="_cppclient"
.venv/bin/python -m mdfeed.services.feedd > "$RUN/feedd.log" 2>&1 &
FEEDD=$!; sleep 2
run_round() {
  local label=$1; shift
  "$@" > "$RUN/gw_$label.log" 2>&1 &
  local GW=$!; sleep 1.5
  echo "[$label] $(curl -sf http://127.0.0.1:29111/healthz | head -c 90)"
  if [ "$CLIENT" = "cpp" ]; then
    ./cpp/build/load_client --port 29101 --admin 29111 --subscribers $SUBS \
      --seconds "$SECONDS_PER" --out "docs/data/load_gateway_${label}${SUFFIX}.json"
  else
    .venv/bin/python bench/load_test.py --port 29101 --admin 29111 --subscribers $SUBS \
      --seconds "$SECONDS_PER" --processes "$PROCS" --out "docs/data/load_gateway_${label}${SUFFIX}.json"
  fi
  kill $GW; wait $GW 2>/dev/null
}
for g in $GATEWAYS; do
  case $g in
    python) run_round python .venv/bin/python -m mdfeed.services.tcp_gateway ;;
    cpp)    run_round cpp ./cpp/build/tcp_gateway ;;
  esac
  sleep 1
done
kill $FEEDD; wait $FEEDD 2>/dev/null
[ "$GATEWAYS" = "python cpp" ] || exit 0
SUFFIX="$SUFFIX" .venv/bin/python - <<'PY'
import json, datetime, os
sfx = os.environ.get("SUFFIX", "")
py=json.load(open(f"docs/data/load_gateway_python{sfx}.json")); cp=json.load(open(f"docs/data/load_gateway_cpp{sfx}.json"))
rows=[]
for a,b in zip(py["rounds"], cp["rounds"]):
    g=lambda k,x: x.get(k) or 0
    rows.append({"subscribers": a["subscribers"], "upstream_msg_per_s": {"python": g("upstream_msg_per_s",a), "cpp": g("upstream_msg_per_s",b)},
                 "p50_ms": {"python": round(g("latency_p50_us",a)/1000,2), "cpp": round(g("latency_p50_us",b)/1000,2)},
                 "p99_ms": {"python": round(g("latency_p99_us",a)/1000,2), "cpp": round(g("latency_p99_us",b)/1000,2)},
                 "max_ms": {"python": round(g("latency_max_us",a)/1000,2), "cpp": round(g("latency_max_us",b)/1000,2)},
                 "throughput_retained_pct": {"python": a.get("throughput_retained_pct"), "cpp": b.get("throughput_retained_pct")},
                 "lost_messages": {"python": a.get("lost_messages"), "cpp": b.get("lost_messages")}})
out={"generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
     "method": "같은 feedd(리플레이 배속) 아래에서 파이썬/C++ 게이트웨이를 번갈아 기동. bench/load_test.py 로 같은 부하. 게이트웨이·클라이언트 같은 기계.",
     "caveat": "한 번 실행한 값이며 기계 부하에 따라 흔들린다. 구독자가 많으면 파이썬 클라이언트 스레드도 병목이라 절대값보다 두 구현의 상대 비교로 본다.",
     "rounds": rows}
out["client"] = "c++ (cpp/bench/load_client.cpp)" if sfx else "python (bench/load_test.py)"
json.dump(out, open(f"docs/data/gateway_compare{sfx}.json","w"), ensure_ascii=False, indent=1)
print(f"{'subs':>5} | {'py p50':>7} {'py p99':>8} | {'cpp p50':>7} {'cpp p99':>8} | p99 개선")
for c in rows: print(f"{c['subscribers']:>5} | {c['p50_ms']['python']:>5.1f}ms {c['p99_ms']['python']:>6.1f}ms | {c['p50_ms']['cpp']:>5.1f}ms {c['p99_ms']['cpp']:>6.1f}ms | {c['p99_ms']['python']/max(c['p99_ms']['cpp'],1e-9):.0f}x")
PY
