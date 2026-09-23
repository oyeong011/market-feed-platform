#!/usr/bin/env bash
# 팬아웃 비용 비교 — 같은 수집기 아래에서 TCP 게이트웨이와 UDP 멀티캐스트 발행자의
# **발행 측 비용**(시스템 콜 수·바이트·CPU)이 구독자 수에 따라 어떻게 변하는가.
#
# TCP 는 구독자마다 send() 를 부르니 선형으로 는다. 멀티캐스트는 한 번 쏘면 끝이라 평탄해야 한다 —
# 그 주장을 재는 게 이 스크립트다. 결과: docs/data/fanout_cost.json
#
# 한계(정직하게): 한 호스트에서 구독자 N 명이면 멀티캐스트도 커널이 N 번 복제한다. 실제 배포에서는
# 그 복제를 스위치가 한다. 그래서 여기서 보는 건 **발행 호스트의 비용**이고, N≥500 에서는 수신 측이
# 기계를 포화시켜 측정이 무너진다 — 설계가 아니라 측정의 한계다.
set -u
cd "$(dirname "$0")/.."
RUN=${MDFEED_RUN_DIR:-/tmp/mdffan}; rm -rf "$RUN"; mkdir -p "$RUN"
SUBS=${SUBS:-"1 10 50 100"}; SECONDS_PER=${SECONDS_PER:-6}; SPEED=${SPEED:-300}
export MDFEED_RUN_DIR=$RUN MDFEED_BUS_PATH=$RUN/bus.sock MDFEED_ADAPTERS=replay MDFEED_REPLAY_FILE=data/replay/sample.mdf
export MDFEED_REPLAY_LOOP=1 MDFEED_REPLAY_SPEED=$SPEED MDFEED_RING_ENABLED=0 MDFEED_STORAGE_PROFILE=test
export MDFEED_STORAGE_BACKEND=sqlite MDFEED_SQLITE_PATH=$RUN/x.db MDFEED_HTTP_HOST=127.0.0.1 MDFEED_TCP_HOST=127.0.0.1
export MDFEED_FEEDD_ADMIN_PORT=29100 MDFEED_TCP_PORT=29101 MDFEED_TCP_ADMIN_PORT=29111 PYTHONPATH=src
export MDFEED_MCAST_GROUP=${MCAST_GROUP:-239.192.9.1} MDFEED_MCAST_PORT=29140 MDFEED_MCAST_RECOVERY_PORT=29141 MDFEED_MCAST_ADMIN_PORT=29142
[ -n "${MCAST_IF:-}" ] && export MDFEED_MCAST_IF=$MCAST_IF

.venv/bin/python -m mdfeed.services.feedd > "$RUN/feedd.log" 2>&1 & FEEDD=$!; sleep 2

cpu_sampler() { while kill -0 "$1" 2>/dev/null; do ps -o %cpu= -p "$1"; sleep 0.5; done; }

# ── TCP: 구독자 N 명, 발행 측 send() 수를 /metrics 에서 읽는다 ──
./cpp/build/tcp_gateway > "$RUN/gw.log" 2>&1 & GW=$!; sleep 1.5
cpu_sampler $GW > "$RUN/cpu_tcp.txt" & S1=$!
./cpp/build/load_client --port 29101 --admin 29111 --subscribers $SUBS --seconds "$SECONDS_PER" --gap 2 --out "$RUN/tcp.json" > "$RUN/tcp_client.txt" 2>&1
kill $S1 2>/dev/null
TCP_CPU=$(awk '{s+=$1;n++} END{printf "%.0f", (n?s/n:0)}' "$RUN/cpu_tcp.txt")
curl -s localhost:29111/metrics > "$RUN/tcp_metrics.txt"
kill $GW; wait $GW 2>/dev/null; sleep 1

# ── 멀티캐스트: 같은 N, 발행 측 datagram 수를 /healthz 에서 읽는다 ──
./cpp/build/mcast_publisher > "$RUN/mc.log" 2>&1 & MC=$!; sleep 1.5
cpu_sampler $MC > "$RUN/cpu_mcast.txt" & S2=$!
./cpp/build/mcast_load_client --group "$MDFEED_MCAST_GROUP" --port 29140 --admin 29142 ${MCAST_IF:+--iface $MCAST_IF} \
  --subscribers $SUBS --seconds "$SECONDS_PER" --gap 2 --out "$RUN/mcast.json" > "$RUN/mcast_client.txt" 2>&1
kill $S2 2>/dev/null
MC_CPU=$(awk '{s+=$1;n++} END{printf "%.0f", (n?s/n:0)}' "$RUN/cpu_mcast.txt")
kill $MC $FEEDD; wait 2>/dev/null

TCP_CPU=$TCP_CPU MC_CPU=$MC_CPU RUN=$RUN .venv/bin/python - <<'PY'
import json, os, re, datetime
run = os.environ["RUN"]
tcp = json.load(open(f"{run}/tcp.json"))["rounds"]
mc = json.load(open(f"{run}/mcast.json"))["rounds"]
rows = []
for t, m in zip(tcp, mc):
    out_frames = t["msg_per_s_total"]
    rows.append({
        "subscribers": t["subscribers"],
        "upstream_msg_per_s": round(t["upstream_msg_per_s"], 1),
        "tcp": {"frames_out_per_s": round(out_frames), "p50_ms": round(t["latency_p50_us"]/1000, 2), "p99_ms": round(t["latency_p99_us"]/1000, 2), "lost": t["lost_messages"]},
        "mcast": {"publish_datagrams_per_s": round(m["publish_datagrams_per_s"]), "publish_mb_per_s": round(m["publish_mb_per_s"], 2),
                  "p50_ms": round(m["latency_p50_us"]/1000, 2), "p99_ms": round(m["latency_p99_us"]/1000, 2), "lost": m["lost_messages"]},
    })
doc = {
    "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
    "method": "같은 리플레이 수집기 아래에서 TCP 게이트웨이와 UDP 멀티캐스트 발행자를 번갈아 기동. 같은 구독자 수, 같은 회차 길이. 발행 측 비용은 각 프로세스의 관리 포트에서 읽는다.",
    "caveat": "한 호스트에서 구독자 N 명이면 멀티캐스트도 커널이 N 번 복제한다. 실제 배포에서는 스위치가 복제한다. 여기서 보는 건 발행 호스트의 비용이며, N 이 커지면 수신 측이 기계를 포화시켜 측정이 무너진다 — 설계가 아니라 측정의 한계다.",
    "publisher_cpu_mean_pct": {"tcp_gateway": float(os.environ["TCP_CPU"]), "mcast_publisher": float(os.environ["MC_CPU"])},
    "rounds": rows,
}
mt = open(f"{run}/tcp_metrics.txt").read()
for k in ("send_calls_total", "send_bytes_total", "sent_total", "bus_reads_total"):
    m = re.search(rf'mdfeed_{k}\{{[^}}]*\}} ([0-9.e+-]+)', mt)
    if m:
        doc.setdefault("tcp_totals_at_end", {})[k] = float(m.group(1))
json.dump(doc, open("docs/data/fanout_cost.json", "w"), ensure_ascii=False, indent=1)
print(f"{'구독자':>6} | {'TCP 나간 frame/s':>16} {'p99':>8} | {'MCAST 발행 dgram/s':>18} {'MB/s':>7} {'p99':>8}")
for r in rows:
    print(f"{r['subscribers']:>6} | {r['tcp']['frames_out_per_s']:>16,} {r['tcp']['p99_ms']:>7.1f}ms | {r['mcast']['publish_datagrams_per_s']:>18,} {r['mcast']['publish_mb_per_s']:>7.2f} {r['mcast']['p99_ms']:>7.1f}ms")
print(f"발행 측 CPU 평균: TCP {doc['publisher_cpu_mean_pct']['tcp_gateway']}% · 멀티캐스트 {doc['publisher_cpu_mean_pct']['mcast_publisher']}%")
PY
