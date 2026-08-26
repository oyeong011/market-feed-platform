"""성능 벤치마크 — README 와 대시보드에 올라가는 수치를 여기서 만든다.

수치를 손으로 옮겨 적지 않는 것이 핵심이다. 이 스크립트가 JSON 을 뱉고,
문서와 대시보드가 그 JSON 을 읽는다. 사람이 옮기는 순간 문서와 실제가 갈라진다.

    python bench/latency_bench.py --out docs/data/bench.json

측정 항목
---------
1. MDFP 인코딩/디코딩 처리량        — 프로토콜 계층 비용
2. 공유메모리 링버퍼 처리량          — 프로세스 간 hot path
3. UDS 버스 종단 지연               — 발행 → 구독 도착
4. HTTP 서버 처리량                 — 관리/조회 평면
5. 전체 파이프라인 (리플레이 최대속도) — 수집→버스→게이트웨이 총 비용

주의: 개발 노트북에서 잰 값이다. 절대 성능이 아니라 **계층별 상대 비용**과
      "이 구조가 어느 규모까지 견디는가"를 보기 위한 수치다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import socket
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdfeed.bus import UDSPublisher, UDSSubscriber          # noqa: E402
from mdfeed.httpd import HTTPServer, Response               # noqa: E402
from mdfeed.metrics import Histogram, Registry              # noqa: E402
from mdfeed.models import MSG_TRADE, Trade, now_ns          # noqa: E402
from mdfeed.protocol import FrameParser, encode             # noqa: E402
from mdfeed.ringbuffer import RingBuffer                    # noqa: E402


def bench_protocol(n: int = 300_000) -> dict:
    t = Trade("BINANCE", "BTCUSDT", now_ns(), now_ns(), 68123.45, 0.01, 1)
    payload = t.pack()

    s = time.perf_counter()
    for i in range(n):
        encode(MSG_TRADE, i, payload)
    enc = time.perf_counter() - s

    frames = b"".join(encode(MSG_TRADE, i, payload) for i in range(n // 10))
    p = FrameParser()
    s = time.perf_counter()
    count = sum(1 for _ in p.feed(frames))
    dec = time.perf_counter() - s

    s = time.perf_counter()
    for _ in range(n):
        Trade.unpack(payload)
    unp = time.perf_counter() - s

    return {
        "encode_ns_per_msg": round(enc / n * 1e9),
        "encode_msg_per_s": round(n / enc),
        "parse_ns_per_msg": round(dec / count * 1e9),
        "parse_msg_per_s": round(count / dec),
        "unpack_ns_per_msg": round(unp / n * 1e9),
        "frame_bytes": len(encode(MSG_TRADE, 0, payload)),
        "payload_bytes": len(payload),
        "overhead_pct": round((len(encode(MSG_TRADE, 0, payload)) / len(payload) - 1) * 100, 1),
    }


def bench_ringbuffer(n: int = 300_000) -> dict:
    ring = RingBuffer("mdfeed_bench_ring", capacity=65536, slot_size=128, create=True)
    try:
        payload = Trade("B", "S", now_ns(), now_ns(), 1.0, 1.0).pack()
        rd = ring.reader()

        s = time.perf_counter()
        for _ in range(n):
            ring.push(payload)
        push = time.perf_counter() - s

        s = time.perf_counter()
        got = 0
        while True:
            batch = rd.poll(4096)
            if not batch:
                break
            got += len(batch)
        poll = time.perf_counter() - s

        return {
            "push_ns_per_msg": round(push / n * 1e9),
            "push_msg_per_s": round(n / push),
            "poll_msg_per_s": round(got / poll) if poll > 0 else 0,
            "capacity": ring.capacity,
            "consumed": got,
            "skipped_by_lap": rd.skipped,
            "torn_reads": rd.torn,
        }
    finally:
        ring.close()


def bench_bus_latency(n: int = 2_000, rate_hz: int = 5_000) -> dict:
    """**속도를 제한한** 발행으로 IPC 순수 지연을 잰다.

    처음엔 최대 속도로 밀어 넣고 지연을 쟀는데 p50 이 105ms 로 나왔다. 이건
    IPC 지연이 아니라 **큐에 쌓여 기다린 시간**이다. 큐가 비어 있어야 "한 건이
    발행돼서 구독자에게 닿기까지"를 재는 것이 된다. 그래서 목표 주기에 맞춰
    페이싱하고, 처리량은 별도 함수에서 버스트로 잰다.

    벤치마크가 조용히 다른 걸 재고 있는 건 없는 것보다 나쁘다.
    """
    import tempfile
    run = tempfile.mkdtemp(prefix="mdfbench", dir="/tmp")
    path = os.path.join(run, "b.sock")

    async def main():
        pub = UDSPublisher(path, queue_size=1 << 16)
        await pub.start()
        sub = UDSSubscriber(path)
        hist = Histogram("bus")
        received = 0
        done = asyncio.Event()

        async def reader():
            nonlocal received
            async for f in sub.frames():
                sent_ns = int.from_bytes(f.payload[:8], "big")
                hist.record((time.time_ns() - sent_ns) / 1000.0)
                received += 1
                if received >= n:
                    done.set()
                    return

        task = asyncio.create_task(reader())
        await asyncio.sleep(0.4)

        period = 1.0 / rate_hz
        start = time.perf_counter()
        for i in range(n):
            pub.publish(encode(MSG_TRADE, i,
                               time.time_ns().to_bytes(8, "big") + b"\x00" * 56))
            # 큐가 비어 있도록 페이싱한다. 이게 없으면 큐잉 지연을 재게 된다.
            target = start + (i + 1) * period
            while True:
                slack = target - time.perf_counter()
                if slack <= 0:
                    break
                await asyncio.sleep(min(slack, 0.001))
        try:
            await asyncio.wait_for(done.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        backlog_max = pub.queue_size
        await pub.close()
        return hist.snapshot(), received, backlog_max

    snap, received, _ = asyncio.run(main())
    return {
        "sent": n, "received": received, "pacing_hz": rate_hz,
        "latency_p50_us": snap["p50_us"], "latency_p99_us": snap["p99_us"],
        "latency_p999_us": snap["p999_us"], "latency_max_us": snap["max_us"],
        "note": "큐가 비어 있는 상태에서의 순수 IPC 지연 (페이싱 적용)",
    }


def bench_bus_throughput(n: int = 50_000) -> dict:
    """버스트로 밀어 넣어 **처리량 상한**을 잰다. 지연은 여기서 보지 않는다."""
    import tempfile
    run = tempfile.mkdtemp(prefix="mdfbenchT", dir="/tmp")
    path = os.path.join(run, "b.sock")

    async def main():
        pub = UDSPublisher(path, queue_size=1 << 17)
        await pub.start()
        sub = UDSSubscriber(path)
        received = 0
        done = asyncio.Event()

        async def reader():
            nonlocal received
            async for _f in sub.frames():
                received += 1
                if received >= n:
                    done.set()
                    return

        task = asyncio.create_task(reader())
        await asyncio.sleep(0.4)
        payload = b"\x00" * 64
        start = time.perf_counter()
        for i in range(n):
            pub.publish(encode(MSG_TRADE, i, payload))
            if i % 1024 == 0:
                await asyncio.sleep(0)
        try:
            await asyncio.wait_for(done.wait(), timeout=60)
        except asyncio.TimeoutError:
            pass
        elapsed = time.perf_counter() - start
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        dropped = pub.dropped
        await pub.close()
        return received, elapsed, dropped

    received, elapsed, dropped = asyncio.run(main())
    return {
        "sent": n, "received": received, "dropped": dropped,
        "throughput_msg_per_s": round(received / elapsed) if elapsed else 0,
        "elapsed_s": round(elapsed, 3),
        "note": "버스트 발행. 큐잉 지연이 섞이므로 지연 지표로 쓰지 말 것",
    }


def bench_http(n: int = 3000) -> dict:
    port = 18999

    async def main():
        reg = Registry("bench")
        srv = HTTPServer("127.0.0.1", port, "bench", reg)
        srv.route("GET", "/ping", lambda r: Response.json({"ok": True}))
        await srv.start()

        def hammer():
            lat = []
            s = socket.create_connection(("127.0.0.1", port), timeout=5)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            req = b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n"
            start = time.perf_counter()
            for _ in range(n):
                t0 = time.perf_counter_ns()
                s.sendall(req)
                buf = b""
                while b"\r\n\r\n" not in buf:
                    buf += s.recv(4096)
                lat.append((time.perf_counter_ns() - t0) / 1000.0)
            total = time.perf_counter() - start
            s.close()
            return lat, total

        lat, total = await asyncio.to_thread(hammer)
        await srv.close()
        return lat, total

    lat, total = asyncio.run(main())
    lat.sort()
    return {
        "requests": n,
        "req_per_s": round(n / total),
        "latency_p50_us": round(lat[len(lat) // 2], 1),
        "latency_p99_us": round(lat[int(len(lat) * 0.99)], 1),
        "keep_alive": True,
    }


def bench_pipeline(replay_file: str) -> dict:
    """리플레이를 최대속도로 흘려 수집→버스→구독 전 구간 처리량을 잰다."""
    if not os.path.exists(replay_file):
        return {"skipped": f"녹화 파일 없음: {replay_file}"}

    import tempfile
    from mdfeed.config import Config
    from mdfeed.services.feedd import FeedDaemon

    run = tempfile.mkdtemp(prefix="mdfpipe", dir="/tmp")
    cfg = Config()
    cfg.run_dir = run
    cfg.bus_path = os.path.join(run, "bus.sock")
    cfg.signal_bus_path = os.path.join(run, "sig.sock")
    cfg.adapters = ["replay"]
    cfg.replay_file = replay_file
    cfg.replay_speed = 0.0        # 최대 속도
    cfg.replay_loop = False
    cfg.ring_enabled = True
    cfg.ring_name = "mdfeed_bench_pipe"
    cfg.http_host = "127.0.0.1"
    cfg.feedd_admin_port = 18100
    cfg.heartbeat_s = 3600

    async def main():
        stop = asyncio.Event()
        feed = FeedDaemon(cfg)
        task = asyncio.create_task(feed.run(stop))
        await asyncio.sleep(0.5)
        start = time.perf_counter()
        start_seq = feed.seq
        await asyncio.sleep(6.0)
        elapsed = time.perf_counter() - start
        published = feed.seq - start_seq
        stop.set()
        await asyncio.gather(task, return_exceptions=True)
        return published, elapsed

    published, elapsed = asyncio.run(main())
    return {
        "published": published,
        "elapsed_s": round(elapsed, 2),
        "throughput_msg_per_s": round(published / elapsed) if elapsed else 0,
        "note": "리플레이 최대속도. 수집→정규화→인코딩→버스+링 전 구간 포함",
    }


def main() -> int:
    ap = argparse.ArgumentParser("latency_bench")
    ap.add_argument("--out", default="")
    ap.add_argument("--replay", default="data/replay/sample.mdf")
    ap.add_argument("--quick", action="store_true", help="반복 횟수를 줄여 빠르게")
    args = ap.parse_args()

    scale = 10 if args.quick else 1
    print("환경:", platform.platform())
    print("Python:", platform.python_version(), "| CPU:", os.cpu_count(), "코어\n")

    results = {
        "env": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "machine": platform.machine(),
        },
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }

    print("1/5 MDFP 프로토콜...")
    results["protocol"] = bench_protocol(300_000 // scale)
    p = results["protocol"]
    print(f"    인코딩 {p['encode_msg_per_s']:>10,} msg/s ({p['encode_ns_per_msg']}ns)")
    print(f"    파싱   {p['parse_msg_per_s']:>10,} msg/s ({p['parse_ns_per_msg']}ns)")
    print(f"    프레임 {p['frame_bytes']}B (페이로드 {p['payload_bytes']}B, "
          f"오버헤드 {p['overhead_pct']}%)")

    print("2/5 공유메모리 링버퍼...")
    results["ringbuffer"] = bench_ringbuffer(300_000 // scale)
    r = results["ringbuffer"]
    print(f"    push {r['push_msg_per_s']:>12,} msg/s ({r['push_ns_per_msg']}ns)")
    print(f"    추월 감지 {r['skipped_by_lap']:,}건 / 찢긴 읽기 {r['torn_reads']}건")

    print("3/5 UDS 버스 (지연·처리량 분리 측정)...")
    results["bus_latency"] = bench_bus_latency(2_000 // max(scale // 5, 1))
    bl = results["bus_latency"]
    print(f"    순수 지연  p50 {bl['latency_p50_us']}µs / p99 {bl['latency_p99_us']}µs "
          f"/ max {bl['latency_max_us']}µs  (페이싱 {bl['pacing_hz']:,}Hz)")
    results["bus_throughput"] = bench_bus_throughput(50_000 // scale)
    bt = results["bus_throughput"]
    print(f"    처리량 상한 {bt['throughput_msg_per_s']:>10,} msg/s "
          f"(드롭 {bt['dropped']:,})")

    print("4/5 HTTP 서버...")
    results["http"] = bench_http(3000 // scale)
    h = results["http"]
    print(f"    {h['req_per_s']:,} req/s | p50 {h['latency_p50_us']}µs "
          f"p99 {h['latency_p99_us']}µs")

    print("5/5 전체 파이프라인 (리플레이 최대속도)...")
    results["pipeline"] = bench_pipeline(args.replay)
    pl = results["pipeline"]
    if "skipped" in pl:
        print(f"    건너뜀: {pl['skipped']}")
    else:
        print(f"    {pl['throughput_msg_per_s']:,} msg/s "
              f"({pl['published']:,}건 / {pl['elapsed_s']}초)")

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False, indent=1)
        print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
