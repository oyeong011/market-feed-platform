"""배포단 부하 시험 — 구독자 수를 늘려 가며 어디서 무너지는지 잰다.

왜 이걸 재나
------------
DESIGN.md §2.5 에 이렇게 적어 뒀다.

    구독자별 seq 재넘버링은 구독자 수만큼 프레임을 재인코딩한다.
    CRC 가 본문 전체를 덮으므로 헤더만 갈아끼울 수 없다.
    수백 명 규모가 되면 채널 기반 시퀀싱으로 바꿔야 한다.

**"수백 명"이 몇 명인지 안 재고 써 놓은 문장이다.** 근거 없는 한계 서술은
없느니만 못하다. 실제로 재서 곡선을 그린다.

측정 방법
---------
구독자 N 명을 동시에 붙이고 일정 시간 받아, 구독자마다
`게이트웨이 수신시각 → 클라이언트 도착시각` 지연을 잰다.
N 을 늘려 가며 다음을 본다.

* 구독자당 처리량이 유지되는가 (팬아웃이 선형인가)
* 지연 p99 가 어디서 꺾이는가
* 드롭이 언제부터 생기는가 (백프레셔가 도는 지점)

각 구독자는 별도 스레드에서 블로킹 소켓으로 받는다. asyncio 로 한 프로세스에
몰면 클라이언트 쪽 이벤트 루프가 먼저 병목이 되어 서버를 재는 게 아니게 된다.

    python bench/load_test.py --subscribers 1 10 50 100 200 --seconds 12
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdfeed.models import MSG_BOOK, MSG_TRADE, BookTop, Trade, now_ns  # noqa: E402
from mdfeed.protocol import FLAG_SNAPSHOT, FrameParser, SequenceTracker  # noqa: E402


class Subscriber(threading.Thread):
    """구독자 한 명. 블로킹 소켓 + 자체 스레드."""

    def __init__(self, host: str, port: int, seconds: float, symbols=None):
        super().__init__(daemon=True)
        self.host, self.port, self.seconds = host, port, seconds
        self.symbols = symbols
        self.latencies: list[float] = []
        self.messages = 0
        self.bytes_in = 0
        self.gaps = 0
        self.lost = 0
        self.crc_errors = 0
        self.connect_error: str | None = None

    def run(self) -> None:
        try:
            s = socket.create_connection((self.host, self.port), timeout=10)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception as e:                       # noqa: BLE001
            self.connect_error = f"{type(e).__name__}: {e}"
            return
        parser, track = FrameParser(), SequenceTracker()
        deadline = time.time() + self.seconds
        s.settimeout(1.0)
        try:
            while time.time() < deadline:
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                self.bytes_in += len(chunk)
                for f in parser.feed(chunk):
                    if f.flags & FLAG_SNAPSHOT:
                        continue
                    lost = track.observe(f.seq)
                    if lost:
                        self.gaps += 1
                        self.lost += lost
                    if f.msg_type == MSG_TRADE and len(f.payload) >= Trade.SIZE:
                        t = Trade.unpack(f.payload)
                        # feedd 수신시각 → 여기 도착까지. 배포단이 더한 지연이다.
                        self.latencies.append((now_ns() - t.ts_recv_ns) / 1000.0)
                        self.messages += 1
                    elif f.msg_type == MSG_BOOK:
                        self.messages += 1
        finally:
            self.crc_errors = parser.crc_error_count
            try:
                s.close()
            except Exception:                        # noqa: BLE001
                pass


def pct(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = min(int(len(xs) * q / 100.0), len(xs) - 1)
    return xs[i]


def gateway_stats(admin_port: int) -> dict:
    import urllib.request
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{admin_port}/healthz", timeout=3) as r:
            return json.loads(r.read())
    except Exception:                                # noqa: BLE001
        return {}


def run_round(host: str, port: int, n: int, seconds: float, admin: int) -> dict:
    subs = [Subscriber(host, port, seconds) for _ in range(n)]
    t0 = time.perf_counter()
    for s in subs:
        s.start()
        time.sleep(0.004)          # 접속 폭주로 SYN 큐가 넘치지 않게 살짝 흘린다
    for s in subs:
        s.join(timeout=seconds + 20)
    elapsed = time.perf_counter() - t0

    connected = [s for s in subs if s.connect_error is None]
    failed = [s for s in subs if s.connect_error]
    all_lat = [x for s in connected for x in s.latencies]
    total_msg = sum(s.messages for s in connected)
    per_sub = [s.messages for s in connected]

    g = gateway_stats(admin)
    return {
        "subscribers": n,
        "connected": len(connected),
        "connect_failed": len(failed),
        "connect_error_sample": failed[0].connect_error if failed else None,
        "elapsed_s": round(elapsed, 2),
        "total_messages": total_msg,
        "msg_per_s_total": round(total_msg / elapsed) if elapsed else 0,
        "msg_per_s_per_sub": round(statistics.mean(per_sub) / seconds, 1) if per_sub else 0,
        "per_sub_min": min(per_sub) if per_sub else 0,
        "per_sub_max": max(per_sub) if per_sub else 0,
        "bytes_total": sum(s.bytes_in for s in connected),
        "latency_p50_us": round(pct(all_lat, 50), 1),
        "latency_p95_us": round(pct(all_lat, 95), 1),
        "latency_p99_us": round(pct(all_lat, 99), 1),
        "latency_max_us": round(max(all_lat), 1) if all_lat else 0,
        "gaps": sum(s.gaps for s in connected),
        "lost_messages": sum(s.lost for s in connected),
        "crc_errors": sum(s.crc_errors for s in connected),
        "gateway_dropped": g.get("total_dropped"),
        "gateway_subscribers": g.get("subscribers"),
    }


def main() -> int:
    ap = argparse.ArgumentParser("load_test")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9101)
    ap.add_argument("--admin", type=int, default=9111)
    ap.add_argument("--subscribers", type=int, nargs="+", default=[1, 10, 50, 100, 200])
    ap.add_argument("--seconds", type=float, default=12.0)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    print(f"대상 {args.host}:{args.port} · 회차당 {args.seconds:.0f}초\n")
    print(f"{'구독자':>6} {'접속':>6} {'총 msg/s':>10} {'1인당 msg/s':>12} "
          f"{'p50':>9} {'p99':>10} {'max':>10} {'유실':>7} {'드롭':>8}")
    print("-" * 88)

    rounds = []
    for n in args.subscribers:
        r = run_round(args.host, args.port, n, args.seconds, args.admin)
        rounds.append(r)
        print(f"{r['subscribers']:>6} {r['connected']:>6} "
              f"{r['msg_per_s_total']:>10,} {r['msg_per_s_per_sub']:>12,.1f} "
              f"{r['latency_p50_us']:>8,.0f}µ {r['latency_p99_us']:>9,.0f}µ "
              f"{r['latency_max_us']:>9,.0f}µ {r['lost_messages']:>7,} "
              f"{str(r['gateway_dropped']):>8}")
        time.sleep(3)              # 게이트웨이가 연결을 정리할 시간

    # 팬아웃이 선형인지 — 1인당 처리량이 유지되면 선형이다
    base = rounds[0]["msg_per_s_per_sub"] or 1
    for r in rounds:
        r["throughput_retained_pct"] = round(r["msg_per_s_per_sub"] / base * 100, 1)

    print("-" * 88)
    print(f"{'구독자':>6} {'1인당 처리량 유지율':>22} {'p99 배율':>12}")
    p99base = rounds[0]["latency_p99_us"] or 1
    for r in rounds:
        print(f"{r['subscribers']:>6} {r['throughput_retained_pct']:>21.1f}% "
              f"{r['latency_p99_us'] / p99base:>11.1f}x")

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "target": f"{args.host}:{args.port}",
        "seconds_per_round": args.seconds,
        "rounds": rounds,
    }
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
        print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
