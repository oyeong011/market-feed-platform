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
import contextlib
import json
import os
import socket
import statistics
import tempfile
import sys
import threading
import time
from pathlib import Path

# 워커 프로세스 간에 지연 원자료를 넘길 때의 상한. p99 판정에는 넉넉하다.
LAT_SAMPLE_CAP = 60_000

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


def _spawn_workers(host: str, port: int, n: int, seconds: float,
                   processes: int, start_at: float, on_start=None) -> list[dict]:
    """구독자를 여러 프로세스에 나눠 붙인다.

    한 프로세스에 다 담으면 **측정 도구가 먼저 병목이 된다.** 100명이 각자
    초당 수백 건을 받으면 GIL 위에서 100개 스레드가 깨어나 경합한다.

    실측(2026-08-29):
        1프로세스 100명   상류 305건/s   p99 2,691ms
        4프로세스×25명    상류 519건/s   p99   363ms
    상류가 1.7배 높은 쪽이 7배 빨랐다. 그때 게이트웨이 큐 깊이와 소켓 전송
    버퍼는 20초 내내 0 이었다 — 서버는 안 밀렸다는 뜻이다.
    """
    import subprocess

    share, extra = divmod(n, processes)
    counts = [share + (1 if i < extra else 0) for i in range(processes)]
    counts = [c for c in counts if c]

    procs, outs = [], []
    for c in counts:
        fd, path = tempfile.mkstemp(suffix=".json", prefix="loadtest-")
        os.close(fd)
        outs.append(path)
        procs.append(subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--worker", path,
             "--host", host, "--port", str(port), "--worker-count", str(c),
             "--seconds", str(seconds), "--start-at", repr(start_at)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE))

    if on_start is not None:
        # 상류 유량을 재는 창은 **구독자가 실제로 붙기 시작한 뒤**여야 한다.
        # 프로세스 기동과 시작 정렬 대기(3초)가 분모에 섞이면 상류가 과대평가되고,
        # 수신율이 실제보다 낮게 나온다. (처음에 4명에서 69.7% 로 찍혔다.)
        delay = start_at - time.time()
        if delay > 0:
            time.sleep(delay)
        on_start()

    results = []
    for proc, path in zip(procs, outs):
        _, err = proc.communicate(timeout=seconds + 120)
        try:
            with open(path, encoding="utf-8") as fh:
                results.append(json.load(fh))
        except Exception as e:                       # noqa: BLE001
            print(f"  [경고] 워커 결과를 못 읽었다: {e} {err[-200:]!r}")
        finally:
            with contextlib.suppress(OSError):
                os.unlink(path)
    return results


def _run_subscribers(host: str, port: int, n: int, seconds: float,
                     start_at: float | None = None) -> dict:
    """이 프로세스 안에서 구독자 n 명을 돌리고 원자료를 낸다."""
    if start_at:                       # 여러 프로세스의 회차를 맞춘다
        delay = start_at - time.time()
        if delay > 0:
            time.sleep(delay)
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
    lat = [x for s in connected for x in s.latencies]
    # 프로세스 간에 원자료를 넘기므로 상한을 둔다. p99 판정에는 넉넉하다.
    if len(lat) > LAT_SAMPLE_CAP:
        stride = len(lat) // LAT_SAMPLE_CAP + 1
        lat = lat[::stride]
    return {
        "elapsed_s": elapsed,
        "connected": len(connected),
        "connect_failed": len(failed),
        "connect_error_sample": failed[0].connect_error if failed else None,
        "per_sub": [s.messages for s in connected],
        "latencies": lat,
        "bytes_total": sum(s.bytes_in for s in connected),
        "gaps": sum(s.gaps for s in connected),
        "lost": sum(s.lost for s in connected),
        "crc_errors": sum(s.crc_errors for s in connected),
    }


def run_round(host: str, port: int, n: int, seconds: float, admin: int,
              processes: int = 1) -> dict:
    # 상류 유량을 회차마다 같이 잰다. 이게 없으면 회차 간 비교가 성립하지 않는다.
    # 실측: 배치 전송 전후를 비교하려다 상류가 227건/s → 140건/s 로 달라져
    # 개선인지 시장이 한산해진 것인지 구분할 수 없었다. 종목 수를 늘리면
    # 유량이 바뀌므로 이 값 없이 잰 수치는 다른 날짜와 비교할 수 없다.
    procs = max(1, min(processes, n))
    win: dict = {}

    if procs == 1:
        win["g0"] = gateway_stats(admin)
        win["t0"] = time.perf_counter()
        parts = [_run_subscribers(host, port, n, seconds)]
    else:
        def _mark_start() -> None:
            win["g0"] = gateway_stats(admin)
            win["t0"] = time.perf_counter()

        parts = _spawn_workers(host, port, n, seconds, procs,
                               start_at=time.time() + 3.0, on_start=_mark_start)
    g0 = win.get("g0", {})
    elapsed = time.perf_counter() - win["t0"]

    all_lat = [x for p in parts for x in p["latencies"]]
    per_sub = [m for p in parts for m in p["per_sub"]]
    total_msg = sum(per_sub)
    connected = per_sub
    failed = [p for p in parts if p["connect_failed"]]

    g = gateway_stats(admin)
    return {
        "subscribers": n,
        "processes": procs,
        "connected": len(connected),
        "connect_failed": sum(p["connect_failed"] for p in parts),
        "connect_error_sample": next(
            (p["connect_error_sample"] for p in parts if p["connect_error_sample"]), None),
        "elapsed_s": round(elapsed, 2),
        "total_messages": total_msg,
        "msg_per_s_total": round(total_msg / elapsed) if elapsed else 0,
        "msg_per_s_per_sub": round(statistics.mean(per_sub) / seconds, 1) if per_sub else 0,
        "per_sub_min": min(per_sub) if per_sub else 0,
        "per_sub_max": max(per_sub) if per_sub else 0,
        "bytes_total": sum(p["bytes_total"] for p in parts),
        "latency_p50_us": round(pct(all_lat, 50), 1),
        "latency_p95_us": round(pct(all_lat, 95), 1),
        "latency_p99_us": round(pct(all_lat, 99), 1),
        "latency_max_us": round(max(all_lat), 1) if all_lat else 0,
        "gaps": sum(p["gaps"] for p in parts),
        "lost_messages": sum(p["lost"] for p in parts),
        "crc_errors": sum(p["crc_errors"] for p in parts),
        "gateway_dropped": g.get("total_dropped"),
        "gateway_subscribers": g.get("subscribers"),
        # 게이트웨이가 상류에서 받은 유량. 회차 비교의 전제 조건이다.
        "upstream_frames_in": g.get("frames_in"),
        "upstream_msg_per_s": round(
            (g.get("frames_in", 0) - g0.get("frames_in", 0)) / elapsed, 1)
        if elapsed and g.get("frames_in") is not None
           and g0.get("frames_in") is not None else None,
        "upstream_symbols": g.get("cached_symbols"),
        # 서버가 실제로 밀렸는지. 이 둘이 0 인데 지연이 크면 그 지연은
        # 서버 것이 아니다 — 측정 도구나 커널을 봐야 한다.
        "gateway_max_backlog": g.get("max_backlog"),
        "gateway_max_wire_bytes": g.get("max_wire_bytes"),
    }


def main() -> int:
    ap = argparse.ArgumentParser("load_test")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9101)
    ap.add_argument("--admin", type=int, default=9111)
    ap.add_argument("--subscribers", type=int, nargs="+", default=[1, 10, 50, 100, 200])
    ap.add_argument("--seconds", type=float, default=12.0)
    ap.add_argument("--out", default="")
    ap.add_argument("--processes", type=int, default=1,
                    help="구독자를 나눠 붙일 프로세스 수. 1이면 예전과 같다. "
                         "100명 이상이면 한 프로세스로는 측정 도구가 먼저 병목이 된다")
    # 아래 셋은 워커 프로세스 내부용이다
    ap.add_argument("--worker", default="", help=argparse.SUPPRESS)
    ap.add_argument("--worker-count", type=int, default=0, help=argparse.SUPPRESS)
    ap.add_argument("--start-at", type=float, default=0.0, help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.worker:                       # 워커 모드: 원자료만 파일로 내고 끝
        r = _run_subscribers(args.host, args.port, args.worker_count,
                             args.seconds, args.start_at or None)
        with open(args.worker, "w", encoding="utf-8") as fh:
            json.dump(r, fh)
        return 0

    print(f"대상 {args.host}:{args.port} · 회차당 {args.seconds:.0f}초 "
          f"· 클라이언트 프로세스 {args.processes}개\n")
    print(f"{'구독자':>6} {'접속':>6} {'상류/s':>8} {'총 msg/s':>10} {'1인당 msg/s':>12} "
          f"{'p50':>9} {'p99':>10} {'max':>10} {'유실':>7} {'드롭':>8}")
    print("-" * 98)

    rounds = []
    for n in args.subscribers:
        r = run_round(args.host, args.port, n, args.seconds, args.admin,
                      processes=args.processes)
        rounds.append(r)
        print(f"{r['subscribers']:>6} {r['connected']:>6} "
              f"{str(r['upstream_msg_per_s'] or '-'):>8} "
              f"{r['msg_per_s_total']:>10,} {r['msg_per_s_per_sub']:>12,.1f} "
              f"{r['latency_p50_us']:>8,.0f}µ {r['latency_p99_us']:>9,.0f}µ "
              f"{r['latency_max_us']:>9,.0f}µ {r['lost_messages']:>7,} "
              f"{str(r['gateway_dropped']):>8}")
        time.sleep(3)              # 게이트웨이가 연결을 정리할 시간

    # 팬아웃이 선형인지 — 구독자마다 상류를 **온전히** 받으면 선형이다.
    #
    # 예전엔 1회차의 1인당 처리량을 기준으로 나눴다. 그런데 상류 유량이
    # 회차마다 다르면 그 기준이 움직인다. 실측에서 25명 회차가 68.3% 로
    # 찍혔는데, 그 회차의 상류가 327건/s 이고 1인당 321건/s 였다 —
    # 실제로는 98% 를 받고 있었다. **기준을 그 회차의 상류로 잡아야 한다.**
    for r in rounds:
        up = r.get("upstream_msg_per_s")
        r["throughput_retained_pct"] = (
            round(r["msg_per_s_per_sub"] / up * 100, 1) if up else None)

    print("-" * 96)
    print(f"{'구독자':>6} {'상류 대비 수신율':>20} {'p99 배율':>11} "
          f"{'서버 큐':>9} {'전송버퍼':>11}   판정")
    p99base = rounds[0]["latency_p99_us"] or 1
    for r in rounds:
        keep = r["throughput_retained_pct"]
        bl = r.get("gateway_max_backlog")
        wb = r.get("gateway_max_wire_bytes")
        # 서버가 안 밀렸는데 지연만 크면 그 지연은 서버 것이 아니다.
        idle_server = (bl in (0, None)) and (wb in (0, None))
        slow = r["latency_p99_us"] > 100_000
        verdict = ("클라이언트 의심 — 프로세스를 늘려 재볼 것"
                   if idle_server and slow and r["processes"] * 40 < r["subscribers"]
                   else "클라이언트 의심 — 서버 큐가 비었다" if idle_server and slow
                   else "")
        print(f"{r['subscribers']:>6} "
              f"{(f'{keep:.1f}%' if keep is not None else '-'):>20} "
              f"{r['latency_p99_us'] / p99base:>10.1f}x "
              f"{str(bl if bl is not None else '-'):>9} "
              f"{(f'{wb:,}B' if wb is not None else '-'):>11}   {verdict}")

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
