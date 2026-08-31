"""느린 구독자에게 stream 과 conflate 중 무엇이 더 신선한 값을 주는가.

왜 재나
-------
컨플레이션은 "밀린 구독자에게 낡은 값 대신 지금 값을 준다"는 **주장**이다.
주장은 재기 전까지 주장이다. 오늘 배치 전송이 나아 보였다가 조건을 고정하니
반대였던 일이 있었다 — 그래서 여기서도 **같은 상류를 같은 시각에** 두 구독자에게
동시에 흘려 조건을 맞춘다.

측정 방법
---------
같은 게이트웨이에 구독자 둘을 **동시에** 붙인다. 하나는 stream, 하나는 conflate.
둘 다 같은 속도로 **일부러 느리게** 읽어 밀림을 만든다.

지표는 **배달 시점의 신선도**다.

    신선도 = 클라이언트가 받은 시각 − 수집기가 그 틱을 받은 시각

stream 은 밀리면 큐에 쌓인 순서대로 나가므로 이 값이 계속 커진다.
conflate 는 큐에 있는 동안 새 값으로 덮이므로 값이 묶인다.
**둘의 상류가 같고 읽는 속도가 같으므로 비교가 성립한다.**

    python bench/conflation_bench.py --seconds 30 --read-rate 60
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

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from mdfeed.models import MSG_SUBSCRIBE, MSG_TRADE, Trade, now_ns   # noqa: E402
from mdfeed.protocol import FLAG_SNAPSHOT, FrameParser, encode      # noqa: E402


class SlowSubscriber(threading.Thread):
    """정해진 속도로만 읽어 일부러 밀리게 만든다."""

    def __init__(self, host: str, port: int, mode: str, seconds: float,
                 read_rate: float, start_at: float):
        super().__init__(daemon=True)
        self.host, self.port, self.mode = host, port, mode
        self.seconds, self.read_rate, self.start_at = seconds, read_rate, start_at
        self.staleness_ms: list[float] = []
        self.messages = 0
        self.symbols_seen: set[str] = set()
        self.error: str | None = None

    def run(self) -> None:
        try:
            sock = socket.create_connection((self.host, self.port), timeout=10)
        except OSError as e:
            self.error = str(e)
            return
        sock.sendall(encode(MSG_SUBSCRIBE, 0,
                            json.dumps({"mode": self.mode}).encode()))
        parser = FrameParser()
        delay = 1.0 / self.read_rate if self.read_rate > 0 else 0.0

        while time.time() < self.start_at:
            time.sleep(0.005)
        deadline = time.time() + self.seconds
        sock.settimeout(1.0)
        try:
            while time.time() < deadline:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                for f in parser.feed(chunk):
                    if f.msg_type != MSG_TRADE or (f.flags & FLAG_SNAPSHOT):
                        continue
                    t = Trade.unpack(f.payload)
                    # 수집기가 받은 시각 → 지금. 이게 소비자가 보는 낡음이다.
                    self.staleness_ms.append((now_ns() - t.ts_recv_ns) / 1e6)
                    self.symbols_seen.add(t.symbol)
                    self.messages += 1
                    if delay:
                        time.sleep(delay)       # 일부러 느리게 읽는다
        finally:
            with_close = getattr(sock, "close", None)
            if with_close:
                with_close()


def pct(xs, q):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(int(len(xs) * q / 100.0), len(xs) - 1)]


def main() -> int:
    ap = argparse.ArgumentParser("conflation_bench")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9101)
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--read-rate", type=float, default=60.0,
                    help="구독자가 초당 읽는 메시지 수. 상류보다 낮아야 밀린다")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    start_at = time.time() + 2.0
    subs = [SlowSubscriber(args.host, args.port, m, args.seconds,
                           args.read_rate, start_at)
            for m in ("stream", "conflate")]
    for s in subs:
        s.start()
    for s in subs:
        s.join(timeout=args.seconds + 30)

    print(f"같은 게이트웨이 · 같은 시각 · 초당 {args.read_rate:.0f}건만 읽음 "
          f"· {args.seconds:.0f}초\n")
    print(f"{'모드':>10} {'수신':>8} {'종목':>6} "
          f"{'신선도 p50':>12} {'p99':>12} {'최악':>12}")
    print("-" * 66)
    rows = []
    for s in subs:
        if s.error:
            print(f"{s.mode:>10}  접속 실패: {s.error}")
            continue
        row = {
            "mode": s.mode, "messages": s.messages,
            "symbols": len(s.symbols_seen),
            "staleness_p50_ms": round(pct(s.staleness_ms, 50), 1),
            "staleness_p99_ms": round(pct(s.staleness_ms, 99), 1),
            "staleness_max_ms": round(max(s.staleness_ms), 1) if s.staleness_ms else 0,
        }
        rows.append(row)
        print(f"{s.mode:>10} {s.messages:>8,} {len(s.symbols_seen):>6} "
              f"{row['staleness_p50_ms']:>11,.1f}ms {row['staleness_p99_ms']:>11,.1f}ms "
              f"{row['staleness_max_ms']:>11,.1f}ms")

    if len(rows) == 2:
        a, b = rows[0], rows[1]        # stream, conflate
        print()
        if a["staleness_p99_ms"] and b["staleness_p99_ms"]:
            ratio = a["staleness_p99_ms"] / b["staleness_p99_ms"]
            print(f"conflate 가 p99 신선도에서 {ratio:.1f}배 "
                  f"{'낫다' if ratio > 1 else '나쁘다'}")
        print(f"메시지 수: stream {a['messages']:,} · conflate {b['messages']:,} "
              f"— conflate 는 적게 받는 게 정상이다(합쳐진 것이지 잃은 게 아니다)")
        print(f"본 종목 수: stream {a['symbols']} · conflate {b['symbols']} "
              f"— 같은 시간에 더 많은 종목을 보면 화면이 덜 비어 있다는 뜻이다")

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                       "read_rate": args.read_rate, "seconds": args.seconds,
                       "rows": rows}, fh, ensure_ascii=False, indent=1)
        print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
