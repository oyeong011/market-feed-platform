"""MDFP/1 참조 구독 클라이언트 — 제3자가 이 피드를 어떻게 쓰는지 보여주는 구현.

피드 서비스는 "우리가 보낼 수 있다"로 끝나지 않는다. 상대가 **갭을 탐지하고,
스냅샷과 증분을 구분하고, 끊기면 재접속**할 수 있어야 서비스다. 그 최소 예제를
여기 둔다. 고객사에 스펙 문서와 함께 건네는 샘플 코드에 해당한다.

    python -m mdfeed client --symbols UPBIT:KRW-BTC BINANCE:BTCUSDT --duration 30
"""

from __future__ import annotations

import json
import socket
import time

from .metrics import Histogram
from .models import (MSG_BOOK, MSG_HEARTBEAT, MSG_NAMES, MSG_SNAPSHOT,
                     MSG_SUBSCRIBE, MSG_TRADE, BookTop, Trade, now_ns)
from .protocol import FLAG_SNAPSHOT, FrameParser, SequenceTracker, encode


def run_client(host: str, port: int, symbols=None, duration: float = 0,
               quiet: bool = False) -> int:
    sock = socket.create_connection((host, port), timeout=10)
    # 시세 클라이언트는 지연이 생명이다. 작은 프레임이 뭉쳐 나가지 않게 한다
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    print(f"[client] {host}:{port} 접속")

    if symbols:
        sock.sendall(encode(MSG_SUBSCRIBE, 0,
                            json.dumps({"symbols": list(symbols)}).encode()))
        print(f"[client] 구독 요청: {', '.join(symbols)}")

    parser = FrameParser()
    seqtrack = SequenceTracker()
    hist = Histogram("client_latency")
    counts = {"trade": 0, "book": 0, "heartbeat": 0, "snapshot": 0, "other": 0}
    snapshot_done = False
    started = time.time()
    last_report = started
    bytes_in = 0

    try:
        while True:
            if duration and time.time() - started >= duration:
                break
            sock.settimeout(1.0)
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                print("[client] 서버가 연결을 닫음")
                break
            bytes_in += len(chunk)

            for f in parser.feed(chunk):
                is_snap = bool(f.flags & FLAG_SNAPSHOT)
                if not is_snap:
                    lost = seqtrack.observe(f.seq)
                    if lost:
                        print(f"[client] !! 시퀀스 갭 {lost}건 (seq={f.seq}) "
                              f"— 스냅샷 재요청이 필요한 상황")

                if f.msg_type == MSG_TRADE:
                    t = Trade.unpack(f.payload)
                    counts["snapshot" if is_snap else "trade"] += 1
                    if not is_snap:
                        # 게이트웨이를 거쳐 도착하기까지의 총 지연
                        hist.record((now_ns() - t.ts_recv_ns) / 1000.0)
                        if not quiet:
                            print(f"  TRADE {t.venue:<8} {t.symbol:<10} "
                                  f"{t.price:>14,.2f} × {t.qty:<12.6f} seq={f.seq}")
                elif f.msg_type == MSG_BOOK:
                    b = BookTop.unpack(f.payload)
                    counts["snapshot" if is_snap else "book"] += 1
                    if not is_snap and not quiet:
                        print(f"  BOOK  {b.venue:<8} {b.symbol:<10} "
                              f"{b.bid:>14,.2f} / {b.ask:<14,.2f} "
                              f"spread={b.spread_bp:.2f}bp")
                elif f.msg_type == MSG_SNAPSHOT:
                    meta = json.loads(f.payload)
                    snapshot_done = True
                    print(f"[client] 스냅샷 수신 완료: {meta['snapshot_count']}건 "
                          f"→ 이제부터 증분")
                elif f.msg_type == MSG_HEARTBEAT:
                    counts["heartbeat"] += 1
                else:
                    counts["other"] += 1

            now = time.time()
            if quiet and now - last_report >= 5.0:
                _report(counts, hist, seqtrack, now - started, bytes_in, parser)
                last_report = now
    except KeyboardInterrupt:
        print("\n[client] 사용자 중단")
    finally:
        sock.close()

    print("\n" + "=" * 66)
    print("구독 세션 요약")
    print("=" * 66)
    _report(counts, hist, seqtrack, time.time() - started, bytes_in, parser)
    return 0


def _report(counts, hist, seqtrack, elapsed, bytes_in, parser) -> None:
    total = counts["trade"] + counts["book"]
    h = hist.snapshot()
    print(f"  경과 {elapsed:.1f}s | 체결 {counts['trade']:,} · 호가 {counts['book']:,} "
          f"· 하트비트 {counts['heartbeat']} · 스냅샷 {counts['snapshot']}")
    print(f"  처리량 {total / elapsed if elapsed else 0:,.1f} msg/s "
          f"| 수신 {bytes_in / 1024:,.1f} KB "
          f"({bytes_in / elapsed / 1024 if elapsed else 0:,.1f} KB/s)")
    if h["count"]:
        print(f"  게이트웨이→클라이언트 지연  p50 {h['p50_us']:,.0f}µs  "
              f"p99 {h['p99_us']:,.0f}µs  max {h['max_us']:,.0f}µs")
    s = seqtrack.stats()
    print(f"  시퀀스: 갭 {s['gap_count']}회 / 유실 {s['lost_messages']}건 "
          f"/ 중복 {s['duplicate_count']}건 "
          f"| 프레이밍: CRC오류 {parser.crc_error_count} 재동기화 {parser.resync_count}")
    integrity = (1 - s["lost_messages"] / max(total + s["lost_messages"], 1)) * 100
    print(f"  데이터 무결성: {integrity:.4f}%")
