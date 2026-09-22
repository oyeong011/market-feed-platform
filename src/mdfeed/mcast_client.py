"""UDP 멀티캐스트 피드 참조 구독자 — 갭을 잡고 복구 채널로 메운다.

    증분: UDP 멀티캐스트(또는 유니캐스트) 데이터그램. 한 데이터그램에 MDFP 프레임 여러 개.
    복구: TCP. MSG_SUBSCRIBE → 스냅샷 + MSG_SNAPSHOT{next_seq}.
          MSG_RETRANS{from,to} → 프레임 재전송 + MSG_ACK{sent, unavailable, oldest_available}.

UDP 는 유실·순서 뒤바뀜이 정상이다. 그래서 수신자가 할 일이 세 가지다.
  1. 그룹에 먼저 가입하고(놓치지 않게) 스냅샷을 받은 뒤, next_seq 이전 증분은 버린다.
  2. seq 가 기대값보다 크면 그 사이를 복구 채널에 요청하고, 도착한 것은 순서가 맞을 때까지 보관한다.
  3. 버퍼 밖으로 밀린 구간(unavailable)은 복구할 수 없다 — 세고, 기대값을 건너뛴다.
     (실제 피드라면 여기서 스냅샷을 다시 받는다. 참조 구현은 사실을 기록하는 데까지만 한다.)

    python -m mdfeed.mcast_client --group 239.192.0.1 --port 9130 --recovery 9131 --duration 10
"""
from __future__ import annotations

import argparse
import json
import select
import socket
import struct
import time
from dataclasses import dataclass, field

from .models import MSG_ACK, MSG_HEARTBEAT, MSG_RETRANS, MSG_SNAPSHOT, MSG_SUBSCRIBE, MSG_TRADE, Trade, now_ns
from .protocol import FLAG_SNAPSHOT, Frame, FrameParser, encode

RETRANS_TIMEOUT_S = 1.0
MAX_PENDING = 200_000


@dataclass
class Stats:
    delivered: int = 0
    trades: int = 0
    heartbeats: int = 0
    snapshot_frames: int = 0
    datagrams: int = 0
    bytes_in: int = 0
    duplicates: int = 0
    discarded_before_snapshot: int = 0
    gaps_detected: int = 0
    retrans_requests: int = 0
    retrans_frames: int = 0
    unrecoverable: int = 0
    reordered: int = 0
    max_pending: int = 0
    latencies_us: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        lat = sorted(self.latencies_us)
        pct = lambda q: (lat[min(int(len(lat) * q / 100), len(lat) - 1)] if lat else 0.0)  # noqa: E731
        d = {k: v for k, v in self.__dict__.items() if k != "latencies_us"}
        d.update({"latency_p50_us": round(pct(50), 1), "latency_p99_us": round(pct(99), 1),
                  "latency_max_us": round(lat[-1], 1) if lat else 0.0})
        return d


class McastSubscriber:
    def __init__(self, group: str, port: int, recovery_host: str, recovery_port: int,
                 iface: str = "", on_frame=None):
        self.group, self.port = group, port
        self.on_frame = on_frame
        self.stats = Stats()
        self.expected: int | None = None          # 다음에 배달해야 할 seq. 스냅샷 전엔 None
        self.pending: dict[int, Frame] = {}       # seq → 순서를 기다리는 프레임
        self.outstanding: dict[tuple[int, int], float] = {}   # 재전송 요청 (from,to) → 요청 시각
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        first = int(group.split(".")[0])
        self.multicast = 224 <= first <= 239
        if self.multicast:
            self.udp.bind(("", port))                 # 그룹에 먼저 가입한다 — 스냅샷보다 먼저
            mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton(iface or "0.0.0.0"))
            self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        else:
            self.udp.bind((group, port))              # 유니캐스트 시험 경로
        self.udp.setblocking(False)
        self.tcp = socket.create_connection((recovery_host, recovery_port), timeout=10)
        self.tcp.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.tcp.setblocking(False)
        self.tcp_parser = FrameParser()
        self.udp_parser = FrameParser()
        self.snapshot_meta: dict | None = None
        self.tcp.sendall(encode(MSG_SUBSCRIBE, 0, b"{}"))

    # ── 순서 복원 ──────────────────────────────────────────────────────
    def _deliver(self, f: Frame) -> None:
        self.stats.delivered += 1
        if f.msg_type == MSG_TRADE and len(f.payload) >= Trade.SIZE:
            self.stats.trades += 1
            self.stats.latencies_us.append((now_ns() - Trade.unpack(f.payload).ts_recv_ns) / 1000.0)
        elif f.msg_type == MSG_HEARTBEAT:
            self.stats.heartbeats += 1
        if self.on_frame:
            self.on_frame(f)

    def _drain_pending(self) -> None:
        while self.expected in self.pending:
            self._deliver(self.pending.pop(self.expected))
            self.expected += 1

    def _observe(self, f: Frame, via_retrans: bool) -> None:
        if self.expected is None:                     # 스냅샷 전: 버퍼링만
            self.pending[f.seq] = f
            if len(self.pending) > MAX_PENDING:
                self.pending.pop(min(self.pending))
            return
        if f.seq < self.expected:
            self.stats.duplicates += 1
            return
        if f.seq == self.expected:
            self._deliver(f)
            self.expected += 1
            self._drain_pending()
            return
        # 갭: expected .. f.seq-1 가 비었다
        if f.seq not in self.pending:
            self.pending[f.seq] = f
            self.stats.max_pending = max(self.stats.max_pending, len(self.pending))
            if not via_retrans:
                self.stats.reordered += 1
        self._request_missing(f.seq)

    def _request_missing(self, upto_exclusive: int) -> None:
        lo = self.expected
        while lo < upto_exclusive:
            if lo in self.pending:
                lo += 1
                continue
            hi = lo
            while hi + 1 < upto_exclusive and (hi + 1) not in self.pending:
                hi += 1
            key = (lo, hi)
            now = time.time()
            if now - self.outstanding.get(key, 0) >= RETRANS_TIMEOUT_S:
                if key not in self.outstanding:
                    self.stats.gaps_detected += 1
                self.outstanding[key] = now
                self.stats.retrans_requests += 1
                self.tcp.sendall(encode(MSG_RETRANS, 0, struct.pack("!QQ", lo, hi)))
            lo = hi + 1

    # ── 소켓 처리 ──────────────────────────────────────────────────────
    def _on_tcp(self, chunk: bytes) -> None:
        for f in self.tcp_parser.feed(chunk):
            if f.flags & FLAG_SNAPSHOT:
                if f.msg_type == MSG_SNAPSHOT:
                    self.snapshot_meta = json.loads(f.payload)
                    self.expected = int(self.snapshot_meta["next_seq"])
                    stale = [s for s in self.pending if s < self.expected]
                    for s in stale:
                        self.pending.pop(s)
                    self.stats.discarded_before_snapshot += len(stale)
                    self._drain_pending()
                    if self.pending:
                        self._request_missing(min(self.pending))
                else:
                    self.stats.snapshot_frames += 1
                    if self.on_frame:
                        self.on_frame(f)
                continue
            if f.msg_type == MSG_ACK:
                ack = json.loads(f.payload)
                if ack.get("type") == "retrans":
                    self.outstanding.pop((ack["from"], ack["to"]), None)
                    self.stats.retrans_frames += int(ack.get("sent", 0))
                    unavailable = int(ack.get("unavailable", 0))
                    if unavailable and self.expected is not None and self.expected < ack["oldest_available"]:
                        # 복구 불가 구간: 사실을 세고 기대값을 건너뛴다
                        self.stats.unrecoverable += ack["oldest_available"] - self.expected
                        self.expected = ack["oldest_available"]
                        self._drain_pending()
                continue
            self._observe(f, via_retrans=True)

    def _on_udp(self, data: bytes) -> None:
        self.stats.datagrams += 1
        self.stats.bytes_in += len(data)
        for f in self.udp_parser.feed(data):      # 프레임은 데이터그램 경계를 넘지 않는다
            self._observe(f, via_retrans=False)

    def run(self, duration: float, stop_when=None) -> Stats:
        deadline = time.time() + duration
        while time.time() < deadline:
            if stop_when and stop_when(self):
                break
            r, _, _ = select.select([self.udp, self.tcp], [], [], 0.05)
            for s in r:
                if s is self.udp:
                    for _ in range(256):
                        try:
                            data = self.udp.recv(65536)
                        except BlockingIOError:
                            break
                        self._on_udp(data)
                else:
                    try:
                        chunk = self.tcp.recv(65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        raise ConnectionError("recovery channel closed")
                    self._on_tcp(chunk)
            # 미응답 재전송 요청은 다시 보낸다
            if self.expected is not None and self.pending:
                self._request_missing(max(self.pending))
        return self.stats

    def close(self) -> None:
        for s in (self.udp, self.tcp):
            try:
                s.close()
            except OSError:
                pass


def main() -> int:
    ap = argparse.ArgumentParser("mdfeed-mcast-client")
    ap.add_argument("--group", default="239.192.0.1")
    ap.add_argument("--port", type=int, default=9130)
    ap.add_argument("--recovery-host", default="127.0.0.1")
    ap.add_argument("--recovery", type=int, default=9131)
    ap.add_argument("--iface", default="")
    ap.add_argument("--duration", type=float, default=10.0)
    args = ap.parse_args()
    sub = McastSubscriber(args.group, args.port, args.recovery_host, args.recovery, args.iface)
    try:
        st = sub.run(args.duration)
    finally:
        sub.close()
    print(json.dumps(st.to_dict(), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
