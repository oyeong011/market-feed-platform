"""tcp-gateway — 외부 구독자에게 MDFP/1 바이너리 피드를 배포하는 프로세스.

    UDS 버스 ─▶ [구독 필터] ─▶ 클라이언트별 유한 큐 ─▶ TCP 소켓

상용 마켓데이터 배포단이 반드시 갖춰야 하는 것들을 그대로 구현했다.

1. **스냅샷 + 증분**
   접속 즉시 최신값 스냅샷을 보내고 그 뒤 증분을 잇는다. 안 그러면 새 구독자는
   다음 체결이 날 때까지 빈 화면을 본다(거래 없는 종목은 몇 분씩 걸린다).

2. **구독 필터**
   클라이언트가 MSG_SUBSCRIBE 로 원하는 심볼만 고른다. 전 종목을 다 밀면
   대역폭이 구독자 수 × 전체 틱으로 곱해진다.

3. **구독자별 시퀀스 번호**
   구독자마다 심볼 필터가 다르므로, 발행자의 전역 seq 를 그대로 흘리면 필터로
   걸러진 번호가 구독자에겐 전부 "유실"로 보인다. 참조 클라이언트로 실측했더니
   데이터 무결성이 48% 로 찍혔다 — 실제로는 한 건도 잃지 않았는데도.
   그래서 게이트웨이가 **구독자별로 연속된 seq 를 다시 매겨** 내보낸다.
   원본 번호는 필요할 때 추적할 수 있도록 로그·스냅샷 메타에 남긴다.

   비용: 구독자 수만큼 프레임을 다시 인코딩한다(CRC 가 본문 전체를 덮으므로
   헤더만 갈아끼울 수 없다). 구독자가 수백 명 규모가 되면 심볼 그룹을 고정
   채널로 묶고 채널 단위 seq 를 쓰는 방식(거래소 멀티캐스트 피드의 표준)으로
   바꿔야 한다. 현재 규모에서는 측정된 오버헤드가 무시할 수준이라 단순한 쪽을 택했다.

4. **느린 구독자 격리 (백프레셔)**
   구독자마다 유한 큐를 두고, 차면 오래된 것부터 버린 뒤 카운트한다.
   `await writer.drain()` 을 발행 경로에서 직접 기다리면 구독자 하나가 느려질 때
   전체 배포가 멈춘다 — head-of-line blocking. 이걸 막는 게 이 프로세스의 존재 이유다.
   드롭이 임계치를 넘으면 그 구독자만 끊는다(연결을 유지한 채 조용히 틀린 데이터를
   주는 것보다, 끊어서 재동기화하게 하는 편이 안전하다).

5. **하트비트 전달**
   무거래 구간에도 seq 를 진행시켜 구독자가 갭과 정지를 구분할 수 있게 한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time

from ..bus import UDSSubscriber
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import (MSG_BOOK, MSG_HEARTBEAT, MSG_SNAPSHOT, MSG_SUBSCRIBE,
                      MSG_TRADE, BookTop, Trade, now_ns)
from ..protocol import FLAG_SNAPSHOT, FrameParser, encode

log = logging.getLogger("mdfeed.tcp_gateway")
SERVICE = "tcp-gateway"

DROP_LIMIT = 5000          # 이만큼 버려진 구독자는 재동기화가 필요하다고 보고 끊는다


class Subscriber:
    __slots__ = ("id", "writer", "queue", "symbols", "dropped", "sent",
                 "connected_at", "peer", "out_seq")

    def __init__(self, cid: int, writer, queue_size: int, peer: str):
        self.id = cid
        self.writer = writer
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.symbols: set[str] | None = None      # None = 전체 구독
        self.dropped = 0
        self.sent = 0
        self.connected_at = time.time()
        self.peer = peer
        self.out_seq = 0            # 이 구독자에게 나가는 프레임의 연속 번호

    def wants(self, key: str) -> bool:
        return self.symbols is None or key in self.symbols

    def info(self) -> dict:
        return {
            "id": self.id, "peer": self.peer,
            "symbols": sorted(self.symbols) if self.symbols else "ALL",
            "sent": self.sent, "dropped": self.dropped,
            "out_seq": self.out_seq, "backlog": self.queue.qsize(),
            "uptime_s": round(time.time() - self.connected_at, 1),
        }


class TCPGateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.subs: dict[int, Subscriber] = {}
        self._next_id = 0
        self.last: dict[str, bytes] = {}          # "VENUE:SYMBOL" → 최신 프레임 페이로드
        self.last_type: dict[str, int] = {}
        self.frames_in = 0
        self.upstream_ok = False
        self.last_frame_at = 0.0
        self._started = time.time()

    # ── 업스트림(버스) ────────────────────────────────────────────────────
    async def _consume_bus(self, stop: asyncio.Event) -> None:
        """샤드가 여러 개면 전부 구독해 하나로 합친다.

        샤드마다 seq 공간이 독립이므로 여기서 합치면 순서가 뒤섞인다.
        어차피 구독자별로 seq 를 다시 매기므로(§구독자별 시퀀스) 문제가 없다.
        """
        paths = self.cfg.bus_paths or [self.cfg.bus_path]
        if len(paths) > 1:
            await asyncio.gather(*(self._consume_one(p, stop) for p in paths))
        else:
            await self._consume_one(paths[0], stop)

    async def _consume_one(self, path: str, stop: asyncio.Event) -> None:
        sub = UDSSubscriber(path)
        async for frame in sub.frames():
            if stop.is_set():
                return
            self.frames_in += 1
            self.last_frame_at = time.time()
            self.upstream_ok = True
            self.registry.counter("frames_in_total")

            key = _key_of(frame)
            if key:
                self.last[key] = frame.payload
                self.last_type[key] = frame.msg_type
            self._fanout(frame, key)

    def _fanout(self, frame, key: str | None) -> None:
        for s in list(self.subs.values()):
            if key is not None and not s.wants(key):
                continue
            # 구독자별로 seq 를 다시 매긴다. 그래야 구독자의 갭 탐지가 유효하다
            raw = encode(frame.msg_type, s.out_seq, frame.payload, frame.flags)
            s.out_seq += 1
            if s.queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    s.queue.get_nowait()
                s.dropped += 1
                self.registry.counter("dropped_total")
                if s.dropped > DROP_LIMIT:
                    # 되돌릴 수 없을 만큼 밀렸다. 끊어서 재접속·재동기화시킨다
                    log.warning("구독자 #%d 드롭 %d 초과 → 강제 종료", s.id, DROP_LIMIT)
                    with contextlib.suppress(Exception):
                        s.writer.close()
                    self.subs.pop(s.id, None)
                    continue
            with contextlib.suppress(asyncio.QueueFull):
                s.queue.put_nowait(raw)

    # ── 다운스트림(TCP 클라이언트) ────────────────────────────────────────
    async def _handle_client(self, reader, writer) -> None:
        cid = self._next_id
        self._next_id += 1
        peer = _peer(writer)
        s = Subscriber(cid, writer, self.cfg.client_queue_size, peer)
        self.subs[cid] = s
        log.info("구독자 #%d 접속 (%s). 현재 %d명", cid, peer, len(self.subs))
        self.registry.counter("connections_total")

        sender = asyncio.create_task(self._send_loop(s))
        try:
            await self._send_snapshot(s)
            parser = FrameParser()
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                for f in parser.feed(chunk):
                    if f.msg_type == MSG_SUBSCRIBE:
                        self._apply_subscribe(s, f.payload)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception as e:                    # noqa: BLE001
            log.warning("구독자 #%d 오류: %s", cid, e)
        finally:
            sender.cancel()
            with contextlib.suppress(Exception):
                await sender
            self.subs.pop(cid, None)
            with contextlib.suppress(Exception):
                writer.close()
            log.info("구독자 #%d 종료 (전송 %d, 드롭 %d). 남은 %d명",
                     cid, s.sent, s.dropped, len(self.subs))

    async def _send_loop(self, s: Subscriber) -> None:
        try:
            while True:
                data = await s.queue.get()
                s.writer.write(data)
                await s.writer.drain()            # 여기서만 기다린다. 발행 경로는 논블로킹
                s.sent += 1
                self.registry.counter("sent_total")
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass

    async def _send_snapshot(self, s: Subscriber) -> None:
        """접속 즉시 최신값 전체를 스냅샷 플래그와 함께 보낸다."""
        n = 0
        for key, payload in list(self.last.items()):
            if not s.wants(key):
                continue
            s.writer.write(encode(self.last_type.get(key, MSG_TRADE), 0, payload,
                                  flags=FLAG_SNAPSHOT))
            n += 1
        # 스냅샷 종료 메시지가 "증분 seq 는 여기서부터"를 알려준다.
        # 이 값이 없으면 구독자는 첫 증분 프레임을 갭으로 오인한다.
        meta = json.dumps({"snapshot_count": n, "ts_ns": now_ns(),
                           "next_seq": s.out_seq}).encode()
        s.writer.write(encode(MSG_SNAPSHOT, 0, meta, flags=FLAG_SNAPSHOT))
        await s.writer.drain()
        log.info("구독자 #%d 스냅샷 %d건 전송", s.id, n)

    def _apply_subscribe(self, s: Subscriber, payload: bytes) -> None:
        try:
            req = json.loads(payload)
        except json.JSONDecodeError:
            return
        syms = req.get("symbols")
        s.symbols = set(syms) if syms else None
        log.info("구독자 #%d 구독 변경 → %s", s.id, s.symbols or "ALL")

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        # 버스에서 프레임이 30초 넘게 안 오면 상류가 죽은 것이다.
        # feedd 가 하트비트를 보내므로 무거래여도 이 값은 갱신된다.
        healthy = self.upstream_ok and (age is None or age < 30.0)
        return {
            "service": SERVICE, "healthy": healthy,
            "uptime_s": round(time.time() - self._started, 1),
            "upstream_connected": self.upstream_ok,
            "last_frame_age_s": round(age, 1) if age is not None else None,
            "frames_in": self.frames_in,
            "subscribers": len(self.subs),
            "cached_symbols": len(self.last),
            "total_dropped": sum(s.dropped for s in self.subs.values()),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        server = await asyncio.start_server(self._handle_client, cfg.tcp_host, cfg.tcp_port)
        log.info("MDFP/1 배포 서버 listening on %s:%d", cfg.tcp_host, cfg.tcp_port)

        http = HTTPServer(cfg.http_host, cfg.tcp_admin_port, SERVICE, self.registry)
        health_routes(http, self.health)
        http.route("GET", "/subscribers", lambda r: Response.json(
            {"count": len(self.subs), "items": [s.info() for s in self.subs.values()]}))
        await http.start()

        bus_task = asyncio.create_task(self._consume_bus(stop))
        gauge_task = asyncio.create_task(self._gauges(stop))
        await stop.wait()

        for t in (bus_task, gauge_task):
            t.cancel()
        await asyncio.gather(bus_task, gauge_task, return_exceptions=True)
        # 구독자 연결을 먼저 끊는다. wait_closed() 는 핸들러 종료를 기다리는데,
        # 핸들러는 reader.read() 에서 대기 중이라 스스로 끝나지 않는다.
        for sub in list(self.subs.values()):
            with contextlib.suppress(Exception):
                sub.writer.close()
        self.subs.clear()
        server.close()
        with contextlib.suppress(Exception, asyncio.TimeoutError):
            await asyncio.wait_for(server.wait_closed(), timeout=5.0)
        await http.close()
        log.info("종료. 수신 %d 프레임", self.frames_in)

    async def _gauges(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1.0)
            self.registry.gauge("subscribers", len(self.subs))
            self.registry.gauge("max_backlog",
                                max((s.queue.qsize() for s in self.subs.values()), default=0))


def _key_of(frame) -> str | None:
    if frame.msg_type == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
        t = Trade.unpack(frame.payload)
        return f"{t.venue}:{t.symbol}"
    if frame.msg_type == MSG_BOOK and len(frame.payload) >= BookTop.SIZE:
        b = BookTop.unpack(frame.payload)
        return f"{b.venue}:{b.symbol}"
    return None       # 하트비트 등 — 필터 없이 전원에게 보낸다


def _peer(writer) -> str:
    p = writer.get_extra_info("peername")
    return f"{p[0]}:{p[1]}" if isinstance(p, tuple) else str(p)


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    return runtime.run(SERVICE, TCPGateway(cfg).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
