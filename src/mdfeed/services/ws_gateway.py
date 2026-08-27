"""ws-gateway — 브라우저용 WebSocket 배포 + 대시보드 서빙.

    UDS 버스 ─▶ JSON 변환 ─▶ 구독자별 유한 큐 ─▶ WebSocket 프레임

바이너리 게이트웨이와 분리한 이유
---------------------------------
브라우저는 MDFP 바이너리를 파싱하기 번거롭고, 반대로 기관 구독자에게 JSON 을
주면 대역폭이 3~4배로 뛴다. 같은 버스를 두 게이트웨이가 각자의 표현으로
내보내는 구조가 정답이다. 하나가 죽어도 다른 하나는 산다.

브라우저 특화로 추가한 것
-------------------------
* **틱 병합(coalescing)**: 초당 수백 틱을 그대로 밀면 브라우저 렌더링이 못 따라가
  탭이 멈춘다. 심볼당 최신값만 남겨 100ms 주기로 묶어 보낸다. 사람 눈은 초당
  10프레임 이상을 구분하지 못하므로 정보 손실이 사실상 없다.
  (체결 로그가 필요한 구독자는 mode=raw 로 원본 스트림을 받는다)
* **동일 포트에서 HTTP + WS**: 대시보드가 정적 파일·API·소켓을 한 오리진에서
  받으므로 CORS·프록시 설정이 필요 없다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time

from ..bus import UDSSubscriber
from ..clock import ClockMonitor
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import (MSG_BOOK, MSG_HEARTBEAT, MSG_SIGNAL, MSG_TRADE,
                      BookTop, Signal, Trade)
from ..wsproto import (FrameDecoder, OP_CLOSE, OP_PING, OP_PONG, OP_TEXT,
                       WSError, handshake_response, server_frame)

log = logging.getLogger("mdfeed.ws_gateway")
SERVICE = "ws-gateway"

COALESCE_MS = 100


class WSClientConn:
    __slots__ = ("id", "writer", "queue", "mode", "symbols", "sent", "dropped",
                 "connected_at", "peer")

    def __init__(self, cid, writer, queue_size, peer):
        self.id = cid
        self.writer = writer
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.mode = "coalesced"          # coalesced | raw
        self.symbols: set[str] | None = None
        self.sent = 0
        self.dropped = 0
        self.connected_at = time.time()
        self.peer = peer

    def info(self):
        return {"id": self.id, "peer": self.peer, "mode": self.mode,
                "symbols": sorted(self.symbols) if self.symbols else "ALL",
                "sent": self.sent, "dropped": self.dropped,
                "uptime_s": round(time.time() - self.connected_at, 1)}


class WSGateway:
    def __init__(self, cfg, static_dir: str | None = None):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "dropped_total", "connections_total", "coalesced_batches_total")
        self.clients: dict[int, WSClientConn] = {}
        self._next_id = 0
        self.snapshot: dict[str, dict] = {}
        self._dirty: set[str] = set()
        self.frames_in = 0
        self.last_frame_at = 0.0
        self.upstream_ok = False
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()
        self.static_dir = static_dir
        self.clock = ClockMonitor()

    # ── 업스트림 ──────────────────────────────────────────────────────────
    async def _consume(self, stop: asyncio.Event) -> None:
        paths = list(self.cfg.bus_paths or [self.cfg.bus_path]) + [self.cfg.signal_bus_path]
        for path in paths:
            asyncio.create_task(self._consume_one(path, stop))
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()

    async def _consume_one(self, path: str, stop: asyncio.Event) -> None:
        sub = UDSSubscriber(path)
        try:
            async for frame in sub.frames():
                if stop.is_set():
                    return
                self.frames_in += 1
                self.last_frame_at = time.time()
                self.upstream_ok = True
                self._ingest(frame)
        except asyncio.CancelledError:
            return

    def _ingest(self, frame) -> None:
        mt = frame.msg_type
        if mt == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
            t = Trade.unpack(frame.payload)
            key = f"{t.venue}:{t.symbol}"
            cur = self.snapshot.setdefault(key, {"venue": t.venue, "symbol": t.symbol})
            prev = cur.get("last")
            cur.update(last=t.price, qty=t.qty, side=t.side, ts_ns=t.ts_event_ns,
                       latency_us=round(self.clock.observe(t.venue, t.latency_us), 1))
            cur["trades"] = cur.get("trades", 0) + 1
            cur["volume"] = round(cur.get("volume", 0.0) + t.qty, 8)
            if prev:
                cur["tick_dir"] = 1 if t.price > prev else (-1 if t.price < prev else 0)
            self._dirty.add(key)
            if any(c.mode == "raw" for c in self.clients.values()):
                self._push_raw({"type": "trade", **t.to_dict()}, key)
        elif mt == MSG_BOOK and len(frame.payload) >= BookTop.SIZE:
            b = BookTop.unpack(frame.payload)
            key = f"{b.venue}:{b.symbol}"
            cur = self.snapshot.setdefault(key, {"venue": b.venue, "symbol": b.symbol})
            cur.update(bid=b.bid, ask=b.ask, mid=b.mid,
                       spread_bp=round(b.spread_bp, 3))
            self._dirty.add(key)
        elif mt == MSG_SIGNAL and len(frame.payload) >= Signal.SIZE:
            sig = Signal.unpack(frame.payload)
            # 시그널은 병합하지 않는다. 하나하나가 사건이다
            self._push_raw({"type": "signal", **sig.to_dict()},
                           f"{sig.venue}:{sig.symbol}")
        elif mt == MSG_HEARTBEAT:
            self._push_raw({"type": "heartbeat", "seq": frame.seq,
                            "ts": time.time()}, None)

    # ── 다운스트림 ────────────────────────────────────────────────────────
    def _push_raw(self, obj: dict, key: str | None) -> None:
        data = server_frame(OP_TEXT, json.dumps(obj, ensure_ascii=False).encode())
        for c in list(self.clients.values()):
            if obj.get("type") == "trade" and c.mode != "raw":
                continue
            if key and c.symbols and key not in c.symbols:
                continue
            self._enqueue(c, data)

    def _enqueue(self, c: WSClientConn, data: bytes) -> None:
        if c.queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                c.queue.get_nowait()
            c.dropped += 1
            self.registry.counter("dropped_total")
        with contextlib.suppress(asyncio.QueueFull):
            c.queue.put_nowait(data)

    async def _coalesce_loop(self, stop: asyncio.Event) -> None:
        """변경된 심볼만 모아 100ms 주기로 한 번에 보낸다."""
        while not stop.is_set():
            await asyncio.sleep(COALESCE_MS / 1000.0)
            if not self._dirty or not self.clients:
                self._dirty.clear()
                continue
            keys = list(self._dirty)
            self._dirty.clear()
            for c in list(self.clients.values()):
                if c.mode == "raw":
                    continue
                items = [self.snapshot[k] for k in keys
                         if k in self.snapshot and (not c.symbols or k in c.symbols)]
                if not items:
                    continue
                msg = json.dumps({"type": "tick", "ts": time.time(), "items": items},
                                 ensure_ascii=False).encode()
                self._enqueue(c, server_frame(OP_TEXT, msg))
            self.registry.counter("coalesced_batches_total")

    async def _ws_handler(self, reader, writer, req) -> None:
        cid = self._next_id
        self._next_id += 1
        peer = writer.get_extra_info("peername")
        peer = f"{peer[0]}:{peer[1]}" if isinstance(peer, tuple) else str(peer)
        try:
            writer.write(handshake_response(req.headers))
            await writer.drain()
        except WSError as e:
            writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            log.warning("WS 핸드셰이크 실패 %s: %s", peer, e)
            return

        c = WSClientConn(cid, writer, self.cfg.client_queue_size, peer)
        if req.query.get("mode") == "raw":
            c.mode = "raw"
        if req.query.get("symbols"):
            c.symbols = set(req.query["symbols"].split(","))
        self.clients[cid] = c
        self.registry.counter("connections_total")
        log.info("WS #%d 접속 (%s, mode=%s). 현재 %d명", cid, peer, c.mode, len(self.clients))

        # 접속 즉시 스냅샷
        self._enqueue(c, server_frame(OP_TEXT, json.dumps(
            {"type": "snapshot", "ts": time.time(),
             "items": list(self.snapshot.values())}, ensure_ascii=False).encode()))

        sender = asyncio.create_task(self._ws_send(c))
        dec = FrameDecoder(expect_masked=True)   # 클라이언트→서버는 반드시 마스킹
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                for op, payload in dec.feed(chunk):
                    if op == OP_PING:
                        writer.write(server_frame(OP_PONG, payload))
                        await writer.drain()
                    elif op == OP_CLOSE:
                        raise ConnectionResetError("client close")
                    elif op == OP_TEXT:
                        self._on_command(c, payload)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError, WSError):
            pass
        except Exception as e:                    # noqa: BLE001
            log.warning("WS #%d 오류: %s", cid, e)
        finally:
            sender.cancel()
            with contextlib.suppress(Exception):
                await sender
            self.clients.pop(cid, None)
            with contextlib.suppress(Exception):
                writer.close()
            log.info("WS #%d 종료 (전송 %d, 드롭 %d). 남은 %d명",
                     cid, c.sent, c.dropped, len(self.clients))

    async def _ws_send(self, c: WSClientConn) -> None:
        try:
            while True:
                data = await c.queue.get()
                c.writer.write(data)
                await c.writer.drain()
                c.sent += 1
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass

    def _on_command(self, c: WSClientConn, payload: bytes) -> None:
        try:
            cmd = json.loads(payload)
        except json.JSONDecodeError:
            return
        if cmd.get("op") == "subscribe":
            syms = cmd.get("symbols")
            c.symbols = set(syms) if syms else None
        elif cmd.get("op") == "mode":
            c.mode = "raw" if cmd.get("value") == "raw" else "coalesced"

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        return {
            "service": SERVICE,
            "healthy": self.upstream_ok and (age is None or age < 30.0),
            "uptime_s": round(time.time() - self._started, 1),
            "upstream_connected": self.upstream_ok,
            "last_frame_age_s": round(age, 1) if age is not None else None,
            "frames_in": self.frames_in,
            "ws_clients": len(self.clients),
            "symbols": len(self.snapshot),
            "clock": self.clock.report(),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        http = HTTPServer(cfg.ws_host, cfg.ws_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.websocket("/ws", self._ws_handler)
        http.route("GET", "/api/snapshot", lambda r: Response.json(
            {"ts": time.time(), "items": sorted(self.snapshot.values(),
                                                key=lambda x: (x["venue"], x["symbol"]))}))
        http.route("GET", "/api/clients", lambda r: Response.json(
            {"count": len(self.clients), "items": [c.info() for c in self.clients.values()]}))
        # static 마운트는 health_routes 뒤에 와야 한다 — 루트 라우트를 걷어내기 때문
        if self.static_dir and os.path.isdir(self.static_dir):
            http.static("/", self.static_dir)
            log.info("대시보드 서빙: %s → http://localhost:%d/", self.static_dir, cfg.ws_port)
        await http.start()

        tasks = [asyncio.create_task(self._consume(stop)),
                 asyncio.create_task(self._coalesce_loop(stop)),
                 asyncio.create_task(self._gauges(stop))]
        await stop.wait()
        for c in list(self.clients.values()):
            with contextlib.suppress(Exception):
                c.writer.write(server_frame(OP_CLOSE, b"\x03\xe8"))
                c.writer.close()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await http.close()
        log.info("종료. 수신 %d 프레임", self.frames_in)

    async def _gauges(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1.0)
            self.registry.gauge("ws_clients", len(self.clients))
            self.registry.gauge("symbols_tracked", len(self.snapshot))


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    static = os.getenv("MDFEED_DASHBOARD_DIR", "docs")
    return runtime.run(SERVICE, WSGateway(cfg, static).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
