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


MODE_STREAM = "stream"        # 모든 틱을 순서대로 (기본)
MODE_CONFLATE = "conflate"    # 종목별 최신값만


class Subscriber:
    """한 구독자의 배포 상태.

    배포 방식이 둘이다.

    * ``stream`` — 발행된 틱을 전부 순서대로 보낸다. 밀리면 오래된 것부터 버린다.
    * ``conflate`` — 종목별 **최신값만** 보낸다. 밀리는 동안 같은 종목이 여러 번
      바뀌면 마지막 값 하나로 합쳐진다.

    왜 둘인가
    ---------
    밀린 구독자에게 `stream` 이 하는 일은 **낡은 값을 순서대로 계속 보내는 것**이다.
    체결 이력을 재구성해야 하는 소비자(적재·정산)에겐 그게 맞다.
    그런데 화면에 호가를 그리거나 시세를 감시하는 소비자에게는
    3초 전 가격을 순서대로 받는 것보다 **지금 가격 하나**가 낫다.
    마켓데이터 벤더가 full-tick 과 conflated 피드를 나눠 파는 이유다.

    두 방식 모두 구독자별로 seq 를 다시 매기므로 갭 탐지는 그대로 유효하다.
    conflate 는 발행 수보다 적게 받는 게 정상이고, 그건 갭이 아니다 —
    합쳐진 것이지 잃은 게 아니다.
    """

    __slots__ = ("id", "writer", "queue", "symbols", "dropped", "sent",
                 "connected_at", "peer", "out_seq", "mode",
                 "pending", "conflated")

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
        self.mode = MODE_STREAM
        # conflate 전용: 키 → (msg_type, payload, flags). 큐에는 키만 넣는다.
        self.pending: dict[str, tuple] = {}
        self.conflated = 0          # 합쳐지며 생략된 프레임 수

    def wants(self, key: str) -> bool:
        return self.symbols is None or key in self.symbols

    def wire_bytes(self) -> int:
        """아직 소켓으로 못 나간 바이트. 우리 큐가 아니라 transport 안이다.

        큐 깊이만 보면 밀림을 놓친다. write() 는 소켓이 느려도 즉시 반환하고
        asyncio 가 내부 버퍼에 쌓아 두기 때문이다. drain() 도 상한(기본 64KB)
        아래에서는 그냥 통과한다. 실측: 구독자 100명 부하에서 큐 깊이는
        20초 내내 0 이었는데 클라이언트가 잰 p99 는 363ms 였다 —
        밀린 데이터가 전부 여기 있었다.
        """
        try:
            # 속성 접근 자체도 터질 수 있다 — 닫힌 연결의 transport 를 훑다가
            # 헬스 응답 전체가 죽으면 안 된다. getattr 을 try 밖에 두면
            # 그 경로가 안 덮인다.
            tr = getattr(self.writer, "transport", None)
            return tr.get_write_buffer_size() if tr is not None else 0
        except Exception:                         # noqa: BLE001
            return 0

    def info(self) -> dict:
        return {
            "id": self.id, "peer": self.peer,
            "symbols": sorted(self.symbols) if self.symbols else "ALL",
            "sent": self.sent, "dropped": self.dropped,
            "mode": self.mode,
            "out_seq": self.out_seq, "backlog": self.queue.qsize(),
            "conflated": self.conflated,
            "wire_bytes": self.wire_bytes(),
            "uptime_s": round(time.time() - self.connected_at, 1),
        }


class TCPGateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        # 사건이 나기 전에도 지표가 존재해야 알람이 평가된다
        self.registry.declare_counters(
            "dropped_total", "sent_total", "frames_in_total", "connections_total",
            "conflated_total")
        self.subs: dict[int, Subscriber] = {}
        self._next_id = 0
        self.last: dict[str, bytes] = {}          # "VENUE:SYMBOL" → 최신 프레임 페이로드
        self.last_type: dict[str, int] = {}
        self.frames_in = 0
        self.upstream_ok = False
        self.last_frame_at = 0.0
        from ..runtime import make_tracker
        self.tracker = make_tracker()
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
        sub = UDSSubscriber(path, name=SERVICE)
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

    def _drop_overflowed(self, s: Subscriber) -> bool:
        """넘친 구독자에서 오래된 것을 하나 버린다. 끊어야 하면 True."""
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
            return True
        return False

    def _fanout(self, frame, key: str | None) -> None:
        for s in list(self.subs.values()):
            if key is not None and not s.wants(key):
                continue

            if s.mode == MODE_CONFLATE and key is not None:
                # 같은 종목이 큐에 이미 있으면 **새 값으로 덮는다.** 큐에는 키만
                # 들어가므로 큐 길이는 종목 수를 넘지 않는다. 밀린 구독자에게
                # 낡은 값을 순서대로 보내는 대신 지금 값을 보낸다.
                #
                # seq 는 여기서 매기지 않고 보낼 때 매긴다. 지금 매기면 덮어쓴
                # 프레임의 번호가 비어 구독자가 갭으로 본다 — 합쳐진 것을
                # 잃은 것으로 오인하게 된다.
                if key in s.pending:
                    s.pending[key] = (frame.msg_type, frame.payload, frame.flags)
                    s.conflated += 1
                    self.registry.counter("conflated_total")
                    continue
                if s.queue.full() and self._drop_overflowed(s):
                    continue
                s.pending[key] = (frame.msg_type, frame.payload, frame.flags)
                with contextlib.suppress(asyncio.QueueFull):
                    s.queue.put_nowait(key)
                continue

            # 구독자별로 seq 를 다시 매긴다. 그래야 구독자의 갭 탐지가 유효하다
            raw = encode(frame.msg_type, s.out_seq, frame.payload, frame.flags)
            s.out_seq += 1
            if s.queue.full() and self._drop_overflowed(s):
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
        """프레임 하나씩 write + drain 한다. 모아 보내지 않는다.

        모아 보내는 쪽이 빠를 거라 보고 64프레임 배치를 넣었다가 되돌렸다.
        drain 마다 이벤트 루프를 한 바퀴 도니 구독자 100명 × 초당 300프레임이면
        초당 30,000번이라 그게 병목처럼 보였다. (인코딩은 초당 320,000건이
        나온다 — 14배 여력이라 인코딩은 애초에 병목이 아니다.)

        리플레이로 상류 유량을 고정해 재 보니 반대였다.
        구독자 100명 · 상류 약 295건/s 에서
            하나씩  p99  83.2ms · 1인당 처리량 유지 100.1%
            64배치  p99 146.7ms · 유지 89.6%
        drain 마다 양보하는 편이 구독자 사이에 공평하다. 배치는 한 구독자가
        루프를 오래 붙잡아 나머지의 꼬리를 늘린다.

        먼저 라이브 시세로 쟀을 땐 배치가 나아 보였는데, 그때는 회차마다
        상류가 227 → 140건/s 로 달라져 있었다. 그래서 load_test 가 회차별
        상류 유량을 같이 기록한다 — 그 값 없이 잰 비교는 성립하지 않는다.
        """
        try:
            while True:
                item = await s.queue.get()
                if isinstance(item, str):
                    # conflate: 큐에는 키만 있다. **보내는 순간의 최신값**을 꺼내
                    # 그때 seq 를 매긴다. 인코딩도 여기서 한 번만 한다.
                    got = s.pending.pop(item, None)
                    if got is None:
                        continue
                    msg_type, payload, flags = got
                    data = encode(msg_type, s.out_seq, payload, flags)
                    s.out_seq += 1
                else:
                    data = item
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
        mode = str(req.get("mode", s.mode)).lower()
        if mode in (MODE_STREAM, MODE_CONFLATE) and mode != s.mode:
            # 모드를 바꾸면 큐에 남은 것의 형태가 섞인다. 비우고 새로 시작한다.
            # 잃는 건 아직 안 보낸 것뿐이고, 구독 변경 자체가 재동기 지점이다.
            while not s.queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    s.queue.get_nowait()
            s.pending.clear()
            s.mode = mode
        log.info("구독자 #%d 구독 변경 → %s (%s)", s.id, s.symbols or "ALL", s.mode)

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
            "max_backlog": max((s.queue.qsize() for s in self.subs.values()), default=0),
            "max_wire_bytes": max((s.wire_bytes() for s in self.subs.values()), default=0),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        server = await asyncio.start_server(self._handle_client, cfg.tcp_host, cfg.tcp_port)
        log.info("MDFP/1 배포 서버 listening on %s:%d", cfg.tcp_host, cfg.tcp_port)

        http = HTTPServer(cfg.http_host, cfg.tcp_admin_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.route("GET", "/subscribers", lambda r: Response.json(
            {"count": len(self.subs), "items": [s.info() for s in self.subs.values()]}))
        await http.start()

        bus_task = asyncio.create_task(self._consume_bus(stop))
        gauge_task = asyncio.create_task(self._gauges(stop))
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()

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
            # 큐 깊이만으로는 밀림을 못 본다. Subscriber.wire_bytes 참고.
            self.registry.gauge("max_wire_bytes",
                                max((s.wire_bytes() for s in self.subs.values()), default=0))


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
