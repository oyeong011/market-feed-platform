"""프로세스 간 메시지 버스 (Unix Domain Socket PUB/SUB, ZeroMQ 선택 가능).

토폴로지
--------
    feedd  ──(UDS PUB)──┬── tcp_gateway   (외부 구독자에게 MDFP/1 바이너리)
                        ├── ws_gateway    (브라우저 대시보드에 JSON)
                        ├── writer        (Postgres/SQLite 적재)
                        └── strategy      (지표 계산 → SIGNAL 재발행)

수집을 한 프로세스로 몰고 배포를 분리한 이유는, 소비자가 느려지거나 죽어도
업스트림 세션은 끊기지 않게 하기 위해서다. 마켓데이터에서 재접속은 곧 갭이고,
갭은 곧 복구 비용이다.

백프레셔 정책
-------------
구독자별로 유한 큐를 둔다. 큐가 차면 **가장 오래된 프레임부터 버리고** 드롭
카운터를 올린다. 느린 구독자 하나가 발행자를 멈춰 세우게 두면 그 순간
전체 피드가 함께 멈춘다(head-of-line blocking). 유실은 허용하되 반드시 센다.

UDS를 기본으로 둔 이유
----------------------
같은 호스트 안에서는 TCP 루프백보다 UDS가 빠르고(체크섬/윈도우 계산 없음),
파일시스템 권한으로 접근제어가 되며, 의존성이 없다. ZeroMQ는 pyzmq가 설치돼
있고 여러 호스트로 흩어질 때만 MDFEED_BUS_BACKEND=zmq 로 켠다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import AsyncIterator, Callable

from .protocol import Frame, FrameParser

log = logging.getLogger("mdfeed.bus")

# sockaddr_un.sun_path 는 리눅스 108바이트, macOS 104바이트로 고정돼 있다.
# 넘으면 bind/connect 가 "AF_UNIX path too long" 으로 실패하는데, 재접속 루프
# 안에서 나면 원인을 찾기까지 한참 걸린다. 기동 시점에 잡아 명확히 알려준다.
MAX_UDS_PATH = 100


def check_uds_path(path: str) -> None:
    if len(path.encode()) > MAX_UDS_PATH:
        raise ValueError(
            f"UDS 소켓 경로가 너무 깁니다 ({len(path.encode())}바이트 > {MAX_UDS_PATH}).\n"
            f"  경로: {path}\n"
            f"  조치: MDFEED_RUN_DIR 를 짧은 경로로 지정하세요 "
            f"(예: MDFEED_RUN_DIR=/run/mdfeed 또는 /tmp/mdfeed)")


class UDSPublisher:
    """Unix Domain Socket 브로드캐스트 발행자."""

    def __init__(self, path: str, queue_size: int = 4096, on_drop: Callable | None = None):
        self.path = path
        self.queue_size = queue_size
        self._on_drop = on_drop
        self._server: asyncio.AbstractServer | None = None
        self._clients: dict[int, asyncio.Queue] = {}
        # 종료 시 정리해야 할 연결. Python 3.12+ 의 Server.wait_closed() 는
        # 핸들러가 전부 끝나야 반환하는데, 핸들러는 q.get() 에서 영원히 대기한다.
        # 이걸 안 끊으면 SIGTERM 을 받고도 프로세스가 안 내려간다.
        self._conns: dict[int, tuple] = {}
        self._next_id = 0
        self.dropped = 0
        self.published = 0

    async def start(self) -> None:
        check_uds_path(self.path)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)
        self._server = await asyncio.start_unix_server(self._handle, path=self.path)
        os.chmod(self.path, 0o660)          # 같은 그룹만 구독 가능
        log.info("bus publisher listening on %s", self.path)

    # reader 는 안 쓰지만 start_unix_server 콜백 규약이라 받는다
    async def _handle(self, _reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        cid = self._next_id
        self._next_id += 1
        q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self._clients[cid] = q
        self._conns[cid] = (asyncio.current_task(), writer)
        peer = writer.get_extra_info("peername") or "uds"
        log.info("bus subscriber #%d connected (%s), total=%d", cid, peer, len(self._clients))
        try:
            while True:
                data = await q.get()
                writer.write(data)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception as e:                      # noqa: BLE001
            log.warning("bus subscriber #%d error: %s", cid, e)
        finally:
            self._clients.pop(cid, None)
            self._conns.pop(cid, None)
            with contextlib.suppress(Exception):
                writer.close()
            log.info("bus subscriber #%d disconnected, total=%d", cid, len(self._clients))

    def publish(self, frame_bytes: bytes) -> None:
        """논블로킹 발행. 큐가 차면 오래된 것부터 버린다."""
        self.published += 1
        for q in self._clients.values():
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()              # 가장 오래된 프레임 폐기
                self.dropped += 1
                if self._on_drop:
                    self._on_drop()
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(frame_bytes)

    @property
    def subscriber_count(self) -> int:
        return len(self._clients)

    async def close(self) -> None:
        # 1) 연결 핸들러를 먼저 끊는다 (안 그러면 wait_closed 가 영원히 안 끝난다)
        tasks = []
        for _cid, (task, writer) in list(self._conns.items()):
            with contextlib.suppress(Exception):
                writer.close()
            if task is not None and not task.done():
                task.cancel()
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._conns.clear()
        self._clients.clear()
        # 2) 그 다음 리스너를 닫는다
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)


class UDSSubscriber:
    """자동 재접속하는 구독자. async for 로 Frame 을 받는다."""

    def __init__(self, path: str, reconnect_s: float = 1.0, max_reconnect_s: float = 15.0):
        check_uds_path(path)
        self.path = path
        self.reconnect_s = reconnect_s
        self.max_reconnect_s = max_reconnect_s
        self.parser = FrameParser()
        self.connect_count = 0

    async def frames(self) -> AsyncIterator[Frame]:
        backoff = self.reconnect_s
        while True:
            try:
                reader, writer = await asyncio.open_unix_connection(self.path)
                self.connect_count += 1
                backoff = self.reconnect_s
                log.info("bus subscriber connected to %s", self.path)
                try:
                    while True:
                        chunk = await reader.read(65536)
                        if not chunk:
                            raise ConnectionResetError("publisher closed")
                        for f in self.parser.feed(chunk):
                            yield f
                finally:
                    with contextlib.suppress(Exception):
                        writer.close()
            except asyncio.CancelledError:
                raise
            except Exception as e:                  # noqa: BLE001
                log.warning("bus 연결 끊김(%s). %.1fs 후 재시도", e, backoff)
                await asyncio.sleep(backoff)
                # 지수 백오프. 발행자가 죽어있는 동안 재시도 폭주를 막는다
                backoff = min(backoff * 2, self.max_reconnect_s)


def make_publisher(cfg) -> "UDSPublisher":
    if cfg.bus_backend == "zmq":
        try:
            from .bus_zmq import ZMQPublisher      # 선택 의존성
            return ZMQPublisher(cfg.bus_zmq_endpoint, cfg.bus_queue_size)
        except ImportError:
            log.warning("pyzmq 없음 → UDS 백엔드로 폴백")
    return UDSPublisher(cfg.bus_path, cfg.bus_queue_size)


def make_subscriber(cfg) -> "UDSSubscriber":
    if cfg.bus_backend == "zmq":
        try:
            from .bus_zmq import ZMQSubscriber
            return ZMQSubscriber(cfg.bus_zmq_endpoint)
        except ImportError:
            log.warning("pyzmq 없음 → UDS 백엔드로 폴백")
    return UDSSubscriber(cfg.bus_path)
