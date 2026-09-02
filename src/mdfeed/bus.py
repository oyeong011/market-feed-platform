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
import json
import logging
import os
import time
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
        # 구독자별 상태. 합계 하나로는 **누가** 느린지 알 수 없다.
        # 실측(2026-08-31): bus_dropped 112,979 을 보고도 다섯 구독자
        # (tcp-gateway·ws-gateway·writer·strategy·quality) 중 누가 흘린 건지
        # 알 방법이 없었다. 무엇을 고쳐야 하는지가 곧 그 답인데.
        self._stats: dict[int, dict] = {}
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

    async def _read_name(self, reader: asyncio.StreamReader) -> str:
        """구독자가 스스로 밝힌 이름. 안 보내도 된다.

        이름이 없으면 드롭 지표가 익명 번호로만 남아, 무엇을 고쳐야 하는지
        알 수 없다. 기다리는 데 기한을 둬서 옛 구독자와도 호환된다.
        """
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            name = json.loads(line).get("name")
            return str(name)[:32] if name else "anonymous"
        except Exception:                           # noqa: BLE001
            return "anonymous"

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        cid = self._next_id
        self._next_id += 1
        q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self._clients[cid] = q
        self._conns[cid] = (asyncio.current_task(), writer)
        peer = writer.get_extra_info("peername") or "uds"
        name = await self._read_name(reader)
        self._stats[cid] = {"name": name, "connected_at": time.time(),
                            "dropped": 0, "sent": 0}
        log.info("bus subscriber #%d(%s) connected (%s), total=%d",
                 cid, name, peer, len(self._clients))
        try:
            st = self._stats[cid]
            while True:
                data = await q.get()
                writer.write(data)
                await writer.drain()
                st["sent"] += 1
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception as e:                      # noqa: BLE001
            log.warning("bus subscriber #%d error: %s", cid, e)
        finally:
            self._clients.pop(cid, None)
            self._conns.pop(cid, None)
            st = self._stats.pop(cid, None)
            if st and st["dropped"]:
                log.warning("bus subscriber #%d(%s) 종료 — 이 구독자에게 %d 프레임을 흘렸다",
                            cid, st["name"], st["dropped"])
            with contextlib.suppress(Exception):
                writer.close()
            log.info("bus subscriber #%d disconnected, total=%d", cid, len(self._clients))

    def publish(self, frame_bytes: bytes) -> None:
        """논블로킹 발행. 큐가 차면 오래된 것부터 버린다."""
        self.published += 1
        for cid, q in self._clients.items():
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()              # 가장 오래된 프레임 폐기
                self.dropped += 1
                st = self._stats.get(cid)
                if st is not None:
                    st["dropped"] += 1          # 누가 흘렸는지가 진단의 전부다
                if self._on_drop:
                    self._on_drop(st["name"] if st else "anonymous")
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(frame_bytes)

    def subscriber_stats(self) -> list[dict]:
        """구독자별 상태. 느린 구독자를 이름으로 지목할 수 있어야 한다."""
        out = []
        for cid, q in self._clients.items():
            st = self._stats.get(cid, {})
            out.append({"id": cid, "name": st.get("name", "anonymous"),
                        "backlog": q.qsize(), "queue_size": self.queue_size,
                        "dropped": st.get("dropped", 0),
                        "sent": st.get("sent", 0),
                        "uptime_s": round(time.time() - st["connected_at"], 1)
                        if st.get("connected_at") else None})
        return sorted(out, key=lambda x: -x["dropped"])

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
        self._stats.clear()
        # 2) 그 다음 리스너를 닫는다
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)


class SourceTracker:
    """버스 소스별 상태. **합쳐서 세면 하나가 죽어도 안 보인다.**

    실측(2026-09-02): ws-gateway 가 크립토 버스 구독을 잃고 KRX 하트비트만
    받는 상태로 30시간 돌았다. 그런데 `upstream_connected` 는 True 였고
    `last_frame_age_s` 는 5초였다 — 살아 있는 다른 소스가 그 값을 갱신했기
    때문이다. 대시보드는 낡은 크립토 시세를 정상처럼 보여주고 있었다.

    거래소 하나를 통째로 잃는 건 이 시스템에서 가장 큰 사고인데,
    지표 하나로 합치면 그게 보이지 않는다.
    """

    def __init__(self, paths, stale_after_s: float = 60.0):
        self.stale_after_s = stale_after_s
        self.state = {p: {"connected": False, "frames": 0,
                          "last_at": 0.0, "restarts": 0} for p in paths}

    def mark(self, path: str) -> None:
        st = self.state.setdefault(path, {"connected": False, "frames": 0,
                                          "last_at": 0.0, "restarts": 0})
        st["connected"] = True
        st["frames"] += 1
        st["last_at"] = time.time()

    def died(self, path: str) -> None:
        st = self.state.get(path)
        if st is not None:
            st["connected"] = False
            st["restarts"] += 1

    def report(self) -> dict:
        now = time.time()
        srcs, degraded = [], []
        for path, st in self.state.items():
            age = (now - st["last_at"]) if st["last_at"] else None
            name = os.path.basename(path)
            stale = st["frames"] > 0 and age is not None and age > self.stale_after_s
            if stale or not st["connected"]:
                degraded.append(name)
            srcs.append({"source": name, "connected": st["connected"],
                         "frames": st["frames"], "restarts": st["restarts"],
                         "last_frame_age_s": round(age, 1) if age is not None else None,
                         "stale": stale})
        return {"sources": srcs, "degraded_sources": degraded}


async def consume_forever(path: str, on_frame, stop, tracker: SourceTracker,
                          name: str = "", backoff_max: float = 15.0) -> None:
    """한 소스를 계속 소비한다. **죽어도 다시 붙고, 죽은 사실을 센다.**

    예전엔 소비자 태스크가 한 번 죽으면 아무도 되살리지 않았다.
    ws-gateway 는 그 태스크를 변수에 담지도 않아 GC 가 수거해 갔다
    ("Task was destroyed but it is pending!"). 거래소 하나가 통째로 사라졌고
    30시간 동안 어느 지표에도 안 남았다.
    """
    backoff = 1.0
    while not stop.is_set():
        try:
            sub = UDSSubscriber(path, name=name)
            async for frame in sub.frames():
                if stop.is_set():
                    return
                tracker.mark(path)
                backoff = 1.0
                on_frame(frame)
        except asyncio.CancelledError:
            raise
        except BaseException as e:                  # noqa: BLE001
            tracker.died(path)
            log.exception("[%s] 소비자가 죽었다 — 되살린다: %s",
                          os.path.basename(path), e)
        if stop.is_set():
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=backoff)
        backoff = min(backoff * 2, backoff_max)


class UDSSubscriber:
    """자동 재접속하는 구독자. async for 로 Frame 을 받는다."""

    def __init__(self, path: str, reconnect_s: float = 1.0, max_reconnect_s: float = 15.0,
                 name: str | None = None):
        check_uds_path(path)
        self.path = path
        # 접속 직후 한 줄로 자기를 밝힌다. 발행자가 드롭을 이름으로 귀속시킨다.
        # 안 밝혀도 동작한다(anonymous) — 옛 구독자와의 호환을 깨지 않는다.
        self.name = name
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
                if self.name:
                    writer.write(json.dumps({"name": self.name}).encode() + b"\n")
                    await writer.drain()
                log.info("bus subscriber connected to %s (%s)",
                         self.path, self.name or "anonymous")
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
