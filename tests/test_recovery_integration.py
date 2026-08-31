"""정체 → 세션 취소 → 정리 → 재접속. **진짜 소켓으로** 도는지 본다.

2026-08-31 에 upbit 이 11.2시간 멎었다. 정체 판정도 지표도 경보 규칙도
전부 정상이었고 태스크도 안 죽었다. **복구만 안 됐다.**
취소한 세션의 정리(죽은 소켓에 close → drain)가 안 끝나서 재접속이
그 뒤에 서 있었던 것이다.

고친 뒤 붙인 시험들은 정리 코루틴을 가짜로 만들어 기한만 확인했다.
그건 "기한이 동작한다"는 증명이지 **"이 경로가 실제 소켓에서 돈다"** 는
증명이 아니다. 여기서는 진짜 TCP 서버를 띄우고, 진짜 WSClient 로 붙고,
진짜 WSClient.close() 를 거쳐 재접속하는지 본다.

서버는 핸드셰이크만 하고 **아무것도 안 보내고 아무것도 안 읽는다** —
반쯤 죽은 연결이 밖에서 보이는 모습 그대로다.
"""
import asyncio
import contextlib
import time

import pytest

from mdfeed.adapters.base import Adapter
from mdfeed.wsproto import WSClient, handshake_response


class SilentWSServer:
    """핸드셰이크만 받고 침묵하는 서버. 소켓은 계속 열려 있다."""

    def __init__(self):
        self.server = None
        self.port = 0
        self.accepted = 0
        self._conns = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        self.accepted += 1
        self._conns.append(writer)
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        except Exception:                            # noqa: BLE001
            return
        headers = {}
        for line in raw.decode("latin1").split("\r\n")[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        writer.write(handshake_response(headers))
        await writer.drain()
        # 여기서부터 침묵한다. 읽지도 보내지도 않는다.
        await asyncio.Event().wait()

    async def close(self):
        for w in self._conns:
            try:
                w.close()
            except Exception:                        # noqa: BLE001
                pass
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), timeout=2)
        except Exception:                            # noqa: BLE001
            pass


class SilentVenue(Adapter):
    """진짜 WSClient 로 붙어 진짜 recv 를 기다리는 어댑터."""

    name = "silent"

    def __init__(self, port: int):
        super().__init__(cfg=None, emit=lambda m: None)
        self.port = port
        self.stale_after_s = 0.4
        self.CANCEL_TIMEOUT_S = 0.5
        self.sessions = 0

    async def session(self) -> None:
        self.sessions += 1
        ws = await WSClient.connect(f"ws://127.0.0.1:{self.port}/")
        try:
            while True:
                # recv 타임아웃을 정체 임계보다 **훨씬 길게** 둔다.
                # 짧으면 recv 가 먼저 터져 세션이 예외로 끝나고, 정작 재현하려는
                # "감시자가 취소 → 정리 → 재접속" 경로를 안 탄다.
                # (처음에 그렇게 써서 시험이 회귀를 못 잡았다.)
                await ws.recv(timeout=30.0)
        finally:
            # 운영 어댑터와 같은 정리 경로다. 여기가 안 끝나면 재접속이 막힌다.
            await ws.close()



async def _stop(a, runner) -> None:
    """정지 요청 → 스스로 끝나길 기다림 → 안 끝나면 취소.

    run() 은 CancelledError 를 다시 던지므로(그게 맞다) 여기서 삼킨다.
    시험 뒷정리가 실패하면 진짜 결과가 안 보인다.
    """
    a.stop()
    _done, pending = await asyncio.wait({runner}, timeout=3)
    if pending:
        runner.cancel()
        await asyncio.wait({runner}, timeout=3)
    with contextlib.suppress(BaseException):
        runner.result()


@pytest.mark.asyncio
async def test_침묵하는_거래소에서_실제로_재접속한다():
    """11.2시간 무음의 경로를 진짜 소켓으로 재현한다."""
    srv = SilentWSServer()
    await srv.start()
    a = SilentVenue(srv.port)

    runner = asyncio.create_task(a.run())
    t0 = time.time()
    try:
        # 정체 판정(0.4s) + 정리 + 백오프. 넉넉히 잡아도 몇 초면 여러 번 돈다.
        while a.reconnects < 2 and time.time() - t0 < 15:
            await asyncio.sleep(0.05)
    finally:
        await _stop(a, runner)
        await srv.close()

    took = time.time() - t0
    assert a.reconnects >= 2, (
        f"{took:.1f}초 동안 재접속 {a.reconnects}회 — 복구가 안 돈다")
    assert srv.accepted >= 2, f"서버가 받은 접속 {srv.accepted}회 — 다시 안 붙었다"
    assert a.sessions >= 2
    # 매번 정체 판정 → 취소 → 정리 → 재접속이 도는 데 걸리는 시간
    assert took < 12, f"{took:.1f}초 — 복구가 지나치게 느리다"


@pytest.mark.asyncio
async def test_정리가_끝나지_않아도_재접속이_막히지_않는다(monkeypatch):
    """close() 가 응답하지 않는 상황을 실제 어댑터 경로에 주입한다.

    앞 시험은 정상 소켓에서 경로가 도는지를 본다. 이건 그 경로의
    **정리 단계가 멈췄을 때**도 재접속이 도는지를 본다 — 그게 실제로
    일어났던 일이다.
    """
    srv = SilentWSServer()
    await srv.start()
    a = SilentVenue(srv.port)

    async def never_returns(self, timeout=None):
        await asyncio.Event().wait()                 # 영원히 안 끝나는 close

    monkeypatch.setattr(WSClient, "close", never_returns)

    runner = asyncio.create_task(a.run())
    t0 = time.time()
    try:
        while a.reconnects < 1 and time.time() - t0 < 15:
            await asyncio.sleep(0.05)
    finally:
        await _stop(a, runner)
        await srv.close()

    assert a.reconnects >= 1, (
        f"{time.time() - t0:.1f}초 동안 재접속 0회 — 정리가 복구를 막고 있다")
