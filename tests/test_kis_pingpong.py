"""KIS 하트비트(PINGPONG)에 **PONG 제어 프레임**으로 답하는가.

README 한계에 "PINGPONG 응답은 실측 검증하지 못했습니다 — 데이터가 흐르는 동안에는 KIS 가
ping 을 보내지 않아 한 번도 받아보지 못했고, 코드는 공식 예제와 맞췄을 뿐입니다"라고 적혀 있었다.

실서버가 보내 주기를 기다릴 수는 없지만, **우리 쪽이 규약대로 답하는지**는 여기서 확인할 수 있다.
이 저장소의 wsproto 로 최소 서버를 띄우고, PINGPONG 을 텍스트 프레임으로 보낸 뒤, 돌아오는 것이
텍스트가 아니라 opcode 0xA(PONG) 제어 프레임인지 바이트로 확인한다.

여전히 확인 못 하는 것: KIS 서버가 그 응답을 하트비트로 인정하는지. 그건 실계좌 세션에서
실제로 ping 을 받아 봐야 안다. 그 경계는 README 에 남겨 둔다.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from mdfeed.config import Config
from mdfeed.wsproto import (OP_PONG, OP_TEXT, FrameDecoder, WSClient,
                            handshake_response, server_frame)


class FakeKISServer:
    """PINGPONG 을 보내고, 클라이언트가 돌려보내는 프레임을 바이트로 받아 두는 최소 서버."""

    def __init__(self):
        self.received: list[tuple[int, bytes]] = []
        self.server = None
        self.port = 0
        self._got = asyncio.Event()

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self.port

    async def _handle(self, reader, writer):
        raw = await reader.readuntil(b"\r\n\r\n")
        headers = {}
        for line in raw.decode("latin-1").split("\r\n")[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        writer.write(handshake_response(headers))
        await writer.drain()

        # KIS 가 보내는 모양 그대로 — 텍스트 메시지다
        ping = json.dumps({"header": {"tr_id": "PINGPONG", "datetime": "20260924090000"}})
        writer.write(server_frame(OP_TEXT, ping.encode()))
        await writer.drain()

        dec = FrameDecoder(expect_masked=True)      # 클라이언트→서버는 반드시 마스킹
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return
            for op, payload in dec.feed(chunk):
                self.received.append((op, payload))
                self._got.set()

    async def wait_for_reply(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._got.wait(), timeout=timeout)

    async def close(self):
        self.server.close()
        try:
            await asyncio.wait_for(self.server.wait_closed(), timeout=3)
        except (asyncio.TimeoutError, Exception):   # noqa: BLE001
            pass


def test_pingpong_is_answered_with_a_pong_control_frame():
    from mdfeed.adapters.kis import KISAdapter

    async def main():
        srv = FakeKISServer()
        port = await srv.start()
        cfg = Config()
        cfg.kis_app_key = "k"
        cfg.kis_app_secret = "s"
        adapter = KISAdapter(cfg, emit=lambda *_a, **_k: None)

        ws = await WSClient.connect(f"ws://127.0.0.1:{port}/")
        try:
            op, payload = await ws.recv(timeout=5)
            assert op == OP_TEXT
            text = payload.decode()
            assert json.loads(text)["header"]["tr_id"] == "PINGPONG"

            await adapter._on_control(text, ws)     # 어댑터가 실제로 하는 일
            await srv.wait_for_reply()
        finally:
            await ws.close(timeout=1)
            await srv.close()
        return srv.received, adapter.pingpongs

    received, pingpongs = asyncio.run(main())

    assert received, "아무것도 안 돌려보냈다 — 이러면 KIS 가 몇 분 뒤 세션을 끊는다"
    op, payload = received[0]
    assert op == OP_PONG, f"텍스트로 되돌려주면 하트비트로 인정 안 될 수 있다 (opcode={op:#x})"
    assert json.loads(payload.decode())["header"]["tr_id"] == "PINGPONG"   # 받은 내용을 그대로
    assert pingpongs == 1                                                   # 지표로도 센다


def test_non_pingpong_control_message_is_not_answered_with_pong():
    """등록 응답 같은 일반 제어 메시지에 PONG 을 쏘면 안 된다."""
    from mdfeed.adapters.kis import KISAdapter

    class RecordingWS:
        def __init__(self):
            self.pongs = 0

        async def pong(self, data: bytes = b"") -> None:
            self.pongs += 1

    async def main():
        cfg = Config()
        cfg.kis_app_key = "k"
        cfg.kis_app_secret = "s"
        adapter = KISAdapter(cfg, emit=lambda *_a, **_k: None)
        ws = RecordingWS()
        await adapter._on_control(json.dumps(
            {"header": {"tr_id": "H0STCNT0", "tr_key": "005930"},
             "body": {"rt_cd": "0", "msg1": "SUBSCRIBE SUCCESS"}}), ws)
        await adapter._on_control("not json at all", ws)
        return ws.pongs, adapter.pingpongs

    pongs, pingpongs = asyncio.run(main())
    assert pongs == 0 and pingpongs == 0
