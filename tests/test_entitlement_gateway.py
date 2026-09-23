"""권한 검사가 **두 게이트웨이 모두에서** 실제로 막는가.

한쪽에만 있으면 "켰는데 이 구현에서는 안 먹는" 상태가 된다. 파이썬 판과 C++ 판을 같은 시험으로 돌린다.

막는 것을 확인한다: 권한 밖 종목은 요청해도, 필터를 비워도(전체 구독) 오지 않는다.
그리고 **조용히 거절하지 않는다** — 구독자가 거절 사실을 MSG_ACK 로 받는다.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time

import pytest

from mdfeed.bus import UDSPublisher
from mdfeed.config import Config
from mdfeed.models import MSG_ACK, MSG_SUBSCRIBE, MSG_TRADE, Trade, now_ns
from mdfeed.protocol import FLAG_SNAPSHOT, FrameParser, encode

from test_cpp_gateway import Gateway, _wait, bus_dir, gateway_bin, trade  # noqa: F401

ENT = """# 토큰        허용 종목
desk-a        TEST:AAA
full-desk     *
"""


class EntClient:
    """구독 요청을 보내고 실제로 오는 프레임과 ACK 를 모은다."""

    def __init__(self, port: int, token: str | None, symbols=None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req: dict = {}
        if token is not None:
            req["token"] = token
        if symbols is not None:
            req["symbols"] = list(symbols)
        self.sock.sendall(encode(MSG_SUBSCRIBE, 0, json.dumps(req).encode()))
        self.parser = FrameParser()
        self.symbols_seen: set[str] = set()
        self.acks: list[dict] = []

    def collect(self, seconds: float) -> None:
        end = time.time() + seconds
        self.sock.settimeout(0.2)
        while time.time() < end:
            try:
                chunk = self.sock.recv(65536)
            except (socket.timeout, TimeoutError):
                continue
            if not chunk:
                break
            for f in self.parser.feed(chunk):
                if f.msg_type == MSG_ACK:
                    self.acks.append(json.loads(f.payload))
                elif f.msg_type == MSG_TRADE and not (f.flags & FLAG_SNAPSHOT):
                    t = Trade.unpack(f.payload)
                    self.symbols_seen.add(f"{t.venue}:{t.symbol}")

    def close(self):
        self.sock.close()


async def _drive(pub: UDSPublisher, n: int = 200) -> None:
    seq = 0
    for i in range(n):
        for sym in ("AAA", "BBB"):
            t = Trade("TEST", sym, now_ns(), now_ns(), 100.0 + i, 1.0, 1)
            pub.publish(encode(MSG_TRADE, seq, t.pack())); seq += 1
        await asyncio.sleep(0.004)


@pytest.fixture
def ent_file(tmp_path):
    p = tmp_path / "entitlements.txt"
    p.write_text(ENT, encoding="utf-8")
    return str(p)


def _run_python_gateway(bus_path: str, ent_path: str, port: int):
    from mdfeed.services.tcp_gateway import TCPGateway
    cfg = Config()
    cfg.bus_path = bus_path
    cfg.bus_paths = [bus_path]
    cfg.tcp_host = "127.0.0.1"
    cfg.tcp_port = port
    cfg.tcp_admin_port = port + 1
    cfg.http_host = "127.0.0.1"
    cfg.entitlements_file = ent_path
    return TCPGateway(cfg)


def free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _scenarios(port: int):
    return {
        "권한 밖 종목 요청": EntClient(port, "desk-a", ["TEST:AAA", "TEST:BBB"]),
        "전체 구독 요청": EntClient(port, "desk-a", None),
        "토큰 없음": EntClient(port, None, ["TEST:AAA"]),
        "모르는 토큰": EntClient(port, "nope", ["TEST:AAA"]),
        "전체 권한": EntClient(port, "full-desk", None),
    }


def _assert_entitlements(cli: dict):
    a = cli["권한 밖 종목 요청"]
    assert a.symbols_seen == {"TEST:AAA"}, a.symbols_seen        # BBB 는 요청해도 안 온다
    assert any(x.get("error") == "NOT_ENTITLED" and x.get("denied") == ["TEST:BBB"] for x in a.acks), a.acks

    b = cli["전체 구독 요청"]
    assert b.symbols_seen == {"TEST:AAA"}, b.symbols_seen        # 필터를 비워도 허용 집합으로 좁혀진다

    for name in ("토큰 없음", "모르는 토큰"):
        c = cli[name]
        assert c.symbols_seen == set(), (name, c.symbols_seen)   # 아무것도 안 온다
        assert any(x.get("error") in ("TOKEN_REQUIRED", "UNKNOWN_TOKEN") for x in c.acks), (name, c.acks)

    f = cli["전체 권한"]
    assert f.symbols_seen == {"TEST:AAA", "TEST:BBB"}, f.symbols_seen


def test_cpp_gateway_enforces_entitlements(gateway_bin, ent_file):
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        gw = Gateway(gateway_bin, bus_path, MDFEED_ENTITLEMENTS_FILE=ent_file)
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway on bus")
            cli = _scenarios(gw.port)
            await _wait(lambda: gw.health()["subscribers"] == len(cli), 5, "clients")
            await asyncio.sleep(0.2)
            await _drive(pub)
            for c in cli.values():
                await asyncio.to_thread(c.collect, 0.4)
            h = gw.health()
            for c in cli.values():
                c.close()
            return cli, h
        finally:
            gw.proc.kill()
            await pub.close()

    cli, health = asyncio.run(main())
    _assert_entitlements(cli)
    assert health["entitlements"]["enabled"] is True
    assert health["entitlements"]["tokens"] == 2
    assert health["entitlements"]["denied"] >= 3


def test_python_gateway_enforces_entitlements(ent_file):
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")
    port = free_port()

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        gw = _run_python_gateway(bus_path, ent_file, port)
        stop = asyncio.Event()
        task = asyncio.create_task(gw.run(stop))
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "python gateway on bus")
            cli = _scenarios(port)
            await asyncio.sleep(0.3)
            await _drive(pub)
            for c in cli.values():
                await asyncio.to_thread(c.collect, 0.4)
            h = gw.health()
            for c in cli.values():
                c.close()
            return cli, h
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=10)
            await pub.close()

    cli, health = asyncio.run(main())
    _assert_entitlements(cli)
    assert health["entitlements"]["enabled"] is True and health["entitlements"]["tokens"] == 2


def test_disabled_by_default_is_logged_not_silent(gateway_bin):
    """권한 파일을 안 주면 검사가 꺼진다. 그 사실이 로그에 남아야 한다 —
    "켜 둔 줄 알았는데 안 켜져 있었다"가 이 저장소에서 반복된 사고 유형이다."""
    run = bus_dir()
    gw = Gateway(gateway_bin, os.path.join(run, "bus.sock"))
    try:
        time.sleep(0.5)
        assert gw.health()["entitlements"]["enabled"] is False
        assert any("권한 검사 꺼짐" in l for l in gw._stderr), gw._stderr[:5]
    finally:
        gw.proc.kill()
