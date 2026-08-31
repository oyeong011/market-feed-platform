"""정지 요청 뒤의 정리를 **진짜 서비스에 SIGTERM 을 보내** 검증한다.

앞서 붙인 종료 기한 시험은 합성 코루틴을 썼다. 그건 "기한이 동작한다"는
증명이지 **"진짜 서비스가 이 경로로 내려간다"** 는 증명이 아니다.

여기서는 실제 writer 를 실제 runtime.run 으로 띄운다. 실제 UDS 버스에 붙고,
실제 SQLite 를 열고, 실제 HTTP 관리 포트를 연다. 그리고 실제 SIGTERM 을 보낸다.

SIGTERM 을 잡는 이유는 버퍼에 든 틱을 flush 하기 위해서다. 잡기만 하고
정리가 안 끝나면 systemd 가 TimeoutStopSec 뒤에 SIGKILL 하고 버퍼는 똑같이
날아간다 — 잡은 쪽이 나은 점이 하나도 없다.
"""
import asyncio
import os
import signal
import socket
import sqlite3
import tempfile
import threading
import time

import pytest

from mdfeed import runtime
from mdfeed.bus import UDSPublisher
from mdfeed.config import Config
from mdfeed.models import MSG_TRADE, Trade
from mdfeed.protocol import encode
from mdfeed.services.writer import Writer

BASE_NS = 1_700_000_000_000_000_000


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.fixture
def cfg(tmp_path):
    # UDS 경로는 길이 제한이 있어 pytest tmp_path 를 못 쓴다
    run_dir = tempfile.mkdtemp(prefix="mdfs", dir="/tmp")
    c = Config()
    c.run_dir = run_dir
    c.sqlite_path = str(tmp_path / "w.db")
    c.pg_dsn = ""
    c.bus_path = os.path.join(run_dir, "b.sock")
    c.bus_paths = [c.bus_path]
    c.signal_bus_path = os.path.join(run_dir, "s.sock")
    c.writer_admin_port = free_port()
    c.http_host = "127.0.0.1"
    c.shutdown_grace_s = 3.0
    c.batch_size = 100000          # 종료 flush 로만 쓰이게 한다
    c.flush_interval_s = 3600.0
    c.log_json = False
    return c


def _sigterm_when_ready(port: int, timeout: float = 20.0,
                        after: threading.Event | None = None) -> threading.Thread:
    """서비스가 실제로 뜬 뒤에 SIGTERM 을 보낸다.

    고정 지연으로 보내면 느린 기동에서 손잡이가 아직 안 걸려 프로세스가 죽고,
    반대로 너무 일찍 보내면 **아직 아무것도 안 쌓인 상태**를 검증하게 된다.
    실제로 처음에 그렇게 써서 0행을 flush 하고 통과할 뻔했다.
    """
    def run():
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.3).close()
                break
            except OSError:
                time.sleep(0.05)
        if after is not None:
            after.wait(timeout)               # 검증할 것이 쌓일 때까지 기다린다
        else:
            time.sleep(0.4)
        os.kill(os.getpid(), signal.SIGTERM)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th


def test_실제_서비스가_SIGTERM_에_기한_안에_내려가고_버퍼를_비운다(cfg, monkeypatch):
    forced = []
    monkeypatch.setattr(runtime, "_force_exit", lambda code: forced.append(code))

    pub = UDSPublisher(cfg.bus_path)
    published = []
    fed = threading.Event()

    async def main(stop):
        await pub.start()

        async def feed():
            # writer 가 버스에 실제로 붙은 뒤에 보낸다. 먼저 보내면 아무도 못 받는다.
            for _ in range(400):
                if pub.subscriber_count:
                    break
                await asyncio.sleep(0.02)
            for i in range(50):
                t = Trade("TEST", "SYNTH", BASE_NS + i, BASE_NS + i,
                          100.0 + i, 1.0, 1)
                pub.publish(encode(MSG_TRADE, i, t.pack()))
                published.append(i)
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.3)          # writer 버퍼에 들어갈 여유
            fed.set()                         # 이제 SIGTERM 을 보내도 된다

        task = asyncio.create_task(feed())
        try:
            await Writer(cfg).run(stop)
        finally:
            task.cancel()
            await pub.close()

    _sigterm_when_ready(cfg.writer_admin_port, after=fed)
    t0 = time.time()
    rc = runtime.run("writer", main, cfg)
    took = time.time() - t0

    assert forced == [], f"정상 정리인데 강제 종료했다 (기한 {cfg.shutdown_grace_s}s)"
    assert rc == 0
    assert took < cfg.shutdown_grace_s + 8, f"{took:.1f}초 — 종료가 지나치게 느리다"
    assert published, "발행 자체가 안 됐다 — 시험이 아무것도 검증하지 못한다"

    # 진짜로 flush 됐는가. 이게 SIGTERM 을 잡는 유일한 이유다.
    c = sqlite3.connect(cfg.sqlite_path)
    n = c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    c.close()
    assert n > 0, "SIGTERM 을 잡고도 버퍼를 안 비웠다 — 잡은 의미가 없다"


def test_정리가_멈추면_기한을_넘기지_않고_강제_종료한다(cfg, monkeypatch):
    """실제 서비스의 종료 경로에 멈춤을 주입한다."""
    forced = []
    monkeypatch.setattr(runtime, "_force_exit", lambda code: forced.append(code))

    async def never(*_a, **_kw):
        await asyncio.Event().wait()

    # 종료 직전 flush 가 안 끝나는 상황 — 디스크가 멎거나 DB 락이 안 풀릴 때다
    monkeypatch.setattr(Writer, "_flush", never)

    pub = UDSPublisher(cfg.bus_path)

    async def main(stop):
        await pub.start()
        try:
            await Writer(cfg).run(stop)
        finally:
            await pub.close()

    _sigterm_when_ready(cfg.writer_admin_port)
    t0 = time.time()
    runtime.run("writer", main, cfg)
    took = time.time() - t0

    assert forced == [runtime.EXIT_SHUTDOWN_TIMEOUT], (
        f"{took:.1f}초 동안 안 내려갔다 — systemd 가 SIGKILL 할 때까지 기다린다")
