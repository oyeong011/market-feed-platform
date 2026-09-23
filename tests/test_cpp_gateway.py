"""C++ tcp_gateway (cpp/src/tcp_gateway.cpp) 가 파이썬 발행자·참조 클라이언트와 같은 규약으로 도는지.

파이썬 게이트웨이의 계약을 그대로 요구한다.
  * 접속 즉시 스냅샷(FLAG_SNAPSHOT) → MSG_SNAPSHOT 메타 → 증분
  * MSG_SUBSCRIBE 필터가 먹고, 걸러진 뒤에도 구독자 seq 는 연속 (재번호)
  * 느린 구독자는 오래된 것부터 버리고 한도를 넘으면 끊는다 (발행자는 안 멈춘다)
  * conflate 모드는 종목별 최신값만 보내되 seq 는 연속
  * /healthz /metrics /subscribers 가 파이썬과 같은 이름으로 나온다
컴파일러가 없으면 스킵한다.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import urllib.error
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from mdfeed.bus import UDSPublisher
from mdfeed.models import MSG_BOOK, MSG_HEARTBEAT, MSG_SNAPSHOT, MSG_SUBSCRIBE, MSG_TRADE, BookTop, Trade
from mdfeed.protocol import FLAG_SNAPSHOT, FrameParser, SequenceTracker, encode, heartbeat

ROOT = Path(__file__).resolve().parents[1]
CPP = ROOT / "cpp"


def _compiler() -> str | None:
    for c in (os.environ.get("CXX"), "c++", "clang++", "g++"):
        if c and shutil.which(c):
            return c
    return None


@pytest.fixture(scope="session")
def gateway_bin(tmp_path_factory) -> Path:
    cxx = _compiler()
    if cxx is None:
        pytest.skip("C++ compiler not found (c++/clang++/g++)")
    out = tmp_path_factory.mktemp("cpp") / "tcp_gateway"
    cmd = [cxx, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", f"-I{CPP / 'include'}",
           str(CPP / "src" / "tcp_gateway.cpp"), "-o", str(out)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.fail(f"C++ build failed:\n{proc.stderr}")
    return out


class Gateway:
    """C++ 게이트웨이 프로세스. 포트 0 으로 띄우고 첫 stdout 줄에서 실제 포트를 읽는다."""

    def __init__(self, binary: Path, bus_path: str, **env_overrides):
        env = {k: v for k, v in os.environ.items() if not k.startswith("MDFEED_")}
        env.update({"MDFEED_BUS_PATH": bus_path, "MDFEED_TCP_HOST": "127.0.0.1", "MDFEED_HTTP_HOST": "127.0.0.1",
                    "MDFEED_TCP_PORT": "0", "MDFEED_TCP_ADMIN_PORT": "0"})
        env.update({k: str(v) for k, v in env_overrides.items()})
        self.proc = subprocess.Popen([str(binary)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = self.proc.stdout.readline()
        info = json.loads(line)
        assert info["event"] == "listening" and info["impl"] == "c++"
        self.port, self.admin = info["tcp_port"], info["admin_port"]
        self._stderr: list[str] = []
        threading.Thread(target=self._drain, daemon=True).start()

    def _drain(self):
        for line in self.proc.stderr:
            self._stderr.append(line)

    def get(self, path: str):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.admin}{path}", timeout=3) as r:
            return r.status, r.read().decode()

    def health(self) -> dict:
        try:
            _, body = self.get("/healthz")
        except urllib.error.HTTPError as e:      # 503 도 본문은 JSON
            body = e.read().decode()
        return json.loads(body)

    def stop(self) -> int:
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=5)


def free_tcp_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def bus_dir() -> str:
    # UDS 경로 104바이트 한도 — pytest tmp_path 는 너무 길다
    return tempfile.mkdtemp(prefix="mdfc", dir="/tmp")


def trade(sym: str, i: int, px: float | None = None) -> bytes:
    t = Trade("TEST", sym, 1_700_000_000_000_000_000 + i * 1_000_000, 1_700_000_000_000_000_000 + i * 1_000_000 + 500, px or 100.0 + i * 0.01, 0.5, 1)
    return t.pack()


class Collector:
    """참조 클라이언트(client.py)와 같은 방식. 스냅샷과 증분을 구분하고 seq 를 검사한다."""

    def __init__(self, port: int, symbols=None, mode=None, rcvbuf: int | None = None):
        self.sock = socket.socket()
        if rcvbuf:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.sock.connect(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req = {}
        if symbols:
            req["symbols"] = list(symbols)
        if mode:
            req["mode"] = mode
        if req:
            self.sock.sendall(encode(MSG_SUBSCRIBE, 0, json.dumps(req).encode()))
        self.parser, self.track = FrameParser(), SequenceTracker()
        self.snapshots: list[tuple[int, bytes]] = []
        self.meta: dict | None = None
        self.incremental: list[tuple[int, int, bytes]] = []   # (msg_type, seq, payload)
        self.heartbeats = 0
        self.first_incremental_after_meta: bool | None = None

    def collect(self, seconds: float, want_trades: int = 0) -> None:
        deadline = time.time() + seconds
        self.sock.settimeout(0.2)
        while time.time() < deadline:
            if want_trades and sum(1 for t, _, _ in self.incremental if t == MSG_TRADE) >= want_trades:
                break
            try:
                chunk = self.sock.recv(65536)
            except (socket.timeout, TimeoutError):
                continue
            if not chunk:
                break
            for f in self.parser.feed(chunk):
                if f.flags & FLAG_SNAPSHOT:
                    if f.msg_type == MSG_SNAPSHOT:
                        self.meta = json.loads(f.payload)
                    else:
                        self.snapshots.append((f.msg_type, f.payload))
                    continue
                self.track.observe(f.seq)
                if self.first_incremental_after_meta is None:
                    self.first_incremental_after_meta = self.meta is not None
                if f.msg_type == MSG_HEARTBEAT:
                    self.heartbeats += 1
                self.incremental.append((f.msg_type, f.seq, f.payload))

    def trades(self, sym: str | None = None) -> list[Trade]:
        out = [Trade.unpack(p) for t, _, p in self.incremental if t == MSG_TRADE]
        return [t for t in out if sym is None or t.symbol == sym]

    def close(self):
        self.sock.close()


def _drained_to_eof(sock: socket.socket, timeout: float = 3.0) -> bool:
    """수신 버퍼에 남은 걸 다 읽고 EOF 가 오면 True. 끊기지 않았으면 타임아웃 → False."""
    sock.settimeout(timeout)
    try:
        while True:
            if not sock.recv(65536):
                return True
    except (socket.timeout, TimeoutError):
        return False
    except OSError:
        return True


async def _wait(pred, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timeout waiting for {what}")


def test_snapshot_filter_and_renumbering(gateway_bin):
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")

    async def main():
        pub = UDSPublisher(bus_path, queue_size=4096)
        await pub.start()
        gw = Gateway(gateway_bin, bus_path)
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway to subscribe to the bus")
            seq = 0
            # 캐시를 채운다: AAA 체결 → BBB 체결 → AAA 호가 (같은 키를 덮는다)
            for frame in (encode(MSG_TRADE, seq, trade("AAA", 0)), encode(MSG_TRADE, seq + 1, trade("BBB", 0)),
                          encode(MSG_BOOK, seq + 2, BookTop("TEST", "AAA", 1, 2, 99.0, 1.0, 101.0, 1.0).pack())):
                pub.publish(frame)
            seq += 3
            await asyncio.sleep(0.05)
            await _wait(lambda: gw.health()["cached_symbols"] == 2, 3, "cache to fill")

            col = Collector(gw.port, symbols=["TEST:AAA"])
            await _wait(lambda: gw.health()["subscribers"] == 1, 3, "client to be accepted")
            # 깨진 구독 요청은 무시돼야 한다 (파이썬: JSONDecodeError → 무시). 필터가 풀리면 BBB 가 섞여 들어온다
            col.sock.sendall(encode(MSG_SUBSCRIBE, 0, b"not json at all"))
            col.sock.sendall(encode(MSG_SUBSCRIBE, 0, b"{\"symbols\": [\"TEST:AAA\"]"))   # 닫는 괄호 없음

            async def publish_stream():
                nonlocal seq
                for i in range(1, 201):
                    pub.publish(encode(MSG_TRADE, seq, trade("AAA" if i % 2 else "BBB", i)))
                    seq += 1
                    if i % 50 == 0:
                        pub.publish(heartbeat(seq, 1)); seq += 1
                    if i % 10 == 0:
                        await asyncio.sleep(0.005)

            producer = asyncio.create_task(publish_stream())
            await asyncio.to_thread(col.collect, 8.0, 100)
            await producer
            # 관리 포트에 쓰레기를 보내도 게이트웨이가 죽지 않는다 (리뷰 지적 1: substr 예외로 terminate)
            for junk in (b"\r\n\r\n", b"GET\r\n\r\n", b"GET /healthz\r\n\r\n", b"\x00\xff junk\r\n\r\n", b"POST /healthz HTTP/1.1\r\n\r\n"):
                with socket.create_connection(("127.0.0.1", gw.admin), timeout=2) as a:
                    a.sendall(junk)
                    a.settimeout(2)
                    try:
                        a.recv(1024)
                    except (socket.timeout, TimeoutError):
                        pass
            assert gw.proc.poll() is None, "쓰레기 HTTP 요청에 게이트웨이가 죽었다"
            health_live = gw.health()
            _, metrics = gw.get("/metrics")
            _, subs = gw.get("/subscribers")
            col.close()
            await _wait(lambda: gw.health()["subscribers"] == 0, 3, "client disconnect to be noticed")
            return col, health_live, metrics, json.loads(subs), gw.stop(), gw
        finally:
            gw.proc.kill()
            await pub.close()

    col, health, metrics, subs, rc, gw = asyncio.run(main())

    # 1) 스냅샷이 먼저, 메타가 그 다음, 증분은 메타 뒤에
    assert {t for t, _ in col.snapshots} <= {MSG_TRADE, MSG_BOOK}
    assert len(col.snapshots) == 2, col.snapshots            # 캐시된 키 2개 (AAA 는 호가가 체결을 덮음)
    assert col.meta and col.meta["snapshot_count"] == 2 and col.meta["next_seq"] == 0
    assert col.first_incremental_after_meta is True

    # 2) 필터: 증분 체결은 AAA 만
    assert len(col.trades("AAA")) >= 100 and not col.trades("BBB")
    assert col.heartbeats >= 1                                # 하트비트는 필터 없이 전원에게

    # 3) 재번호: 절반이 걸러졌는데도 구독자 seq 는 연속
    s = col.track.stats()
    assert s["lost_messages"] == 0 and s["gap_count"] == 0 and s["duplicate_count"] == 0, s
    assert col.parser.crc_error_count == 0 and col.parser.resync_count == 0

    # 4) 헬스·지표가 파이썬 게이트웨이와 같은 이름
    assert health["healthy"] is True and health["impl"] == "c++" and health["frames_in"] >= 200
    assert health["subscribers"] == 1 and health["total_dropped"] == 0
    assert health["sources"][0]["connected"] is True and health["degraded_sources"] == []
    for name in ("mdfeed_sent_total", "mdfeed_frames_in_total", "mdfeed_dropped_total", "mdfeed_subscribers"):
        assert f'{name}{{service="tcp-gateway"}}' in metrics, metrics
    assert subs["count"] == 1 and subs["items"][0]["symbols"] == ["TEST:AAA"] and subs["items"][0]["mode"] == "stream"

    # 5) SIGTERM 에 0 으로 내려간다
    assert rc == 0, gw._stderr[-5:]


def test_slow_subscriber_is_isolated_and_cut(gateway_bin):
    """안 읽는 구독자 하나가 발행을 멈추지 못한다. 한도를 넘으면 그 구독자만 끊긴다."""
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        # 큐 2048(기본) + 커널 송신버퍼를 넘긴 뒤 100건 더 버리면 끊는다.
        # 송신버퍼는 16KB 로 고정한다 — 리눅스 루프백은 자동조정으로 수 MB 까지 커져서
        # 20,000프레임(1.76MB)이 통째로 커널에 들어가 드롭이 안 났다 (CI 첫 실행에서 실제로 그랬다).
        gw = Gateway(gateway_bin, bus_path, MDFEED_DROP_LIMIT=100, MDFEED_TCP_SNDBUF=16384)
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway on bus")
            slow = Collector(gw.port, rcvbuf=4096)          # 접속만 하고 절대 안 읽는다
            fast = Collector(gw.port)
            await _wait(lambda: gw.health()["subscribers"] == 2, 3, "two clients")
            seq = 0
            n_frames = 20000

            async def paced():
                # 초당 약 2.5만 건. 읽는 쪽(파이썬 스레드)이 따라올 수 있는 속도여야
                # "느린 구독자만" 끊기는지 볼 수 있다. 폭주시키면 둘 다 끊긴다.
                nonlocal seq
                for i in range(n_frames):
                    pub.publish(encode(MSG_TRADE, seq, trade("AAA", i))); seq += 1
                    if i % 50 == 0:
                        await asyncio.sleep(0.002)

            producer = asyncio.create_task(paced())
            await asyncio.to_thread(fast.collect, 15.0, n_frames)
            await producer
            await _wait(lambda: gw.health()["subscribers"] == 1, 5, "slow subscriber to be cut")
            h = gw.health()
            _, metrics = gw.get("/metrics")
            slow_closed = _drained_to_eof(slow.sock)         # 서버가 끊었으니 버퍼를 비우면 EOF
            slow.close(); fast.close()
            return fast, h, metrics, pub.dropped, slow_closed, n_frames
        finally:
            gw.proc.kill()
            await pub.close()

    fast, health, metrics, bus_dropped, slow_closed, n_frames = asyncio.run(main())
    assert len(fast.trades()) == n_frames                  # 빠른 구독자는 전부 받았다
    assert fast.track.stats()["lost_messages"] == 0        # 한 건도 안 잃었다
    assert health["subscribers"] == 1 and slow_closed      # 느린 쪽만 끊겼다
    dropped = [l for l in metrics.splitlines() if l.startswith("mdfeed_dropped_total")]
    assert dropped and float(dropped[0].split()[-1]) > 100 # 누적 드롭이 한도를 넘었다
    assert bus_dropped == 0, "버스(발행자) 쪽은 밀리면 안 된다 — 게이트웨이가 상류를 막았다"


def test_conflate_mode_sends_latest_only_with_contiguous_seq(gateway_bin):
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        gw = Gateway(gateway_bin, bus_path)
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway on bus")
            # 수신 버퍼를 줄여 소켓이 막히게 한다. 커널이 다 흡수하면 합칠 일이 없다 —
            # 실제로 2,000건(176KB)은 게이트웨이가 한 번도 막히지 않고 전부 내보냈다.
            col = Collector(gw.port, symbols=["TEST:AAA", "TEST:BBB"], mode="CONFLATE", rcvbuf=4096)   # 대소문자 무시 (파이썬 lower())
            await _wait(lambda: gw.health()["subscribers"] == 1, 3, "client")
            await asyncio.sleep(0.1)                         # 구독 요청이 처리될 시간
            _, subs = gw.get("/subscribers")
            assert json.loads(subs)["items"][0]["mode"] == "conflate"
            n = 20000
            for i in range(n):                               # 클라이언트가 읽기 전에 몰아 발행
                pub.publish(encode(MSG_TRADE, i, trade("AAA", i, px=1000.0 + i)))
            pub.publish(encode(MSG_TRADE, n, trade("BBB", 0, px=5.0)))
            for k in range(3):                                   # 키 없는 프레임도 conflate 구독자에게 가야 한다 (리뷰 지적 2)
                pub.publish(heartbeat(n + 1 + k, 1))
            await asyncio.sleep(0.5)
            await asyncio.to_thread(col.collect, 3.0)
            _, subs = gw.get("/subscribers")
            col.close()
            return col, json.loads(subs)["items"][0]
        finally:
            gw.proc.kill()
            await pub.close()

    col, info = asyncio.run(main())
    aaa = col.trades("AAA")
    assert aaa and aaa[-1].price == 1000.0 + 19999         # 마지막 값은 반드시 도착
    assert len(aaa) < 20000                                 # 합쳐졌다
    assert info["conflated"] > 0 and info["conflated"] + len(aaa) == 20000
    assert col.trades("BBB")[-1].price == 5.0
    assert col.heartbeats == 3                              # 하트비트는 합치지 않고 전부 전달
    assert col.track.stats()["lost_messages"] == 0          # 합쳐진 건 갭이 아니다


def test_unresolvable_host_fails_fast(gateway_bin):
    """호스트를 못 풀면 0.0.0.0 으로 조용히 떨어지지 말고 실패해야 한다 (리뷰 지적 4)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("MDFEED_")}
    env.update({"MDFEED_BUS_PATH": "/tmp/none.sock", "MDFEED_TCP_HOST": "not-a-host.invalid", "MDFEED_TCP_PORT": "0", "MDFEED_TCP_ADMIN_PORT": "0"})
    proc = subprocess.run([str(gateway_bin)], env=env, capture_output=True, text=True, timeout=5)
    assert proc.returncode != 0 and "호스트를 해석할 수 없습니다" in proc.stderr
    assert '"event":"listening"' not in proc.stdout


def test_localhost_binds_loopback(gateway_bin):
    run = bus_dir()
    gw = Gateway(gateway_bin, os.path.join(run, "bus.sock"), MDFEED_TCP_HOST="localhost", MDFEED_HTTP_HOST="localhost")
    try:
        assert gw.get("/readyz")[0] == 200
    finally:
        gw.proc.kill()


def test_fanout_is_fair_across_subscribers(gateway_bin):
    """접속 순서가 지연 우선순위를 정하면 안 된다.

    순차 팬아웃을 늘 같은 순서로 돌면 먼저 접속한 구독자가 구조적으로 유리하다. 게이트웨이
    안에서 재보니 기울기가 완벽한 직선이었다(2026-09-23, 구독자 100명): 배치 시작 → send() 완료가
    첫 구독자 6µs, 마지막 390µs, 최대/최소 62배. 시작점을 배치마다 밀면 1.0배가 된다.

    이 시험은 **게이트웨이 자신의 지표**로 본다. 클라이언트가 잰 p99 는 잡음이 커서 380µs 효과를
    가렸고, 그래서 한 번은 "회전은 효과가 없다"는 틀린 결론을 냈다. 잴 수 있는 자리에서 잰다.
    """
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")
    n_subs = 24

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        gw = Gateway(gateway_bin, bus_path)
        cols = []
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway on bus")
            for _ in range(n_subs):
                cols.append(Collector(gw.port))
            await _wait(lambda: gw.health()["subscribers"] == n_subs, 5, "all subscribers")

            seq = 0
            for batch in range(60):          # 배치를 여러 번 만들어야 회전이 한 바퀴 돈다
                for _ in range(20):
                    pub.publish(encode(MSG_TRADE, seq, trade("AAA", seq))); seq += 1
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.5)
            for c in cols:                   # 소켓을 비워 둬야 전송이 막히지 않는다
                await asyncio.to_thread(c.collect, 0.2)
            _, subs = gw.get("/subscribers")
            _, metrics = gw.get("/metrics")
            return json.loads(subs)["items"], metrics
        finally:
            for c in cols:
                c.close()
            gw.proc.kill()
            await pub.close()

    items, metrics = asyncio.run(main())
    delays = [(i["id"], i["mean_send_delay_us"]) for i in items if i["mean_send_delay_us"] > 0]
    assert len(delays) >= n_subs // 2, items
    lo = min(d for _, d in delays)
    hi = max(d for _, d in delays)
    spread = hi / lo if lo > 0 else float("inf")
    assert spread < 3.0, f"팬아웃이 기울었다 — 최대/최소 {spread:.1f}배: {sorted(delays)[:5]} … {sorted(delays)[-3:]}"
    # 지표로도 같은 값이 나와야 한다 (알람 FanoutUnfair 가 이걸 본다)
    line = [l for l in metrics.splitlines() if l.startswith("mdfeed_fanout_delay_spread")]
    assert line, metrics
    assert float(line[0].split()[-1]) < 3.0, line


def _connect_probe(port: int, stop: threading.Event, result: dict) -> None:
    """교체 중에도 새 접속이 되는지 계속 두드린다. 실패 횟수가 이 시험의 값이다."""
    ok = fail = 0
    errs: list[str] = []
    while not stop.is_set():
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            ok += 1
        except OSError as e:
            fail += 1
            if len(errs) < 3:
                errs.append(f"{type(e).__name__}: {e}")
        time.sleep(0.01)
    result.update(ok=ok, fail=fail, errors=errs)


def test_zero_downtime_replacement(gateway_bin):
    """게이트웨이를 교체하는 동안 새 접속이 실패하지 않는다.

    예전엔 재시작 = 포트가 닫히는 구간이었다. 지금은 새 프로세스가 같은 포트를 SO_REUSEPORT 로
    함께 듣고, 옛 프로세스는 SIGTERM 에 **리스너만 닫고** 기존 구독자에게 계속 배포하다 끝낸다.

    확인하는 것:
      1. 교체 내내 connect() 실패 0
      2. 옛 프로세스에 붙어 있던 구독자는 드레인 동안 계속 받는다 (유실 0)
      3. 드레인 중 /readyz 는 503 (로드밸런서가 빼라는 신호)
    """
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")
    port = free_tcp_port()

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        old = Gateway(gateway_bin, bus_path, MDFEED_TCP_PORT=port, MDFEED_DRAIN_SECONDS=3)
        new = None
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "old gateway on bus")
            col = Collector(old.port)
            await _wait(lambda: old.health()["subscribers"] == 1, 3, "subscriber on old")

            stop = threading.Event()
            probe: dict = {}
            th = threading.Thread(target=_connect_probe, args=(old.port, stop, probe), daemon=True)
            th.start()

            seq = 0

            async def produce(n: int):
                nonlocal seq
                for _ in range(n):
                    pub.publish(encode(MSG_TRADE, seq, trade("AAA", seq))); seq += 1
                    await asyncio.sleep(0.002)

            await produce(100)
            # 새 프로세스가 같은 포트에 합류
            new = Gateway(gateway_bin, bus_path, MDFEED_TCP_PORT=port, MDFEED_DRAIN_SECONDS=3)
            await _wait(lambda: pub.subscriber_count == 2, 5, "new gateway on bus")
            await produce(100)

            old.proc.send_signal(signal.SIGTERM)          # 옛 프로세스 드레인 시작
            await asyncio.sleep(0.3)
            ready_status = None
            try:
                old.get("/readyz")
                ready_status = 200
            except urllib.error.HTTPError as e:
                ready_status = e.code
            except Exception:                              # noqa: BLE001  — 관리 리스너는 이미 닫혔다
                ready_status = "closed"

            await produce(300)                             # 드레인 동안에도 계속 발행
            await asyncio.to_thread(col.collect, 1.0)
            drained_trades = len(col.trades())
            rc = old.proc.wait(timeout=15)                 # 드레인 기한 안에 스스로 끝나야 한다
            stop.set(); th.join(timeout=5)

            # 교체가 끝난 뒤에도 새 프로세스가 받는다
            after = Collector(new.port)
            await _wait(lambda: new.health()["subscribers"] >= 1, 5, "subscriber on new")
            after.close()
            col.close()
            return probe, rc, drained_trades, col.track.stats(), ready_status
        finally:
            if new:
                new.proc.kill()
            old.proc.kill()
            await pub.close()

    probe, rc, drained_trades, seqstats, ready_status = asyncio.run(main())

    assert probe["fail"] == 0, f"교체 중 접속 실패 {probe['fail']}건: {probe.get('errors')}"
    assert probe["ok"] > 20, probe                          # 실제로 두드렸는지
    assert rc == 0                                          # 드레인 뒤 정상 종료
    assert drained_trades > 100, drained_trades             # 드레인 동안에도 계속 받았다
    assert seqstats["lost_messages"] == 0, seqstats         # 그 사이 유실 0
    assert ready_status in (503, "closed"), ready_status     # 드레인 중에는 준비 안 됨
