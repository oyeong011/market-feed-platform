"""전 구간 통합 테스트 — 네트워크 없이 수집→버스→게이트웨이→클라이언트→DB 를 검증한다.

CI 는 거래소에 붙을 수 없다(불안정하고, 지역 제한이 있고, 남의 서버다).
그래서 합성 틱으로 MDFP 녹화 파일을 만들고 리플레이 어댑터로 흘려보낸다.
실제 운영 경로와 **같은 코드**가 돌기 때문에, 파이프라인이 끊기면 여기서 잡힌다.

검증 항목
---------
1. 수집기가 버스에 발행하고 게이트웨이가 TCP 로 배포한다
2. 구독자가 받는 seq 가 연속이다 (구독자별 재넘버링이 동작)
3. 스냅샷이 증분보다 먼저 온다
4. writer 가 DB에 적재하고 1분봉을 만든다
5. 전략 엔진이 봉을 닫고 시그널 버스에 발행한다
6. SIGTERM 상당의 종료에서 데이터 유실 없이 정리된다
"""
import asyncio
import json
import os
import socket
import tempfile
import time

import pytest

from mdfeed.config import Config
from mdfeed.models import (MSG_BOOK, MSG_SNAPSHOT, MSG_TRADE, BookTop, Trade)
from mdfeed.protocol import FLAG_SNAPSHOT, FrameParser, SequenceTracker, encode
from mdfeed.services.feedd import FeedDaemon
from mdfeed.services.strategy import StrategyEngine
from mdfeed.services.tcp_gateway import TCPGateway
from mdfeed.services.writer import Writer

BASE_NS = 1_700_000_000_000_000_000
N_TICKS = 1200


def make_replay_file(path: str) -> int:
    """합성 틱으로 녹화 파일을 만든다. 결정론적이라 실패가 재현된다."""
    import math
    frames = []
    seq = 0
    for i in range(N_TICKS):
        ts = BASE_NS + i * 100_000_000            # 100ms 간격 → 총 120초 = 2분봉
        px = 100.0 + math.sin(i / 25.0) * 12.0 + (i % 7) * 0.3
        t = Trade("TEST", "SYNTH", ts, ts + 500_000, px, 0.01 + (i % 5) * 0.001, 1)
        frames.append(encode(MSG_TRADE, seq, t.pack())); seq += 1
        if i % 10 == 0:
            b = BookTop("TEST", "SYNTH", ts, ts + 500_000,
                        px - 0.05, 1.0, px + 0.05, 1.0)
            frames.append(encode(MSG_BOOK, seq, b.pack())); seq += 1
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"".join(frames))
    return seq


def make_cfg(tmp_path) -> Config:
    cfg = Config()
    # UDS 경로는 104바이트 제한이 있어 pytest 의 긴 tmp_path 를 쓸 수 없다.
    # 소켓만 짧은 경로에 두고 나머지 산출물은 tmp_path 에 둔다.
    run = tempfile.mkdtemp(prefix="mdf", dir="/tmp")
    cfg.run_dir = run
    cfg.bus_path = os.path.join(run, "bus.sock")
    cfg.signal_bus_path = os.path.join(run, "signals.sock")
    cfg.adapters = ["replay"]
    cfg.replay_file = str(tmp_path / "replay.mdf")
    cfg.replay_speed = 0.0                  # 최대 속도
    cfg.replay_loop = False
    cfg.ring_enabled = False                # 테스트 간 공유메모리 이름 충돌 회피
    cfg.pg_dsn = ""
    cfg.sqlite_path = str(tmp_path / "e2e.db")
    cfg.http_host = "127.0.0.1"
    cfg.tcp_host = "127.0.0.1"
    # 테스트 전용 포트 (운영 기본값과 겹치지 않게)
    cfg.tcp_port = 19101
    cfg.feedd_admin_port = 19100
    cfg.tcp_admin_port = 19111
    cfg.writer_admin_port = 19104
    cfg.strategy_admin_port = 19105
    cfg.bar_interval_s = 10                 # 2분 데이터에서 여러 봉이 나오게
    cfg.write_flush_s = 0.3
    cfg.write_batch = 100
    cfg.heartbeat_s = 0.5
    cfg.signal_cooldown_s = 0.0
    cfg.strategies = ["sma_cross", "rsi_revert"]
    return cfg


def subscribe_and_collect(port: int, duration: float) -> dict:
    """참조 클라이언트와 같은 방식으로 구독해 결과를 모은다."""
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    parser, track = FrameParser(), SequenceTracker()
    trades = books = snaps = 0
    snapshot_end_seen = False
    first_incremental_after_snapshot = None
    deadline = time.time() + duration
    s.settimeout(0.5)
    while time.time() < deadline:
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            continue
        if not chunk:
            break
        for f in parser.feed(chunk):
            if f.flags & FLAG_SNAPSHOT:
                if f.msg_type == MSG_SNAPSHOT:
                    snapshot_end_seen = True
                else:
                    snaps += 1
                continue
            track.observe(f.seq)
            if first_incremental_after_snapshot is None:
                first_incremental_after_snapshot = snapshot_end_seen
            if f.msg_type == MSG_TRADE:
                trades += 1
            elif f.msg_type == MSG_BOOK:
                books += 1
    s.close()
    return {"trades": trades, "books": books, "snapshots": snaps,
            "snapshot_end_seen": snapshot_end_seen,
            "incremental_after_snapshot": first_incremental_after_snapshot,
            "seq": track.stats(),
            "crc_errors": parser.crc_error_count,
            "resyncs": parser.resync_count}


def test_full_pipeline(tmp_path):
    cfg = make_cfg(tmp_path)
    written = make_replay_file(cfg.replay_file)
    assert written > N_TICKS

    async def main():
        stop = asyncio.Event()
        feed = FeedDaemon(cfg)
        gw = TCPGateway(cfg)
        wr = Writer(cfg)
        st = StrategyEngine(cfg)

        tasks = [asyncio.create_task(feed.run(stop))]
        await asyncio.sleep(0.6)                 # 버스 소켓 생성 대기
        tasks += [asyncio.create_task(gw.run(stop)),
                  asyncio.create_task(wr.run(stop)),
                  asyncio.create_task(st.run(stop))]
        await asyncio.sleep(1.0)                 # 게이트웨이 리스닝 대기

        client = asyncio.create_task(
            asyncio.to_thread(subscribe_and_collect, cfg.tcp_port, 4.0))
        result = await client

        health = {"feedd": feed.health(), "gateway": gw.health(),
                  "writer": wr.health(), "strategy": st.health()}
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        return result, health

    result, health = asyncio.run(main())

    # 1) 데이터가 끝까지 흘렀다
    assert result["trades"] > 100, f"체결이 배포되지 않음: {result}"

    # 2) 구독자 시퀀스가 연속이다 (구독자별 재넘버링 검증)
    assert result["seq"]["lost_messages"] == 0, (
        f"시퀀스 갭 발생 — 게이트웨이 재넘버링 결함: {result['seq']}")
    assert result["seq"]["gap_count"] == 0

    # 3) 프레이밍 무결성
    assert result["crc_errors"] == 0 and result["resyncs"] == 0

    # 4) 스냅샷이 증분보다 먼저 왔다
    assert result["snapshot_end_seen"] is True
    assert result["incremental_after_snapshot"] is True

    # 5) 전 구성요소가 정상 판정
    for name, h in health.items():
        assert h["healthy"] is True, f"{name} unhealthy: {h}"

    # 6) DB 에 실제로 적재됐다
    import sqlite3
    conn = sqlite3.connect(cfg.sqlite_path)
    n_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    n_bars = conn.execute("SELECT COUNT(*) FROM bars_1m").fetchone()[0]
    bar = conn.execute("SELECT open, high, low, close, volume, tick_count "
                       "FROM bars_1m ORDER BY bucket LIMIT 1").fetchone()
    conn.close()
    assert n_trades > 100, f"DB 적재 실패: {n_trades}행"
    assert n_bars >= 1, "1분봉이 만들어지지 않음"
    o, h_, l_, c, vol, ticks = bar
    assert l_ <= o <= h_ and l_ <= c <= h_, f"OHLC 관계 위반: {bar}"
    assert vol > 0 and ticks > 0

    # 7) 전략 엔진이 봉을 닫았다
    assert health["strategy"]["bars_closed"] > 0


def test_replay_adapter_disabled_without_file(tmp_path):
    """녹화 파일이 없으면 어댑터는 스스로 비활성화되고, 그 사유가 노출돼야 한다."""
    from mdfeed.adapters import build
    cfg = make_cfg(tmp_path)
    cfg.replay_file = str(tmp_path / "없는파일.mdf")
    active, inactive = build(["replay"], cfg, lambda m: None)
    assert active == []
    assert "녹화 파일 없음" in inactive[0]["reason"]


def test_graceful_shutdown_flushes_pending_rows(tmp_path):
    """종료 시 버퍼가 flush 되지 않으면 배포마다 데이터가 샌다."""
    cfg = make_cfg(tmp_path)
    cfg.write_flush_s = 60.0        # 주기 flush 가 절대 안 오게 해서 종료 flush 만 검증
    cfg.write_batch = 10 ** 9
    make_replay_file(cfg.replay_file)

    async def main():
        stop = asyncio.Event()
        feed, wr = FeedDaemon(cfg), Writer(cfg)
        tasks = [asyncio.create_task(feed.run(stop))]
        await asyncio.sleep(0.6)
        tasks.append(asyncio.create_task(wr.run(stop)))
        await asyncio.sleep(2.0)
        pending = len(wr._trades)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        return pending

    pending = asyncio.run(main())
    assert pending > 0, "테스트 전제 실패: 종료 전 버퍼에 쌓인 게 없다"

    import sqlite3
    conn = sqlite3.connect(cfg.sqlite_path)
    n = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    conn.close()
    assert n >= pending, f"종료 flush 누락: 대기 {pending}행 중 {n}행만 저장"


def test_replay_does_not_pollute_latency_metrics(tmp_path):
    """리플레이는 지연·시계 지표에 끼어들면 안 된다.

    녹화 시각과 현재 시각의 차이는 시계 오차도 네트워크 지연도 아니다.
    CI 에서 실제로 "시계 오프셋 2,343,288ms" 경고가 떠 빌드가 실패했다.
    """
    from mdfeed.adapters import build
    from mdfeed.adapters.base import CLOCK
    from mdfeed.adapters.replay import ReplayAdapter
    from mdfeed.metrics import Registry

    cfg = make_cfg(tmp_path)
    make_replay_file(cfg.replay_file)
    reg = Registry("t")
    before = set(CLOCK.report())

    (adapter,), _ = build(["replay"], cfg, lambda m: None, reg)
    assert isinstance(adapter, ReplayAdapter)
    assert adapter.measures_latency is False

    t = Trade("TEST", "SYNTH", BASE_NS, BASE_NS + 10 ** 15, 1.0, 1.0)
    adapter._mark(t)

    # 시계 감시기에 REPLAY 항목이 새로 생기지 않아야 한다
    assert set(CLOCK.report()) == before
    # 틱 카운터는 정상 증가
    assert reg.snapshot()["counters"].get('ticks_total{venue="REPLAY"}') == 1
    # 지연 히스토그램에는 기록되지 않아야 한다
    assert "ingest_latency" not in reg.snapshot()["histograms"]
