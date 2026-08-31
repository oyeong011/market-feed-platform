"""writer — 버스를 구독해 DB에 적재하고 1분봉을 집계하는 프로세스.

    UDS 버스 ─▶ 배치 버퍼 ─▶ executemany ─▶ SQLite/Postgres
                    └────▶ 1분봉 누적 ─▶ 버킷 종료 시 upsert

설계 판단
---------
* **배치 적재**: 틱마다 INSERT+COMMIT 하면 초당 수백 건에서 이미 fsync 가 병목이다.
  500건 또는 2초 중 먼저 오는 쪽에 밀어 넣는다.
* **DB I/O 를 스레드로 분리**: sqlite3/psycopg2 는 블로킹이다. 이벤트 루프에서
  직접 부르면 그동안 버스 수신이 멈춰 큐가 넘친다. asyncio.to_thread 로 뺀다.
* **호가는 샘플링**: BookTop 은 체결보다 훨씬 자주 바뀐다. 전부 적재하면 용량이
  수십 배가 되는데, 스프레드 통계는 1초 샘플로 충분하다. 심볼당 1초 1건만 남긴다.
* **지연시간은 보정해서 적재**: 거래소 시계와 우리 시계가 어긋나면 원시 지연이
  음수로 나온다(실측: 바이낸스 -30ms). 음수 지연이 든 테이블은 어떤 SQL 집계를
  해도 결론이 틀린다. clock.ClockMonitor 로 보정한 값을 넣되, ts 와 recv_ts 를
  둘 다 보관하므로 원시값은 언제든 다시 계산할 수 있다.
* **종료 시 flush**: SIGTERM 을 받으면 버퍼에 남은 것을 반드시 쓰고 내려간다.
  안 그러면 정상 배포마다 최대 2초치 데이터가 사라진다.

DB 접근 직렬화 (실제로 세그폴트를 낸 문제)
------------------------------------------
`asyncio.to_thread` 로 띄운 작업은 **await 를 취소해도 스레드가 멈추지 않는다.**
종료 시 태스크를 cancel 하고 곧바로 커넥션을 닫으면, 아직 executemany 를 돌고 있는
워커 스레드 밑에서 sqlite3 객체가 해제돼 use-after-free 로 프로세스가 죽는다.
통합 테스트를 전체로 돌렸을 때 실제로 재현됐다.

그래서 DB 를 만지는 모든 경로(주기 flush, 배치 flush, 종료 close)가 하나의
threading.Lock 을 통과하게 했다. 종료 시 close 는 진행 중인 flush 가 끝날 때까지
자연스럽게 대기한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import threading
import time

from ..bus import UDSSubscriber
from ..clock import ClockMonitor
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import (MSG_BOOK, MSG_SIGNAL, MSG_TRADE, Bar, BookTop, Signal, Trade)
from ..protocol import SequenceTracker
from ..storage.db import open_storage

log = logging.getLogger("mdfeed.writer")
SERVICE = "writer"

BOOK_SAMPLE_S = 1.0


class Writer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "gap_messages_total", "rows_written_total", "bars_written_total",
            "db_errors_total",
            # 보존이 꺼져 있어도 0 으로 존재해야 한다. 사건이 나야 생기는
            # 지표에는 그 전에 알람을 걸 수 없다.
            "rows_pruned_total")
        self.storage = None
        # 샤드마다 seq 공간이 독립이다. 하나로 추적하면 샤드 전환마다
        # 거짓 갭이 잡힌다 — 구독자별 재넘버링 때 겪은 것과 같은 문제다.
        self.seqtracks: dict[str, SequenceTracker] = {}
        self._trades: list[tuple] = []
        self._books: list[tuple] = []
        self._signals: list[tuple] = []
        self._bars: dict[tuple, Bar] = {}          # (venue,symbol,bucket) → Bar
        self._last_book_at: dict[str, float] = {}
        self.rows_written = 0
        self.bars_written = 0
        self.frames_in = 0
        self.last_frame_at = 0.0
        self.upstream_ok = False
        self.db_errors = 0
        self.clock = ClockMonitor()
        from ..retention import DiskWatch
        self.disk = DiskWatch(cfg.sqlite_path)
        self.pruned_rows = 0
        self.prune_runs = 0
        # DB 를 만지는 모든 스레드가 이 락을 통과한다 (위 주석 참고)
        self._db_lock = threading.Lock()
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()

    # ── 수신 ──────────────────────────────────────────────────────────────
    def _ingest(self, frame, source: str = "-") -> None:
        self.frames_in += 1
        self.last_frame_at = time.time()
        self.upstream_ok = True
        tracker = self.seqtracks.get(source)
        if tracker is None:
            tracker = self.seqtracks[source] = SequenceTracker()
        lost = tracker.observe(frame.seq)
        if lost:
            self.registry.counter("gap_messages_total", lost)
            log.warning("시퀀스 갭 %d건 (%s seq=%d). 그 구간 데이터는 영구 유실",
                        lost, source, frame.seq)

        mt = frame.msg_type
        if mt == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
            t = Trade.unpack(frame.payload)
            ts_us = t.ts_event_ns // 1000
            lat = int(self.clock.observe(t.venue, t.latency_us))
            self._trades.append((ts_us, t.venue, t.symbol, t.price, t.qty, t.side,
                                 t.ts_recv_ns // 1000, lat, frame.seq))
            self._accumulate_bar(t)
        elif mt == MSG_BOOK and len(frame.payload) >= BookTop.SIZE:
            b = BookTop.unpack(frame.payload)
            key = f"{b.venue}:{b.symbol}"
            now = time.time()
            if now - self._last_book_at.get(key, 0.0) < BOOK_SAMPLE_S:
                return                              # 샘플링으로 솎아낸다
            self._last_book_at[key] = now
            self._books.append((b.ts_event_ns // 1000, b.venue, b.symbol,
                                b.bid, b.bid_qty, b.ask, b.ask_qty, b.spread_bp))
        elif mt == MSG_SIGNAL and len(frame.payload) >= Signal.SIZE:
            s = Signal.unpack(frame.payload)
            self._signals.append((s.ts_ns // 1000, s.venue, s.symbol, s.strategy,
                                  s.action, s.strength, s.ref_price))

    def _accumulate_bar(self, t: Trade) -> None:
        iv = self.cfg.bar_interval_s
        bucket_ns = (t.ts_event_ns // (iv * 1_000_000_000)) * (iv * 1_000_000_000)
        key = (t.venue, t.symbol, bucket_ns)
        bar = self._bars.get(key)
        if bar is None:
            bar = self._bars[key] = Bar(t.venue, t.symbol, bucket_ns, iv)
        bar.update(t)

    # ── 적재 ──────────────────────────────────────────────────────────────
    def _flush_sync(self) -> tuple[int, int]:
        """블로킹 DB I/O. 반드시 스레드에서 호출한다."""
        with self._db_lock:
            return self._flush_locked()

    def _flush_locked(self) -> tuple[int, int]:
        trades, self._trades = self._trades, []
        books, self._books = self._books, []
        sigs, self._signals = self._signals, []

        # 이미 끝난 버킷의 봉만 확정한다. 진행 중인 봉은 아직 갱신될 수 있다
        iv_ns = self.cfg.bar_interval_s * 1_000_000_000
        cutoff = (time.time_ns() // iv_ns) * iv_ns
        done = [k for k in self._bars if k[2] < cutoff]
        bar_rows = []
        for k in done:
            b = self._bars.pop(k)
            bar_rows.append((b.bucket_ns // 1000, b.venue, b.symbol, b.open, b.high,
                             b.low, b.close, b.volume, b.notional, b.vwap, b.tick_count))

        n = 0
        n += self.storage.insert_trades(trades)
        n += self.storage.insert_book(books)
        n += self.storage.insert_signals(sigs)
        nb = self.storage.upsert_bars(bar_rows)
        return n, nb

    async def _flush(self) -> None:
        if not (self._trades or self._books or self._signals or self._bars):
            return
        try:
            n, nb = await asyncio.to_thread(self._flush_sync)
        except Exception as e:                      # noqa: BLE001
            self.db_errors += 1
            self.registry.counter("db_errors_total")
            log.error("적재 실패: %s: %s", type(e).__name__, e)
            return
        self.rows_written += n
        self.bars_written += nb
        if n or nb:
            self.registry.counter("rows_written_total", n)
            self.registry.counter("bars_written_total", nb)

    async def _flush_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(self.cfg.write_flush_s)
            await self._flush()

    async def _consume(self, path: str, stop: asyncio.Event) -> None:
        sub = UDSSubscriber(path, name=SERVICE)
        async for frame in sub.frames():
            if stop.is_set():
                return
            self._ingest(frame, path)
            # 배치가 다 차면 주기를 기다리지 않고 바로 민다
            if len(self._trades) >= self.cfg.write_batch:
                await self._flush()

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def _close_storage(self) -> None:
        """진행 중인 flush 가 끝난 뒤에만 커넥션을 닫는다."""
        with self._db_lock:
            self.storage.close()

    def _prune_locked(self, prune_fn) -> dict:
        with self._db_lock:
            return prune_fn(self.storage, self.cfg.retention_days)

    async def _retention_loop(self, stop: asyncio.Event) -> None:
        """디스크를 재고, 보존 기간이 지난 원시 데이터를 지운다.

        지우는 것보다 재는 게 먼저다. 보존을 꺼 둬도(기본) 디스크가 언제
        차는지는 항상 봐야 한다 — 차고 나서 알면 이미 프로세스가 죽어 있다.
        """
        from ..retention import prune
        cfg = self.cfg
        interval = max(cfg.retention_interval_s, 60.0)
        while not stop.is_set():
            self.disk.sample()
            r = self.disk.report()
            self.registry.gauge("db_bytes", r["db_bytes"])
            self.registry.gauge("disk_free_bytes", r["disk_free_bytes"])
            # 증가율은 항상 낸다. 조건부로 내면 "안 늘고 있다"와 "계측이 안 된다"가
            # 구분되지 않고, 알람은 지표가 없는 동안 평가 자체가 안 된다.
            self.registry.gauge("db_growth_bytes_per_hour",
                                self.disk.growth_bytes_per_hour())
            if r["hours_until_full"] is not None:
                self.registry.gauge("disk_hours_until_full", r["hours_until_full"])
                if r["hours_until_full"] < cfg.disk_warn_hours:
                    log.warning("디스크가 약 %.1f시간 뒤에 찬다 "
                                "(여유 %.1fGB · 증가 %.0fMB/h). 보존 일수를 줄이거나 "
                                "MDFEED_RETENTION_DAYS 를 켜라",
                                r["hours_until_full"], r["disk_free_bytes"] / 1e9,
                                r["growth_mb_per_hour"])

            if cfg.retention_days > 0 and self.storage:
                # 삭제는 블로킹이다. 이벤트 루프에서 직접 돌리면 적재가 멈춘다.
                # flush 와 같은 락을 거친다. 같은 SQLite 연결을 두 스레드가
                # 동시에 만지면 락 경합으로 적재가 밀리고, 최악엔 깨진다.
                deleted = await asyncio.to_thread(self._prune_locked, prune)
                n = sum(deleted.values())
                if n:
                    self.pruned_rows += n
                    self.registry.counter("rows_pruned_total", value=n)
                self.prune_runs += 1

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval)

    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        return {
            "service": SERVICE,
            "healthy": (self.upstream_ok and (age is None or age < 30.0)
                        and self.db_errors < 10),
            "uptime_s": round(time.time() - self._started, 1),
            "backend": self.storage.kind if self.storage else None,
            "upstream_connected": self.upstream_ok,
            "last_frame_age_s": round(age, 1) if age is not None else None,
            "frames_in": self.frames_in,
            "rows_written": self.rows_written,
            "bars_written": self.bars_written,
            "pending_rows": len(self._trades) + len(self._books),
            "open_bars": len(self._bars),
            "db_errors": self.db_errors,
            "storage": {**self.disk.report(),
                        "retention_days": self.cfg.retention_days,
                        "pruned_rows": self.pruned_rows},
            "sequence": {k: v.stats() for k, v in self.seqtracks.items()},
            "clock": self.clock.report(),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        self.storage = await asyncio.to_thread(open_storage, cfg)
        log.info("저장소 백엔드: %s", self.storage.kind)

        http = HTTPServer(cfg.http_host, cfg.writer_admin_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.route("GET", "/counts", lambda r: Response.json(self.storage.counts()))
        await http.start()

        sources = list(cfg.bus_paths or [cfg.bus_path]) + [cfg.signal_bus_path]
        log.info("구독 대상 %d개: %s", len(sources),
                 ", ".join(os.path.basename(p) for p in sources))
        tasks = [asyncio.create_task(self._consume(p, stop)) for p in sources] + [
            asyncio.create_task(self._flush_loop(stop)),
            asyncio.create_task(self._retention_loop(stop)),
        ]
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # 종료 전 마지막 flush. 진행 중인 봉도 이때는 확정해서 쓴다.
        # cancel 된 태스크의 워커 스레드가 아직 DB 를 만지고 있을 수 있으므로
        # 이 아래 모든 DB 접근은 _db_lock 을 거친다.
        log.info("종료 전 잔여 버퍼 flush...")
        await self._flush()
        try:
            rows = [(b.bucket_ns // 1000, b.venue, b.symbol, b.open, b.high, b.low,
                     b.close, b.volume, b.notional, b.vwap, b.tick_count)
                    for b in self._bars.values()]
            if rows:
                def _final():
                    with self._db_lock:
                        return self.storage.upsert_bars(rows)
                await asyncio.to_thread(_final)
                log.info("진행 중이던 봉 %d개 확정", len(rows))
        except Exception as e:                      # noqa: BLE001
            log.error("종료 flush 실패: %s", e)
        await http.close()
        await asyncio.to_thread(self._close_storage)
        log.info("종료. 총 %d행 / %d봉 적재", self.rows_written, self.bars_written)


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    return runtime.run(SERVICE, Writer(cfg).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
