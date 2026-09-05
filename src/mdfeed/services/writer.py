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

# 삭제가 밀렸을 때 다시 오는 간격. 정상 주기(1시간)를 기다리면
# 첫 삭제가 반나절 걸린다.
RETRY_INTERVAL_S = 60.0


def _iso_us(us):
    if not us:
        return None
    import datetime as _dt
    return _dt.datetime.fromtimestamp(
        us / 1e6, _dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

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
            "rows_pruned_total",
            # 아카이브를 안 켜도 0 으로 존재해야 알람이 평가된다.
            "rows_archived_total")
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
        # 예산에 걸려 다 못 지운 상태. 계속 True 면 유입을 못 따라가는 것이다.
        self.prune_incomplete = False
        self.archived_days = 0
        self.archived_rows = 0
        self.archive_failures: list[str] = []
        # 지금 내보내는 중인 조각. None 이면 쉬는 중이다.
        self.archive_current: str | None = None
        # 목적지에 검증된 아카이브가 이어지는 끝. 프로세스 재기동에도
        # 살아 있는 사실이다(폴더에서 다시 읽는다).
        self.archive_floor_us: int | None = None
        # DB 를 만지는 모든 스레드가 이 락을 통과한다 (위 주석 참고)
        self._db_lock = threading.Lock()
        from ..supervisor import Supervisor
        self.sup = Supervisor(SERVICE, self.registry)
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

    # ── 아카이브 ──────────────────────────────────────────────────────────
    def _archive_once(self) -> dict:
        """아직 안 올린 날들을 내보내고, 목적지에서 다시 읽어 검증한다.

        블로킹이라 스레드에서 돈다. 읽기만 하므로 적재 락은 안 잡는다 —
        SQLite 리더는 스레드별로 따로 열려 있고 WAL 이라 쓰기와 안 부딪친다.
        """
        from .. import archive as ar
        cfg = self.cfg
        out_dir = cfg.archive_dir
        done, failed, rows = [], [], 0
        # 날짜 바깥, 테이블 안쪽으로 돈다. 반대로 하면 trades 를 8일치 다
        # 올린 뒤에야 book_top 첫 날을 시작하는데, 삭제 빗장은 "그 날의 모든
        # 테이블"을 요구하므로 그동안 빗장이 한 발짝도 못 올라간다.
        # 실측으로 그렇게 됐다 — trades 4일치가 올라갔는데 삭제 허용은 계속 0.
        pend = {t: ar.pending_days(self.storage, out_dir, t,
                                   lag_s=cfg.archive_lag_s)
                for t in ar.ARCHIVE_TABLES}
        for day in sorted({d for v in pend.values() for d in v}):
            for table in ar.ARCHIVE_TABLES:
                if day not in pend[table]:
                    continue
                # 조각 하나가 16M 행이면 3분 걸린다. 밀린 날이 7일이면 20분
                # 넘게 도는데, 그동안 헬스가 계속 0 이면 **멈춘 것과 구분이
                # 안 된다.** 지금 무엇을 하는 중인지 먼저 올린다.
                self.archive_current = f"{table}/{day}"
                try:
                    man = ar.export_day(self.storage, table, day, out_dir)
                except Exception as e:                    # noqa: BLE001
                    log.warning("[archive] %s %s 내보내기 실패: %s: %s",
                                table, day, type(e).__name__, e)
                    failed.append(f"{table}/{day}")
                    continue
                path = os.path.join(out_dir, ar._name(table, day))
                if cfg.archive_upload and not man.get("skipped"):
                    if not ar.upload(path, path + ".json", cfg.archive_upload):
                        failed.append(f"{table}/{day}(업로드)")
                        continue
                # 올린 뒤 **다시 읽어** 확인한다. 올렸다는 종료코드 0 은
                # 올라갔다는 증거가 아니다 — 이 프로젝트에서 네 번 난 사고가
                # 전부 "선언은 됐는데 실제로는 안 돌았다"였다.
                if not ar.verify_file(path, man):
                    failed.append(f"{table}/{day}(검증)")
                    continue
                if not man.get("skipped"):
                    done.append(f"{table}/{day}")
                    rows += man.rows
                    # 조각이 끝날 때마다 올린다. 전부 끝난 뒤에 한꺼번에
                    # 올리면 진행 중인 20분이 통째로 안 보인다.
                    self.archived_days += 1
                    self.archived_rows += man.rows
                    self.registry.counter("rows_archived_total",
                                          value=man.rows)
        self.archive_current = None
        return {"archived": done, "failed": failed, "rows": rows}

    def _archive_floor_us(self) -> int | None:
        """지워도 되는 상한. 아카이브를 안 쓰면 None(제한 없음).

        구한 값을 들고 있는다. archived_days 는 이 프로세스가 이번에 만든
        조각 수라, 재기동하면 0 으로 돌아간다 — 폴더에 3일치가 있는데
        헬스가 0 을 내면 "아카이브가 안 된다"로 읽힌다. 오래가는 사실은
        **목적지에 무엇이 검증돼 있는가**이고, 그게 이 값이다.
        """
        cfg = self.cfg
        if not (cfg.archive_dir and cfg.retention_requires_archive):
            self.archive_floor_us = None
            return None
        from .. import archive as ar
        self.archive_floor_us = ar.safe_delete_cutoff_us(cfg.archive_dir)
        return self.archive_floor_us

    def _prune_locked(self, prune_fn) -> dict:
        """락은 prune 이 **배치마다** 잡는다. 여기서 통째로 잡으면 안 된다.

        예전엔 이 함수가 락을 잡고 prune 을 통째로 돌렸다. 그러면 50,000행
        배치로 끊는 코드가 아무 의미가 없다 — 첫 실행에서 554배치가 도는
        동안 적재가 멈추고, 버스가 drop-oldest 라 그만큼 틱이 버려진다.
        """
        return prune_fn(self.storage, self.cfg.retention_days,
                        guard=self._db_lock,
                        budget_s=self.cfg.retention_budget_s,
                        floor_us=self._archive_floor_us())

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
            self.registry.gauge("db_reclaimable_bytes", r["reclaimable_bytes"])
            # 보존이 꺼져 있어도 0 으로 낸다. 조건부로 내면 지표가 없는 동안
            # 알람 평가 자체가 안 돈다 — 바로 위 growth 주석과 같은 이유다.
            # make verify-alerts 가 이걸 잡아 줬다.
            self.registry.gauge("retention_prune_incomplete",
                                1 if self.prune_incomplete else 0)
            self.registry.gauge("archive_enabled", 1 if cfg.archive_dir else 0)
            self.registry.gauge("archive_failed_segments",
                                len(self.archive_failures))
            if r["hours_until_full"] is not None:
                self.registry.gauge("disk_hours_until_full", r["hours_until_full"])
                if r["hours_until_full"] < cfg.disk_warn_hours:
                    log.warning("디스크가 약 %.1f시간 뒤에 찬다 "
                                "(여유 %.1fGB · 증가 %.0fMB/h). 보존 일수를 줄이거나 "
                                "MDFEED_RETENTION_DAYS 를 켜라",
                                r["hours_until_full"], r["disk_free_bytes"] / 1e9,
                                r["growth_mb_per_hour"])

            # 지우기 전에 옮긴다. 순서가 뒤바뀌면 되돌릴 수 없다.
            if cfg.archive_dir and self.storage:
                # 아카이브를 **시작하기 전에** 먼저 상한을 낸다. 끝난 뒤에만
                # 내면, 밀린 날이 많아 한 바퀴가 한 시간 걸리는 동안 헬스가
                # 계속 None 이다 — 목적지에 6일치가 검증돼 있는데도 그랬다.
                # 캐시가 있어 이 계산은 싸다.
                await asyncio.to_thread(self._archive_floor_us)
                r2 = await asyncio.to_thread(self._archive_once)
                if r2["archived"]:
                    # 집계는 _archive_once 가 조각마다 이미 올렸다.
                    log.info("[archive] %d개 조각 · %d행 완료",
                             len(r2["archived"]), r2["rows"])
                self.archive_failures = r2["failed"]
                self.registry.gauge("archive_failed_segments",
                                    len(r2["failed"]))
                if r2["failed"]:
                    log.warning("[archive] 실패 %d건: %s",
                                len(r2["failed"]), ", ".join(r2["failed"][:5]))

            # 아카이브가 새로 만든 조각을 반영해 다시 낸다. 보존이 꺼져
            # 있어도 구해 둔다 — 켜기 전에 "지금 켜면 어디까지 지워지나"를
            # 볼 수 있어야 결정을 내린다.
            if cfg.archive_dir and self.storage:
                await asyncio.to_thread(self._archive_floor_us)

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
                # 예산에 걸려 남긴 상태가 계속되면 삭제가 유입을 못 따라가는
                # 것이다. 그건 보존 일수를 더 줄여야 한다는 신호다.
                self.prune_incomplete = bool(getattr(deleted, "budget_hit", False))

            # 예산에 걸려 남겼으면 한 시간을 기다릴 이유가 없다. 밀린 만큼
            # 빨리 이어서 지운다. 실측 0.63초/배치, 30초 예산이면 주기당 47배치
            # — 첫 삭제 557배치를 1시간 주기로 하면 반나절, 1분 주기면 12분이다.
            wait = min(interval, RETRY_INTERVAL_S) if self.prune_incomplete else interval
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=wait)

    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        return {
            # 태스크별 상태. 합쳐서 세면 하나가 죽어도 안 보인다 —
            # 그게 8/28·8/31·9/1·9/2 사고의 공통점이었다.
            **self.sup.report(),
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
                        "pruned_rows": self.pruned_rows,
                        "prune_incomplete": self.prune_incomplete,
                        "archive_dir": self.cfg.archive_dir or None,
                        "archived_days": self.archived_days,
                        "archived_rows": self.archived_rows,
                        "archive_current": self.archive_current,
                        # 이 프로세스가 만든 조각 수(archived_days)와 달리,
                        # 아래는 목적지에 실제로 검증돼 있는 지점이다.
                        "archive_verified_through": _iso_us(self.archive_floor_us),
                        # 실패가 있으면 삭제도 그 앞에서 멈춘다. 둘을 같이
                        # 봐야 "왜 안 지워지지"가 바로 설명된다.
                        "archive_failed": self.archive_failures},
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
        # 장수 태스크는 전부 감독을 거친다. supervisor.py 참고 —
        # 같은 가족의 사고가 네 번 난 뒤에 만든 계약이다.
        for path in sources:
            self.sup.spawn(f"consume:{os.path.basename(path)}",
                           lambda p=path: self._consume(p, stop), stop)
        self.sup.spawn("flush", lambda: self._flush_loop(stop), stop)
        self.sup.spawn("retention", lambda: self._retention_loop(stop), stop)
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()

        await self.sup.shutdown()
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
