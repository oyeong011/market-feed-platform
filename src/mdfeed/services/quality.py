"""quality — 배포되는 데이터가 틀리지 않았는지 계속 검사하는 프로세스.

    UDS 버스 ─▶ 품질 검사 ─▶ 이상 이벤트 ─▶ 로그 · 지표 · DB · /healthz

왜 별도 프로세스인가
--------------------
검사를 수집기(feedd)에 붙이면 hot path 가 무거워지고, 검사 로직에 버그가 나면
수집까지 죽는다. 검사는 **관찰자**여야 한다 — 데이터 흐름을 바꾸지 않고 옆에서 본다.

검사기가 죽어도 시세는 계속 흐른다. 반대로 시세가 멈추면 검사기도 조용해지는데,
그건 이미 feedd 헬스체크가 잡는다.

왜 배포를 막지 않는가
---------------------
이상을 발견해도 **데이터를 버리지 않는다.** 마켓데이터에서 "이상해 보이는 값"의
상당수는 진짜 시장 움직임이다. 급등락, 유동성 고갈, 서킷브레이커.
자동으로 걸러내면 정작 중요한 순간의 데이터를 잃는다.

대신 **표시하고 기록한다.** 판단은 사람이 한다. 이건 마켓데이터 벤더가
'suspect' 플래그를 붙여 내보내되 지우지는 않는 관행과 같다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from ..bus import UDSSubscriber
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import MSG_BOOK, MSG_TRADE, BookTop, Trade
from ..quality import SEV_CRITICAL, QualityMonitor

log = logging.getLogger("mdfeed.quality")
SERVICE = "quality"


class QualityService:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(quality_events_total=[
            {"check": c, "severity": s}
            for c in ("price_jump", "quote_sanity", "stale_value",
                      "bar_integrity", "cross_venue")
            for s in ("CRITICAL", "WARNING")])
        self.monitor = QualityMonitor(cfg)
        self.frames_in = 0
        self.last_frame_at = 0.0
        self.upstream_ok = False
        self.storage = None
        self._pending: list[tuple] = []
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()

    # ── 수신 ──────────────────────────────────────────────────────────────
    def _ingest(self, frame) -> None:
        self.frames_in += 1
        self.last_frame_at = time.time()
        self.upstream_ok = True
        events = []
        if frame.msg_type == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
            t = Trade.unpack(frame.payload)
            events = self.monitor.on_trade(t.venue, t.symbol, t.price, t.ts_event_ns)
        elif frame.msg_type == MSG_BOOK and len(frame.payload) >= BookTop.SIZE:
            b = BookTop.unpack(frame.payload)
            events = self.monitor.on_quote(b.venue, b.symbol, b.bid, b.ask, b.ts_event_ns)
        for ev in events:
            self._on_event(ev)

    def _on_event(self, ev) -> None:
        self.registry.counter("quality_events_total", check=ev.check, severity=ev.severity)
        level = logging.ERROR if ev.severity == SEV_CRITICAL else logging.WARNING
        log.log(level, "[%s] %s %s — %s", ev.check, ev.venue, ev.symbol, ev.detail)
        self._pending.append((ev.ts_ns // 1000, ev.check, ev.severity,
                              ev.venue, ev.symbol, ev.detail, ev.value))

    async def _consume(self, path: str, stop: asyncio.Event) -> None:
        sub = UDSSubscriber(path)
        async for frame in sub.frames():
            if stop.is_set():
                return
            self._ingest(frame)

    async def _gauges(self, stop: asyncio.Event) -> None:
        """검사 결과와 다른 축의 값을 주기적으로 낸다.

        기준가 재설정은 "데이터가 이상하다"가 아니라 "수집이 끊겼다"는 뜻이다.
        같은 quality_events 카운터에 섞으면 두 사건이 구분되지 않는다.
        """
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=5.0)
            self.registry.gauge("price_ref_resets", self.monitor.jump.ref_resets)

    async def _flush_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(3.0)
            await self._flush()

    async def _flush(self) -> None:
        if not self._pending or self.storage is None:
            return
        rows, self._pending = self._pending, []
        p = self.storage.placeholder
        sql = (f"INSERT INTO quality_events "
               f"(ts, check_name, severity, venue, symbol, detail, value) "
               f"VALUES ({p},{p},{p},{p},{p},{p},{p})")
        if self.storage.kind == "postgres":
            sql = sql.replace(f"VALUES ({p},", f"VALUES (to_timestamp({p}/1e6),", 1)
        try:
            await asyncio.to_thread(self.storage._many, sql, rows)
        except Exception as e:                        # noqa: BLE001
            log.error("품질 이벤트 적재 실패: %s", e)

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        rep = self.monitor.report()
        return {
            "service": SERVICE,
            "healthy": self.upstream_ok and (age is None or age < 60.0),
            "uptime_s": round(time.time() - self._started, 1),
            "frames_in": self.frames_in,
            "last_frame_age_s": round(age, 1) if age is not None else None,
            # 이상이 있다고 unhealthy 로 두지 않는다. 검사기는 관찰자이고,
            # 이상 자체는 데이터의 상태이지 이 프로세스의 상태가 아니다.
            "checked": rep["checked"],
            "critical": rep["critical"],
            "warning": rep["warning"],
            "by_check": rep["by_check"],
            "implied_fx_krw_per_usd": rep["implied_fx"],
            "price_ref_resets": rep["price_ref_resets"],
            "pending_writes": len(self._pending),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        from ..storage.db import open_storage
        self.storage = await asyncio.to_thread(open_storage, cfg)
        await asyncio.to_thread(self._ensure_table)

        http = HTTPServer(cfg.http_host, cfg.quality_admin_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.route("GET", "/events", lambda r: Response.json(self.monitor.report()))
        http.route("GET", "/fx", lambda r: Response.json({
            "implied_krw_per_usd": self.monitor.cross.implied_fx(),
            "note": "업비트 원화가 ÷ 바이낸스 달러가. 외부 환율 소스 없이 "
                    "자산별로 뽑은 값이라 서로 벌어지면 한쪽 시세가 이상하다는 뜻."}))
        await http.start()

        sources = list(cfg.bus_paths or [cfg.bus_path])
        log.info("품질 검사 시작 — 구독 %d개", len(sources))
        tasks = [asyncio.create_task(self._consume(p, stop)) for p in sources]
        tasks.append(asyncio.create_task(self._flush_loop(stop)))
        tasks.append(asyncio.create_task(self._gauges(stop)))
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._flush()
        await http.close()
        with contextlib.suppress(Exception):
            self.storage.close()
        rep = self.monitor.report()
        log.info("종료. 검사 %d건 / CRITICAL %d / WARNING %d",
                 rep["checked"], rep["critical"], rep["warning"])

    def _ensure_table(self) -> None:
        ts_type = "TIMESTAMPTZ" if self.storage.kind == "postgres" else "INTEGER"
        num = "DOUBLE PRECISION" if self.storage.kind == "postgres" else "REAL"
        self.storage.query(
            f"CREATE TABLE IF NOT EXISTS quality_events ("
            f" ts {ts_type} NOT NULL, check_name TEXT NOT NULL, severity TEXT NOT NULL,"
            f" venue TEXT, symbol TEXT, detail TEXT, value {num})")
        with contextlib.suppress(Exception):
            self.storage.query(
                "CREATE INDEX IF NOT EXISTS idx_quality_ts ON quality_events (ts DESC)")


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    return runtime.run(SERVICE, QualityService(cfg).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
