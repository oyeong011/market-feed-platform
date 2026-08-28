"""rest-api — 적재된 데이터를 조회하는 HTTP API (실시간 경로와 분리).

실시간은 ws-gateway / tcp-gateway 가, **과거 조회는 이 프로세스가** 맡는다.
분리한 이유는 부하 특성이 정반대이기 때문이다. 실시간 배포는 작은 메시지를 아주
자주, 과거 조회는 큰 결과를 가끔 낸다. 한 프로세스에 섞으면 무거운 조회 한 방이
실시간 배포의 p99 를 통째로 밀어 올린다.

DB 호출은 전부 asyncio.to_thread 로 뺀다 — sqlite3/psycopg2 는 블로킹이라
이벤트 루프에서 직접 부르면 그 시간 동안 다른 요청이 전부 멈춘다.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

from ..httpd import HTTPServer, Request, Response, health_routes
from ..metrics import Registry
from ..storage.db import open_storage

log = logging.getLogger("mdfeed.rest_api")
SERVICE = "rest-api"


class RestAPI:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.storage = None
        self.db_errors = 0
        # 조회 스레드가 DB 를 읽는 중에 종료가 커넥션을 닫으면 세그폴트가 난다.
        # writer 와 같은 이유로 직렬화한다.
        self._db_lock = threading.Lock()
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()

    def _call_locked(self, fn, *a, **kw):
        with self._db_lock:
            return fn(*a, **kw)

    async def _q(self, fn, *a, **kw):
        try:
            return await asyncio.to_thread(self._call_locked, fn, *a, **kw)
        except Exception as e:                       # noqa: BLE001
            self.db_errors += 1
            self.registry.counter("db_errors_total")
            raise

    # ── 라우트 ────────────────────────────────────────────────────────────
    async def symbols(self, _req: Request) -> Response:
        rows = await self._q(self.storage.symbols)
        return Response.json({"count": len(rows), "items": rows})

    async def quotes(self, req: Request) -> Response:
        # 걸러내기를 SQL 로 내린다. 전부 읽은 뒤 파이썬에서 거르면
        # symbol 하나를 물어도 비용이 전체 조회와 같고, limit 도 거르기 전에
        # 잘려서 원하는 종목이 빠질 수 있다.
        rows = await self._q(self.storage.latest,
                             req.q_int("limit", 200, 1, 2000),
                             req.query.get("venue"), req.query.get("symbol"))
        return Response.json({"ts": time.time(), "count": len(rows), "items": rows})

    async def bars(self, req: Request) -> Response:
        venue, sym = req.query.get("venue"), req.query.get("symbol")
        if not venue or not sym:
            return Response.json({"error": "venue 와 symbol 이 필요합니다"}, 400)
        rows = await self._q(self.storage.bars, venue.upper(), sym,
                             req.q_int("limit", 200, 1, 5000))
        rows.reverse()                               # 차트용으로 오름차순
        return Response.json({"venue": venue.upper(), "symbol": sym,
                              "count": len(rows), "items": rows})

    async def trades(self, req: Request) -> Response:
        venue, sym = req.query.get("venue"), req.query.get("symbol")
        if not venue or not sym:
            return Response.json({"error": "venue 와 symbol 이 필요합니다"}, 400)
        rows = await self._q(self.storage.trades, venue.upper(), sym,
                             req.q_int("limit", 100, 1, 2000))
        return Response.json({"venue": venue.upper(), "symbol": sym,
                              "count": len(rows), "items": rows})

    async def signals(self, req: Request) -> Response:
        p = self.storage.placeholder
        limit = req.q_int("limit", 50, 1, 500)
        rows = await self._q(
            self.storage.query,
            f"SELECT ts, venue, symbol, strategy, action, strength, ref_price "
            f"FROM signals ORDER BY ts DESC LIMIT {limit}")
        return Response.json({"count": len(rows), "items": rows})

    async def stats(self, _req: Request) -> Response:
        counts = await self._q(self.storage.counts)
        return Response.json({
            "backend": self.storage.kind,
            "counts": counts,
            "uptime_s": round(time.time() - self._started, 1),
        })

    def health(self) -> dict:
        return {"service": SERVICE, "healthy": self.db_errors < 20,
                "backend": self.storage.kind if self.storage else None,
                "uptime_s": round(time.time() - self._started, 1),
                "db_errors": self.db_errors}

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        self.storage = await asyncio.to_thread(open_storage, cfg)
        http = HTTPServer(cfg.http_host, cfg.http_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.route("GET", "/api/v1/symbols", self.symbols)
        http.route("GET", "/api/v1/quotes", self.quotes)
        http.route("GET", "/api/v1/bars", self.bars)
        http.route("GET", "/api/v1/trades", self.trades)
        http.route("GET", "/api/v1/signals", self.signals)
        http.route("GET", "/api/v1/stats", self.stats)
        http.route("GET", "/api/v1", lambda r: Response.json({
            "service": SERVICE,
            "endpoints": {
                "GET /api/v1/symbols": "적재된 종목 목록",
                "GET /api/v1/quotes?venue=&symbol=&limit=": "최신 시세",
                "GET /api/v1/bars?venue=&symbol=&limit=": "1분봉 (오름차순)",
                "GET /api/v1/trades?venue=&symbol=&limit=": "최근 체결",
                "GET /api/v1/signals?limit=": "전략 시그널",
                "GET /api/v1/stats": "적재 통계",
            }}))
        await http.start()
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()
        await http.close()
        await asyncio.to_thread(self._call_locked, self.storage.close)


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    return runtime.run(SERVICE, RestAPI(cfg).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
