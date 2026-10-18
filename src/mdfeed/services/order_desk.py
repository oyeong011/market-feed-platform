"""주문 데스크 — 웹 MTS · 주문 · 원장 대사 (선택 서비스, MDFEED_ORDER_DESK_ENABLED=1).

    브라우저(휴대폰) ──HTTPS(TLS 1.2+)·세션쿠키──▶ order-desk :8443 ──HTTPS──▶ 한국투자증권
                                                   │  원장(SQLite, write-ahead)
                                                   └─ 대사 루프: 체결합계 → 원장, 보유수량 ↔ 원장
    Prometheus ──HTTP──▶ :9140 /healthz /metrics (관리 포트는 다른 서비스와 같은 규약)

기동 조건이 안 맞으면 **조용히 낮춰 뜨지 않고 실패한다.**
인증서·비밀번호 해시·자격증명이 없으면 시작하지 않는다. 실전(real)은
MDFEED_ORDER_ALLOW_REAL=1 이 같이 있어야 한다. 평문 HTTP 로 주문을 받는 경로는 없다.

주문 한 건의 순서 (원장 규칙은 ledger.py)
  1. 같은 client_order_id 가 있으면 그 결과를 돌려준다 (이중 클릭·재시도)
  2. 같은 종목·방향에 열린 주문이 있으면 거절 (체결 배분이 추측이 되지 않게)
  3. 금액 한도: 건당 ORDER_MAX_KRW, 하루 ORDER_DAILY_MAX_KRW. 시장가는 현재가로 추정
  4. 그 종목·방향의 증권사 합계 기준선이 없으면 먼저 잡는다 (못 잡으면 보내지 않는다)
  5. PENDING_SUBMIT 기록 → 전송 → ACCEPTED / REJECTED / UNKNOWN
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import re
import ssl
import time
from pathlib import Path

from .. import auth, ledger as L
from ..adapters.kis import KST, market_is_open
from ..httpd import HTTPServer, Request, Response, health_routes
from ..kis_trading import THROTTLED, KISTrading, Rejected, Uncertain
from ..metrics import Registry

SERVICE = "order-desk"
log = logging.getLogger(SERVICE)
WEB_DIR = Path(__file__).resolve().parents[3] / "web" / "mts"
COOKIE = "desk_session"
SYMBOL_RE = re.compile(r"^\d{6}$")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
# 현재가는 주문·대사와 같은 초당 호출 한도를 나눠 쓴다. 화면(탭)이 몇 개든 종목당 이 간격에 한 번만 부른다.
# 모의는 한도가 초당 1건이라, 화면이 2초마다 부르면 그것만으로 예산의 절반을 쓴다.
QUOTE_TTL_S = {"mock": 5.0, "real": 1.0}

SECURITY_HEADERS = {
    "Strict-Transport-Security": "max-age=31536000",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": ("default-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
                                "form-action 'self'"),
    "Cache-Control": "no-store",
}


class StartupError(SystemExit):
    pass


def trade_date(now: dt.datetime | None = None) -> str:
    return (now or dt.datetime.now(KST)).strftime("%Y%m%d")


def make_tls_context(cert: str, key: str) -> ssl.SSLContext:
    for p in (cert, key):
        if not os.path.isfile(p):
            raise StartupError(f"TLS 파일이 없습니다: {p} — `make desk-cert` 로 개발용 인증서를 만드세요")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert, key)
    return ctx


def make_client(cfg) -> KISTrading:
    if cfg.order_env == "real":
        if not cfg.order_allow_real:
            raise StartupError("MDFEED_ORDER_ENV=real 은 MDFEED_ORDER_ALLOW_REAL=1 이 함께 있어야 합니다")
        key, secret, acct = (os.getenv("KIS_APP_KEY", ""), os.getenv("KIS_APP_SECRET", ""),
                             os.getenv("KIS_ACCOUNT", ""))
    elif cfg.order_env == "mock":
        key, secret, acct = (os.getenv("KIS_MOCK_APP_KEY", ""), os.getenv("KIS_MOCK_APP_SECRET", ""),
                             os.getenv("KIS_MOCK_ACCOUNT", ""))
    else:
        raise StartupError(f"MDFEED_ORDER_ENV 는 mock 또는 real: {cfg.order_env!r}")
    if not (key and secret and acct):
        prefix = "KIS_" if cfg.order_env == "real" else "KIS_MOCK_"
        raise StartupError(f"{prefix}APP_KEY / {prefix}APP_SECRET / {prefix}ACCOUNT 가 필요합니다")
    try:
        return KISTrading(cfg.order_env, key, secret, acct, base=cfg.order_kis_base or None)
    except ValueError as e:
        raise StartupError(str(e)) from None


class OrderDesk:
    def __init__(self, cfg, client: KISTrading | None = None, ledger: L.Ledger | None = None,
                 tls: ssl.SSLContext | None = None):
        self.cfg = cfg
        self.tls = tls or make_tls_context(cfg.desk_tls_cert, cfg.desk_tls_key)
        if not cfg.desk_password_hash.startswith("pbkdf2_sha256$"):
            raise StartupError("MDFEED_DESK_PASSWORD_HASH 가 없습니다 — `python -m mdfeed.auth hash` 로 만드세요")
        self.client = client or make_client(cfg)
        self.ledger = ledger or L.Ledger(cfg.ledger_path)
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "login_failures_total", "reconcile_runs_total", "reconcile_errors_total",
            orders_total=[{"result": r} for r in ("accepted", "rejected", "unknown", "blocked")],
            kis_throttled_total=[{"code": c} for c in sorted(THROTTLED)])
        self.client.observer = self._kis_event
        self.quote_ttl_s = QUOTE_TTL_S[cfg.order_env]
        self._quotes: dict[str, tuple[float, dict]] = {}
        self._quote_inflight: dict[str, asyncio.Task] = {}
        self.sessions = auth.SessionStore()
        self.throttle = auth.LoginThrottle()
        self._submit_lock = asyncio.Lock()
        self._kick = asyncio.Event()
        self._seen_breaks: set[tuple] = set()
        self.breaks: list[dict] = []           # 두 번 연속 보인 불일치만 (일시적 지연을 거른다)
        self.last_reconcile: dict = {}
        self.balance_cache: dict | None = None
        self.started = time.time()
        recovered = self.ledger.recover_pending()
        if recovered:
            log.warning("보내던 중 끊긴 주문 %d건을 UNKNOWN 으로 바꿨다 — 다시 보내지 않고 대사로 판정한다",
                        recovered)

    # ── 인증 ──────────────────────────────────────────────────────────────
    def _authed(self, req: Request) -> bool:
        return self.sessions.check(auth.cookie_value(req.headers.get("cookie", ""), COOKIE))

    def guarded(self, fn, write: bool = False):
        """세션 확인. 쓰기 요청은 사용자 정의 헤더까지 요구한다(CSRF).

        다른 사이트의 form 이나 img 는 이 헤더를 붙일 수 없고, fetch 로 붙이려면 사전요청(preflight)이
        나가는데 이 서버는 CORS 를 열지 않는다. SameSite=Strict 쿠키와 겹으로 막는다.
        """
        async def _h(req: Request):
            if write and req.headers.get("x-desk-request") != "1":
                return Response.json({"error": "csrf"}, 403)
            if not self._authed(req):
                return Response.json({"error": "login required"}, 401)
            r = fn(req)
            return await r if asyncio.iscoroutine(r) else r
        return _h

    async def login(self, req: Request) -> Response:
        if req.headers.get("x-desk-request") != "1":
            return Response.json({"error": "csrf"}, 403)
        wait = self.throttle.locked_for()
        if wait > 0:
            return Response.json({"error": "locked", "retry_after_s": round(wait)}, 429)
        try:
            pw = json.loads(req.body or b"{}").get("password", "")
        except ValueError:
            return Response.json({"error": "bad json"}, 400)
        ok = await asyncio.to_thread(auth.verify_password, str(pw), self.cfg.desk_password_hash)
        if not ok:
            self.throttle.fail()
            self.registry.counter("login_failures_total")
            log.warning("로그인 실패")
            return Response.json({"error": "wrong password"}, 401)
        self.throttle.success()
        tok = self.sessions.create()
        return Response(200, json.dumps({"ok": True}), "application/json; charset=utf-8", headers={
            "Set-Cookie": f"{COOKIE}={tok}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={auth.ABSOLUTE_TTL_S}"})

    def logout(self, req: Request) -> Response:
        self.sessions.revoke(auth.cookie_value(req.headers.get("cookie", ""), COOKIE))
        return Response(200, json.dumps({"ok": True}), "application/json; charset=utf-8", headers={
            "Set-Cookie": f"{COOKIE}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0"})

    def session(self, req: Request) -> Response:
        return Response.json({"authenticated": self._authed(req), "env": self.cfg.order_env,
                              "account": self.client.account_label})

    def _kis_event(self, kind: str, value: str) -> None:
        if kind == "call":
            self.registry.counter("kis_calls_total", tr=value)
        elif kind == "throttled":
            self.registry.counter("kis_throttled_total", code=value)

    # ── 조회 ──────────────────────────────────────────────────────────────
    async def price(self, sym: str) -> dict:
        """종목당 quote_ttl_s 안에서는 증권사를 한 번만 부른다. 동시에 온 요청은 같은 호출을 기다린다."""
        hit = self._quotes.get(sym)
        if hit and time.monotonic() - hit[0] < self.quote_ttl_s:
            return hit[1]
        task = self._quote_inflight.get(sym)
        if task is None:
            task = asyncio.ensure_future(self.client.price(sym))
            self._quote_inflight[sym] = task
            task.add_done_callback(lambda _t: self._quote_inflight.pop(sym, None))
        q = {**await asyncio.shield(task), "as_of": time.time()}
        self._quotes[sym] = (time.monotonic(), q)
        return q

    async def quote(self, req: Request) -> Response:
        sym = req.query.get("symbol", "")
        if not SYMBOL_RE.match(sym):
            return Response.json({"error": "종목코드는 숫자 6자리"}, 400)
        try:
            q = await self.price(sym)
        except (Rejected, Uncertain) as e:
            return Response.json({"error": str(e)}, 502)
        # 몇 초 묵은 값인지 서버 시계로 알려 준다(휴대폰 시계와 어긋나도 맞게)
        return Response.json({**q, "age_s": round(time.time() - q["as_of"], 1)})

    async def balance(self, _req: Request) -> Response:
        # 대사가 매 주기 잔고를 새로 받아 둔다. 화면 요청마다 증권사를 부르면 한도를 나눠 먹는다(결함 52).
        if self.balance_cache is None:
            try:
                self.balance_cache = {**await self.client.balance(), "as_of": time.time()}
            except (Rejected, Uncertain) as e:
                return Response.json({"error": str(e)}, 502)
        return Response.json(self.balance_cache)

    def orders(self, req: Request) -> Response:
        return Response.json({"trade_date": trade_date(),
                              "orders": self.ledger.orders(limit=req.q_int("limit", 50, 1, 500))})

    def reconcile_status(self, _req: Request) -> Response:
        return Response.json({**self.last_reconcile, "breaks": self.breaks,
                              "events": self.ledger.events(20)})

    # ── 주문 ──────────────────────────────────────────────────────────────
    async def submit(self, req: Request) -> Response:
        try:
            b = json.loads(req.body or b"{}")
            cid, sym, side = str(b["client_order_id"]), str(b["symbol"]), str(b["side"])
            qty, price = int(b["qty"]), int(b.get("price", 0))
        except (ValueError, KeyError, TypeError):
            return Response.json({"error": "client_order_id·symbol·side·qty 가 필요합니다"}, 400)
        if not CLIENT_ID_RE.match(cid) or not SYMBOL_RE.match(sym) or side not in ("buy", "sell"):
            return Response.json({"error": "형식이 틀렸습니다"}, 400)
        if not (1 <= qty <= self.cfg.order_max_qty) or price < 0:
            return Response.json({"error": f"수량은 1~{self.cfg.order_max_qty}"}, 400)

        async with self._submit_lock:           # 열린 주문 검사와 기록 사이에 다른 주문이 끼지 않게
            day = trade_date()
            existing = self.ledger.by_client_id(cid)
            if existing:
                return Response.json({"order": existing, "duplicate": True})
            if self.ledger.open_order(day, sym, side):
                return self._blocked("같은 종목·방향에 아직 열린 주문이 있습니다", 409)
            try:
                unit = price or (await self.price(sym))["price"]
            except (Rejected, Uncertain) as e:
                return self._blocked(f"현재가를 못 가져와 금액 한도를 확인할 수 없습니다: {e}", 503)
            notional = unit * qty
            if notional > self.cfg.order_max_krw:
                return self._blocked(f"건당 한도 {self.cfg.order_max_krw:,}원 초과 (약 {notional:,}원)", 422)
            used = self.ledger.notional_today(day)
            if used + notional > self.cfg.order_daily_max_krw:
                return self._blocked(f"하루 한도 {self.cfg.order_daily_max_krw:,}원 초과 "
                                     f"(오늘 {used:,}원 + 이번 {notional:,}원)", 422)
            if self.ledger.totals_baseline(day, sym, side) is None:
                try:
                    self.ledger.set_totals_baseline(day, sym, side,
                                                    await self.client.day_totals(day, sym, side))
                except (Rejected, Uncertain) as e:
                    return self._blocked(f"대사 기준선을 못 잡아 보내지 않았습니다: {e}", 503)

            order, _ = self.ledger.new_order(cid, day, sym, side, qty, price, est_price=unit)
            try:
                r = await self.client.order(side, sym, qty, price)
            except Rejected as e:
                order = self.ledger.mark(order["id"], L.REJECTED, message=str(e))
                self.registry.counter("orders_total", result="rejected")
                return Response.json({"order": order}, 422)
            except Uncertain as e:
                order = self.ledger.mark(order["id"], L.UNKNOWN, message=str(e))
                self.registry.counter("orders_total", result="unknown")
                log.error("주문 결과 불확실 — 재전송하지 않는다 (id=%d): %s", order["id"], e)
                self._kick.set()
                return Response.json({"order": order}, 202)
            order = self.ledger.mark(order["id"], L.ACCEPTED, odno=r["odno"], message=r["msg"])
            self.registry.counter("orders_total", result="accepted")
            log.info("주문 접수 %s %s %d주 %s · 주문번호 %s", side, sym, qty,
                     "시장가" if price == 0 else f"{price:,}원", r["odno"])
        self._kick.set()
        return Response.json({"order": order}, 201)

    def _blocked(self, msg: str, status: int) -> Response:
        self.registry.counter("orders_total", result="blocked")
        return Response.json({"error": msg}, status)

    # ── 대사 ──────────────────────────────────────────────────────────────
    async def reconcile_once(self) -> list[dict]:
        day = trade_date()
        if not self.ledger.has_holdings_baseline(day):
            bal = await self.client.balance()
            self.ledger.set_holdings_baseline(day, {h["symbol"]: h["qty"] for h in bal["holdings"]})
        found: list[dict] = []
        for sym, side in self.ledger.keys_to_watch(day):
            found += self.ledger.apply_totals(day, sym, side,
                                              await self.client.day_totals(day, sym, side))
        bal = await self.client.balance()
        self.balance_cache = {**bal, "as_of": time.time()}
        found += self.ledger.reconcile_holdings(
            day, {h["symbol"]: h["qty"] for h in bal["holdings"]})
        return found

    def _confirm(self, found: list[dict]) -> None:
        keys = {tuple(sorted(b.items())) for b in found}
        self.breaks = [dict(k) for k in keys & self._seen_breaks]
        self._seen_breaks = keys
        self.registry.gauge("reconcile_breaks", float(len(self.breaks)))

    async def reconcile_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                found = await self.reconcile_once()
                self._confirm(found)
                self.registry.counter("reconcile_runs_total")
                self.registry.gauge("reconcile_last_success_timestamp_seconds", time.time())
                self.last_reconcile = {"at": time.time(), "trade_date": trade_date(),
                                       "candidates": found}
                if self.breaks:
                    log.warning("원장 불일치 %d건: %s", len(self.breaks), self.breaks)
            except (Rejected, Uncertain) as e:
                self.registry.counter("reconcile_errors_total")
                log.warning("대사 실패: %s", e)
            except Exception:                          # noqa: BLE001
                self.registry.counter("reconcile_errors_total")
                log.exception("대사 루프 예외")
            unknown = sum(1 for o in self.ledger.orders(trade_date(), 500) if o["state"] == L.UNKNOWN)
            self.registry.gauge("orders_unknown", float(unknown))
            interval = self.cfg.reconcile_open_s if market_is_open() else self.cfg.reconcile_closed_s
            self._kick.clear()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wait_any(stop), timeout=interval)

    async def _wait_any(self, stop: asyncio.Event) -> None:
        kick, halt = asyncio.ensure_future(self._kick.wait()), asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({kick, halt}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            kick.cancel()
            halt.cancel()

    def health(self) -> dict:
        last = self.last_reconcile.get("at", 0)
        return {"service": SERVICE, "env": self.cfg.order_env,
                "healthy": bool(last) and time.time() - last < 3 * self.cfg.reconcile_closed_s,
                "reconcile_breaks": len(self.breaks), "sessions": len(self.sessions),
                "kis_calls": self.client.calls, "uptime_s": round(time.time() - self.started, 1)}

    # ── 수명주기 ──────────────────────────────────────────────────────────
    def build_servers(self, tls: ssl.SSLContext) -> tuple[HTTPServer, HTTPServer]:
        cfg = self.cfg
        web = HTTPServer(cfg.desk_host, cfg.desk_port, SERVICE, self.registry, ssl_context=tls,
                         cors=False, default_headers=SECURITY_HEADERS)
        web.route("POST", "/api/login", self.login)
        web.route("POST", "/api/logout", self.guarded(self.logout, write=True))
        web.route("GET", "/api/session", self.session)
        web.route("GET", "/api/quote", self.guarded(self.quote))
        web.route("GET", "/api/balance", self.guarded(self.balance))
        web.route("GET", "/api/orders", self.guarded(self.orders))
        web.route("POST", "/api/orders", self.guarded(self.submit, write=True))
        web.route("GET", "/api/reconcile", self.guarded(self.reconcile_status))
        web.static("/", str(WEB_DIR))
        admin = HTTPServer(cfg.http_host, cfg.desk_admin_port, SERVICE, self.registry)
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        health_routes(admin, self.health, tracker=self.tracker)
        return web, admin

    async def run(self, stop: asyncio.Event) -> None:
        web, admin = self.build_servers(self.tls)
        await admin.start()
        await web.start()
        log.info("주문 데스크 env=%s 계좌=%s https://%s:%d", self.cfg.order_env,
                 self.client.account_label, self.cfg.desk_host, self.cfg.desk_port)
        from ..runtime import sample_resources
        tasks = [asyncio.create_task(self.reconcile_loop(stop)),
                 asyncio.create_task(sample_resources(self.tracker, stop))]
        await stop.wait()
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        await web.close()
        await admin.close()
        self.ledger.close()


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    try:
        desk = OrderDesk(cfg)
    except StartupError as e:
        logging.basicConfig()
        log.error("기동 거부: %s", e)
        return 2
    return runtime.run(SERVICE, desk.run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
