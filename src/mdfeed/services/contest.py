"""모의투자대회 — 참가자 여러 명이 가상 자금으로 겨루는 웹 MTS (선택 서비스, MDFEED_CONTEST_ENABLED=1).

    참가자 휴대폰 ──HTTPS·세션쿠키──▶ contest :8444 ──HTTPS(시세만)──▶ 한국투자증권 현재가
                                         │  대회 장부(SQLite, 해시 체인 거래 기록)
                                         └─ 대사 루프: 잔고표 ↔ 거래 기록. 어긋나면 주문 정지
    Prometheus ──HTTP──▶ :9150 /healthz /metrics

증권사로 주문을 보내지 않는다. 시장가 주문은 그 순간 증권사 현재가로 대회 장부 안에서 체결된다.
장부 규칙은 contest.py 에 있다. 여기서는 접속·권한·거래 시간·운영(정지·재개)을 맡는다.

참가는 운영자가 만든 초대 코드로만 한다. 코드 하나에 한 명이다.
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

from .. import auth
from ..adapters.kis import KST, market_is_open
from ..contest import ContestError, ContestLedger
from ..httpd import HTTPServer, Request, Response, health_routes
from ..kis_trading import THROTTLED, KISTrading, Rejected, Uncertain
from ..metrics import Registry
from .order_desk import SECURITY_HEADERS, StartupError, make_tls_context

SERVICE = "contest"
log = logging.getLogger(SERVICE)
WEB_DIR = Path(__file__).resolve().parents[3] / "web" / "contest"
COOKIE, ADMIN_COOKIE = "contest_session", "contest_admin"
SYMBOL_RE = re.compile(r"^\d{6}$")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
NICK_RE = re.compile(r"^[0-9A-Za-z가-힣_]{2,16}$")
MIN_PASSWORD = 10
QUOTE_TTL_S = 1.0                 # 체결가로도 쓰므로 짧게. 실전 시세 키는 초당 20건까지 된다
INTEGRITY_INTERVAL_S = 10.0
VALUATION_INTERVAL_S = 15.0


def make_quote_client(cfg) -> KISTrading:
    prefix = "KIS_" if cfg.contest_quote_env == "real" else "KIS_MOCK_"
    key, secret = os.getenv(prefix + "APP_KEY", ""), os.getenv(prefix + "APP_SECRET", "")
    if not (key and secret):
        raise StartupError(f"시세용 {prefix}APP_KEY / {prefix}APP_SECRET 가 필요합니다")
    try:
        return KISTrading(cfg.contest_quote_env, key, secret, "", base=cfg.contest_kis_base or None)
    except ValueError as e:
        raise StartupError(str(e)) from None


class Contest:
    def __init__(self, cfg, quotes: KISTrading | None = None, ledger: ContestLedger | None = None,
                 tls: ssl.SSLContext | None = None):
        self.cfg = cfg
        self.tls = tls or make_tls_context(cfg.desk_tls_cert, cfg.desk_tls_key)
        if not cfg.contest_admin_hash.startswith("pbkdf2_sha256$"):
            raise StartupError("MDFEED_CONTEST_ADMIN_HASH 가 없습니다 — `python -m mdfeed.auth hash` 로 만드세요")
        self.quotes = quotes or make_quote_client(cfg)
        self.ledger = ledger or ContestLedger(cfg.contest_db, cfg.contest_initial_cash,
                                              cfg.contest_fee_ppm, cfg.contest_tax_ppm)
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "login_failures_total", "contest_quote_errors_total",
            contest_orders_total=[{"result": r} for r in ("filled", "rejected", "blocked")],
            kis_throttled_total=[{"code": c} for c in sorted(THROTTLED)])
        self.quotes.observer = self._kis_event
        self.sessions = auth.SessionStore()
        self.admin_sessions = auth.SessionStore()
        self.throttles: dict[str, auth.LoginThrottle] = {}     # 닉네임별. 한 명이 남의 계정을 잠그지 못하게
        self.admin_throttle = auth.LoginThrottle()
        self._quotes: dict[str, tuple[float, dict]] = {}
        self._inflight: dict[str, asyncio.Task] = {}
        self.prices: dict[str, int] = {}
        self.breaks: list[dict] = []
        self.last_integrity = 0.0
        self.last_valuation = 0.0
        self._now = lambda: dt.datetime.now(KST)
        self.started = time.time()

    def _kis_event(self, kind: str, value: str) -> None:
        if kind == "call":
            self.registry.counter("kis_calls_total", tr=value)
        elif kind == "throttled":
            self.registry.counter("kis_throttled_total", code=value)

    # ── 거래 시간 ─────────────────────────────────────────────────────────
    def trading_state(self) -> tuple[bool, str]:
        """(주문을 받는가, 이유). 정지·대회 기간·장 시간 순으로 본다."""
        if self.ledger.halted():
            return False, f"장부 점검으로 주문이 멈춰 있습니다 ({self.ledger.halted()})"
        now = self._now()
        day = now.strftime("%Y%m%d")
        if self.cfg.contest_start and day < self.cfg.contest_start:
            return False, f"대회는 {self.cfg.contest_start} 에 시작합니다"
        if self.cfg.contest_end and day > self.cfg.contest_end:
            return False, "대회가 끝났습니다"
        if not self.cfg.contest_off_hours and not market_is_open(now):
            return False, "정규장(평일 09:00~15:30)에만 주문할 수 있습니다"
        return True, "장외 시험 모드" if self.cfg.contest_off_hours else "주문 가능"

    # ── 시세 ──────────────────────────────────────────────────────────────
    async def price(self, sym: str) -> dict:
        hit = self._quotes.get(sym)
        if hit and time.monotonic() - hit[0] < QUOTE_TTL_S:
            return hit[1]
        task = self._inflight.get(sym)
        if task is None:
            task = asyncio.ensure_future(self.quotes.price(sym))
            self._inflight[sym] = task
            task.add_done_callback(lambda _t: self._inflight.pop(sym, None))
        try:
            q = await asyncio.shield(task)
        except (Rejected, Uncertain):
            self.registry.counter("contest_quote_errors_total")
            raise
        self._quotes[sym] = (time.monotonic(), q)
        if q["price"] > 0:
            self.prices[sym] = q["price"]
        return q

    # ── 인증 ──────────────────────────────────────────────────────────────
    def _me(self, req: Request) -> int | None:
        return self.sessions.subject(auth.cookie_value(req.headers.get("cookie", ""), COOKIE))

    def _is_admin(self, req: Request) -> bool:
        return self.admin_sessions.check(auth.cookie_value(req.headers.get("cookie", ""), ADMIN_COOKIE))

    def guarded(self, fn, write: bool = False, admin: bool = False):
        """세션 확인. 쓰기 요청은 사용자 정의 헤더까지 요구한다(CSRF — order_desk 와 같은 방식)."""
        async def _h(req: Request):
            if write and req.headers.get("x-contest-request") != "1":
                return Response.json({"error": "csrf"}, 403)
            if admin:
                if not self._is_admin(req):
                    return Response.json({"error": "admin login required"}, 401)
                r = fn(req)
            else:
                pid = self._me(req)
                if pid is None:
                    return Response.json({"error": "login required"}, 401)
                r = fn(req, pid)
            return await r if asyncio.iscoroutine(r) else r
        return _h

    @staticmethod
    def _body(req: Request) -> dict:
        try:
            b = json.loads(req.body or b"{}")
        except ValueError:
            return {}
        return b if isinstance(b, dict) else {}

    def _cookie(self, name: str, tok: str, max_age: int) -> dict:
        return {"Set-Cookie": f"{name}={tok}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={max_age}"}

    async def signup(self, req: Request) -> Response:
        if req.headers.get("x-contest-request") != "1":
            return Response.json({"error": "csrf"}, 403)
        b = self._body(req)
        code, nick, pw = str(b.get("invite", "")), str(b.get("nickname", "")), str(b.get("password", ""))
        if not NICK_RE.match(nick):
            return Response.json({"error": "닉네임은 한글·영문·숫자 2~16자"}, 400)
        if len(pw) < MIN_PASSWORD:
            return Response.json({"error": f"비밀번호는 {MIN_PASSWORD}자 이상"}, 400)
        pw_hash = await asyncio.to_thread(auth.hash_password, pw)
        try:
            p = self.ledger.signup(code, nick, pw_hash)
        except ContestError as e:
            return Response.json({"error": str(e)}, 409)
        log.info("참가 %s (참가자 %d명)", nick, self.ledger.participant_count())
        self.registry.gauge("contest_participants", float(self.ledger.participant_count()))
        tok = self.sessions.create(p["id"])
        return Response(201, json.dumps({"ok": True, "nickname": nick}), "application/json; charset=utf-8",
                        headers=self._cookie(COOKIE, tok, auth.ABSOLUTE_TTL_S))

    async def login(self, req: Request) -> Response:
        if req.headers.get("x-contest-request") != "1":
            return Response.json({"error": "csrf"}, 403)
        b = self._body(req)
        nick, pw = str(b.get("nickname", "")), str(b.get("password", ""))
        th = self.throttles.setdefault(nick, auth.LoginThrottle())
        if th.locked_for() > 0:
            return Response.json({"error": "locked", "retry_after_s": round(th.locked_for())}, 429)
        cred = self.ledger.credentials(nick)
        ok = cred is not None and await asyncio.to_thread(auth.verify_password, pw, cred[1])
        if not ok:
            th.fail()
            self.registry.counter("login_failures_total")
            return Response.json({"error": "닉네임 또는 비밀번호가 틀렸습니다"}, 401)
        th.success()
        tok = self.sessions.create(cred[0])
        return Response(200, json.dumps({"ok": True}), "application/json; charset=utf-8",
                        headers=self._cookie(COOKIE, tok, auth.ABSOLUTE_TTL_S))

    def logout(self, req: Request, _pid: int) -> Response:
        self.sessions.revoke(auth.cookie_value(req.headers.get("cookie", ""), COOKIE))
        return Response(200, json.dumps({"ok": True}), "application/json; charset=utf-8",
                        headers=self._cookie(COOKIE, "", 0))

    def session(self, req: Request) -> Response:
        pid = self._me(req)
        open_, why = self.trading_state()
        return Response.json({
            "authenticated": pid is not None,
            "nickname": self.ledger.participant(pid)["nickname"] if pid else None,
            "admin": self._is_admin(req),
            "contest": {"start": self.cfg.contest_start, "end": self.cfg.contest_end,
                        "initial_cash": self.cfg.contest_initial_cash, "trading": open_, "status": why,
                        "fee_pct": self.cfg.contest_fee_ppm / 1e4, "tax_pct": self.cfg.contest_tax_ppm / 1e4}})

    # ── 참가자 ────────────────────────────────────────────────────────────
    async def quote(self, req: Request, _pid: int) -> Response:
        sym = req.query.get("symbol", "")
        if not SYMBOL_RE.match(sym):
            return Response.json({"error": "종목코드는 숫자 6자리"}, 400)
        try:
            q = await self.price(sym)
        except (Rejected, Uncertain) as e:
            return Response.json({"error": str(e)}, 502)
        return Response.json(q)

    def account(self, _req: Request, pid: int) -> Response:
        a = self.ledger.account(pid)
        equity = a["cash"]
        for p in a["positions"]:
            p["price"] = self.prices.get(p["symbol"])
            p["avg_price"] = round(p["cost"] / p["qty"])
            p["value"] = p["qty"] * p["price"] if p["price"] else p["cost"]
            p["pnl"] = p["value"] - p["cost"]
            equity += p["value"]
        return Response.json({**a, "equity": equity,
                              "return_pct": round((equity / self.cfg.contest_initial_cash - 1) * 100, 2)})

    def my_orders(self, req: Request, pid: int) -> Response:
        return Response.json({"orders": self.ledger.orders(pid, req.q_int("limit", 50, 1, 500))})

    def leaderboard(self, _req: Request, _pid: int) -> Response:
        return Response.json({"board": self.ledger.leaderboard(self.prices), "as_of": self.last_valuation})

    async def submit(self, req: Request, pid: int) -> Response:
        b = self._body(req)
        try:
            cid, sym, side, qty = (str(b["client_order_id"]), str(b["symbol"]), str(b["side"]), int(b["qty"]))
        except (KeyError, TypeError, ValueError):
            return Response.json({"error": "client_order_id·symbol·side·qty 가 필요합니다"}, 400)
        if not CLIENT_ID_RE.match(cid) or not SYMBOL_RE.match(sym) or side not in ("buy", "sell"):
            return Response.json({"error": "형식이 틀렸습니다"}, 400)
        if not 1 <= qty <= self.cfg.contest_max_qty:
            return Response.json({"error": f"수량은 1~{self.cfg.contest_max_qty}"}, 400)
        open_, why = self.trading_state()
        if not open_:
            self.registry.counter("contest_orders_total", result="blocked")
            return Response.json({"error": why}, 423 if self.ledger.halted() else 409)
        try:
            price = (await self.price(sym))["price"]
        except (Rejected, Uncertain) as e:
            self.registry.counter("contest_orders_total", result="blocked")
            return Response.json({"error": f"현재가를 못 가져와 체결하지 않았습니다: {e}"}, 503)
        order, created = self.ledger.place(pid, cid, sym, side, qty, price)
        if not created:
            return Response.json({"order": order, "duplicate": True})
        result = "filled" if order["state"] == "FILLED" else "rejected"
        self.registry.counter("contest_orders_total", result=result)
        return Response.json({"order": order}, 201 if result == "filled" else 422)

    # ── 운영자 ────────────────────────────────────────────────────────────
    async def admin_login(self, req: Request) -> Response:
        if req.headers.get("x-contest-request") != "1":
            return Response.json({"error": "csrf"}, 403)
        if self.admin_throttle.locked_for() > 0:
            return Response.json({"error": "locked"}, 429)
        pw = str(self._body(req).get("password", ""))
        if not await asyncio.to_thread(auth.verify_password, pw, self.cfg.contest_admin_hash):
            self.admin_throttle.fail()
            self.registry.counter("login_failures_total")
            log.warning("운영자 로그인 실패")
            return Response.json({"error": "wrong password"}, 401)
        self.admin_throttle.success()
        return Response(200, json.dumps({"ok": True}), "application/json; charset=utf-8",
                        headers=self._cookie(ADMIN_COOKIE, self.admin_sessions.create("admin"),
                                             auth.ABSOLUTE_TTL_S))

    def admin_create_invites(self, req: Request) -> Response:
        n = int(self._body(req).get("n", 1))
        if not 1 <= n <= 100:
            return Response.json({"error": "1~100장"}, 400)
        codes = self.ledger.create_invites(n)
        log.info("초대 코드 %d장 발급", n)
        return Response.json({"codes": codes}, 201)

    def admin_invites(self, _req: Request) -> Response:
        return Response.json({"invites": self.ledger.invites()})

    def admin_status(self, _req: Request) -> Response:
        open_, why = self.trading_state()
        return Response.json({"participants": self.ledger.participant_count(),
                              "orders": self.ledger.order_count(), "halted": self.ledger.halted(),
                              "breaks": self.breaks, "last_integrity": self.last_integrity,
                              "trading": open_, "status": why,
                              "board": self.ledger.leaderboard(self.prices)})

    def admin_orders(self, req: Request) -> Response:
        return Response.json({"orders": self.ledger.orders(None, req.q_int("limit", 200, 1, 5000))})

    def admin_resume(self, _req: Request) -> Response:
        """사람이 원인을 확인한 뒤에만 재개한다. 대사가 아직 어긋나면 거부한다."""
        breaks = self.ledger.integrity()
        if breaks:
            return Response.json({"error": "장부가 아직 어긋나 있습니다", "breaks": breaks}, 409)
        self.ledger.resume()
        self.breaks = []
        self.registry.gauge("contest_halted", 0.0)
        log.warning("운영자가 주문을 재개했다")
        return Response.json({"ok": True})

    # ── 대사·평가 ─────────────────────────────────────────────────────────
    def integrity_once(self) -> list[dict]:
        self.breaks = self.ledger.integrity()
        self.last_integrity = time.time()
        self.registry.gauge("contest_integrity_breaks", float(len(self.breaks)))
        self.registry.gauge("contest_integrity_last_success_timestamp_seconds", self.last_integrity)
        if self.breaks and not self.ledger.halted():
            self.ledger.halt(self.breaks[0]["kind"])
            log.error("장부 불일치 %d건 — 주문을 멈춘다: %s", len(self.breaks), self.breaks)
        self.registry.gauge("contest_halted", 1.0 if self.ledger.halted() else 0.0)
        return self.breaks

    async def valuate_once(self) -> None:
        for sym in self.ledger.held_symbols():
            with contextlib.suppress(Rejected, Uncertain):
                await self.price(sym)
        self.last_valuation = time.time()

    async def _every(self, interval: float, fn, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                r = fn()
                if asyncio.iscoroutine(r):
                    await r
            except Exception:                          # noqa: BLE001
                log.exception("%s 예외", fn.__name__)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval)

    def health(self) -> dict:
        return {"service": SERVICE, "healthy": time.time() - self.last_integrity < 3 * INTEGRITY_INTERVAL_S,
                "halted": self.ledger.halted(), "integrity_breaks": len(self.breaks),
                "participants": self.ledger.participant_count(), "kis_calls": self.quotes.calls,
                "uptime_s": round(time.time() - self.started, 1)}

    # ── 수명주기 ──────────────────────────────────────────────────────────
    def build_servers(self, tls: ssl.SSLContext) -> tuple[HTTPServer, HTTPServer]:
        cfg = self.cfg
        web = HTTPServer(cfg.contest_host, cfg.contest_port, SERVICE, self.registry, ssl_context=tls,
                         cors=False, default_headers=SECURITY_HEADERS)
        g = self.guarded
        web.route("POST", "/api/signup", self.signup)
        web.route("POST", "/api/login", self.login)
        web.route("POST", "/api/logout", g(self.logout, write=True))
        web.route("GET", "/api/session", self.session)
        web.route("GET", "/api/quote", g(self.quote))
        web.route("GET", "/api/account", g(self.account))
        web.route("GET", "/api/orders", g(self.my_orders))
        web.route("POST", "/api/orders", g(self.submit, write=True))
        web.route("GET", "/api/leaderboard", g(self.leaderboard))
        web.route("POST", "/api/admin/login", self.admin_login)
        web.route("POST", "/api/admin/invites", g(self.admin_create_invites, write=True, admin=True))
        web.route("GET", "/api/admin/invites", g(self.admin_invites, admin=True))
        web.route("GET", "/api/admin/status", g(self.admin_status, admin=True))
        web.route("GET", "/api/admin/orders", g(self.admin_orders, admin=True))
        web.route("POST", "/api/admin/resume", g(self.admin_resume, write=True, admin=True))
        web.static("/", str(WEB_DIR))
        admin = HTTPServer(cfg.http_host, cfg.contest_admin_port, SERVICE, self.registry)
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        health_routes(admin, self.health, tracker=self.tracker)
        return web, admin

    async def run(self, stop: asyncio.Event) -> None:
        web, admin = self.build_servers(self.tls)
        await admin.start()
        await web.start()
        self.registry.gauge("contest_participants", float(self.ledger.participant_count()))
        open_, why = self.trading_state()
        log.info("모의투자대회 https://%s:%d · 참가자 %d명 · %s", self.cfg.contest_host,
                 self.cfg.contest_port, self.ledger.participant_count(), why)
        from ..runtime import sample_resources
        tasks = [asyncio.create_task(self._every(INTEGRITY_INTERVAL_S, self.integrity_once, stop)),
                 asyncio.create_task(self._every(VALUATION_INTERVAL_S, self.valuate_once, stop)),
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
        c = Contest(cfg)
    except StartupError as e:
        logging.basicConfig()
        log.error("기동 거부: %s", e)
        return 2
    return runtime.run(SERVICE, c.run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
