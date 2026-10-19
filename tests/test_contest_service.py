"""모의투자대회 통합 시험 — 가짜 시세 서버 + 진짜 TLS.

참가·체결·이중 클릭·거래 시간·장부 불일치 시 정지·운영자 권한·닉네임별 잠금을 재현한다.
"""
import asyncio
import json
import ssl
import urllib.error
import urllib.request
from http.cookiejar import CookieJar

import pytest

from mdfeed import config
from mdfeed.contest import ContestLedger
from mdfeed.httpd import HTTPServer, Response
from mdfeed.kis_trading import KISTrading
from mdfeed.services import contest as cs
from test_order_desk import HASH, PASSWORD, _free_port, at, cert  # noqa: F401  (cert 는 픽스처)

USER_PW = "alice-password-1"


class FakeQuotes:
    def __init__(self, port):
        self.price, self.fail, self.calls = 70_000, False, 0
        self.srv = HTTPServer("127.0.0.1", port, "fake-kis")
        self.srv.route("POST", "/oauth2/tokenP", lambda q: Response.json({"access_token": "t", "expires_in": 86400}))
        self.srv.route("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", self._price)

    def _price(self, q):
        self.calls += 1
        if self.fail:
            return Response.json({"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "시세 장애"})
        return Response.json({"rt_cd": "0", "output": {"stck_prpr": str(self.price), "prdy_vrss": "0",
                                                       "prdy_ctrt": "0"}})


class Phone:
    def __init__(self, port, cafile):
        ctx = ssl.create_default_context(cafile=cafile)
        self.op = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),
                                              urllib.request.HTTPCookieProcessor(CookieJar()))
        self.base = f"https://localhost:{port}"

    def call(self, path, body=None, csrf=True):
        h = {"content-type": "application/json"}
        if csrf:
            h["x-contest-request"] = "1"
        req = urllib.request.Request(self.base + path, headers=h, method="POST" if body is not None else "GET",
                                     data=json.dumps(body).encode() if body is not None else None)
        try:
            with self.op.open(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def order(self, cid, side="buy", qty=1, sym="005930"):
        return self.call("/api/orders", {"client_order_id": cid, "symbol": sym, "side": side, "qty": qty})


def run_contest(cert, fn, tmp_path, now=None, **cfg_over):
    async def main():
        kis_port, web_port, admin_port = _free_port(), _free_port(), _free_port()
        fake = FakeQuotes(kis_port)
        await fake.srv.start()
        cfg = config.Config()
        cfg.contest_admin_hash = HASH
        cfg.contest_host, cfg.contest_port, cfg.contest_admin_port, cfg.http_host = \
            "127.0.0.1", web_port, admin_port, "127.0.0.1"
        cfg.contest_initial_cash = 1_000_000
        for k, v in cfg_over.items():
            setattr(cfg, k, v)
        quotes = KISTrading("real", "k", "s", "", base=f"http://127.0.0.1:{kis_port}",
                            token_cache=str(tmp_path / "tok.json"), timeout_s=0.5, min_interval_s=0)
        led = ContestLedger(str(tmp_path / "contest.db"), cfg.contest_initial_cash, 150, 2000)
        c = cs.Contest(cfg, quotes=quotes, ledger=led, tls=cs.make_tls_context(*cert))
        c._now = now or (lambda: at(10, 0))
        stop = asyncio.Event()
        task = asyncio.create_task(c.run(stop))
        await asyncio.sleep(0.3)
        try:
            return await asyncio.to_thread(fn, lambda: Phone(web_port, cert[0]), fake, c)
        finally:
            stop.set()
            await task
            await fake.srv.close()
    return asyncio.run(main())


def _weekday_10am():
    """시험이 주말에 돌아도 장중으로 보이게 다음 평일 10:00 을 쓴다."""
    import datetime
    d = at(10, 0)
    while d.weekday() >= 5:
        d += datetime.timedelta(days=1)
    return d


def _join(c, phone, nick="alice", pw=USER_PW):
    code, = c.ledger.create_invites(1)
    st, d = phone.call("/api/signup", {"invite": code, "nickname": nick, "password": pw})
    assert st == 201, d
    return code


def test_signup_with_invite_once(cert, tmp_path):
    def check(new, fake, c):
        a = new()
        code = _join(c, a)
        assert a.call("/api/account")[1]["cash"] == 1_000_000
        st, d = new().call("/api/signup", {"invite": code, "nickname": "bob", "password": USER_PW})
        assert st == 409 and "초대" in d["error"]
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_market_order_fills_at_current_price_once(cert, tmp_path):
    def check(new, fake, c):
        a = new()
        _join(c, a)
        st, d = a.order("order-000001", qty=10)
        assert st == 201 and d["order"]["state"] == "FILLED" and d["order"]["price"] == 70_000
        st, d = a.order("order-000001", qty=10)                     # 두 번 누름
        assert st == 200 and d["duplicate"]
        acct = a.call("/api/account")[1]
        assert acct["positions"][0]["qty"] == 10
        assert acct["cash"] == 1_000_000 - 700_000 - 105
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_leaderboard_ranks_by_equity(cert, tmp_path):
    def check(new, fake, c):
        a, b = new(), new()
        _join(c, a, "alice")
        _join(c, b, "bob")
        a.order("order-000001", qty=10)
        fake.price = 80_000
        c._quotes.clear()
        a.call("/api/quote?symbol=005930")                         # 평가용 현재가 갱신
        board = a.call("/api/leaderboard")[1]["board"]
        assert [r["nickname"] for r in board] == ["alice", "bob"]
        assert board[0]["return_pct"] > 0
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_orders_only_in_market_hours(cert, tmp_path):
    import datetime

    def saturday():
        d = at(10, 0)
        while d.weekday() != 5:
            d += datetime.timedelta(days=1)
        return d

    def check(new, fake, c):
        a = new()
        _join(c, a)
        st, d = a.order("order-000001")
        assert st == 409 and "정규장" in d["error"]
        assert a.call("/api/account")[1]["cash"] == 1_000_000
    run_contest(cert, check, tmp_path, now=saturday)


def test_quote_failure_fills_nothing(cert, tmp_path):
    def check(new, fake, c):
        a = new()
        _join(c, a)
        fake.fail = True
        st, d = a.order("order-000001")
        assert st == 503
        assert a.call("/api/account")[1]["cash"] == 1_000_000
        assert c.ledger.orders() == []
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_ledger_mismatch_halts_trading_until_admin_resumes(cert, tmp_path):
    """잔고표를 몰래 고치면 주문이 멈추고, 장부가 다시 맞기 전에는 운영자도 재개하지 못한다."""
    def check(new, fake, c):
        a, admin = new(), new()
        _join(c, a)
        a.order("order-000001", qty=1)
        c.ledger._db.execute("UPDATE accounts SET cash = cash + 5000000")
        assert c.integrity_once()[0]["kind"] == "cash_mismatch"
        st, d = a.order("order-000002")
        assert st == 423 and "점검" in d["error"]
        assert admin.call("/api/admin/login", {"password": PASSWORD})[0] == 200
        st, d = admin.call("/api/admin/resume", {})
        assert st == 409 and d["breaks"]
        c.ledger._db.execute("UPDATE accounts SET cash = cash - 5000000")
        assert admin.call("/api/admin/resume", {})[0] == 200
        assert a.order("order-000002")[0] == 201
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_participant_is_not_admin_and_writes_need_csrf(cert, tmp_path):
    def check(new, fake, c):
        a = new()
        _join(c, a)
        assert a.call("/api/admin/status")[0] == 401
        assert a.call("/api/admin/invites", {"n": 5})[0] == 401
        st, _ = a.call("/api/orders", {"client_order_id": "order-000001", "symbol": "005930",
                                       "side": "buy", "qty": 1}, csrf=False)
        assert st == 403
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_login_lock_is_per_nickname(cert, tmp_path):
    """한 사람이 남의 닉네임으로 틀린 비밀번호를 넣어도 다른 참가자는 잠기지 않는다."""
    def check(new, fake, c):
        _join(c, new(), "alice")
        _join(c, new(), "bob")
        attacker = new()
        for _ in range(5):
            attacker.call("/api/login", {"nickname": "alice", "password": "wrong-password"})
        assert attacker.call("/api/login", {"nickname": "alice", "password": USER_PW})[0] == 429
        assert new().call("/api/login", {"nickname": "bob", "password": USER_PW})[0] == 200
    run_contest(cert, check, tmp_path, now=_weekday_10am)


def test_startup_refuses_without_admin_hash(cert, tmp_path):
    cfg = config.Config()
    cfg.contest_admin_hash = ""
    with pytest.raises(cs.StartupError):
        cs.Contest(cfg, quotes=object(), ledger=object(), tls=cs.make_tls_context(*cert))
