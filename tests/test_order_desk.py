"""주문 데스크 통합 시험 — 가짜 KIS 서버 + 진짜 TLS.

네트워크 없이 사고가 나는 경로를 재현한다:
  보낸 뒤 응답이 끊김(들어갔는지 모름) · 버튼 두 번 · 밖에서 낸 주문 · 한도 · 로그인·CSRF·평문 접속.
"""
import asyncio
import json
import shutil
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.cookiejar import CookieJar

import pytest

from mdfeed import auth, config, ledger as L
from mdfeed.httpd import HTTPServer, Response
from mdfeed.kis_trading import KISTrading
from mdfeed.services import order_desk as od

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl 없음")
PASSWORD = "correct horse battery"
HASH = auth.hash_password(PASSWORD, iterations=1000)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def cert(tmp_path_factory):
    d = tmp_path_factory.mktemp("tls")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                    "-nodes", "-days", "2", "-subj", "/CN=localhost",
                    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-keyout", str(d / "k.pem"), "-out", str(d / "c.pem")],
                   check=True, capture_output=True)
    return str(d / "c.pem"), str(d / "k.pem")


class FakeKIS:
    """주문을 받으면 바로 체결시킨다. mode 로 사고를 흉내 낸다."""

    def __init__(self, port: int):
        self.port = port
        self.mode = "ok"                  # ok · reject · throttle_once · sent_then_hang · hang_not_sent
        self.price = 1000
        self.throttle_code = "EGW00201"
        self.price_calls = 0
        self.balance_calls = 0
        self.orders: list[dict] = []
        self.totals: dict[tuple, dict] = {}
        self.holdings: dict[str, int] = {"005930": 2}
        self.srv = HTTPServer("127.0.0.1", port, "fake-kis")
        r = self.srv.route
        r("POST", "/oauth2/tokenP", lambda q: Response.json({"access_token": "t", "expires_in": 86400}))
        r("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", self._price)
        r("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", self._balance)
        r("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", self._ccld)
        r("POST", "/uapi/domestic-stock/v1/trading/order-cash", self._order)

    async def _price(self, q):
        self.price_calls += 1
        await asyncio.sleep(0.05)                    # 동시 요청이 겹칠 틈
        return Response.json({"rt_cd": "0", "output": {"stck_prpr": str(self.price),
                                                       "prdy_vrss": "0", "prdy_ctrt": "0"}})

    def _balance(self, q):
        self.balance_calls += 1
        return Response.json({"rt_cd": "0", "output2": [{"dnca_tot_amt": "1000000"}], "output1": [
            {"pdno": s, "prdt_name": s, "hldg_qty": str(n), "pchs_avg_pric": "1000", "prpr": "1000",
             "evlu_pfls_amt": "0", "evlu_pfls_rt": "0"} for s, n in self.holdings.items()]})

    def _ccld(self, q):
        side = "sell" if q.query["SLL_BUY_DVSN_CD"] == "01" else "buy"
        t = self.totals.get((q.query["PDNO"], side), {"o": 0, "f": 0, "a": 0})
        return Response.json({"rt_cd": "0", "output1": [], "output2": {
            "tot_ord_qty": str(t["o"]), "tot_ccld_qty": str(t["f"]), "tot_ccld_amt": str(t["a"])}})

    def execute(self, sym: str, side: str, qty: int):
        t = self.totals.setdefault((sym, side), {"o": 0, "f": 0, "a": 0})
        t["o"] += qty
        t["f"] += qty
        t["a"] += qty * self.price
        self.holdings[sym] = self.holdings.get(sym, 0) + (qty if side == "buy" else -qty)

    async def _order(self, q):
        b = json.loads(q.body)
        side = "buy" if q.headers["tr_id"].endswith("0802U") else "sell"
        if self.mode == "throttle_once":
            self.mode = "ok"
            return Response.json({"rt_cd": "1", "msg_cd": self.throttle_code, "msg1": "초당 거래건수를 초과하였습니다."})
        if self.mode == "reject":
            return Response.json({"rt_cd": "1", "msg_cd": "APBK0013", "msg1": "주문가능금액 부족"})
        if self.mode != "hang_not_sent":
            self.orders.append(b)
            self.execute(b["PDNO"], side, int(b["ORD_QTY"]))
        if self.mode in ("sent_then_hang", "hang_not_sent"):
            await asyncio.sleep(1.5)                 # 클라이언트 시간초과(0.5초)보다 길게
        return Response.json({"rt_cd": "0", "msg1": "주문 완료",
                              "output": {"ODNO": f"{len(self.orders):010d}", "ORD_TMD": "100000"}})


class Browser:
    def __init__(self, port: int, cafile: str):
        ctx = ssl.create_default_context(cafile=cafile)
        self.jar = CookieJar()
        self.op = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),
                                              urllib.request.HTTPCookieProcessor(self.jar))
        self.base = f"https://localhost:{port}"

    def call(self, path, body=None, csrf=True):
        h = {"content-type": "application/json"}
        if csrf:
            h["x-desk-request"] = "1"
        req = urllib.request.Request(self.base + path, headers=h, method="POST" if body is not None else "GET",
                                     data=json.dumps(body).encode() if body is not None else None)
        try:
            with self.op.open(req, timeout=5) as r:
                return r.status, json.loads(r.read()), dict(r.headers)
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}"), dict(e.headers)

    def login(self):
        st, _, _ = self.call("/api/login", {"password": PASSWORD})
        assert st == 200

    def order(self, cid, qty=1, side="buy", sym="005930", price=0):
        return self.call("/api/orders", {"client_order_id": cid, "symbol": sym, "side": side,
                                         "qty": qty, "price": price})


def run_desk(cert, fn, tmp_path, **cfg_over):
    async def main():
        kis_port, web_port, admin_port = _free_port(), _free_port(), _free_port()
        kis = FakeKIS(kis_port)
        await kis.srv.start()
        cfg = config.Config()
        cfg.desk_password_hash = HASH
        cfg.desk_host, cfg.desk_port, cfg.desk_admin_port, cfg.http_host = \
            "127.0.0.1", web_port, admin_port, "127.0.0.1"
        cfg.reconcile_open_s = cfg.reconcile_closed_s = 0.1
        for k, v in cfg_over.items():
            setattr(cfg, k, v)
        client = KISTrading("mock", "k", "s", "12345678-01", base=f"http://127.0.0.1:{kis_port}",
                            token_cache=str(tmp_path / "tok.json"), timeout_s=0.5, min_interval_s=0)
        led = L.Ledger(str(tmp_path / "orders.db"))
        desk = od.OrderDesk(cfg, client=client, ledger=led, tls=od.make_tls_context(*cert))
        stop = asyncio.Event()
        task = asyncio.create_task(desk.run(stop))
        await asyncio.sleep(0.3)
        try:
            return await asyncio.to_thread(fn, Browser(web_port, cert[0]), kis, desk)
        finally:
            stop.set()
            await task
            await kis.srv.close()
    return asyncio.run(main())


def wait_for(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_order_is_filled_from_broker_totals(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        st, d, _ = b.order("test-order-0001", qty=2)
        assert st == 201 and d["order"]["state"] == "ACCEPTED"
        assert wait_for(lambda: desk.ledger.orders()[0]["state"] == "FILLED")
        o = desk.ledger.orders()[0]
        assert (o["filled_qty"], o["filled_amt"]) == (2, 2000)
        time.sleep(0.4)
        assert desk.breaks == []                     # 체결 반영 후 잔고도 맞는다
    run_desk(cert, check, tmp_path)


def test_double_click_sends_one_order(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        b.order("test-order-0001")
        st, d, _ = b.order("test-order-0001")
        assert st == 200 and d["duplicate"] is True
        assert len(kis.orders) == 1
    run_desk(cert, check, tmp_path)


def test_timeout_after_send_is_not_resent_and_is_resolved(cert, tmp_path):
    """응답이 안 와도 주문은 들어갔을 수 있다. 다시 보내면 두 번 체결된다."""
    def check(b, kis, desk):
        b.login()
        kis.mode = "sent_then_hang"
        st, d, _ = b.order("test-order-0001")
        assert st == 202 and d["order"]["state"] == "UNKNOWN"
        kis.mode = "ok"
        assert wait_for(lambda: desk.ledger.orders()[0]["state"] == "FILLED")
        assert len(kis.orders) == 1
    run_desk(cert, check, tmp_path)


def test_timeout_without_send_becomes_not_sent(cert, tmp_path, monkeypatch):
    monkeypatch.setattr(L, "UNKNOWN_GRACE_S", 0.3)

    def check(b, kis, desk):
        b.login()
        kis.mode = "hang_not_sent"
        st, d, _ = b.order("test-order-0001")
        assert d["order"]["state"] == "UNKNOWN"
        assert wait_for(lambda: desk.ledger.orders()[0]["state"] == "NOT_SENT")
        kis.mode = "ok"
        st, _, _ = b.order("test-order-0002")       # 열린 주문이 닫혀야 다음 주문이 된다
        assert st == 201
    run_desk(cert, check, tmp_path)


def test_rejection_is_recorded(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        kis.mode = "reject"
        st, d, _ = b.order("test-order-0001")
        assert st == 422 and d["order"]["state"] == "REJECTED" and "부족" in d["order"]["message"]
    run_desk(cert, check, tmp_path)


@pytest.mark.parametrize("code", ["EGW00201", "EGW00215"])
def test_throttled_call_is_retried_once(cert, tmp_path, code):
    """초당 한도 거절은 주문이 안 들어간 것이라 다시 보내도 1건이다."""
    def check(b, kis, desk):
        b.login()
        kis.throttle_code = code
        kis.mode = "throttle_once"
        st, d, _ = b.order("test-order-0001")
        assert st == 201 and d["order"]["state"] != "REJECTED"
        assert len(kis.orders) == 1
    run_desk(cert, check, tmp_path)


def test_quotes_from_many_tabs_share_one_broker_call(cert, tmp_path):
    """현재가는 주문·대사와 같은 초당 한도를 쓴다. 화면 수만큼 호출이 늘면 대사가 밀린다."""
    def check(b, kis, desk):
        desk.quote_ttl_s = 0.5
        b.login()
        before = kis.price_calls
        with ThreadPoolExecutor(8) as ex:
            got = list(ex.map(lambda _: b.call("/api/quote?symbol=005930")[0], range(8)))
        assert got == [200] * 8
        assert kis.price_calls - before == 1
        time.sleep(desk.quote_ttl_s + 0.1)
        b.call("/api/quote?symbol=005930")
        assert kis.price_calls - before == 2
    run_desk(cert, check, tmp_path)


def test_screen_balance_reads_reconcile_cache(cert, tmp_path):
    """화면이 증권사 잔고 조회를 직접 일으키지 못한다. 예전 화면은 10초마다 ?fresh=1 로 캐시를 건너뛰었다."""
    def check(b, kis, desk):
        b.login()
        assert wait_for(lambda: desk.balance_cache is not None)
        before = kis.balance_calls
        for _ in range(5):
            assert b.call("/api/balance?fresh=1")[0] == 200
        assert kis.balance_calls - before <= 1        # 그 사이 대사 주기 한 번은 있을 수 있다
    run_desk(cert, check, tmp_path, reconcile_open_s=5.0, reconcile_closed_s=5.0)


def test_throttle_is_counted_per_code(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        kis.throttle_code = "EGW00215"
        kis.mode = "throttle_once"
        b.order("test-order-0001")
        lines = desk.registry.prometheus().splitlines()

        def value(name, label):
            return [float(x.rsplit(" ", 1)[1]) for x in lines if x.startswith(name + "{") and label in x]
        assert value("mdfeed_kis_throttled_total", 'code="EGW00215"') == [1.0]
        assert value("mdfeed_kis_throttled_total", 'code="EGW00201"') == [0.0]
        assert value("mdfeed_kis_calls_total", 'tr="VTTC0802U"') == [2.0]     # 거절 1 + 재시도 1
    run_desk(cert, check, tmp_path)


def test_outside_order_is_a_break_not_absorbed(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        b.order("test-order-0001")
        assert wait_for(lambda: desk.ledger.orders()[0]["state"] == "FILLED")
        kis.execute("005930", "buy", 1)              # HTS 에서 낸 주문
        assert wait_for(lambda: len(desk.breaks) == 2)
        assert {x["kind"] for x in desk.breaks} == {"unexplained_broker_orders", "position_mismatch"}
        assert desk.ledger.orders()[0]["filled_qty"] == 1
        st, d, _ = b.call("/api/reconcile")
        assert len(d["breaks"]) == 2
    run_desk(cert, check, tmp_path)


def test_limits_block_before_sending(cert, tmp_path):
    def check(b, kis, desk):
        b.login()
        st, d, _ = b.order("test-order-0001", qty=11)           # 11,000원 > 건당 10,000원
        assert st == 422 and "건당" in d["error"]
        assert b.order("test-order-0002", qty=8)[0] == 201
        assert wait_for(lambda: desk.ledger.orders()[0]["state"] == "FILLED")
        st, d, _ = b.order("test-order-0003", qty=8)            # 오늘 8,000 + 8,000 > 15,000
        assert st == 422 and "하루" in d["error"]
        assert len(kis.orders) == 1
    run_desk(cert, check, tmp_path, order_max_krw=10_000, order_daily_max_krw=15_000)


def test_login_csrf_and_cookie_flags(cert, tmp_path):
    def check(b, kis, desk):
        assert b.call("/api/balance")[0] == 401
        assert b.call("/api/login", {"password": PASSWORD}, csrf=False)[0] == 403
        st, _, h = b.call("/api/login", {"password": PASSWORD})
        assert st == 200
        cookie = h["Set-Cookie"]
        for flag in ("HttpOnly", "Secure", "SameSite=Strict"):
            assert flag in cookie
        assert b.call("/api/orders", {"client_order_id": "x" * 10}, csrf=False)[0] == 403
        st, _, h = b.call("/api/session")
        assert "Access-Control-Allow-Origin" not in h
        assert "default-src 'self'" in h["Content-Security-Policy"]
    run_desk(cert, check, tmp_path)


def test_bruteforce_is_locked(cert, tmp_path):
    def check(b, kis, desk):
        for _ in range(auth.FAIL_LIMIT):
            assert b.call("/api/login", {"password": "nope"})[0] == 401
        assert b.call("/api/login", {"password": PASSWORD})[0] == 429   # 맞는 비밀번호도 잠금 중엔 안 된다
    run_desk(cert, check, tmp_path)


def test_plain_http_gets_nothing(cert, tmp_path):
    def check(b, kis, desk):
        port = int(b.base.rsplit(":", 1)[1])
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(b"GET /api/session HTTP/1.1\r\nHost: x\r\n\r\n")
            try:
                data = s.recv(100)
            except (ConnectionResetError, socket.timeout):
                data = b""
        assert not data.startswith(b"HTTP/1.1 200")
    run_desk(cert, check, tmp_path)


def test_startup_refuses_unsafe_config(cert, tmp_path):
    cfg = config.Config()
    cfg.desk_password_hash = ""
    with pytest.raises(od.StartupError):
        od.OrderDesk(cfg, client=object(), ledger=L.Ledger(":memory:"), tls=object())
    cfg.desk_password_hash = HASH
    cfg.order_env, cfg.order_allow_real = "real", False
    with pytest.raises(od.StartupError, match="ALLOW_REAL"):
        od.make_client(cfg)
    with pytest.raises(od.StartupError, match="TLS"):
        od.make_tls_context(str(tmp_path / "none.crt"), str(tmp_path / "none.key"))
