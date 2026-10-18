"""한국투자증권 국내주식 주문·잔고·체결합계 클라이언트 (주문 데스크용).

scripts/kis_paper.py 는 사람이 치는 CLI 이고, 이 모듈은 서비스가 쓰는 쪽이다.
같은 엔드포인트를 쓰지만 **실패를 두 종류로 나눠 올린다**는 점이 다르다.

* ``Rejected``  — 증권사가 응답을 줬고 rt_cd != '0'. 주문이 **안 들어갔다**는 게 확실하다.
* ``Uncertain`` — 시간초과·연결 끊김·해석 불가 응답. 주문이 들어갔는지 **모른다**.

주문에서 이 둘을 섞으면 사고가 난다. "실패했으니 다시 보내자"를 Uncertain 에 하면
같은 주문이 두 번 체결될 수 있다. 그래서 주문 데스크는 Uncertain 을 받으면 재전송하지 않고
UNKNOWN 상태로 두고, 증권사 쪽 합계와 대조해서 나중에 판정한다(ledger.py).

실측으로 확인한 것 (2026-10-12, 모의투자)
-----------------------------------------
* 일별주문체결조회(VTTC8001R)는 rt_cd 0 이면서 상세 목록(output1)이 **항상 비어 있다**
  ("모의투자 조회할 내역(자료)이 없습니다"). 합계(output2: 주문수량·체결수량·체결금액)만 온다.
* 그 합계는 **종목(PDNO)·매수매도(SLL_BUY_DVSN_CD)·날짜로는 걸러지지만 주문번호(ODNO)로는 안 걸러진다.**
  없는 주문번호를 넣어도 같은 합계가 나온다.
  → 주문별 체결은 (날짜, 종목, 방향) 합계의 증가분을 원장이 먼저 낸 주문 순서대로 배분해서 얻는다.

실전 TR ID 는 문서 기준값이고 이 저장소에서 실전 주문으로 확인한 적은 없다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request

from .adapters.kis_token import TokenStore

log = logging.getLogger("mdfeed.kis_trading")

BASES = {
    "mock": "https://openapivts.koreainvestment.com:29443",
    "real": "https://openapi.koreainvestment.com:9443",
}
TR = {
    "mock": {"buy": "VTTC0802U", "sell": "VTTC0801U", "balance": "VTTC8434R", "ccld": "VTTC8001R"},
    "real": {"buy": "TTTC0802U", "sell": "TTTC0801U", "balance": "TTTC8434R", "ccld": "TTTC8001R"},
}
TR_PRICE = "FHKST01010100"
# 모의투자는 초당 호출 한도가 실전보다 훨씬 낮다. 넘으면 EGW00201 로 거절된다.
# 한도는 TR 종류와 무관하게 사실상 초당 1건이다(2026-10-18 실측, 재시도 없이 원시 응답):
#   0.55초 30건 중 8건 · 0.8초 40건 중 5건 · 1.05초 70건 중 1건 · 1.2초·1.5초 80건 중 0건 거절.
# 처음 값 0.55 는 "초당 2건"이라는 가정이었고, 24시간 가동에서 한도 거절이 하루 수십 번 난 원인이었다.
MIN_INTERVAL_S = {"mock": 1.1, "real": 0.06}
# 간격을 지켜도 토큰 발급 직후 첫 호출은 같은 초로 묶여 EGW00201 이 난다(모의 실측).
# EGW00215 는 원장 쪽 초당 한도로, 간격을 지켜도 하루 수십 번 산발한다(모의 24시간 실측).
# 둘 다 접수 전 거절이라 주문도 안 들어간 게 확실하므로 1초 뒤 한 번만 다시 보낸다.
THROTTLED = {"EGW00201", "EGW00215"}
THROTTLE_RETRY_S = 1.0
SIDE_CODE = {"sell": "01", "buy": "02"}


class Rejected(Exception):
    def __init__(self, code: str, msg: str):
        super().__init__(f"[{code}] {msg}")
        self.code, self.msg = code, msg


class Uncertain(Exception):
    pass


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


class KISTrading:
    def __init__(self, env: str, app_key: str, app_secret: str, account: str,
                 base: str | None = None, token_cache: str | None = None,
                 timeout_s: float = 10.0, min_interval_s: float | None = None):
        if env not in BASES:
            raise ValueError(f"env 는 mock 또는 real: {env!r}")
        cano, _, prdt = account.replace(" ", "").partition("-")
        if len(cano) != 8 or not cano.isdigit():
            raise ValueError("계좌번호 앞자리는 숫자 8자리여야 합니다")
        self.env = env
        self.cano, self.prdt = cano, (prdt or "01")
        self.app_key, self.app_secret = app_key, app_secret
        self.base = base or BASES[env]
        cache = token_cache or ("~/.mdfeed/kis_vts_token.json" if env == "mock"
                                else "~/.mdfeed/kis_token.json")
        self._tokens = TokenStore.get(app_key, app_secret, self.base, cache)
        self.timeout_s = timeout_s
        self._gap = MIN_INTERVAL_S[env] if min_interval_s is None else min_interval_s
        self._last = 0.0
        self._pace = asyncio.Lock()
        self.calls = 0
        # (사건, 값) 을 받는 콜백. 데스크가 지표로 바꾼다: ("call", tr_id) · ("throttled", msg_cd)
        self.observer = None

    @property
    def account_label(self) -> str:
        return f"{self.cano[:4]}****-{self.prdt}"

    async def _call(self, method: str, path: str, tr_id: str,
                    params: dict | None = None, body: dict | None = None) -> dict:
        try:
            return await self._call_once(method, path, tr_id, params, body)
        except Rejected as e:
            if e.code not in THROTTLED:
                raise
            log.warning("[kis] %s 초당 한도 초과 → %.0f초 뒤 1회 재시도", tr_id, THROTTLE_RETRY_S)
            await asyncio.sleep(THROTTLE_RETRY_S)
            return await self._call_once(method, path, tr_id, params, body)

    async def _call_once(self, method: str, path: str, tr_id: str,
                         params: dict | None, body: dict | None) -> dict:
        token = await self._tokens.token()
        async with self._pace:
            wait = self._last + self._gap - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()
        self.calls += 1
        self._notify("call", tr_id)
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        headers = {"content-type": "application/json; charset=utf-8",
                   "authorization": f"Bearer {token}",
                   "appkey": self.app_key, "appsecret": self.app_secret,
                   "tr_id": tr_id, "custtype": "P"}
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        def _do() -> dict:
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                # 4xx/5xx 라도 본문에 rt_cd 가 있으면 증권사의 확정 응답이다
                raw = e.read()
                try:
                    return json.loads(raw)
                except ValueError:
                    raise Uncertain(f"HTTP {e.code} {tr_id}") from None
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
                raise Uncertain(f"{type(e).__name__} {tr_id}") from None

        d = await asyncio.to_thread(_do)
        if "rt_cd" not in d:
            raise Uncertain(f"rt_cd 없는 응답 {tr_id}")
        if d["rt_cd"] != "0":
            code = d.get("msg_cd", "")
            if code in THROTTLED:
                self._notify("throttled", code)
            raise Rejected(code, (d.get("msg1") or "").strip())
        return d

    def _notify(self, kind: str, value: str) -> None:
        if self.observer:
            self.observer(kind, value)

    # ── 조회 ──────────────────────────────────────────────────────────────
    async def price(self, symbol: str) -> dict:
        d = await self._call("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", TR_PRICE,
                             {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol})
        o = d.get("output") or {}
        return {"symbol": symbol, "price": int(_num(o.get("stck_prpr"))),
                "change": int(_num(o.get("prdy_vrss"))), "change_pct": _num(o.get("prdy_ctrt")),
                "volume": int(_num(o.get("acml_vol"))), "high": int(_num(o.get("stck_hgpr"))),
                "low": int(_num(o.get("stck_lwpr"))), "open": int(_num(o.get("stck_oprc")))}

    async def balance(self) -> dict:
        d = await self._call("GET", "/uapi/domestic-stock/v1/trading/inquire-balance",
                             TR[self.env]["balance"], {
                                 "CANO": self.cano, "ACNT_PRDT_CD": self.prdt, "AFHR_FLPR_YN": "N",
                                 "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01",
                                 "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N",
                                 "PRCS_DVSN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
        s = (d.get("output2") or [{}])[0]
        holdings = []
        for h in d.get("output1") or []:
            qty = int(_num(h.get("hldg_qty")))
            if qty > 0:
                holdings.append({"symbol": h.get("pdno"), "name": h.get("prdt_name"), "qty": qty,
                                 "avg_price": _num(h.get("pchs_avg_pric")),
                                 "price": int(_num(h.get("prpr"))),
                                 "pnl": int(_num(h.get("evlu_pfls_amt"))),
                                 "pnl_pct": _num(h.get("evlu_pfls_rt"))})
        return {"cash": int(_num(s.get("dnca_tot_amt"))), "total": int(_num(s.get("tot_evlu_amt"))),
                "pnl": int(_num(s.get("evlu_pfls_smtl_amt"))), "holdings": holdings}

    async def day_totals(self, date: str, symbol: str, side: str) -> dict:
        """(날짜, 종목, 방향)의 주문·체결 합계. 모의투자에서 믿을 수 있는 유일한 체결 출처."""
        d = await self._call("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                             TR[self.env]["ccld"], {
                                 "CANO": self.cano, "ACNT_PRDT_CD": self.prdt,
                                 "INQR_STRT_DT": date, "INQR_END_DT": date,
                                 "SLL_BUY_DVSN_CD": SIDE_CODE[side], "INQR_DVSN": "00",
                                 "PDNO": symbol, "CCLD_DVSN": "00", "ORD_GNO_BRNO": "", "ODNO": "",
                                 "INQR_DVSN_3": "00", "INQR_DVSN_1": "",
                                 "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
        o = d.get("output2") or {}
        if isinstance(o, list):
            o = o[0] if o else {}
        return {"ord_qty": int(_num(o.get("tot_ord_qty"))),
                "ccld_qty": int(_num(o.get("tot_ccld_qty"))),
                "ccld_amt": int(_num(o.get("tot_ccld_amt")))}

    # ── 주문 ──────────────────────────────────────────────────────────────
    async def order(self, side: str, symbol: str, qty: int, price: int) -> dict:
        """현금 주문. price 0 = 시장가. 성공하면 주문번호를 돌려준다.

        Rejected 면 안 들어간 것, Uncertain 이면 들어갔는지 모르는 것이다. 여기서 재시도하지 않는다.
        """
        d = await self._call("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                             TR[self.env][side], body={
                                 "CANO": self.cano, "ACNT_PRDT_CD": self.prdt, "PDNO": symbol,
                                 "ORD_DVSN": "01" if price == 0 else "00",
                                 "ORD_QTY": str(qty), "ORD_UNPR": str(price)})
        o = d.get("output") or {}
        odno = o.get("ODNO")
        if not odno:
            raise Uncertain("주문번호 없는 성공 응답")
        return {"odno": odno, "time": o.get("ORD_TMD"), "msg": (d.get("msg1") or "").strip()}

