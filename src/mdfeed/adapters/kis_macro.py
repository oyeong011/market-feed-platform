"""금리·지수 어댑터 — 주식 밖의 자산군.

왜 넣었나
---------
마켓데이터 서비스에서 주식만 다루는 경우는 거의 없다. 금리와 지수는 다른 모든
자산의 기준선이고, 국내 금융정보 서비스의 주력 상품이기도 하다.
주식·크립토만 있던 피드에 이 두 축을 더해 자산군 커버리지를 넓힌다.

다루는 것
---------
* **금리**: 미국 T-BOND/T-NOTE/T-BILL·연방기금금리, 일본 10년 국채, 국내 국고채·CD 등
* **지수**: 코스피 종합·코스피200·코스닥 종합·대형주·중형주 등

둘 다 REST 폴링이다. 실시간 웹소켓이 없기도 하고, 금리는 하루 단위로 갱신되므로
초 단위로 볼 이유도 없다. 지수는 장중에 계속 움직이므로 더 자주 본다.

유량 예산은 kis_rest 와 같은 계정을 공유한다. 처음엔 제한기 없이 붙였다가
지수 폴링이 예산을 다 써서 **금리 호출이 통째로 거절당했다**(갱신 0건).
같은 AdaptiveRateLimiter 를 쓰되, 금리·휴장일처럼 드문 호출이 굶지 않도록
지수 루프에 간격을 넉넉히 준다.

국내 금리(output2)를 발행하지 않는 이유
---------------------------------------
`comp-interest` 의 `output2` 는 **두 가지가 동시에 깨져 있다.**

1. **키와 값의 대응이 행마다 어긋난다.**

       {"bcdt_code": "Y0101", "hts_kor_isnm": "Y0109", "bond_mnrt_prpr": "Y0117",
        "prdy_vrss_sign": "국고채 30년", "bond_mnrt_prdy_vrss": "4.5950", ...}

   `prdy_vrss_sign`(전일대비부호) 자리에 종목명이, 전일대비 자리에 금리가 있다.

2. **일부 행은 종목명이 이미 손상된 채로 온다.**
   바이트를 직접 확인하니 `\xef\xbf\xbd`(U+FFFD 대체문자)였다. 우리 쪽 디코딩
   문제가 아니라 **서버가 보내기 전에 이미 원본을 잃은 것**이라 복원할 방법이 없다.

값의 모양으로 짝지어 봤지만(`parse_rate_rows`) 그 짝짓기도 못 믿는다.
`CD AAA 3개월(13주)` 에 2.0000 이 붙는데 `CD 91일` 은 3.12 다. 하나는 틀렸다.

그래서 **국내 금리는 발행하지 않는다.** 이 프로젝트가 세운 원칙 중 하나가
"틀린 값을 조용히 배포하는 것이 값이 없는 것보다 나쁘다" 이고, 여기가 정확히
그 경우다. 그럴듯한 숫자를 내보내는 대신 안 내보내고, 그 사실과 이유를
`/healthz` 에 남긴다.

`output1`(해외 금리 7종)은 키 대응이 정상이고 값도 교차 확인되므로 발행한다.
국내 금리가 필요하면 장내채권 시세 API 를 별도 경로로 붙이는 것이 맞다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request

from ..models import Trade, SIDE_UNKNOWN, now_ns
from .base import Adapter
from .kis import ENDPOINTS, market_is_open
from .kis_rest import AdaptiveRateLimiter

log = logging.getLogger("mdfeed.adapter.kis_macro")

TR_INTEREST = "FHPST07020000"     # 금리 종합 (국내채권/금리)
TR_INDEX = "FHPUP02100000"        # 국내업종 현재지수
TR_HOLIDAY = "CTCA0903R"          # 국내휴장일조회

# 업종 지수. 코드는 KIS 업종 분류 기준.
INDEX_CODES = [
    ("0001", "코스피"), ("2001", "코스피200"), ("1001", "코스닥"),
    ("0002", "코스피 대형주"), ("0003", "코스피 중형주"), ("0004", "코스피 소형주"),
]

HANGUL = re.compile(r"[가-힣]")


def _is_name(v) -> bool:
    return isinstance(v, str) and bool(HANGUL.search(v))


def _as_float(v):
    try:
        f = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return f


def parse_rate_rows(rows: list) -> tuple[list[tuple[str, float]], int]:
    """키를 믿지 않고 값의 모양으로 (이름, 금리)를 뽑는다.

    **현재 발행 경로에서는 쓰지 않는다** (위 docstring 참고 — 짝짓기를 신뢰할 수 없다).
    응답 손상 정도를 계측하기 위해 남겨 두고, `/healthz` 의 통계로만 노출한다.
    KIS 가 응답 형식을 고치면 이 함수의 실패율이 먼저 떨어질 것이다.
    """
    out, failed = [], 0
    for r in rows:
        if not isinstance(r, dict):
            failed += 1
            continue
        vals = list(r.values())
        found = False
        for i, v in enumerate(vals):
            if not _is_name(v):
                continue
            for nxt in vals[i + 1:i + 3]:          # 이름 바로 뒤 1~2칸에서 찾는다
                f = _as_float(nxt)
                if f is not None and 0.0 < f < 100.0:   # 금리는 퍼센트 범위
                    out.append((v.strip(), f))
                    found = True
                    break
            if found:
                break
        if not found:
            failed += 1
    return out, failed


class KISMacroAdapter(Adapter):
    """금리와 지수를 폴링해 피드에 얹는다."""

    name = "kis_macro"
    stale_after_s = 900.0
    measures_latency = False        # 폴링 주기가 곧 지연이다

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.app_key = cfg.kis_app_key
        self.app_secret = cfg.kis_app_secret
        env = (getattr(cfg, "kis_env", "real") or "real").lower()
        self.rest_base = ENDPOINTS.get(env, ENDPOINTS["real"])[0]
        self.index_interval_s = float(getattr(cfg, "kis_index_interval_s", 10.0))
        self.rate_interval_s = float(getattr(cfg, "kis_rate_interval_s", 300.0))
        self.token_cache = os.path.expanduser(
            getattr(cfg, "kis_token_cache", "~/.mdfeed/kis_token.json"))
        self._token: str | None = None
        self._token_exp = 0.0
        self.limiter = AdaptiveRateLimiter(float(getattr(cfg, "kis_rest_rate", 3.0)))
        self.rate_limited = 0
        self.rate_rows = 0
        self.rate_parse_failed = 0
        # output2 는 발행하지 않고 손상 정도만 계측한다
        self.rate2_total = 0
        self.rate2_corrupt = 0
        self.rate2_parsed = 0
        self.index_rows = 0
        self.holidays: dict[str, bool] = {}       # YYYYMMDD → 개장 여부
        self._last: dict[str, float] = {}

    def enabled(self) -> bool:
        return bool(self.app_key and self.app_secret)

    def disabled_reason(self) -> str:
        return "KIS_APP_KEY / KIS_APP_SECRET 미설정"

    def expects_data(self) -> bool:
        return self.is_market_open()

    def is_market_open(self) -> bool:
        """휴장일 캘린더를 반영한 개장 판정.

        기존 판정은 요일과 시각만 봤다. 공휴일에는 그냥 데이터가 없는 것으로
        처리됐지만, 그건 "장이 닫혔다"와 "피드가 죽었다"를 구분하지 못한다는 뜻이다.
        KIS 휴장일 조회로 실제 캘린더를 받아 둔다.
        """
        import datetime as dt
        from .kis import KST
        today = dt.datetime.now(KST).strftime("%Y%m%d")
        if today in self.holidays and not self.holidays[today]:
            return False
        return market_is_open()

    # ── 인증 ──────────────────────────────────────────────────────────────
    async def _token_get(self) -> str:
        if self._token and self._token_exp > time.time() + 60:
            return self._token
        try:
            with open(self.token_cache, encoding="utf-8") as fh:
                d = json.load(fh)
            if d.get("expires_at", 0) > time.time() + 600:
                self._token, self._token_exp = d["access_token"], d["expires_at"]
                return self._token
        except Exception:                            # noqa: BLE001
            pass

        def _issue():
            body = json.dumps({"grant_type": "client_credentials",
                               "appkey": self.app_key,
                               "appsecret": self.app_secret}).encode()
            req = urllib.request.Request(f"{self.rest_base}/oauth2/tokenP", data=body,
                                         headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                d = json.loads(r.read())
            return d["access_token"], time.time() + int(d.get("expires_in", 86400)) - 120

        tok, exp = await asyncio.to_thread(_issue)
        self._token, self._token_exp = tok, exp
        os.makedirs(os.path.dirname(self.token_cache), exist_ok=True)
        with open(self.token_cache, "w", encoding="utf-8") as fh:
            json.dump({"access_token": tok, "expires_at": exp}, fh)
        os.chmod(self.token_cache, 0o600)
        return tok

    async def _call(self, path: str, tr_id: str, params: dict) -> dict | None:
        await self.limiter.acquire()
        tok = await self._token_get()

        def _do():
            q = "&".join(f"{k}={v}" for k, v in params.items())
            req = urllib.request.Request(f"{self.rest_base}{path}?{q}", headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {tok}", "appkey": self.app_key,
                "appsecret": self.app_secret, "tr_id": tr_id, "custtype": "P"})
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:
                    return json.loads(e.read())
                except Exception:                    # noqa: BLE001
                    return {"rt_cd": str(e.code)}

        d = await asyncio.to_thread(_do)
        if d.get("msg_cd") == "EGW00201":
            self.rate_limited += 1
            self.limiter.on_rate_limited()
            return None
        if d.get("rt_cd") != "0":
            self.errors += 1
            return None
        self.limiter.on_success()
        return d

    # ── 발행 ──────────────────────────────────────────────────────────────
    def _emit_value(self, venue: str, symbol: str, value: float) -> None:
        """값이 바뀌었을 때만 내보낸다. 지수·금리는 같은 값이 계속 반복된다."""
        key = f"{venue}:{symbol}"
        if self._last.get(key) == value:
            return
        self._last[key] = value
        ts = now_ns()
        self._mark(Trade(venue=venue, symbol=symbol, ts_event_ns=ts, ts_recv_ns=ts,
                         price=value, qty=0.0, side=SIDE_UNKNOWN))

    # ── 루프 ──────────────────────────────────────────────────────────────
    async def _index_loop(self) -> None:
        while True:
            for code, label in INDEX_CODES:
                d = await self._call(
                    "/uapi/domestic-stock/v1/quotations/inquire-index-price",
                    TR_INDEX, {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": code})
                if d:
                    o = d.get("output") or {}
                    v = _as_float(o.get("bstp_nmix_prpr"))
                    if v and v > 0:
                        self.index_rows += 1
                        self._emit_value("KRX-IDX", label, v)
                await asyncio.sleep(0.6)   # 금리·휴장일 호출이 굶지 않게 여유를 둔다
            await asyncio.sleep(self.index_interval_s)

    async def _rate_loop(self) -> None:
        while True:
            d = await self._call(
                "/uapi/domestic-stock/v1/quotations/comp-interest", TR_INTEREST,
                {"FID_COND_MRKT_DIV_CODE": "I", "FID_COND_SCR_DIV_CODE": "20702",
                 "FID_DIV_CLS_CODE": "1", "FID_DIV_CLS_CODE1": ""})
            if d:
                # output1 은 키 대응이 정상이라 그대로 읽는다
                for r in (d.get("output1") or []):
                    name = (r.get("hts_kor_isnm") or "").strip()
                    v = _as_float(r.get("bond_mnrt_prpr"))
                    if name and v is not None:
                        self.rate_rows += 1
                        self._emit_value("RATE", name, v)
                # output2(국내 금리)는 발행하지 않는다. 계측만 한다 — 이유는 위 docstring.
                rows2 = d.get("output2") or []
                pairs, failed = parse_rate_rows(rows2)
                self.rate2_total += len(rows2)
                self.rate2_corrupt += failed
                self.rate2_parsed += len(pairs)
            await asyncio.sleep(self.rate_interval_s)

    async def _holiday_loop(self) -> None:
        """휴장일 캘린더를 하루 한 번 갱신한다."""
        import datetime as dt
        from .kis import KST
        while True:
            today = dt.datetime.now(KST).strftime("%Y%m%d")
            d = await self._call("/uapi/domestic-stock/v1/quotations/chk-holiday",
                                 TR_HOLIDAY, {"BASS_DT": today,
                                              "CTX_AREA_NK": "", "CTX_AREA_FK": ""})
            if d:
                cal = {r.get("bass_dt"): (r.get("opnd_yn") == "Y")
                       for r in (d.get("output") or []) if r.get("bass_dt")}
                if cal:
                    self.holidays = cal
                    closed = [k for k, v in sorted(cal.items()) if not v][:5]
                    log.info("[kis_macro] 휴장일 캘린더 %d일 확보. 가까운 휴장일: %s",
                             len(cal), ", ".join(closed) or "없음")
            await asyncio.sleep(6 * 3600)

    async def session(self) -> None:
        if not self.enabled():
            raise RuntimeError(self.disabled_reason())
        log.info("[kis_macro] 지수 %d종 (%.0f초) / 금리 (%.0f초) / 휴장일 캘린더",
                 len(INDEX_CODES), self.index_interval_s, self.rate_interval_s)
        await asyncio.gather(self._index_loop(), self._rate_loop(), self._holiday_loop())

    def health(self) -> dict:
        d = super().health()
        d.update({
            "index_updates": self.index_rows,
            "rate_updates": self.rate_rows,
            "domestic_rates_published": False,
            "domestic_rates_reason": (
                "응답의 키·값 대응이 어긋나고 일부 종목명이 서버 쪽에서 이미 "
                "U+FFFD 로 손상돼 온다. 짝짓기를 신뢰할 수 없어 발행하지 않는다."),
            "domestic_rate_rows_seen": self.rate2_total,
            "domestic_rate_rows_corrupt": self.rate2_corrupt,
            "rate_limited": self.rate_limited,
            "current_rate_per_s": round(self.limiter.current_rate, 2),
            "holiday_calendar_days": len(self.holidays),
            "market_open": self.is_market_open(),
            "tracked": len(self._last),
        })
        return d
