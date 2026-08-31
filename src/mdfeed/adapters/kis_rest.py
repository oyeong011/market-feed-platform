"""KRX 광역 스냅샷 어댑터 — 웹소켓 한도를 REST 로 우회한다.

왜 필요한가
-----------
KIS 실시간 웹소켓은 **계정당 등록 한도**가 있다. 이 계정은 실측 결과 **3종목**이었다
(새 연결에서 한 종목씩 등록해도 4번째부터 `MAX SUBSCRIBE OVER`). 문서 기준값 41과
다르고, 계정마다 다르다.

코스피 전체를 틱 단위로 받으려면 거래소와의 정식 시세 계약이 필요하다.
개인 API 로는 불가능하다. 대신 실제 마켓데이터 시스템이 쓰는 **계층형 피드**로 우회한다.

    1계층  웹소켓 3종목        체결 단위 · 밀리초        ← adapters/kis.py
    2계층  순위 API           30종목/요청 · 수십 초      ← 이 파일
    3계층  유니버스 라운드로빈  1종목/요청 · 회전 주기     ← 이 파일

2계층이 효율의 핵심이다. 순위 API 는 한 번 호출에 30종목의 현재가·거래량을 주므로,
같은 요청 예산으로 30배 넓게 덮는다. 거래가 활발한 종목이 자동으로 상위에 오므로
"지금 움직이는 종목"이 우선 갱신되는 효과도 있다.

유량 제한
---------
실측 결과 초당 5건을 넘기면 `EGW00201`(초당 거래건수 초과)이 뜬다.
4건에서도 20건 중 2건이 실패했다. 안전하게 **3건/초**로 페이싱하고,
그래도 걸리면 지수 백오프로 물러난다.

스냅샷을 체결로 바꾸는 문제
---------------------------
REST 응답은 체결이 아니라 **그 순간의 상태**다. 이걸 그대로 Trade 로 내보내면
있지도 않은 체결을 만들어내는 것이다. 그래서 직전 폴링 대비 **누적거래량이
늘어난 만큼만** 합성 체결로 발행하고, venue 를 `KRX` 로 따로 두어 웹소켓에서
온 진짜 체결(`KIS`)과 섞이지 않게 한다.

지연 지표에서도 제외한다(`measures_latency = False`). 폴링 주기가 곧 지연이라
네트워크 지연을 재는 의미가 없다.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
import urllib.error
import urllib.request

from ..models import BookTop, Trade, SIDE_UNKNOWN, now_ns
from .base import Adapter, load_universe
from .kis_token import TokenStore
from .kis import ENDPOINTS, market_is_open

log = logging.getLogger("mdfeed.adapter.kis_rest")

TR_PRICE = "FHKST01010100"          # 주식현재가 시세 (1종목)
TR_ASK = "FHKST01010200"            # 주식현재가 호가 (1종목)
TR_VOLUME_RANK = "FHPST01710000"    # 거래량순위 (30종목)
TR_FLUCT_RANK = "FHPST01700000"     # 등락률순위 (30종목)

RANK_SPECS = [
    ("volume", "/uapi/domestic-stock/v1/quotations/volume-rank", TR_VOLUME_RANK, {
        "FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
        "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0", "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "111111111", "FID_TRGT_EXLS_CLS_CODE": "000000",
        "FID_INPUT_PRICE_1": "", "FID_INPUT_PRICE_2": "", "FID_VOL_CNT": "",
        "FID_INPUT_DATE_1": ""}),
    ("gainers", "/uapi/domestic-stock/v1/ranking/fluctuation", TR_FLUCT_RANK, {
        "fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0000", "fid_rank_sort_cls_code": "0", "fid_input_cnt_1": "0",
        "fid_prc_cls_code": "0", "fid_input_price_1": "", "fid_input_price_2": "",
        "fid_vol_cnt": "", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
        "fid_div_cls_code": "0", "fid_rsfl_rate1": "", "fid_rsfl_rate2": ""}),
    ("losers", "/uapi/domestic-stock/v1/ranking/fluctuation", TR_FLUCT_RANK, {
        "fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0000", "fid_rank_sort_cls_code": "1", "fid_input_cnt_1": "0",
        "fid_prc_cls_code": "0", "fid_input_price_1": "", "fid_input_price_2": "",
        "fid_vol_cnt": "", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
        "fid_div_cls_code": "0", "fid_rsfl_rate1": "", "fid_rsfl_rate2": ""}),
]


class AdaptiveRateLimiter:
    """스스로 속도를 찾아가는 유량 제한기 (AIMD).

    서버가 알려주는 한도는 문서값과 다르고 시간대에 따라서도 달라진다.
    실측에서 3 req/s 로 고정했더니 60초에 15번 `EGW00201` 을 맞았다 — 요청의 10%가
    낭비된 것이다.

    그래서 TCP 혼잡제어와 같은 방식을 쓴다.

        유량 초과   → 간격을 1.15배로 늘린다 (곱셈 감소)
        연속 성공   → 간격을 조금씩 줄인다 (덧셈 증가), 설정값 아래로는 안 내려간다

    한도를 모르는 상태에서 가장 빠른 지속 가능 속도로 수렴한다.

    조정 폭은 실측으로 정했다. 처음엔 1.3배/20회로 잡았더니 유량 초과는 줄었지만
    (60초 15회 → 5회) 속도가 1 req/s 까지 떨어져 전체 처리량이 오히려 나빠졌다.
    거절 한 번의 비용은 요청 슬롯 하나뿐이라, 거절을 완전히 피하는 것보다
    **성공 요청 수를 최대화**하는 쪽이 맞다.

    max_per_second
    ---------------
    원래 덧셈 증가가 설정 속도(base)에서 멈췄다. 이름은 AIMD 인데 실제로는
    설정값을 천장으로 삼고 그 아래에서만 움직이는 반쪽이었다 — 서버에 여유가
    생겨도 알아낼 방법이 없다. 문서·실측 한도가 5 req/s 인데 3 으로 두고 있었으니
    KRX 1,783종목 한 바퀴가 594초였다(5 라면 357초).

    max_per_second 를 주면 거기까지 올라가며 실제 한도를 찾는다. 기본값은
    base 와 같아서 켜기 전엔 동작이 바뀌지 않는다 — 요청을 더 보내는 변경이
    기본으로 켜져 있으면 안 된다.
    """

    def __init__(self, per_second: float, max_interval: float = 2.0,
                 max_per_second: float | None = None):
        self.base_interval = 1.0 / per_second
        self.min_interval = 1.0 / max(max_per_second or per_second, per_second)
        self.interval = self.base_interval
        self.max_interval = max_interval
        self._next = 0.0
        self._ok_streak = 0
        self._lock = asyncio.Lock()
        self.backoffs = 0
        self.attempts = 0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next = max(now, self._next) + self.interval

    def on_success(self) -> None:
        self._ok_streak += 1
        self.attempts += 1
        if self._ok_streak >= 8 and self.interval > self.min_interval:
            self.interval = max(self.min_interval, self.interval * 0.90)
            self._ok_streak = 0

    def on_rate_limited(self) -> None:
        self._ok_streak = 0
        self.backoffs += 1
        self.attempts += 1
        self.interval = min(self.max_interval, self.interval * 1.15)
        self._next = max(self._next, time.monotonic() + self.interval)

    @property
    def current_rate(self) -> float:
        return 1.0 / self.interval

    @property
    def reject_pct(self) -> float:
        """거절 비율. 이 값 없이는 유량 설정이 맞는지 알 수 없다.
        너무 높으면 슬롯을 버리는 것이고, 0 에 가까우면 여유를 안 쓰는 것이다."""
        return (self.backoffs / self.attempts * 100.0) if self.attempts else 0.0




class KISRestAdapter(Adapter):
    name = "kis_rest"
    stale_after_s = 300.0
    measures_latency = False        # 폴링 주기가 곧 지연이라 네트워크 지연이 아니다

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.app_key = cfg.kis_app_key
        self.app_secret = cfg.kis_app_secret
        env = (getattr(cfg, "kis_env", "real") or "real").lower()
        self.rest_base = ENDPOINTS.get(env, ENDPOINTS["real"])[0]
        self.universe_path = getattr(cfg, "krx_universe_path", "data/reference/krx_symbols.csv")
        markets = set(getattr(cfg, "krx_markets", ["KOSPI"]))
        self.universe = load_universe(self.universe_path, markets,
                                      getattr(cfg, "krx_universe_limit", 0))
        self.names = dict(self.universe)
        self.limiter = AdaptiveRateLimiter(
            getattr(cfg, "kis_rest_rate", 3.0),
            max_per_second=getattr(cfg, "kis_rest_rate_max", 0) or None)
        self.rank_interval_s = getattr(cfg, "kis_rank_interval_s", 10.0)
        self.token_cache = os.path.expanduser(
            getattr(cfg, "kis_token_cache", "~/.mdfeed/kis_token.json"))
        self._tokens = TokenStore.get(self.app_key, self.app_secret,
                                      self.rest_base, self.token_cache)
        self._token: str | None = None
        self._token_exp = 0.0
        self._last_vol: dict[str, float] = {}
        self.rank_calls = 0
        self.poll_calls = 0
        self.rate_limited = 0
        self.sweep_laps = 0
        self.symbols_seen: set[str] = set()
        # 한 바퀴 도는 데 걸린 시간. 유니버스를 늘리면 데이터가 늘지 않고
        # 이 값이 길어진다 — 종목당 갱신 주기가 그만큼 늘어난다는 뜻이다.
        self._lap_started = 0.0
        self.last_lap_s = 0.0

    def enabled(self) -> bool:
        return bool(self.app_key and self.app_secret and self.universe)

    def disabled_reason(self) -> str:
        if not (self.app_key and self.app_secret):
            return "KIS_APP_KEY / KIS_APP_SECRET 미설정"
        return (f"종목 유니버스 없음: {self.universe_path} "
                f"(python scripts/fetch_krx_symbols.py 로 생성)")

    def expects_data(self) -> bool:
        return market_is_open()

    # ── 인증 ──────────────────────────────────────────────────────────────
    # 토큰은 kis_macro 와 **공유**한다. 예전엔 둘이 각자 발급해서,
    # 캐시가 만료된 콜드 스타트마다 재기동 한 번에 두 번씩 받았다.
    # KIS 는 1일 1회 발급이 원칙이고 잦으면 이용이 제한된다.
    async def _token_get(self) -> str:
        return await self._tokens.token()

    # ── HTTP ──────────────────────────────────────────────────────────────
    async def _call(self, path: str, tr_id: str, params: dict) -> dict | None:
        await self.limiter.acquire()
        tok = await self._token_get()

        def _do() -> dict:
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
                    return {"rt_cd": str(e.code), "msg_cd": "HTTP"}

        d = await asyncio.to_thread(_do)
        if d.get("msg_cd") == "EGW00201":
            # 유량 초과. 속도를 낮추고 물러난다.
            self.rate_limited += 1
            self.limiter.on_rate_limited()
            if self.registry:
                self.registry.counter("rest_rate_limited_total")
            return None
        if d.get("rt_cd") != "0":
            self.errors += 1
            return None
        self.limiter.on_success()
        return d

    # ── 발행 ──────────────────────────────────────────────────────────────
    def _publish_quote(self, code: str, price: float, acc_vol: float) -> None:
        """누적거래량 증가분만 합성 체결로 내보낸다."""
        if price <= 0:
            return
        ts = now_ns()
        self.symbols_seen.add(code)
        prev = self._last_vol.get(code)
        self._last_vol[code] = acc_vol
        if prev is None or acc_vol <= prev:
            return                                   # 첫 관측이거나 변화 없음
        self._mark(Trade(venue="KRX", symbol=code, ts_event_ns=ts, ts_recv_ns=ts,
                         price=price, qty=acc_vol - prev, side=SIDE_UNKNOWN))

    # ── 루프 ──────────────────────────────────────────────────────────────
    async def _rank_loop(self) -> None:
        """순위 API 스윕. 요청 하나에 30종목이라 커버리지 효율이 가장 좋다."""
        i = 0
        while True:
            label, path, tr, params = RANK_SPECS[i % len(RANK_SPECS)]
            i += 1
            d = await self._call(path, tr, params)
            if d:
                self.rank_calls += 1
                rows = d.get("output") or d.get("output1") or []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    code = r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd")
                    try:
                        px = float(r.get("stck_prpr") or 0)
                        vol = float(r.get("acml_vol") or 0)
                    except (TypeError, ValueError):
                        continue
                    if code:
                        self._publish_quote(code, px, vol)
            await asyncio.sleep(self.rank_interval_s / len(RANK_SPECS))

    async def _sweep_loop(self) -> None:
        """유니버스 라운드로빈. 순위에 안 잡히는 종목까지 결국 한 번씩 훑는다."""
        idx = 0
        while True:
            if not self.universe:
                await asyncio.sleep(5)
                continue
            code, _name = self.universe[idx % len(self.universe)]
            idx += 1
            if idx % len(self.universe) == 0:
                self.sweep_laps += 1
                now = time.time()
                if self._lap_started:
                    self.last_lap_s = now - self._lap_started
                self._lap_started = now
                log.info("[kis_rest] 유니버스 %d종목 %d회전 완료 "
                         "(관측 종목 %d개, 한 바퀴 %.0f초 = 종목당 갱신 주기)",
                         len(self.universe), self.sweep_laps,
                         len(self.symbols_seen), self.last_lap_s)
            d = await self._call("/uapi/domestic-stock/v1/quotations/inquire-price",
                                 TR_PRICE, {"FID_COND_MRKT_DIV_CODE": "J",
                                            "FID_INPUT_ISCD": code})
            if d:
                self.poll_calls += 1
                o = d.get("output") or {}
                try:
                    self._publish_quote(code, float(o.get("stck_prpr") or 0),
                                        float(o.get("acml_vol") or 0))
                except (TypeError, ValueError):
                    pass

    async def session(self) -> None:
        if not self.enabled():
            raise RuntimeError(self.disabled_reason())
        log.info("[kis_rest] 유니버스 %d종목 / 시작 유량 %.1f req/s (자동 조절) / "
                 "순위 스윕 %.0f초 주기",
                 len(self.universe), self.limiter.current_rate, self.rank_interval_s)
        await asyncio.gather(self._rank_loop(), self._sweep_loop())

    def health(self) -> dict:
        d = super().health()
        laps = self.sweep_laps
        seen = len(self.symbols_seen)
        d.update({
            "universe": len(self.universe),
            "symbols_observed": seen,
            "coverage_pct": round(seen / len(self.universe) * 100, 1) if self.universe else 0,
            "rank_calls": self.rank_calls,
            "poll_calls": self.poll_calls,
            "sweep_laps": laps,
            # 종목당 갱신 주기. 아직 한 바퀴를 못 돌았으면 현재 유량으로 추정한다.
            "lap_s": round(self.last_lap_s, 1) if self.last_lap_s else None,
            "projected_lap_s": round(
                len(self.universe) / max(self.limiter.current_rate, 0.1), 1),
            "rate_limited": self.rate_limited,
            "current_rate_per_s": round(self.limiter.current_rate, 2),
            "max_rate_per_s": round(1.0 / self.limiter.min_interval, 2),
            "backoffs": self.limiter.backoffs,
            "reject_pct": round(self.limiter.reject_pct, 2),
            "market_open": market_is_open(),
        })
        return d
