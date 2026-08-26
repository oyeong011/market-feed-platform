"""한국투자증권(KIS) 국내주식 실시간 체결 어댑터.

크립토 어댑터와 달리 자격증명이 필요하다. 키가 없으면 스스로 비활성화되고
feedd 는 나머지 어댑터로 정상 기동한다 — 선택적 업스트림이 전체 서비스를
막지 않게 하는 게 운영 관점의 핵심이다.

프로토콜 메모
-------------
* 접속키(approval_key)를 HTTPS POST /oauth2/Approval 로 먼저 받는다.
  (계좌 주문용 access_token 과는 다른 키다)
* 실시간은 평문 ws:// (ops.koreainvestment.com:21000, 모의투자는 31000).
* 구독/해지는 JSON, 그런데 **시세 본문은 파이프(|)와 캐럿(^) 구분 문자열**이다.
      0|H0STCNT0|001|005930^093015^70000^...
      [0]암호화여부 [1]TR_ID [2]데이터건수 [3]본문(^ 구분)
* 체결(H0STCNT0) 필드 인덱스는 KIS 문서 기준으로 아래 상수에 고정했다.
  문서가 바뀌면 여기만 고치면 된다.
* 하나의 소켓에 41건까지만 등록 가능 → 종목 수 초과 시 잘라내고 경고한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import urllib.request

from ..models import BookTop, Trade, SIDE_BUY, SIDE_SELL, now_ns
from ..wsproto import WSClient
from .base import Adapter

log = logging.getLogger("mdfeed.adapter.kis")

REST_REAL = "https://openapi.koreainvestment.com:9443"
WS_REAL = "ws://ops.koreainvestment.com:21000"
TR_TRADE = "H0STCNT0"
MAX_SUBSCRIPTIONS = 41

# H0STCNT0 본문 필드 인덱스 (KIS 실시간 시세 명세)
F_CODE, F_TIME, F_PRICE = 0, 1, 2
F_ASK1, F_BID1, F_VOL = 10, 11, 12
F_SIGN = 21          # 체결구분: 1 매수, 3 매도, 5 장전


class KISAdapter(Adapter):
    name = "kis"
    stale_after_s = 120.0

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.app_key = cfg.kis_app_key
        self.app_secret = cfg.kis_app_secret
        self.symbols = cfg.kis_symbols[:MAX_SUBSCRIPTIONS]
        if len(cfg.kis_symbols) > MAX_SUBSCRIPTIONS:
            log.warning("KIS 소켓당 등록 한도 %d 초과 → 앞의 %d개만 구독",
                        MAX_SUBSCRIPTIONS, MAX_SUBSCRIPTIONS)
        self._approval_key: str | None = None

    def enabled(self) -> bool:
        return bool(self.app_key and self.app_secret)

    def disabled_reason(self) -> str:
        return "KIS_APP_KEY / KIS_APP_SECRET 미설정"

    async def _get_approval_key(self) -> str:
        """블로킹 HTTP 호출을 스레드로 밀어 이벤트 루프를 막지 않는다."""
        def _call() -> str:
            body = json.dumps({
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "secretkey": self.app_secret,
            }).encode()
            req = urllib.request.Request(
                f"{REST_REAL}/oauth2/Approval", data=body,
                headers={"content-type": "application/json; charset=utf-8"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())["approval_key"]
        return await asyncio.to_thread(_call)

    async def session(self) -> None:
        if not self.enabled():
            raise RuntimeError(self.disabled_reason())
        if not self._approval_key:
            self._approval_key = await self._get_approval_key()

        ws = await WSClient.connect(WS_REAL)
        try:
            for code in self.symbols:
                await ws.send_text(json.dumps({
                    "header": {"approval_key": self._approval_key, "custtype": "P",
                               "tr_type": "1", "content-type": "utf-8"},
                    "body": {"input": {"tr_id": TR_TRADE, "tr_key": code}},
                }))
                await asyncio.sleep(0.05)      # 등록 요청 폭주 방지
            while True:
                _op, raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_after_s)
                self._on_message(raw)
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    def _on_message(self, raw: bytes) -> None:
        text = raw.decode("utf-8", "ignore")
        if text.startswith("{"):               # 등록 응답 / PINGPONG 제어 메시지
            return
        parts = text.split("|")
        if len(parts) < 4 or parts[1] != TR_TRADE:
            return
        ts_recv = now_ns()
        # 한 프레임에 여러 체결이 붙어 올 수 있다(데이터 건수 = parts[2])
        f = parts[3].split("^")
        try:
            count = int(parts[2])
        except ValueError:
            count = 1
        stride = len(f) // count if count else len(f)
        for i in range(count):
            g = f[i * stride: (i + 1) * stride]
            if len(g) <= F_SIGN:
                continue
            try:
                price = float(g[F_PRICE]); vol = float(g[F_VOL])
            except ValueError:
                continue
            side = SIDE_BUY if g[F_SIGN] == "1" else (SIDE_SELL if g[F_SIGN] == "3" else 0)
            self._mark(Trade(
                venue="KIS", symbol=g[F_CODE],
                ts_event_ns=ts_recv,   # KIS 는 HHMMSS 만 줘 날짜를 합성해야 한다.
                                       # 조용히 틀린 날짜를 만드느니 수신시각을 쓰고
                                       # 지연시간 지표에서 제외한다(DESIGN.md 참고)
                ts_recv_ns=ts_recv, price=price, qty=vol, side=side,
            ))
            try:
                bid, ask = float(g[F_BID1]), float(g[F_ASK1])
            except (ValueError, IndexError):
                continue
            if bid and ask:
                self._mark(BookTop(
                    venue="KIS", symbol=g[F_CODE], ts_event_ns=ts_recv,
                    ts_recv_ns=ts_recv, bid=bid, bid_qty=0.0, ask=ask, ask_qty=0.0,
                ))
