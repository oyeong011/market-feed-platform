"""바이낸스 실시간 체결 어댑터 (USDT 마켓).

프로토콜 메모
-------------
* combined stream 을 쓴다: /stream?streams=btcusdt@trade/ethusdt@trade
  스트림마다 소켓을 여는 대신 하나로 묶어 파일디스크립터와 재접속 비용을 줄인다.
* 응답은 텍스트 프레임. 가격/수량이 **문자열**로 온다(정밀도 보존 목적).
  float 변환은 우리 쪽 책임.
* 서버가 3분마다 PING 프레임을 보내고 PONG 을 안 하면 끊는다.
  → WSClient.recv() 안에서 자동 응답하게 만들어 뒀다.
* `m` 필드는 "매수자가 메이커인가". True 면 시장가 매도가 체결된 것이므로
  공격 방향(aggressor)은 SELL 이다. 이 부호를 뒤집으면 주문흐름 지표가 통째로
  반대가 된다.
"""

from __future__ import annotations

import asyncio
import logging
import contextlib
import json

from ..models import Trade, SIDE_BUY, SIDE_SELL, now_ns
from ..wsproto import WSClient
from .base import Adapter, _resolve_symbols

log = logging.getLogger("mdfeed.binance")

BASE = "wss://stream.binance.com:9443/stream?streams="
STREAM_ENDPOINT = "wss://stream.binance.com:9443/stream"
# URL 로 구독하면 종목 수가 곧 요청줄 길이가 된다. 400종목에서 6,023바이트가
# 나왔고, 요청줄 상한(보통 4~8KB)을 넘으면 핸드셰이크 단계에서 끊긴다.
# 접속 후 SUBSCRIBE 메시지로 나눠 보내면 그 한계가 사라진다.
URL_STREAM_MAX = 50          # 이 이하면 URL 방식(디버깅이 쉽다)
SUBSCRIBE_CHUNK = 200        # 바이낸스 문서상 한 요청당 스트림 상한


class BinanceAdapter(Adapter):
    name = "binance"
    stale_after_s = 60.0

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.symbols = [s.lower() for s in _resolve_symbols(
            cfg, "binance_symbols", "BINANCE",
            getattr(cfg, "binance_universe_limit", 0), log)]

    def _streams(self) -> list[str]:
        return [f"{s}@trade" for s in self.symbols]

    def _url(self) -> str:
        return BASE + "/".join(self._streams())

    async def _subscribe(self, ws) -> None:
        """접속 후 SUBSCRIBE 로 나눠 구독한다.

        한 번에 다 보내면 상한에 걸린다. 나눠 보내되 몇 개를 보냈는지
        로그에 남긴다 — 조용히 앞쪽 N개만 구독되면 뒤쪽 종목은 영영
        안 오는데, 그게 "거래가 없는 종목"과 구분되지 않는다.
        """
        streams = self._streams()
        for i in range(0, len(streams), SUBSCRIBE_CHUNK):
            chunk = streams[i:i + SUBSCRIBE_CHUNK]
            await ws.send_text(json.dumps(
                {"method": "SUBSCRIBE", "params": chunk, "id": i // SUBSCRIBE_CHUNK + 1}))
        log.info("[binance] %d종목 구독 요청 (%d회 분할)", len(streams),
                 (len(streams) + SUBSCRIBE_CHUNK - 1) // SUBSCRIBE_CHUNK)

    async def session(self) -> None:
        many = len(self.symbols) > URL_STREAM_MAX
        ws = await WSClient.connect(STREAM_ENDPOINT if many else self._url())
        if many:
            await self._subscribe(ws)
        try:
            while True:
                _op, raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_after_s)
                self._on_message(raw)
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    def _on_message(self, raw: bytes) -> None:
        try:
            env = json.loads(raw)
        except json.JSONDecodeError:
            self.errors += 1
            return
        d = env.get("data") or env
        if d.get("e") != "trade":
            return
        self._mark(Trade(
            venue="BINANCE", symbol=d["s"],
            ts_event_ns=int(d["T"]) * 1_000_000, ts_recv_ns=now_ns(),
            price=float(d["p"]), qty=float(d["q"]),
            # m=True → 매수자가 메이커 → 공격자는 매도자
            side=SIDE_SELL if d.get("m") else SIDE_BUY,
        ))
