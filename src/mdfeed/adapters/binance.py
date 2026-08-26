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
import contextlib
import json

from ..models import Trade, SIDE_BUY, SIDE_SELL, now_ns
from ..wsproto import WSClient
from .base import Adapter

BASE = "wss://stream.binance.com:9443/stream?streams="


class BinanceAdapter(Adapter):
    name = "binance"
    stale_after_s = 60.0

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.symbols = [s.lower() for s in cfg.binance_symbols]

    def _url(self) -> str:
        return BASE + "/".join(f"{s}@trade" for s in self.symbols)

    async def session(self) -> None:
        ws = await WSClient.connect(self._url())
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
