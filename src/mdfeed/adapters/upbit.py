"""업비트 실시간 체결 어댑터 (KRW 마켓).

프로토콜 메모
-------------
* 엔드포인트: wss://api.upbit.com/websocket/v1
* 구독은 JSON 배열 한 번: [{ticket}, {type, codes}, {format}]
* 응답은 **바이너리 opcode(0x2)** 인데 내용은 UTF-8 JSON이다. 텍스트 프레임만
  처리하도록 짜면 조용히 아무것도 못 받는다 — 실제로 흔한 함정이라 둘 다 받는다.
* 무거래 구간에 서버가 끊으므로 주기적 PING 프레임이 필요하다.
* trade 스트림에 best_ask/best_bid 가 함께 실려 와서, 별도 orderbook 구독 없이
  BBO(최우선호가)까지 만들 수 있다. 구독 대역폭을 아끼는 선택.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from ..models import BookTop, Trade, SIDE_BUY, SIDE_SELL, now_ns
from ..wsproto import WSClient
from .base import Adapter

URL = "wss://api.upbit.com/websocket/v1"


class UpbitAdapter(Adapter):
    name = "upbit"
    stale_after_s = 90.0        # 새벽 한산한 시간엔 분 단위로 거래가 비기도 한다
    ping_interval_s = 20.0

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.symbols = cfg.upbit_symbols

    async def session(self) -> None:
        ws = await WSClient.connect(URL)
        sub = [
            {"ticket": f"mdfeed-{int(time.time())}"},
            {"type": "trade", "codes": self.symbols, "isOnlyRealtime": False},
            {"format": "DEFAULT"},
        ]
        await ws.send_text(json.dumps(sub))

        pinger = asyncio.create_task(self._ping_loop(ws))
        try:
            while True:
                _op, raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_after_s)
                self._on_message(raw)
        finally:
            pinger.cancel()
            with contextlib.suppress(Exception):
                await pinger
            with contextlib.suppress(Exception):
                await ws.close()

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_s)
            await ws.ping()

    def _on_message(self, raw: bytes) -> None:
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            self.errors += 1
            return
        if m.get("type") != "trade":
            return

        ts_recv = now_ns()
        # trade_timestamp 는 ms. 거래소가 체결을 확정한 시각이다
        ts_event = int(m["trade_timestamp"]) * 1_000_000
        side = SIDE_BUY if m.get("ask_bid") == "BID" else SIDE_SELL
        self._mark(Trade(
            venue="UPBIT", symbol=m["code"],
            ts_event_ns=ts_event, ts_recv_ns=ts_recv,
            price=float(m["trade_price"]), qty=float(m["trade_volume"]), side=side,
        ))

        bid, ask = m.get("best_bid_price"), m.get("best_ask_price")
        if bid and ask:
            self._mark(BookTop(
                venue="UPBIT", symbol=m["code"],
                ts_event_ns=ts_event, ts_recv_ns=ts_recv,
                bid=float(bid), bid_qty=float(m.get("best_bid_size") or 0),
                ask=float(ask), ask_qty=float(m.get("best_ask_size") or 0),
            ))
