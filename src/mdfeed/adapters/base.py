"""어댑터 공통 골격 — 감독 루프, 재접속 백오프, 정체(staleness) 감지.

거래소 세션은 반드시 끊긴다. 점검, 네트워크 순단, 상대 서버의 idle timeout.
따라서 어댑터의 본질은 "파싱"이 아니라 "끊겨도 알아서 살아나는 것"이다.

여기서 구현한 것
----------------
* 지수 백오프 + 지터: 여러 어댑터가 동시에 끊겼을 때 재접속이 한 시점에
  몰려 상대 서버를 때리는 thundering herd 를 피한다.
* 정체 감지: 연결은 살아있는데 데이터가 안 오는 상태(TCP half-open, 구독 유실)가
  실제로 가장 흔한 장애다. stale_after_s 동안 메시지가 없으면 스스로 끊고 다시 붙는다.
* 애플리케이션 하트비트: 거래소가 요구하는 주기적 ping.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import random
import time
from typing import Callable

from ..clock import ClockMonitor

log = logging.getLogger("mdfeed.adapter")

# 어댑터 전체가 공유하는 시계 감시기. venue 별로 오프셋을 따로 추정한다.
CLOCK = ClockMonitor()


class Adapter(abc.ABC):
    name: str = "base"
    stale_after_s: float = 60.0
    ping_interval_s: float = 30.0

    def __init__(self, cfg, emit: Callable, registry=None):
        self.cfg = cfg
        self.emit = emit                # emit(msg) — Trade | BookTop
        self.registry = registry
        self.last_msg_at: float = 0.0
        self.messages = 0
        self.reconnects = 0
        self.errors = 0
        self._stop = asyncio.Event()

    # ── 하위 클래스가 구현 ────────────────────────────────────────────────
    @abc.abstractmethod
    async def session(self) -> None:
        """한 번의 연결 수명주기. 끊기면 예외를 던지고 반환한다."""

    def enabled(self) -> bool:
        return True

    def disabled_reason(self) -> str:
        return ""

    # ── 공통 ──────────────────────────────────────────────────────────────
    def _mark(self, msg) -> None:
        """어댑터가 정규화 메시지를 하나 만들 때마다 호출."""
        self.messages += 1
        self.last_msg_at = time.time()
        if self.registry:
            venue = self.name.upper()
            self.registry.counter("ticks_total", venue=venue)
            if hasattr(msg, "latency_us"):
                raw = msg.latency_us
                # 원시값과 시계 보정값을 둘 다 남긴다. 보정 로직이 틀렸을 때
                # 원시값이 없으면 그 사실조차 알 수 없다.
                self.registry.observe("ingest_latency_raw", abs(raw))
                self.registry.observe("ingest_latency", CLOCK.observe(venue, raw))
        self.emit(msg)

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                log.info("[%s] 세션 시작", self.name)
                await self.session()
                backoff = 1.0                        # 정상 종료 → 백오프 리셋
            except asyncio.CancelledError:
                raise
            except Exception as e:                   # noqa: BLE001
                self.errors += 1
                if self.registry:
                    self.registry.counter("adapter_errors_total", venue=self.name.upper())
                log.warning("[%s] 세션 종료: %s: %s", self.name, type(e).__name__, e)

            if self._stop.is_set():
                break
            self.reconnects += 1
            if self.registry:
                self.registry.counter("reconnects_total", venue=self.name.upper())
            # 지터를 섞어 동시 재접속 폭주를 흩는다
            delay = min(backoff, 30.0) * (0.5 + random.random())
            log.info("[%s] %.1fs 후 재접속 (%d번째)", self.name, delay, self.reconnects)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            backoff = min(backoff * 2, 30.0)

    def stop(self) -> None:
        self._stop.set()

    @property
    def is_stale(self) -> bool:
        return bool(self.last_msg_at) and (time.time() - self.last_msg_at) > self.stale_after_s

    def health(self) -> dict:
        age = (time.time() - self.last_msg_at) if self.last_msg_at else None
        return {
            "venue": self.name,
            "enabled": self.enabled(),
            "messages": self.messages,
            "reconnects": self.reconnects,
            "errors": self.errors,
            "last_msg_age_s": round(age, 1) if age is not None else None,
            "stale": self.is_stale,
        }


class StaleGuard:
    """세션 안에서 recv 타임아웃을 관리하는 도우미."""

    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    async def recv(self, ws, timeout: float | None = None):
        t = timeout if timeout is not None else self.adapter.stale_after_s
        return await asyncio.wait_for(ws.recv(), timeout=t)
