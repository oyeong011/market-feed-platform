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
import csv
import os
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


def load_universe(path: str, markets: set[str], limit: int = 0) -> list[tuple[str, str]]:
    """종목 마스터 CSV(market,code,name)에서 (코드, 이름) 목록을 읽는다.

    KRX 도 암호화폐도 같은 형식을 쓴다. 유니버스 관리가 갈리면 한쪽에만
    한도·필터가 붙고 다른 쪽은 조용히 빠진다.
    """
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("market") in markets:
                out.append((row["code"], row.get("name", "")))
    return out[:limit] if limit else out


def _resolve_symbols(cfg, attr: str, market: str, limit: int, log) -> list[str]:
    """구독 종목을 정한다. 한도가 0이면 명시 목록, >0 이면 마스터 앞의 N개.

    유니버스 파일이 없거나 비면 명시 목록으로 되돌아간다 — 종목이 0개면
    어댑터는 붙긴 하는데 아무것도 안 오고, 그게 장애와 구분되지 않는다.
    """
    explicit = list(getattr(cfg, attr, []) or [])
    if limit <= 0:
        return explicit
    path = getattr(cfg, "crypto_universe_path", "")
    uni = [code for code, _ in load_universe(path, {market}, limit)]
    if not uni:
        log.warning("[%s] 유니버스 %s 를 못 읽었다 — 명시 목록 %d종목으로 진행 "
                    "(python scripts/fetch_crypto_symbols.py 로 생성)",
                    market.lower(), path, len(explicit))
        return explicit
    log.info("[%s] 유니버스 %d종목 구독 (한도 %d)", market.lower(), len(uni), limit)
    return uni


class Adapter(abc.ABC):
    name: str = "base"
    stale_after_s: float = 60.0
    ping_interval_s: float = 30.0

    # 이 업스트림의 지연시간과 시계 오프셋을 측정할 가치가 있는가.
    # 실시간 거래소는 True. 리플레이는 False — 녹화 시각과 현재 시각의 차이는
    # 시계 오차도 네트워크 지연도 아니라 그냥 "언제 녹화했는가"일 뿐인데,
    # 이걸 지연 지표에 넣으면 시계 오프셋이 수천 초로 나와 알람이 거짓으로 울린다.
    # (CI 에서 실제로 "시계 오프셋 2,343,288ms" 경고가 떴다)
    measures_latency: bool = True

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

    def expects_data(self) -> bool:
        """지금 이 업스트림에서 데이터가 와야 정상인가.

        24시간 도는 암호화폐는 항상 True. 국내 주식은 장이 열려 있을 때만 True다.
        정체 판정은 반드시 이 훅을 거친다 — 장이 닫혀 조용한 것과
        피드가 죽어 조용한 것은 겉보기가 같아서, 구분하지 않으면
        휴장 중에 30초마다 재접속하는 폭풍이 된다.
        """
        return True

    # ── 공통 ──────────────────────────────────────────────────────────────
    def _mark(self, msg, measure: bool = True) -> None:
        """어댑터가 정규화 메시지를 하나 만들 때마다 호출.

        measure=False 는 구독 직후 받는 스냅샷용이다. 스냅샷의 체결 시각은
        "마지막으로 거래된 때"라서 거래가 뜸한 종목이면 몇 시간 전이다.
        그걸 수집 지연으로 세면 지표가 통째로 망가진다 — 실측에서 업비트를
        4종목에서 288종목으로 늘리자 p99 가 106초, max 가 53분으로 나왔고,
        전부 스냅샷이었다. 데이터는 쓰되 지연에는 넣지 않는다.
        """
        self.messages += 1
        self.last_msg_at = time.time()
        if self.registry:
            venue = self.name.upper()
            self.registry.counter("ticks_total", venue=venue)
            if not measure:
                self.registry.counter("snapshot_msgs_total", venue=venue)
            if measure and self.measures_latency and hasattr(msg, "latency_us"):
                raw = msg.latency_us
                # 원시값과 시계 보정값을 둘 다 남긴다. 보정 로직이 틀렸을 때
                # 원시값이 없으면 그 사실조차 알 수 없다.
                self.registry.observe("ingest_latency_raw", abs(raw), venue=venue)
                self.registry.observe("ingest_latency", CLOCK.observe(venue, raw),
                                      venue=venue)
        self.emit(msg)

    async def _stale_watchdog(self) -> None:
        """업무 메시지 기준으로 정체를 감시한다.

        소켓 recv 타임아웃만으로는 부족하다. 거래소가 체결이 아닌 프레임
        (핑퐁·상태 통지)을 계속 보내면 recv 는 매번 성공해 타임아웃이 리셋되고,
        정작 시세는 몇 시간째 끊겨 있어도 재접속이 일어나지 않는다.
        실제로 업비트에서 9.6시간 동안 reconnects=0 인 채 시세만 멎었다.

        그래서 recv 가 아니라 `_mark()` 가 갱신하는 last_msg_at 을 본다.
        판정되면 세션 코루틴을 취소해 감독 루프의 재접속 경로로 보낸다.
        """
        # 첫 메시지를 아직 못 받은 세션에도 기한을 준다
        deadline = time.time() + self.stale_after_s
        while True:
            await asyncio.sleep(min(self.stale_after_s / 2, 15.0))
            if not self.expects_data():
                # 휴장 중엔 조용한 게 정상이다. 기한을 미뤄 두지 않으면
                # 개장 직후 첫 체결이 오기도 전에 한 번 끊고 들어간다.
                deadline = time.time() + self.stale_after_s
                self.last_msg_at = 0.0
                continue
            last = self.last_msg_at or deadline
            idle = time.time() - last
            if idle > self.stale_after_s:
                log.warning(
                    "[%s] 업무 메시지 %.0fs 정체 — 소켓은 살아 있으나 세션을 끊는다",
                    self.name, idle,
                )
                if self.registry:
                    self.registry.counter("stale_restarts_total", venue=self.name.upper())
                return

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                log.info("[%s] 세션 시작", self.name)
                await self._session_with_watchdog()
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

    CANCEL_TIMEOUT_S = 5.0

    async def _abandon(self, task: asyncio.Future, what: str) -> None:
        """취소를 기다리되 기한을 두고, 안 끝나면 버리고 진행한다.

        **복구가 정리의 성공에 의존하면 안 된다.** 취소한 코루틴의 ``finally``
        가 죽은 소켓에서 멈추면, 그걸 기다리는 재접속 경로도 같이 멈춘다.
        asyncio 는 이미 취소를 처리 중인 태스크에 CancelledError 를 다시
        보내지 않으므로, 그 대기는 **스스로 풀리지 않는다.**

        실측(2026-08-29): upbit 이 11.2시간 멎었는데 재접속은 3회,
        태스크 사망 0, 정체 판정은 켜져 있었다. 감시도 지표도 경보도 전부
        제대로 돌았는데 **복구만 안 됐다.** 원인은 `await session` 이
        기한 없이 서 있었던 것이다.

        여기서는 취소를 두 번 시도한다. 두 번째 취소는 ``finally`` 안에서
        기다리던 지점을 깨울 수 있다. 그래도 안 끝나면 태스크를 버리고
        재접속으로 넘어간다. 버린 태스크는 지표에 남긴다 — 조용히 새는
        태스크가 있다는 사실 자체가 알려져야 한다.
        """
        for attempt in (1, 2):
            task.cancel()
            _done, pending = await asyncio.wait({task}, timeout=self.CANCEL_TIMEOUT_S)
            if not pending:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    task.result()             # 예외를 읽어 경고 로그를 막는다
                return
            if attempt == 2:
                log.error(
                    "[%s] %s 취소가 %.0fs 안에 안 끝났다 — 버리고 재접속한다",
                    self.name, what, self.CANCEL_TIMEOUT_S * 2)
                # 버린 태스크의 예외는 아무도 안 읽으므로 여기서 삼킨다
                task.add_done_callback(
                    lambda t: t.cancelled() or t.exception())
                if self.registry:
                    self.registry.counter("session_cancel_timeouts_total",
                                          venue=self.name.upper())

    async def _session_with_watchdog(self):
        """세션과 정체 감시를 나란히 돌린다. 감시가 먼저 끝나면 세션을 취소한다."""
        session = asyncio.ensure_future(self.session())
        guard = asyncio.ensure_future(self._stale_watchdog())
        abandoned: set = set()
        try:
            done, _ = await asyncio.wait(
                {session, guard}, return_when=asyncio.FIRST_COMPLETED
            )
            if guard in done and session not in done:
                await self._abandon(session, "세션")
                abandoned.add(session)
                raise StaleSessionError(
                    f"{self.name}: 업무 메시지 {self.stale_after_s:.0f}s 이상 정체"
                )
            return session.result()
        finally:
            # 이미 버린 태스크를 여기서 또 버리면 기한을 두 번 기다린다.
            # 정체 복구가 10초가 아니라 20초가 되고, 버림 지표도 두 번 오른다
            # (그 지표가 SessionCancelTimeout 경보를 움직인다).
            for t, what in ((session, "세션"), (guard, "정체 감시")):
                if t not in abandoned and not t.done():
                    await self._abandon(t, what)

    def stop(self) -> None:
        self._stop.set()

    @property
    def is_stale(self) -> bool:
        if not self.expects_data():
            return False
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
            # 구독하기로 한 종목 수. 지금까지 **본** 종목 수와 다르다.
            #
            # 2026-09-03: 재기동 한 번에 유니버스가 4,327 → 7 종목으로
            # 줄었는데 상태판에는 아무 표시가 없었다. 한도 설정이 셸 환경변수에만
            # 있었고, 다른 셸에서 띄우니 조용히 기본값으로 돌아갔다.
            # 헬스가 "본 종목 수"만 내면 장 마감이라 안 오는 것과
            # 구독 자체를 안 한 것이 구분되지 않는다.
            # 어댑터마다 이름이 다르다. WS 구독형은 symbols, KRX 광역
            # 폴링형은 universe 다. 하나만 세면 3,554종목이 0 으로 보인다 —
            # "설정이 사라졌다"와 똑같이 생겨서 오진을 부른다.
            "symbols_subscribed": len(getattr(self, "symbols", None)
                                      or getattr(self, "universe", None) or []),
            "expects_data": self.expects_data(),
            "measures_latency": self.measures_latency,
        }


class StaleSessionError(RuntimeError):
    """업무 메시지가 끊겨 세션을 강제 종료했을 때."""


class StaleGuard:
    """세션 안에서 recv 타임아웃을 관리하는 도우미."""

    def __init__(self, adapter: Adapter):
        self.adapter = adapter

    async def recv(self, ws, timeout: float | None = None):
        t = timeout if timeout is not None else self.adapter.stale_after_s
        return await asyncio.wait_for(ws.recv(), timeout=t)
