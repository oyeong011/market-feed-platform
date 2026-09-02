"""strategy — 피드 위에 지표를 얹어 매매 시그널을 만드는 프로세스.

    UDS 버스(틱) ─▶ 1분봉 집계 ─▶ 전략(증분 지표) ─▶ SIGNAL ─▶ signals.sock

이 프로세스는 **버스의 소비자이면서 동시에 다른 버스의 발행자**다. 다단 파이프라인
구조를 이 한 프로세스가 증명한다. writer 와 ws-gateway 가 signals.sock 을 함께
구독해 각각 DB 적재와 브라우저 알림을 담당한다.

봉이 닫힐 때만 판단하는 이유
----------------------------
틱마다 지표를 다시 보면 같은 봉 안에서 조건이 들락날락하며 시그널이 폭주한다.
봉 종가 기준으로 확정된 값만 쓰는 것이 백테스트와도 일치하는 유일한 방법이다.

쿨다운
------
같은 심볼·전략에서 연속 시그널을 억제한다. 없으면 경계선 부근에서 진동할 때
분당 수십 건이 나가고, 그건 시그널이 아니라 노이즈다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from ..bus import UDSPublisher, UDSSubscriber
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import MSG_SIGNAL, MSG_TRADE, Bar, Signal, Trade, now_ns
from ..protocol import encode
from ..strategies import HOLD, REGISTRY as STRAT_REGISTRY, SignalGate

log = logging.getLogger("mdfeed.strategy")
SERVICE = "strategy"


class StrategyEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "bars_closed_total", "signals_suppressed_total")
        self.pub = UDSPublisher(cfg.signal_bus_path, cfg.bus_queue_size)
        self.seq = 0
        self._bars: dict[str, Bar] = {}                 # key → 진행 중인 봉
        self._strats: dict[str, dict[str, object]] = {} # key → {전략명: 인스턴스}
        # 쿨다운은 백테스트와 **같은 구현**을 쓴다. strategies.SignalGate 참고.
        self.gate = SignalGate(cfg.signal_cooldown_s)
        # 쿨다운은 시장 시각으로 잰다. 봉 간격보다 짧으면 같은 (종목, 전략)의
        # 두 시그널이 항상 그보다 멀리 떨어져 있어 **절대 발동하지 않는다.**
        # 켜 뒀다고 믿는 장치가 실은 꺼져 있는 상태라 기동 때 밝힌다.
        if 0 < cfg.signal_cooldown_s < cfg.bar_interval_s:
            log.warning(
                "SIGNAL_COOLDOWN_S=%.0f초가 BAR_INTERVAL_S=%d초보다 짧다 — "
                "봉 마감 간격이 항상 더 크므로 이 설정은 발동하지 않는다. "
                "억제하려면 봉 간격보다 크게 잡을 것",
                cfg.signal_cooldown_s, cfg.bar_interval_s)
        self.signals_emitted = 0
        self.bars_closed = 0
        self.frames_in = 0
        self.last_frame_at = 0.0
        self.upstream_ok = False
        self.recent: list[dict] = []
        from ..supervisor import Supervisor
        self.sup = Supervisor(SERVICE, self.registry)
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()

    def _strategies_for(self, key: str) -> dict:
        s = self._strats.get(key)
        if s is None:
            s = self._strats[key] = {
                n: STRAT_REGISTRY[n]()
                for n in self.cfg.strategies if n in STRAT_REGISTRY
            }
        return s

    def _on_trade(self, t: Trade) -> None:
        key = f"{t.venue}:{t.symbol}"
        iv_ns = self.cfg.bar_interval_s * 1_000_000_000
        bucket = (t.ts_event_ns // iv_ns) * iv_ns
        bar = self._bars.get(key)

        if bar is not None and bar.bucket_ns != bucket:
            self._close_bar(key, bar)               # 새 버킷 → 직전 봉 확정
            bar = None
        if bar is None:
            bar = self._bars[key] = Bar(t.venue, t.symbol, bucket,
                                        self.cfg.bar_interval_s)
        bar.update(t)

    def _close_bar(self, key: str, bar: Bar) -> None:
        self.bars_closed += 1
        self.registry.counter("bars_closed_total")
        for name, strat in self._strategies_for(key).items():
            try:
                action = strat.on_bar(bar)
            except Exception as e:                  # noqa: BLE001
                log.error("전략 %s 오류 (%s): %s", name, key, e)
                continue
            if action == HOLD:
                continue
            # 시장 시각으로 잰다. 벽시계로 재면 같은 테이프를 다시 흘려도
            # 결과가 달라져 백테스트와 대조가 성립하지 않는다.
            if not self.gate.allow(key, name, bar.bucket_ns):
                self.registry.counter("signals_suppressed_total")
                continue
            self._emit(bar, name, action)

    def _emit(self, bar: Bar, strategy: str, action: int) -> None:
        sig = Signal(venue=bar.venue, symbol=bar.symbol, ts_ns=now_ns(),
                     strategy=strategy, action=action, strength=1.0,
                     ref_price=bar.close)
        self.pub.publish(encode(MSG_SIGNAL, self.seq, sig.pack()))
        self.seq += 1
        self.signals_emitted += 1
        self.registry.counter("signals_total", strategy=strategy)
        d = sig.to_dict()
        d["bar_ts"] = bar.bucket_ns
        self.recent.append(d)
        del self.recent[:-50]
        log.info("SIGNAL %s %s %s @ %s", sig.venue, sig.symbol, d["action_name"], bar.close)

    async def _consume(self, stop: asyncio.Event) -> None:
        paths = self.cfg.bus_paths or [self.cfg.bus_path]
        await asyncio.gather(*(self._consume_one(p, stop) for p in paths))

    async def _consume_one(self, path: str, stop: asyncio.Event) -> None:
        sub = UDSSubscriber(path, name=SERVICE)
        async for frame in sub.frames():
            if stop.is_set():
                return
            self.frames_in += 1
            self.last_frame_at = time.time()
            self.upstream_ok = True
            if frame.msg_type == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
                self._on_trade(Trade.unpack(frame.payload))

    async def _sweep(self, stop: asyncio.Event) -> None:
        """거래가 끊긴 심볼의 봉도 시간이 지나면 확정한다.

        다음 틱을 기다려 봉을 닫으면, 거래가 멈춘 종목은 영원히 닫히지 않는다.
        """
        while not stop.is_set():
            await asyncio.sleep(self.cfg.bar_interval_s / 4)
            iv_ns = self.cfg.bar_interval_s * 1_000_000_000
            cur = (now_ns() // iv_ns) * iv_ns
            for key, bar in list(self._bars.items()):
                if bar.bucket_ns < cur:
                    self._close_bar(key, bar)
                    del self._bars[key]

    def health(self) -> dict:
        age = time.time() - self.last_frame_at if self.last_frame_at else None
        ready = sum(1 for d in self._strats.values()
                    for s in d.values() if getattr(s, "ready", False))
        return {
            # 태스크별 상태. 합쳐서 세면 하나가 죽어도 안 보인다 —
            # 그게 8/28·8/31·9/1·9/2 사고의 공통점이었다.
            **self.sup.report(),
            "service": SERVICE,
            "healthy": self.upstream_ok and (age is None or age < 30.0),
            "uptime_s": round(time.time() - self._started, 1),
            "upstream_connected": self.upstream_ok,
            "last_frame_age_s": round(age, 1) if age is not None else None,
            "frames_in": self.frames_in,
            "symbols": len(self._strats),
            "strategies": self.cfg.strategies,
            "warmed_up": ready,
            "bars_closed": self.bars_closed,
            "signals_emitted": self.signals_emitted,
            "subscribers": self.pub.subscriber_count,
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        await self.pub.start()
        http = HTTPServer(cfg.http_host, cfg.strategy_admin_port, SERVICE, self.registry)
        health_routes(http, self.health, tracker=self.tracker)
        http.route("GET", "/signals", lambda r: Response.json(
            {"count": self.signals_emitted, "recent": list(reversed(self.recent))}))
        http.route("GET", "/state", lambda r: Response.json(
            {k: {n: s.state() for n, s in d.items()} for k, d in self._strats.items()}))
        await http.start()

        # 장수 태스크는 전부 감독을 거친다. supervisor.py 참고.
        self.sup.spawn("consume", lambda: self._consume(stop), stop)
        self.sup.spawn("sweep", lambda: self._sweep(stop), stop)
        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()
        await self.sup.shutdown()
        await http.close()
        await self.pub.close()
        log.info("종료. 봉 %d개 처리, 시그널 %d건 발행", self.bars_closed, self.signals_emitted)


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    return runtime.run(SERVICE, StrategyEngine(cfg).run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
