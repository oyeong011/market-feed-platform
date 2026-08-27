"""feedd — 수집 데몬. 이 시스템의 단일 진입점이자 유일한 발행자.

    거래소 WS ─▶ 어댑터(정규화) ─▶ seq 부여 ─▶ MDFP 인코딩 ─┬─▶ UDS 버스 (다운스트림 프로세스)
                                                           ├─▶ 공유메모리 링 (저지연 소비자)
                                                           ├─▶ 최신값 캐시 (스냅샷 응답용)
                                                           └─▶ 녹화 파일 (선택)

왜 수집을 한 프로세스로 몰았나
------------------------------
거래소 세션은 비싸고 끊기면 갭이 생긴다. 배포/적재/전략이 각자 거래소에 붙으면
같은 데이터를 N번 받고 N배로 끊긴다. 수집은 하나로 두고 팬아웃은 로컬 IPC로 한다.
상용 마켓데이터 시스템이 feed handler 와 distributor 를 나누는 이유와 같다.

스냅샷 + 증분(snapshot + delta)
-------------------------------
새 구독자는 과거 틱을 못 받아 처음엔 아무 화면도 못 그린다. 그래서 최신값 캐시를
들고 있다가 접속 즉시 스냅샷을 먼저 쏘고 이후 증분을 잇는다. 거래소 피드의
표준 패턴이다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time

from ..adapters import build as build_adapters
from ..adapters.base import CLOCK
from ..bus import UDSPublisher
from ..httpd import HTTPServer, Response, health_routes
from ..metrics import Registry
from ..models import (MSG_BOOK, MSG_TRADE, BookTop, Trade, now_ns)
from ..protocol import encode, heartbeat
from ..ringbuffer import RingBuffer

log = logging.getLogger("mdfeed.feedd")
SERVICE = "feedd"


class FeedDaemon:
    def __init__(self, cfg):
        self.cfg = cfg
        self.registry = Registry(SERVICE)
        self.registry.declare_counters(
            "published_total", "bus_drops_total", "ring_oversize_total")
        self.bus = UDSPublisher(cfg.bus_path, cfg.bus_queue_size,
                                on_drop=lambda: self.registry.counter("bus_drops_total"))
        self.ring: RingBuffer | None = None
        self.seq = 0
        self.snapshot: dict[str, dict] = {}      # "VENUE:SYMBOL" → 최신 상태
        self.adapters: list = []
        self.inactive: list = []
        self._recorder = None
        from ..runtime import make_tracker
        self.tracker = make_tracker()
        self._started = time.time()
        self.first_tick_at: float | None = None

    # ── 발행 경로 (hot path) ──────────────────────────────────────────────
    def _publish(self, msg) -> None:
        if isinstance(msg, Trade):
            mtype, payload = MSG_TRADE, msg.pack()
        elif isinstance(msg, BookTop):
            mtype, payload = MSG_BOOK, msg.pack()
        else:
            return

        frame = encode(mtype, self.seq, payload)
        self.seq += 1

        self.bus.publish(frame)
        if self.ring is not None:
            if len(payload) <= self.ring.payload_max:
                self.ring.push(payload)
            else:
                # 슬롯보다 큰 페이로드는 링에 못 넣는다. 조용히 자르면 파서가
                # 깨지므로 버리고 센다. 버스 경로로는 정상 전달된다.
                self.registry.counter("ring_oversize_total")
        if self._recorder is not None:
            self._recorder.write(frame)

        key = f"{msg.venue}:{msg.symbol}"
        cur = self.snapshot.setdefault(key, {"venue": msg.venue, "symbol": msg.symbol})
        if isinstance(msg, Trade):
            cur.update(last=msg.price, qty=msg.qty, side=msg.side,
                       ts_event_ns=msg.ts_event_ns, ts_recv_ns=msg.ts_recv_ns,
                       latency_us=round(msg.latency_us, 1))
            cur["trades"] = cur.get("trades", 0) + 1
            cur["volume"] = round(cur.get("volume", 0.0) + msg.qty, 8)
            if self.first_tick_at is None:
                self.first_tick_at = time.time()
        else:
            cur.update(bid=msg.bid, ask=msg.ask, bid_qty=msg.bid_qty,
                       ask_qty=msg.ask_qty, mid=msg.mid,
                       spread_bp=round(msg.spread_bp, 3))
        self.registry.counter("published_total")

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def health(self) -> dict:
        ups = [a.health() for a in self.adapters]
        # 활성 업스트림이 하나도 없거나 전부 정체면 unhealthy
        healthy = bool(ups) and not all(u["stale"] for u in ups)
        return {
            "service": SERVICE, "healthy": healthy,
            "uptime_s": round(time.time() - self._started, 1),
            "seq": self.seq,
            "subscribers": self.bus.subscriber_count,
            "bus_dropped": self.bus.dropped,
            "symbols": len(self.snapshot),
            "upstreams": ups,
            "inactive_upstreams": self.inactive,
            # 시계가 어긋나면 지연시간 지표를 믿을 수 없다는 사실을 표면에 올린다
            "clock": CLOCK.report(),
            "clock_warning": ("로컬 시계가 거래소보다 뒤처짐 — NTP 동기화 점검 필요"
                              if CLOCK.any_suspicious() else None),
        }

    def ready(self) -> dict:
        # 첫 틱을 받아 실제로 데이터가 흐르기 시작해야 준비 완료
        ok = self.first_tick_at is not None
        return {"ready": ok, "first_tick_at": self.first_tick_at,
                "reason": None if ok else "아직 첫 틱 수신 전"}

    # ── 실행 ──────────────────────────────────────────────────────────────
    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        if cfg.ring_enabled:
            try:
                self.ring = RingBuffer(cfg.ring_name, cfg.ring_capacity,
                                       cfg.ring_slot_size, create=True)
                log.info("공유메모리 링 생성: %s (%d슬롯 × %dB)",
                         cfg.ring_name, cfg.ring_capacity, cfg.ring_slot_size)
            except Exception as e:                # noqa: BLE001
                log.warning("공유메모리 링 비활성 (%s). 버스만 사용", e)

        if cfg.record_file:
            os.makedirs(os.path.dirname(cfg.record_file) or ".", exist_ok=True)
            self._recorder = open(cfg.record_file, "ab", buffering=1 << 16)
            log.info("녹화 시작 → %s", cfg.record_file)

        await self.bus.start()

        self.adapters, self.inactive = build_adapters(
            cfg.adapters, cfg, self._publish, self.registry)
        if not self.adapters:
            log.error("활성 어댑터가 없다. 비활성 사유: %s", self.inactive)
        for item in self.inactive:
            log.warning("업스트림 비활성 [%s]: %s", item["venue"], item["reason"])

        http = HTTPServer(cfg.http_host, cfg.feedd_admin_port, SERVICE, self.registry)
        health_routes(http, self.health, self.ready, self.tracker)
        http.route("GET", "/snapshot", lambda r: Response.json({
            "ts": time.time(), "count": len(self.snapshot),
            "items": sorted(self.snapshot.values(),
                            key=lambda x: (x["venue"], x["symbol"]))}))
        http.route("GET", "/stats", lambda r: Response.json(self.registry.snapshot()))
        await http.start()

        tasks = [asyncio.create_task(a.run(), name=f"adapter:{a.name}")
                 for a in self.adapters]
        tasks.append(asyncio.create_task(self._heartbeat(stop), name="heartbeat"))
        tasks.append(asyncio.create_task(self._gauges(stop), name="gauges"))

        from ..runtime import sample_resources
        res_task = asyncio.create_task(sample_resources(self.tracker, stop))
        await stop.wait()
        res_task.cancel()
        log.info("종료 신호. 어댑터 정리 중...")
        for a in self.adapters:
            a.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await http.close()
        await self.bus.close()
        if self.ring:
            self.ring.close()
        if self._recorder:
            self._recorder.flush()
            self._recorder.close()
        log.info("정리 완료. 총 %d 프레임 발행", self.seq)

    async def _heartbeat(self, stop: asyncio.Event) -> None:
        """무거래 구간에도 구독자가 '살아있음'과 seq 진행을 확인하게 한다."""
        while not stop.is_set():
            await asyncio.sleep(self.cfg.heartbeat_s)
            self.bus.publish(heartbeat(self.seq, now_ns()))
            self.seq += 1

    async def _gauges(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1.0)
            self.registry.gauge("subscribers", self.bus.subscriber_count)
            self.registry.gauge("symbols_tracked", len(self.snapshot))
            self.registry.gauge("seq", self.seq)
            if self.ring:
                self.registry.gauge("ring_write_seq", self.ring.write_seq)
            for a in self.adapters:
                self.registry.gauge("upstream_stale", 1 if a.is_stale else 0,
                                    venue=a.name.upper())
            # 시계 오프셋을 지표로도 내보낸다. /healthz 에만 있으면 사람이 볼 때만
            # 보이고, 알람은 걸 수 없다.
            for venue, c in CLOCK.report().items():
                self.registry.gauge("clock_offset_us", c["offset_us"], venue=venue)
            # 장 시간을 지표로 내보낸다. 이게 없으면 "발행량 0" 알람이 국내 시장
            # 마감 후 매일 밤 울리고, 사람이 그 알람을 무시하게 된다.
            for a in self.adapters:
                mo = a.health().get("market_open")
                if mo is not None:
                    self.registry.gauge("market_open", 1 if mo else 0,
                                        venue=a.name.upper())


def main() -> int:
    from .. import config, runtime
    cfg = config.load()
    daemon = FeedDaemon(cfg)
    return runtime.run(SERVICE, daemon.run, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
