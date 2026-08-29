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
        # 활성 업스트림이 하나도 없거나 전부 정체면 unhealthy.
        # any 가 아니라 all 인 이유: 거래소 하나가 죽었다고 프로세스를 재시작해도
        # 그 거래소는 살아나지 않는다. 재시작이 고치지 못하는 것으로 생존 판정을
        # 뒤집으면 재시작만 반복된다.
        healthy = bool(ups) and not all(u["stale"] for u in ups)
        # 다만 "한 거래소가 죽었다"가 healthy:true 뒤에 숨으면 안 된다.
        # 실측(2026-08-29): upbit 이 11.2시간 멎어 있는 동안 이 응답은 계속
        # healthy:true 였다. 정체 지표와 경보는 제대로 돌았지만, 헬스를 눈으로
        # 보는 사람에게는 아무 표시도 없었다. 목록으로 표면에 올린다.
        degraded = [u["venue"] for u in ups if u["stale"]]
        return {
            "service": SERVICE, "healthy": healthy,
            "degraded_upstreams": degraded,
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
    def _declare_venue_counters(self) -> None:
        """업스트림별 카운터를 0으로 미리 만든다.

        사건이 나야 생기는 지표에는 그 전에 알람을 걸 수 없다. Prometheus 는
        없는 지표에 오류를 내지 않고 조용히 no data 를 주므로,
        `increase(mdfeed_reconnects_total[1h]) > 10` 같은 규칙이 영원히
        평가되지 않는다. 재접속이 0회인 것과 계측이 안 되는 것은 다르다.
        """
        venues = [{"venue": a.name.upper()} for a in self.adapters]
        if not venues:
            return
        self.registry.declare_counters(
            ticks_total=venues,
            reconnects_total=venues,
            adapter_errors_total=venues,
            stale_restarts_total=venues,
            adapter_task_deaths_total=venues,
            snapshot_msgs_total=venues,
            session_cancel_timeouts_total=venues,
        )

    async def _keep_running(self, adapter, stop) -> None:
        """어댑터 루프가 죽으면 되살린다.

        create_task 로 띄우고 아무도 결과를 안 보면, run() 이 예외로 끝났을 때
        태스크만 조용히 사라진다. 예외는 태스크 안에 담긴 채 아무도 안 읽고,
        서비스는 계속 healthy 를 보고하며 그 업스트림만 영원히 멎는다.

        실측(2026-08-28): upbit 이 2.97시간 멎었는데 재접속은 8회에서 멈춰
        있었다. 재접속 로직이 안 돈 게 아니라 재접속을 돌릴 주체가 없었다.
        """
        deaths = 0
        while not stop.is_set():
            try:
                await adapter.run()
                return                                  # stop 요청에 의한 정상 종료
            except asyncio.CancelledError:
                raise
            except BaseException as e:                  # noqa: BLE001
                deaths += 1
                log.exception("[%s] 어댑터 루프가 죽었다(%d번째) — 되살린다: %s",
                              adapter.name, deaths, e)
                self.registry.counter("adapter_task_deaths_total",
                                      venue=adapter.name.upper())
            with contextlib.suppress(asyncio.TimeoutError):
                # 되살리기도 폭주하면 안 된다. 지수 백오프로 벌린다.
                await asyncio.wait_for(
                    stop.wait(), timeout=min(0.5 * 2 ** (deaths - 1), 30.0))

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

        # 잘린 심볼 목록. 개수만 세면 "10종이 잘린다"까지는 알아도
        # 어느 심볼인지 몰라 아무 조치도 못 한다. 지표는 조치 가능해야 한다.
        def _trunc(_req):
            from ..models import truncated_symbols
            t = truncated_symbols()
            return Response.json({
                "count": len(t),
                "note": "전송 심볼 폭(16바이트)을 넘어 잘린 원본 이름",
                "symbols": sorted(t.items(), key=lambda kv: -kv[1]),
            })
        http.route("GET", "/symbols/truncated", _trunc)
        await http.start()

        self._declare_venue_counters()
        tasks = [asyncio.create_task(self._keep_running(a, stop),
                                     name=f"adapter:{a.name}")
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
            # 심볼이 전송 폭에 안 들어가면 하류에서 두 종목이 한 계열로 섞인다.
            # 실제로 KRX 지수명이 전부 빈 문자열이 돼 "한 틱에 709% 이동"
            # CRITICAL 이 쌓였다. 잘림은 조용하면 안 된다.
            from ..models import truncated_symbols
            trunc = truncated_symbols()
            self.registry.gauge("symbol_truncated_kinds", len(trunc))
            self.registry.gauge("symbol_truncated_total", sum(trunc.values()))
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
