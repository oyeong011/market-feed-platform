"""녹화 리플레이 어댑터 — 네트워크 없이 전 구간을 재현한다.

이게 왜 중요한가
----------------
* CI 는 외부 거래소에 붙을 수 없다(불안정하고, 지역 제한이 있고, 남의 서버다).
  리플레이가 있으면 수집→버스→게이트웨이→적재→전략 전 경로를 결정론적으로 테스트할 수 있다.
* 장애 재현. "어제 14시 32분에 튄 구간"을 그대로 다시 흘려보내야 원인을 잡는다.
* 배속 재생. 실시간 30분치를 60배속으로 흘려 부하 시험을 한다.

파일 형식은 MDFP/1 프레임을 그대로 이어붙인 것이다. 별도 포맷을 만들지 않으면
녹화기는 "버스에서 받은 바이트를 파일에 쓰기"만 하면 되고, 리플레이는 게이트웨이와
똑같은 파서를 쓴다. 포맷이 하나면 버그도 한 군데서만 난다.

시각 재기준(re-stamping)
------------------------
실시간 배속(speed > 0)으로 재생할 때는 체결 시각을 **현재 시각 기준으로 평행이동**한다.
안 그러면 몇 시간 전 타임스탬프로 봉이 만들어져 대시보드와 REST 조회에 과거 시각이 뜬다.
평행이동이므로 틱 사이 간격은 그대로 보존된다.

최대속도(speed = 0)에서는 재기준하지 않는다. 18분 데이터를 20초에 쏟아내므로
첫 프레임 기준으로 옮기면 뒷부분이 **미래 시각**이 되어 더 이상해진다.
부하시험·CI 용도라 시각의 절대값이 의미 없기도 하다.

장애 재현(forensics)에는 원본 시각이 필요하므로 MDFEED_REPLAY_RESTAMP=0 으로 끈다.
"""

from __future__ import annotations

import asyncio
import logging
import os

from ..models import MSG_BOOK, MSG_TRADE, BookTop, Trade, now_ns
from ..protocol import FrameParser
from .base import Adapter

log = logging.getLogger("mdfeed.adapter.replay")


class ReplayAdapter(Adapter):
    name = "replay"
    stale_after_s = 1e9         # 리플레이는 정체 개념이 없다
    # 녹화 시각과 현재 시각의 차이는 지연이 아니다. 측정하지 않는다.
    measures_latency = False

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.path = cfg.replay_file
        self.speed = max(cfg.replay_speed, 0.0)   # 0 = 최대 속도(부하시험)
        self.loop = cfg.replay_loop
        # 최대속도에서는 재기준이 오히려 미래 시각을 만든다 (docstring 참고)
        self.restamp = cfg.replay_restamp and self.speed > 0
        self.laps = 0

    def enabled(self) -> bool:
        return os.path.exists(self.path)

    def disabled_reason(self) -> str:
        return f"녹화 파일 없음: {self.path} (scripts/record.py 로 생성)"

    async def session(self) -> None:
        if not self.enabled():
            raise RuntimeError(self.disabled_reason())
        while True:
            await self._play_once()
            self.laps += 1
            if not self.loop:
                return
            log.info("[replay] %d회차 재생 완료, 처음부터 반복", self.laps)

    async def _play_once(self) -> None:
        parser = FrameParser()
        prev_event_ns: int | None = None
        shift_ns: int | None = None      # 첫 프레임에서 정해지는 평행이동량
        with open(self.path, "rb") as fh:
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    return
                for frame in parser.feed(chunk):
                    msg = self._decode(frame)
                    if msg is None:
                        continue
                    # 원본의 틱 간격을 speed 배율로 재현
                    if self.speed > 0 and prev_event_ns is not None:
                        gap = (msg.ts_event_ns - prev_event_ns) / 1e9 / self.speed
                        if 0 < gap < 5.0:      # 5초 넘는 공백은 건너뛴다
                            await asyncio.sleep(gap)
                    prev_event_ns = msg.ts_event_ns
                    if self.restamp:
                        if shift_ns is None:
                            shift_ns = now_ns() - msg.ts_event_ns
                        msg.ts_event_ns += shift_ns
                    msg.ts_recv_ns = now_ns()
                    self._mark(msg)
                if self.speed == 0:
                    await asyncio.sleep(0)     # 최대속도여도 이벤트 루프는 양보

    @staticmethod
    def _decode(frame):
        if frame.msg_type == MSG_TRADE and len(frame.payload) >= Trade.SIZE:
            return Trade.unpack(frame.payload)
        if frame.msg_type == MSG_BOOK and len(frame.payload) >= BookTop.SIZE:
            return BookTop.unpack(frame.payload)
        return None
