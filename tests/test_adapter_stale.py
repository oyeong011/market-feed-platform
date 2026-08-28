"""소켓은 살아 있는데 업무 메시지만 끊긴 경우를 잡는지.

실제로 업비트에서 9.6시간 동안 reconnects=0 인 채 시세만 멎었다.
거래소가 체결이 아닌 프레임을 계속 보내면 recv 타임아웃은 매번 리셋되고,
정체 판정은 recv 가 아니라 _mark() 가 갱신하는 last_msg_at 으로 해야 한다.
"""
import asyncio
import time

import pytest

from mdfeed.adapters.base import Adapter, StaleSessionError


class ChattyButDead(Adapter):
    """프레임은 계속 오지만 업무 메시지는 하나도 안 만드는 거래소."""

    name = "chatty"

    def __init__(self, emit=lambda m: None):
        super().__init__(cfg=None, emit=emit)
        self.stale_after_s = 0.4
        self.session_starts = 0

    async def session(self) -> None:
        self.session_starts += 1
        while True:            # recv 는 계속 성공한다고 가정
            await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_업무_메시지가_끊기면_세션을_끊는다():
    a = ChattyButDead()
    with pytest.raises(StaleSessionError):
        await a._session_with_watchdog()
    assert a.session_starts == 1


@pytest.mark.asyncio
async def test_메시지가_계속_오면_세션을_유지한다():
    a = ChattyButDead()

    async def keep_alive():
        for _ in range(12):
            await asyncio.sleep(0.05)
            a.last_msg_at = time.time()   # _mark() 가 하는 일

    feeder = asyncio.ensure_future(keep_alive())
    try:
        await asyncio.wait_for(a._session_with_watchdog(), timeout=0.7)
    except asyncio.TimeoutError:
        pass                              # 안 끊긴 것이 정상
    except StaleSessionError:
        pytest.fail("메시지가 오는데도 세션을 끊었다")
    finally:
        feeder.cancel()


@pytest.mark.asyncio
async def test_첫_메시지를_못_받아도_기한이_적용된다():
    """연결 직후 구독이 실패해 한 건도 못 받는 경우도 재접속 대상이다."""
    a = ChattyButDead()
    a.last_msg_at = 0.0
    with pytest.raises(StaleSessionError):
        await a._session_with_watchdog()


# ── 휴장 중 재접속 폭풍 회귀 ────────────────────────────────────────────────
# 2026-08-28 실측: KRX 샤드에서 kis 재접속 94회, kis_macro 91회가 났다.
# 세 어댑터 모두 is_stale 에 장시간 가드를 갖고 있었는데, 워치독이 그 가드를
# 거치지 않고 idle 을 직접 계산해 전부 무력화됐다. 가드가 있는데 안 불리는
# 구조였다 — 그래서 판정 경로를 expects_data() 하나로 합쳤다.

class ClosedMarket(Adapter):
    """장이 닫혀 있어 데이터가 오지 않는 업스트림."""

    name = "closed"

    def __init__(self, emit=lambda m: None):
        super().__init__(cfg=None, emit=emit)
        self.stale_after_s = 0.4
        self.open_now = False

    def expects_data(self) -> bool:
        return self.open_now

    async def session(self) -> None:
        while True:
            await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_휴장_중에는_조용해도_세션을_끊지_않는다():
    a = ClosedMarket()
    try:
        await asyncio.wait_for(a._session_with_watchdog(), timeout=1.5)
    except asyncio.TimeoutError:
        pass                              # 안 끊긴 것이 정상
    except StaleSessionError:
        pytest.fail("휴장 중인데 재접속을 유발했다")
    assert a.is_stale is False


@pytest.mark.asyncio
async def test_개장_중_정체는_그대로_잡는다():
    a = ClosedMarket()
    a.open_now = True
    with pytest.raises(StaleSessionError):
        await a._session_with_watchdog()


@pytest.mark.asyncio
async def test_개장_직후_첫_체결_전에_끊지_않는다():
    """휴장 내내 last_msg_at 이 비어 있다가 개장하면 기한이 새로 시작돼야 한다."""
    a = ClosedMarket()
    task = asyncio.ensure_future(a._session_with_watchdog())
    await asyncio.sleep(1.0)              # 휴장 상태로 기한을 여러 번 넘긴다
    a.open_now = True                     # 개장
    await asyncio.sleep(0.3)              # 기한(0.4s) 안 — 아직 끊기면 안 된다
    alive = not task.done()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    assert alive, "개장하자마자 첫 체결도 오기 전에 세션을 끊었다"
