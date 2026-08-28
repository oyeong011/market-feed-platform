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
