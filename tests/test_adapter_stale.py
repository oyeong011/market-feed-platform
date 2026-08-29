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


# ── 스냅샷을 지연으로 세지 않는지 ───────────────────────────────────────────
# 업비트를 4종목 → 288종목으로 늘리자 지연 p99 가 106초, max 가 53분으로
# 나왔다. 전부 구독 직후 오는 스냅샷이었다. 스냅샷의 체결 시각은
# "마지막으로 거래된 때"라 거래가 뜸한 종목이면 몇 시간 전이다.

class _Fake:
    latency_us = 5_000_000.0          # 5초


@pytest.mark.asyncio
async def test_스냅샷은_지연_히스토그램에_안_들어간다():
    from mdfeed.metrics import Registry
    a = ChattyButDead()
    a.registry = Registry("feedd")
    a.emit = lambda m: None

    a._mark(_Fake(), measure=False)
    assert a.registry.histogram("ingest_latency", venue="CHATTY").snapshot()["count"] == 0
    assert a.registry.snapshot()["counters"].get(
        'snapshot_msgs_total{venue="CHATTY"}') == 1

    a._mark(_Fake(), measure=True)
    assert a.registry.histogram("ingest_latency", venue="CHATTY").snapshot()["count"] == 1


@pytest.mark.asyncio
async def test_스냅샷도_메시지_수와_정체_판정에는_들어간다():
    """지연에서만 빼는 것이다. 데이터가 온 사실 자체는 세야 한다."""
    a = ChattyButDead()
    a.emit = lambda m: None
    a._mark(_Fake(), measure=False)
    assert a.messages == 1
    assert a.last_msg_at > 0


# ── 정리가 복구를 막는 경우 ────────────────────────────────────────────────
# 실측(2026-08-29): upbit 이 11.2시간 멎었는데 재접속 3회, 태스크 사망 0,
# 정체 지표는 1, 경보 규칙도 존재했다. 감시·지표·경보가 전부 제대로 돌았는데
# **복구만 안 됐다.** 취소한 세션의 finally 가 죽은 소켓에서 drain 을 무기한
# 기다렸고, 재접속 경로가 그 뒤에 서 있었다.

class CleanupHangs(Adapter):
    """취소돼도 정리가 안 끝나는 세션. 반쯤 죽은 소켓의 close 를 흉내낸다."""

    name = "hang"

    def __init__(self, emit=lambda m: None):
        super().__init__(cfg=None, emit=emit)
        self.stale_after_s = 0.3
        self.CANCEL_TIMEOUT_S = 0.2
        self.session_starts = 0
        self.cleanup_entered = 0

    async def session(self) -> None:
        self.session_starts += 1
        try:
            while True:
                await asyncio.sleep(0.02)
        finally:
            # 취소 처리 중에는 CancelledError 가 다시 오지 않는다.
            # 이 대기는 스스로 풀리지 않는다.
            self.cleanup_entered += 1
            await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_정리가_안_끝나도_재접속으로_넘어간다():
    """복구가 정리의 성공에 의존하면 안 된다."""
    a = CleanupHangs()
    t0 = time.time()
    with pytest.raises(StaleSessionError):
        await asyncio.wait_for(a._session_with_watchdog(), timeout=5.0)
    took = time.time() - t0
    assert a.cleanup_entered == 1, "정리에 들어가긴 해야 한다"
    assert took < 3.0, f"{took:.1f}s 걸렸다 — 정리를 무기한 기다리고 있다"


class CleanupSwallowsCancel(CleanupHangs):
    """취소를 삼키는 정리. 두 번째 취소로도 안 깨지는 최악의 경우.

    ``release`` 를 풀어 주기 전까지는 어떤 취소도 이 태스크를 못 끝낸다.
    (시험이 불멸의 태스크를 남기지 않도록 마지막에 풀어 준다.)
    """

    name = "swallow"

    def __init__(self, emit=lambda m: None):
        super().__init__(emit)
        self.release = asyncio.Event()

    async def session(self) -> None:
        self.session_starts += 1
        try:
            while True:
                await asyncio.sleep(0.02)
        finally:
            self.cleanup_entered += 1
            while not self.release.is_set():
                try:
                    await asyncio.wait_for(self.release.wait(), timeout=0.05)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass      # 넓게 잡는 정리 코드가 실제로 이렇게 생긴다


@pytest.mark.asyncio
async def test_두_번_취소해도_안_끝나면_버리고_지표에_남긴다():
    """조용히 새는 태스크가 있다는 사실 자체가 알려져야 한다."""
    from mdfeed.metrics import Registry

    reg = Registry("test")
    a = CleanupSwallowsCancel()
    a.registry = reg
    t0 = time.time()
    with pytest.raises(StaleSessionError):
        await asyncio.wait_for(a._session_with_watchdog(), timeout=5.0)
    took = time.time() - t0
    assert took < 3.0, f"{took:.1f}s — 버리지 않고 계속 기다리고 있다"
    assert "session_cancel_timeouts_total" in reg.prometheus()
    a.release.set()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_정리가_정상이면_기다려_준다():
    """기한은 도망갈 구멍이지 기본 경로가 아니다. 멀쩡한 정리는 끝까지 본다."""
    done = []

    class CleanupOK(ChattyButDead):
        name = "ok"

        async def session(self) -> None:
            self.session_starts += 1
            try:
                while True:
                    await asyncio.sleep(0.02)
            finally:
                await asyncio.sleep(0.05)
                done.append(True)

    a = CleanupOK()
    with pytest.raises(StaleSessionError):
        await a._session_with_watchdog()
    assert done == [True], "정상 정리가 중간에 잘렸다"
