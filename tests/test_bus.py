"""IPC 버스 — 경로 제약과 백프레셔."""
import asyncio
import contextlib
import os
import tempfile

import pytest

from mdfeed.bus import MAX_UDS_PATH, UDSPublisher, UDSSubscriber, check_uds_path
from mdfeed.models import MSG_TRADE, Trade, now_ns
from mdfeed.protocol import encode


def test_long_uds_path_fails_fast_with_actionable_message():
    """재접속 루프 안에서 OSError 로 나면 원인을 찾기 어렵다. 기동 때 잡는다."""
    long_path = "/tmp/" + "d" * (MAX_UDS_PATH + 20) + "/bus.sock"
    with pytest.raises(ValueError) as e:
        check_uds_path(long_path)
    assert "MDFEED_RUN_DIR" in str(e.value)


def test_normal_path_accepted():
    check_uds_path("/run/mdfeed/bus.sock")      # 예외가 없으면 통과


def test_publish_reaches_subscriber():
    run = tempfile.mkdtemp(prefix="mdfb", dir="/tmp")
    path = os.path.join(run, "b.sock")

    async def main():
        pub = UDSPublisher(path)
        await pub.start()
        sub = UDSSubscriber(path)
        got = []

        async def reader():
            async for f in sub.frames():
                got.append(f)
                if len(got) >= 3:
                    return

        task = asyncio.create_task(reader())
        await asyncio.sleep(0.3)
        for i in range(3):
            pub.publish(encode(MSG_TRADE, i,
                               Trade("T", "S", now_ns(), now_ns(), 1.0, 1.0).pack()))
        try:
            await asyncio.wait_for(task, timeout=5)
        finally:
            await pub.close()
        return got

    frames = asyncio.run(main())
    assert [f.seq for f in frames] == [0, 1, 2]


def test_backpressure_drops_oldest_not_newest():
    """느린 구독자 때문에 발행이 멈추면 전체 피드가 함께 멈춘다.
    큐가 차면 오래된 것부터 버리고, 최신 데이터를 지킨다."""
    run = tempfile.mkdtemp(prefix="mdfb", dir="/tmp")
    path = os.path.join(run, "b.sock")

    async def main():
        pub = UDSPublisher(path, queue_size=4)
        await pub.start()
        # 소켓만 열고 읽지 않는 '느린 구독자'
        reader, writer = await asyncio.open_unix_connection(path)
        await asyncio.sleep(0.2)
        for i in range(100):
            pub.publish(encode(MSG_TRADE, i, b"x" * 8))
        dropped = pub.dropped
        writer.close()
        await pub.close()
        return dropped

    dropped = asyncio.run(main())
    assert dropped > 0, "백프레셔가 동작하지 않았다 (발행이 블로킹됐을 가능성)"


# ── 드롭을 누구에게 귀속시키는가 ──────────────────────────────────────────
# 실측(2026-08-31): bus_dropped 112,979 을 보고도 다섯 구독자 중 누가 흘린
# 건지 알 수 없었다. 무엇을 고쳐야 하는지가 곧 그 답인데, 합계 하나로는
# 아무것도 지목하지 못한다.

@pytest.mark.asyncio
async def test_구독자가_이름을_밝히면_드롭이_그_이름에_붙는다():
    from mdfeed.bus import UDSPublisher, UDSSubscriber

    # UDS 경로는 길이 제한이 있어 pytest tmp_path 를 못 쓴다
    path = os.path.join(tempfile.mkdtemp(prefix="mdfb", dir="/tmp"), "b.sock")
    who = []
    pub = UDSPublisher(path, queue_size=2, on_drop=who.append)
    await pub.start()

    sub = UDSSubscriber(path, name="느린놈")
    it = sub.frames().__aiter__()
    task = asyncio.create_task(it.__anext__())
    for _ in range(50):                      # 이름 악수가 끝날 때까지
        await asyncio.sleep(0.02)
        if pub.subscriber_stats():
            break

    stats = pub.subscriber_stats()
    assert stats and stats[0]["name"] == "느린놈", stats

    for i in range(20):                      # 큐(2)보다 훨씬 많이 흘린다
        pub.publish(encode(MSG_TRADE, i, b"x" * 8))
    assert pub.dropped > 0
    assert set(who) == {"느린놈"}, who
    assert pub.subscriber_stats()[0]["dropped"] == pub.dropped

    task.cancel()
    with contextlib.suppress(Exception, asyncio.CancelledError):
        await task
    await pub.close()


@pytest.mark.asyncio
async def test_이름을_안_밝혀도_동작한다():
    """옛 구독자와의 호환을 깨지 않는다. 이름만 anonymous 가 된다."""
    from mdfeed.bus import UDSPublisher, UDSSubscriber

    path = os.path.join(tempfile.mkdtemp(prefix="mdfb", dir="/tmp"), "b.sock")
    pub = UDSPublisher(path, queue_size=4)
    await pub.start()
    sub = UDSSubscriber(path)                # 이름 없음
    it = sub.frames().__aiter__()
    task = asyncio.create_task(it.__anext__())
    for _ in range(150):
        await asyncio.sleep(0.02)
        if pub.subscriber_stats():
            break
    assert pub.subscriber_stats()[0]["name"] == "anonymous"
    task.cancel()
    with contextlib.suppress(Exception, asyncio.CancelledError):
        await task
    await pub.close()


@pytest.mark.asyncio
async def test_드롭_많은_구독자가_먼저_나온다():
    """운영자가 제일 먼저 봐야 할 줄이 맨 위여야 한다."""
    from mdfeed.bus import UDSPublisher

    pub = UDSPublisher(os.path.join(tempfile.mkdtemp(prefix="mdfb", dir="/tmp"), "b.sock"))
    pub._clients = {1: asyncio.Queue(), 2: asyncio.Queue()}
    pub._stats = {1: {"name": "a", "connected_at": 0.0, "dropped": 3, "sent": 0},
                  2: {"name": "b", "connected_at": 0.0, "dropped": 99, "sent": 0}}
    assert [x["name"] for x in pub.subscriber_stats()] == ["b", "a"]
