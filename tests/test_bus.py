"""IPC 버스 — 경로 제약과 백프레셔."""
import asyncio
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
