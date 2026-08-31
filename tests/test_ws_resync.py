"""배치를 버린 WS 클라이언트를 어떻게 되맞추는가.

WS 경로에는 시퀀스가 없다. TCP(MDFP)는 구독자별 seq 로 갭을 알려 주고
클라이언트가 재접속해 스냅샷을 받지만, WS 는 그 장치가 없다.

그래서 배치를 하나 버리면 그 안에 있던 종목은 **다음 체결이 올 때까지**
낡은 값이 화면에 떠 있다. 거래가 뜸한 종목이면 몇 시간이다.
그동안 클라이언트는 자기가 뭘 놓쳤는지 모른다 — 조용한 손실이다.

버렸으면 다음 주기에 전체를 다시 준다.
"""
import asyncio
import json

import pytest

from mdfeed.config import Config
from mdfeed.services.ws_gateway import WSClientConn, WSGateway
from mdfeed.wsproto import FrameDecoder


class FakeWriter:
    def __init__(self):
        self.chunks = []

    def write(self, d):
        self.chunks.append(d)

    async def drain(self):
        pass

    def close(self):
        pass

    def get_extra_info(self, *_a):
        return None


def _msgs(c: WSClientConn):
    """큐에 든 프레임을 JSON 으로 푼다."""
    out = []
    dec = FrameDecoder(expect_masked=False)
    while not c.queue.empty():
        for _op, payload in dec.feed(c.queue.get_nowait()):
            out.append(json.loads(payload))
    return out


def _gw(queue_size=2):
    g = WSGateway(Config())
    c = WSClientConn(1, FakeWriter(), queue_size, "t")
    g.clients[1] = c
    g.snapshot = {f"TEST:S{i}": {"symbol": f"S{i}", "price": 100.0 + i}
                  for i in range(5)}
    return g, c


@pytest.mark.asyncio
async def test_버렸으면_다음_주기에_전체를_다시_준다():
    g, c = _gw(queue_size=1)
    # 큐(1)를 넘겨 버리게 만든다
    g._enqueue(c, b"a")
    g._enqueue(c, b"b")
    assert c.dropped == 1 and c.needs_resync is True

    g._dirty = {"TEST:S0"}                 # 바뀐 건 하나뿐
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.25)
    stop.set()
    task.cancel()

    msgs = [m for m in _msgs(c) if isinstance(m, dict)]
    snaps = [m for m in msgs if m.get("type") == "snapshot"]
    assert snaps, f"되맞춤을 안 보냈다: {[m.get('type') for m in msgs]}"
    assert snaps[-1]["reason"] == "dropped"
    assert len(snaps[-1]["items"]) == 5, "바뀐 것만 보내면 놓친 종목이 안 고쳐진다"
    assert c.resyncs == 1
    assert c.needs_resync is False, "되맞춤 뒤에도 표시가 남아 매 주기 전체를 보낸다"


@pytest.mark.asyncio
async def test_클라이언트가_놓쳤다는_사실을_안다():
    """WS 에는 시퀀스가 없다. 서버가 말해 주지 않으면 알 방법이 없다."""
    g, c = _gw(queue_size=1)
    g._enqueue(c, b"a")
    g._enqueue(c, b"b")
    g._dirty = {"TEST:S0"}
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.25)
    stop.set(); task.cancel()

    snaps = [m for m in _msgs(c) if m.get("type") == "snapshot"]
    assert snaps[-1]["dropped"] >= 1


@pytest.mark.asyncio
async def test_안_버린_클라이언트는_바뀐_것만_받는다():
    """되맞춤은 예외 경로다. 평시에 전체를 보내면 대역폭이 종목 수에 비례한다."""
    g, c = _gw(queue_size=100)
    g._dirty = {"TEST:S0"}
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.25)
    stop.set(); task.cancel()

    msgs = _msgs(c)
    ticks = [m for m in msgs if m.get("type") == "tick"]
    assert ticks and len(ticks[0]["items"]) == 1
    assert not [m for m in msgs if m.get("type") == "snapshot"]
    assert c.resyncs == 0


@pytest.mark.asyncio
async def test_필터_없는_클라이언트끼리는_같은_프레임을_쓴다():
    """구독 필터가 없으면 메시지가 같다. 클라이언트마다 다시 만들 이유가 없다."""
    g, c1 = _gw(queue_size=100)
    c2 = WSClientConn(2, FakeWriter(), 100, "t2")
    g.clients[2] = c2
    g._dirty = {"TEST:S0", "TEST:S1"}
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.25)
    stop.set(); task.cancel()

    a, b = c1.queue.get_nowait(), c2.queue.get_nowait()
    assert a is b, "같은 내용을 클라이언트 수만큼 다시 직렬화하고 있다"


@pytest.mark.asyncio
async def test_되맞춤이_매_주기_반복되지_않는다():
    """전체 스냅샷을 큐 비우지 않고 넣으면 그게 또 넘쳐 매 주기 되맞춤이 된다.

    처음 구현이 그랬고, 위 시험이 잡았다. 되맞춤은 예외 경로여야지
    정상 경로가 되면 대역폭이 종목 수 × 주기로 늘어난다.
    """
    g, c = _gw(queue_size=1)
    g._enqueue(c, b"a")
    g._enqueue(c, b"b")                     # 한 번 버린다
    assert c.needs_resync

    g._dirty = {"TEST:S0"}
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.55)               # 여러 주기 돈다
    stop.set(); task.cancel()

    assert c.resyncs == 1, f"되맞춤이 {c.resyncs}회 — 매 주기 반복되고 있다"


@pytest.mark.asyncio
async def test_되맞춤은_밀린_델타를_버리고_보낸다():
    """전체 스냅샷은 대기 중인 델타를 전부 무의미하게 만든다.

    큐를 안 비우면 이미 밀려 있는 클라이언트에게 **쓸모없어진 델타를 먼저**
    보내고 그 뒤에 스냅샷을 보낸다. 최종 상태는 같지만, 가장 느린
    클라이언트에게 가장 많은 바이트를 쓰는 셈이다.
    """
    g, c = _gw(queue_size=10)
    for i in range(4):                      # 밀린 델타 4건
        g._enqueue(c, b"stale%d" % i)
    c.needs_resync = True                   # 앞서 버렸다고 표시
    c.dropped = 1

    g._dirty = {"TEST:S0"}
    stop = asyncio.Event()
    task = asyncio.create_task(g._coalesce_loop(stop))
    await asyncio.sleep(0.25)
    stop.set(); task.cancel()

    assert c.queue.qsize() == 1, (
        f"큐에 {c.queue.qsize()}건 — 쓸모없어진 델타를 스냅샷 앞에 보내고 있다")
    msgs = _msgs(c)
    assert len(msgs) == 1 and msgs[0]["type"] == "snapshot"
