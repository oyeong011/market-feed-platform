"""밀린 구독자에게 무엇을 보낼 것인가 — 낡은 값의 순서인가, 지금 값인가.

측정으로 나온 문제다. 구독자 100명 · 상류 688건/s 에서 게이트웨이 큐가
최고 92 까지 찼다(용량 2,048). 더 밀리면 drop-oldest 가 도는데, 그때
`stream` 이 하는 일은 **낡은 값을 순서대로 계속 보내는 것**이다.

체결 이력을 재구성하는 소비자(적재·정산)에겐 그게 맞다. 그런데 시세를
보는 소비자에게는 3초 전 가격의 행렬보다 지금 가격 하나가 낫다.
마켓데이터 벤더가 full-tick 과 conflated 를 나눠 파는 이유다.
"""
import asyncio
import json

import pytest

from mdfeed.config import Config
from mdfeed.models import MSG_TRADE, Trade
from mdfeed.protocol import FrameParser, encode
from mdfeed.services.tcp_gateway import (MODE_CONFLATE, MODE_STREAM,
                                         Subscriber, TCPGateway)

BASE_NS = 1_700_000_000_000_000_000


class FakeWriter:
    """쓴 것을 모아 두고, 요청할 때만 흘려보내는 소켓."""

    def __init__(self):
        self.chunks = []
        self.transport = None
        self.blocked = asyncio.Event()      # set = 흐름 허용

    def write(self, data):
        self.chunks.append(data)

    async def drain(self):
        await self.blocked.wait()

    def close(self):
        pass


def _tick(symbol: str, price: float, i: int):
    t = Trade(venue="TEST", symbol=symbol, ts_event_ns=BASE_NS + i,
              ts_recv_ns=BASE_NS + i, price=price, qty=1.0, side=1)
    return type("F", (), {"msg_type": MSG_TRADE, "payload": t.pack(), "flags": 0})()


def _gw():
    g = TCPGateway(Config())
    return g


def _decode(chunks):
    p = FrameParser()
    out = []
    for c in chunks:
        out.extend(p.feed(c))
    return out


@pytest.mark.asyncio
async def test_밀리는_동안_같은_종목은_최신값_하나로_합쳐진다():
    g = _gw()
    w = FakeWriter()
    s = Subscriber(1, w, 100, "t")
    s.mode = MODE_CONFLATE
    g.subs[1] = s
    sender = asyncio.create_task(g._send_loop(s))

    for i, px in enumerate([100.0, 101.0, 102.0, 103.0]):
        g._fanout(_tick("AAA", px, i), "TEST:AAA")
    await asyncio.sleep(0)

    w.blocked.set()                       # 이제 흘려보낸다
    await asyncio.sleep(0.05)
    sender.cancel()

    frames = _decode(w.chunks)
    prices = [Trade.unpack(f.payload).price for f in frames]
    assert prices == [103.0], f"합쳐지지 않았다: {prices}"
    assert s.conflated == 3


@pytest.mark.asyncio
async def test_다른_종목은_합쳐지지_않는다():
    g = _gw()
    w = FakeWriter()
    s = Subscriber(1, w, 100, "t")
    s.mode = MODE_CONFLATE
    g.subs[1] = s
    sender = asyncio.create_task(g._send_loop(s))

    for i, sym in enumerate(["AAA", "BBB", "CCC"]):
        g._fanout(_tick(sym, 100.0 + i, i), f"TEST:{sym}")
    await asyncio.sleep(0)
    w.blocked.set()
    await asyncio.sleep(0.05)
    sender.cancel()

    frames = _decode(w.chunks)
    assert sorted(Trade.unpack(f.payload).symbol for f in frames) == ["AAA", "BBB", "CCC"]
    assert s.conflated == 0


@pytest.mark.asyncio
async def test_합쳐져도_seq_는_연속이다():
    """합쳐진 것은 잃은 것이 아니다. 구독자가 갭으로 오인하면 안 된다."""
    g = _gw()
    w = FakeWriter()
    s = Subscriber(1, w, 100, "t")
    s.mode = MODE_CONFLATE
    g.subs[1] = s
    sender = asyncio.create_task(g._send_loop(s))

    for i in range(20):
        g._fanout(_tick("AAA" if i % 2 else "BBB", 100.0 + i, i),
                  f"TEST:{'AAA' if i % 2 else 'BBB'}")
        if i % 5 == 0:
            w.blocked.set()
            await asyncio.sleep(0)
            w.blocked.clear()
    w.blocked.set()
    await asyncio.sleep(0.05)
    sender.cancel()

    seqs = [f.seq for f in _decode(w.chunks)]
    assert seqs == list(range(len(seqs))), f"seq 가 안 연속이다: {seqs[:10]}"


@pytest.mark.asyncio
async def test_큐가_종목_수를_넘지_않는다():
    """conflate 의 큐 길이는 발행량이 아니라 종목 수에 비례한다."""
    g = _gw()
    w = FakeWriter()
    s = Subscriber(1, w, 1000, "t")
    s.mode = MODE_CONFLATE
    g.subs[1] = s

    for i in range(5000):
        sym = f"S{i % 7}"
        g._fanout(_tick(sym, float(i), i), f"TEST:{sym}")
    assert s.queue.qsize() <= 7, f"큐 {s.queue.qsize()} — 종목은 7개뿐이다"
    assert s.dropped == 0, "합칠 수 있는데 버렸다"
    assert s.conflated == 5000 - 7


@pytest.mark.asyncio
async def test_stream_은_그대로_전부_보낸다():
    """기본 동작은 안 바뀐다."""
    g = _gw()
    w = FakeWriter()
    s = Subscriber(1, w, 100, "t")
    g.subs[1] = s
    assert s.mode == MODE_STREAM
    sender = asyncio.create_task(g._send_loop(s))

    for i, px in enumerate([100.0, 101.0, 102.0]):
        g._fanout(_tick("AAA", px, i), "TEST:AAA")
    await asyncio.sleep(0)
    w.blocked.set()
    await asyncio.sleep(0.05)
    sender.cancel()

    prices = [Trade.unpack(f.payload).price for f in _decode(w.chunks)]
    assert prices == [100.0, 101.0, 102.0]
    assert s.conflated == 0


@pytest.mark.asyncio
async def test_모드를_바꾸면_남은_큐를_비운다():
    """섞이면 송신 루프가 키와 프레임을 구분하지 못한다."""
    g = _gw()
    s = Subscriber(1, FakeWriter(), 100, "t")
    g.subs[1] = s
    g._fanout(_tick("AAA", 100.0, 0), "TEST:AAA")
    assert s.queue.qsize() == 1
    g._apply_subscribe(s, json.dumps({"mode": "conflate"}).encode())
    assert s.mode == MODE_CONFLATE
    assert s.queue.qsize() == 0 and not s.pending


@pytest.mark.asyncio
async def test_알_수_없는_모드는_무시한다():
    s = Subscriber(1, FakeWriter(), 100, "t")
    _gw()._apply_subscribe(s, json.dumps({"mode": "turbo"}).encode())
    assert s.mode == MODE_STREAM
