"""밀림이 어디에 쌓이는지 — 큐만 보면 놓친다.

실측(2026-08-29): 구독자 100명 부하에서 게이트웨이 큐 깊이는 20초 내내 0
이었는데 클라이언트가 잰 p99 는 363ms 였다. write() 는 소켓이 느려도 즉시
반환하고 asyncio 가 내부 버퍼에 쌓아 두기 때문이다. drain() 도 상한(기본
64KB) 아래에서는 그냥 통과한다.

일부러 안 읽는 구독자를 붙여 확인하니 transport 가 0 → 18,576B →
65,624B(상한) 로 차고, **그 다음에야** 우리 큐가 147까지 찼다.
큐 깊이만 지표로 내면 앞의 65KB 구간이 통째로 안 보인다.
"""
import asyncio

import pytest

from mdfeed.services.tcp_gateway import Subscriber


class FakeTransport:
    def __init__(self, size):
        self._size = size

    def get_write_buffer_size(self):
        return self._size


class FakeWriter:
    def __init__(self, transport=None):
        self.transport = transport


def test_transport_버퍼를_읽어_낸다():
    s = Subscriber(1, FakeWriter(FakeTransport(65_624)), 10, "peer")
    assert s.wire_bytes() == 65_624
    assert s.info()["wire_bytes"] == 65_624


def test_큐가_비어도_밀려_있을_수_있다():
    """이 조합이 실제로 관측된 상태다 — 큐 0, transport 65KB."""
    s = Subscriber(1, FakeWriter(FakeTransport(65_624)), 10, "peer")
    info = s.info()
    assert info["backlog"] == 0
    assert info["wire_bytes"] > 0, "큐만 보면 밀림이 없다고 판정된다"


def test_transport_가_없으면_0_이고_터지지_않는다():
    """연결이 이미 닫힌 구독자를 훑을 때 헬스 전체가 죽으면 안 된다."""
    assert Subscriber(1, FakeWriter(None), 10, "peer").wire_bytes() == 0

    class Broken:
        @property
        def transport(self):
            raise RuntimeError("closed")

    assert Subscriber(1, Broken(), 10, "peer").wire_bytes() == 0
