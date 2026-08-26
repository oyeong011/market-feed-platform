"""MDFP/1 프레이밍 — 실전에서 깨지는 지점들을 겨냥한 테스트."""
import random
import struct
import zlib

import pytest

from mdfeed.models import MSG_TRADE, Trade, now_ns
from mdfeed.protocol import (CRC_SIZE, HEADER_SIZE, MAGIC, FrameParser,
                             ProtocolError, SequenceTracker, encode, heartbeat)


def mk(seq=0, price=100.0):
    return encode(MSG_TRADE, seq,
                  Trade("UPBIT", "KRW-BTC", now_ns(), now_ns(), price, 0.1).pack())


def test_frame_roundtrip():
    raw = mk(42)
    (frame,) = list(FrameParser().feed(raw))
    assert frame.seq == 42 and frame.msg_type == MSG_TRADE
    assert Trade.unpack(frame.payload).symbol == "KRW-BTC"


def test_header_size_is_fixed():
    assert HEADER_SIZE == 20
    assert len(mk()) == HEADER_SIZE + Trade.SIZE + CRC_SIZE


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 19, 64, 1000])
def test_partial_reads_reassemble(chunk_size):
    """TCP 는 메시지 경계를 지켜주지 않는다. 어떤 크기로 쪼개도 복원돼야 한다."""
    stream = b"".join(mk(i) for i in range(20))
    p = FrameParser()
    out = []
    for i in range(0, len(stream), chunk_size):
        out += list(p.feed(stream[i:i + chunk_size]))
    assert [f.seq for f in out] == list(range(20))


def test_random_chunking_fuzz():
    stream = b"".join(mk(i) for i in range(200))
    rnd = random.Random(1234)
    p = FrameParser()
    out, i = [], 0
    while i < len(stream):
        n = rnd.randint(1, 97)
        out += list(p.feed(stream[i:i + n]))
        i += n
    assert [f.seq for f in out] == list(range(200))


def test_crc_detects_corruption_and_recovers():
    """오염된 프레임 하나만 버리고 나머지는 살아야 한다.

    한 번 깨졌다고 세션 전체가 멎으면 실전에서 재접속 폭풍이 난다.
    """
    stream = bytearray(b"".join(mk(i) for i in range(5)))
    stream[HEADER_SIZE + 5] ^= 0xFF          # 첫 프레임 페이로드 오염
    p = FrameParser()
    got = list(p.feed(bytes(stream)))
    assert p.crc_error_count == 1
    assert p.resync_count == 1
    assert [f.seq for f in got] == [1, 2, 3, 4]


def test_garbage_prefix_resyncs():
    p = FrameParser()
    got = list(p.feed(b"\x00rubbish\xff\xfe" + mk(7)))
    assert [f.seq for f in got] == [7]


def test_bad_version_rejected():
    raw = bytearray(mk(1))
    raw[4] = 99                               # version 필드
    p = FrameParser()
    assert list(p.feed(bytes(raw))) == []
    assert p.resync_count >= 1


def test_oversized_payload_refused():
    with pytest.raises(ProtocolError):
        encode(MSG_TRADE, 0, b"x" * (1 << 21))


def test_heartbeat_carries_timestamp():
    ts = now_ns()
    (f,) = list(FrameParser().feed(heartbeat(9, ts)))
    assert struct.unpack("!Q", f.payload)[0] == ts


class TestSequenceTracker:
    def test_contiguous_has_no_gap(self):
        t = SequenceTracker()
        assert [t.observe(i) for i in range(10)] == [0] * 10
        assert t.stats()["lost_messages"] == 0

    def test_gap_is_counted(self):
        t = SequenceTracker()
        t.observe(0)
        assert t.observe(5) == 4
        assert t.stats()["gap_count"] == 1
        assert t.stats()["lost_messages"] == 4

    def test_duplicate_is_not_a_gap(self):
        t = SequenceTracker()
        t.observe(0); t.observe(1); t.observe(1)
        s = t.stats()
        assert s["duplicate_count"] == 1 and s["gap_count"] == 0
