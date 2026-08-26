"""공유메모리 링버퍼 — 논블로킹 생산자와 추월 감지."""
import pytest

from mdfeed.ringbuffer import RingBuffer


@pytest.fixture
def ring():
    r = RingBuffer("mdfeed_pytest_ring", capacity=16, slot_size=64, create=True)
    yield r
    r.close()


def test_push_pop_order(ring):
    rd = ring.reader()
    for i in range(8):
        ring.push(f"msg{i}".encode())
    assert [b.decode() for b in rd.poll()] == [f"msg{i}" for i in range(8)]


def test_reader_starts_at_current_write_position(ring):
    ring.push(b"before")
    rd = ring.reader()                  # 과거를 소급해 읽지 않는다
    assert rd.poll() == []
    ring.push(b"after")
    assert rd.poll() == [b"after"]


def test_producer_never_blocks_and_laps_slow_consumer(ring):
    """소비자가 느려도 생산자는 멈추지 않고, 유실은 정확히 계수된다."""
    rd = ring.reader()
    for i in range(100):                # capacity=16 을 크게 초과
        ring.push(f"{i}".encode())
    got = rd.poll(1000)
    assert len(got) == 16               # 링 크기만큼만 남는다
    assert rd.skipped == 84             # 100 - 16
    assert got[-1] == b"99"             # 최신 데이터가 살아남는다


def test_multiple_readers_are_independent(ring):
    a, b = ring.reader(), ring.reader()
    for i in range(5):
        ring.push(f"{i}".encode())
    assert len(a.poll()) == 5
    assert len(b.poll()) == 5           # a 가 읽어도 b 는 영향 없음


def test_payload_larger_than_slot_is_refused(ring):
    with pytest.raises(ValueError):
        ring.push(b"x" * (ring.payload_max + 1))


def test_stats_report_backlog():
    r = RingBuffer("mdfeed_pytest_ring2", capacity=8, slot_size=64, create=True)
    try:
        rd = r.reader()
        for i in range(3):
            r.push(b"x")
        s = rd.stats()
        assert s["backlog"] == 3 and s["torn"] == 0
    finally:
        r.close()


def test_attach_to_existing_segment():
    a = RingBuffer("mdfeed_pytest_ring3", capacity=8, slot_size=64, create=True)
    try:
        b = RingBuffer("mdfeed_pytest_ring3")      # 다른 프로세스 역할
        assert b.capacity == 8 and b.slot_size == 64
        rd = b.reader()
        a.push(b"cross-process")
        assert rd.poll() == [b"cross-process"]
        b.close()
    finally:
        a.close()
