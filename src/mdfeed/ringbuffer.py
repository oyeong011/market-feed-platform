"""공유메모리 SPSC 링버퍼 — 수집 프로세스와 배포 프로세스 사이의 hot path.

왜 큐가 아니라 링버퍼인가
-------------------------
multiprocessing.Queue 는 pickle 직렬화 + 내부 락 + 피더 스레드를 거친다.
틱 하나당 수십 µs가 든다. 마켓데이터는 초당 수천 건이 들어오고 그중 대부분이
"최신값만 의미 있는" 데이터라, 고정 슬롯 링버퍼에 memcpy 하는 편이 훨씬 싸다.

핵심 정책: **생산자는 절대 블로킹하지 않는다**
---------------------------------------------
소비자가 느리면 생산자가 소비자를 덮어쓰고 지나간다(lap). 소비자는 그 사실을
seq 비교로 감지해 "몇 건 건너뛰었다"를 카운트한다. 시세 피드에서 오래된 틱을
지키자고 최신 틱 수신을 막는 건 본말전도다. 대신 유실은 반드시 계측한다.

찢긴 읽기(torn read) 방지
-------------------------
락 없이 읽으므로, 복사 도중 생산자가 같은 슬롯을 덮어쓸 수 있다. 슬롯 앞뒤에
같은 seq를 적어두고(seq_head / seq_tail) 복사 후 둘이 일치하는지 확인한다.
불일치면 그 슬롯은 버린다 — Disruptor 계열이 쓰는 고전적 기법이다.
"""

from __future__ import annotations

import struct
from multiprocessing import shared_memory

HDR_FMT = "!4sIIIQQQ"           # magic, slot_size, capacity, reserved, write_seq, read_hint, dropped
HDR_SIZE = 64                   # 캐시라인 정렬용으로 64B 확보
SLOT_HDR = 12                   # seq_head(8) + length(4)
SLOT_TAIL = 8                   # seq_tail(8)
MAGIC = b"MDRB"


class RingBuffer:
    """단일 생산자 / 다중 소비자 브로드캐스트 링버퍼."""

    def __init__(self, name: str, capacity: int = 8192, slot_size: int = 128,
                 create: bool = False):
        self.capacity = capacity
        self.slot_size = slot_size
        self.payload_max = slot_size - SLOT_HDR - SLOT_TAIL
        if self.payload_max <= 0:
            raise ValueError("slot_size가 슬롯 헤더보다 작다")
        total = HDR_SIZE + capacity * slot_size

        if create:
            try:                                    # 이전 실행이 남긴 세그먼트 정리
                shared_memory.SharedMemory(name=name).unlink()
            except FileNotFoundError:
                pass
            self.shm = shared_memory.SharedMemory(name=name, create=True, size=total)
            struct.pack_into(HDR_FMT, self.shm.buf, 0,
                             MAGIC, slot_size, capacity, 0, 0, 0, 0)
        else:
            self.shm = shared_memory.SharedMemory(name=name)
            magic, ss, cap, *_ = struct.unpack_from(HDR_FMT, self.shm.buf, 0)
            if magic != MAGIC:
                raise ValueError(f"공유메모리 {name} 가 링버퍼가 아니다")
            self.slot_size, self.capacity = ss, cap
            self.payload_max = ss - SLOT_HDR - SLOT_TAIL

        self._buf = self.shm.buf
        self._owner = create

    # ── 생산자 ────────────────────────────────────────────────────────────
    @property
    def write_seq(self) -> int:
        return struct.unpack_from("!Q", self._buf, 16)[0]

    def _set_write_seq(self, v: int) -> None:
        struct.pack_into("!Q", self._buf, 16, v)

    @property
    def dropped(self) -> int:
        return struct.unpack_from("!Q", self._buf, 32)[0]

    def push(self, payload: bytes) -> int:
        """블로킹 없이 슬롯 하나를 쓴다. 반환값은 부여된 seq."""
        n = len(payload)
        if n > self.payload_max:
            raise ValueError(f"payload {n}B > slot payload {self.payload_max}B")
        seq = self.write_seq
        off = HDR_SIZE + (seq % self.capacity) * self.slot_size
        struct.pack_into("!QI", self._buf, off, seq, n)
        self._buf[off + SLOT_HDR: off + SLOT_HDR + n] = payload
        struct.pack_into("!Q", self._buf, off + self.slot_size - SLOT_TAIL, seq)
        self._set_write_seq(seq + 1)
        return seq

    # ── 소비자 ────────────────────────────────────────────────────────────
    def reader(self) -> "RingReader":
        return RingReader(self)

    def close(self) -> None:
        try:
            self._buf = None
            self.shm.close()
            if self._owner:
                self.shm.unlink()
        except Exception:
            pass


class RingReader:
    """독립 커서를 가진 소비자. 여러 개가 같은 버퍼를 병렬로 읽을 수 있다."""

    def __init__(self, ring: RingBuffer, start: int | None = None):
        self.ring = ring
        self.cursor = ring.write_seq if start is None else start
        self.skipped = 0          # 생산자에게 추월당해 건너뛴 건수
        self.torn = 0             # 찢긴 읽기로 버린 건수

    def poll(self, max_items: int = 256) -> list[bytes]:
        r = self.ring
        buf = r._buf
        out: list[bytes] = []
        write_seq = r.write_seq

        # 추월당했는가? 링 한 바퀴 이상 밀렸으면 최신 쪽으로 커서를 점프시킨다
        behind = write_seq - self.cursor
        if behind > r.capacity:
            lost = behind - r.capacity
            self.skipped += lost
            self.cursor = write_seq - r.capacity

        while self.cursor < write_seq and len(out) < max_items:
            seq = self.cursor
            off = HDR_SIZE + (seq % r.capacity) * r.slot_size
            head, n = struct.unpack_from("!QI", buf, off)
            if head != seq or n > r.payload_max:
                self.torn += 1
                self.cursor += 1
                continue
            data = bytes(buf[off + SLOT_HDR: off + SLOT_HDR + n])
            tail = struct.unpack_from("!Q", buf, off + r.slot_size - SLOT_TAIL)[0]
            if tail != seq:                     # 복사 도중 덮어쓰였다
                self.torn += 1
                self.cursor += 1
                continue
            out.append(data)
            self.cursor += 1
        return out

    def stats(self) -> dict:
        return {
            "cursor": self.cursor,
            "write_seq": self.ring.write_seq,
            "backlog": self.ring.write_seq - self.cursor,
            "skipped": self.skipped,
            "torn": self.torn,
        }
