"""MDFP/1 — MDFeed 자체 TCP 배포 프로토콜.

왜 직접 만들었나
----------------
TCP는 바이트 스트림이라 "메시지 경계"가 없다. recv() 한 번이 메시지 1.5개를
줄 수도, 3개를 줄 수도 있다. 상용 마켓데이터 피드(FIX/FAST, ITCH, 인포맥스
자체 프로토콜)가 공통으로 푸는 문제가 정확히 이것이라, 길이 프리픽스 프레이밍 ·
시퀀스 번호 · 갭 탐지 · 하트비트를 직접 구현해 그 구조를 그대로 재현했다.

프레임 레이아웃 (빅엔디언, 헤더 20바이트)
-----------------------------------------
    +0  magic    4B  b"MDF1"      스트림 재동기화 앵커
    +4  version  1B  = 1
    +5  msg_type 1B  models.MSG_*
    +6  flags    2B  bit0 = 스냅샷, bit1 = 압축(예약)
    +8  seq      8B  채널 단위 단조증가. 구독자가 갭을 탐지하는 근거
    +16 length   4B  페이로드 바이트 수
    +20 payload  NB
        crc32    4B  헤더+페이로드에 대한 CRC32. 프레이밍 오류 즉시 탐지

설계 판단
---------
* CRC32를 넣은 이유: TCP 체크섬은 16비트라 약하고, 무엇보다 프레이밍 버그
  (오프셋 하나 밀림)를 조용히 통과시킨다. CRC 불일치를 만나면 조용히 잘못된
  가격을 배포하는 대신 재동기화를 시도한다.
* 파서를 상태머신으로 분리한 이유: asyncio 스트림, 소켓 recv, 파일 리플레이가
  모두 같은 파서를 공유해야 테스트가 하나로 끝난다.
"""

from __future__ import annotations

import struct
import zlib
from typing import Iterator

from .models import MSG_HEARTBEAT, MSG_NAMES

MAGIC = b"MDF1"
VERSION = 1
HEADER_FMT = "!4sBBHQI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 20
CRC_SIZE = 4
MAX_PAYLOAD = 1 << 20                        # 1MB. 넘으면 스트림이 깨진 것으로 본다

FLAG_SNAPSHOT = 1 << 0
FLAG_COMPRESSED = 1 << 1


class ProtocolError(Exception):
    pass


# 파서 내부 상태 신호 (프레임 완성 / 데이터 부족 / 재동기화 후 재시도)
_OK, _NEED_MORE, _RETRY = object(), object(), object()


def encode(msg_type: int, seq: int, payload: bytes = b"", flags: int = 0) -> bytes:
    """프레임 1개를 바이트로."""
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(f"payload too large: {len(payload)}")
    header = struct.pack(HEADER_FMT, MAGIC, VERSION, msg_type, flags, seq, len(payload))
    body = header + payload
    return body + struct.pack("!I", zlib.crc32(body) & 0xFFFFFFFF)


def heartbeat(seq: int, ts_ns: int) -> bytes:
    """하트비트. 무거래 구간에서도 연결 생존과 seq 진행을 알린다."""
    return encode(MSG_HEARTBEAT, seq, struct.pack("!Q", ts_ns))


class Frame:
    __slots__ = ("msg_type", "seq", "flags", "payload")

    def __init__(self, msg_type: int, seq: int, flags: int, payload: bytes):
        self.msg_type = msg_type
        self.seq = seq
        self.flags = flags
        self.payload = payload

    def __repr__(self) -> str:
        name = MSG_NAMES.get(self.msg_type, str(self.msg_type))
        return f"<Frame {name} seq={self.seq} len={len(self.payload)}>"


class FrameParser:
    """부분 수신을 견디는 스트리밍 파서.

    feed(chunk) 는 완성된 프레임만 순서대로 내놓고, 남은 조각은 내부 버퍼에
    보관한다. CRC/매직이 깨지면 다음 MAGIC 위치까지 건너뛰며 재동기화한다.
    """

    def __init__(self, on_resync=None):
        self._buf = bytearray()
        self._on_resync = on_resync
        self.resync_count = 0
        self.crc_error_count = 0

    def feed(self, chunk: bytes) -> Iterator[Frame]:
        self._buf += chunk
        while True:
            status, frame = self._try_one()
            if status is _NEED_MORE:
                return
            if status is _RETRY:
                # 재동기화 직후. 버퍼에 온전한 프레임이 더 있을 수 있으므로
                # 여기서 멈추면 한 번 깨진 세션이 영구히 멎는다. 계속 돈다.
                continue
            yield frame

    def _try_one(self):
        buf = self._buf
        if len(buf) < HEADER_SIZE:
            return _NEED_MORE, None

        if bytes(buf[:4]) != MAGIC:
            return self._resync()

        magic, ver, mtype, flags, seq, length = struct.unpack(
            HEADER_FMT, bytes(buf[:HEADER_SIZE])
        )
        if ver != VERSION or length > MAX_PAYLOAD:
            return self._resync()

        total = HEADER_SIZE + length + CRC_SIZE
        if len(buf) < total:
            return _NEED_MORE, None   # 아직 덜 왔다. 다음 recv를 기다린다

        body = bytes(buf[: HEADER_SIZE + length])
        (crc,) = struct.unpack("!I", bytes(buf[HEADER_SIZE + length : total]))
        if crc != (zlib.crc32(body) & 0xFFFFFFFF):
            self.crc_error_count += 1
            return self._resync()

        del self._buf[:total]
        return _OK, Frame(mtype, seq, flags, body[HEADER_SIZE:])

    def _resync(self):
        """다음 MAGIC 경계로 점프. 못 찾으면 꼬리 3바이트만 남긴다."""
        idx = self._buf.find(MAGIC, 1)
        if idx == -1:
            keep = len(MAGIC) - 1
            del self._buf[: max(0, len(self._buf) - keep)]
        else:
            del self._buf[:idx]
        self.resync_count += 1
        if self._on_resync:
            self._on_resync()
        # MAGIC을 못 찾았으면 더 받아야 하고, 찾았으면 그 자리에서 다시 시도한다
        return (_RETRY, None) if idx != -1 else (_NEED_MORE, None)


class SequenceTracker:
    """구독자 측 갭 탐지기.

    마켓데이터에서 '조용한 유실'은 최악이다. 값이 틀린 것보다 없는 걸 모르는
    게 위험하다. 그래서 수신자는 seq를 늘 검사하고, 갭을 발견하면 스냅샷
    재요청으로 복구한다.
    """

    def __init__(self):
        self.expected: int | None = None
        self.gap_count = 0
        self.lost_messages = 0
        self.duplicate_count = 0

    def observe(self, seq: int) -> int:
        """이번 프레임에서 유실된 개수를 돌려준다 (0이면 정상)."""
        if self.expected is None:
            self.expected = seq + 1
            return 0
        if seq == self.expected:
            self.expected += 1
            return 0
        if seq < self.expected:
            self.duplicate_count += 1
            return 0
        lost = seq - self.expected
        self.gap_count += 1
        self.lost_messages += lost
        self.expected = seq + 1
        return lost

    def stats(self) -> dict:
        return {
            "expected_seq": self.expected,
            "gap_count": self.gap_count,
            "lost_messages": self.lost_messages,
            "duplicate_count": self.duplicate_count,
        }
