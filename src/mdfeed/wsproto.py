"""RFC 6455 WebSocket — 클라이언트/서버 양쪽 직접 구현 (표준 라이브러리만).

왜 websockets 라이브러리를 안 썼나
----------------------------------
이 프로젝트가 증명하려는 항목 중 하나가 "네트워크 프로토콜(TCP/HTTP)에 대한
이해"다. 라이브러리를 부르면 그 이해가 코드에 남지 않는다. 여기서 실제로
다루는 것들:

* HTTP/1.1 Upgrade 핸드셰이크와 Sec-WebSocket-Accept 계산
  (SHA1(key + GUID) → base64. 캐시/프록시가 WS를 HTTP로 오인하지 못하게 하는 장치)
* 프레임 레이아웃: FIN/opcode, 확장 길이(7 / 7+16 / 7+64비트), 마스킹
* 클라이언트→서버는 반드시 마스킹, 서버→클라이언트는 반드시 비마스킹
  (마스킹은 보안이 아니라 캐시 오염 방지용이다)
* 단편화(continuation frame) 재조립
* 제어 프레임(ping/pong/close)은 125바이트 이하, 단편화 불가

부수 효과로 의존성이 0이 되어 어떤 리눅스 박스에서도 `python3` 만 있으면 돈다.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import ssl
import struct
from typing import Iterator
from urllib.parse import urlparse

GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT, OP_TEXT, OP_BINARY = 0x0, 0x1, 0x2
OP_CLOSE, OP_PING, OP_PONG = 0x8, 0x9, 0xA
CONTROL_OPS = {OP_CLOSE, OP_PING, OP_PONG}

MAX_FRAME = 16 * 1024 * 1024        # 16MB. 넘으면 상대가 이상한 것


class WSError(Exception):
    pass


# ── 프레임 인코딩 ──────────────────────────────────────────────────────────
def encode_frame(opcode: int, payload: bytes, mask: bool) -> bytes:
    """프레임 1개를 바이트로. mask=True 는 클라이언트가 보낼 때만."""
    if opcode in CONTROL_OPS and len(payload) > 125:
        raise WSError("제어 프레임은 125바이트를 넘을 수 없다")
    out = bytearray()
    out.append(0x80 | opcode)                       # FIN=1
    n = len(payload)
    mask_bit = 0x80 if mask else 0x00
    if n < 126:
        out.append(mask_bit | n)
    elif n < 65536:
        out.append(mask_bit | 126)
        out += struct.pack("!H", n)
    else:
        out.append(mask_bit | 127)
        out += struct.pack("!Q", n)
    if mask:
        key = os.urandom(4)
        out += key
        out += bytes(b ^ key[i & 3] for i, b in enumerate(payload))
    else:
        out += payload
    return bytes(out)


class FrameDecoder:
    """스트리밍 프레임 디코더. 단편화된 메시지를 재조립해서 내놓는다."""

    def __init__(self, expect_masked: bool):
        self._buf = bytearray()
        self._expect_masked = expect_masked
        self._frag_op = 0
        self._frag = bytearray()

    def feed(self, data: bytes) -> Iterator[tuple[int, bytes]]:
        """(opcode, payload) 를 완성된 메시지 단위로 내놓는다."""
        self._buf += data
        while True:
            item = self._parse()
            if item is None:
                return
            fin, opcode, payload = item

            if opcode in CONTROL_OPS:               # 제어 프레임은 단편화 사이에 끼어들 수 있다
                yield opcode, payload
                continue

            if opcode == OP_CONT:
                if not self._frag_op:
                    raise WSError("시작 프레임 없는 continuation")
                self._frag += payload
                if fin:
                    op, data_ = self._frag_op, bytes(self._frag)
                    self._frag_op, self._frag = 0, bytearray()
                    yield op, data_
            else:
                if fin:
                    yield opcode, payload
                else:
                    self._frag_op = opcode
                    self._frag = bytearray(payload)

    def _parse(self):
        buf = self._buf
        if len(buf) < 2:
            return None
        b0, b1 = buf[0], buf[1]
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        off = 2

        if length == 126:
            if len(buf) < off + 2:
                return None
            length = struct.unpack_from("!H", buf, off)[0]
            off += 2
        elif length == 127:
            if len(buf) < off + 8:
                return None
            length = struct.unpack_from("!Q", buf, off)[0]
            off += 8
        if length > MAX_FRAME:
            raise WSError(f"프레임이 너무 큼: {length}")

        if masked != self._expect_masked:
            # RFC 6455 §5.1 위반. 서버는 마스킹 안 된 클라이언트 프레임을 끊어야 한다
            raise WSError(f"마스킹 규칙 위반 (masked={masked})")

        key = b""
        if masked:
            if len(buf) < off + 4:
                return None
            key = bytes(buf[off:off + 4])
            off += 4

        if len(buf) < off + length:
            return None
        payload = bytes(buf[off:off + length])
        if masked:
            payload = bytes(b ^ key[i & 3] for i, b in enumerate(payload))
        del self._buf[: off + length]
        return fin, opcode, payload


# ── 클라이언트 ─────────────────────────────────────────────────────────────
class WSClient:
    """거래소 wss:// 엔드포인트에 붙는 최소 클라이언트."""

    def __init__(self, reader, writer, url: str):
        self.reader, self.writer, self.url = reader, writer, url
        self._dec = FrameDecoder(expect_masked=False)   # 서버→클라이언트는 비마스킹
        self._pending: list[tuple[int, bytes]] = []
        self.closed = False
        self.bytes_in = 0

    @classmethod
    async def connect(cls, url: str, headers: dict | None = None,
                      timeout: float = 10.0) -> "WSClient":
        u = urlparse(url)
        secure = u.scheme in ("wss", "https")
        port = u.port or (443 if secure else 80)
        path = u.path or "/"
        if u.query:
            path += "?" + u.query

        ctx = ssl.create_default_context() if secure else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(u.hostname, port, ssl=ctx, server_hostname=u.hostname if secure else None),
            timeout=timeout,
        )
        # Nagle 알고리즘 비활성화: 작은 구독 요청이 40ms씩 지연되면 안 된다
        sock = writer.get_extra_info("socket")
        if sock is not None:
            import socket as _s
            with __import__("contextlib").suppress(OSError):
                sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_NODELAY, 1)

        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            f"GET {path} HTTP/1.1",
            f"Host: {u.hostname}:{port}" if u.port else f"Host: {u.hostname}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            "User-Agent: mdfeed/1.0",
        ]
        for k, v in (headers or {}).items():
            req.append(f"{k}: {v}")
        writer.write(("\r\n".join(req) + "\r\n\r\n").encode())
        await writer.drain()

        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=timeout)
        head = raw.decode("latin-1")
        status = head.split("\r\n", 1)[0]
        if "101" not in status:
            raise WSError(f"업그레이드 거부: {status}")

        # 서버가 정말 우리 key 를 봤는지 검증. 프록시가 끼어들면 여기서 걸린다
        expect = base64.b64encode(hashlib.sha1(key.encode() + GUID).digest()).decode()
        accept = ""
        for line in head.split("\r\n")[1:]:
            if line.lower().startswith("sec-websocket-accept:"):
                accept = line.split(":", 1)[1].strip()
        if accept != expect:
            raise WSError("Sec-WebSocket-Accept 불일치")
        return cls(reader, writer, url)

    async def send_text(self, s: str) -> None:
        self.writer.write(encode_frame(OP_TEXT, s.encode(), mask=True))
        await self.writer.drain()

    async def send_binary(self, b: bytes) -> None:
        self.writer.write(encode_frame(OP_BINARY, b, mask=True))
        await self.writer.drain()

    async def ping(self, data: bytes = b"") -> None:
        self.writer.write(encode_frame(OP_PING, data, mask=True))
        await self.writer.drain()

    async def pong(self, data: bytes = b"") -> None:
        """PONG 제어 프레임을 보낸다.

        서버가 보낸 PING 에 대한 응답은 recv() 안에서 자동으로 나가지만,
        **애플리케이션 레벨 하트비트**(KIS 의 PINGPONG 처럼 텍스트 메시지로 오는 것)에
        PONG 으로 답해야 하는 경우가 있어 밖으로 열어 둔다.

        제어 프레임은 125바이트를 넘을 수 없다(RFC 6455 §5.5). 넘으면 잘라 보낸다 —
        하트비트 응답은 페이로드보다 '왔다는 사실'이 중요하다.
        """
        self.writer.write(encode_frame(OP_PONG, data[:125], mask=True))
        await self.writer.drain()

    async def recv(self, timeout: float | None = None) -> tuple[int, bytes]:
        """데이터 메시지 하나. ping 은 내부에서 pong 으로 응답하고 넘어간다."""
        while True:
            if self._pending:
                return self._pending.pop(0)
            chunk = await (asyncio.wait_for(self.reader.read(65536), timeout)
                           if timeout else self.reader.read(65536))
            if not chunk:
                self.closed = True
                raise WSError("연결이 닫힘")
            self.bytes_in += len(chunk)
            for op, payload in self._dec.feed(chunk):
                if op == OP_PING:
                    self.writer.write(encode_frame(OP_PONG, payload, mask=True))
                    await self.writer.drain()
                elif op == OP_PONG:
                    continue
                elif op == OP_CLOSE:
                    self.closed = True
                    raise WSError("상대가 close 프레임 전송")
                else:
                    self._pending.append((op, payload))

    CLOSE_TIMEOUT_S = 3.0

    async def close(self, timeout: float | None = None) -> None:
        """닫기 프레임을 보내되, 기한을 둔다.

        상대가 사라졌는데 RST 가 안 온 반쯤 죽은 연결에서는 송신 버퍼가 차서
        ``drain()`` 이 무기한 멈춘다. 이 대기가 세션 정리 경로에 있으면
        **재접속이 그 뒤에 줄을 서서 영원히 시작되지 않는다.**
        실측(2026-08-29): upbit 이 11.2시간 멎었는데 재접속은 3회에서
        멈춰 있었다. 어댑터 태스크는 살아 있었고, 정리가 안 끝나고 있었다.

        닫기 프레임은 예의이고 **소켓을 닫는 것이 목적**이다.
        예의를 지키느라 목적을 못 이루면 안 된다.
        """
        self.closed = True
        t = self.CLOSE_TIMEOUT_S if timeout is None else timeout
        try:
            self.writer.write(encode_frame(OP_CLOSE, struct.pack("!H", 1000), mask=True))
            await asyncio.wait_for(self.writer.drain(), timeout=t)
        except asyncio.CancelledError:
            # 이미 취소 중이어도 아래 finally 로 소켓은 반드시 닫는다
            pass
        except Exception:
            pass
        finally:
            try:
                self.writer.close()
            except Exception:
                pass


# ── 서버 ───────────────────────────────────────────────────────────────────
def accept_key(client_key: str) -> str:
    """Sec-WebSocket-Accept 계산."""
    return base64.b64encode(hashlib.sha1(client_key.encode() + GUID).digest()).decode()


def handshake_response(headers: dict[str, str]) -> bytes:
    key = headers.get("sec-websocket-key")
    if not key:
        raise WSError("Sec-WebSocket-Key 없음")
    return (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept_key(key)}\r\n\r\n"
    ).encode()


def server_frame(opcode: int, payload: bytes) -> bytes:
    return encode_frame(opcode, payload, mask=False)
