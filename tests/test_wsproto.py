"""RFC 6455 WebSocket 구현 — 명세 벡터와 실전 함정."""
import pytest

from mdfeed.wsproto import (OP_BINARY, OP_CLOSE, OP_CONT, OP_PING, OP_TEXT,
                            FrameDecoder, WSError, accept_key,
                            encode_frame, handshake_response, server_frame)


def test_accept_key_matches_rfc6455_example():
    """RFC 6455 §1.3 의 예제 벡터. 이게 틀리면 어떤 서버와도 붙지 못한다."""
    assert accept_key("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="


@pytest.mark.parametrize("size,header_len", [
    (10, 2 + 4),          # 7비트 길이 + 마스크키
    (200, 4 + 4),         # 16비트 확장 길이
    (70_000, 10 + 4),     # 64비트 확장 길이
])
def test_extended_length_encoding(size, header_len):
    f = encode_frame(OP_BINARY, b"x" * size, mask=True)
    assert len(f) == header_len + size


def test_masking_roundtrip():
    payload = bytes(range(256)) * 4
    f = encode_frame(OP_BINARY, payload, mask=True)
    (op, out), = FrameDecoder(expect_masked=True).feed(f)
    assert op == OP_BINARY and out == payload


def test_server_frames_are_unmasked():
    f = server_frame(OP_TEXT, b"hi")
    assert (f[1] & 0x80) == 0        # MASK 비트 0


def test_masking_rule_violation_is_rejected():
    """서버는 마스킹 안 된 클라이언트 프레임을 거부해야 한다 (RFC 6455 §5.1)."""
    unmasked = encode_frame(OP_TEXT, b"hi", mask=False)
    with pytest.raises(WSError):
        list(FrameDecoder(expect_masked=True).feed(unmasked))


def test_fragmented_message_reassembly():
    a = bytearray(encode_frame(OP_TEXT, b"hel", mask=False))
    a[0] &= 0x7F                                     # FIN=0
    b = encode_frame(OP_CONT, b"lo!", mask=False)
    d = FrameDecoder(expect_masked=False)
    assert list(d.feed(bytes(a) + b)) == [(OP_TEXT, b"hello!")]


def test_control_frame_may_interleave_fragments():
    """제어 프레임은 단편 사이에 끼어들 수 있다. 못 다루면 ping 에서 연결이 끊긴다."""
    a = bytearray(encode_frame(OP_TEXT, b"AA", mask=False)); a[0] &= 0x7F
    ping = encode_frame(OP_PING, b"p", mask=False)
    b = encode_frame(OP_CONT, b"BB", mask=False)
    d = FrameDecoder(expect_masked=False)
    out = list(d.feed(bytes(a) + ping + b))
    assert out == [(OP_PING, b"p"), (OP_TEXT, b"AABB")]


def test_control_frame_over_125_bytes_refused():
    with pytest.raises(WSError):
        encode_frame(OP_CLOSE, b"x" * 126, mask=False)


def test_partial_frame_waits_for_more_data():
    f = encode_frame(OP_TEXT, b"hello world", mask=True)
    d = FrameDecoder(expect_masked=True)
    assert list(d.feed(f[:5])) == []
    assert list(d.feed(f[5:])) == [(OP_TEXT, b"hello world")]


def test_handshake_response_contains_accept():
    resp = handshake_response({"sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ=="})
    assert b"101 Switching Protocols" in resp
    assert b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in resp


def test_handshake_without_key_rejected():
    with pytest.raises(WSError):
        handshake_response({})
