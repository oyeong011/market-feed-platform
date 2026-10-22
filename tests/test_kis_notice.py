"""체결통보 해석 — 복호화와 필드 위치(명세 기준). 실측 전이라 위치는 상수 한 곳에서만 쓴다."""
import base64

import pytest

kn = pytest.importorskip("mdfeed.kis_notice")
pytest.importorskip("cryptography")

KEY, IV = "k" * 32, "i" * 16


def encrypt(text: str) -> str:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    p = padding.PKCS7(128).padder()
    data = p.update(text.encode()) + p.finalize()
    enc = Cipher(algorithms.AES(KEY.encode()), modes.CBC(IV.encode())).encryptor()
    return base64.b64encode(enc.update(data) + enc.finalize()).decode()


def fields(odno="0000012345", side="02", sym="005930", qty="2", price="70000", filled="2", refused="N"):
    f = [""] * 23
    f[kn.F_ODNO], f[kn.F_SIDE], f[kn.F_SYMBOL], f[kn.F_QTY], f[kn.F_PRICE] = odno, side, sym, qty, price
    f[kn.F_TIME], f[kn.F_REFUSED], f[kn.F_FILLED], f[kn.F_ORDER_QTY] = "093015", refused, filled, "5"
    return f


def test_decrypt_roundtrip_with_korean():
    text = "^".join(fields()) + "^삼성전자"
    assert kn.decrypt(KEY, IV, encrypt(text)) == text


def test_fill_notice():
    n = kn.parse(fields())
    assert (n.kind, n.side, n.symbol, n.qty, n.price, n.order_qty) == ("fill", "buy", "005930", 2, 70000, 5)


def test_ack_notice_carries_no_fill_qty():
    n = kn.parse(fields(filled="1"))
    assert n.kind == "ack" and n.qty == 0


def test_refused_notice():
    assert kn.parse(fields(refused="Y")).kind == "refused"


def test_short_or_garbled_body_is_not_a_notice():
    assert kn.parse(["1", "2"]) is None
    assert kn.parse(fields(qty="x")) is None


def test_dedupe_key_ignores_leading_zeros():
    assert kn.parse(fields(odno="0000012345")).key == kn.parse(fields(odno="12345")).key
