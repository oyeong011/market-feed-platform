"""정규화 스키마 — 바이너리 레이아웃과 파생 지표."""
import pytest

from mdfeed.models import (SIDE_BUY, SIDE_SELL, Bar, BookTop, Signal, Trade, now_ns)


def test_trade_is_cache_line_sized():
    assert Trade.SIZE == 64


def test_trade_roundtrip_preserves_fields():
    t = Trade("BINANCE", "BTCUSDT", 1_700_000_000_000_000_000,
              1_700_000_000_012_000_000, 68123.45, 0.00123456, SIDE_SELL)
    b = Trade.unpack(t.pack())
    assert (b.venue, b.symbol, b.side) == ("BINANCE", "BTCUSDT", SIDE_SELL)
    assert b.price == pytest.approx(68123.45)
    assert b.qty == pytest.approx(0.00123456)
    assert b.latency_us == pytest.approx(12000.0)


def test_symbol_longer_than_16_is_truncated_not_corrupting():
    """긴 심볼이 들어와도 옆 필드를 침범하면 안 된다."""
    t = Trade("VERYLONGVENUE", "A" * 40, now_ns(), now_ns(), 1.0, 1.0)
    b = Trade.unpack(t.pack())
    assert len(b.symbol) == 16 and len(b.venue) == 8
    assert b.price == 1.0 and b.qty == 1.0


def test_booktop_spread_basis_points():
    b = BookTop("UPBIT", "KRW-BTC", 0, 0, 100.0, 1.0, 101.0, 1.0)
    assert b.mid == pytest.approx(100.5)
    assert b.spread_bp == pytest.approx(1 / 100.5 * 10_000, rel=1e-9)


def test_booktop_empty_book_does_not_divide_by_zero():
    b = BookTop("UPBIT", "X", 0, 0, 0.0, 0.0, 0.0, 0.0)
    assert b.mid == 0.0 and b.spread_bp == 0.0


def test_signal_roundtrip():
    s = Signal("UPBIT", "KRW-ETH", now_ns(), "rsi_revert", -1, 0.75, 3_400_000.0)
    b = Signal.unpack(s.pack())
    assert b.strategy == "rsi_revert" and b.action == -1
    assert b.strength == pytest.approx(0.75)
    assert b.to_dict()["action_name"] == "SELL"


class TestBar:
    def test_ohlcv_and_vwap(self):
        bar = Bar("UPBIT", "KRW-BTC", 0, 60)
        for px, qty in [(100.0, 1.0), (110.0, 2.0), (90.0, 1.0), (105.0, 1.0)]:
            bar.update(Trade("UPBIT", "KRW-BTC", 0, 0, px, qty))
        assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 110.0, 90.0, 105.0)
        assert bar.volume == pytest.approx(5.0)
        assert bar.tick_count == 4
        # VWAP = Σ(px·qty)/Σqty = (100+220+90+105)/5
        assert bar.vwap == pytest.approx(515.0 / 5.0)

    def test_empty_bar_vwap_falls_back_to_close(self):
        assert Bar("U", "S", 0, 60).vwap == 0.0


# ── 심볼 인코딩 회귀 ────────────────────────────────────────────────────────
# 실측(2026-08-28): KRX 지수명이 전송 폭에서 전부 사라졌다.
# _fix 가 encode("ascii", "ignore") 를 써서 한글이 통째로 버려졌고,
# '코스피 대형주'와 '코스피 중형주'가 똑같이 b' ' 하나로 뭉개졌다.
# 두 지수의 가격이 한 계열로 섞여 품질 검사에 CRITICAL 이 쌓였다.

def test_한글_심볼이_왕복한다():
    from mdfeed.models import _fix, _unfix
    for s in ("코스피", "코스닥", "코스피200"):
        assert _unfix(_fix(s, 16)) == s


def test_서로_다른_지수가_같은_바이트로_뭉개지지_않는다():
    from mdfeed.models import _fix
    a = _fix("코스피 대형주", 16)
    b = _fix("코스피 중형주", 16)
    c = _fix("코스피 소형주", 16)
    assert len({a, b, c}) == 3


def test_폭에서_자를_때_문자_경계를_지킨다():
    from mdfeed.models import _fix, _unfix
    # 한글 한 자는 3바이트다. 16바이트 경계가 글자 중간에 걸린다.
    out = _unfix(_fix("가나다라라마바사", 16))
    assert "�" not in out       # 깨진 바이트가 남으면 안 된다
    assert out == "가나다라라"


def test_잘린_심볼은_기록된다():
    from mdfeed.models import _fix, truncated_symbols
    _fix("아주아주아주긴심볼이름입니다", 16)
    assert any("아주아주" in k for k in truncated_symbols())


def test_ascii_심볼은_그대로다():
    from mdfeed.models import _fix, _unfix
    for s in ("BTCUSDT", "KRW-BTC", "005930"):
        assert _unfix(_fix(s, 16)) == s
