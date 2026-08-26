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
