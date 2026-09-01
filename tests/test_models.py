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


# ── 잘림과 충돌은 다르다 ────────────────────────────────────────────────────
# 2026-08-28 의 사고는 잘림이 아니라 **충돌**이었다. '코스피 대형주'와
# '코스피 중형주'가 같은 바이트가 되어 두 지수 가격이 한 계열로 섞였고,
# 품질 검사에 "한 틱에 709.73% 이동" CRITICAL 이 2,262건 쌓였다.
# 잘림만 세면 "잘리고는 있는데 해가 있나?" 에 답할 수 없다.

def _reset_trunc():
    from mdfeed import models
    models._TRUNCATED.clear()
    models._TRUNC_MAP.clear()


def test_잘려도_안_겹치면_충돌이_아니다():
    from mdfeed.models import Trade, symbol_collisions

    _reset_trunc()
    for name in ("미국 30년T-BOND", "미국 1년T-BILL"):
        Trade("KRX", name, 1, 1, 1.0, 1.0, 1).pack()
    assert symbol_collisions() == {}, "안 겹치는데 충돌로 봤다"


def test_잘린_뒤_같아지면_충돌로_잡는다():
    """UTF-8 로 고친 뒤에도 **앞 5글자가 같으면** 여전히 겹친다.

    '코스피 대형주' 와 '코스피 중형주' 는 이제 안 겹친다(대/중이 다르다) —
    그게 2026-08-28 수정의 성과다. 그런데 '코스피 대형주' 와 '코스피 대표주' 는
    둘 다 '코스피 대' 로 잘려 여전히 겹친다. 폭을 넓히지 않는 한 남는 위험이고,
    그래서 잘림이 아니라 **충돌**을 지표로 내야 한다.
    """
    from mdfeed.models import Trade, symbol_collisions

    _reset_trunc()
    for name in ("코스피 대형주", "코스피 대표주"):
        Trade("KRX", name, 1, 1, 1.0, 1.0, 1).pack()
    col = symbol_collisions()
    assert col, "같은 바이트가 됐는데 못 잡았다"
    (wire, originals), = col.items()
    assert wire == "코스피 대"
    assert sorted(originals) == sorted(["코스피 대형주", "코스피 대표주"])


def test_UTF8_수정_덕에_안_겹치게_된_쌍은_충돌이_아니다():
    """2026-08-28 사고의 두 지수. ascii+ignore 시절엔 둘 다 공백 하나였다."""
    from mdfeed.models import Trade, symbol_collisions

    _reset_trunc()
    for name in ("코스피 대형주", "코스피 중형주"):
        Trade("KRX", name, 1, 1, 1.0, 1.0, 1).pack()
    assert symbol_collisions() == {}, "고쳐진 쌍을 아직 충돌로 본다"


def test_안_잘리는_심볼은_기록도_안_남는다():
    from mdfeed.models import Trade, symbol_collisions, truncated_symbols

    _reset_trunc()
    Trade("UPBIT", "KRW-BTC", 1, 1, 1.0, 1.0, 1).pack()
    assert truncated_symbols() == {} and symbol_collisions() == {}


def test_충돌은_잘림보다_심각하다는_걸_구분해_낸다():
    """지표가 둘로 갈려야 경보 심각도를 다르게 걸 수 있다."""
    from mdfeed.models import Trade, symbol_collisions, truncated_symbols

    _reset_trunc()
    for name in ("미국 30년T-BOND", "코스피 대형주", "코스피 대표주"):
        Trade("KRX", name, 1, 1, 1.0, 1.0, 1).pack()
    assert len(truncated_symbols()) == 3      # 셋 다 잘렸고
    assert len(symbol_collisions()) == 1      # 겹친 건 한 쌍뿐이다
