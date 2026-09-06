"""라이브 시그널 모의 체결 장부.

백테스트가 실전보다 좋게 나오는 이유는 대개 하나다 — **볼 수 없었던 값으로
체결시키기 때문**이다. 여기서 못 박는 것도 그것이다.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "quant"))
import paper_ledger as pl                                     # noqa: E402

BAR = pl.BAR_US


class _Cur(list):
    """sqlite3 커서 흉내. 순회도 되고 fetchall() 도 된다."""

    def fetchall(self):
        return list(self)


class _Conn:
    """sqlite 대신 쓰는 최소 저장소. signals 와 bars_1m 만 낸다."""

    def __init__(self, signals, bars):
        self._sig = signals
        # 실제 쿼리는 ORDER BY venue, symbol, bucket 이다. Bars.load 가
        # 그 순서를 전제하므로 여기서도 맞춰 준다.
        self._bars = sorted(bars, key=lambda b: (b[0], b[1], b[2]))

    def execute(self, sql, params=()):
        if "PRAGMA" in sql:
            return _Cur()
        if "FROM signals" in sql:
            return _Cur(self._sig)
        if "FROM bars_1m" in sql:
            return _Cur(self._bars)
        return _Cur()

    def close(self):
        pass


def _run(signals, bars, monkeypatch, **kw):
    monkeypatch.setattr(pl.sqlite3, "connect", lambda *a, **k: _Conn(signals, bars))
    return pl.run("x.db", **kw)


# ── 미래 참조 ──────────────────────────────────────────────────────────────

def test_시그널이_난_봉으로_체결하지_않는다(monkeypatch):
    """시그널은 봉이 닫힌 뒤 나온다. 그 봉 종가로 체결하면 '종가를 보고
    종가에 산' 것이 되어 성과가 부풀려진다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0),
           (t + BAR, "UPBIT", "KRW-A", "s", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t, 999.0),            # 시그널이 난 봉 — 쓰면 안 된다
            ("UPBIT", "KRW-A", t + BAR, 100.0),      # 매수는 여기서
            ("UPBIT", "KRW-A", t + 2 * BAR, 110.0)]  # 매도는 여기서
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    d = r["strategies"]["s"]
    assert d["trades"] == 1
    assert d["avg_ret"] == pytest.approx(0.10, abs=1e-9)   # 100 → 110


def test_ref_price_를_체결가로_쓰지_않는다(monkeypatch):
    """ref_price 는 시그널이 난 봉의 종가다. 그걸 체결가로 쓰면 위와 같은 반칙이다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 50.0),      # ref 50 — 쓰면 안 됨
           (t + BAR, "UPBIT", "KRW-A", "s", pl.SELL, 50.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0),
            ("UPBIT", "KRW-A", t + 2 * BAR, 100.0)]
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    # ref(50)로 사고 100에 팔았다면 +100%. 실제로는 100 → 100 이라 0%.
    assert r["strategies"]["s"]["avg_ret"] == pytest.approx(0.0, abs=1e-9)


def test_다음_봉이_없으면_미체결로_센다(monkeypatch):
    """피드가 멎어 봉이 없는 구간이다. 조용히 버리면 '거래가 없었다'와
    '체결할 수 없었다'가 구분되지 않는다 — 그게 이 장부의 핵심 지표다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0)]
    bars = [("UPBIT", "KRW-A", t, 100.0)]            # 시그널 이후 봉이 없다
    r = _run(sig, bars, monkeypatch)
    assert r["unfilled_total"] == 1
    assert r["strategies"] == {}                      # 거래는 안 생긴다


def test_봉이_너무_멀면_체결하지_않는다(monkeypatch):
    """하루 뒤에 체결하는 건 그 전략이 의도한 거래가 아니다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0)]
    bars = [("UPBIT", "KRW-A", t + 100 * BAR, 100.0)]
    r = _run(sig, bars, monkeypatch)
    assert r["unfilled_total"] == 1


# ── 매매 대상 ──────────────────────────────────────────────────────────────

def test_지수는_매매_대상이_아니다(monkeypatch):
    """코스피를 살 수는 없다.

    이걸 안 걸렀을 때 KRX-IDX 가 단일 거래 +712.7% 를 냈고 총손익의
    대부분을 차지했다. 08-28 오후 지수 이름이 잘려 빈 문자열이 된 구간에서
    코스닥(830)과 코스피(6,874)가 한 종목으로 뭉쳤던 탓이다.
    데이터 결함이 없었더라도 지수를 매매 대상에 넣은 것 자체가 잘못이다.
    """
    t = 1_000 * BAR
    sig = [(t, "KRX-IDX", "코스피", "s", pl.BUY, 100.0),
           (t + BAR, "KRX-IDX", "코스피", "s", pl.SELL, 100.0)]
    bars = [("KRX-IDX", "코스피", t + BAR, 100.0),
            ("KRX-IDX", "코스피", t + 2 * BAR, 900.0)]
    r = _run(sig, bars, monkeypatch)
    assert r["strategies"] == {}
    assert r["excluded"]["non_tradable_venue"] == 2


def test_빈_종목명은_제외한다(monkeypatch):
    """빈 종목명은 서로 다른 것이 뭉쳐 있다는 뜻이다. 값을 믿을 수 없다."""
    t = 1_000 * BAR
    sig = [(t, "KRX", "", "s", pl.BUY, 100.0),
           (t + BAR, "KRX", "  ", "s", pl.SELL, 100.0)]
    r = _run(sig, [], monkeypatch)
    assert r["excluded"]["blank_symbol"] == 2
    assert r["unfilled_total"] == 0        # 미체결이 아니라 제외다


# ── 포지션·비용 ────────────────────────────────────────────────────────────

def test_왕복_비용을_뺀다(monkeypatch):
    """비용을 안 뺀 수익률은 의미가 없다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0),
           (t + BAR, "UPBIT", "KRW-A", "s", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0),
            ("UPBIT", "KRW-A", t + 2 * BAR, 100.0)]
    free = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    paid = _run(sig, bars, monkeypatch, fee=0.001, slip_bp=10.0)
    assert free["strategies"]["s"]["avg_ret"] == pytest.approx(0.0, abs=1e-9)
    assert paid["strategies"]["s"]["avg_ret"] < -0.003      # 왕복이니 두 번 문다


def test_포지션이_없는데_매도하면_무시한다(monkeypatch):
    """공매도를 모델링하지 않는다. 없는 걸 판 것으로 세면 안 된다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0)]
    r = _run(sig, bars, monkeypatch)
    assert r["strategies"] == {}


def test_이미_보유중이면_또_사지_않는다(monkeypatch):
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0),
           (t + BAR, "UPBIT", "KRW-A", "s", pl.BUY, 100.0),
           (t + 2 * BAR, "UPBIT", "KRW-A", "s", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0),
            ("UPBIT", "KRW-A", t + 2 * BAR, 200.0),
            ("UPBIT", "KRW-A", t + 3 * BAR, 110.0)]
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    d = r["strategies"]["s"]
    assert d["trades"] == 1
    assert d["avg_ret"] == pytest.approx(0.10, abs=1e-9)   # 첫 진입 100 기준


def test_전략별로_포지션을_따로_잡는다(monkeypatch):
    """같은 종목이라도 전략이 다르면 별개 거래다. 섞으면 한쪽 매도가
    다른 쪽 포지션을 닫아 버린다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "a", pl.BUY, 100.0),
           (t, "UPBIT", "KRW-A", "b", pl.BUY, 100.0),
           (t + BAR, "UPBIT", "KRW-A", "a", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0),
            ("UPBIT", "KRW-A", t + 2 * BAR, 110.0)]
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    assert r["strategies"]["a"]["trades"] == 1
    assert "b" not in r["strategies"]                  # b 는 아직 들고 있다
    assert r["strategies"]["a"]["open_at_end"] == 0


# ── 집계 ───────────────────────────────────────────────────────────────────

def test_겹치는_거래에_복리를_먹이지_않는다(monkeypatch):
    """처음엔 수익률을 순차로 곱했다. 그런데 거래는 시간상 겹치므로
    한 계좌를 차례로 복리 굴린 것처럼 계산할 수 없다 — 같은 순간에
    자본을 여러 번 전액 투입할 수는 없다.

    실측: 평균 +0.731% 인데 곱셈 누적은 −40% 가 나왔다. 명목 고정이면
    손익은 그냥 더한 값이고 평균 × 거래수와 일치한다.
    """
    t = 1_000 * BAR
    sig, bars = [], []
    for i in range(3):                    # 세 종목이 같은 시각에 겹쳐 거래된다
        s = f"KRW-{i}"
        sig += [(t, "UPBIT", s, "s", pl.BUY, 100.0),
                (t + BAR, "UPBIT", s, "s", pl.SELL, 100.0)]
        bars += [("UPBIT", s, t + BAR, 100.0),
                 ("UPBIT", s, t + 2 * BAR, 110.0)]
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    d = r["strategies"]["s"]
    assert d["trades"] == 3
    # 더한 값이어야 한다. 곱셈이면 1.1^3-1 = 0.331 이 나온다.
    assert d["pnl_units"] == pytest.approx(0.30, abs=1e-9)
    assert d["pnl_units"] == pytest.approx(d["avg_ret"] * d["trades"], abs=1e-9)


def test_손실은_손실로_보고한다(monkeypatch):
    """파라미터를 성과가 나올 때까지 고르는 게 백테스트를 쓸모없게 만든다."""
    t = 1_000 * BAR
    sig = [(t, "UPBIT", "KRW-A", "s", pl.BUY, 100.0),
           (t + BAR, "UPBIT", "KRW-A", "s", pl.SELL, 100.0)]
    bars = [("UPBIT", "KRW-A", t + BAR, 100.0),
            ("UPBIT", "KRW-A", t + 2 * BAR, 90.0)]
    r = _run(sig, bars, monkeypatch, fee=0.0, slip_bp=0.0)
    d = r["strategies"]["s"]
    assert d["avg_ret"] < 0 and d["pnl_units"] < 0
    assert d["win_rate"] == 0.0
