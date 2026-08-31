"""데이터 품질 검사 — 이 프로젝트의 첫 번째 원칙을 집행하는 부분.

    틀린 값을 조용히 배포하는 것이 값이 없는 것보다 나쁘다.

검사기의 실패 방식은 두 가지고, 둘 다 위험하다.
  놓침(false negative) — 틀린 값이 그대로 나간다
  오탐(false positive) — 사람이 알람을 무시하게 되고, 진짜가 와도 무시한다
아래 테스트는 양쪽을 다 고정한다.
"""
import pytest

from mdfeed.quality import (SEV_CRITICAL, SEV_WARNING, BarIntegrityCheck,
                            CrossVenueCheck, PriceJumpCheck, QualityMonitor,
                            QuoteSanityCheck, StaleValueCheck)

NS = 1_700_000_000_000_000_000


class TestPriceJump:
    def test_large_absolute_jump_is_critical(self):
        c = PriceJumpCheck(abs_pct=10.0)
        assert c.check("T", "S", 100.0, NS) is None      # 첫 관측은 기준선
        ev = c.check("T", "S", 120.0, NS)
        assert ev and ev.severity == SEV_CRITICAL

    def test_normal_movement_is_silent(self):
        """오탐이 잦으면 알람 전체가 무시된다."""
        c = PriceJumpCheck()
        px = 100.0
        fired = 0
        for i in range(300):
            px *= 1.0005 if i % 2 else 0.9995
            if c.check("T", "S", px, NS):
                fired += 1
        assert fired == 0

    def test_volatility_relative_catches_quiet_symbol(self):
        """평소 0.01% 움직이던 종목의 1% 는 이상하다.
        절대 임계만 쓰면 이런 건 못 잡는다."""
        c = PriceJumpCheck(abs_pct=10.0, sigma=8.0)
        px = 100.0
        for _ in range(40):
            px += 0.01
            c.check("T", "QUIET", px, NS)
        ev = c.check("T", "QUIET", px * 1.02, NS)
        assert ev and ev.severity == SEV_WARNING

    def test_volatile_symbol_not_flagged_for_same_move(self):
        """평소 3% 씩 흔들리던 종목의 2% 는 정상이다.
        같은 변화폭이라도 종목마다 정상 범위가 다르다."""
        import random
        rnd = random.Random(3)
        c = PriceJumpCheck(abs_pct=10.0, sigma=8.0)
        px = 100.0
        for _ in range(60):
            px *= 1 + rnd.uniform(-0.03, 0.03)
            c.check("T", "WILD", px, NS)
        assert c.check("T", "WILD", px * 1.02, NS) is None

    def test_symbols_are_independent(self):
        c = PriceJumpCheck()
        c.check("T", "A", 100.0, NS)
        assert c.check("T", "B", 5.0, NS) is None      # B 의 첫 관측일 뿐


class TestQuoteSanity:
    def test_crossed_quote_is_critical(self):
        """매수 > 매도는 정상 시장에서 지속될 수 없다.
        필드를 뒤바꿔 읽었을 가능성이 훨씬 높다."""
        ev = QuoteSanityCheck().check("T", "S", bid=105.0, ask=100.0, ts_ns=NS)
        assert ev and ev.severity == SEV_CRITICAL
        assert "뒤바뀌" in ev.detail

    def test_normal_quote_silent(self):
        assert QuoteSanityCheck().check("T", "S", 99.9, 100.1, NS) is None

    def test_wide_spread_warns(self):
        ev = QuoteSanityCheck(max_spread_bp=500).check("T", "S", 90.0, 110.0, NS)
        assert ev and ev.severity == SEV_WARNING

    def test_zero_or_missing_quote_ignored(self):
        """호가가 아직 안 온 상태를 이상으로 보면 기동 직후 알람이 쏟아진다."""
        c = QuoteSanityCheck()
        assert c.check("T", "S", 0.0, 100.0, NS) is None
        assert c.check("T", "S", 100.0, 0.0, NS) is None


class TestStaleValue:
    def test_repeated_value_warns_once(self):
        c = StaleValueCheck(after_s=10.0, min_updates=5)
        base = NS
        events = [c.check("T", "S", 100.0, base + i * 3_000_000_000) for i in range(8)]
        fired = [e for e in events if e]
        assert len(fired) == 1, "같은 이상으로 반복 알람하면 무시하게 된다"
        assert fired[0].severity == SEV_WARNING

    def test_change_resets(self):
        c = StaleValueCheck(after_s=10.0, min_updates=3)
        base = NS
        for i in range(6):
            c.check("T", "S", 100.0, base + i * 3_000_000_000)
        c.check("T", "S", 101.0, base + 30_000_000_000)          # 값이 바뀜
        assert c.check("T", "S", 101.0, base + 33_000_000_000) is None


class TestBarIntegrity:
    def test_low_above_open_is_critical(self):
        ev = BarIntegrityCheck().check("T", "S", o=100, h=110, l=105, c=108, ts_ns=NS)
        assert ev and ev.severity == SEV_CRITICAL

    def test_valid_bar_silent(self):
        assert BarIntegrityCheck().check("T", "S", o=100, h=110, l=95, c=105, ts_ns=NS) is None

    def test_flat_bar_is_valid(self):
        """네 값이 모두 같은 봉은 거래가 한 번뿐인 정상 상태다."""
        assert BarIntegrityCheck().check("T", "S", 100, 100, 100, 100, NS) is None


class TestCrossVenue:
    def _feed(self, c, krw_btc, usd_btc, krw_eth, usd_eth, krw_sol, usd_sol):
        for sym, px in (("UPBIT:KRW-BTC", krw_btc), ("BINANCE:BTCUSDT", usd_btc),
                        ("UPBIT:KRW-ETH", krw_eth), ("BINANCE:ETHUSDT", usd_eth),
                        ("UPBIT:KRW-SOL", krw_sol), ("BINANCE:SOLUSDT", usd_sol)):
            v, s = sym.split(":")
            c.observe(v, s, px)

    def test_implied_fx_agrees_when_data_is_consistent(self):
        c = CrossVenueCheck()
        self._feed(c, 1.5e8, 1.0e5, 6.0e6, 4000.0, 1.51e5, 100.0)
        fx = c.implied_fx()
        assert set(fx) == {"BTC", "ETH", "SOL"}
        assert c.check(NS) is None

    def test_divergent_asset_is_detected(self):
        """한 종목만 크게 벌어지면 그 종목의 시세가 이상하다는 뜻이다."""
        c = CrossVenueCheck(divergence_pct=3.0)
        self._feed(c, 1.5e8, 1.0e5, 6.0e6, 4000.0, 2.1e5, 100.0)   # SOL 만 40% 이탈
        ev = c.check(NS)
        assert ev and ev.symbol == "SOL"

    def test_first_alarm_is_not_swallowed_by_cooldown(self):
        """쿨다운 기준값을 0.0 으로 두면 (now - 0) < 60 구간에서
        첫 발화가 통째로 사라진다. 실제로 그 버그가 있었다."""
        c = CrossVenueCheck(divergence_pct=3.0)
        self._feed(c, 1.5e8, 1.0e5, 6.0e6, 4000.0, 2.1e5, 100.0)
        assert c.check(0) is not None

    def test_cooldown_suppresses_repeat(self):
        c = CrossVenueCheck(divergence_pct=3.0)
        self._feed(c, 1.5e8, 1.0e5, 6.0e6, 4000.0, 2.1e5, 100.0)
        assert c.check(NS) is not None
        assert c.check(NS + 1_000_000_000) is None      # 1초 뒤 — 억제

    def test_not_enough_assets_is_silent(self):
        """자산이 둘뿐이면 어느 쪽이 틀렸는지 알 수 없다. 판정하지 않는다."""
        c = CrossVenueCheck(min_assets=3)
        c.observe("UPBIT", "KRW-BTC", 1.5e8)
        c.observe("BINANCE", "BTCUSDT", 1.0e5)
        assert c.check(NS) is None


class TestMonitor:
    def test_counts_and_recent_are_bounded(self):
        m = QualityMonitor()
        for i in range(400):
            m.on_quote("T", f"S{i}", bid=110.0, ask=100.0, ts_ns=NS)
        rep = m.report()
        assert rep["critical"] == 400
        assert len(rep["recent"]) <= 20, "무한히 쌓이면 메모리가 샌다"

    def test_report_shape(self):
        m = QualityMonitor()
        m.on_trade("T", "S", 100.0, NS)
        rep = m.report()
        for k in ("checked", "critical", "warning", "by_check", "implied_fx", "recent"):
            assert k in rep


class TestPriceJump시간간격:
    """`한 틱에 X% 이동` 은 두 틱이 붙어 있을 때만 참이다.

    실측(2026-08-29): upbit 이 11.2시간 멎었다가 돌아온 직후 CRITICAL 2건이 났다.
    데이터에는 아무 문제가 없었다 — 기준가가 11시간 전 값이었다.
    이 검사는 ts_ns 를 받아 놓고 간격에 쓰지 않아, 스스로 이름 붙인 조건을
    확인하지 않고 있었다.
    """

    SEC = 1_000_000_000

    def test_오래_비었다_돌아오면_한_틱_판정을_안_한다(self):
        """CRITICAL 이 아니어야 한다. 그렇다고 조용히 버려서도 안 된다."""
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        assert c.check("UPBIT", "KRW-BTC", 100.0, t) is None
        # 11.2시간 뒤 첫 틱. 값은 정상인데 기준가가 낡았다.
        later = t + int(11.2 * 3600 * self.SEC)
        ev = c.check("UPBIT", "KRW-BTC", 130.0, later)
        assert c.ref_resets == 1
        assert ev is not None and ev.severity == "WARNING", "조용히 버리면 안 된다"
        assert "첫 틱" in ev.detail and "40,320초" in ev.detail

    def test_간격이_벌어져도_작은_움직임은_안_낸다(self):
        """벌어진 간격 자체가 사건은 아니다. 큰 움직임일 때만 알린다."""
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        c.check("UPBIT", "KRW-BTC", 100.0, t)
        assert c.check("UPBIT", "KRW-BTC", 101.0, t + 3600 * self.SEC) is None
        assert c.ref_resets == 1

    def test_비유동_종목의_큰_움직임을_통째로_놓치지_않는다(self):
        """거래가 뜸한 종목은 장중에도 몇 분씩 비는데, 그때 상한가가 온다.

        간격이 벌어졌다고 아무것도 안 내면 **정작 큰 움직임이 중요한 종목에서만**
        판정이 꺼진다. 기본 임계를 60초에서 300초로 올린 이유이기도 하다.
        """
        c = PriceJumpCheck(abs_pct=10.0)          # 기본값
        assert c.max_gap_s == 300.0
        t = 1_700_000_000 * self.SEC
        c.check("KIS", "005930", 70_000.0, t)
        ev = c.check("KIS", "005930", 91_000.0, t + 600 * self.SEC)   # 10분 뒤 +30%
        assert ev is not None and ev.severity == "WARNING"

    def test_재설정_뒤에는_다시_정상_판정한다(self):
        """기준을 버리는 것이지 검사를 끄는 게 아니다."""
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        c.check("UPBIT", "KRW-BTC", 100.0, t)
        c.check("UPBIT", "KRW-BTC", 130.0, t + 3600 * self.SEC)   # 재설정
        ev = c.check("UPBIT", "KRW-BTC", 200.0, t + 3601 * self.SEC)
        assert ev is not None and ev.severity == "CRITICAL"

    def test_간격이_짧으면_그대로_잡는다(self):
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        c.check("UPBIT", "KRW-BTC", 100.0, t)
        ev = c.check("UPBIT", "KRW-BTC", 130.0, t + self.SEC // 2)
        assert ev is not None and ev.severity == "CRITICAL"
        assert c.ref_resets == 0

    def test_경보_문구에_간격을_적는다(self):
        """읽는 사람이 '한 틱'이 몇 초인지 알아야 판단할 수 있다."""
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        c.check("UPBIT", "KRW-BTC", 100.0, t)
        ev = c.check("UPBIT", "KRW-BTC", 130.0, t + 2 * self.SEC)
        assert "2.0초 간격" in ev.detail

    def test_시각이_거꾸로_와도_기준을_버린다(self):
        """샤드나 재생이 섞이면 과거 시각이 뒤에 올 수 있다."""
        c = PriceJumpCheck(abs_pct=10.0, max_gap_s=60.0)
        t = 1_700_000_000 * self.SEC
        c.check("UPBIT", "KRW-BTC", 100.0, t)
        ev = c.check("UPBIT", "KRW-BTC", 130.0, t - 3600 * self.SEC)
        assert c.ref_resets == 1
        assert ev is None or ev.severity == "WARNING", "한 틱 판정을 하면 안 된다"

    def test_간격_임계를_설정으로_바꿀_수_있다(self):
        from mdfeed.config import Config
        from mdfeed.quality import QualityMonitor

        cfg = Config()
        cfg.qc_jump_max_gap_s = 42.0
        assert QualityMonitor(cfg).jump.max_gap_s == 42.0

    def test_재설정_횟수가_보고에_나온다(self):
        from mdfeed.quality import QualityMonitor

        m = QualityMonitor()
        t = 1_700_000_000 * self.SEC
        m.on_trade("UPBIT", "KRW-BTC", 100.0, t)
        m.on_trade("UPBIT", "KRW-BTC", 130.0, t + 3600 * self.SEC)
        assert m.report()["price_ref_resets"] == 1
