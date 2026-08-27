"""KIS 어댑터 — 실전 계좌로 확인한 동작을 고정한다.

여기 있는 테스트는 전부 2026-08-27 장중에 실계좌로 관측한 결과를 근거로 한다.
문서와 실제가 달랐던 항목들이라, 회귀하면 조용히 데이터가 망가진다.
"""
import datetime as dt

import pytest

from mdfeed.adapters.kis import (KST, SIDE_BY_SIGN, market_is_open,
                                 synth_event_ns, ENDPOINTS, MAX_TS_DRIFT_S)
from mdfeed.models import SIDE_BUY, SIDE_SELL


class TestSideMapping:
    def test_five_is_sell_not_premarket(self):
        """문서는 `5 장전`이라고 하지만 실측은 매도였다.

        실전 계좌 300건 표본:
          체결구분 1 → 매도호가 이상 70건 / 매수호가 이하 3건  → 매수
          체결구분 5 → 매도호가 이상  0건 / 매수호가 이하 173건 → 매도

        문서대로 3만 매도로 두면 매도 체결이 전부 UNKNOWN 이 되어
        주문흐름 방향이 통째로 사라진다.
        """
        assert SIDE_BY_SIGN["5"] == SIDE_SELL

    def test_one_is_buy(self):
        assert SIDE_BY_SIGN["1"] == SIDE_BUY

    def test_three_also_treated_as_sell(self):
        """문서상 매도 코드도 함께 매도로 둔다 (다른 구간에서 올 수 있다)."""
        assert SIDE_BY_SIGN["3"] == SIDE_SELL

    def test_unmapped_sign_is_not_silently_a_side(self):
        assert "9" not in SIDE_BY_SIGN


class TestTimestampSynthesis:
    """KIS 는 HHMMSS 만 준다. 날짜를 붙여야 지연을 잴 수 있다."""

    def test_recent_time_synthesizes(self):
        now = dt.datetime.now(KST)
        recv = int(now.timestamp() * 1e9)
        hhmmss = (now - dt.timedelta(seconds=3)).strftime("%H%M%S")
        ns, ok = synth_event_ns(hhmmss, recv)
        assert ok is True
        assert 2.0 < (recv - ns) / 1e9 < 4.0

    def test_large_drift_falls_back_to_recv(self):
        """장 마감 후 지연 전송이나 시계 이상으로 크게 벌어지면 믿지 않는다.
        조용히 틀린 타임스탬프를 만드는 게 못 재는 것보다 나쁘다."""
        now = dt.datetime.now(KST)
        recv = int(now.timestamp() * 1e9)
        hhmmss = (now - dt.timedelta(hours=3)).strftime("%H%M%S")
        ns, ok = synth_event_ns(hhmmss, recv)
        assert ok is False and ns == recv

    @pytest.mark.parametrize("junk", ["", "abcdef", "99", "1234567", "259999", "  "])
    def test_malformed_input_falls_back(self, junk):
        recv = 1_700_000_000_000_000_000
        ns, ok = synth_event_ns(junk, recv)
        assert ok is False and ns == recv

    def test_drift_threshold_is_explicit(self):
        assert MAX_TS_DRIFT_S == 600.0


class TestMarketHours:
    """장 시간 밖의 무데이터를 정체로 판정하면 밤새 재접속만 반복한다."""

    def test_weekday_midday_open(self):
        assert market_is_open(dt.datetime(2026, 8, 27, 10, 30, tzinfo=KST)) is True

    def test_before_open_closed(self):
        assert market_is_open(dt.datetime(2026, 8, 27, 8, 59, tzinfo=KST)) is False

    def test_after_close_closed(self):
        assert market_is_open(dt.datetime(2026, 8, 27, 16, 0, tzinfo=KST)) is False

    @pytest.mark.parametrize("day", [29, 30])   # 2026-08-29 토, 08-30 일
    def test_weekend_closed(self, day):
        assert market_is_open(dt.datetime(2026, 8, day, 11, 0, tzinfo=KST)) is False


class TestEndpoints:
    def test_real_and_mock_are_distinct(self):
        assert ENDPOINTS["real"][1] != ENDPOINTS["vts"][1]
        assert ":21000" in ENDPOINTS["real"][1]
        assert ":31000" in ENDPOINTS["vts"][1]


class TestSubscriptionLimit:
    def test_limit_is_learned_from_rejection(self):
        """문서 기준값 41을 믿지 않는다. 실계좌에서는 3에서 거절됐다."""
        from mdfeed.adapters.kis import KISAdapter
        from mdfeed.config import Config

        cfg = Config()
        cfg.kis_app_key = "x"
        cfg.kis_app_secret = "y"
        cfg.kis_symbols = ["A", "B", "C", "D", "E"]
        a = KISAdapter(cfg, lambda m: None)
        assert a.effective_limit is None

        a.subscribed = ["A", "B", "C"]
        msg = {"header": {"tr_id": "H0STCNT0", "tr_key": "D"},
               "body": {"rt_cd": "1", "msg1": "MAX SUBSCRIBE OVER"}}
        import asyncio, json
        asyncio.run(a._on_control(json.dumps(msg), None))
        assert a.effective_limit == 3
        assert a.rejected and a.rejected[0]["tr_key"] == "D"

    def test_already_in_subscribe_is_not_an_error(self):
        """재접속 직후 흔히 나온다. 오류로 처리하면 멀쩡한 세션을 버린다."""
        from mdfeed.adapters.kis import KISAdapter
        from mdfeed.config import Config
        import asyncio, json

        cfg = Config()
        cfg.kis_app_key, cfg.kis_app_secret = "x", "y"
        cfg.kis_symbols = ["005930"]
        a = KISAdapter(cfg, lambda m: None)
        msg = {"header": {"tr_id": "H0STCNT0", "tr_key": "005930"},
               "body": {"rt_cd": "1", "msg1": "ALREADY IN SUBSCRIBE"}}
        asyncio.run(a._on_control(json.dumps(msg), None))
        assert a.rejected == []
        assert a.subscribed == ["005930"]


def test_adapter_disabled_without_credentials():
    from mdfeed.adapters.kis import KISAdapter
    from mdfeed.config import Config
    cfg = Config()
    cfg.kis_app_key = cfg.kis_app_secret = ""
    a = KISAdapter(cfg, lambda m: None)
    assert a.enabled() is False
    assert "KIS_APP_KEY" in a.disabled_reason()


class TestAdaptiveRateLimiter:
    """유량 제한기는 서버가 알려주지 않는 한도를 스스로 찾아야 한다."""

    def test_backs_off_on_rate_limit(self):
        from mdfeed.adapters.kis_rest import AdaptiveRateLimiter
        r = AdaptiveRateLimiter(3.0)
        before = r.interval
        r.on_rate_limited()
        assert r.interval > before
        assert r.backoffs == 1

    def test_recovers_after_sustained_success(self):
        from mdfeed.adapters.kis_rest import AdaptiveRateLimiter
        r = AdaptiveRateLimiter(3.0)
        r.on_rate_limited()
        slowed = r.interval
        for _ in range(40):
            r.on_success()
        assert r.interval < slowed

    def test_never_faster_than_configured_base(self):
        """회복이 설정값을 넘어 가속하면 결국 다시 거절당한다."""
        from mdfeed.adapters.kis_rest import AdaptiveRateLimiter
        r = AdaptiveRateLimiter(3.0)
        for _ in range(500):
            r.on_success()
        assert r.interval >= r.base_interval - 1e-12
        assert r.current_rate <= 3.0 + 1e-9

    def test_backoff_is_capped(self):
        """연속 거절에도 무한히 느려지면 안 된다."""
        from mdfeed.adapters.kis_rest import AdaptiveRateLimiter
        r = AdaptiveRateLimiter(3.0, max_interval=2.0)
        for _ in range(200):
            r.on_rate_limited()
        assert r.interval <= 2.0


class TestBreadthAdapter:
    def test_synthetic_trade_only_on_volume_increase(self):
        """REST 응답은 체결이 아니라 스냅샷이다. 누적거래량이 늘었을 때만
        그 증가분을 합성 체결로 내보낸다. 안 그러면 없는 체결을 만들어낸다."""
        from mdfeed.adapters.kis_rest import KISRestAdapter
        from mdfeed.config import Config
        from mdfeed.models import Trade

        cfg = Config()
        cfg.kis_app_key, cfg.kis_app_secret = "x", "y"
        got = []
        a = KISRestAdapter(cfg, got.append)

        a._publish_quote("005930", 70000.0, 1000.0)   # 첫 관측 → 기준선만
        assert got == []
        a._publish_quote("005930", 70100.0, 1000.0)   # 변화 없음
        assert got == []
        a._publish_quote("005930", 70200.0, 1500.0)   # +500주
        assert len(got) == 1
        t = got[0]
        assert isinstance(t, Trade) and t.qty == 500.0 and t.price == 70200.0

    def test_venue_is_separate_from_tick_feed(self):
        """웹소켓의 진짜 체결(KIS)과 스냅샷 합성(KRX)이 섞이면 안 된다."""
        from mdfeed.adapters.kis_rest import KISRestAdapter
        from mdfeed.config import Config
        cfg = Config()
        cfg.kis_app_key, cfg.kis_app_secret = "x", "y"
        got = []
        a = KISRestAdapter(cfg, got.append)
        a._publish_quote("005930", 70000.0, 100.0)
        a._publish_quote("005930", 70000.0, 200.0)
        assert got[0].venue == "KRX"

    def test_latency_not_measured_for_snapshots(self):
        """폴링 주기가 곧 지연이라 네트워크 지연을 재는 의미가 없다."""
        from mdfeed.adapters.kis_rest import KISRestAdapter
        assert KISRestAdapter.measures_latency is False

    def test_disabled_without_universe(self, tmp_path):
        from mdfeed.adapters.kis_rest import KISRestAdapter
        from mdfeed.config import Config
        cfg = Config()
        cfg.kis_app_key, cfg.kis_app_secret = "x", "y"
        cfg.krx_universe_path = str(tmp_path / "없음.csv")
        a = KISRestAdapter(cfg, lambda m: None)
        assert a.enabled() is False
        assert "fetch_krx_symbols" in a.disabled_reason()

    def test_universe_loads_and_filters_market(self, tmp_path):
        from mdfeed.adapters.kis_rest import load_universe
        p = tmp_path / "u.csv"
        p.write_text("market,code,name\nKOSPI,005930,삼성전자\nKOSDAQ,900110,딥커머스\n",
                     encoding="utf-8")
        assert load_universe(str(p), {"KOSPI"}, 0) == [("005930", "삼성전자")]
        assert len(load_universe(str(p), {"KOSPI", "KOSDAQ"}, 0)) == 2
        assert len(load_universe(str(p), {"KOSPI", "KOSDAQ"}, 1)) == 1


class TestMacroAdapter:
    """금리·지수 어댑터 — 신뢰할 수 없는 응답을 다루는 규칙."""

    def test_domestic_rates_are_not_published(self):
        """`comp-interest` 의 output2 는 키·값 대응이 어긋나고 일부 종목명이
        서버 쪽에서 U+FFFD 로 이미 손상돼 온다. 짝짓기를 신뢰할 수 없다.

        그럴듯한 숫자를 내보내는 것이 안 내보내는 것보다 나쁘다.
        발행 경로에 output2 가 들어가면 이 테스트가 실패해야 한다.
        """
        import inspect
        from mdfeed.adapters import kis_macro
        src = inspect.getsource(kis_macro.KISMacroAdapter._rate_loop)
        assert "output1" in src
        # output2 를 _emit_value 로 흘리는 코드가 없어야 한다
        after = src.split("output2", 1)[1] if "output2" in src else ""
        assert "_emit_value" not in after, "국내 금리가 발행 경로에 들어갔다"

    def test_corruption_is_counted_not_hidden(self):
        from mdfeed.adapters.kis_macro import KISMacroAdapter
        from mdfeed.config import Config
        cfg = Config(); cfg.kis_app_key = cfg.kis_app_secret = "x"
        h = KISMacroAdapter(cfg, lambda m: None).health()
        assert h["domestic_rates_published"] is False
        assert "domestic_rate_rows_corrupt" in h
        assert "손상" in h["domestic_rates_reason"]

    def test_shape_based_parser_recovers_shifted_keys(self):
        """계측용 파서. 키가 밀려 있어도 값의 모양으로 이름·금리를 찾는다."""
        from mdfeed.adapters.kis_macro import parse_rate_rows
        rows = [
            {"a": "Y0101", "b": "Y0109", "c": "Y0117",
             "prdy_vrss_sign": "국고채 30년", "d": "4.5950"},
            {"a": "Y0103", "b": "CD AAA 3개월(13주)", "c": "3.1200"},
        ]
        pairs, failed = parse_rate_rows(rows)
        assert ("국고채 30년", 4.595) in pairs
        assert failed == 0

    def test_corrupted_name_counts_as_failure(self):
        """U+FFFD 로 깨진 이름은 복원 불가. 조용히 넘기지 않고 센다."""
        from mdfeed.adapters.kis_macro import parse_rate_rows
        rows = [{"a": "Y0108", "b": "����ä 20��", "c": "4.5420"}]
        pairs, failed = parse_rate_rows(rows)
        assert pairs == [] and failed == 1

    def test_rate_range_guard(self):
        """금리는 퍼센트 범위다. 100 을 넘는 값은 다른 필드를 잘못 잡은 것이다."""
        from mdfeed.adapters.kis_macro import parse_rate_rows
        pairs, failed = parse_rate_rows([{"a": "국고채 30년", "b": "218.3372"}])
        assert pairs == [] and failed == 1

    def test_only_emits_on_change(self):
        """지수·금리는 같은 값이 계속 반복된다. 바뀔 때만 내보낸다."""
        from mdfeed.adapters.kis_macro import KISMacroAdapter
        from mdfeed.config import Config
        cfg = Config(); cfg.kis_app_key = cfg.kis_app_secret = "x"
        got = []
        a = KISMacroAdapter(cfg, got.append)
        a._emit_value("KRX-IDX", "코스피", 2500.0)
        a._emit_value("KRX-IDX", "코스피", 2500.0)
        a._emit_value("KRX-IDX", "코스피", 2501.0)
        assert [m.price for m in got] == [2500.0, 2501.0]

    def test_holiday_calendar_overrides_weekday_check(self):
        """요일·시각만 보면 공휴일에 '데이터가 안 온다'를 장애로 오인한다."""
        import datetime as dt
        from mdfeed.adapters.kis_macro import KISMacroAdapter
        from mdfeed.adapters.kis import KST
        from mdfeed.config import Config
        cfg = Config(); cfg.kis_app_key = cfg.kis_app_secret = "x"
        a = KISMacroAdapter(cfg, lambda m: None)
        today = dt.datetime.now(KST).strftime("%Y%m%d")
        a.holidays = {today: False}
        assert a.is_market_open() is False
        assert a.is_stale is False      # 휴장일엔 정체로 판정하지 않는다
