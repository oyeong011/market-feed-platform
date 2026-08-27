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
