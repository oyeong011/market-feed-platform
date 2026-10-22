"""주문 원장 규칙 — 증권사 합계와 대조해 체결을 배분하고, 설명 못 하는 차이는 멈춘다."""
from mdfeed import ledger as L

DAY = "20261012"


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _ledger():
    clock = Clock()
    return L.Ledger(":memory:", clock=clock), clock


def _totals(o=0, f=0, a=0):
    return {"ord_qty": o, "ccld_qty": f, "ccld_amt": a}


def _accepted(led, cid, qty, side="buy", sym="005930"):
    o, _ = led.new_order(cid, DAY, sym, side, qty, 0, est_price=100)
    return led.mark(o["id"], L.ACCEPTED, odno="1")


def test_same_client_order_id_is_one_order():
    led, _ = _ledger()
    a, made_a = led.new_order("cid-00000001", DAY, "005930", "buy", 1, 0)
    b, made_b = led.new_order("cid-00000001", DAY, "005930", "buy", 1, 0)
    assert made_a and not made_b and a["id"] == b["id"]


def test_pending_at_startup_becomes_unknown_not_resent():
    led, _ = _ledger()
    led.new_order("cid-00000001", DAY, "005930", "buy", 1, 0)
    assert led.recover_pending() == 1
    assert led.orders()[0]["state"] == L.UNKNOWN


def test_baseline_excludes_earlier_activity_and_fills_are_applied():
    led, _ = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals(3, 3, 750))   # 데스크 이전 주문 3주
    o = _accepted(led, "cid-00000001", 2)
    assert led.apply_totals(DAY, "005930", "buy", _totals(5, 4, 1250)) == []
    assert led.order(o["id"])["state"] == L.PARTIAL
    assert led.apply_totals(DAY, "005930", "buy", _totals(5, 5, 1750)) == []
    got = led.order(o["id"])
    assert (got["state"], got["filled_qty"], got["filled_amt"]) == (L.FILLED, 2, 1000)


def test_same_totals_twice_does_not_double_fill():
    led, _ = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o = _accepted(led, "cid-00000001", 1)
    led.apply_totals(DAY, "005930", "buy", _totals(1, 1, 500))
    led.apply_totals(DAY, "005930", "buy", _totals(1, 1, 500))
    assert led.order(o["id"])["filled_qty"] == 1


def test_unknown_resolved_as_sent_when_broker_count_explains_it():
    led, _ = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o, _ = led.new_order("cid-00000001", DAY, "005930", "buy", 2, 0)
    led.mark(o["id"], L.UNKNOWN)
    assert led.apply_totals(DAY, "005930", "buy", _totals(2, 2, 1000)) == []
    assert led.order(o["id"])["state"] == L.FILLED


def test_unknown_becomes_not_sent_only_after_grace():
    led, clock = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o, _ = led.new_order("cid-00000001", DAY, "005930", "buy", 1, 0)
    led.mark(o["id"], L.UNKNOWN)
    led.apply_totals(DAY, "005930", "buy", _totals())
    assert led.order(o["id"])["state"] == L.UNKNOWN          # 아직 늦게 도착할 수 있다
    clock.t += L.UNKNOWN_GRACE_S + 1
    led.apply_totals(DAY, "005930", "buy", _totals())
    assert led.order(o["id"])["state"] == L.NOT_SENT


def test_unexplained_broker_order_stops_attribution():
    """밖에서 낸 주문의 체결을 데스크 주문에 붙이면 숫자는 맞아 보이고 원장은 틀린다."""
    led, _ = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o = _accepted(led, "cid-00000001", 1)
    breaks = led.apply_totals(DAY, "005930", "buy", _totals(2, 2, 1000))
    assert [b["kind"] for b in breaks] == ["unexplained_broker_orders"]
    assert led.order(o["id"])["filled_qty"] == 0


def test_fills_beyond_order_room_is_a_break():
    led, _ = _ledger()
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    _accepted(led, "cid-00000001", 1)
    led.apply_totals(DAY, "005930", "buy", _totals(1, 1, 500))
    breaks = led.apply_totals(DAY, "005930", "buy", _totals(1, 0, 0))
    assert breaks[0]["kind"] == "broker_fills_below_ledger"


def test_missing_baseline_is_reported_not_guessed():
    led, _ = _ledger()
    _accepted(led, "cid-00000001", 1)
    assert led.apply_totals(DAY, "005930", "buy", _totals(1, 1, 1))[0]["kind"] == "no_baseline"


def test_holdings_reconcile_uses_baseline_plus_net_fills():
    led, _ = _ledger()
    led.set_holdings_baseline(DAY, {"005930": 2})
    led.set_totals_baseline(DAY, "005930", "sell", _totals())
    _accepted(led, "cid-00000001", 1, side="sell")
    led.apply_totals(DAY, "005930", "sell", _totals(1, 1, 500))
    assert led.reconcile_holdings(DAY, {"005930": 1}) == []
    assert led.reconcile_holdings(DAY, {"005930": 2})[0] == {
        "symbol": "005930", "kind": "position_mismatch", "ledger_qty": 1, "broker_qty": 2}


def test_daily_notional_counts_unfilled_market_orders_at_estimate():
    led, _ = _ledger()
    _accepted(led, "cid-00000001", 3)                   # 시장가 3주, 추정 단가 100
    assert led.notional_today(DAY) == 300


def _limit_order_half_filled(led):
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o = _accepted(led, "cid-00000001", 3)
    led.apply_totals(DAY, "005930", "buy", _totals(3, 1, 100))
    return o


def test_close_expires_unfilled_remainder_once():
    led, _ = _ledger()
    o = _limit_order_half_filled(led)
    r = led.close_day(DAY, open_breaks=0)
    assert r["closed"] and (r["expired_orders"], r["expired_qty"], r["filled_qty"]) == (1, 2, 1)
    assert led.order(o["id"])["state"] == L.EXPIRED
    again = led.close_day(DAY, open_breaks=0)
    assert again["closed"] and again["ts"] == r["ts"]
    assert sum(e["kind"] == "day_closed" for e in led.events()) == 1


def test_expired_order_still_explains_broker_orders_but_takes_no_fills():
    """마감 뒤 체결이 늘면 닫힌 주문에 붙이지 않는다."""
    led, _ = _ledger()
    _limit_order_half_filled(led)
    led.close_day(DAY, open_breaks=0)
    assert led.apply_totals(DAY, "005930", "buy", _totals(3, 1, 100)) == []
    breaks = led.apply_totals(DAY, "005930", "buy", _totals(3, 2, 200))
    assert [b["kind"] for b in breaks] == ["fills_exceed_orders"]


def test_unknown_order_blocks_close():
    led, _ = _ledger()
    o, _ = led.new_order("cid-00000001", DAY, "005930", "buy", 1, 0)
    led.mark(o["id"], L.UNKNOWN)
    r = led.close_day(DAY, open_breaks=0)
    assert (r["closed"], r["reason"], r["order_ids"]) == (False, "unknown_orders", [o["id"]])
    assert led.day_close(DAY) is None


def test_open_break_blocks_close():
    led, _ = _ledger()
    o = _limit_order_half_filled(led)
    assert led.close_day(DAY, open_breaks=1) == {"closed": False, "reason": "breaks", "breaks": 1}
    assert led.order(o["id"])["state"] == L.PARTIAL


# ── 체결통보 ─────────────────────────────────────────────────────────────
def _with_odno(led, qty=3, odno="0000012345"):
    led.set_totals_baseline(DAY, "005930", "buy", _totals())
    o, _ = led.new_order("cid-00000001", DAY, "005930", "buy", qty, 0, est_price=100)
    return led.mark(o["id"], L.ACCEPTED, odno=odno)


def test_notice_fills_order_before_totals_and_totals_do_not_double_count():
    led, _ = _ledger()
    o = _with_odno(led)
    assert led.apply_notice(DAY, "12345", 2, 100, "k1") == "applied"      # 앞자리 0 이 달라도 같은 주문
    assert led.order(o["id"])["filled_qty"] == 2
    assert led.apply_totals(DAY, "005930", "buy", _totals(3, 2, 200)) == []
    assert led.order(o["id"])["filled_qty"] == 2
    assert led.apply_totals(DAY, "005930", "buy", _totals(3, 3, 300)) == []  # 통보를 놓친 1주는 합계가 채운다
    assert led.order(o["id"])["state"] == L.FILLED


def test_same_notice_twice_is_one_fill():
    led, _ = _ledger()
    o = _with_odno(led)
    led.apply_notice(DAY, "0000012345", 1, 100, "k1")
    assert led.apply_notice(DAY, "0000012345", 1, 100, "k1") == "duplicate"
    assert led.order(o["id"])["filled_qty"] == 1


def test_notice_for_unknown_order_is_reported_not_attached():
    led, _ = _ledger()
    o = _with_odno(led)
    assert led.apply_notice(DAY, "99999", 1, 100, "k1") == "unmatched"
    assert led.order(o["id"])["filled_qty"] == 0
    assert led.events(1)[0]["kind"] == "notice_unmatched"


def test_notice_beyond_remaining_is_ignored():
    led, _ = _ledger()
    o = _with_odno(led, qty=1)
    assert led.apply_notice(DAY, "12345", 2, 100, "k1") == "ignored"
    assert led.order(o["id"])["filled_qty"] == 0


def test_totals_lagging_behind_notice_wait_then_break():
    """통보가 합계보다 먼저 오는 건 정상이다. 그런데 유예가 지나도 합계가 안 따라오면 통보가 틀린 것이다."""
    led, clock = _ledger()
    _with_odno(led)
    led.apply_notice(DAY, "12345", 2, 100, "k1")
    assert led.apply_totals(DAY, "005930", "buy", _totals(3, 0, 0)) == []
    clock.t += L.NOTICE_GRACE_S + 1
    breaks = led.apply_totals(DAY, "005930", "buy", _totals(3, 0, 0))
    assert [b["kind"] for b in breaks] == ["broker_fills_below_ledger"]


def test_holdings_wait_for_broker_after_notice_then_break():
    led, clock = _ledger()
    led.set_holdings_baseline(DAY, {"005930": 2})
    _with_odno(led)
    led.apply_notice(DAY, "12345", 2, 100, "k1")
    assert led.reconcile_holdings(DAY, {"005930": 2}) == []          # 잔고가 아직 안 따라옴: 기다린다
    assert led.reconcile_holdings(DAY, {"005930": 4}) == []
    assert led.reconcile_holdings(DAY, {"005930": 1})                 # 반대 방향 차이는 통보로 설명 안 됨
    clock.t += L.NOTICE_GRACE_S + 1
    assert led.reconcile_holdings(DAY, {"005930": 2})[0]["kind"] == "position_mismatch"
