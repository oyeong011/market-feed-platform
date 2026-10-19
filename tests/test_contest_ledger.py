"""대회 원장 규칙 — 거절은 돈을 안 움직이고, 잔고표는 거래 기록과 늘 같아야 한다."""
import pytest

from mdfeed import contest as C


def _ledger(**kw):
    led = C.ContestLedger(":memory:", initial_cash=1_000_000, fee_ppm=150, tax_ppm=2000, **kw)
    code, = led.create_invites(1)
    p = led.signup(code, "alice", "h")
    return led, p["id"]


def test_invite_is_single_use():
    led, _ = _ledger()
    code = led.invites()[0]["code"]
    with pytest.raises(C.ContestError):
        led.signup(code, "bob", "h")


def test_signup_deposits_initial_cash():
    led, pid = _ledger()
    assert led.account(pid) == {"cash": 1_000_000, "positions": []}
    assert led.integrity() == []


def test_buy_then_sell_moves_cash_with_fee_and_tax():
    led, pid = _ledger()
    o, made = led.place(pid, "cid-0001", "005930", "buy", 10, 70_000)
    assert made and o["state"] == C.FILLED and (o["amount"], o["fee"], o["tax"]) == (700_000, 105, 0)
    assert led.account(pid)["cash"] == 1_000_000 - 700_105
    o, _ = led.place(pid, "cid-0002", "005930", "sell", 4, 75_000)
    assert (o["amount"], o["fee"], o["tax"]) == (300_000, 45, 600)
    acct = led.account(pid)
    assert acct["cash"] == 1_000_000 - 700_105 + 300_000 - 45 - 600
    assert acct["positions"] == [{"symbol": "005930", "qty": 6, "cost": 700_105 - 700_105 * 4 // 10}]
    assert led.integrity() == []


def test_same_client_order_id_is_one_fill():
    led, pid = _ledger()
    a, made_a = led.place(pid, "cid-0001", "005930", "buy", 1, 70_000)
    b, made_b = led.place(pid, "cid-0001", "005930", "buy", 1, 70_000)
    assert made_a and not made_b and a["id"] == b["id"]
    assert led.account(pid)["positions"][0]["qty"] == 1


@pytest.mark.parametrize("side,qty,price", [("buy", 20, 70_000), ("sell", 1, 70_000), ("buy", 1, 0)])
def test_rejected_order_moves_nothing(side, qty, price):
    led, pid = _ledger()
    o, _ = led.place(pid, "cid-0001", "005930", side, qty, price)
    assert o["state"] == C.REJECTED and o["reason"]
    assert led.account(pid) == {"cash": 1_000_000, "positions": []}
    assert led.integrity() == []


def test_leaderboard_values_holdings_at_current_price():
    led, pid = _ledger()
    code, = led.create_invites(1)
    led.signup(code, "bob", "h")
    led.place(pid, "cid-0001", "005930", "buy", 10, 70_000)
    board = led.leaderboard({"005930": 80_000})
    assert [r["nickname"] for r in board] == ["alice", "bob"]
    assert board[0]["equity"] == 1_000_000 - 700_105 + 800_000
    assert board[0]["rank"] == 1 and board[0]["unpriced"] == []


def test_leaderboard_marks_unpriced_holdings():
    led, pid = _ledger()
    led.place(pid, "cid-0001", "005930", "buy", 10, 70_000)
    assert led.leaderboard({})[0]["unpriced"] == ["005930"]


# ── 대사: 장부를 몰래 고치면 잡아야 한다 ─────────────────────────────────
def _traded():
    led, pid = _ledger()
    led.place(pid, "cid-0001", "005930", "buy", 10, 70_000)
    led.place(pid, "cid-0002", "005930", "sell", 3, 72_000)
    assert led.integrity() == []
    return led, pid


def test_cash_edited_outside_journal_is_caught():
    led, pid = _traded()
    led._db.execute("UPDATE accounts SET cash = cash + 1000000 WHERE participant_id=?", (pid,))
    assert [b["kind"] for b in led.integrity()] == ["cash_mismatch"]


def test_position_edited_outside_journal_is_caught():
    led, pid = _traded()
    led._db.execute("UPDATE positions SET qty = qty + 5 WHERE participant_id=?", (pid,))
    assert [b["kind"] for b in led.integrity()] == ["position_mismatch"]


def test_journal_line_edited_is_caught():
    led, _ = _traded()
    led._db.execute("UPDATE journal SET detail = replace(detail, '\"qty\": 3', '\"qty\": 1') WHERE kind='fill'")
    assert led.integrity()[0]["kind"] == "journal_chain_broken"


def test_journal_line_deleted_is_caught():
    led, _ = _traded()
    led._db.execute("DELETE FROM journal WHERE id = (SELECT MIN(id) FROM journal WHERE kind='fill')")
    assert led.integrity()[0]["kind"] == "journal_chain_broken"


def test_order_price_edited_is_caught():
    led, _ = _traded()
    led._db.execute("UPDATE orders SET price = 1 WHERE client_order_id='cid-0001'")
    assert [b["kind"] for b in led.integrity()] == ["order_journal_mismatch"]


def test_halt_and_resume():
    led, _ = _ledger()
    assert led.halted() is None
    led.halt("cash_mismatch")
    assert led.halted() == "cash_mismatch"
    led.resume()
    assert led.halted() is None
