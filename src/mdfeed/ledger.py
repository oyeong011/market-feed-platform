"""주문 원장 — 내가 낸 주문과 증권사가 기록한 결과를 대조한다.

원장이 지키는 규칙
------------------
1. **먼저 적고 나서 보낸다 (write-ahead).** 주문은 PENDING_SUBMIT 으로 기록된 뒤에 전송된다.
   보내는 도중 프로세스가 죽어도 "보냈을지도 모르는 주문"이 기록에 남는다.
   기동할 때 남아 있는 PENDING_SUBMIT 은 UNKNOWN 으로 바꾼다.
2. **모르면 다시 보내지 않는다.** 전송 결과가 불확실하면(시간초과 등) UNKNOWN 으로 두고,
   증권사의 (날짜·종목·방향) 주문수량 합계가 그 주문만큼 늘었는지로 나중에 판정한다.
   늘었으면 ACCEPTED, 유예시간이 지나도 안 늘었으면 NOT_SENT.
3. **같은 client_order_id 는 한 번만.** 버튼 두 번 누르기·네트워크 재시도가 주문 두 건이 되지 않는다.
4. **체결은 증권사 합계에서만 온다.** 모의투자는 체결 상세가 비어 있고 합계만 오므로(kis_trading.py),
   합계 증가분을 열린 주문에 배분한다. 배분이 추측이 되지 않도록 **같은 종목·방향에는
   열린 주문을 하나만** 허용한다(주문 데스크가 강제). 그러면 증가분의 주인이 하나뿐이다.
5. **설명 못 하는 차이는 고치지 말고 멈춘다.** 증권사 주문수량이 원장보다 많은데 UNKNOWN 으로도
   설명이 안 되면(HTS 등 밖에서 낸 주문) 그 종목·방향의 배분을 멈추고 불일치(break)로 올린다.
   맞춰 넣으면 숫자는 맞아 보이지만 원장이 틀린 채로 굳는다.

잔고 대사는 별도다: 오늘 첫 대사 때 잡은 보유수량 기준선 + 원장 체결 순증감 = 증권사 보유수량.

6. **장이 끝나면 하루를 닫는다.** 미체결 잔량은 EXPIRED 로 닫고 그날 결과를 한 건으로 남긴다.
   결과를 모르는 주문(UNKNOWN)이나 불일치가 남아 있으면 닫지 않는다 — 틀린 하루를 확정하지 않는다.
   EXPIRED 는 증권사에 들어간 주문이라 주문수량 합계는 계속 설명하지만, 더는 체결을 받지 않는다.
   마감 뒤 체결이 늘면 흡수하지 않고 불일치로 올린다.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

PENDING_SUBMIT = "PENDING_SUBMIT"
ACCEPTED = "ACCEPTED"
PARTIAL = "PARTIAL"
FILLED = "FILLED"
REJECTED = "REJECTED"
UNKNOWN = "UNKNOWN"
NOT_SENT = "NOT_SENT"
EXPIRED = "EXPIRED"           # 장 마감까지 체결되지 않은 잔량. 증권사에 들어간 주문이다

OPEN_STATES = (PENDING_SUBMIT, ACCEPTED, PARTIAL, UNKNOWN)
LIVE_STATES = (ACCEPTED, PARTIAL, FILLED, EXPIRED)  # 증권사에 들어간 것이 확실한 주문
FILLABLE_STATES = (ACCEPTED, PARTIAL)                # 그중 아직 체결을 받을 수 있는 주문
UNKNOWN_GRACE_S = 30.0
# 체결통보로 먼저 붙인 체결을 증권사 합계가 따라잡을 때까지 기다리는 시간.
# 이 안에서는 "합계가 원장보다 작다"를 불일치로 보지 않는다(통보가 합계보다 먼저 오는 게 정상이다).
NOTICE_GRACE_S = 60.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL UNIQUE,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    qty INTEGER NOT NULL CHECK (qty > 0),
    price INTEGER NOT NULL CHECK (price >= 0),
    est_price INTEGER NOT NULL DEFAULT 0,          -- 시장가 주문의 한도 계산용 추정 단가
    state TEXT NOT NULL,
    odno TEXT,
    message TEXT,
    filled_qty INTEGER NOT NULL DEFAULT 0,
    filled_amt INTEGER NOT NULL DEFAULT 0,
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_key ON orders (trade_date, symbol, side);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    qty INTEGER NOT NULL,
    amt INTEGER NOT NULL,
    source TEXT NOT NULL,
    ts REAL NOT NULL
);
-- 데스크가 그 종목·방향에 처음 주문하기 직전의 증권사 합계. 이전 주문(다른 경로)은 여기서 빠진다.
CREATE TABLE IF NOT EXISTS totals_baseline (
    trade_date TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
    ord_qty INTEGER NOT NULL, ccld_qty INTEGER NOT NULL, ccld_amt INTEGER NOT NULL,
    PRIMARY KEY (trade_date, symbol, side)
);
CREATE TABLE IF NOT EXISTS holdings_baseline (
    trade_date TEXT NOT NULL, symbol TEXT NOT NULL, qty INTEGER NOT NULL,
    PRIMARY KEY (trade_date, symbol)
);
CREATE TABLE IF NOT EXISTS holdings_baseline_taken (trade_date TEXT PRIMARY KEY, ts REAL NOT NULL);
-- 체결통보로 받은 체결. 같은 통보가 다시 와도 한 번만 붙인다(key = 주문번호·시각·수량·가격).
CREATE TABLE IF NOT EXISTS notice_fills (
    key TEXT PRIMARY KEY, order_id INTEGER NOT NULL, qty INTEGER NOT NULL, amt INTEGER NOT NULL, ts REAL NOT NULL
);
-- 하루 마감 기록. 한 번 닫힌 날은 다시 닫지 않는다.
CREATE TABLE IF NOT EXISTS day_close (trade_date TEXT PRIMARY KEY, ts REAL NOT NULL, summary TEXT NOT NULL);
-- 감사 기록. 지우거나 고치지 않는다.
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL, kind TEXT NOT NULL, order_id INTEGER, detail TEXT
);
"""


class Ledger:
    def __init__(self, path: str, clock=time.time):
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")      # 주문 기록은 전원이 나가도 남아야 한다
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()
        self._clock = clock

    def close(self) -> None:
        self._db.close()

    # ── 기록 ──────────────────────────────────────────────────────────────
    def _event(self, kind: str, order_id: int | None = None, **detail) -> None:
        self._db.execute("INSERT INTO events (ts, kind, order_id, detail) VALUES (?,?,?,?)",
                         (self._clock(), kind, order_id, json.dumps(detail, ensure_ascii=False)))

    def new_order(self, client_order_id: str, trade_date: str, symbol: str, side: str,
                  qty: int, price: int, est_price: int = 0) -> tuple[dict, bool]:
        """(주문, 새로 만들었는가). 같은 client_order_id 면 기존 주문을 그대로 돌려준다."""
        with self._lock:
            row = self._db.execute("SELECT * FROM orders WHERE client_order_id=?",
                                   (client_order_id,)).fetchone()
            if row:
                return dict(row), False
            now = self._clock()
            cur = self._db.execute(
                "INSERT INTO orders (client_order_id, trade_date, symbol, side, qty, price, est_price,"
                " state, created_ts, updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (client_order_id, trade_date, symbol, side, qty, price, est_price or price,
                 PENDING_SUBMIT, now, now))
            self._event("order_created", cur.lastrowid, symbol=symbol, side=side, qty=qty, price=price)
            return self.order(cur.lastrowid), True

    def mark(self, order_id: int, state: str, odno: str | None = None,
             message: str | None = None) -> dict:
        with self._lock:
            self._db.execute(
                "UPDATE orders SET state=?, odno=COALESCE(?, odno), message=COALESCE(?, message),"
                " updated_ts=? WHERE id=?", (state, odno, message, self._clock(), order_id))
            self._event(f"order_{state.lower()}", order_id, odno=odno, message=message)
            return self.order(order_id)

    def recover_pending(self) -> int:
        """기동 시: 보내던 중 죽었을 수 있는 주문을 UNKNOWN 으로. 다시 보내지 않는다."""
        with self._lock:
            rows = self._db.execute("SELECT id FROM orders WHERE state=?", (PENDING_SUBMIT,)).fetchall()
            for r in rows:
                self._db.execute("UPDATE orders SET state=?, updated_ts=? WHERE id=?",
                                 (UNKNOWN, self._clock(), r["id"]))
                self._event("recovered_pending_as_unknown", r["id"])
            return len(rows)

    # ── 조회 ──────────────────────────────────────────────────────────────
    def by_client_id(self, client_order_id: str) -> dict | None:
        row = self._db.execute("SELECT * FROM orders WHERE client_order_id=?",
                               (client_order_id,)).fetchone()
        return dict(row) if row else None

    def order(self, order_id: int) -> dict:
        return dict(self._db.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone())

    def orders(self, trade_date: str | None = None, limit: int = 100) -> list[dict]:
        if trade_date:
            rows = self._db.execute("SELECT * FROM orders WHERE trade_date=? ORDER BY id DESC LIMIT ?",
                                    (trade_date, limit))
        else:
            rows = self._db.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    def open_order(self, trade_date: str, symbol: str, side: str) -> dict | None:
        q = ",".join("?" * len(OPEN_STATES))
        row = self._db.execute(
            f"SELECT * FROM orders WHERE trade_date=? AND symbol=? AND side=? AND state IN ({q})"
            " ORDER BY id LIMIT 1", (trade_date, symbol, side, *OPEN_STATES)).fetchone()
        return dict(row) if row else None

    def keys_to_watch(self, trade_date: str) -> list[tuple[str, str]]:
        """오늘 데스크가 주문한 (종목, 방향). 다 체결된 것도 본다 — 정정된 합계를 놓치지 않게."""
        rows = self._db.execute("SELECT DISTINCT symbol, side FROM orders WHERE trade_date=?"
                                " AND state NOT IN (?, ?)", (trade_date, REJECTED, NOT_SENT))
        return [(r["symbol"], r["side"]) for r in rows]

    def notional_today(self, trade_date: str) -> int:
        """오늘 데스크가 낸 주문의 금액 합. 일일 한도 계산용.

        시장가는 체결 전까지 금액을 모르므로 주문 때 잡은 추정 단가로 센다. 체결금액만 세면
        체결이 반영되기 전 몇 초 사이에 한도를 넘는 주문이 더 들어갈 수 있다.
        """
        row = self._db.execute(
            "SELECT COALESCE(SUM(MAX(qty*est_price, filled_amt)),0) AS n"
            " FROM orders WHERE trade_date=? AND state NOT IN (?, ?)",
            (trade_date, REJECTED, NOT_SENT)).fetchone()
        return int(row["n"])

    def events(self, limit: int = 50) -> list[dict]:
        rows = self._db.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
        return [{**dict(r), "detail": json.loads(r["detail"] or "{}")} for r in rows]

    # ── 기준선 ────────────────────────────────────────────────────────────
    def totals_baseline(self, trade_date: str, symbol: str, side: str) -> dict | None:
        row = self._db.execute("SELECT ord_qty, ccld_qty, ccld_amt FROM totals_baseline"
                               " WHERE trade_date=? AND symbol=? AND side=?",
                               (trade_date, symbol, side)).fetchone()
        return dict(row) if row else None

    def set_totals_baseline(self, trade_date: str, symbol: str, side: str, totals: dict) -> None:
        with self._lock:
            self._db.execute("INSERT OR IGNORE INTO totals_baseline VALUES (?,?,?,?,?,?)",
                             (trade_date, symbol, side, totals["ord_qty"], totals["ccld_qty"],
                              totals["ccld_amt"]))

    def has_holdings_baseline(self, trade_date: str) -> bool:
        return self._db.execute("SELECT 1 FROM holdings_baseline_taken WHERE trade_date=?",
                                (trade_date,)).fetchone() is not None

    def set_holdings_baseline(self, trade_date: str, holdings: dict[str, int]) -> None:
        with self._lock:
            if self.has_holdings_baseline(trade_date):
                return
            for sym, qty in holdings.items():
                self._db.execute("INSERT INTO holdings_baseline VALUES (?,?,?)", (trade_date, sym, qty))
            self._db.execute("INSERT INTO holdings_baseline_taken VALUES (?,?)",
                             (trade_date, self._clock()))
            self._event("holdings_baseline", None, trade_date=trade_date, holdings=holdings)

    # ── 대사 ──────────────────────────────────────────────────────────────
    def apply_totals(self, trade_date: str, symbol: str, side: str, broker: dict) -> list[dict]:
        """증권사 합계를 원장에 반영한다. 설명 못 하는 차이는 break 로 돌려준다."""
        with self._lock:
            return self._apply_totals(trade_date, symbol, side, broker)

    def _apply_totals(self, trade_date, symbol, side, broker) -> list[dict]:
        key = {"symbol": symbol, "side": side}
        base = self.totals_baseline(trade_date, symbol, side)
        if base is None:
            return [{**key, "kind": "no_baseline"}]
        b_ord = broker["ord_qty"] - base["ord_qty"]
        b_fill = broker["ccld_qty"] - base["ccld_qty"]
        b_amt = broker["ccld_amt"] - base["ccld_amt"]
        rows = [dict(r) for r in self._db.execute(
            "SELECT * FROM orders WHERE trade_date=? AND symbol=? AND side=? ORDER BY id",
            (trade_date, symbol, side))]
        live = [o for o in rows if o["state"] in LIVE_STATES]
        unknown = [o for o in rows if o["state"] == UNKNOWN]
        now = self._clock()
        breaks: list[dict] = []

        extra = b_ord - sum(o["qty"] for o in live)
        if extra > 0:
            if unknown and sum(o["qty"] for o in unknown) == extra:
                for o in unknown:
                    self._set_state(o["id"], ACCEPTED, now)
                    self._event("unknown_resolved_sent", o["id"], broker_ord_qty=broker["ord_qty"])
                    o["state"] = ACCEPTED
                live = sorted(live + unknown, key=lambda o: o["id"])
                unknown = []
            else:
                # 원장이 모르는 주문이 증권사에 있다. 배분을 멈춘다(규칙 5).
                return [{**key, "kind": "unexplained_broker_orders", "extra_qty": extra}]
        elif extra < 0:
            breaks.append({**key, "kind": "ledger_orders_missing_at_broker", "missing_qty": -extra})
        if extra <= 0:
            for o in unknown:
                if now - o["created_ts"] > UNKNOWN_GRACE_S:
                    self._set_state(o["id"], NOT_SENT, now)
                    self._event("unknown_resolved_not_sent", o["id"], broker_ord_qty=broker["ord_qty"])

        delta = b_fill - sum(o["filled_qty"] for o in live)
        amt_delta = b_amt - sum(o["filled_amt"] for o in live)
        if delta < 0:
            ids = [o["id"] for o in live]
            recent = self._db.execute(
                f"SELECT COALESCE(SUM(qty),0) FROM notice_fills WHERE ts > ? AND order_id IN ({','.join('?' * len(ids))})",
                (now - NOTICE_GRACE_S, *ids)).fetchone()[0] if ids else 0
            if -delta <= recent:
                return breaks            # 통보가 먼저 왔고 합계가 아직 못 따라왔다. 기다린다
            return breaks + [{**key, "kind": "broker_fills_below_ledger", "delta": delta}]
        if delta == 0:
            return breaks
        open_live = [o for o in live if o["state"] in FILLABLE_STATES and o["filled_qty"] < o["qty"]]
        room = sum(o["qty"] - o["filled_qty"] for o in open_live)
        if delta > room:
            return breaks + [{**key, "kind": "fills_exceed_orders", "delta": delta, "room": room}]
        left, amt_left = delta, amt_delta
        for o in open_live:
            if left == 0:
                break
            take = min(o["qty"] - o["filled_qty"], left)
            amt = amt_left if take == left else round(amt_delta * take / delta)
            left -= take
            amt_left -= amt
            filled = o["filled_qty"] + take
            self._db.execute("INSERT INTO fills (order_id, qty, amt, source, ts) VALUES (?,?,?,?,?)",
                             (o["id"], take, amt, "day_totals", now))
            self._db.execute("UPDATE orders SET filled_qty=?, filled_amt=filled_amt+?, state=?,"
                             " updated_ts=? WHERE id=?",
                             (filled, amt, FILLED if filled == o["qty"] else PARTIAL, now, o["id"]))
            self._event("fill", o["id"], qty=take, amt=amt)
        return breaks

    def apply_notice(self, trade_date: str, odno: str, qty: int, price: int, key: str) -> str:
        """체결통보 한 건을 그 주문에 붙인다. 결과: applied · duplicate · unmatched · ignored.

        unmatched 는 원장에 없는 주문번호다 — 데스크 밖에서 낸 주문의 체결이 합계보다 먼저 보인 것이다.
        ignored 는 붙이면 원장이 틀릴 수 있는 경우(닫힌 주문, 남은 수량 초과)다. 합계 대사가 판정한다.
        """
        with self._lock:
            if self._db.execute("SELECT 1 FROM notice_fills WHERE key=?", (key,)).fetchone():
                return "duplicate"
            want = odno.lstrip("0")
            o = next((dict(r) for r in self._db.execute(
                "SELECT * FROM orders WHERE trade_date=? AND odno IS NOT NULL", (trade_date,))
                if (r["odno"] or "").lstrip("0") == want), None)
            if o is None:
                self._event("notice_unmatched", None, odno=odno, qty=qty, price=price)
                return "unmatched"
            if o["state"] not in FILLABLE_STATES or qty <= 0 or qty > o["qty"] - o["filled_qty"]:
                self._event("notice_ignored", o["id"], qty=qty, price=price, state=o["state"])
                return "ignored"
            now = self._clock()
            filled, amt = o["filled_qty"] + qty, qty * price
            self._db.execute("INSERT INTO notice_fills VALUES (?,?,?,?,?)", (key, o["id"], qty, amt, now))
            self._db.execute("INSERT INTO fills (order_id, qty, amt, source, ts) VALUES (?,?,?,?,?)",
                             (o["id"], qty, amt, "notice", now))
            self._db.execute("UPDATE orders SET filled_qty=?, filled_amt=filled_amt+?, state=?, updated_ts=?"
                             " WHERE id=?", (filled, amt, FILLED if filled == o["qty"] else PARTIAL, now, o["id"]))
            self._event("fill", o["id"], qty=qty, amt=amt, source="notice")
            return "applied"

    def _set_state(self, order_id: int, state: str, now: float) -> None:
        self._db.execute("UPDATE orders SET state=?, updated_ts=? WHERE id=?", (state, now, order_id))

    def expected_holdings(self, trade_date: str) -> dict[str, int]:
        pos = {r["symbol"]: r["qty"] for r in self._db.execute(
            "SELECT symbol, qty FROM holdings_baseline WHERE trade_date=?", (trade_date,))}
        for r in self._db.execute(
                "SELECT symbol, side, SUM(filled_qty) AS q FROM orders WHERE trade_date=?"
                " GROUP BY symbol, side", (trade_date,)):
            sign = 1 if r["side"] == "buy" else -1
            pos[r["symbol"]] = pos.get(r["symbol"], 0) + sign * int(r["q"] or 0)
        return {s: q for s, q in pos.items() if q != 0}

    def reconcile_holdings(self, trade_date: str, broker: dict[str, int]) -> list[dict]:
        exp = self.expected_holdings(trade_date)
        # 통보로 방금 붙인 체결은 증권사 잔고에 아직 안 보일 수 있다. 그 방향·크기 안의 차이는 기다린다.
        recent: dict[str, int] = {}
        for r in self._db.execute(
                "SELECT o.symbol, o.side, SUM(n.qty) AS q FROM notice_fills n JOIN orders o ON o.id = n.order_id"
                " WHERE o.trade_date=? AND n.ts > ? GROUP BY o.symbol, o.side",
                (trade_date, self._clock() - NOTICE_GRACE_S)):
            recent[r["symbol"]] = recent.get(r["symbol"], 0) + (r["q"] if r["side"] == "buy" else -r["q"])
        out = []
        for s in sorted(set(exp) | set(broker)):
            diff = exp.get(s, 0) - broker.get(s, 0)
            pending = recent.get(s, 0)
            if diff == 0 or (diff * pending > 0 and abs(diff) <= abs(pending)):
                continue
            out.append({"symbol": s, "kind": "position_mismatch",
                        "ledger_qty": exp.get(s, 0), "broker_qty": broker.get(s, 0)})
        return out

    # ── 마감 ──────────────────────────────────────────────────────────────
    def day_close(self, trade_date: str) -> dict | None:
        row = self._db.execute("SELECT ts, summary FROM day_close WHERE trade_date=?",
                               (trade_date,)).fetchone()
        return {"trade_date": trade_date, "ts": row["ts"], **json.loads(row["summary"])} if row else None

    def close_day(self, trade_date: str, open_breaks: int) -> dict:
        """미체결 잔량을 EXPIRED 로 닫고 하루 결과를 남긴다. 닫을 수 없으면 이유를 돌려준다."""
        with self._lock:
            done = self.day_close(trade_date)
            if done:
                return {"closed": True, **done}
            rows = self.orders(trade_date, limit=100_000)
            unresolved = [o["id"] for o in rows if o["state"] in (PENDING_SUBMIT, UNKNOWN)]
            if unresolved:
                return {"closed": False, "reason": "unknown_orders", "order_ids": unresolved}
            if open_breaks:
                return {"closed": False, "reason": "breaks", "breaks": open_breaks}
            now = self._clock()
            expired = [o for o in rows if o["state"] in FILLABLE_STATES]
            for o in expired:
                self._set_state(o["id"], EXPIRED, now)
                self._event("order_expired", o["id"], unfilled_qty=o["qty"] - o["filled_qty"])
            sent = [o for o in rows if o["state"] in LIVE_STATES]
            summary = {
                "orders_sent": len(sent),
                "orders_rejected": sum(o["state"] == REJECTED for o in rows),
                "orders_not_sent": sum(o["state"] == NOT_SENT for o in rows),
                "filled_qty": sum(o["filled_qty"] for o in sent),
                "filled_amt": sum(o["filled_amt"] for o in sent),
                "expired_orders": len(expired),
                "expired_qty": sum(o["qty"] - o["filled_qty"] for o in expired),
            }
            self._db.execute("INSERT INTO day_close VALUES (?,?,?)",
                             (trade_date, now, json.dumps(summary, ensure_ascii=False)))
            self._event("day_closed", None, trade_date=trade_date, **summary)
            return {"closed": True, "trade_date": trade_date, "ts": now, **summary}
