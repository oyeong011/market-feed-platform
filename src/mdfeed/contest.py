"""모의투자대회 원장 — 참가자 가상 계좌, 시장가 체결, 스스로 대사하는 장부.

증권사로 주문을 보내지 않는다. 체결가는 증권사 현재가이고, 돈과 주식은 이 장부 안에서만 움직인다.

장부가 지키는 규칙
------------------
1. **돈이 움직이면 거래 기록(journal)에 먼저 남는다.** 입금·체결은 잔고표(accounts·positions)를
   바꾸는 같은 트랜잭션에서 기록된다. 둘 중 하나만 남는 경우가 없다.
2. **거래 기록은 지우거나 고치지 않는다.** 각 줄은 앞 줄의 해시를 품는다(해시 체인).
   한 줄이라도 고치거나 빼면 그 뒤 체인이 맞지 않는다.
3. **잔고표는 거래 기록으로 다시 계산한 값과 같아야 한다.** integrity() 가 매 주기 다시 계산해
   대조한다. 어긋나면 대회 운영(service)이 주문을 멈춘다 — 틀린 잔고 위에서 거래를 계속하지 않는다.
4. **같은 client_order_id 는 한 번만.** 버튼 두 번 누르기·재시도가 주문 두 건이 되지 않는다.
5. **없는 돈으로 사지 않고, 없는 주식을 팔지 않는다.** 거절된 주문은 돈을 한 푼도 움직이지 않는다.

금액은 모두 원 단위 정수다. 수수료·세금은 원 미만을 버린다.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time

FILLED = "FILLED"
REJECTED = "REJECTED"
GENESIS = "0" * 64

SCHEMA = """
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nickname TEXT NOT NULL UNIQUE,
    pw_hash TEXT NOT NULL,
    created_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS invites (
    code TEXT PRIMARY KEY,
    created_ts REAL NOT NULL,
    used_by INTEGER REFERENCES participants(id),
    used_ts REAL
);
CREATE TABLE IF NOT EXISTS accounts (
    participant_id INTEGER PRIMARY KEY REFERENCES participants(id),
    cash INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    participant_id INTEGER NOT NULL REFERENCES participants(id),
    symbol TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty >= 0),
    cost INTEGER NOT NULL,                       -- 남은 수량의 매수 원가(평균단가 계산용)
    PRIMARY KEY (participant_id, symbol)
);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    participant_id INTEGER NOT NULL REFERENCES participants(id),
    client_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    qty INTEGER NOT NULL CHECK (qty > 0),
    price INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    fee INTEGER NOT NULL,
    tax INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    ts REAL NOT NULL,
    UNIQUE (participant_id, client_order_id)
);
CREATE TABLE IF NOT EXISTS journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,                          -- deposit · fill
    participant_id INTEGER NOT NULL,
    detail TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class ContestError(Exception):
    """참가·주문 요청이 규칙에 맞지 않는다(사용자에게 그대로 보여 줄 문장)."""


def _chain(prev: str, ts: float, kind: str, pid: int, detail: str) -> str:
    return hashlib.sha256(f"{prev}|{ts!r}|{kind}|{pid}|{detail}".encode()).hexdigest()


class ContestLedger:
    def __init__(self, path: str, initial_cash: int = 10_000_000, fee_ppm: int = 150,
                 tax_ppm: int = 2000, clock=time.time):
        """fee_ppm·tax_ppm 은 백만분율. 기본 수수료 0.015%, 매도 세금 0.20% (대회 설정값)."""
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()
        self._clock = clock
        self.initial_cash, self.fee_ppm, self.tax_ppm = initial_cash, fee_ppm, tax_ppm

    def close(self) -> None:
        self._db.close()

    def _tx(self):
        """한 번에 한 트랜잭션. 잔고표와 거래 기록이 함께 바뀌거나 함께 안 바뀐다."""
        lock, db = self._lock, self._db

        class _T:
            def __enter__(self):
                lock.acquire()
                db.execute("BEGIN IMMEDIATE")
                return db

            def __exit__(self, exc_type, *_):
                try:
                    db.execute("ROLLBACK" if exc_type else "COMMIT")
                finally:
                    lock.release()
        return _T()

    def _journal(self, db, kind: str, pid: int, **detail) -> None:
        row = db.execute("SELECT hash FROM journal ORDER BY id DESC LIMIT 1").fetchone()
        prev = row["hash"] if row else GENESIS
        ts = self._clock()
        d = json.dumps(detail, sort_keys=True, ensure_ascii=False)
        db.execute("INSERT INTO journal (ts, kind, participant_id, detail, prev_hash, hash)"
                   " VALUES (?,?,?,?,?,?)", (ts, kind, pid, d, prev, _chain(prev, ts, kind, pid, d)))

    # ── 참가 ──────────────────────────────────────────────────────────────
    def create_invites(self, n: int) -> list[str]:
        codes = [secrets.token_urlsafe(9) for _ in range(n)]
        with self._tx() as db:
            for c in codes:
                db.execute("INSERT INTO invites (code, created_ts) VALUES (?,?)", (c, self._clock()))
        return codes

    def invites(self) -> list[dict]:
        return [dict(r) for r in self._db.execute(
            "SELECT i.code, i.created_ts, i.used_ts, p.nickname FROM invites i"
            " LEFT JOIN participants p ON p.id = i.used_by ORDER BY i.created_ts")]

    def signup(self, code: str, nickname: str, pw_hash: str) -> dict:
        """초대 코드 하나로 한 명. 가입과 동시에 시작 자금이 입금된다."""
        with self._tx() as db:
            inv = db.execute("SELECT used_by FROM invites WHERE code=?", (code,)).fetchone()
            if inv is None or inv["used_by"] is not None:
                raise ContestError("초대 코드가 없거나 이미 쓰였습니다")
            if db.execute("SELECT 1 FROM participants WHERE nickname=?", (nickname,)).fetchone():
                raise ContestError("이미 있는 닉네임입니다")
            now = self._clock()
            pid = db.execute("INSERT INTO participants (nickname, pw_hash, created_ts) VALUES (?,?,?)",
                             (nickname, pw_hash, now)).lastrowid
            db.execute("UPDATE invites SET used_by=?, used_ts=? WHERE code=?", (pid, now, code))
            db.execute("INSERT INTO accounts VALUES (?,?)", (pid, self.initial_cash))
            self._journal(db, "deposit", pid, amount=self.initial_cash)
        return self.participant(pid)

    def participant(self, pid: int) -> dict:
        return dict(self._db.execute("SELECT id, nickname, created_ts FROM participants WHERE id=?",
                                     (pid,)).fetchone())

    def credentials(self, nickname: str) -> tuple[int, str] | None:
        row = self._db.execute("SELECT id, pw_hash FROM participants WHERE nickname=?",
                               (nickname,)).fetchone()
        return (row["id"], row["pw_hash"]) if row else None

    def participant_count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM participants").fetchone()[0]

    # ── 주문 ──────────────────────────────────────────────────────────────
    def costs(self, side: str, qty: int, price: int) -> tuple[int, int, int]:
        amount = qty * price
        fee = amount * self.fee_ppm // 1_000_000
        tax = amount * self.tax_ppm // 1_000_000 if side == "sell" else 0
        return amount, fee, tax

    def place(self, pid: int, client_order_id: str, symbol: str, side: str, qty: int,
              price: int) -> tuple[dict, bool]:
        """시장가 주문을 price 에 체결한다. (주문, 새로 만들었는가).

        규칙에 안 맞으면 REJECTED 로 기록만 하고 돈은 움직이지 않는다.
        """
        with self._tx() as db:
            row = db.execute("SELECT * FROM orders WHERE participant_id=? AND client_order_id=?",
                             (pid, client_order_id)).fetchone()
            if row:
                return dict(row), False
            amount, fee, tax = self.costs(side, qty, price)
            cash = db.execute("SELECT cash FROM accounts WHERE participant_id=?", (pid,)).fetchone()["cash"]
            pos = db.execute("SELECT qty, cost FROM positions WHERE participant_id=? AND symbol=?",
                             (pid, symbol)).fetchone()
            held, cost = (pos["qty"], pos["cost"]) if pos else (0, 0)
            reason = None
            if price <= 0:
                reason = "현재가를 알 수 없습니다"
            elif side == "buy" and amount + fee > cash:
                reason = f"예수금 부족 (필요 {amount + fee:,}원, 보유 {cash:,}원)"
            elif side == "sell" and qty > held:
                reason = f"보유 수량 부족 (보유 {held}주)"
            state = REJECTED if reason else FILLED
            oid = db.execute(
                "INSERT INTO orders (participant_id, client_order_id, symbol, side, qty, price, amount,"
                " fee, tax, state, reason, ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (pid, client_order_id, symbol, side, qty, price, amount, fee, tax, state, reason,
                 self._clock())).lastrowid
            if state == FILLED:
                if side == "buy":
                    cash_delta, new_qty, new_cost = -(amount + fee), held + qty, cost + amount + fee
                else:
                    # 판 만큼 원가를 비례로 덜어 낸다. 남은 수량의 평균단가는 그대로다.
                    cash_delta, new_qty = amount - fee - tax, held - qty
                    new_cost = cost - cost * qty // held
                db.execute("UPDATE accounts SET cash=cash+? WHERE participant_id=?", (cash_delta, pid))
                db.execute("INSERT INTO positions VALUES (?,?,?,?) ON CONFLICT (participant_id, symbol)"
                           " DO UPDATE SET qty=excluded.qty, cost=excluded.cost",
                           (pid, symbol, new_qty, new_cost))
                self._journal(db, "fill", pid, order_id=oid, symbol=symbol, side=side, qty=qty,
                              price=price, amount=amount, fee=fee, tax=tax, cash_delta=cash_delta)
        return self.order(oid), True

    def order(self, oid: int) -> dict:
        return dict(self._db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone())

    def orders(self, pid: int | None = None, limit: int = 50) -> list[dict]:
        if pid is None:
            rows = self._db.execute("SELECT o.*, p.nickname FROM orders o JOIN participants p"
                                    " ON p.id = o.participant_id ORDER BY o.id DESC LIMIT ?", (limit,))
        else:
            rows = self._db.execute("SELECT * FROM orders WHERE participant_id=? ORDER BY id DESC LIMIT ?",
                                    (pid, limit))
        return [dict(r) for r in rows]

    def order_count(self) -> dict[str, int]:
        return {r["state"]: r["n"] for r in self._db.execute(
            "SELECT state, COUNT(*) AS n FROM orders GROUP BY state")}

    # ── 조회 ──────────────────────────────────────────────────────────────
    def account(self, pid: int) -> dict:
        cash = self._db.execute("SELECT cash FROM accounts WHERE participant_id=?", (pid,)).fetchone()["cash"]
        pos = [dict(r) for r in self._db.execute(
            "SELECT symbol, qty, cost FROM positions WHERE participant_id=? AND qty > 0 ORDER BY symbol",
            (pid,))]
        return {"cash": cash, "positions": pos}

    def held_symbols(self) -> list[str]:
        return [r["symbol"] for r in self._db.execute(
            "SELECT DISTINCT symbol FROM positions WHERE qty > 0 ORDER BY symbol")]

    def leaderboard(self, prices: dict[str, int]) -> list[dict]:
        """평가금액 = 예수금 + 보유수량 × 현재가. 현재가를 모르는 종목은 원가로 센다(표시한다)."""
        board = []
        for p in self._db.execute("SELECT p.id, p.nickname, a.cash FROM participants p"
                                  " JOIN accounts a ON a.participant_id = p.id"):
            value, unpriced = p["cash"], []
            for pos in self._db.execute("SELECT symbol, qty, cost FROM positions"
                                        " WHERE participant_id=? AND qty > 0", (p["id"],)):
                if prices.get(pos["symbol"]):
                    value += pos["qty"] * prices[pos["symbol"]]
                else:
                    value += pos["cost"]
                    unpriced.append(pos["symbol"])
            board.append({"nickname": p["nickname"], "equity": value,
                          "return_pct": round((value / self.initial_cash - 1) * 100, 2),
                          "unpriced": unpriced})
        board.sort(key=lambda r: r["equity"], reverse=True)
        for i, r in enumerate(board, 1):
            r["rank"] = i
        return board

    # ── 대사 ──────────────────────────────────────────────────────────────
    def integrity(self) -> list[dict]:
        """잔고표·주문표를 거래 기록으로 다시 계산해 대조한다. 어긋난 것을 전부 돌려준다."""
        with self._lock:
            return self._integrity()

    def _integrity(self) -> list[dict]:
        breaks: list[dict] = []
        cash: dict[int, int] = {}
        pos: dict[tuple, int] = {}
        fills: dict[int, dict] = {}
        prev = GENESIS
        for j in self._db.execute("SELECT * FROM journal ORDER BY id"):
            if j["prev_hash"] != prev or j["hash"] != _chain(prev, j["ts"], j["kind"],
                                                             j["participant_id"], j["detail"]):
                breaks.append({"kind": "journal_chain_broken", "journal_id": j["id"]})
                return breaks                  # 체인이 끊기면 그 뒤 기록은 믿을 수 없다
            prev = j["hash"]
            d = json.loads(j["detail"])
            pid = j["participant_id"]
            if j["kind"] == "deposit":
                cash[pid] = cash.get(pid, 0) + d["amount"]
            elif j["kind"] == "fill":
                cash[pid] = cash.get(pid, 0) + d["cash_delta"]
                k = (pid, d["symbol"])
                pos[k] = pos.get(k, 0) + (d["qty"] if d["side"] == "buy" else -d["qty"])
                fills[d["order_id"]] = d

        for r in self._db.execute("SELECT participant_id, cash FROM accounts"):
            if cash.get(r["participant_id"], 0) != r["cash"]:
                breaks.append({"kind": "cash_mismatch", "participant_id": r["participant_id"],
                               "table": r["cash"], "journal": cash.get(r["participant_id"], 0)})
        table_pos = {(r["participant_id"], r["symbol"]): r["qty"]
                     for r in self._db.execute("SELECT participant_id, symbol, qty FROM positions")}
        for k in sorted(set(table_pos) | set(pos)):
            if table_pos.get(k, 0) != pos.get(k, 0):
                breaks.append({"kind": "position_mismatch", "participant_id": k[0], "symbol": k[1],
                               "table": table_pos.get(k, 0), "journal": pos.get(k, 0)})
        filled = {r["id"]: dict(r) for r in self._db.execute("SELECT * FROM orders WHERE state=?", (FILLED,))}
        for oid in sorted(set(filled) | set(fills)):
            o, f = filled.get(oid), fills.get(oid)
            if o is None or f is None or any(o[k] != f[k] for k in ("qty", "price", "amount", "fee", "tax")):
                breaks.append({"kind": "order_journal_mismatch", "order_id": oid})
        return breaks

    # ── 운영 상태 ─────────────────────────────────────────────────────────
    def halted(self) -> str | None:
        row = self._db.execute("SELECT value FROM settings WHERE key='halted'").fetchone()
        return row["value"] if row else None

    def halt(self, reason: str) -> None:
        with self._tx() as db:
            db.execute("INSERT INTO settings VALUES ('halted', ?) ON CONFLICT (key) DO UPDATE"
                       " SET value=excluded.value", (reason,))

    def resume(self) -> None:
        with self._tx() as db:
            db.execute("DELETE FROM settings WHERE key='halted'")
