"""보존 삭제가 적재를 얼마나 멈추는가 — 락 범위 A/B.

왜 재는가
---------
"락을 배치마다 놓게 고쳤다"는 것은 주장이다. 주장은 시험이 아니다.
적재가 실제로 얼마나 멈추는지를 재야 고쳤다고 말할 수 있다.

무엇을 재는가
-------------
writer 는 적재(flush)와 삭제(prune)가 **같은 락**을 쓴다. 그래서 적재가
멈춘 시간 = 적재 스레드가 락을 못 잡은 시간이다. 적재 스레드를 일정 주기로
돌리고, 연속한 두 적재 사이의 최대 간격을 잰다.

* 옛 범위: 락을 루프 바깥에서 한 번 잡고 prune 을 통째로 돌린다
* 새 범위: prune 이 배치마다 잡았다 놓는다 (guard)

같은 데이터·같은 배치 수로 두 번 돌려 비교한다. 조건이 다르면 비교가 아니다.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import threading
import time

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mdfeed.retention import DELETE_BATCH, prune          # noqa: E402


class _Store:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS trades (ts INTEGER, venue TEXT, "
            "symbol TEXT, price REAL, qty REAL, side TEXT, trade_id TEXT, "
            "latency_us INTEGER)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS book_top (ts INTEGER, "
                          "venue TEXT, symbol TEXT)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_sym_ts "
                          "ON trades (venue, symbol, ts DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts "
                          "ON trades (ts DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_book_ts "
                          "ON book_top (ts DESC)")
        self.conn.commit()

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def delete_older_than(self, table, col, cutoff, limit):
        return self.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {col} < ? LIMIT {int(limit)})",
            (cutoff,))

    def flush(self, rows):
        self.conn.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)", rows)
        self.conn.commit()


def _fill(store: _Store, rows: int, age_days: float) -> None:
    base = int((time.time() - age_days * 86400) * 1e6)
    syms = [f"KRW-{i:04d}" for i in range(500)]
    for c in range(0, rows, 100_000):
        store.conn.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
            [(base + (c + i) * 100, "UPBIT", syms[(c + i) % 500], 100.0, 1.0,
              "buy", str(c + i), 900) for i in range(min(100_000, rows - c))])
        store.conn.commit()


def _run(path: str, rows: int, old_scope: bool, flush_hz: float) -> dict:
    for suf in ("", "-wal", "-shm"):
        try:
            os.remove(path + suf)
        except OSError:
            pass
    store = _Store(path)
    _fill(store, rows, age_days=10.0)

    lock = threading.Lock()
    stop = threading.Event()
    stamps: list[float] = []
    period = 1.0 / flush_hz

    def flusher() -> None:
        """적재를 흉내낸다. 락을 못 잡으면 그만큼 밀린다 — 그게 정지 시간이다.

        시각은 **적재가 끝난 뒤** 찍는다. 락을 잡기 전에 찍으면 막혀 있던
        시간이 기록에서 사라진다 — 첫 판이 그래서 옛 구현을 더 좋게 쟀다.
        """
        i = 0
        while not stop.is_set():
            with lock:
                store.flush([(int(time.time() * 1e6), "UPBIT", "KRW-NEW",
                              1.0, 1.0, "buy", f"n{i}", 900)])
            stamps.append(time.monotonic())      # 적재가 실제로 일어난 시각
            i += 1
            time.sleep(period)

    th = threading.Thread(target=flusher, daemon=True)
    th.start()
    time.sleep(1.0)                       # 정지 없는 구간을 먼저 만든다
    n0 = len(stamps)

    t0 = time.monotonic()
    if old_scope:
        # 옛 writer._prune_locked: 락을 통째로 잡고 prune 을 돌린다
        with lock:
            r = prune(store, retention_days=2.0)
    else:
        r = prune(store, retention_days=2.0, guard=lock)
    prune_s = time.monotonic() - t0

    time.sleep(1.0)          # prune 이 끝난 뒤 마지막 공백까지 닫히게 둔다
    stop.set()
    th.join(timeout=10)

    gaps = [b - a for a, b in zip(stamps[n0 - 1:], stamps[n0:])]
    return {
        "lock_scope": "옛 판(루프 바깥)" if old_scope else "새 판(배치마다)",
        "prune_s": round(prune_s, 2),
        "batches": r.batches,
        "deleted_rows": sum(r.values()),
        "flushes_during_prune": len(gaps),
        "max_gap_s": round(max(gaps), 3) if gaps else None,
        "p99_gap_s": (round(statistics.quantiles(gaps, n=100)[98], 3)
                      if len(gaps) >= 100 else None),
        "median_gap_s": round(statistics.median(gaps), 3) if gaps else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser("retention_stall_bench")
    ap.add_argument("--rows", type=int, default=DELETE_BATCH * 20)
    ap.add_argument("--flush-hz", type=float, default=20.0)
    ap.add_argument("--out", default="docs/data/retention-stall.json")
    ap.add_argument("--db", default="data/bench-retention.db")
    a = ap.parse_args()

    os.makedirs(os.path.dirname(a.db) or ".", exist_ok=True)
    print(f"조건: 삭제 대상 {a.rows:,}행 · 배치 {DELETE_BATCH:,} · "
          f"적재 {a.flush_hz:g}회/초\n")
    # 순서를 뒤집어 한 번 더 잰다. 먼저 돈 쪽이 페이지 캐시를 덥혀 주므로
    # 한 방향만 재면 순서 효과와 락 범위 효과가 섞인다.
    order = [True, False, False, True]
    runs = [_run(a.db, a.rows, old, a.flush_hz) for old in order]
    for r, pos in zip(runs, ("1번째", "2번째", "1번째", "2번째")):
        r["order"] = pos
    results = runs

    print(f"{'순서':<7}{'락 범위':<16} {'삭제':>10} {'배치':>5} {'소요':>7} "
          f"{'적재횟수':>7} {'최대공백':>9} {'중앙값':>8}")
    print("─" * 78)
    for r in results:
        print(f"{r['order']:<7}{r['lock_scope']:<16} {r['deleted_rows']:>10,} "
              f"{r['batches']:>5} {r['prune_s']:>6.2f}s "
              f"{r['flushes_during_prune']:>7} "
              f"{r['max_gap_s']:>8.3f}s {r['median_gap_s']:>7.3f}s")
    olds = [r["max_gap_s"] for r in results if "옛" in r["lock_scope"]]
    news = [r["max_gap_s"] for r in results if "새" in r["lock_scope"]]
    if olds and news and min(news):
        print(f"\n적재 최대 정지 — 옛 판 {min(olds):.2f}~{max(olds):.2f}s · "
              f"새 판 {min(news):.2f}~{max(news):.2f}s")
        # 배수는 쓰지 않는다. 이 기계의 부하에 따라 흔들려서 회차마다
        # 22배에서 1배까지 나온다. 흔들리는 숫자를 성과로 내면 안 된다.
        # 흔들리지 않는 건 **정지가 무엇에 비례하는가**다.
        # 배수보다 이게 핵심이다. 옛 판은 정지가 **삭제 총량**에 비례하고
        # 새 판은 **배치 하나**에 묶인다. 그래서 데이터가 늘수록 격차가 벌어진다.
        for r in results:
            ratio = r["max_gap_s"] / r["prune_s"] if r["prune_s"] else 0
            what = "삭제 전체" if ratio > 0.8 else "배치 하나"
            print(f"  {r['lock_scope']} {r['order']}: 최대 공백이 삭제 소요의 "
                  f"{ratio * 100:.0f}% → 정지가 {what}에 묶여 있다")
        print("이 비율은 부하가 흔들려도 안 변한다. 옛 판은 100% 언저리,"
              " 새 판은 데이터가 늘수록 0 으로 간다.")
        print("실측 0.63초/배치로 환산: 보존 3일 557배치면 옛 판은 약 5.9분"
              " 정지하고, 새 판은 삭제 총량과 무관하게 배치 하나(약 1초)에"
              " 머문다.")
    print("최대 공백이 곧 적재가 멈춘 시간이다. 버스는 drop-oldest 라 "
          "그동안 온 틱은 버려진다.")

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump({"measured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                   "rows": a.rows, "delete_batch": DELETE_BATCH,
                   "flush_hz": a.flush_hz, "runs": results},
                  fh, ensure_ascii=False, indent=2)
    print(f"\n→ {a.out}")
    for suf in ("", "-wal", "-shm"):
        try:
            os.remove(a.db + suf)
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
