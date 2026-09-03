"""보존 정책과 디스크 여유 추정.

688종목을 붙이니 초당 285행, 하루 약 2,460만 행이 쌓였다. 보존 정책이
없으면 디스크가 찰 때까지 쓰다가 죽고, 그건 "프로세스가 죽었다"로만 보인다.
"""
import os
import sqlite3
import time

import pytest

from mdfeed.retention import DiskWatch, prune


class _Store:
    """query() 만 있으면 되는 최소 저장소."""

    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE trades (ts INTEGER, venue TEXT, symbol TEXT, price REAL)")
        self.conn.execute(
            "CREATE TABLE book_top (ts INTEGER, venue TEXT, symbol TEXT)")
        self.conn.execute("CREATE TABLE bars_1m (bucket INTEGER, symbol TEXT)")

    def query(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def delete_older_than(self, table, col, cutoff, limit):
        return self.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {col} < ? LIMIT {int(limit)})",
            (cutoff,))

    def count(self, t):
        return self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]


@pytest.fixture
def store(tmp_path):
    s = _Store(str(tmp_path / "t.db"))
    now_us = int(time.time() * 1_000_000)
    day = 86_400 * 1_000_000
    rows = [(now_us - int(d * day), "UPBIT", "KRW-BTC", 1.0)
            for d in (0.1, 0.5, 1.5, 3.0, 10.0)]
    s.conn.executemany("INSERT INTO trades VALUES (?,?,?,?)", rows)
    s.conn.executemany("INSERT INTO book_top VALUES (?,?,?)",
                       [(r[0], r[1], r[2]) for r in rows])
    s.conn.executemany("INSERT INTO bars_1m VALUES (?,?)",
                       [(r[0], r[2]) for r in rows])
    s.conn.commit()
    return s


def test_보존_기간이_지난_원시_데이터만_지운다(store):
    prune(store, retention_days=2.0)
    assert store.count("trades") == 3        # 0.1 · 0.5 · 1.5일
    assert store.count("book_top") == 3


def test_봉은_지우지_않는다(store):
    """봉은 원시 데이터의 요약이라 지우면 과거를 복구할 수 없다."""
    prune(store, retention_days=0.2)
    assert store.count("bars_1m") == 5


def test_0이면_아무것도_안_지운다(store):
    """데이터를 지우는 기능이 기본으로 켜져 있으면 안 된다."""
    assert prune(store, retention_days=0) == {}
    assert store.count("trades") == 5


def test_표본이_짧으면_증가율을_추정하지_않는다(tmp_path):
    """기동 직후 급증을 정상 증가율로 오해하면 거짓 경보가 난다."""
    p = tmp_path / "x.db"
    p.write_bytes(b"0" * 1000)
    w = DiskWatch(str(p))
    w.sample()
    p.write_bytes(b"0" * 100_000)
    w.sample()
    assert w.growth_bytes_per_hour() == 0.0
    assert w.hours_until_full() is None


def test_증가율에서_남은_시간을_뽑는다(tmp_path):
    p = tmp_path / "x.db"
    p.write_bytes(b"0" * 1000)
    w = DiskWatch(str(p))
    w._first = (time.time() - 3600, 0)            # 1시간 전 0바이트
    w._last = (time.time(), 1_000_000_000)        # 지금 1GB
    assert w.growth_bytes_per_hour() == pytest.approx(1e9, rel=0.01)
    h = w.hours_until_full()
    assert h is not None and h > 0


def test_안_늘면_추정하지_않는다(tmp_path):
    p = tmp_path / "x.db"
    p.write_bytes(b"0")
    w = DiskWatch(str(p))
    w._first = (time.time() - 3600, 500)
    w._last = (time.time(), 500)
    assert w.hours_until_full() is None            # 남은 GB 로 착각하면 안 된다


def test_wal_도_DB_크기에_넣는다(tmp_path):
    """WAL 이 수GB 로 자라는 게 실제 장애다. 본 파일만 재면 안 보인다."""
    p = tmp_path / "x.db"
    p.write_bytes(b"0" * 100)
    (tmp_path / "x.db-wal").write_bytes(b"0" * 900)
    assert DiskWatch(str(p)).db_bytes() == 1000


def test_삭제는_커밋된다(store, tmp_path):
    """query() 로 DELETE 를 돌리면 커밋이 안 돼 아무 일도 안 일어난다.
    보존을 켜 놓고도 디스크가 계속 차는 실패 모드다."""
    prune(store, retention_days=2.0)
    store.conn.close()
    import sqlite3
    fresh = sqlite3.connect(str(tmp_path / "t.db"))     # 새 연결 = 커밋된 것만 보임
    assert fresh.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 3


def test_삭제를_지원하지_않는_저장소는_건너뛴다(store):
    """조용히 실패하지 말고 로그를 남기고 아무것도 안 지웠다고 보고한다."""
    class NoDelete:
        query = store.query
    assert prune(NoDelete(), retention_days=2.0) == {}


def test_백엔드마다_삭제_구현이_있다():
    """rowid 는 SQLite 전용이다. Postgres 에 그대로 보내면 거기서만 조용히
    안 돈다. 백엔드 차이는 저장소 계층이 흡수해야 한다."""
    import inspect
    from mdfeed.storage import db
    for cls in (db.SQLiteStorage, db.PostgresStorage):
        assert hasattr(cls, "delete_older_than"), cls.__name__
    # 주석 말고 실제 SQL 을 본다
    def sql_of(cls):
        return [ln for ln in inspect.getsource(cls.delete_older_than).splitlines()
                if "DELETE FROM" in ln or "SELECT" in ln]
    assert any("ctid" in ln for ln in sql_of(db.PostgresStorage))
    assert any("rowid" in ln for ln in sql_of(db.SQLiteStorage))


# ── 켜는 순간이 가장 위험하다 ────────────────────────────────────────────
#
# 실측(9/3): trades 7,438만 행. 보존 3일이면 첫 실행에서 2,780만 행,
# 50,000행 배치로 557번을 지워야 한다. 배치로 끊는 코드는 있었지만 writer 가
# 락을 루프 **바깥**에서 잡고 있어서, 그 557배치가 도는 동안 적재가 통째로
# 멈춘다. 버스는 drop-oldest 라 그 시간만큼 틱이 버려진다.


class _CountingGuard:
    """잡고 놓은 횟수를 센다. 배치마다 놓는지 보려면 횟수를 봐야 한다."""

    def __init__(self):
        self.enters = 0
        self.max_held_batches = 0
        self._batches_this_hold = 0

    def __enter__(self):
        self.enters += 1
        self._batches_this_hold = 0
        return self

    def __exit__(self, *a):
        self.max_held_batches = max(self.max_held_batches,
                                    self._batches_this_hold)
        return False


@pytest.fixture
def big_store(tmp_path):
    """배치가 여러 번 돌 만큼 넣는다. 한 배치로 끝나면 락 범위를 못 잰다."""
    from mdfeed.retention import DELETE_BATCH
    s = _Store(str(tmp_path / "big.db"))
    now_us = int(time.time() * 1_000_000)
    old = now_us - 10 * 86_400 * 1_000_000
    n = DELETE_BATCH * 3 + 7            # 3배치 + 나머지
    s.conn.executemany(
        "INSERT INTO trades VALUES (?,?,?,?)",
        [(old + i, "UPBIT", "KRW-BTC", 1.0) for i in range(n)])
    s.conn.commit()
    return s


def test_락은_배치마다_잡았다_놓는다(big_store):
    """락을 루프 바깥에서 잡으면 배치로 끊는 코드가 아무 일도 안 한다.

    적재는 같은 락을 쓴다. 삭제가 락을 통째로 쥐고 있으면 그동안 적재가
    멈추고, 버스가 drop-oldest 라 틱이 버려진다 — 보존을 켜는 행위 자체가
    데이터 손실이 된다. 옛 구현에서 이 시험은 enters == 1 로 실패한다.
    """
    g = _CountingGuard()
    r = prune(big_store, retention_days=2.0, guard=g)
    assert big_store.count("trades") == 0
    # trades 4배치(50k·50k·50k·7) + 마지막 0행 확인, book_top 1배치
    assert r.batches >= 4
    # 배치 수만큼 잡았다 놓았어야 한다. 1이면 통째로 쥐고 있었다는 뜻이다.
    assert g.enters == r.batches, f"배치 {r.batches}회인데 락은 {g.enters}회만 잡았다"


def test_시간_예산을_넘으면_남기고_돌아온다(big_store):
    """첫 삭제가 가장 크다. 상한이 없으면 그게 한 번에 다 돈다."""
    r = prune(big_store, retention_days=2.0, budget_s=-1.0)   # 즉시 소진
    assert r.budget_hit is True
    assert sum(r.values()) == 0
    assert big_store.count("trades") == 150_007        # 하나도 안 지워졌다


def test_다_지우면_예산에_안_걸렸다고_보고한다(big_store):
    r = prune(big_store, retention_days=2.0, budget_s=600.0)
    assert r.budget_hit is False
    assert big_store.count("trades") == 0


def test_결과는_여전히_테이블별_행수_dict_다(store):
    """호출부가 sum(values()) 를 쓴다. 형이 바뀌면 조용히 깨진다."""
    r = prune(store, retention_days=2.0)
    assert isinstance(r, dict)
    assert sum(r.values()) == 4                # trades 2 + book_top 2


def test_예산에_걸린_건_상태로_보고한다(big_store):
    """다 지운 것과 지우다 만 것이 구분되지 않으면, 삭제가 유입을 못 따라가는
    상태로 매 주기 제자리를 맴돌아도 아무도 모른다."""
    r = prune(big_store, retention_days=2.0, budget_s=-1.0)
    assert hasattr(r, "budget_hit") and hasattr(r, "elapsed_s")


def test_보존_삭제_조건이_인덱스를_탄다(tmp_path):
    """`WHERE ts < ?` 는 (venue, symbol, ts) 복합 인덱스를 못 탄다.

    실측: book_top 만 EXPLAIN 이 SCAN 을 냈다. 배치마다 전체 스캔이면
    배치로 끊는 의미가 절반 사라진다. 스키마에 ts 단독 인덱스가 있어야 한다.
    """
    import os
    import sqlite3
    from mdfeed.storage import db as dbmod
    path = str(tmp_path / "plan.db")
    conn = sqlite3.connect(path)
    with open(os.path.join(os.path.dirname(dbmod.__file__),
                           "schema_sqlite.sql"), encoding="utf-8") as fh:
        conn.executescript(fh.read())
    for table in ("trades", "book_top"):
        plan = conn.execute(
            f"EXPLAIN QUERY PLAN SELECT rowid FROM {table} "
            f"WHERE ts < ? LIMIT 50000", (0,)).fetchall()
        text = " ".join(str(r[3]) for r in plan)
        assert "SEARCH" in text, f"{table}: 인덱스를 못 탄다 — {text}"


def test_지운_뒤_안_줄어든_파일_크기를_설명할_수_있다(tmp_path):
    """SQLite 는 DELETE 한 페이지를 freelist 에 넣고 파일은 안 줄인다.

    이걸 안 내면 운영자가 "보존이 안 돈다"로 오해한다. 실제로는 증가가
    멈춘 것이고, 빈 자리는 다음 적재가 재사용한다.
    """
    import sqlite3
    path = str(tmp_path / "f.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (x BLOB)")
    conn.executemany("INSERT INTO t VALUES (?)", [(b"0" * 4000,) for _ in range(500)])
    conn.commit()
    before = os.path.getsize(path)
    conn.execute("DELETE FROM t")
    conn.commit()
    assert os.path.getsize(path) == before          # 파일은 그대로다
    conn.close()
    w = DiskWatch(path)
    assert w.reclaimable_bytes() > 0                # 빈 자리로는 보인다
    assert w.report()["reclaimable_bytes"] > 0


def test_계획은_지우지_않는다(store):
    """되돌릴 수 없는 결정을 숫자 없이 내리게 하면 안 된다."""
    from mdfeed.retention import prune_plan
    plan = prune_plan(store, retention_days=2.0)
    assert plan["tables"]["trades"]["delete_rows"] == 2
    assert plan["tables"]["trades"]["keep_rows"] == 3
    assert plan["delete_rows_total"] == 4
    assert store.count("trades") == 5              # 아무것도 안 지웠다
