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
