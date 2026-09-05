"""아카이브 — 지우기 전에 바깥으로 옮긴다.

여기서 제일 중요한 건 압축이 아니라 순서다.

    내보내기 → 목적지에 놓기 → **다시 읽어서 검증** → 그 다음에만 삭제 허용

이 프로젝트에서 네 번 난 사고가 전부 "선언은 됐는데 실제로는 안 돌았다"였다.
아카이브에서 같은 일이 나면 되돌릴 수 없다.
"""
import datetime as dt
import gzip
import os
import sqlite3
import time

import pytest

from mdfeed import archive as ar
from mdfeed.retention import prune

DAY = 86_400 * 1_000_000


class _Store:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE trades (ts INTEGER, venue TEXT, symbol TEXT, "
            "price REAL, qty REAL, side TEXT, recv_ts INTEGER, "
            "latency_us INTEGER, seq INTEGER)")
        self.conn.execute(
            "CREATE TABLE book_top (ts INTEGER, venue TEXT, symbol TEXT, "
            "bid REAL, bid_qty REAL, ask REAL, ask_qty REAL, spread_bp REAL)")

    def query(self, sql, params=()):
        return [dict(r) for r in self.conn.execute(sql, tuple(params))]

    def stream(self, sql, params=(), chunk=50_000):
        cur = self.conn.execute(sql, tuple(params))
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                return
            yield [tuple(r) for r in rows]

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, tuple(params))
        self.conn.commit()
        return cur.rowcount

    def delete_older_than(self, table, col, cutoff, limit):
        return self.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {col} < ? LIMIT {int(limit)})",
            (cutoff,))

    def count(self, t):
        return self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]


DAYS = [dt.date(2026, 8, 30), dt.date(2026, 8, 31), dt.date(2026, 9, 1)]


@pytest.fixture
def store(tmp_path):
    s = _Store(str(tmp_path / "a.db"))
    for d in DAYS:
        lo, _hi = ar.day_bounds_us(d)
        s.conn.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?)",
            [(lo + i * 1000, "UPBIT", f"KRW-{i % 7}", 100.0 + i, 1.5, "buy",
              lo + i * 1000 + 900, 900, i) for i in range(50)])
        s.conn.executemany(
            "INSERT INTO book_top VALUES (?,?,?,?,?,?,?,?)",
            [(lo + i * 1000, "UPBIT", f"KRW-{i % 7}", 99.0, 1.0, 101.0, 1.0,
              20.0) for i in range(10)])
    s.conn.commit()
    return s


# ── 내보내기 ──────────────────────────────────────────────────────────────

def test_내보낸_행이_원본과_같다(store, tmp_path):
    """행수만 맞춰 놓고 내용이 틀리면 아카이브는 쓰레기다."""
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    assert man.rows == 50
    lo, hi = ar.day_bounds_us(DAYS[0])
    src = store.query("SELECT ts,venue,symbol,price,qty,side,recv_ts,"
                      "latency_us,seq FROM trades WHERE ts>=? AND ts<? "
                      "ORDER BY ts", (lo, hi))
    with gzip.open(os.path.join(out, ar._name("trades", DAYS[0])),
                   "rt", encoding="utf-8", newline="") as fh:
        lines = fh.read().splitlines()
    assert lines[0] == "ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq"
    assert len(lines) - 1 == 50
    first = lines[1].split(",")
    assert int(first[0]) == src[0]["ts"]
    assert first[1] == src[0]["venue"] and first[2] == src[0]["symbol"]
    assert float(first[3]) == src[0]["price"]


def test_그날_것만_담는다(store, tmp_path):
    """날짜 경계가 새면 어떤 행은 두 파일에, 어떤 행은 어디에도 없다."""
    out = str(tmp_path / "arc")
    for d in DAYS:
        assert ar.export_day(store, "trades", d, out).rows == 50
    lo, hi = ar.day_bounds_us(DAYS[1])
    with gzip.open(os.path.join(out, ar._name("trades", DAYS[1])),
                   "rt", encoding="utf-8") as fh:
        next(fh)
        for line in fh:
            assert lo <= int(line.split(",")[0]) < hi


def test_같은_입력은_같은_바이트를_낸다(store, tmp_path):
    """gzip 은 기본으로 현재 시각을 헤더에 넣는다. 그러면 해시가 매번
    달라져서 검증 수단이 못 된다."""
    a, b = str(tmp_path / "a"), str(tmp_path / "b")
    m1 = ar.export_day(store, "trades", DAYS[0], a)
    time.sleep(1.1)                       # gzip mtime 이 초 단위라 1초 이상
    m2 = ar.export_day(store, "trades", DAYS[0], b)
    assert m1.sha256 == m2.sha256


def test_이미_있고_검증되면_다시_안_만든다(store, tmp_path):
    out = str(tmp_path / "arc")
    ar.export_day(store, "trades", DAYS[0], out)
    again = ar.export_day(store, "trades", DAYS[0], out)
    assert again.get("skipped") is True


def test_반쪽_파일은_완성본_행세를_못_한다(store, tmp_path):
    """도중에 죽었을 때 정상 이름으로 남으면, 다음 실행이 그걸 완성본으로
    믿고 넘어가고 원본은 지워진다."""
    out = str(tmp_path / "arc")
    ar.export_day(store, "trades", DAYS[0], out)
    assert not [n for n in os.listdir(out) if n.endswith(".partial")]


# ── 검증 ──────────────────────────────────────────────────────────────────

def test_잘린_파일은_검증에서_떨어진다(store, tmp_path):
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    assert ar.verify_file(path, man)
    with open(path, "r+b") as fh:                 # 뒤를 자른다
        fh.truncate(os.path.getsize(path) - 20)
    assert not ar.verify_file(path, man)


def test_빈_파일은_검증에서_떨어진다(store, tmp_path):
    """있는지만 보면 0바이트도 통과하고, 그 다음에 원본을 지운다."""
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    open(path, "wb").close()
    assert not ar.verify_file(path, man)


def test_행수가_다르면_해시가_맞아도_떨어진다(store, tmp_path):
    """매니페스트가 거짓말을 하는 경우. 해시는 '내가 쓴 바이트 그대로'만
    보증하지 내용이 온전한지는 모른다."""
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    man["rows"] = 999
    assert not ar.verify_file(path, man)


# ── 삭제 인터록: 이게 이 모듈의 존재 이유다 ────────────────────────────────

def test_검증된_구간까지만_지운다(store, tmp_path):
    """보존 일수가 더 지우라고 해도, 안 옮긴 건 안 지운다."""
    out = str(tmp_path / "arc")
    for t in ("trades", "book_top"):
        ar.export_day(store, t, DAYS[0], out)
    floor = ar.safe_delete_cutoff_us(out)
    assert floor == ar.day_bounds_us(DAYS[1])[0]     # 8/30 까지만

    # 아주 옛날까지 지우라는 설정이어도 floor 가 막는다
    r = prune(store, retention_days=0.000001, floor_us=floor)
    assert store.count("trades") == 100             # 8/31 · 9/1 은 남는다
    assert sum(r.values()) == 60                    # 8/30 의 50 + 10


def test_아카이브가_없으면_아무것도_안_지운다(store, tmp_path):
    """가장 위험한 경우다. 여기서 실수하면 데이터가 영원히 사라진다."""
    out = str(tmp_path / "empty")
    os.makedirs(out)
    floor = ar.safe_delete_cutoff_us(out)
    assert floor == 0
    r = prune(store, retention_days=0.000001, floor_us=floor)
    assert sum(r.values()) == 0
    assert store.count("trades") == 150


def test_중간에_빠진_날이_있으면_거기서_멈춘다(store, tmp_path):
    """3일치가 있고 2일째가 빠지고 3일째가 있을 때 3일째까지 지우면
    2일째가 영원히 사라진다."""
    out = str(tmp_path / "arc")
    for t in ("trades", "book_top"):
        ar.export_day(store, t, DAYS[0], out)
        ar.export_day(store, t, DAYS[2], out)       # DAYS[1] 을 건너뛴다
    assert ar.safe_delete_cutoff_us(out) == ar.day_bounds_us(DAYS[1])[0]


def test_테이블_하나만_올린_날은_안_센다(store, tmp_path):
    """trades 만 올리고 book_top 을 안 올린 날을 지우면 호가가 사라진다."""
    out = str(tmp_path / "arc")
    ar.export_day(store, "trades", DAYS[0], out)    # book_top 은 안 올림
    assert ar.safe_delete_cutoff_us(out) == 0


def test_깨진_아카이브는_삭제_허용에_안_들어간다(store, tmp_path):
    """파일이 있다는 것과 읽힌다는 것은 다르다."""
    out = str(tmp_path / "arc")
    for t in ("trades", "book_top"):
        ar.export_day(store, t, DAYS[0], out)
    assert ar.safe_delete_cutoff_us(out) > 0
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    with open(path, "r+b") as fh:
        fh.seek(0)
        fh.write(b"\x00" * 64)
    assert ar.safe_delete_cutoff_us(out) == 0


def test_보존이_아카이브보다_짧으면_보존을_따른다(store, tmp_path):
    """빗장은 상한이지 하한이 아니다. 더 적게 지우는 쪽이 이긴다."""
    out = str(tmp_path / "arc")
    for t in ("trades", "book_top"):
        for d in DAYS:
            ar.export_day(store, t, d, out)
    floor = ar.safe_delete_cutoff_us(out)            # 9/2 00:00
    # 보존 10,000일 = 아무것도 안 지운다. floor 가 더 크지만 따르면 안 된다.
    r = prune(store, retention_days=10_000, floor_us=floor)
    assert sum(r.values()) == 0
    assert store.count("trades") == 150


# ── 대상 선정 ─────────────────────────────────────────────────────────────

def test_끝나지_않은_날은_안_올린다(store, tmp_path):
    """반쪽이 올라가고, 그 뒤에 온 체결은 영원히 아카이브에 없다."""
    out = str(tmp_path / "arc")
    # 9/1 이 끝난 직후를 '지금'으로 본다 (lag 1시간)
    now = ar.day_bounds_us(DAYS[2])[1] / 1e6 + 60
    days = ar.pending_days(store, out, "trades", lag_s=3600.0, now=now)
    assert DAYS[2] not in days                       # 아직 lag 이 안 지났다
    assert DAYS[0] in days and DAYS[1] in days


def test_이미_올린_날은_다시_안_올린다(store, tmp_path):
    out = str(tmp_path / "arc")
    ar.export_day(store, "trades", DAYS[0], out)
    now = ar.day_bounds_us(DAYS[2])[1] / 1e6 + 7200
    assert DAYS[0] not in ar.pending_days(store, out, "trades", now=now)


def test_업로드_명령에_file_자리가_없으면_거부한다(tmp_path):
    """{file} 없이 실행하면 명령은 성공하는데 아무것도 안 올라간다.
    그리고 그걸 '올렸다'로 세면 원본을 지운다."""
    p = tmp_path / "x"
    p.write_text("x")
    assert ar.upload(str(p), str(p), "true") is False


def test_봉은_아카이브_대상이_아니다():
    """봉은 지우지도 않으므로 옮길 이유가 없다. 대상 목록에 있으면
    지워도 되는 것처럼 보인다."""
    assert "bars_1m" not in ar.ARCHIVE_TABLES
    assert set(ar.ARCHIVE_TABLES) == {"trades", "book_top"}


# ── 진행 상황이 보여야 한다 ────────────────────────────────────────────────
#
# 조각 하나가 16M 행이면 3분 걸린다. 밀린 날이 7일이면 20분 넘게 도는데,
# 그동안 헬스가 계속 0 이면 **멈춘 것과 구분이 안 된다.** 실제로 첫 판이
# 그랬다 — 4분을 기다려도 archived_days 가 0 이라 고장인 줄 알았다.

def test_조각마다_진행_상황을_올린다(store, tmp_path, monkeypatch):
    from mdfeed.config import Config
    from mdfeed.services.writer import Writer

    monkeypatch.setenv("MDFEED_SQLITE_PATH", str(tmp_path / "w.db"))
    monkeypatch.setenv("MDFEED_ARCHIVE_DIR", str(tmp_path / "arc"))
    monkeypatch.setenv("MDFEED_ARCHIVE_LAG_S", "0")
    w = Writer(Config())
    w.storage = store

    seen = []
    real = ar.export_day

    def spy(storage, table, day, out_dir):
        # 내보내는 **도중에** 무엇을 하는 중인지 나와 있어야 한다
        seen.append((w.archive_current, w.archived_days))
        return real(storage, table, day, out_dir)

    monkeypatch.setattr(ar, "export_day", spy)
    r = w._archive_once()

    assert not r["failed"], r["failed"]
    assert len(seen) == 6                       # 2테이블 × 3일
    # 첫 조각을 만드는 동안 이미 "무엇을 하는 중"이 나와 있다
    assert seen[0][0] == f"trades/{DAYS[0]}"
    assert seen[0][1] == 0
    # 두 번째 조각을 만들 때는 첫 조각이 이미 집계돼 있다.
    # 전부 끝난 뒤 한꺼번에 올리면 여기가 0 이라 실패한다.
    assert seen[1][1] == 1, "조각이 끝났는데 집계가 안 올라갔다"
    assert w.archived_days == 6
    assert w.archived_rows == 3 * 50 + 3 * 10
    # 끝나면 "쉬는 중"으로 돌아간다. 안 지우면 영원히 진행 중으로 보인다.
    assert w.archive_current is None


def test_아카이브가_막히면_삭제_상한도_안_올라간다(store, tmp_path, monkeypatch):
    """설계대로다. 그래서 디스크가 차는데 '보존이 안 돈다'로 보인다 —
    헬스에 archive_failed 를 같이 내야 원인이 바로 설명된다."""
    from mdfeed.config import Config
    from mdfeed.services.writer import Writer

    out = str(tmp_path / "arc")
    monkeypatch.setenv("MDFEED_SQLITE_PATH", str(tmp_path / "w.db"))
    monkeypatch.setenv("MDFEED_ARCHIVE_DIR", out)
    monkeypatch.setenv("MDFEED_ARCHIVE_LAG_S", "0")
    w = Writer(Config())
    w.storage = store

    def boom(*a, **k):
        raise OSError("목적지가 마운트 해제됨")

    monkeypatch.setattr(ar, "export_day", boom)
    r = w._archive_once()
    assert len(r["failed"]) == 6
    assert w._archive_floor_us() == 0          # 지워도 되는 구간이 없다


# ── 검증 캐시 ─────────────────────────────────────────────────────────────
#
# 검증은 파일을 통째로 두 번 읽는다(해시 + gzip 행수). 90일치 21GB 가
# 쌓이면 매 주기 21GB 를 다시 읽는다. 캐시가 필요한데, 캐시가 손상을
# 통과시키면 **캐시가 데이터를 지운다.**

def test_캐시는_같은_파일을_다시_안_읽는다(store, tmp_path, monkeypatch):
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    os.utime(path, (0, 0))                     # 충분히 오래된 것으로
    ar._VERIFY_CACHE.clear()

    calls = []
    real = ar._verify_file_uncached
    monkeypatch.setattr(ar, "_verify_file_uncached",
                        lambda p, m: (calls.append(p), real(p, m))[1])
    assert ar.verify_file(path, man)
    assert ar.verify_file(path, man)
    assert len(calls) == 1, "두 번째는 캐시를 써야 한다"


def test_내용이_바뀌면_캐시를_안_쓴다(store, tmp_path):
    """크기가 같아도 내용이 바뀌면 다시 읽어야 한다. 안 그러면 손상된
    아카이브를 통과시키고 원본을 지운다."""
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    os.utime(path, (0, 0))
    ar._VERIFY_CACHE.clear()
    assert ar.verify_file(path, man)

    size = os.path.getsize(path)
    with open(path, "r+b") as fh:              # 크기는 그대로, 내용만 훼손
        fh.seek(size // 2)
        fh.write(b"\xff" * 64)
    assert os.path.getsize(path) == size
    assert not ar.verify_file(path, man), "손상을 캐시가 통과시켰다"


def test_방금_쓴_파일은_캐시에_안_넣는다(store, tmp_path):
    """mtime 해상도 안에서 내용이 바뀌는 창을 닫는다. 아카이브는 한 번 쓰고
    안 고치는 파일이라, 조용해진 뒤부터 캐싱해도 손해가 없다."""
    out = str(tmp_path / "arc")
    man = ar.export_day(store, "trades", DAYS[0], out)
    path = os.path.join(out, ar._name("trades", DAYS[0]))
    ar._VERIFY_CACHE.clear()
    assert ar.verify_file(path, man)
    assert ar._VERIFY_CACHE == {}, "막 쓰인 파일이 캐시에 들어갔다"


def test_최소_최대_시각은_인덱스를_탄다(tmp_path):
    """`SELECT MIN(ts), MAX(ts)` 를 한 쿼리에 쓰면 SQLite 가 인덱스
    최적화를 못 걸고 전체를 훑는다.

    실측(7,438만 행): 한 쿼리 **50.16초** · 따로 두 번 **0.000초**.
    EXPLAIN 이 각각 SCAN 과 SEARCH 를 냈다. 이 50초 동안 아카이브는
    아무것도 안 하고 헬스에도 표시가 없다 — 멈춘 것처럼 보인다.
    """
    import sqlite3
    from mdfeed.storage import db as dbmod
    conn = sqlite3.connect(str(tmp_path / "p.db"))
    with open(os.path.join(os.path.dirname(dbmod.__file__),
                           "schema_sqlite.sql"), encoding="utf-8") as fh:
        conn.executescript(fh.read())
    for table in ("trades", "book_top"):
        for agg in ("MIN", "MAX"):
            plan = conn.execute(
                f"EXPLAIN QUERY PLAN SELECT {agg}(ts) FROM {table}").fetchall()
            assert "SEARCH" in str(plan[0][3]), f"{table} {agg}: {plan}"


def test_남은_partial_은_지우고_다시_만든다(store, tmp_path):
    """중간에 죽으면 .partial 이 남는다. 완성본 행세는 못 하지만
    그냥 두면 하루치 수백MB 가 목적지에 쌓인다."""
    out = str(tmp_path / "arc")
    os.makedirs(out)
    tmp = os.path.join(out, ar._name("trades", DAYS[0]) + ".partial")
    with open(tmp, "wb") as fh:
        fh.write(b"\x00" * 4096)
    ar.export_day(store, "trades", DAYS[0], out)
    assert not os.path.exists(tmp)


def test_날짜별로_돈다_테이블별이_아니라(store, tmp_path, monkeypatch):
    """삭제 빗장은 '그 날의 모든 테이블'을 요구한다. 테이블을 다 돌고
    다음 테이블로 가면, trades 를 8일치 올리는 동안 빗장이 한 발짝도
    못 올라간다. 실측으로 그렇게 됐다 — trades 4일치가 올라갔는데
    삭제 허용은 계속 0 이었다."""
    from mdfeed.config import Config
    from mdfeed.services.writer import Writer

    monkeypatch.setenv("MDFEED_SQLITE_PATH", str(tmp_path / "w.db"))
    monkeypatch.setenv("MDFEED_ARCHIVE_DIR", str(tmp_path / "arc"))
    monkeypatch.setenv("MDFEED_ARCHIVE_LAG_S", "0")
    w = Writer(Config())
    w.storage = store

    order = []
    real = ar.export_day
    monkeypatch.setattr(ar, "export_day",
                        lambda s, t, d, o: (order.append((d, t)),
                                            real(s, t, d, o))[1])
    w._archive_once()
    # 첫 날의 두 테이블이 먼저 끝나야 그 날이 삭제 가능해진다
    assert order[0] == (DAYS[0], "trades")
    assert order[1] == (DAYS[0], "book_top")
    assert [d for d, _ in order] == sorted(d for d, _ in order)
    # 첫 날을 마친 시점에 이미 빗장이 한 칸 올라가 있어야 한다
    assert ar.safe_delete_cutoff_us(str(tmp_path / "arc")) > 0
