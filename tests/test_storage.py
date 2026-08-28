"""저장소 계층 — 배치 적재와 봉 병합."""
import time

import pytest

from mdfeed.config import Config
from mdfeed.storage.db import SQLiteStorage, open_storage


@pytest.fixture
def store(tmp_path):
    s = SQLiteStorage(str(tmp_path / "t.db"))
    s.ensure_schema()
    yield s
    s.close()


def us_now():
    return int(time.time() * 1e6)


def test_batch_insert_and_count(store):
    t = us_now()
    rows = [(t + i, "UPBIT", "KRW-BTC", 1e8 + i, 0.01, 1, t + i + 500, 500, i)
            for i in range(1000)]
    assert store.insert_trades(rows) == 1000
    assert store.counts()["trades"] == 1000


def test_bar_upsert_merges_correctly(store):
    """같은 버킷이 두 번 들어오면 high/low/volume 이 올바르게 합쳐져야 한다.

    writer 가 재시작하거나 배치가 버킷 경계에 걸치면 실제로 일어나는 상황이다.
    단순 INSERT 면 PK 충돌로 실패하고, 단순 REPLACE 면 앞의 거래량이 사라진다.
    """
    b = 1_700_000_000_000_000
    store.upsert_bars([(b, "UPBIT", "KRW-BTC", 100, 110, 90, 105, 10.0, 1000.0, 100.0, 50)])
    store.upsert_bars([(b, "UPBIT", "KRW-BTC", 105, 120, 80, 118, 5.0, 550.0, 110.0, 25)])
    (row,) = store.query("SELECT * FROM bars_1m")
    assert row["high"] == 120 and row["low"] == 80
    assert row["close"] == 118
    assert row["volume"] == pytest.approx(15.0)
    assert row["tick_count"] == 75


def test_latest_view(store):
    t = us_now()
    store.insert_trades([
        (t, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1),
        (t + 1000, "UPBIT", "KRW-BTC", 200.0, 1.0, 1, t, 10, 2),
        (t, "BINANCE", "BTCUSDT", 300.0, 1.0, 1, t, 10, 3),
    ])
    rows = {r["venue"]: r for r in store.latest()}
    assert len(rows) == 2
    assert rows["UPBIT"]["ts"] == t + 1000


def test_bars_query_is_symbol_scoped(store):
    b = 1_700_000_000_000_000
    store.upsert_bars([
        (b, "UPBIT", "KRW-BTC", 1, 1, 1, 1, 1, 1, 1, 1),
        (b, "UPBIT", "KRW-ETH", 2, 2, 2, 2, 2, 2, 2, 2),
    ])
    assert len(store.bars("UPBIT", "KRW-BTC")) == 1


def test_open_storage_falls_back_to_sqlite_on_bad_dsn(tmp_path):
    """DB 가 안 떠도 수집은 계속돼야 한다. 그날 데이터를 통째로 잃는 것보다 낫다."""
    cfg = Config()
    cfg.pg_dsn = "postgresql://nobody:nobody@127.0.0.1:1/nonexistent"
    cfg.sqlite_path = str(tmp_path / "fallback.db")
    s = open_storage(cfg)
    try:
        assert s.kind == "sqlite"
        assert s.counts()["trades"] == 0
    finally:
        s.close()


# ── latest 테이블 ───────────────────────────────────────────────────────────
# v_latest 뷰는 MAX(ts) 를 매번 계산해 조회 비용이 누적 행수에 비례한다.
# 실측: 274만 행에서 1,299ms, /api/v1/quotes 가 358ms. 마켓데이터에서
# 가장 자주 쓰는 조회가 히스토리가 쌓일수록 느려지면 안 된다.

def test_적재하면_latest_가_같이_갱신된다(store):
    t = us_now()
    store.insert_trades([(t, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1)])
    (row,) = store.query("SELECT * FROM latest")
    assert row["symbol"] == "KRW-BTC" and row["price"] == 100.0


def test_과거_체결이_최신을_밀어내지_않는다(store):
    """샤드나 재생이 섞이면 순서가 뒤집힌 배치가 들어올 수 있다."""
    t = us_now()
    store.insert_trades([(t, "UPBIT", "KRW-BTC", 200.0, 1.0, 1, t, 10, 2)])
    store.insert_trades([(t - 60_000_000, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1)])
    (row,) = store.query("SELECT * FROM latest")
    assert row["price"] == 200.0


def test_한_배치_안의_최신만_남는다(store):
    t = us_now()
    store.insert_trades([
        (t, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1),
        (t + 5000, "UPBIT", "KRW-BTC", 300.0, 1.0, 1, t, 10, 2),
        (t + 1000, "UPBIT", "KRW-BTC", 200.0, 1.0, 1, t, 10, 3),
    ])
    (row,) = store.query("SELECT * FROM latest")
    assert row["price"] == 300.0


def test_조회가_SQL_에서_걸러진다(store):
    t = us_now()
    store.insert_trades([
        (t, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1),
        (t, "UPBIT", "KRW-ETH", 50.0, 1.0, 1, t, 10, 2),
        (t, "BINANCE", "BTCUSDT", 300.0, 1.0, 1, t, 10, 3),
    ])
    assert len(store.latest(venue="UPBIT")) == 2
    assert len(store.latest(symbol="KRW-BTC")) == 1
    # limit 이 거르기 전에 잘리면 원하는 종목이 빠진다
    assert len(store.latest(limit=1, symbol="BTCUSDT")) == 1


def test_기존_DB_는_히스토리에서_백필된다(tmp_path):
    """latest 를 처음 만들면 비어 있다. 다음 체결이 올 때까지 조회가 비면
    거래가 뜸한 종목은 몇 시간씩 안 보인다."""
    import sqlite3
    from mdfeed.storage.db import SQLiteStorage
    path = str(tmp_path / "old.db")
    s1 = SQLiteStorage(path); s1.ensure_schema()
    t = us_now()
    s1.insert_trades([(t, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, t, 10, 1)])
    s1.execute("DELETE FROM latest")               # latest 없던 시절 DB 를 흉내
    s1.close()

    s2 = SQLiteStorage(path); s2.ensure_schema()   # 재기동
    assert len(s2.latest()) == 1
    s2.close()


def test_체결과_latest_가_한_트랜잭션이다(store, tmp_path):
    """따로 커밋하면 flush 마다 fsync 가 두 번이고, 중간에 죽으면
    trades 는 들어갔는데 latest 는 옛날 값인 상태가 남는다."""
    import inspect
    from mdfeed.storage.db import SQLiteStorage
    src = inspect.getsource(SQLiteStorage.insert_trades)
    assert src.count("commit()") == 1, "커밋이 한 번이어야 한다"
    assert "_LATEST_UPSERT" in src
