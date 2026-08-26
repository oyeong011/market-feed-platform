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
