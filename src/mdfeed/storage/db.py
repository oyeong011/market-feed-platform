"""저장소 추상화 — SQLite(기본) / PostgreSQL(운영) 이중 백엔드.

왜 두 개인가
------------
* 운영은 Postgres(+TimescaleDB)가 맞다. 파티셔닝·보존정책·동시 조회가 필요하다.
* 하지만 CI 와 로컬 데모까지 Postgres 를 요구하면 "일단 돌려보기"의 문턱이 높아진다.
  SQLite 폴백이 있으면 `git clone && make demo` 로 전 구간이 돈다.

두 스키마의 컬럼 이름과 의미를 1:1로 맞춰 두었기 때문에, 애플리케이션 쿼리는
플레이스홀더(`?` vs `%s`)만 바뀌고 나머지는 그대로다.

성능 판단
---------
틱은 한 건씩 INSERT 하지 않는다. 배치(기본 500건) 또는 2초 중 먼저 오는 쪽에
executemany 로 밀어 넣는다. 한 건씩 커밋하면 초당 수백 건에서 이미 디스크가
병목이 되고, WAL fsync 가 이벤트 루프를 막는다.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from typing import Any, Iterable, Sequence

log = logging.getLogger("mdfeed.db")

HERE = os.path.dirname(os.path.abspath(__file__))


class Storage:
    """공통 인터페이스."""

    placeholder = "?"
    kind = "base"

    def ensure_schema(self) -> None: ...
    def insert_trades(self, rows: Sequence[tuple]) -> int: ...
    def insert_book(self, rows: Sequence[tuple]) -> int: ...
    def upsert_bars(self, rows: Sequence[tuple]) -> int: ...
    def insert_signals(self, rows: Sequence[tuple]) -> int: ...
    def query(self, sql: str, params: Sequence = ()) -> list[dict]: ...
    def close(self) -> None: ...

    # ── 공통 조회 (백엔드 무관) ───────────────────────────────────────────
    def latest(self, limit: int = 100) -> list[dict]:
        return self.query(
            "SELECT venue, symbol, ts, price, qty, side, latency_us "
            "FROM v_latest ORDER BY venue, symbol LIMIT " + str(int(limit)))

    def bars(self, venue: str, symbol: str, limit: int = 200) -> list[dict]:
        p = self.placeholder
        return self.query(
            f"SELECT bucket, open, high, low, close, volume, vwap, tick_count "
            f"FROM bars_1m WHERE venue={p} AND symbol={p} "
            f"ORDER BY bucket DESC LIMIT {int(limit)}", (venue, symbol))

    def trades(self, venue: str, symbol: str, limit: int = 200) -> list[dict]:
        p = self.placeholder
        return self.query(
            f"SELECT ts, price, qty, side, latency_us FROM trades "
            f"WHERE venue={p} AND symbol={p} ORDER BY ts DESC LIMIT {int(limit)}",
            (venue, symbol))

    def symbols(self) -> list[dict]:
        return self.query(
            "SELECT venue, symbol, COUNT(*) AS bars, MAX(bucket) AS last_bucket "
            "FROM bars_1m GROUP BY venue, symbol ORDER BY venue, symbol")

    def counts(self) -> dict:
        out = {}
        for t in ("trades", "book_top", "bars_1m", "signals"):
            try:
                out[t] = self.query(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"]
            except Exception:                     # noqa: BLE001
                out[t] = -1
        return out


class SQLiteStorage(Storage):
    placeholder = "?"
    kind = "sqlite"

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        # check_same_thread=False: writer 는 단일 스레드지만 REST 가 별도 스레드에서 읽는다
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

    def ensure_schema(self) -> None:
        with open(os.path.join(HERE, "schema_sqlite.sql"), encoding="utf-8") as fh:
            self.conn.executescript(fh.read())
        self.conn.commit()
        log.info("SQLite 스키마 준비 완료: %s", self.path)

    def _many(self, sql: str, rows: Sequence[tuple]) -> int:
        if not rows:
            return 0
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def insert_trades(self, rows):
        return self._many(
            "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (?,?,?,?,?,?,?,?,?)", rows)

    def insert_book(self, rows):
        return self._many(
            "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
            "VALUES (?,?,?,?,?,?,?,?)", rows)

    def upsert_bars(self, rows):
        return self._many(
            "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,"
            "notional,vwap,tick_count) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(venue,symbol,bucket) DO UPDATE SET "
            "high=MAX(high,excluded.high), low=MIN(low,excluded.low), "
            "close=excluded.close, volume=volume+excluded.volume, "
            "notional=notional+excluded.notional, vwap=excluded.vwap, "
            "tick_count=tick_count+excluded.tick_count", rows)

    def insert_signals(self, rows):
        return self._many(
            "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
            "VALUES (?,?,?,?,?,?,?)", rows)

    def query(self, sql: str, params: Sequence = ()) -> list[dict]:
        cur = self.conn.execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]

    def close(self) -> None:
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:                         # noqa: BLE001
            pass


class PostgresStorage(Storage):
    placeholder = "%s"
    kind = "postgres"

    def __init__(self, dsn: str):
        import psycopg2                            # 선택 의존성
        import psycopg2.extras
        self._pg = psycopg2
        self._extras = psycopg2.extras
        self.dsn = dsn
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True

    def _reconnect(self) -> None:
        """운영에서 DB 재시작·네트워크 순단은 정상 사건이다. 죽지 말고 다시 붙는다."""
        try:
            self.conn.close()
        except Exception:                         # noqa: BLE001
            pass
        for attempt in range(5):
            try:
                self.conn = self._pg.connect(self.dsn)
                self.conn.autocommit = True
                log.info("Postgres 재연결 성공 (%d번째 시도)", attempt + 1)
                return
            except Exception as e:                # noqa: BLE001
                log.warning("Postgres 재연결 실패 %d/5: %s", attempt + 1, e)
                time.sleep(min(2 ** attempt, 10))
        raise RuntimeError("Postgres 재연결 실패")

    def ensure_schema(self) -> None:
        with open(os.path.join(HERE, "schema.sql"), encoding="utf-8") as fh:
            sql = fh.read()
        with self.conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO venues (code,name,asset_class) VALUES "
                "('UPBIT','Upbit','CRYPTO'),('BINANCE','Binance','CRYPTO'),"
                "('KIS','한국투자증권','EQUITY') ON CONFLICT (code) DO NOTHING")
        log.info("PostgreSQL 스키마 준비 완료")

    def _many(self, sql: str, rows: Sequence[tuple]) -> int:
        if not rows:
            return 0
        for attempt in (0, 1):
            try:
                with self.conn.cursor() as cur:
                    self._extras.execute_batch(cur, sql, rows, page_size=500)
                return len(rows)
            except self._pg.OperationalError:
                if attempt == 0:
                    self._reconnect()
                else:
                    raise
        return 0

    def insert_trades(self, rows):
        return self._many(
            "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (to_timestamp(%s/1e6),%s,%s,%s,%s,%s,to_timestamp(%s/1e6),%s,%s)", rows)

    def insert_book(self, rows):
        return self._many(
            "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
            "VALUES (to_timestamp(%s/1e6),%s,%s,%s,%s,%s,%s,%s)", rows)

    def upsert_bars(self, rows):
        return self._many(
            "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,"
            "notional,vwap,tick_count) VALUES (to_timestamp(%s/1e6),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (venue,symbol,bucket) DO UPDATE SET "
            "high=GREATEST(bars_1m.high,EXCLUDED.high), low=LEAST(bars_1m.low,EXCLUDED.low), "
            "close=EXCLUDED.close, volume=bars_1m.volume+EXCLUDED.volume, "
            "notional=bars_1m.notional+EXCLUDED.notional, vwap=EXCLUDED.vwap, "
            "tick_count=bars_1m.tick_count+EXCLUDED.tick_count", rows)

    def insert_signals(self, rows):
        return self._many(
            "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
            "VALUES (to_timestamp(%s/1e6),%s,%s,%s,%s,%s,%s)", rows)

    def query(self, sql: str, params: Sequence = ()) -> list[dict]:
        with self.conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
            cur.execute(sql, tuple(params))
            return [dict(r) for r in cur.fetchall()]

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:                         # noqa: BLE001
            pass


def open_storage(cfg) -> Storage:
    """DATABASE_URL 이 있으면 Postgres, 없거나 실패하면 SQLite.

    운영에서 DB 가 안 뜬다고 수집까지 멈추면 그날 데이터를 통째로 잃는다.
    폴백해서 로컬에 쌓아두고, 나중에 이관하는 편이 낫다.
    """
    if cfg.pg_dsn:
        try:
            s = PostgresStorage(cfg.pg_dsn)
            s.ensure_schema()
            return s
        except Exception as e:                    # noqa: BLE001
            log.error("Postgres 연결 실패 (%s) → SQLite 폴백. 데이터는 계속 쌓인다", e)
    s = SQLiteStorage(cfg.sqlite_path)
    s.ensure_schema()
    return s
