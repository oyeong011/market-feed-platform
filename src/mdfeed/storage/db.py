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
import threading
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
    def execute(self, sql: str, params: Sequence = ()) -> int: ...
    def upsert_latest(self, rows: Sequence[tuple]) -> int: ...

    # ── latest 유지 (백엔드 공통) ─────────────────────────────────────────
    @staticmethod
    def _latest_rows(trades: Sequence[tuple]) -> list[tuple]:
        """체결 배치에서 종목별 최신 한 줄만 추린다.

        행 수가 아니라 종목 수에 비례한다. 이걸 writer 에 두면 다른 경로로
        적재할 때(백필·복구 도구) latest 가 조용히 뒤처진다. 그래서
        insert_trades 안에서 항상 같이 갱신한다.
        """
        newest: dict[tuple, tuple] = {}
        for r in trades:
            # (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq)
            k = (r[1], r[2])
            if k not in newest or r[0] > newest[k][0]:
                newest[k] = r
        return [(r[1], r[2], r[0], r[3], r[4], r[5], r[7]) for r in newest.values()]
    def delete_older_than(self, table: str, col: str, cutoff: int,
                          limit: int) -> int: ...
    def close(self) -> None: ...

    # ── 공통 조회 (백엔드 무관) ───────────────────────────────────────────
    def latest(self, limit: int = 100, venue: str | None = None,
               symbol: str | None = None) -> list[dict]:
        """종목별 최신 시세. 뷰가 아니라 적재 때 갱신해 둔 테이블을 읽는다.

        걸러내기도 SQL 에서 한다. 전부 읽어 온 뒤 파이썬에서 거르면
        symbol 하나를 물어도 비용은 전체 조회와 같다.
        """
        p = self.placeholder
        where, params = [], []
        if venue:
            where.append(f"venue={p}"); params.append(venue.upper())
        if symbol:
            where.append(f"symbol={p}"); params.append(symbol)
        cond = (" WHERE " + " AND ".join(where)) if where else ""
        return self.query(
            "SELECT venue, symbol, ts, price, qty, side, latency_us "
            f"FROM latest{cond} ORDER BY venue, symbol LIMIT {int(limit)}",
            params)

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
        self._readers: dict[int, sqlite3.Connection] = {}
        self._readers_lock = threading.Lock()
        self._closed = False

    def ensure_schema(self) -> None:
        with open(os.path.join(HERE, "schema_sqlite.sql"), encoding="utf-8") as fh:
            self.conn.executescript(fh.read())
        self.conn.commit()
        self._backfill_latest()
        log.info("SQLite 스키마 준비 완료: %s", self.path)

    def _backfill_latest(self) -> None:
        """기존 DB 에 latest 테이블을 처음 만들면 비어 있다.

        비워 두면 다음 체결이 올 때까지 /api/v1/quotes 가 그 종목을 못 준다.
        거래가 뜸한 종목은 몇 시간씩 안 보인다 — 배포하자마자 조회가 비는
        건 장애로 보인다. 히스토리에서 한 번 채운다.
        """
        n = self.conn.execute("SELECT COUNT(*) FROM latest").fetchone()[0]
        if n:
            return
        rows = self.conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        if not rows:
            return
        t0 = time.time()
        self.conn.execute(
            "INSERT INTO latest (venue,symbol,ts,price,qty,side,latency_us) "
            "SELECT venue, symbol, MAX(ts), price, qty, side, latency_us "
            "FROM trades GROUP BY venue, symbol")
        self.conn.commit()
        got = self.conn.execute("SELECT COUNT(*) FROM latest").fetchone()[0]
        log.info("latest 백필: %d종목 (%d행 스캔, %.1fs)", got, rows, time.time() - t0)

    def _many(self, sql: str, rows: Sequence[tuple]) -> int:
        if not rows:
            return 0
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def insert_trades(self, rows):
        if not rows:
            return 0
        # 체결 적재와 latest 갱신을 한 트랜잭션으로 묶는다.
        # 따로 커밋하면 flush 마다 fsync 가 두 번이고, 중간에 죽으면
        # trades 는 들어갔는데 latest 는 옛날 값인 상태가 남는다.
        self.conn.executemany(
            "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (?,?,?,?,?,?,?,?,?)", rows)
        self.conn.executemany(self._LATEST_UPSERT, self._latest_rows(rows))
        self.conn.commit()
        return len(rows)

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

    # ts 가 더 최신일 때만 덮는다. 샤드나 재생이 섞이면 과거 값이
    # 최신을 밀어낼 수 있다.
    _LATEST_UPSERT = (
        "INSERT INTO latest (venue,symbol,ts,price,qty,side,latency_us) "
        "VALUES (?,?,?,?,?,?,?) "
        "ON CONFLICT(venue,symbol) DO UPDATE SET "
        "ts=excluded.ts, price=excluded.price, qty=excluded.qty, "
        "side=excluded.side, latency_us=excluded.latency_us "
        "WHERE excluded.ts >= latest.ts")

    def upsert_latest(self, rows: Sequence[tuple]) -> int:
        return self._many(self._LATEST_UPSERT, rows)

    def _reader(self) -> sqlite3.Connection:
        """조회는 스레드마다 별도 커넥션을 쓴다.

        커넥션 하나를 락으로 직렬화하면 무거운 조회 하나가 나머지 전부를 막는다.
        실측(560만 행): ``SELECT COUNT(*) FROM trades`` 4.6초가 도는 동안
        0.01초짜리 최신시세 조회가 **4.44초**로 밀렸다 — 444배다.
        WAL 은 읽기끼리 동시성이 있으니 막고 있던 건 DB 가 아니라 우리 락이었다.

        ``query_only`` 로 이 커넥션에서는 쓰기가 아예 안 되게 잠근다.
        조회 경로와 쓰기 경로를 나눠 놨어도(``query``/``execute``) 규약은
        언젠가 깨지는데, 커넥션이 거부하면 그때 바로 드러난다.
        """
        if self._closed:
            return self.conn
        tid = threading.get_ident()
        conn = self._readers.get(tid)
        if conn is not None:
            return conn
        try:
            conn = sqlite3.connect(self.path, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
        except sqlite3.Error as e:                # noqa: BLE001
            log.warning("조회 커넥션을 못 열었다(%s) → 본 커넥션으로 처리한다", e)
            return self.conn
        with self._readers_lock:
            self._readers[tid] = conn
        return conn

    def query(self, sql: str, params: Sequence = ()) -> list[dict]:
        cur = self._reader().execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]

    def execute(self, sql: str, params: Sequence = ()) -> int:
        """쓰기용. query() 로 DELETE 를 돌리면 커밋이 안 돼 아무 일도 안 일어난다.
        조회 경로와 쓰기 경로를 나누지 않으면 이런 게 조용히 통과한다."""
        cur = self.conn.execute(sql, tuple(params))
        self.conn.commit()
        return cur.rowcount

    def delete_older_than(self, table: str, col: str, cutoff: int,
                          limit: int) -> int:
        # 한 번에 다 지우면 락을 오래 잡아 적재가 밀린다. rowid 로 끊어 지운다.
        return self.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {col} < ? LIMIT {int(limit)})",
            (cutoff,))

    def close(self) -> None:
        """쓰기 커넥션만 닫는다. 남의 스레드가 읽는 중인 커넥션은 건드리지 않는다.

        읽는 중인 sqlite3 커넥션을 다른 스레드에서 닫으면 세그폴트가 난다.
        예전엔 락으로 막았는데, 그 락이 곧 위의 444배 지연이었다.
        종료 직전 프로세스에서 리더 몇 개를 안 닫는 건 대가가 없다 —
        OS 가 회수한다. 대신 닫힌 뒤 새 리더는 열지 않는다.
        """
        self._closed = True
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
        n = self._many(
            "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
            "VALUES (to_timestamp(%s/1e6),%s,%s,%s,%s,%s,to_timestamp(%s/1e6),%s,%s)", rows)
        if rows:
            self.upsert_latest(self._latest_rows(rows))
        return n

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

    def upsert_latest(self, rows: Sequence[tuple]) -> int:
        # trades.ts 가 timestamptz 이므로 latest 도 같은 타입이다.
        # 한쪽만 정수로 두면 두 테이블의 시각을 비교할 수 없다.
        return self._many(
            "INSERT INTO latest (venue,symbol,ts,price,qty,side,latency_us) "
            "VALUES (%s,%s,to_timestamp(%s/1e6),%s,%s,%s,%s) "
            "ON CONFLICT (venue,symbol) DO UPDATE SET "
            "ts=EXCLUDED.ts, price=EXCLUDED.price, qty=EXCLUDED.qty, "
            "side=EXCLUDED.side, latency_us=EXCLUDED.latency_us "
            "WHERE EXCLUDED.ts >= latest.ts", rows)

    def query(self, sql: str, params: Sequence = ()) -> list[dict]:
        with self.conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
            cur.execute(sql, tuple(params))
            return [dict(r) for r in cur.fetchall()]

    def execute(self, sql: str, params: Sequence = ()) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.rowcount

    def delete_older_than(self, table: str, col: str, cutoff: int,
                          limit: int) -> int:
        # Postgres 에는 rowid 가 없다. SQLite 용 rowid 쿼리를 그대로 보내면
        # 여기서만 조용히 실패한다 — 백엔드 차이는 저장소 계층이 흡수해야 한다.
        return self.execute(
            f"DELETE FROM {table} WHERE ctid IN "
            f"(SELECT ctid FROM {table} WHERE {col} < %s LIMIT {int(limit)})",
            (cutoff,))

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
        except ImportError:
            # 설정 오류다. 폴백은 하되 조용히 넘기면 안 된다 — compose 스택에서
            # DATABASE_URL 을 줬는데도 SQLite 로 떨어져 있는 걸 한참 뒤에야 알았다.
            log.error(
                "DATABASE_URL 이 설정됐지만 psycopg2 드라이버가 없어 Postgres 를 쓸 수 없다. "
                "SQLite 로 폴백한다. 설치: pip install 'mdfeed[postgres]'")
        except Exception as e:                    # noqa: BLE001
            log.error("Postgres 연결 실패 (%s) → SQLite 폴백. 데이터는 계속 쌓인다", e)
    s = SQLiteStorage(cfg.sqlite_path)
    s.ensure_schema()
    return s
