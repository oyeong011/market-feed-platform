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

import contextlib
import logging
import os
import re
import sqlite3
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from json import dumps
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

log = logging.getLogger("mdfeed.db")

HERE = os.path.dirname(os.path.abspath(__file__))
TIMESTAMP_COLUMNS = frozenset({"ts", "recv_ts", "bucket", "first_seen", "last_seen"})
DELETE_COLUMNS = frozenset({"ts", "recv_ts", "bucket"})
DELETE_TABLES = frozenset({"trades", "book_top", "bars_1m"})
PG_RUNTIME_TABLE_PRIVILEGES: Final[dict[str, tuple[str, ...]]] = {
    "venues": ("SELECT",),
    "instruments": ("SELECT",),
    "trades": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "book_top": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "bars_1m": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "signals": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "feed_stats": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "latest": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "quality_events": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    "ingest_batch_receipts": ("SELECT", "INSERT"),
    "data_gaps": ("SELECT", "INSERT", "UPDATE"),
    "migration_runs": ("SELECT",),
    "migration_checkpoints": ("SELECT",),
    "backup_restore_receipts": ("SELECT",),
    "archive_remote_receipts": ("SELECT",),
    "authoritative_gap_coverages": ("SELECT",),
    "gap_recovery_receipts": ("SELECT",),
}
PG_REQUIRED_TABLES: Final = tuple(PG_RUNTIME_TABLE_PRIVILEGES)
PG_REQUIRED_VIEWS: Final = ("v_latest", "v_daily_ohlcv", "v_spread_hourly", "v_feed_gaps")
PG_RUNTIME_VIEW_PRIVILEGES: Final[dict[str, tuple[str, ...]]] = {
    view: ("SELECT",) for view in PG_REQUIRED_VIEWS
}
POSTGRES_URL_PASSWORD = re.compile(
    r"((?:postgres|postgresql)://[^\s:/@]+:)[^@\s/?#]+(@)",
    re.IGNORECASE,
)
StorageValue = str | int | float | bytes | datetime | None
StorageRow = tuple[StorageValue, ...]


@dataclass(slots=True)
class StorageConfigurationError(RuntimeError):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class StorageUnavailableError(RuntimeError):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class SanitizedStorageCause(RuntimeError):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class StorageBatch:
    batch_id: str
    digest: str
    trades: tuple[StorageRow, ...]
    books: tuple[StorageRow, ...]
    signals: tuple[StorageRow, ...]
    bars: tuple[StorageRow, ...]

    @property
    def rows_written(self) -> int:
        return len(self.trades) + len(self.books) + len(self.signals)

    @property
    def bars_written(self) -> int:
        return len(self.bars)

    @property
    def table_counts(self) -> tuple[tuple[str, int], ...]:
        return (
            ("trades", len(self.trades)),
            ("book_top", len(self.books)),
            ("signals", len(self.signals)),
            ("bars_1m", len(self.bars)),
        )


@dataclass(frozen=True, slots=True)
class BatchWriteReceipt:
    batch_id: str
    digest: str
    rows_written: int
    bars_written: int


def _sanitize(value: str) -> str:
    out = POSTGRES_URL_PASSWORD.sub(r"\1***\2", value)
    parsed = urlsplit(out)
    if parsed.password:
        out = out.replace(parsed.password, "***")
    for marker in ("password=", "PGPASSWORD=", "passfile="):
        idx = out.lower().find(marker.lower())
        if idx >= 0:
            start = idx + len(marker)
            end = len(out)
            for sep in (" ", "&"):
                pos = out.find(sep, start)
                if pos >= 0:
                    end = min(end, pos)
            out = out[:start] + "***" + out[end:]
    return out


def _storage_cause(exc: BaseException) -> SanitizedStorageCause:
    return SanitizedStorageCause(_sanitize(f"{type(exc).__name__}: {exc}"))


def _us_to_utc(us: int) -> datetime:
    seconds, micros = divmod(int(us), 1_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=micros)


def _int_cell(value: StorageValue) -> int:
    if isinstance(value, int):
        return value
    raise StorageConfigurationError("timestamp column requires integer epoch microseconds")


def _dt_to_us(value: datetime) -> int:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    delta = aware.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def _normalize_value(column: str, value: StorageValue) -> StorageValue:
    if isinstance(value, datetime):
        return _dt_to_us(value)
    return value


def make_storage_batch(
    *,
    trades: Iterable[StorageRow],
    books: Iterable[StorageRow],
    signals: Iterable[StorageRow],
    bars: Iterable[StorageRow],
) -> StorageBatch:
    payload = {
        "trades": list(trades),
        "books": list(books),
        "signals": list(signals),
        "bars": list(bars),
    }
    encoded = dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    digest = sha256(encoded.encode("utf-8")).hexdigest()
    return StorageBatch(
        batch_id=f"batch:{uuid4().hex}",
        digest=digest,
        trades=tuple(tuple(row) for row in payload["trades"]),
        books=tuple(tuple(row) for row in payload["books"]),
        signals=tuple(tuple(row) for row in payload["signals"]),
        bars=tuple(tuple(row) for row in payload["bars"]),
    )


class Storage:
    """공통 인터페이스."""

    placeholder = "?"
    kind = "base"

    def ensure_schema(self) -> None: ...
    def insert_trades(self, rows: Sequence[StorageRow]) -> int: ...
    def insert_book(self, rows: Sequence[StorageRow]) -> int: ...
    def upsert_bars(self, rows: Sequence[StorageRow]) -> int: ...
    def insert_signals(self, rows: Sequence[StorageRow]) -> int: ...
    def query(self, sql: str, params: Sequence[StorageValue] = ()) -> list[dict[str, StorageValue]]: ...
    def execute(self, sql: str, params: Sequence[StorageValue] = ()) -> int: ...
    def upsert_latest(self, rows: Sequence[StorageRow]) -> int: ...
    def write_batch(self, batch: StorageBatch) -> BatchWriteReceipt: ...

    # ── latest 유지 (백엔드 공통) ─────────────────────────────────────────
    @staticmethod
    def _latest_rows(trades: Sequence[StorageRow]) -> list[StorageRow]:
        """체결 배치에서 종목별 최신 한 줄만 추린다.

        행 수가 아니라 종목 수에 비례한다. 이걸 writer 에 두면 다른 경로로
        적재할 때(백필·복구 도구) latest 가 조용히 뒤처진다. 그래서
        insert_trades 안에서 항상 같이 갱신한다.
        """
        newest: dict[tuple[StorageValue, StorageValue], StorageRow] = {}
        for r in trades:
            # (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq)
            k = (r[1], r[2])
            if k not in newest or _int_cell(r[0]) > _int_cell(newest[k][0]):
                newest[k] = r
        return [(r[1], r[2], r[0], r[3], r[4], r[5], r[7]) for r in newest.values()]
    def delete_older_than(self, table: str, col: str, cutoff: int,
                          limit: int) -> int: ...
    def close(self) -> None: ...

    # ── 공통 조회 (백엔드 무관) ───────────────────────────────────────────
    def latest(self, limit: int = 100, venue: str | None = None,
               symbol: str | None = None) -> list[dict[str, StorageValue]]:
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

    def bars(self, venue: str, symbol: str, limit: int = 200) -> list[dict[str, StorageValue]]:
        p = self.placeholder
        return self.query(
            f"SELECT bucket, open, high, low, close, volume, vwap, tick_count "
            f"FROM bars_1m WHERE venue={p} AND symbol={p} "
            f"ORDER BY bucket DESC LIMIT {int(limit)}", (venue, symbol))

    def trades(self, venue: str, symbol: str, limit: int = 200) -> list[dict[str, StorageValue]]:
        p = self.placeholder
        return self.query(
            f"SELECT ts, price, qty, side, latency_us FROM trades "
            f"WHERE venue={p} AND symbol={p} ORDER BY ts DESC LIMIT {int(limit)}",
            (venue, symbol))

    def symbols(self) -> list[dict[str, StorageValue]]:
        return self.query(
            "SELECT venue, symbol, COUNT(*) AS bars, MAX(bucket) AS last_bucket "
            "FROM bars_1m GROUP BY venue, symbol ORDER BY venue, symbol")

    def counts(self) -> dict[str, int]:
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

    def _many(self, sql: str, rows: Sequence[StorageRow]) -> int:
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

    def upsert_latest(self, rows: Sequence[StorageRow]) -> int:
        return self._many(self._LATEST_UPSERT, rows)

    def write_batch(self, batch: StorageBatch) -> BatchWriteReceipt:
        if not (batch.trades or batch.books or batch.signals or batch.bars):
            return BatchWriteReceipt(batch.batch_id, batch.digest, 0, 0)
        if self._batch_already_committed(batch):
            return BatchWriteReceipt(
                batch.batch_id, batch.digest, batch.rows_written, batch.bars_written)
        self.conn.execute("BEGIN")
        try:
            n = len(batch.trades) + len(batch.books) + len(batch.signals)
            self._write_receipts(batch)
            self.conn.executemany(
                "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                "VALUES (?,?,?,?,?,?,?,?,?)", batch.trades)
            self.conn.executemany(self._LATEST_UPSERT, self._latest_rows(batch.trades))
            self.conn.executemany(
                "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
                "VALUES (?,?,?,?,?,?,?,?)", batch.books)
            self.conn.executemany(
                "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
                "VALUES (?,?,?,?,?,?,?)", batch.signals)
            self.conn.executemany(
                "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,"
                "notional,vwap,tick_count) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(venue,symbol,bucket) DO UPDATE SET "
                "high=MAX(high,excluded.high), low=MIN(low,excluded.low), "
                "close=excluded.close, volume=volume+excluded.volume, "
                "notional=notional+excluded.notional, vwap=excluded.vwap, "
                "tick_count=tick_count+excluded.tick_count", batch.bars)
            nb = len(batch.bars)
            self.conn.commit()
        except sqlite3.Error:
            self.conn.rollback()
            raise
        return BatchWriteReceipt(batch.batch_id, batch.digest, n, nb)

    def _batch_already_committed(self, batch: StorageBatch) -> bool:
        expected = {name: count for name, count in batch.table_counts if count}
        if not expected:
            return False
        cur = self.conn.execute(
            "SELECT receipt_id, table_name, batch_hash, row_count "
            "FROM ingest_batch_receipts WHERE receipt_id LIKE ?",
            (f"{batch.batch_id}:%",),
        )
        rows = cur.fetchall()
        if not rows:
            return False
        found = {row["table_name"]: row for row in rows}
        if set(found) != set(expected):
            raise StorageUnavailableError("partial ingest batch receipt requires operator reconciliation")
        for name, count in expected.items():
            row = found[name]
            if row["batch_hash"] != batch.digest or row["row_count"] != count:
                raise StorageUnavailableError("conflicting ingest batch receipt requires operator reconciliation")
        return True

    def _write_receipts(self, batch: StorageBatch) -> None:
        committed_at = datetime.now(timezone.utc).isoformat()
        self.conn.executemany(
            "INSERT INTO ingest_batch_receipts "
            "(receipt_id,service,table_name,batch_hash,row_count,committed_at) "
            "VALUES (?,?,?,?,?,?)",
            [
                (f"{batch.batch_id}:{name}", "writer", name, batch.digest, count, committed_at)
                for name, count in batch.table_counts
                if count
            ],
        )

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
            # 닫힌 뒤에 본 커넥션으로 폴백하면, 방금 close() 한 커넥션을 다른
            # 스레드가 만지게 된다. 그게 예전에 락을 걸었던 이유다.
            # 폴백 대신 거부한다 — 종료 중 조회는 실패하는 게 맞다.
            raise RuntimeError("저장소가 이미 닫혔다 — 조회를 받지 않는다")
        tid = threading.get_ident()
        conn = self._readers.get(tid)
        if conn is not None:
            return conn
        try:
            conn = sqlite3.connect(self.path, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
        except sqlite3.Error as e:
            log.warning("조회 커넥션을 못 열었다(%s) → 본 커넥션으로 처리한다", e)
            return self.conn
        with self._readers_lock:
            self._readers[tid] = conn
        return conn

    def query(self, sql: str, params: Sequence[StorageValue] = ()) -> list[dict[str, StorageValue]]:
        cur = self._reader().execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]

    def stream(self, sql: str, params: Sequence[StorageValue] = (), chunk: int = 50_000):
        """결과를 청크로 흘린다. 아카이브가 하루 950만 행을 읽는다.

        query() 는 fetchall() 이라 전부 메모리에 올린다. 하루치면 GB 단위가
        되고, 그건 적재 프로세스와 같은 기계에서 OOM 을 부른다.
        """
        cur = self._reader().execute(sql, tuple(params))
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                return
            yield [tuple(r) for r in rows]

    def execute(self, sql: str, params: Sequence[StorageValue] = ()) -> int:
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
        with contextlib.suppress(Exception):
            self.conn.commit()
            self.conn.close()


class SQLiteSourceStorage(Storage):
    placeholder = "?"
    kind = "sqlite-source"

    def __init__(self, path: str, conn: sqlite3.Connection):
        self.path = path
        self.conn = conn

    def query(self, sql: str, params: Sequence[StorageValue] = ()) -> list[dict[str, StorageValue]]:
        cur = self.conn.execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]

    def stream(self, sql: str, params: Sequence[StorageValue] = (), chunk: int = 50_000):
        cur = self.conn.execute(sql, tuple(params))
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                return
            yield [tuple(r) for r in rows]

    def execute(self, sql: str, params: Sequence[StorageValue] = ()) -> int:
        del sql, params
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def ensure_schema(self) -> None:
        raise StorageConfigurationError("immutable SQLite source cannot initialize schema")

    def insert_trades(self, rows: Sequence[StorageRow]) -> int:
        del rows
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def insert_book(self, rows: Sequence[StorageRow]) -> int:
        del rows
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def upsert_bars(self, rows: Sequence[StorageRow]) -> int:
        del rows
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def insert_signals(self, rows: Sequence[StorageRow]) -> int:
        del rows
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def upsert_latest(self, rows: Sequence[StorageRow]) -> int:
        del rows
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def write_batch(self, batch: StorageBatch) -> BatchWriteReceipt:
        del batch
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def delete_older_than(self, table: str, col: str, cutoff: int, limit: int) -> int:
        del table, col, cutoff, limit
        raise StorageConfigurationError("immutable SQLite source is read-only")

    def close(self) -> None:
        self.conn.close()


class PostgresStorage(Storage):
    placeholder = "%s"
    kind = "postgres"

    def __init__(self, dsn: str):
        import psycopg2  # 선택 의존성
        import psycopg2.extras
        import psycopg2.sql
        self._pg = psycopg2
        self._extras = psycopg2.extras
        self._sql = psycopg2.sql
        self.dsn = dsn
        self.conn = connect_postgres(dsn)

    def _reconnect(self) -> None:
        """운영에서 DB 재시작·네트워크 순단은 정상 사건이다. 죽지 말고 다시 붙는다."""
        with contextlib.suppress(Exception):
            self.conn.close()
        for attempt in range(5):
            try:
                self.conn = connect_postgres(self.dsn)
                log.info("Postgres 재연결 성공 (%d번째 시도)", attempt + 1)
                return
            except Exception as e:                # noqa: BLE001
                log.warning("Postgres 재연결 실패 %d/5: %s", attempt + 1, e)
                time.sleep(min(2 ** attempt, 10))
        raise RuntimeError("Postgres 재연결 실패")

    def ensure_schema(self) -> None:
        with open(os.path.join(HERE, "schema.sql"), encoding="utf-8") as fh:
            sql = fh.read()
        with self.conn, self.conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO venues (code,name,asset_class) VALUES "
                "('UPBIT','Upbit','CRYPTO'),('BINANCE','Binance','CRYPTO'),"
                "('KIS','한국투자증권','EQUITY') ON CONFLICT (code) DO NOTHING")
        log.info("PostgreSQL 스키마 준비 완료")

    def validate_runtime_schema(self) -> None:
        expected = PG_REQUIRED_TABLES + PG_REQUIRED_VIEWS
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT current_schema(), current_user"
            )
            schema_row = cur.fetchone()
            if schema_row is None:
                raise StorageConfigurationError("PostgreSQL schema validation returned no role")
            schema_name = schema_row[0]
            user_name = schema_row[1]
            if schema_name is None:
                schema_name = "public"
            cur.execute(
                "SELECT c.relname, c.relkind "
                "FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = ANY(%s)",
                (schema_name, list(expected)),
            )
            found = {name: kind for name, kind in cur.fetchall()}
            missing_tables = [
                table for table in PG_REQUIRED_TABLES
                if found.get(table) not in ("r", "p")
            ]
            missing_views = [
                view for view in PG_REQUIRED_VIEWS
                if found.get(view) not in ("v", "m")
            ]
            if missing_tables or missing_views:
                missing = ", ".join(missing_tables + missing_views)
                raise StorageConfigurationError(
                    f"PostgreSQL schema missing required relations: {missing}")
            missing_privileges: list[str] = []
            for table, privileges in PG_RUNTIME_TABLE_PRIVILEGES.items():
                for privilege in privileges:
                    cur.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (user_name, f"{schema_name}.{table}", privilege),
                    )
                    privilege_row = cur.fetchone()
                    if privilege_row is None or not privilege_row[0]:
                        missing_privileges.append(f"{table}:{privilege}")
            for view, privileges in PG_RUNTIME_VIEW_PRIVILEGES.items():
                for privilege in privileges:
                    cur.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (user_name, f"{schema_name}.{view}", privilege),
                    )
                    privilege_row = cur.fetchone()
                    if privilege_row is None or not privilege_row[0]:
                        missing_privileges.append(f"{view}:{privilege}")
            if missing_privileges:
                raise StorageConfigurationError(
                    "PostgreSQL runtime role missing required privileges: "
                    + ", ".join(missing_privileges))
        log.info("PostgreSQL runtime schema validated")

    def _many(self, sql: str, rows: Sequence[StorageRow]) -> int:
        if not rows:
            return 0
        with self.conn, self.conn.cursor() as cur:
            self._extras.execute_batch(cur, sql, rows, page_size=500)
        return len(rows)

    def _many_in_cursor(self, cur, sql: str, rows: Sequence[StorageRow]) -> int:
        if not rows:
            return 0
        self._extras.execute_batch(cur, sql, rows, page_size=500)
        return len(rows)

    def insert_trades(self, rows):
        if not rows:
            return 0
        trades = [(self._ts_row(r, 0, 6)) for r in rows]
        latest = [(r[0], r[1], _us_to_utc(_int_cell(r[2])), r[3], r[4], r[5], r[6])
                  for r in self._latest_rows(rows)]
        with self.conn, self.conn.cursor() as cur:
            n = self._many_in_cursor(
                cur,
                "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                trades)
            self._many_in_cursor(cur, self._LATEST_UPSERT, latest)
        return n

    def insert_book(self, rows):
        converted = [self._ts_row(r, 0) for r in rows]
        return self._many(
            "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", converted)

    def upsert_bars(self, rows):
        converted = [self._ts_row(r, 0) for r in rows]
        return self._many(
            "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,"
            "notional,vwap,tick_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (venue,symbol,bucket) DO UPDATE SET "
            "high=GREATEST(bars_1m.high,EXCLUDED.high), low=LEAST(bars_1m.low,EXCLUDED.low), "
            "close=EXCLUDED.close, volume=bars_1m.volume+EXCLUDED.volume, "
            "notional=bars_1m.notional+EXCLUDED.notional, vwap=EXCLUDED.vwap, "
            "tick_count=bars_1m.tick_count+EXCLUDED.tick_count", converted)

    def insert_signals(self, rows):
        converted = [self._ts_row(r, 0) for r in rows]
        return self._many(
            "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)", converted)

    def upsert_latest(self, rows: Sequence[StorageRow]) -> int:
        # trades.ts 가 timestamptz 이므로 latest 도 같은 타입이다.
        # 한쪽만 정수로 두면 두 테이블의 시각을 비교할 수 없다.
        return self._many(
            self._LATEST_UPSERT,
            [(r[0], r[1], _us_to_utc(_int_cell(r[2])), r[3], r[4], r[5], r[6]) for r in rows])

    _LATEST_UPSERT = (
            "INSERT INTO latest (venue,symbol,ts,price,qty,side,latency_us) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (venue,symbol) DO UPDATE SET "
            "ts=EXCLUDED.ts, price=EXCLUDED.price, qty=EXCLUDED.qty, "
            "side=EXCLUDED.side, latency_us=EXCLUDED.latency_us "
            "WHERE EXCLUDED.ts >= latest.ts")

    def _ts_row(self, row: StorageRow, *positions: int) -> StorageRow:
        values = list(row)
        for pos in positions:
            values[pos] = _us_to_utc(_int_cell(values[pos]))
        return tuple(values)

    def write_batch(self, batch: StorageBatch) -> BatchWriteReceipt:
        if not (batch.trades or batch.books or batch.signals or batch.bars):
            return BatchWriteReceipt(batch.batch_id, batch.digest, 0, 0)
        if self._batch_already_committed(batch):
            return BatchWriteReceipt(
                batch.batch_id, batch.digest, batch.rows_written, batch.bars_written)
        trades = [self._ts_row(row, 0, 6) for row in batch.trades]
        latest = [
            (row[0], row[1], _us_to_utc(_int_cell(row[2])), row[3], row[4], row[5], row[6])
            for row in self._latest_rows(batch.trades)
        ]
        books = [self._ts_row(row, 0) for row in batch.books]
        signals = [self._ts_row(row, 0) for row in batch.signals]
        bars = [self._ts_row(row, 0) for row in batch.bars]
        with self.conn, self.conn.cursor() as cur:
            if not self._claim_receipts(cur, batch):
                return BatchWriteReceipt(
                    batch.batch_id, batch.digest, batch.rows_written, batch.bars_written)
            n = self._many_in_cursor(
                cur,
                "INSERT INTO trades (ts,venue,symbol,price,qty,side,recv_ts,latency_us,seq) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                trades)
            self._many_in_cursor(cur, self._LATEST_UPSERT, latest)
            n += self._many_in_cursor(
                cur,
                "INSERT INTO book_top (ts,venue,symbol,bid,bid_qty,ask,ask_qty,spread_bp) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                books)
            n += self._many_in_cursor(
                cur,
                "INSERT INTO signals (ts,venue,symbol,strategy,action,strength,ref_price) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                signals)
            nb = self._many_in_cursor(
                cur,
                "INSERT INTO bars_1m (bucket,venue,symbol,open,high,low,close,volume,"
                "notional,vwap,tick_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (venue,symbol,bucket) DO UPDATE SET "
                "high=GREATEST(bars_1m.high,EXCLUDED.high), low=LEAST(bars_1m.low,EXCLUDED.low), "
                "close=EXCLUDED.close, volume=bars_1m.volume+EXCLUDED.volume, "
                "notional=bars_1m.notional+EXCLUDED.notional, vwap=EXCLUDED.vwap, "
                "tick_count=bars_1m.tick_count+EXCLUDED.tick_count",
                bars)
        return BatchWriteReceipt(batch.batch_id, batch.digest, n, nb)

    def _batch_already_committed(self, batch: StorageBatch) -> bool:
        expected = {name: count for name, count in batch.table_counts if count}
        if not expected:
            return False
        with self.conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.ingest_batch_receipts') AS table_name")
            row = cur.fetchone()
            if not row or not row[0]:
                raise StorageUnavailableError("ingest_batch_receipts table is required")
            cur.execute(
                "SELECT receipt_id, table_name, batch_hash, row_count "
                "FROM ingest_batch_receipts WHERE receipt_id = ANY(%s)",
                ([f"{batch.batch_id}:{name}" for name in expected],),
            )
            rows = cur.fetchall()
        if not rows:
            return False
        found = {row[1]: row for row in rows}
        if set(found) != set(expected):
            raise StorageUnavailableError("partial ingest batch receipt requires operator reconciliation")
        for name, count in expected.items():
            row = found[name]
            if row[2] != batch.digest or row[3] != count:
                raise StorageUnavailableError("conflicting ingest batch receipt requires operator reconciliation")
        return True

    def _write_receipts(self, cur, batch: StorageBatch) -> None:
        rows = [
            (f"{batch.batch_id}:{name}", "writer", name, batch.digest, count)
            for name, count in batch.table_counts
            if count
        ]
        self._extras.execute_batch(cur,
            "INSERT INTO ingest_batch_receipts "
            "(receipt_id,service,table_name,batch_hash,row_count) "
            "VALUES (%s,%s,%s,%s,%s)",
            rows,
            page_size=4)

    def _claim_receipts(self, cur, batch: StorageBatch) -> bool:
        rows = [
            (f"{batch.batch_id}:{name}", "writer", name, batch.digest, count)
            for name, count in batch.table_counts
            if count
        ]
        claimed = 0
        for row in rows:
            cur.execute(
                "INSERT INTO ingest_batch_receipts "
                "(receipt_id,service,table_name,batch_hash,row_count) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (receipt_id) DO NOTHING RETURNING receipt_id",
                row,
            )
            if cur.fetchone() is not None:
                claimed += 1
        if claimed == len(rows):
            return True
        if claimed:
            raise StorageUnavailableError(
                "partial ingest batch receipt requires operator reconciliation")
        cur.execute(
            "SELECT receipt_id, table_name, batch_hash, row_count "
            "FROM ingest_batch_receipts WHERE receipt_id = ANY(%s)",
            ([row[0] for row in rows],),
        )
        found = {row[1]: row for row in cur.fetchall()}
        expected = {name: count for name, count in batch.table_counts if count}
        if set(found) != set(expected):
            raise StorageUnavailableError(
                "partial ingest batch receipt requires operator reconciliation")
        for name, count in expected.items():
            row = found[name]
            if row[2] != batch.digest or row[3] != count:
                raise StorageUnavailableError(
                    "conflicting ingest batch receipt requires operator reconciliation")
        return False

    def query(self, sql: str, params: Sequence[StorageValue] = ()) -> list[dict[str, StorageValue]]:
        with self.conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
            cur.execute(sql, tuple(params))
            return [
                {k: _normalize_value(k, v) for k, v in dict(r).items()}
                for r in cur.fetchall()
            ]

    def stream(self, sql: str, params: Sequence[StorageValue] = (), chunk: int = 50_000):
        """서버측 커서로 흘린다.

        이름 없는 커서는 psycopg 가 결과를 통째로 클라이언트에 받아 온다 —
        fetchmany 를 써도 메모리는 이미 다 쓴 뒤다. 이름을 주면 서버가
        들고 있다가 필요한 만큼만 보낸다.
        """
        name = f"mdfeed_stream_{id(sql):x}"
        conn = connect_postgres(self.dsn)
        try:
            with conn.cursor(name=name, cursor_factory=self._extras.RealDictCursor) as cur:
                cur.itersize = chunk
                cur.execute(sql, tuple(params))
                while True:
                    rows = cur.fetchmany(chunk)
                    if not rows:
                        return
                    yield [
                        tuple(_normalize_value(key, value)
                              for key, value in dict(row).items())
                        for row in rows
                    ]
        finally:
            conn.close()

    def execute(self, sql: str, params: Sequence[StorageValue] = ()) -> int:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.rowcount

    def delete_older_than(self, table: str, col: str, cutoff: int,
                          limit: int) -> int:
        # Postgres 에는 rowid 가 없다. SQLite 용 rowid 쿼리를 그대로 보내면
        # 여기서만 조용히 실패한다 — 백엔드 차이는 저장소 계층이 흡수해야 한다.
        if table not in DELETE_TABLES or col not in DELETE_COLUMNS:
            raise StorageConfigurationError(f"delete target is not allow-listed: {table}.{col}")
        stmt = self._sql.SQL(
            "DELETE FROM {table} WHERE ctid IN "
            "(SELECT ctid FROM {table} WHERE {col} < %s LIMIT %s)"
        ).format(table=self._sql.Identifier(table), col=self._sql.Identifier(col))
        cutoff_value = _us_to_utc(cutoff)
        with self.conn, self.conn.cursor() as cur:
            cur.execute(stmt, (cutoff_value, int(limit)))
            return cur.rowcount

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.conn.close()


def connect_postgres(dsn: str):
    import psycopg2
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def _dsn_parts(dsn: str) -> dict[str, str]:
    if "://" in dsn:
        parsed = urlsplit(dsn)
        return {k: v[-1] for k, v in parse_qs(parsed.query).items() if v}
    parts: dict[str, str] = {}
    for item in dsn.split():
        key, sep, value = item.partition("=")
        if sep:
            parts[key] = value
    return parts


def validate_storage_config(cfg) -> None:
    backend = cfg.storage_backend.lower()
    profile = cfg.storage_profile.lower()
    match backend:
        case "postgres":
            if not cfg.pg_dsn:
                raise StorageConfigurationError("DATABASE_URL is required when MDFEED_STORAGE_BACKEND=postgres")
            if profile == "production":
                parts = _dsn_parts(cfg.pg_dsn)
                if parts.get("sslmode") != "verify-full":
                    raise StorageConfigurationError(
                        "production PostgreSQL requires sslmode=verify-full")
                if not parts.get("sslrootcert"):
                    raise StorageConfigurationError(
                        "production PostgreSQL requires sslrootcert")
            elif profile != "test":
                raise StorageConfigurationError(
                    "MDFEED_STORAGE_PROFILE must be production or test")
        case "sqlite":
            if profile != "test":
                raise StorageConfigurationError(
                    "sqlite backend requires MDFEED_STORAGE_PROFILE=test")
            if not cfg.sqlite_path:
                raise StorageConfigurationError("MDFEED_SQLITE_PATH is required for sqlite backend")
        case _:
            raise StorageConfigurationError(
                "MDFEED_STORAGE_BACKEND must be postgres or sqlite")


def open_sqlite_source(path: str) -> Storage:
    if not os.path.exists(path):
        raise StorageConfigurationError(f"SQLite source does not exist: {path}")
    wal_path = f"{path}-wal"
    if os.path.exists(wal_path) and os.path.getsize(wal_path) > 0:
        raise StorageConfigurationError(f"SQLite source has a non-empty WAL: {wal_path}")
    uri = f"{Path(path).resolve().as_uri()}?mode=ro&immutable=1"
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
    except sqlite3.DatabaseError as exc:
        if conn is not None:
            conn.close()
        raise StorageConfigurationError(
            f"SQLite source is not a SQLite database: {path}") from _storage_cause(exc)
    if conn is None:
        raise StorageConfigurationError(f"SQLite source could not be opened: {path}")
    return SQLiteSourceStorage(path, conn)


def open_storage(cfg) -> Storage:
    validate_storage_config(cfg)
    backend = cfg.storage_backend.lower()
    match backend:
        case "postgres":
            pg_store: PostgresStorage | None = None
            try:
                pg_store = PostgresStorage(cfg.pg_dsn)
                if cfg.storage_profile.lower() == "test":
                    pg_store.ensure_schema()
                else:
                    pg_store.validate_runtime_schema()
                return pg_store
            except ImportError as exc:
                if pg_store is not None:
                    pg_store.close()
                raise StorageUnavailableError(
                    "PostgreSQL storage is unavailable: psycopg2 driver is not installed"
                ) from _storage_cause(exc)
            except StorageConfigurationError:
                if pg_store is not None:
                    pg_store.close()
                raise
            except Exception as exc:                    # noqa: BLE001
                if pg_store is not None:
                    pg_store.close()
                raise StorageUnavailableError(
                    f"PostgreSQL storage is unavailable: {type(exc).__name__}"
                ) from _storage_cause(exc)
        case "sqlite":
            sqlite_store = SQLiteStorage(cfg.sqlite_path)
            sqlite_store.ensure_schema()
            return sqlite_store
        case _:
            raise StorageConfigurationError(
                "MDFEED_STORAGE_BACKEND must be postgres or sqlite")
