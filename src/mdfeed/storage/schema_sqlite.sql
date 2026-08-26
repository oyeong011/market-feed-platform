-- MDFeed SQLite 스키마 (로컬 개발 · CI · 단일 노드 배포용)
-- Postgres 스키마와 컬럼 이름·의미를 1:1로 맞춰, 애플리케이션 쿼리를 공유한다.

PRAGMA journal_mode = WAL;      -- 읽는 동안 쓰기가 막히지 않게
PRAGMA synchronous  = NORMAL;   -- 시세 데이터는 재수집 가능하므로 fsync를 늦춘다

CREATE TABLE IF NOT EXISTS instruments (
    venue      TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    base       TEXT,
    quote      TEXT,
    active     INTEGER NOT NULL DEFAULT 1,
    first_seen TEXT,
    last_seen  TEXT,
    PRIMARY KEY (venue, symbol)
);

CREATE TABLE IF NOT EXISTS trades (
    ts         INTEGER NOT NULL,        -- epoch microsecond
    venue      TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    price      REAL    NOT NULL,
    qty        REAL    NOT NULL,
    side       INTEGER NOT NULL DEFAULT 0,
    recv_ts    INTEGER NOT NULL,
    latency_us INTEGER,
    seq        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_trades_sym_ts ON trades (venue, symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_ts     ON trades (ts DESC);

CREATE TABLE IF NOT EXISTS book_top (
    ts        INTEGER NOT NULL,
    venue     TEXT    NOT NULL,
    symbol    TEXT    NOT NULL,
    bid       REAL, bid_qty REAL, ask REAL, ask_qty REAL,
    spread_bp REAL
);
CREATE INDEX IF NOT EXISTS idx_book_sym_ts ON book_top (venue, symbol, ts DESC);

CREATE TABLE IF NOT EXISTS bars_1m (
    bucket     INTEGER NOT NULL,
    venue      TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL NOT NULL, notional REAL NOT NULL, vwap REAL,
    tick_count INTEGER NOT NULL,
    PRIMARY KEY (venue, symbol, bucket)
);
CREATE INDEX IF NOT EXISTS idx_bars_bucket ON bars_1m (bucket DESC);

CREATE TABLE IF NOT EXISTS signals (
    ts        INTEGER NOT NULL,
    venue     TEXT    NOT NULL,
    symbol    TEXT    NOT NULL,
    strategy  TEXT    NOT NULL,
    action    INTEGER NOT NULL,
    strength  REAL,
    ref_price REAL
);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals (ts DESC);

CREATE TABLE IF NOT EXISTS feed_stats (
    ts INTEGER NOT NULL, service TEXT NOT NULL, venue TEXT,
    ticks INTEGER, latency_p50_us INTEGER, latency_p99_us INTEGER,
    gaps INTEGER, drops INTEGER, subscribers INTEGER,
    PRIMARY KEY (ts, service, venue)
);

CREATE VIEW IF NOT EXISTS v_latest AS
SELECT venue, symbol, MAX(ts) AS ts, price, qty, side, latency_us
FROM trades GROUP BY venue, symbol;
