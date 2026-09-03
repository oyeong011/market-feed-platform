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
-- 보존 삭제는 `WHERE ts < ?` 로 지운다. 위 복합 인덱스는 선두가 venue 라
-- 이 조건을 못 탄다 — EXPLAIN 이 SCAN 을 냈다. trades 는 idx_trades_ts 가
-- 있어 SEARCH 인데 book_top 만 배치마다 전체 스캔이었다.
CREATE INDEX IF NOT EXISTS idx_book_ts ON book_top (ts DESC);

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

-- 종목별 최신 시세. 뷰(v_latest)로 MAX(ts) 를 매번 계산하면 조회 비용이
-- 누적 행수에 비례한다. 실측: 274만 행에서 1,299ms, /api/v1/quotes 가 358ms.
-- 마켓데이터에서 가장 자주 쓰는 조회가 히스토리가 쌓일수록 느려지면 안 된다.
-- 적재할 때 종목당 한 줄만 갱신하면 조회는 종목 수에 비례한다(수천 행).
CREATE TABLE IF NOT EXISTS latest (
    venue      TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    ts         INTEGER NOT NULL,
    price      REAL    NOT NULL,
    qty        REAL    NOT NULL,
    side       INTEGER NOT NULL DEFAULT 0,
    latency_us INTEGER,
    PRIMARY KEY (venue, symbol)
);

-- 뷰는 남긴다. 임시 조회와 과거 호환용이고, 서비스 경로에서는 쓰지 않는다.
CREATE VIEW IF NOT EXISTS v_latest AS
SELECT venue, symbol, MAX(ts) AS ts, price, qty, side, latency_us
FROM trades GROUP BY venue, symbol;
