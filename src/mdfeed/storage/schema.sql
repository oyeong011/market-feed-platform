-- MDFeed PostgreSQL 스키마
-- TimescaleDB 확장이 있으면 하이퍼테이블로, 없으면 일반 파티션 없는 테이블로 동작한다.
-- psql -f schema.sql 로 멱등 실행 가능.

-- ── 표시 시간대 ────────────────────────
-- TIMESTAMPTZ 는 UTC 로 저장되고 조회 시 세션 시간대로 변환된다. 기본값이 UTC 라
-- 국내 장 시간(09:00~15:30 KST)을 조회하면 00:00~06:30 으로 보인다. 장 시작 전인지
-- 마감 후인지 눈으로 판단할 수 없어 운영 조회에서 실수가 난다.
-- 저장 값은 그대로이고 표시만 바뀜다.
DO $tz$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''Asia/Seoul''', current_database());
END
$tz$;
SET timezone TO 'Asia/Seoul';

CREATE TABLE IF NOT EXISTS venues (
    code        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    asset_class TEXT NOT NULL,              -- CRYPTO / EQUITY / FX
    tz          TEXT NOT NULL DEFAULT 'UTC',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    venue       TEXT NOT NULL REFERENCES venues(code) ON DELETE CASCADE,
    symbol      TEXT NOT NULL,
    base        TEXT,
    quote       TEXT,
    tick_size   NUMERIC(20,10),
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ,
    PRIMARY KEY (venue, symbol)
);

-- ── 체결(틱) ─────────────────────────────────────────────────────────────
-- 가장 크게 자라는 테이블. 파티셔닝/보존정책의 대상이다.
CREATE TABLE IF NOT EXISTS trades (
    ts          TIMESTAMPTZ      NOT NULL,   -- 거래소 체결 시각
    venue       TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    qty         DOUBLE PRECISION NOT NULL,
    side        SMALLINT         NOT NULL DEFAULT 0,   -- 1 매수, 2 매도
    recv_ts     TIMESTAMPTZ      NOT NULL,   -- 우리가 받은 시각
    latency_us  INTEGER,                     -- 시계 보정된 수집 지연
    seq         BIGINT
);

-- 조회 패턴은 사실상 "특정 종목의 특정 기간"이다. (venue,symbol,ts) 복합 인덱스가
-- 단일 컬럼 인덱스 3개보다 훨씬 낫고, ts DESC 로 최신 조회를 인덱스만으로 끝낸다.
CREATE INDEX IF NOT EXISTS idx_trades_sym_ts ON trades (venue, symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_ts     ON trades (ts DESC);

-- ── 최우선호가 ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS book_top (
    ts          TIMESTAMPTZ      NOT NULL,
    venue       TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    bid         DOUBLE PRECISION,
    bid_qty     DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    ask_qty     DOUBLE PRECISION,
    spread_bp   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_book_sym_ts ON book_top (venue, symbol, ts DESC);

-- ── 1분봉 ────────────────────────────────────────────────────────────────
-- 틱을 그대로 조회하면 대시보드/백테스트가 매번 수백만 행을 스캔한다.
-- 적재 시점에 집계해 두는 편이 압도적으로 싸다(사전 집계, pre-aggregation).
CREATE TABLE IF NOT EXISTS bars_1m (
    bucket      TIMESTAMPTZ      NOT NULL,
    venue       TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    notional    DOUBLE PRECISION NOT NULL,
    vwap        DOUBLE PRECISION,
    tick_count  INTEGER          NOT NULL,
    PRIMARY KEY (venue, symbol, bucket)
);
CREATE INDEX IF NOT EXISTS idx_bars_bucket ON bars_1m (bucket DESC);

-- ── 전략 시그널 ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals (
    ts          TIMESTAMPTZ      NOT NULL,
    venue       TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    strategy    TEXT             NOT NULL,
    action      SMALLINT         NOT NULL,   -- 1 매수, -1 매도
    strength    DOUBLE PRECISION,
    ref_price   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals (ts DESC);

-- ── 피드 운영 지표 ───────────────────────────────────────────────────────
-- 장애 사후분석의 근거. "그 시각에 지연이 얼마였나"를 로그 뒤지지 않고 SQL로 답한다.
CREATE TABLE IF NOT EXISTS feed_stats (
    ts              TIMESTAMPTZ NOT NULL,
    service         TEXT        NOT NULL,
    venue           TEXT,
    ticks           BIGINT,
    latency_p50_us  INTEGER,
    latency_p99_us  INTEGER,
    gaps            INTEGER,
    drops           INTEGER,
    subscribers     INTEGER,
    PRIMARY KEY (ts, service, venue)
);

-- ── TimescaleDB (있으면 적용) ────────────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        PERFORM create_hypertable('trades',   'ts',     if_not_exists => TRUE);
        PERFORM create_hypertable('book_top', 'ts',     if_not_exists => TRUE);
        PERFORM create_hypertable('bars_1m',  'bucket', if_not_exists => TRUE);
        -- 틱은 30일만 보관. 1분봉은 영구 보관(용량 차이가 수백 배다)
        PERFORM add_retention_policy('trades',   INTERVAL '30 days', if_not_exists => TRUE);
        PERFORM add_retention_policy('book_top', INTERVAL '7 days',  if_not_exists => TRUE);
    END IF;
END $$;

-- ── 조회 뷰 ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_latest AS
SELECT DISTINCT ON (venue, symbol)
       venue, symbol, ts, price, qty, side, latency_us
FROM trades
ORDER BY venue, symbol, ts DESC;

CREATE OR REPLACE VIEW v_daily_ohlcv AS
SELECT venue, symbol,
       date_trunc('day', bucket)                    AS day,
       (array_agg(open  ORDER BY bucket ASC ))[1]   AS open,
       MAX(high)                                    AS high,
       MIN(low)                                     AS low,
       (array_agg(close ORDER BY bucket DESC))[1]   AS close,
       SUM(volume)                                  AS volume,
       SUM(notional)                                AS notional,
       CASE WHEN SUM(volume) > 0
            THEN SUM(notional) / SUM(volume) END    AS vwap
FROM bars_1m
GROUP BY venue, symbol, date_trunc('day', bucket);

-- 유동성 품질: 스프레드가 넓어지는 구간은 체결 비용이 뛰는 구간이다
CREATE OR REPLACE VIEW v_spread_hourly AS
SELECT venue, symbol,
       date_trunc('hour', ts) AS hour,
       AVG(spread_bp)         AS avg_spread_bp,
       MAX(spread_bp)         AS max_spread_bp,
       COUNT(*)               AS samples
FROM book_top
WHERE spread_bp IS NOT NULL AND spread_bp BETWEEN 0 AND 1000
GROUP BY venue, symbol, date_trunc('hour', ts);

-- 수집 품질: 분당 틱 수가 갑자기 0이 되는 구간을 찾는다(피드 끊김 탐지)
CREATE OR REPLACE VIEW v_feed_gaps AS
SELECT venue, symbol, bucket,
       tick_count,
       LAG(tick_count) OVER (PARTITION BY venue, symbol ORDER BY bucket) AS prev_ticks
FROM bars_1m;
