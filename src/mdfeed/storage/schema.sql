-- MDFeed PostgreSQL 스키마
-- TimescaleDB 확장이 있으면 하이퍼테이블로, 없으면 일반 파티션 없는 테이블로 동작한다.
-- psql -f schema.sql 로 멱등 실행 가능.

CREATE TABLE IF NOT EXISTS venues (
    code        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    asset_class TEXT NOT NULL,              -- CRYPTO / EQUITY / FX
    tz          TEXT NOT NULL DEFAULT 'UTC',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO venues (code,name,asset_class) VALUES
('UPBIT','Upbit','CRYPTO'),
('BINANCE','Binance','CRYPTO'),
('KIS','한국투자증권','EQUITY'),
('KRX','Korea Exchange','EQUITY'),
('KRX-IDX','Korea Exchange Index','EQUITY'),
('RATE','Korea Interest Rate','FX')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS instruments (
    venue       TEXT NOT NULL REFERENCES venues(code) ON DELETE CASCADE,
    symbol      TEXT NOT NULL,
    base        TEXT,
    quote       TEXT,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen  TIMESTAMPTZ,
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
-- 보존 삭제(`WHERE ts < ...`)용. 복합 인덱스는 선두가 venue 라 못 탄다.
CREATE INDEX IF NOT EXISTS idx_book_ts ON book_top (ts DESC);

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
    ts              TIMESTAMPTZ,
    service         TEXT        NOT NULL,
    venue           TEXT,
    ticks           BIGINT,
    latency_p50_us  INTEGER,
    latency_p99_us  INTEGER,
    gaps            INTEGER,
    drops           INTEGER,
    subscribers     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_feed_stats_ts ON feed_stats (ts DESC);

-- ── TimescaleDB (있으면 적용) ────────────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        PERFORM create_hypertable('trades',   'ts',     if_not_exists => TRUE);
        PERFORM create_hypertable('book_top', 'ts',     if_not_exists => TRUE);
        PERFORM create_hypertable('bars_1m',  'bucket', if_not_exists => TRUE);
    END IF;
END $$;

-- 종목별 최신 시세. 뷰로 매번 계산하면 조회 비용이 누적 행수에 비례한다.
-- 실측(SQLite, 274만 행): 뷰 1,299ms → 테이블 조회는 종목 수에만 비례한다.
CREATE TABLE IF NOT EXISTS latest (
    venue       TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    ts          TIMESTAMPTZ      NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    qty         DOUBLE PRECISION NOT NULL,
    side        SMALLINT         NOT NULL DEFAULT 0,
    latency_us  BIGINT,
    PRIMARY KEY (venue, symbol)
);

CREATE TABLE IF NOT EXISTS quality_events (
    ts          TIMESTAMPTZ      NOT NULL,
    check_name  TEXT             NOT NULL,
    severity    TEXT             NOT NULL,
    venue       TEXT,
    symbol      TEXT,
    detail      TEXT,
    value       DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_quality_ts ON quality_events (ts DESC);

CREATE TABLE IF NOT EXISTS ingest_batch_receipts (
    receipt_id  TEXT PRIMARY KEY,
    service     TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    batch_hash  TEXT        NOT NULL,
    row_count   BIGINT      NOT NULL CHECK (row_count >= 0),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS migration_runs (
    run_id             TEXT PRIMARY KEY,
    source_fingerprint TEXT        NOT NULL,
    state              TEXT        NOT NULL CHECK (state IN ('RUNNING','COMPLETED','FAILED')),
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS migration_checkpoints (
    run_id             TEXT        NOT NULL REFERENCES migration_runs(run_id) ON DELETE CASCADE,
    table_name         TEXT        NOT NULL,
    first_source_rowid BIGINT      NOT NULL CHECK (first_source_rowid >= 1),
    last_source_rowid  BIGINT      NOT NULL CHECK (last_source_rowid >= 0),
    chunk_row_count    BIGINT      NOT NULL CHECK (chunk_row_count >= 0),
    row_count          BIGINT      NOT NULL CHECK (row_count >= 0),
    chunk_hash         TEXT        NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, table_name)
);

CREATE TABLE IF NOT EXISTS backup_restore_receipts (
    receipt_id    TEXT PRIMARY KEY,
    backup_id     TEXT        NOT NULL,
    manifest_hash TEXT        NOT NULL,
    pg_dump_hash  TEXT        NOT NULL,
    verified_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT        NOT NULL CHECK (status IN ('VERIFIED','FAILED'))
);

CREATE TABLE IF NOT EXISTS archive_remote_receipts (
    receipt_id    TEXT PRIMARY KEY,
    object_id     TEXT        NOT NULL,
    table_name    TEXT,
    day           DATE,
    local_sha256  TEXT        NOT NULL,
    local_bytes   BIGINT      NOT NULL CHECK (local_bytes >= 0),
    local_rows    BIGINT      NOT NULL CHECK (local_rows >= 0),
    remote_sha256 TEXT        NOT NULL,
    remote_bytes  BIGINT      NOT NULL CHECK (remote_bytes >= 0),
    remote_rows   BIGINT      NOT NULL CHECK (remote_rows >= 0),
    verified_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    transport     TEXT        NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('VERIFIED','FAILED'))
);


CREATE TABLE IF NOT EXISTS data_gaps (
    gap_id              TEXT PRIMARY KEY,
    state               TEXT        NOT NULL CHECK (state IN ('OPEN','ENDED_UNRECOVERED','BACKFILLING','RECOVERED')),
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ,
    recovered_at        TIMESTAMPTZ,
    required_scope_hash TEXT        NOT NULL,
    required_scope_json JSONB       NOT NULL,
    backfill_receipt_id TEXT,
    CHECK (
        (state = 'OPEN' AND ended_at IS NULL AND recovered_at IS NULL AND backfill_receipt_id IS NULL)
        OR (state = 'ENDED_UNRECOVERED' AND ended_at IS NOT NULL AND recovered_at IS NULL)
        OR (state = 'BACKFILLING' AND ended_at IS NOT NULL AND recovered_at IS NULL)
        OR (state = 'RECOVERED' AND ended_at IS NOT NULL AND recovered_at IS NOT NULL AND backfill_receipt_id IS NOT NULL)
    )
);


CREATE TABLE IF NOT EXISTS authoritative_gap_coverages (
    coverage_id         TEXT PRIMARY KEY,
    covered_start       TIMESTAMPTZ NOT NULL,
    covered_end         TIMESTAMPTZ NOT NULL,
    scope_hash          TEXT        NOT NULL,
    scope_json          JSONB       NOT NULL,
    source_fingerprint  TEXT        NOT NULL,
    table_counts_json   JSONB       NOT NULL,
    evidence_hash       TEXT        NOT NULL,
    producer            TEXT        NOT NULL,
    verified_at         TIMESTAMPTZ NOT NULL,
    status              TEXT        NOT NULL CHECK (status IN ('VERIFIED','FAILED'))
);

CREATE TABLE IF NOT EXISTS gap_recovery_receipts (
    receipt_id                 TEXT PRIMARY KEY,
    gap_id                     TEXT        NOT NULL REFERENCES data_gaps(gap_id) ON DELETE CASCADE,
    covered_start              TIMESTAMPTZ NOT NULL,
    covered_end                TIMESTAMPTZ NOT NULL,
    scope_hash                 TEXT        NOT NULL,
    scope_json                 JSONB       NOT NULL,
    actual_reconciliation_id   TEXT        NOT NULL REFERENCES migration_runs(run_id),
    authoritative_coverage_id  TEXT        NOT NULL REFERENCES authoritative_gap_coverages(coverage_id),
    missing_intervals          BIGINT      NOT NULL CHECK (missing_intervals >= 0),
    missing_rows               BIGINT      NOT NULL CHECK (missing_rows >= 0),
    evidence_hash              TEXT        NOT NULL,
    verified_at                TIMESTAMPTZ NOT NULL,
    status                     TEXT        NOT NULL CHECK (status IN ('VERIFIED','FAILED'))
);

-- ── 조회 뷰 ──────────────────────────────────────────────────────────────
-- 뷰는 남긴다. 임시 조회와 과거 호환용이고, 서비스 경로에서는 쓰지 않는다.
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
