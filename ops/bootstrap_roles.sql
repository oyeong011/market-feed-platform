DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mdfeed_runtime') THEN
    CREATE ROLE mdfeed_runtime LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mdfeed_backup') THEN
    CREATE ROLE mdfeed_backup LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mdfeed_maintenance') THEN
    CREATE ROLE mdfeed_maintenance LOGIN CREATEDB;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mdfeed_verifier') THEN
    CREATE ROLE mdfeed_verifier LOGIN;
  END IF;
END
$$;

GRANT CONNECT ON DATABASE mdfeed TO mdfeed_runtime, mdfeed_backup, mdfeed_maintenance, mdfeed_verifier;
GRANT USAGE ON SCHEMA public TO mdfeed_runtime, mdfeed_backup, mdfeed_maintenance, mdfeed_verifier;
REVOKE CREATE ON SCHEMA public FROM mdfeed_runtime;

GRANT SELECT, INSERT, UPDATE, DELETE
  ON trades, book_top, bars_1m, signals, feed_stats, latest, quality_events
  TO mdfeed_runtime;
GRANT SELECT ON instruments, venues TO mdfeed_runtime;
GRANT SELECT, INSERT ON ingest_batch_receipts TO mdfeed_runtime;
GRANT SELECT, INSERT, UPDATE ON data_gaps TO mdfeed_runtime;
GRANT SELECT
  ON migration_runs, migration_checkpoints, backup_restore_receipts,
     archive_remote_receipts, authoritative_gap_coverages, gap_recovery_receipts
  TO mdfeed_runtime;
GRANT SELECT
  ON v_latest, v_daily_ohlcv, v_spread_hourly, v_feed_gaps
  TO mdfeed_runtime;

GRANT SELECT
  ON instruments, trades, book_top, bars_1m, signals, feed_stats, latest, quality_events
  TO mdfeed_backup;

GRANT SELECT
  ON instruments, trades, book_top, bars_1m, signals, feed_stats, latest, quality_events
  TO mdfeed_maintenance;
GRANT SELECT ON backup_restore_receipts, archive_remote_receipts, gap_recovery_receipts TO mdfeed_maintenance;
GRANT SELECT, INSERT, UPDATE ON data_gaps TO mdfeed_maintenance;
GRANT TEMPORARY ON DATABASE mdfeed TO mdfeed_maintenance;

GRANT SELECT, INSERT
  ON backup_restore_receipts, archive_remote_receipts, gap_recovery_receipts
  TO mdfeed_verifier;
GRANT SELECT ON migration_runs, migration_checkpoints TO mdfeed_verifier;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO mdfeed_runtime, mdfeed_backup, mdfeed_maintenance, mdfeed_verifier;
