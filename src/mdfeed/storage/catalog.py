from __future__ import annotations

from dataclasses import dataclass
from typing import Final

TIME_ENCODING: Final = "epoch_us <-> aware UTC datetime"
APPLICATION_TABLES: Final = (
    "instruments",
    "trades",
    "book_top",
    "bars_1m",
    "signals",
    "feed_stats",
    "latest",
    "quality_events",
)
RECEIPT_TABLES: Final = frozenset(
    {
        "ingest_batch_receipts",
        "migration_runs",
        "migration_checkpoints",
        "backup_restore_receipts",
        "archive_remote_receipts",
        "authoritative_gap_coverages",
        "gap_recovery_receipts",
        "data_gaps",
    }
)
SQLITE_RECEIPT_TABLES: Final = RECEIPT_TABLES
RAW_RETENTION_TABLES: Final = frozenset({"trades", "book_top"})


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    name: str
    sqlite_type: str
    postgres_type: str
    nullable: bool
    time_encoding: str | None = None


@dataclass(frozen=True, slots=True)
class TableCatalog:
    name: str
    columns: tuple[ColumnMapping, ...]
    primary_key: tuple[str, ...]
    natural_key: tuple[str, ...]
    verification_order: tuple[str, ...]
    raw_retention: bool = False

    @property
    def sqlite_columns(self) -> list[str]:
        return [column.name for column in self.columns]

    @property
    def postgres_columns(self) -> list[str]:
        return [column.name for column in self.columns]

    @property
    def timestamp_columns(self) -> tuple[str, ...]:
        return tuple(
            column.name for column in self.columns if column.time_encoding == TIME_ENCODING
        )

    def column(self, name: str) -> ColumnMapping:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(name)


def _col(
    name: str,
    sqlite_type: str,
    postgres_type: str,
    *,
    nullable: bool = False,
    time: bool = False,
) -> ColumnMapping:
    return ColumnMapping(
        name=name,
        sqlite_type=sqlite_type,
        postgres_type=postgres_type,
        nullable=nullable,
        time_encoding=TIME_ENCODING if time else None,
    )


CATALOG: Final = (
    TableCatalog(
        name="instruments",
        columns=(
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("base", "TEXT", "TEXT", nullable=True),
            _col("quote", "TEXT", "TEXT", nullable=True),
            _col("active", "INTEGER", "BOOLEAN"),
            _col("first_seen", "TEXT", "TIMESTAMPTZ", nullable=True, time=True),
            _col("last_seen", "TEXT", "TIMESTAMPTZ", nullable=True, time=True),
        ),
        primary_key=("venue", "symbol"),
        natural_key=("venue", "symbol"),
        verification_order=("venue", "symbol"),
    ),
    TableCatalog(
        name="trades",
        columns=(
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("price", "REAL", "DOUBLE PRECISION"),
            _col("qty", "REAL", "DOUBLE PRECISION"),
            _col("side", "INTEGER", "SMALLINT"),
            _col("recv_ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("latency_us", "INTEGER", "INTEGER", nullable=True),
            _col("seq", "INTEGER", "BIGINT", nullable=True),
        ),
        primary_key=(),
        natural_key=("venue", "symbol", "ts", "seq"),
        verification_order=("venue", "symbol", "ts", "seq"),
        raw_retention=True,
    ),
    TableCatalog(
        name="book_top",
        columns=(
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("bid", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("bid_qty", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("ask", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("ask_qty", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("spread_bp", "REAL", "DOUBLE PRECISION", nullable=True),
        ),
        primary_key=(),
        natural_key=("venue", "symbol", "ts"),
        verification_order=("venue", "symbol", "ts"),
        raw_retention=True,
    ),
    TableCatalog(
        name="bars_1m",
        columns=(
            _col("bucket", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("open", "REAL", "DOUBLE PRECISION"),
            _col("high", "REAL", "DOUBLE PRECISION"),
            _col("low", "REAL", "DOUBLE PRECISION"),
            _col("close", "REAL", "DOUBLE PRECISION"),
            _col("volume", "REAL", "DOUBLE PRECISION"),
            _col("notional", "REAL", "DOUBLE PRECISION"),
            _col("vwap", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("tick_count", "INTEGER", "INTEGER"),
        ),
        primary_key=("venue", "symbol", "bucket"),
        natural_key=("venue", "symbol", "bucket"),
        verification_order=("venue", "symbol", "bucket"),
    ),
    TableCatalog(
        name="signals",
        columns=(
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("strategy", "TEXT", "TEXT"),
            _col("action", "INTEGER", "SMALLINT"),
            _col("strength", "REAL", "DOUBLE PRECISION", nullable=True),
            _col("ref_price", "REAL", "DOUBLE PRECISION", nullable=True),
        ),
        primary_key=(),
        natural_key=("venue", "symbol", "strategy", "ts"),
        verification_order=("venue", "symbol", "strategy", "ts"),
    ),
    TableCatalog(
        name="feed_stats",
        columns=(
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("service", "TEXT", "TEXT"),
            _col("venue", "TEXT", "TEXT", nullable=True),
            _col("ticks", "INTEGER", "BIGINT", nullable=True),
            _col("latency_p50_us", "INTEGER", "INTEGER", nullable=True),
            _col("latency_p99_us", "INTEGER", "INTEGER", nullable=True),
            _col("gaps", "INTEGER", "INTEGER", nullable=True),
            _col("drops", "INTEGER", "INTEGER", nullable=True),
            _col("subscribers", "INTEGER", "INTEGER", nullable=True),
        ),
        primary_key=(),
        natural_key=("service", "venue", "ts"),
        verification_order=("service", "venue", "ts"),
    ),
    TableCatalog(
        name="latest",
        columns=(
            _col("venue", "TEXT", "TEXT"),
            _col("symbol", "TEXT", "TEXT"),
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("price", "REAL", "DOUBLE PRECISION"),
            _col("qty", "REAL", "DOUBLE PRECISION"),
            _col("side", "INTEGER", "SMALLINT"),
            _col("latency_us", "INTEGER", "BIGINT", nullable=True),
        ),
        primary_key=("venue", "symbol"),
        natural_key=("venue", "symbol"),
        verification_order=("venue", "symbol"),
    ),
    TableCatalog(
        name="quality_events",
        columns=(
            _col("ts", "INTEGER", "TIMESTAMPTZ", time=True),
            _col("check_name", "TEXT", "TEXT"),
            _col("severity", "TEXT", "TEXT"),
            _col("venue", "TEXT", "TEXT", nullable=True),
            _col("symbol", "TEXT", "TEXT", nullable=True),
            _col("detail", "TEXT", "TEXT", nullable=True),
            _col("value", "REAL", "DOUBLE PRECISION", nullable=True),
        ),
        primary_key=(),
        natural_key=("check_name", "severity", "venue", "symbol", "ts"),
        verification_order=("check_name", "severity", "venue", "symbol", "ts"),
    ),
)
CATALOG_BY_NAME: Final = {table.name: table for table in CATALOG}


def catalog_table(name: str) -> TableCatalog:
    return CATALOG_BY_NAME[name]
