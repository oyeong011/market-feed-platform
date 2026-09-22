import asyncio
from pathlib import Path

from pytest import MonkeyPatch

from mdfeed import retention
from mdfeed.config import Config
from mdfeed.models import MSG_TRADE, Trade
from mdfeed.protocol import Frame
from mdfeed.services.writer import Writer
from mdfeed.storage.db import BatchWriteReceipt, Storage, StorageBatch


class FailingOnceStorage(Storage):
    kind = "fake"

    def __init__(self) -> None:
        self.calls: int = 0
        self.batches: list[StorageBatch] = []

    def write_batch(self, batch: StorageBatch) -> BatchWriteReceipt:
        self.calls += 1
        self.batches.append(batch)
        if self.calls == 1:
            raise RuntimeError("synthetic write failure")
        return BatchWriteReceipt(batch.batch_id, batch.digest, batch.rows_written, 0)


def test_writer_retains_unacknowledged_snapshot_when_flush_fails():
    cfg = Config()
    cfg.storage_backend = "sqlite"
    cfg.storage_profile = "test"
    writer = Writer(cfg)
    storage = FailingOnceStorage()
    writer.storage = storage
    writer._trades.append((1, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, 1, 1, 1))

    asyncio.run(writer._flush())

    assert writer._retry_batch is not None
    assert writer._retry_batch.trades == (storage.batches[0].trades[0],)
    assert writer.rows_written == 0
    assert writer.db_errors == 1


def test_writer_retries_same_batch_digest_and_preserves_new_arrivals():
    cfg = Config()
    cfg.storage_backend = "sqlite"
    cfg.storage_profile = "test"
    writer = Writer(cfg)
    storage = FailingOnceStorage()
    writer.storage = storage
    first = (1, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, 1, 1, 1)
    second = (2, "UPBIT", "KRW-ETH", 101.0, 1.0, 1, 2, 1, 2)
    writer._trades.append(first)

    asyncio.run(writer._flush())
    first_batch = storage.batches[0]
    writer._trades.append(second)
    asyncio.run(writer._flush())

    assert storage.batches[1].digest == first_batch.digest
    assert writer.rows_written == 1
    assert writer._trades == [second]


def test_writer_bounds_new_arrivals_during_prolonged_outage():
    cfg = Config()
    cfg.storage_backend = "sqlite"
    cfg.storage_profile = "test"
    cfg.writer_pending_max_rows = 1
    writer = Writer(cfg)
    storage = FailingOnceStorage()
    writer.storage = storage
    writer._trades.append((1, "UPBIT", "KRW-BTC", 100.0, 1.0, 1, 1, 1, 1))

    asyncio.run(writer._flush())
    frame = Frame(
        MSG_TRADE,
        2,
        0,
        Trade("UPBIT", "KRW-ETH", 2_000, 3_000, 101.0, 1.0, 1).pack(),
    )
    writer._ingest(frame)

    assert writer._retry_batch is not None
    assert len(writer._retry_batch.trades) == 1
    assert writer._trades == []
    assert writer.dropped_rows == 1
    assert writer.health()["pending_backpressure"] is True


def test_writer_archive_floor_uses_remote_retention_cutoff(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
):
    cfg = Config()
    cfg.storage_backend = "sqlite"
    cfg.storage_profile = "test"
    cfg.archive_dir = str(tmp_path)
    cfg.retention_requires_archive = True
    writer = Writer(cfg)
    writer.storage = FailingOnceStorage()
    calls: list[tuple[Storage, str]] = []

    def fake_retention_cutoff(
        storage: Storage,
        remote_receipt_dir: str,
    ) -> retention.RetentionCutoff:
        calls.append((storage, remote_receipt_dir))
        return retention.RetentionCutoff(True, 123_000_000, "verified_remote_coverage")

    monkeypatch.setattr(retention, "retention_cutoff", fake_retention_cutoff)

    assert writer._archive_floor_us() == 123_000_000
    assert calls == [(writer.storage, str(tmp_path))]


def test_writer_archive_floor_blocks_prune_when_remote_proof_missing(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
):
    cfg = Config()
    cfg.storage_backend = "sqlite"
    cfg.storage_profile = "test"
    cfg.archive_dir = str(tmp_path)
    cfg.retention_requires_archive = True
    writer = Writer(cfg)
    writer.storage = FailingOnceStorage()

    def fake_retention_cutoff(
        storage: Storage,
        remote_receipt_dir: str,
    ) -> retention.RetentionCutoff:
        return retention.RetentionCutoff(False, 0, "missing_remote_coverage")

    monkeypatch.setattr(retention, "retention_cutoff", fake_retention_cutoff)

    assert writer._archive_floor_us() == 0
