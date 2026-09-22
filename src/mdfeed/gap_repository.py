from __future__ import annotations

import hashlib
import json
import os
from typing import Iterable

from .migration.postgres_target import canonical_postgres_digest_from_connection

from .gaps import (
    BackfillReceipt,
    GapRecord,
    GapTransitionError,
    GapRepository,
    GapScope,
    GapState,
    TrustedReceiptStore,
    _coerce_utc,
    _format_utc,
    _scope_hash,
    known_incident,
    parse_utc,
)


class MemoryGapRepository:
    def __init__(self, records: Iterable[GapRecord] = ()):
        self._records = {record.id: record for record in records}

    def list(self) -> list[GapRecord]:
        return sorted(self._records.values(), key=lambda gap: gap.started_at)

    def get(self, gap_id: str) -> GapRecord | None:
        return self._records.get(gap_id)

    def save(self, record: GapRecord) -> None:
        self._records[record.id] = record


class FileGapRepository(MemoryGapRepository):
    def __init__(self, path: str, seeds: Iterable[GapRecord] = ()):
        self.path = path
        records = list(seeds)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                raw = json.load(fh)
            records.extend(_record_from_dict(item) for item in raw.get("records", []))
        super().__init__(_dedupe(records))

    def save(self, record: GapRecord) -> None:
        super().save(record)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"schema_version": 1, "records": [
                _record_to_dict(item) for item in self.list()
            ]}, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, self.path)


class StorageGapRepository:
    def __init__(self, storage, seeds: Iterable[GapRecord] = ()):
        self.storage = storage
        self.seeds = {record.id: record for record in seeds}
        self._seed()

    def list(self) -> list[GapRecord]:
        rows = self.storage.query(
            "SELECT gap_id, state, started_at, ended_at, recovered_at, "
            "required_scope_hash, required_scope_json, backfill_receipt_id "
            "FROM data_gaps ORDER BY started_at"
        )
        return [self._from_row(row) for row in rows]

    def get(self, gap_id: str) -> GapRecord | None:
        p = self.storage.placeholder
        rows = self.storage.query(
            "SELECT gap_id, state, started_at, ended_at, recovered_at, "
            "required_scope_hash, required_scope_json, backfill_receipt_id "
            f"FROM data_gaps WHERE gap_id={p}",
            (gap_id,),
        )
        if not rows:
            return None
        return self._from_row(rows[0])

    def save(self, record: GapRecord) -> None:
        p = self.storage.placeholder
        self.storage.execute(
            "INSERT INTO data_gaps "
            "(gap_id,state,started_at,ended_at,recovered_at,required_scope_hash,"
            "required_scope_json,backfill_receipt_id) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p}) "
            "ON CONFLICT(gap_id) DO UPDATE SET "
            "state=excluded.state, ended_at=excluded.ended_at, recovered_at=excluded.recovered_at, "
            "required_scope_hash=excluded.required_scope_hash, "
            "required_scope_json=excluded.required_scope_json, "
            "backfill_receipt_id=excluded.backfill_receipt_id",
            _record_params(record),
        )

    def _seed(self) -> None:
        for record in self.seeds.values():
            self._insert_seed(record)

    def _insert_seed(self, record: GapRecord) -> None:
        p = self.storage.placeholder
        self.storage.execute(
            "INSERT INTO data_gaps "
            "(gap_id,state,started_at,ended_at,recovered_at,required_scope_hash,"
            "required_scope_json,backfill_receipt_id) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p}) "
            "ON CONFLICT(gap_id) DO NOTHING",
            _record_params(record),
        )

    def _from_row(self, row: dict) -> GapRecord:
        seed = self.seeds.get(str(row["gap_id"]))
        scope = _scope_from_stored(row.get("required_scope_json"), seed)
        if row.get("required_scope_hash") and str(row["required_scope_hash"]) != _scope_hash(scope):
            raise _incomplete("stored required scope hash mismatch")
        reason = seed.reason if seed else "stored data gap requires verified backfill"
        return GapRecord(
            id=str(row["gap_id"]),
            started_at=_coerce_utc(row["started_at"]),
            ended_at=_coerce_utc(row["ended_at"]) if row.get("ended_at") else None,
            state=GapState(str(row["state"])),
            required_scope=scope,
            reason=reason,
            backfill_receipt_id=row.get("backfill_receipt_id"),
            recovered_at=_coerce_utc(row["recovered_at"]) if row.get("recovered_at") else None,
            reconciliation_result=self._reconciliation_result(row.get("backfill_receipt_id")),
        )

    def _reconciliation_result(self, receipt_id) -> str | None:
        if not receipt_id:
            return None
        p = self.storage.placeholder
        rows = self.storage.query(
            f"SELECT actual_reconciliation_id FROM gap_recovery_receipts "
            f"WHERE receipt_id={p} AND status='VERIFIED'",
            (str(receipt_id),),
        )
        return str(rows[0]["actual_reconciliation_id"]) if rows else None


class StorageTrustedReceiptStore:
    def __init__(self, storage):
        self.storage = storage

    def get_verified(self, receipt_id: str) -> BackfillReceipt | None:
        p = self.storage.placeholder
        rows = self.storage.query(
            "SELECT receipt_id, gap_id, covered_start, covered_end, scope_hash, scope_json, "
            "actual_reconciliation_id, authoritative_coverage_id, missing_intervals, "
            f"missing_rows, evidence_hash, verified_at FROM gap_recovery_receipts "
            f"WHERE receipt_id={p} AND status='VERIFIED'",
            (receipt_id,),
        )
        if not rows:
            return None
        try:
            return _trusted_receipt_from_row(self.storage, rows[0])
        except GapTransitionError:
            return None


def record_trusted_coverage(
    storage,
    *,
    coverage_id: str,
    covered_start,
    covered_end,
    scope: GapScope,
    source_fingerprint: str,
    table_counts: dict[str, int],
    producer: str,
    verified_at,
) -> None:
    payload = {
        "coverage_id": coverage_id,
        "covered_start": _format_utc(covered_start),
        "covered_end": _format_utc(covered_end),
        "scope_hash": _scope_hash(scope),
        "source_fingerprint": source_fingerprint,
        "table_counts": table_counts,
        "producer": producer,
        "verified_at": _format_utc(verified_at),
    }
    p = storage.placeholder
    storage.execute(
        "INSERT INTO authoritative_gap_coverages "
        "(coverage_id,covered_start,covered_end,scope_hash,scope_json,"
        "source_fingerprint,table_counts_json,evidence_hash,producer,verified_at,status) "
        f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p}) "
        "ON CONFLICT(coverage_id) DO UPDATE SET status=excluded.status",
        (
            coverage_id, _format_utc(covered_start), _format_utc(covered_end),
            _scope_hash(scope), _scope_json(scope), source_fingerprint,
            _json(table_counts), _hash_json(payload), producer, _format_utc(verified_at),
            "VERIFIED",
        ),
    )


def record_trusted_receipt(
    storage,
    *,
    gap_id: str,
    receipt_id: str,
    actual_reconciliation_id: str,
    authoritative_coverage_id: str,
    verified_at,
) -> BackfillReceipt:
    record = StorageGapRepository(storage, ()).get(gap_id)
    if record is None or record.ended_at is None:
        raise _incomplete("gap must exist with a closed interval")
    _validate_authorities(storage, record, actual_reconciliation_id, authoritative_coverage_id)
    receipt = BackfillReceipt(
        receipt_id, record.started_at, record.ended_at, record.required_scope,
        "stored_reconciliation", actual_reconciliation_id, authoritative_coverage_id,
        0, 0, parse_utc(_format_utc(verified_at)),
    )
    p = storage.placeholder
    storage.execute(
        "INSERT INTO gap_recovery_receipts "
        "(receipt_id,gap_id,covered_start,covered_end,scope_hash,scope_json,"
        "actual_reconciliation_id,authoritative_coverage_id,missing_intervals,"
        "missing_rows,evidence_hash,verified_at,status) "
        f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p}) ",
        (
            receipt.id, gap_id, _format_utc(receipt.covered_start),
            _format_utc(receipt.covered_end), _scope_hash(receipt.scope),
            _scope_json(receipt.scope), actual_reconciliation_id, authoritative_coverage_id,
            0, 0, _receipt_hash(receipt), _format_utc(receipt.verified_at), "VERIFIED",
        ),
    )
    return receipt

class MemoryTrustedReceiptStore:
    def __init__(self, receipts: Iterable[BackfillReceipt] = ()):
        self._receipts = {receipt.id: receipt for receipt in receipts}

    def get_verified(self, receipt_id: str) -> BackfillReceipt | None:
        return self._receipts.get(receipt_id)


def open_repository(state_file: str | None, storage, seeds: list[GapRecord]) -> GapRepository:
    if storage is not None:
        return StorageGapRepository(storage, seeds)
    if state_file:
        return FileGapRepository(state_file, seeds)
    env_path = os.getenv("MDFEED_GAP_STATE_FILE", "")
    if env_path:
        return FileGapRepository(env_path, seeds)
    return MemoryGapRepository(seeds)


def load_seed_records(root: str | None = None) -> list[GapRecord]:
    base = root or os.path.join(os.getcwd(), "ops", "incidents")
    records: list[GapRecord] = []
    if not os.path.isdir(base):
        return [known_incident()]
    for name in sorted(os.listdir(base)):
        if name.endswith(".json"):
            with open(os.path.join(base, name), encoding="utf-8") as fh:
                records.append(_record_from_dict(json.load(fh)))
    return records or [known_incident()]


def _dedupe(records: Iterable[GapRecord]) -> list[GapRecord]:
    out: dict[str, GapRecord] = {}
    for record in records:
        out[record.id] = record
    return list(out.values())


def _gap_for_validation(storage, gap_id: str) -> GapRecord:
    p = storage.placeholder
    rows = storage.query(
        "SELECT gap_id, state, started_at, ended_at, recovered_at, "
        "required_scope_json, backfill_receipt_id FROM data_gaps "
        f"WHERE gap_id={p}",
        (gap_id,),
    )
    if not rows or not rows[0].get("ended_at"):
        raise _incomplete("receipt gap is missing or open")
    row = rows[0]
    return GapRecord(
        str(row["gap_id"]), _coerce_utc(row["started_at"]), _coerce_utc(row["ended_at"]),
        GapState(str(row["state"])), _scope_from_stored(row["required_scope_json"], None),
        "stored data gap requires verified backfill", row.get("backfill_receipt_id"),
        _coerce_utc(row["recovered_at"]) if row.get("recovered_at") else None, None,
    )


def _trusted_receipt_from_row(storage, row: dict) -> BackfillReceipt:
    receipt = _receipt_from_row(row)
    if receipt is None:
        raise _incomplete("stored recovery receipt hash mismatch")
    record = _gap_for_validation(storage, str(row["gap_id"]))
    _validate_authorities(
        storage, record, receipt.actual_reconciliation_id, receipt.authoritative_coverage_id)
    if receipt.covered_start != record.started_at or receipt.covered_end != record.ended_at:
        raise _incomplete("receipt interval does not match gap interval")
    if receipt.scope != record.required_scope:
        raise _incomplete("receipt scope does not match gap scope")
    return receipt


def _validate_authorities(
    storage,
    record: GapRecord,
    actual_reconciliation_id: str,
    authoritative_coverage_id: str,
) -> None:
    migration = _completed_migration(storage, actual_reconciliation_id)
    coverage = _trusted_coverage(storage, authoritative_coverage_id)
    if migration["source_fingerprint"] != coverage["source_fingerprint"]:
        raise _incomplete("coverage is not bound to migration fingerprint")
    if coverage["covered_start"] > record.started_at or coverage["covered_end"] < record.ended_at:
        raise _incomplete("coverage does not cover the closed interval")
    if coverage["scope"] != record.required_scope:
        raise _incomplete("coverage scope does not match required scope")
    digest = migration.get("target_digest")
    if digest is not None:
        for table in coverage["scope"].tables:
            expected = coverage["table_counts"].get(table)
            if expected is None or int(expected) != digest.table(table).row_count:
                raise _incomplete("coverage table counts do not match migration target")


def _completed_migration(storage, run_id: str) -> dict:
    p = storage.placeholder
    rows = storage.query(
        f"SELECT run_id, source_fingerprint FROM migration_runs WHERE run_id={p} AND state='COMPLETED'",
        (run_id,),
    )
    if not rows:
        raise _incomplete("completed migration record is missing")
    row = rows[0]
    out = {"run_id": str(row["run_id"]), "source_fingerprint": str(row["source_fingerprint"])}
    if storage.kind == "postgres":
        digest = canonical_postgres_digest_from_connection(storage.conn)
        if digest.fingerprint != row["source_fingerprint"]:
            raise _incomplete("migration target fingerprint no longer matches completed run")
        out["target_digest"] = digest
    return out


def _trusted_coverage(storage, coverage_id: str) -> dict:
    p = storage.placeholder
    rows = storage.query(
        "SELECT coverage_id, covered_start, covered_end, scope_hash, scope_json, "
        "source_fingerprint, table_counts_json, evidence_hash, producer, verified_at "
        f"FROM authoritative_gap_coverages WHERE coverage_id={p} AND status='VERIFIED'",
        (coverage_id,),
    )
    if not rows:
        raise _incomplete("trusted authoritative coverage is missing")
    row = rows[0]
    scope = _scope_from_stored(row["scope_json"], None)
    counts = _json_load(row["table_counts_json"])
    payload = {
        "coverage_id": row["coverage_id"],
        "covered_start": _format_utc(_coerce_utc(row["covered_start"])),
        "covered_end": _format_utc(_coerce_utc(row["covered_end"])),
        "scope_hash": row["scope_hash"],
        "source_fingerprint": row["source_fingerprint"],
        "table_counts": counts,
        "producer": row["producer"],
        "verified_at": _format_utc(_coerce_utc(row["verified_at"])),
    }
    if str(row["scope_hash"]) != _scope_hash(scope) or str(row["evidence_hash"]) != _hash_json(payload):
        raise _incomplete("trusted coverage evidence hash mismatch")
    return {
        "covered_start": _coerce_utc(row["covered_start"]),
        "covered_end": _coerce_utc(row["covered_end"]),
        "scope": scope,
        "source_fingerprint": str(row["source_fingerprint"]),
        "table_counts": counts,
    }


def _incomplete(detail: str) -> GapTransitionError:
    from .gaps import GAP_RECOVERY_EVIDENCE_INCOMPLETE
    return GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, detail)


def _json(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_load(value) -> dict:
    if isinstance(value, dict):
        return value
    return json.loads(str(value))


def _hash_json(value: dict) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _record_params(record: GapRecord) -> tuple:
    return (
        record.id, record.state.value, _format_utc(record.started_at),
        _format_utc(record.ended_at) if record.ended_at else None,
        _format_utc(record.recovered_at) if record.recovered_at else None,
        _scope_hash(record.required_scope), _scope_json(record.required_scope),
        record.backfill_receipt_id,
    )


def _receipt_from_row(row: dict) -> BackfillReceipt | None:
    scope = _scope_from_stored(row["scope_json"], None)
    receipt = BackfillReceipt(
        str(row["receipt_id"]), _coerce_utc(row["covered_start"]),
        _coerce_utc(row["covered_end"]), scope, "stored_reconciliation",
        str(row["actual_reconciliation_id"]), str(row["authoritative_coverage_id"]),
        int(row["missing_intervals"]), int(row["missing_rows"]),
        _coerce_utc(row["verified_at"]),
    )
    if str(row["scope_hash"]) != _scope_hash(scope):
        return None
    if str(row["evidence_hash"]) != _receipt_hash(receipt):
        return None
    return receipt


def _receipt_hash(receipt: BackfillReceipt) -> str:
    payload = {
        "receipt_id": receipt.id,
        "covered_start": _format_utc(receipt.covered_start),
        "covered_end": _format_utc(receipt.covered_end),
        "scope_hash": _scope_hash(receipt.scope),
        "actual_reconciliation_id": receipt.actual_reconciliation_id,
        "authoritative_coverage_id": receipt.authoritative_coverage_id,
        "missing_intervals": receipt.missing_intervals,
        "missing_rows": receipt.missing_rows,
        "verified_at": _format_utc(receipt.verified_at),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _record_from_dict(raw: dict) -> GapRecord:
    return GapRecord(
        id=str(raw["id"]),
        started_at=parse_utc(str(raw["started_at"])),
        ended_at=parse_utc(str(raw["ended_at"])) if raw.get("ended_at") else None,
        state=GapState(str(raw["state"])),
        required_scope=_scope_from_dict(raw["required_scope"]),
        reason=str(raw["reason"]),
        backfill_receipt_id=raw.get("backfill_receipt_id"),
        recovered_at=parse_utc(str(raw["recovered_at"])) if raw.get("recovered_at") else None,
        reconciliation_result=raw.get("reconciliation_result"),
    )


def _record_to_dict(record: GapRecord) -> dict:
    return {
        "id": record.id,
        "started_at": _format_utc(record.started_at),
        "ended_at": _format_utc(record.ended_at) if record.ended_at else None,
        "state": record.state.value,
        "required_scope": {
            "venues": list(record.required_scope.venues),
            "symbols": list(record.required_scope.symbols),
            "tables": list(record.required_scope.tables),
        },
        "reason": record.reason,
        "backfill_receipt_id": record.backfill_receipt_id,
        "recovered_at": _format_utc(record.recovered_at) if record.recovered_at else None,
        "reconciliation_result": record.reconciliation_result,
    }


def _scope_from_dict(raw: dict) -> GapScope:
    return GapScope(
        tuple(str(item).upper() for item in raw["venues"]),
        tuple(str(item) for item in raw["symbols"]),
        tuple(str(item) for item in raw["tables"]),
    )


def _scope_json(scope: GapScope) -> str:
    return json.dumps({
        "venues": list(scope.venues),
        "symbols": list(scope.symbols),
        "tables": list(scope.tables),
    }, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _scope_from_stored(value, seed: GapRecord | None) -> GapScope:
    if value:
        raw = value if isinstance(value, dict) else json.loads(str(value))
        return _scope_from_dict(raw)
    if seed is not None:
        return seed.required_scope
    return GapScope((), (), ())
