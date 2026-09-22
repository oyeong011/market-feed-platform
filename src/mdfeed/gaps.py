from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import NoReturn, Protocol

UTC = dt.timezone.utc
KST = dt.timezone(dt.timedelta(hours=9))
GAP_RECOVERY_EVIDENCE_INCOMPLETE = "GAP_RECOVERY_EVIDENCE_INCOMPLETE"
KNOWN_INCIDENT_ID = "collection-stop-20260908"
KNOWN_INCIDENT_START = dt.datetime(2026, 9, 8, 5, 16, 54, tzinfo=UTC)
CATALOG_TABLES = (
    "instruments",
    "trades",
    "book_top",
    "bars_1m",
    "signals",
    "feed_stats",
    "latest",
    "quality_events",
)
UNKNOWN_HISTORICAL_SYMBOL_SCOPE = "UNKNOWN_HISTORICAL_UNIVERSE"
KNOWN_VENUES = ("UPBIT", "BINANCE", "KIS", "KRX")


class GapState(str, Enum):
    OPEN = "OPEN"
    ENDED_UNRECOVERED = "ENDED_UNRECOVERED"
    BACKFILLING = "BACKFILLING"
    RECOVERED = "RECOVERED"


@dataclass(frozen=True, slots=True)
class GapScope:
    venues: tuple[str, ...]
    symbols: tuple[str, ...]
    tables: tuple[str, ...]

    @classmethod
    def known_market_scope(cls) -> "GapScope":
        return cls(KNOWN_VENUES, (UNKNOWN_HISTORICAL_SYMBOL_SCOPE,), CATALOG_TABLES)

    def covers(self, required: "GapScope") -> bool:
        return (_covers(self.venues, required.venues)
                and _covers(self.symbols, required.symbols)
                and _covers(self.tables, required.tables))

    def counts(self) -> dict[str, int]:
        return {
            "venues": len(self.venues),
            "symbols": len(self.symbols),
            "tables": len(self.tables),
        }


@dataclass(frozen=True, slots=True)
class BackfillReceipt:
    id: str
    covered_start: dt.datetime
    covered_end: dt.datetime
    scope: GapScope
    evidence_type: str
    actual_reconciliation_id: str
    authoritative_coverage_id: str
    missing_intervals: int
    missing_rows: int
    verified_at: dt.datetime


@dataclass(frozen=True, slots=True)
class GapRecord:
    id: str
    started_at: dt.datetime
    ended_at: dt.datetime | None
    state: GapState
    required_scope: GapScope
    reason: str
    backfill_receipt_id: str | None = None
    recovered_at: dt.datetime | None = None
    reconciliation_result: str | None = None

    def to_public_dict(self, now: dt.datetime | None = None) -> dict:
        current = _to_utc(now or dt.datetime.now(UTC))
        stop = self.ended_at or current
        elapsed = max(0, int((stop - self.started_at).total_seconds()))
        recovered = self.state is GapState.RECOVERED
        return {
            "id": self.id,
            "state": self.state.value,
            "started_at_utc": _format_utc(self.started_at),
            "started_at_kst": self.started_at.astimezone(KST).isoformat(),
            "ended_at_utc": _format_utc(self.ended_at) if self.ended_at else None,
            "ended_at_kst": self.ended_at.astimezone(KST).isoformat() if self.ended_at else None,
            "elapsed_seconds": elapsed,
            "required_scope": asdict(self.required_scope),
            "required_scope_counts": self.required_scope.counts(),
            "covered_scope_counts": (
                self.required_scope.counts() if recovered else {"venues": 0, "symbols": 0, "tables": 0}
            ),
            "backfill_receipt_id": self.backfill_receipt_id,
            "recovered_at_utc": _format_utc(self.recovered_at) if self.recovered_at else None,
            "recovered": recovered,
            "reconciliation_result": self.reconciliation_result,
            "blocking_reason": None if recovered else self.reason,
        }


class GapTransitionError(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


class GapRepository(Protocol):
    def list(self) -> list[GapRecord]: ...
    def get(self, gap_id: str) -> GapRecord | None: ...
    def save(self, record: GapRecord) -> None: ...


class TrustedReceiptStore(Protocol):
    def get_verified(self, receipt_id: str) -> BackfillReceipt | None: ...


def known_incident() -> GapRecord:
    return GapRecord(
        id=KNOWN_INCIDENT_ID,
        started_at=KNOWN_INCIDENT_START,
        ended_at=None,
        state=GapState.OPEN,
        required_scope=GapScope.known_market_scope(),
        reason="collection stopped at 2026-09-08 14:16:54 KST; no verified backfill exists",
    )


def open_repository(state_file: str | None = None, storage=None,
                    storage_profile: str | None = None,
                    incidents_dir: str | None = None) -> GapRepository:
    """공백 저장소를 연다. 알려진 사고 기록(ops/incidents/)은 **운영 데이터에 난 구멍**이다.

    incidents_dir 를 명시하면 그 폴더를 심는다 (REST 표면을 시험할 때처럼).
    명시가 없으면 프로파일이 결정한다: production 은 ops/incidents/ 를 심고,
    test(합성 리플레이 + 일회용 SQLite)는 아무것도 심지 않는다 — 그 데이터에는 이 구멍이 없다.
    심으면 CI 스모크의 헬스체크가 "열린 공백 1개 → CRIT" 로 떨어진다 (2026-09-22 첫 실행에서 실제로 그랬다).
    운영에서는 그대로 심는다 — 현재 헬스가 정상이라고 과거 구멍이 메워진 건 아니다.
    """
    import os
    from .gap_repository import load_seed_records, open_repository as open_repo
    if incidents_dir:
        seeds = load_seed_records(incidents_dir)
    else:
        profile = storage_profile or os.getenv("MDFEED_STORAGE_PROFILE", "production")
        seeds = [] if profile == "test" else load_seed_records()
    return open_repo(state_file, storage, seeds)


def close_gap(record: GapRecord, ended_at: dt.datetime) -> GapRecord:
    match record.state:
        case GapState.OPEN:
            return GapRecord(
                id=record.id,
                started_at=record.started_at,
                ended_at=_to_utc(ended_at),
                state=GapState.ENDED_UNRECOVERED,
                required_scope=record.required_scope,
                reason="gap has an end time but no verified backfill receipt",
            )
        case GapState.ENDED_UNRECOVERED | GapState.BACKFILLING | GapState.RECOVERED:
            raise GapTransitionError("GAP_ILLEGAL_TRANSITION", "only an open gap can be closed")
        case unreachable:
            _assert_never(unreachable)


def begin_backfill(record: GapRecord) -> GapRecord:
    match record.state:
        case GapState.ENDED_UNRECOVERED:
            return _replace_state(record, GapState.BACKFILLING, "backfill is in progress")
        case GapState.OPEN | GapState.BACKFILLING | GapState.RECOVERED:
            raise GapTransitionError("GAP_ILLEGAL_TRANSITION", "only a closed unrecovered gap can enter backfill")
        case unreachable:
            _assert_never(unreachable)


def mark_recovered(record: GapRecord, receipt: BackfillReceipt) -> GapRecord:
    if record.state is GapState.OPEN:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "open gaps have no closed interval")
    if record.ended_at is None:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "gap end time is required")
    if not _scope_resolved(record.required_scope) or not _scope_resolved(receipt.scope):
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "resolved non-empty historical scope is required")
    if receipt.evidence_type != "stored_reconciliation":
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "stored reconciliation evidence is required")
    if not receipt.actual_reconciliation_id or not receipt.authoritative_coverage_id:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "coverage and reconciliation ids are required")
    if receipt.covered_start > record.started_at or receipt.covered_end < record.ended_at:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "receipt does not cover the full time interval")
    if receipt.scope != record.required_scope:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "receipt does not match the exact required scope")
    if receipt.missing_intervals != 0 or receipt.missing_rows != 0:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "receipt reports missing market data")
    return GapRecord(
        id=record.id,
        started_at=record.started_at,
        ended_at=record.ended_at,
        state=GapState.RECOVERED,
        required_scope=record.required_scope,
        reason="recovered by verified stored reconciliation",
        backfill_receipt_id=receipt.id,
        recovered_at=receipt.verified_at,
        reconciliation_result=receipt.actual_reconciliation_id,
    )


def mark_recovered_from_store(
    record: GapRecord,
    receipt_id: str,
    receipts: TrustedReceiptStore,
) -> GapRecord:
    receipt = receipts.get_verified(receipt_id)
    if receipt is None:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "trusted recovery receipt is missing")
    return mark_recovered(record, receipt)


def summarize(records: Iterable[GapRecord], now: dt.datetime | None = None) -> dict:
    items = [record.to_public_dict(now) for record in records]
    open_items = [item for item in items if item["state"] != GapState.RECOVERED.value]
    max_duration = max((item["elapsed_seconds"] for item in open_items), default=0)
    return {
        "count": len(items),
        "open_count": len(open_items),
        "recovered_count": len(items) - len(open_items),
        "unrecovered_duration_seconds": max_duration,
        "healthy": len(open_items) == 0,
        "data_completeness": "incomplete" if open_items else "complete",
        "items": items,
    }


def receipt_from_dict(raw: dict) -> BackfillReceipt:
    try:
        return BackfillReceipt(
            id=str(raw["id"]),
            covered_start=parse_utc(str(raw["covered_start"])),
            covered_end=parse_utc(str(raw["covered_end"])),
            scope=_scope_from_dict(raw["scope"]),
            evidence_type=str(raw["evidence_type"]),
            actual_reconciliation_id=str(raw["actual_reconciliation_id"]),
            authoritative_coverage_id=str(raw["authoritative_coverage_id"]),
            missing_intervals=int(raw["missing_intervals"]),
            missing_rows=int(raw["missing_rows"]),
            verified_at=parse_utc(str(raw["verified_at"])),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GapTransitionError(GAP_RECOVERY_EVIDENCE_INCOMPLETE, "invalid receipt shape") from exc


def parse_utc(text: str) -> dt.datetime:
    normalized = text.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return _to_utc(parsed)


def _replace_state(record: GapRecord, state: GapState, reason: str) -> GapRecord:
    return GapRecord(record.id, record.started_at, record.ended_at, state,
                     record.required_scope, reason, record.backfill_receipt_id,
                     record.recovered_at, record.reconciliation_result)


def _scope_resolved(scope: GapScope) -> bool:
    parts = scope.venues + scope.symbols + scope.tables
    return bool(
        scope.venues
        and scope.symbols
        and scope.tables
        and UNKNOWN_HISTORICAL_SYMBOL_SCOPE not in parts
        and "*" not in parts
    )


def _covers(actual: tuple[str, ...], required: tuple[str, ...]) -> bool:
    actual_set = set(actual)
    return "*" in actual_set or set(required).issubset(actual_set)


def _to_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return value.astimezone(UTC)


def _format_utc(value: dt.datetime) -> str:
    return _to_utc(value).isoformat().replace("+00:00", "Z")


def _scope_hash(scope: GapScope) -> str:
    payload = json.dumps(asdict(scope), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _coerce_utc(value) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return _to_utc(value)
    if isinstance(value, int):
        seconds, micros = divmod(value, 1_000_000)
        return dt.datetime.fromtimestamp(seconds, UTC).replace(microsecond=micros)
    return parse_utc(str(value))


def _assert_never(value: NoReturn) -> NoReturn:
    raise AssertionError(f"unhandled gap state: {value}")
