import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mdfeed import gaps

ROOT = Path(__file__).resolve().parents[1]


def _cli_env() -> dict[str, str]:
    # 서브프로세스는 conftest 의 sys.path 를 상속하지 않는다
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    return env


def test_known_incident_reports_open_unknown_historical_scope():
    record = gaps.known_incident()
    public = record.to_public_dict(dt.datetime(2026, 9, 9, tzinfo=gaps.UTC))
    assert public["id"] == "collection-stop-20260908"
    assert public["started_at_utc"] == "2026-09-08T05:16:54Z"
    assert public["started_at_kst"] == "2026-09-08T14:16:54+09:00"
    assert public["state"] == "OPEN"
    assert public["ended_at_utc"] is None
    assert public["backfill_receipt_id"] is None
    assert public["recovered"] is False
    assert public["required_scope"]["venues"] == ("UPBIT", "BINANCE", "KIS", "KRX")
    assert public["required_scope"]["symbols"] == ("UNKNOWN_HISTORICAL_UNIVERSE",)


def test_recovery_without_closed_interval_is_rejected():
    receipt = _complete_receipt(gaps.known_incident())
    with pytest.raises(gaps.GapTransitionError) as raised:
        gaps.mark_recovered(gaps.known_incident(), receipt)
    assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def test_incomplete_scope_or_fabricated_receipt_is_rejected():
    closed = gaps.close_gap(gaps.known_incident(), dt.datetime(2026, 9, 8, 6, tzinfo=gaps.UTC))
    partial = _complete_receipt(closed, scope=gaps.GapScope(("UPBIT",), ("* ",), gaps.CATALOG_TABLES))
    fabricated = _complete_receipt(closed, evidence_type="current_feed_health")
    for receipt in (partial, fabricated):
        with pytest.raises(gaps.GapTransitionError) as raised:
            gaps.mark_recovered(closed, receipt)
        assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def test_verified_stored_reconciliation_recovers_closed_synthetic_gap():
    record = gaps.GapRecord(
        id="synthetic-gap",
        started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
        ended_at=None,
        state=gaps.GapState.OPEN,
        required_scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
        reason="synthetic",
    )
    closed = gaps.close_gap(record, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC))
    receipt = _complete_receipt(closed)
    recovered = gaps.mark_recovered(closed, receipt)
    assert recovered.state is gaps.GapState.RECOVERED
    assert recovered.backfill_receipt_id == receipt.id


def test_gap_status_cli_uses_stable_exit_codes(tmp_path):
    state_file = tmp_path / "gaps.json"
    result = subprocess.run(
        [sys.executable, "-m", "mdfeed.cli", "gap", "status", "--state-file", str(state_file)],
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    body = json.loads(result.stdout)
    assert body["items"][0]["id"] == "collection-stop-20260908"
    assert body["items"][0]["state"] == "OPEN"

    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps({"verified": True}), encoding="utf-8")
    bad = subprocess.run(
        [
            sys.executable,
            "-m",
            "mdfeed.cli",
            "gap",
            "verify-recovery",
            "--id",
            "collection-stop-20260908",
            "--receipt",
            str(receipt_path),
            "--state-file",
            str(state_file),
        ],
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert bad.returncode == 2
    assert json.loads(bad.stdout)["error"] == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"

    missing = subprocess.run(
        [
            sys.executable,
            "-m",
            "mdfeed.cli",
            "gap",
            "status",
            "--id",
            "missing-gap",
            "--state-file",
            str(state_file),
        ],
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert missing.returncode == 2
    assert json.loads(missing.stdout)["error"] == "GAP_NOT_FOUND"


def test_storage_repository_persists_known_gap_in_shared_table(tmp_path):
    from mdfeed.storage.db import SQLiteStorage

    store = SQLiteStorage(str(tmp_path / "gaps.db"))
    store.ensure_schema()
    repo = gaps.open_repository(storage=store)
    record = repo.get("collection-stop-20260908")
    assert record is not None
    assert record.state is gaps.GapState.OPEN
    rows = store.query("SELECT gap_id, state, required_scope_json FROM data_gaps")
    assert rows[0]["gap_id"] == "collection-stop-20260908"
    assert rows[0]["state"] == "OPEN"
    persisted_scope = json.loads(str(rows[0]["required_scope_json"]))
    assert persisted_scope["venues"] == ["UPBIT", "BINANCE", "KIS", "KRX"]
    assert persisted_scope["symbols"] == ["UNKNOWN_HISTORICAL_UNIVERSE"]
    repo.save(gaps.close_gap(record, dt.datetime(2026, 9, 8, 6, 0, tzinfo=gaps.UTC)))
    reopened = gaps.open_repository(storage=store)
    assert reopened.get("collection-stop-20260908").state is gaps.GapState.ENDED_UNRECOVERED
    store.close()


def test_recovery_uses_trusted_receipt_store_not_user_json():
    record = gaps.GapRecord(
        id="trusted-store-synthetic",
        started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
        ended_at=None,
        state=gaps.GapState.OPEN,
        required_scope=gaps.GapScope(("UPBIT",), ("KRW-BTC",), ("trades",)),
        reason="synthetic",
    )
    closed = gaps.close_gap(record, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC))
    receipt = _complete_receipt(closed)
    from mdfeed.gap_repository import MemoryTrustedReceiptStore

    trusted = MemoryTrustedReceiptStore([receipt])
    recovered = gaps.mark_recovered_from_store(closed, receipt.id, trusted)
    assert recovered.state is gaps.GapState.RECOVERED
    with pytest.raises(gaps.GapTransitionError) as raised:
        gaps.mark_recovered_from_store(closed, "missing", MemoryTrustedReceiptStore())
    assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def _complete_receipt(
    record: gaps.GapRecord,
    scope: gaps.GapScope | None = None,
    evidence_type: str = "stored_reconciliation",
) -> gaps.BackfillReceipt:
    end = record.ended_at or record.started_at + dt.timedelta(minutes=1)
    return gaps.BackfillReceipt(
        id=f"{record.id}-receipt",
        covered_start=record.started_at,
        covered_end=end,
        scope=scope or record.required_scope,
        evidence_type=evidence_type,
        actual_reconciliation_id="reconcile-1",
        authoritative_coverage_id="coverage-1",
        missing_intervals=0,
        missing_rows=0,
        verified_at=end + dt.timedelta(seconds=1),
    )


def test_known_unknown_scope_cannot_be_recovered_even_with_matching_placeholder():
    closed = gaps.close_gap(gaps.known_incident(), dt.datetime(2026, 9, 8, 6, tzinfo=gaps.UTC))
    receipt = _complete_receipt(closed)
    with pytest.raises(gaps.GapTransitionError) as raised:
        gaps.mark_recovered(closed, receipt)
    assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def test_wildcard_scope_cannot_be_recovered_as_exact_historical_scope():
    record = gaps.GapRecord(
        id="wildcard-scope",
        started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
        ended_at=None,
        state=gaps.GapState.OPEN,
        required_scope=gaps.GapScope(("UPBIT",), ("*",), ("trades",)),
        reason="unresolved wildcard synthetic",
    )
    closed = gaps.close_gap(record, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC))
    with pytest.raises(gaps.GapTransitionError) as raised:
        gaps.mark_recovered(closed, _complete_receipt(closed))
    assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def test_empty_required_scope_cannot_be_recovered():
    record = gaps.GapRecord(
        id="empty-scope",
        started_at=dt.datetime(2026, 1, 1, tzinfo=gaps.UTC),
        ended_at=None,
        state=gaps.GapState.OPEN,
        required_scope=gaps.GapScope((), (), ()),
        reason="invalid synthetic",
    )
    closed = gaps.close_gap(record, dt.datetime(2026, 1, 1, 0, 1, tzinfo=gaps.UTC))
    with pytest.raises(gaps.GapTransitionError) as raised:
        gaps.mark_recovered(closed, _complete_receipt(closed))
    assert raised.value.code == "GAP_RECOVERY_EVIDENCE_INCOMPLETE"


def test_test_profile_does_not_seed_production_incident(tmp_path, monkeypatch):
    """사고 기록은 운영 데이터의 구멍이다. 합성 데이터(test 프로파일)에 심으면 CI 스모크가 CRIT 로 떨어진다."""
    monkeypatch.chdir(tmp_path)                      # ops/incidents 가 없어도 known_incident 폴백이 있다
    prod = gaps.open_repository(str(tmp_path / "prod.json"), storage_profile="production")
    assert [r.id for r in prod.list()] == ["collection-stop-20260908"]
    test = gaps.open_repository(str(tmp_path / "test.json"), storage_profile="test")
    assert test.list() == []
    monkeypatch.setenv("MDFEED_STORAGE_PROFILE", "test")   # 인자가 없으면 환경변수를 따른다
    assert gaps.open_repository(str(tmp_path / "env.json")).list() == []
    # 폴더를 명시하면 프로파일과 무관하게 심는다 (REST 표면 시험용)
    explicit = gaps.open_repository(str(tmp_path / "explicit.json"), storage_profile="test",
                                    incidents_dir=str(Path(__file__).resolve().parents[1] / "ops" / "incidents"))
    assert [r.id for r in explicit.list()] == ["collection-stop-20260908"]
