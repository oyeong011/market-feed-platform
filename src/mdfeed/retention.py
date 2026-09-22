"""보존 정책과 디스크 여유 감시.

왜 필요한가
-----------
688종목을 붙이고 재 보니 초당 285행, 하루 약 2,460만 행이 쌓인다.
보존 정책이 없으면 디스크가 찰 때까지 쓰다가 죽는다. 그리고 그건
"프로세스가 죽었다"로만 보여서, 원인을 찾는 데 시간이 걸린다.

디스크가 언제 차는지는 **차기 전에** 보여야 한다. 그래서 남은 용량이 아니라
"현재 증가율로 몇 시간 뒤에 차는가"를 낸다. 남은 GB 는 유량을 모르면
해석할 수 없는 숫자다 — 40GB 남았다는 게 하루치인지 한 달치인지 알 수 없다.

무엇을 지우나
-------------
원시 체결(trades)과 호가(book_top)만 지운다. 1분봉(bars_1m)과 품질 이벤트는
남긴다. 봉은 원시 데이터의 요약이라 지우면 과거를 복구할 수 없고, 크기도
체결의 1/100 수준이다(실측 5.0MB 대 590.2MB).

기본값은 0(끄기)이다. 데이터를 지우는 기능이 기본으로 켜져 있으면 안 된다.

켜는 순간이 가장 위험하다
-------------------------
9/3 에 실제로 켜려고 재 보니 trades 가 7,424만 행(6.1일치)이었다. 보존
3일이면 첫 실행에서 2,768만 행, 50,000행 배치로 **554번**을 지워야 한다.
그런데 writer 는 락을 배치마다가 아니라 **루프 바깥에서** 잡고 있었다.
배치로 끊는 코드는 "락을 오래 잡지 않으려고"라고 적혀 있었지만, 실제로는
첫 삭제가 끝날 때까지 적재가 통째로 멈춘다. 버스는 drop-oldest 라서
그 시간만큼 틱이 버려진다 — **보존을 켜는 행위 자체가 데이터 손실**이었다.

그래서 두 가지를 바꾼다.
* 락은 배치마다 잡고 놓는다(guard). 배치 사이에 적재가 끼어들 수 있다.
* 한 번에 도는 시간에 상한을 둔다(budget_s). 남은 건 다음 주기에 지운다.
  첫 삭제는 몇 분 멈추는 대신 몇 시간에 걸쳐 나눠 진행된다.

지워도 파일은 안 줄어든다
-------------------------
SQLite 는 auto_vacuum=0 이면 DELETE 한 페이지를 freelist 에 넣고 파일
크기는 그대로 둔다(실측: 이 DB 는 auto_vacuum=0). 다음 적재가 그 자리를
재사용하므로 **증가는 멈추지만 db_bytes 는 안 준다**. 이걸 모르면 운영자가
"보존이 안 돈다"로 오해한다. 그래서 report() 가 회수 가능한 바이트를 같이 낸다.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass

log = logging.getLogger("mdfeed.retention")

# 원시 데이터 테이블과 그 시각 컬럼. 봉·품질 이벤트는 여기 없다(보존).
PRUNE_TABLES = (("trades", "ts"), ("book_top", "ts"))
# 한 번에 지우는 행 수 상한. 통째로 DELETE 하면 락을 오래 잡아 적재가 밀린다.
DELETE_BATCH = 50_000


@dataclass(frozen=True, slots=True)
class RetentionCutoff:
    allowed: bool
    cutoff_us: int
    reason: str


class DiskWatch:
    """DB 크기 증가율로 디스크가 언제 차는지 추정한다."""

    def __init__(self, path: str):
        self.path = path
        self._first: tuple[float, int] | None = None      # (시각, 바이트)
        self._last: tuple[float, int] | None = None

    def db_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                total += os.path.getsize(self.path + suffix)
            except OSError:
                pass
        return total

    def free_bytes(self) -> int:
        try:
            return shutil.disk_usage(os.path.dirname(self.path) or ".").free
        except OSError:
            return 0

    def sample(self) -> None:
        now, size = time.time(), self.db_bytes()
        if self._first is None:
            self._first = (now, size)
        self._last = (now, size)

    def growth_bytes_per_hour(self) -> float:
        if not self._first or not self._last:
            return 0.0
        dt = self._last[0] - self._first[0]
        if dt < 60:                      # 표본이 짧으면 추정하지 않는다.
            return 0.0                   # 기동 직후 급증을 정상 증가율로 오해한다.
        return (self._last[1] - self._first[1]) / dt * 3600.0

    def hours_until_full(self) -> float | None:
        g = self.growth_bytes_per_hour()
        if g <= 0:
            return None                  # 안 늘거나 줄고 있음 — 추정 불가
        return self.free_bytes() / g

    def reclaimable_bytes(self) -> int:
        """DELETE 로 비었지만 파일에는 남아 있는 바이트(SQLite freelist).

        0 이 아니면 "지웠는데 파일이 안 줄었다"가 정상이라는 뜻이다.
        SQLite 가 아니거나 읽을 수 없으면 0 — 없는 걸 추정하지 않는다.
        """
        try:
            import sqlite3
            conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True,
                                   timeout=2.0)
            try:
                page = conn.execute("PRAGMA page_size").fetchone()[0]
                free = conn.execute("PRAGMA freelist_count").fetchone()[0]
                return int(page) * int(free)
            finally:
                conn.close()
        except Exception:                                  # noqa: BLE001
            return 0

    def report(self) -> dict[str, int | float | None]:
        h = self.hours_until_full()
        return {
            "db_bytes": self.db_bytes(),
            # 파일 크기에서 이만큼은 이미 빈 자리다. 지운 뒤 db_bytes 가
            # 그대로인 걸 사고로 오인하지 않게 같이 낸다.
            "reclaimable_bytes": self.reclaimable_bytes(),
            "disk_free_bytes": self.free_bytes(),
            "growth_mb_per_hour": round(self.growth_bytes_per_hour() / 1e6, 1),
            # 이 값이 지표다. 남은 용량만으로는 하루치인지 한 달치인지 모른다.
            "hours_until_full": round(h, 1) if h is not None else None,
        }


class PruneResult(dict[str, int]):
    """테이블별 삭제 행수. dict 라서 기존 호출부(sum(values()))가 그대로 돈다.

    다 못 지웠는지를 같이 들고 다닌다. 이게 없으면 "지웠다"와 "지우다 말았다"가
    구분되지 않고, 예산에 걸려 매 주기 같은 자리를 맴돌아도 아무도 모른다.
    """

    def __init__(self, rows: dict[str, int] | None = None, *,
                 budget_hit: bool = False, elapsed_s: float = 0.0,
                 batches: int = 0, blocked_reason: str | None = None):
        super().__init__(rows or {})
        self.budget_hit: bool = budget_hit
        self.elapsed_s: float = elapsed_s
        self.batches: int = batches
        self.blocked_reason: str | None = blocked_reason


def _fmt_us(us: int) -> str:
    import datetime as _dt
    return _dt.datetime.fromtimestamp(
        us / 1e6, _dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _cutoff_us(retention_days: float, now_us: int | None) -> int:
    return int(((now_us / 1e6) if now_us else time.time())
               - retention_days * 86400) * 1_000_000


def _day_start_us(day: dt.date) -> int:
    return int(dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc).timestamp() * 1_000_000)


def _source_floor_day(storage) -> dt.date | None:
    floors = []
    for table, col in PRUNE_TABLES:
        try:
            row = storage.query(f"SELECT MIN({col}) AS v FROM {table}")[0]
        except (IndexError, KeyError):
            return None
        value = row.get("v")
        if value is None:
            continue
        if isinstance(value, dt.datetime):
            instant = value if value.tzinfo is not None else value.replace(tzinfo=dt.timezone.utc)
            floors.append(instant.astimezone(dt.timezone.utc).date())
        else:
            floors.append(dt.datetime.fromtimestamp(int(value) / 1e6, dt.timezone.utc).date())
    return min(floors) if floors else None



def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        ch in "0123456789abcdef" for ch in value.lower())


def _positive_int(value: object) -> int | None:
    return value if isinstance(value, int) and value >= 0 else None


def _strict_receipt(data: object, table: str) -> dict[str, object] | None:
    if not isinstance(data, dict):
        return None
    if data.get("schema_version") != 1 or data.get("status") != "verified":
        return None
    day_value = data.get("day")
    if data.get("table") != table or not isinstance(day_value, str):
        return None
    try:
        day = dt.date.fromisoformat(day_value)
    except ValueError:
        return None
    local_sha = data.get("local_sha256")
    remote_sha = data.get("remote_fetch_sha256")
    manifest_sha = data.get("manifest_sha256")
    remote_manifest_sha = data.get("remote_manifest_sha256")
    source_sha = data.get("source_content_sha256")
    if not all(_is_sha256(v) for v in (local_sha, remote_sha, manifest_sha,
                                       remote_manifest_sha, source_sha)):
        return None
    if local_sha != remote_sha or manifest_sha != remote_manifest_sha:
        return None
    local_bytes = _positive_int(data.get("local_bytes"))
    remote_bytes = _positive_int(data.get("remote_fetch_bytes"))
    local_rows = _positive_int(data.get("local_rows"))
    remote_rows = _positive_int(data.get("remote_fetch_rows"))
    source_rows = _positive_int(data.get("source_rows"))
    if None in (local_bytes, remote_bytes, local_rows, remote_rows, source_rows):
        return None
    if local_bytes != remote_bytes or local_rows != remote_rows or local_rows != source_rows:
        return None
    lo, hi = _day_bounds_us(day)
    if data.get("source_table") != table or data.get("source_day") != day_value:
        return None
    if data.get("source_from_us") != lo or data.get("source_to_us") != hi:
        return None
    object_id = data.get("object_id")
    if not isinstance(object_id, str) or not object_id:
        return None
    return {
        "day": day,
        "source_rows": source_rows,
        "source_content_sha256": source_sha,
    }


def _remote_verified_receipts(receipt_dir: str, table: str) -> dict[dt.date, dict[str, object]]:
    receipts: dict[dt.date, dict[str, object]] = {}
    try:
        names = os.listdir(receipt_dir)
    except OSError:
        return receipts
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(receipt_dir, name), encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            continue
        receipt = _strict_receipt(data, table)
        if receipt is None:
            continue
        day = receipt["day"]
        if isinstance(day, dt.date):
            receipts[day] = receipt
    return receipts


def _day_bounds_us(day: dt.date) -> tuple[int, int]:
    start = _day_start_us(day)
    return start, start + 86_400 * 1_000_000


def _query_bounds(storage, lo: int, hi: int) -> tuple[object, object]:
    if getattr(storage, "kind", "") == "postgres":
        from mdfeed.migration.time import epoch_us_to_datetime

        return epoch_us_to_datetime(lo), epoch_us_to_datetime(hi)
    return lo, hi


def _source_day_proof(storage, table: str, day: dt.date) -> tuple[int, str]:
    from mdfeed.archive import ARCHIVE_TABLES, FETCH_CHUNK

    cols = ARCHIVE_TABLES[table]
    lo, hi = _day_bounds_us(day)
    ph = getattr(storage, "placeholder", "?")
    order_cols = ", ".join(cols)
    sql = (f"SELECT {', '.join(cols)} FROM {table} "
           f"WHERE ts >= {ph} AND ts < {ph} ORDER BY {order_cols}")
    digest = hashlib.sha256()
    rows = 0
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(cols)
    cursor = getattr(storage, "stream", None)
    if cursor is not None:
        iterator = cursor(sql, _query_bounds(storage, lo, hi), FETCH_CHUNK)
    else:
        queried = storage.query(sql, _query_bounds(storage, lo, hi))
        iterator = (queried[i:i + FETCH_CHUNK]
                    for i in range(0, len(queried), FETCH_CHUNK))
    for chunk in iterator:
        values = [tuple(row.get(col) for col in cols) if isinstance(row, dict)
                  else tuple(row) for row in chunk]
        writer.writerows(values)
        rows += len(values)
        data = buf.getvalue().encode()
        digest.update(data)
        buf.seek(0)
        buf.truncate(0)
    data = buf.getvalue().encode()
    digest.update(data)
    return rows, digest.hexdigest()


def _progress_path(receipt_dir: str, table: str, day: dt.date) -> str:
    return os.path.join(receipt_dir, f".retention-progress-{table}-{day.isoformat()}.json")


def _read_progress(receipt_dir: str, table: str, day: dt.date) -> dict[str, object] | None:
    try:
        with open(_progress_path(receipt_dir, table, day), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("table") != table or data.get("day") != day.isoformat():
        return None
    rows = _positive_int(data.get("remaining_rows"))
    sha = data.get("remaining_content_sha256")
    if rows is None or not _is_sha256(sha):
        return None
    return data


def _write_progress(receipt_dir: str, table: str, day: dt.date, rows: int, sha: str) -> None:
    data = {
        "schema_version": 1,
        "table": table,
        "day": day.isoformat(),
        "remaining_rows": rows,
        "remaining_content_sha256": sha,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    os.makedirs(receipt_dir, exist_ok=True)
    path = _progress_path(receipt_dir, table, day)
    tmp = f"{path}.partial"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def _source_matches_receipt(storage, receipt_dir: str, table: str,
                            receipt: dict[str, object]) -> str | None:
    day = receipt.get("day")
    if not isinstance(day, dt.date):
        return "invalid_remote_receipt"
    rows, sha = _source_day_proof(storage, table, day)
    progress = _read_progress(receipt_dir, table, day)
    if progress is not None:
        if (progress.get("remaining_rows") == rows
                and progress.get("remaining_content_sha256") == sha):
            return None
        return "source_day_changed"
    if receipt.get("source_rows") != rows or receipt.get("source_content_sha256") != sha:
        return "source_day_changed"
    return None


def retention_cutoff(storage, remote_receipt_dir: str) -> RetentionCutoff:
    floor = _source_floor_day(storage)
    if floor is None:
        return RetentionCutoff(False, 0, "no_source_rows")
    per_table = {table: _remote_verified_receipts(remote_receipt_dir, table)
                 for table, _col in PRUNE_TABLES}
    common = set.intersection(*(set(receipts) for receipts in per_table.values())) if per_table else set()
    if floor not in common:
        return RetentionCutoff(False, 0, "missing_remote_coverage")
    day = floor
    while day in common:
        for table, receipts in per_table.items():
            reason = _source_matches_receipt(storage, remote_receipt_dir, table, receipts[day])
            if reason is not None:
                return RetentionCutoff(False, 0, reason)
        day += dt.timedelta(days=1)
    return RetentionCutoff(True, _day_start_us(day), "verified_remote_coverage")


class _NoGuard:
    def __enter__(self): return self
    def __exit__(self, *a): return False


def prune(storage, retention_days: float, now_us: int | None = None,
          *, guard=None, budget_s: float | None = None,
          floor_us: int | None = None,
          remote_receipt_dir: str | None = None,
          allow_local_archive_floor: bool = False) -> PruneResult:
    """보존 기간이 지난 원시 데이터를 지운다. 지운 행 수를 테이블별로 반환.

    guard    배치마다 잡았다 놓는 컨텍스트 매니저(보통 적재용 락). 루프
             바깥에서 잡으면 배치로 끊는 의미가 사라진다 — 첫 실행에서
             수백 배치가 도는 동안 적재가 통째로 멈춘다.
    budget_s 한 번에 도는 시간 상한. 넘으면 남기고 돌아온다. 남은 건
             다음 주기가 이어서 지운다.
    floor_us 여기 이후는 절대 안 지운다. 아카이브가 검증된 구간의 끝을
             넣는다 — 바깥에 옮겨 놓지 않은 데이터를 지우지 않기 위한
             빗장이다. 보존 일수보다 이쪽이 항상 우선한다.
             (보존 3일이라도 아카이브가 2일치뿐이면 2일치만 지운다.)
    """
    if retention_days <= 0:
        return PruneResult()
    if floor_us is not None and remote_receipt_dir is None and not allow_local_archive_floor:
        return PruneResult(blocked_reason="missing_remote_coverage")
    cutoff = _cutoff_us(retention_days, now_us)
    if floor_us is not None:
        if floor_us <= 0:
            # 아카이브를 요구하는데 검증된 게 하나도 없다. 아무것도 안 지운다.
            # "설정은 켰는데 아무 일도 안 일어난다"로 보이면 안 되므로 남긴다.
            log.info("[retention] 검증된 아카이브가 없다 — 삭제를 보류한다")
            return PruneResult(blocked_reason="missing_remote_coverage" if remote_receipt_dir else None)
        if floor_us < cutoff:
            log.info("[retention] 아카이브가 %s 까지만 검증됨 — 보존 기준(%s)"
                     " 대신 그쪽에 맞춘다",
                     _fmt_us(floor_us), _fmt_us(cutoff))
        cutoff = min(cutoff, floor_us)
    lock = guard if guard is not None else _NoGuard()
    started = time.monotonic()
    deleted: dict[str, int] = {}
    batches = 0
    budget_hit = False
    for table, col in PRUNE_TABLES:
        n = 0
        while True:
            if budget_s is not None and time.monotonic() - started >= budget_s:
                budget_hit = True
                break
            try:
                # 락은 여기서만 잡는다. 배치 하나가 끝나면 놓아서 적재가
                # 끼어들 수 있게 한다.
                with lock:
                    batch_cutoff = cutoff
                    batch_day: dt.date | None = None
                    if remote_receipt_dir is not None:
                        remote_cutoff = retention_cutoff(storage,
                                                         remote_receipt_dir)
                        if not remote_cutoff.allowed:
                            reason = remote_cutoff.reason
                            current_floor = _source_floor_day(storage)
                            if (reason == "missing_remote_coverage" and floor_us is not None
                                    and current_floor is not None
                                    and _day_start_us(current_floor) < floor_us):
                                reason = "source_floor_changed"
                            if reason == "no_source_rows" and deleted:
                                break
                            return PruneResult(deleted, budget_hit=False,
                                               elapsed_s=time.monotonic() - started,
                                               batches=batches,
                                               blocked_reason=reason)
                        batch_cutoff = min(cutoff, remote_cutoff.cutoff_us)
                        batch_day = _source_floor_day(storage)
                        if batch_day is not None:
                            _lo, day_hi = _day_bounds_us(batch_day)
                            batch_cutoff = min(batch_cutoff, day_hi)
                    got = storage.delete_older_than(table, col, batch_cutoff,
                                                    DELETE_BATCH)
                    if got > 0 and remote_receipt_dir is not None and batch_day is not None:
                        rows, sha = _source_day_proof(storage, table, batch_day)
                        _write_progress(remote_receipt_dir, table, batch_day, rows, sha)
            except AttributeError:
                log.warning("[retention] %s: 저장소가 삭제를 지원하지 않는다 "
                            "(읽기 전용?) — 건너뛴다", table)
                break
            except Exception as e:                        # noqa: BLE001
                log.warning("[retention] %s 삭제 실패: %s: %s",
                            table, type(e).__name__, e)
                break
            batches += 1
            if got <= 0:
                break
            n += got
            if got < DELETE_BATCH:
                break
        if n:
            deleted[table] = n
            log.info("[retention] %s 에서 %d행 삭제 (%.1f일 이전)",
                     table, n, retention_days)
        if budget_hit:
            break
    elapsed = time.monotonic() - started
    if budget_hit:
        log.info("[retention] 시간 예산 %.0fs 소진 — %d행 지우고 남긴다. "
                 "다음 주기가 이어서 지운다", budget_s, sum(deleted.values()))
    return PruneResult(deleted, budget_hit=budget_hit, elapsed_s=elapsed,
                       batches=batches)


def prune_plan(storage, retention_days: float,
               now_us: int | None = None) -> dict[str, object]:
    """지우지 않고 **무엇이 지워질지만** 낸다.

    보존 일수는 되돌릴 수 없는 결정이다. 숫자를 모르고 고르면 안 된다 —
    3일이 하루치인지 한 달치인지는 테이블마다 다르다. 그래서 켜기 전에
    테이블별 대상 행수와 남는 기간을 먼저 보여 준다.
    """
    cutoff = _cutoff_us(retention_days, now_us) if retention_days > 0 else 0
    tables: dict[str, object] = {}
    total = 0
    for table, col in PRUNE_TABLES:
        try:
            row = storage.query(
                f"SELECT COUNT(*) AS n, MIN({col}) AS lo, MAX({col}) AS hi "
                f"FROM {table}")[0]
            ph = getattr(storage, "placeholder", "?")
            doomed = storage.query(
                f"SELECT COUNT(*) AS n FROM {table} WHERE {col} < {ph}",
                (cutoff,))[0]["n"] if retention_days > 0 else 0
        except Exception as e:                            # noqa: BLE001
            tables[table] = {"error": f"{type(e).__name__}: {e}"}
            continue
        n, lo, hi = row["n"], row["lo"], row["hi"]
        span_days = (hi - lo) / 1e6 / 86400 if n else 0.0
        tables[table] = {
            "rows": n,
            "span_days": round(span_days, 2),
            "delete_rows": doomed,
            "keep_rows": n - doomed,
            # 배치 수가 첫 실행의 비용이다. 시간 예산을 정하는 근거가 된다.
            "batches": (doomed + DELETE_BATCH - 1) // DELETE_BATCH,
        }
        total += doomed
    out: dict[str, object] = {
        "retention_days": retention_days,
        "cutoff_us": cutoff,
        "tables": tables,
    }
    out["delete_rows_total"] = total
    return out
