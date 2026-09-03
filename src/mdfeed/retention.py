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

import logging
import os
import shutil
import time

log = logging.getLogger("mdfeed.retention")

# 원시 데이터 테이블과 그 시각 컬럼. 봉·품질 이벤트는 여기 없다(보존).
PRUNE_TABLES = (("trades", "ts"), ("book_top", "ts"))
# 한 번에 지우는 행 수 상한. 통째로 DELETE 하면 락을 오래 잡아 적재가 밀린다.
DELETE_BATCH = 50_000


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

    def report(self) -> dict:
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


class PruneResult(dict):
    """테이블별 삭제 행수. dict 라서 기존 호출부(sum(values()))가 그대로 돈다.

    다 못 지웠는지를 같이 들고 다닌다. 이게 없으면 "지웠다"와 "지우다 말았다"가
    구분되지 않고, 예산에 걸려 매 주기 같은 자리를 맴돌아도 아무도 모른다.
    """

    def __init__(self, *a, budget_hit: bool = False, elapsed_s: float = 0.0,
                 batches: int = 0, **kw):
        super().__init__(*a, **kw)
        self.budget_hit = budget_hit
        self.elapsed_s = elapsed_s
        self.batches = batches


def _cutoff_us(retention_days: float, now_us: int | None) -> int:
    return int(((now_us / 1e6) if now_us else time.time())
               - retention_days * 86400) * 1_000_000


class _NoGuard:
    def __enter__(self): return self
    def __exit__(self, *a): return False


def prune(storage, retention_days: float, now_us: int | None = None,
          *, guard=None, budget_s: float | None = None) -> PruneResult:
    """보존 기간이 지난 원시 데이터를 지운다. 지운 행 수를 테이블별로 반환.

    guard    배치마다 잡았다 놓는 컨텍스트 매니저(보통 적재용 락). 루프
             바깥에서 잡으면 배치로 끊는 의미가 사라진다 — 첫 실행에서
             수백 배치가 도는 동안 적재가 통째로 멈춘다.
    budget_s 한 번에 도는 시간 상한. 넘으면 남기고 돌아온다. 남은 건
             다음 주기가 이어서 지운다.
    """
    if retention_days <= 0:
        return PruneResult()
    cutoff = _cutoff_us(retention_days, now_us)
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
                    # rowid / ctid 차이는 저장소가 흡수한다. 여기서 SQL 을 쓰면
                    # 한쪽 백엔드에서만 조용히 안 도는 코드가 된다.
                    got = storage.delete_older_than(table, col, cutoff,
                                                    DELETE_BATCH)
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
               now_us: int | None = None) -> dict:
    """지우지 않고 **무엇이 지워질지만** 낸다.

    보존 일수는 되돌릴 수 없는 결정이다. 숫자를 모르고 고르면 안 된다 —
    3일이 하루치인지 한 달치인지는 테이블마다 다르다. 그래서 켜기 전에
    테이블별 대상 행수와 남는 기간을 먼저 보여 준다.
    """
    cutoff = _cutoff_us(retention_days, now_us) if retention_days > 0 else 0
    out = {"retention_days": retention_days, "cutoff_us": cutoff, "tables": {}}
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
            out["tables"][table] = {"error": f"{type(e).__name__}: {e}"}
            continue
        n, lo, hi = row["n"], row["lo"], row["hi"]
        span_days = (hi - lo) / 1e6 / 86400 if n else 0.0
        out["tables"][table] = {
            "rows": n,
            "span_days": round(span_days, 2),
            "delete_rows": doomed,
            "keep_rows": n - doomed,
            # 배치 수가 첫 실행의 비용이다. 시간 예산을 정하는 근거가 된다.
            "batches": (doomed + DELETE_BATCH - 1) // DELETE_BATCH,
        }
        total += doomed
    out["delete_rows_total"] = total
    return out
