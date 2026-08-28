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

    def report(self) -> dict:
        h = self.hours_until_full()
        return {
            "db_bytes": self.db_bytes(),
            "disk_free_bytes": self.free_bytes(),
            "growth_mb_per_hour": round(self.growth_bytes_per_hour() / 1e6, 1),
            # 이 값이 지표다. 남은 용량만으로는 하루치인지 한 달치인지 모른다.
            "hours_until_full": round(h, 1) if h is not None else None,
        }


def prune(storage, retention_days: float, now_us: int | None = None) -> dict:
    """보존 기간이 지난 원시 데이터를 지운다. 지운 행 수를 테이블별로 반환."""
    if retention_days <= 0:
        return {}
    cutoff = int(((now_us / 1e6) if now_us else time.time())
                 - retention_days * 86400) * 1_000_000
    deleted = {}
    for table, col in PRUNE_TABLES:
        n = 0
        while True:
            try:
                # rowid / ctid 차이는 저장소가 흡수한다. 여기서 SQL 을 쓰면
                # 한쪽 백엔드에서만 조용히 안 도는 코드가 된다.
                got = storage.delete_older_than(table, col, cutoff, DELETE_BATCH)
            except AttributeError:
                log.warning("[retention] %s: 저장소가 삭제를 지원하지 않는다 "
                            "(읽기 전용?) — 건너뛴다", table)
                break
            except Exception as e:                        # noqa: BLE001
                log.warning("[retention] %s 삭제 실패: %s: %s",
                            table, type(e).__name__, e)
                break
            if got <= 0:
                break
            n += got
            if got < DELETE_BATCH:
                break
        if n:
            deleted[table] = n
            log.info("[retention] %s 에서 %d행 삭제 (%.1f일 이전)",
                     table, n, retention_days)
    return deleted
