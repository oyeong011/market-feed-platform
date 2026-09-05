"""원시 데이터를 외부 저장소로 내보낸다 — 지우기 전에 옮긴다.

왜 지우는 대신 옮기나
---------------------
보존 정책은 오래된 체결을 **지운다**. 디스크는 지켜지지만 과거는 사라진다.
백테스트는 원시 체결이 있어야 다시 돌릴 수 있다. 그래서 지우기 전에
바깥으로 옮긴다.

실측(2026-09-05, 39.8만 행 1시간치):
    DB 안 몫  69.7MB   (인덱스 포함)
    CSV       32.6MB
    .csv.gz    6.5MB   ← 압축 5.0배, DB 대비 10.7배
    하루치 1.67GB → 0.16GB · 현재 16.6GB 전체 → 약 1.6GB

10.7배가 이 설계의 근거다. 원시 그대로 클라우드에 밀어 넣으면 하루 1.7GB라
무료 용량 15GB가 9일 만에 찬다. 압축하면 94일치가 들어간다.

왜 CSV + gzip 인가
------------------
표준 라이브러리만 쓴다(이 프로젝트의 규칙). Parquet 은 의존성이 필요하고,
sqlite 파일 통째 복사는 인덱스까지 같이 올려 10배 크다. CSV.gz 는
pandas·duckdb·엑셀 어디서든 열리고, 반쯤 깨져도 앞부분은 읽힌다.

**지우기 전에 올라갔는지 확인한다**
-----------------------------------
이 모듈에서 제일 중요한 건 압축이 아니라 이 순서다.

    내보내기 → 목적지에 놓기 → **다시 읽어서 검증** → 그 다음에만 삭제 허용

이 프로젝트에서 네 번 난 사고가 전부 "선언은 됐는데 실제로는 안 돌았다"였다.
아카이브에서 같은 일이 나면 되돌릴 수 없다 — 안 올라간 걸 올라갔다고 믿고
지우면 데이터가 영원히 사라진다. 그래서 삭제는 검증된 날짜만 허용한다.
검증은 파일이 있다는 확인이 아니라 **다시 읽어 행수와 해시를 맞춰보는 것**이다.

목적지
------
`MDFEED_ARCHIVE_DIR` 하나면 된다 — iCloud Drive·구글 드라이브 동기화 폴더·
외장 디스크·NFS 마운트 무엇이든 경로면 된다. 명령으로 올려야 하는 곳
(rclone, aws s3)은 `MDFEED_ARCHIVE_UPLOAD` 에 명령 틀을 준다.
어느 쪽이든 검증 단계는 같다 — 올린 뒤 **목적지에서 다시 읽는다.**
"""
from __future__ import annotations

import contextlib
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import logging
import os
import shlex
import subprocess
import time

log = logging.getLogger("mdfeed.archive")

# 검증 결과 캐시: (경로, 크기, mtime) → 통과 여부.
#
# 검증은 파일을 통째로 두 번 읽는다(해시 + gzip 행수). 삭제 상한을 구할
# 때마다 전부 다시 하면, 아카이브가 90일치 21GB 로 자란 뒤엔 매 주기
# 21GB 를 읽는다. 파일이 안 바뀌었으면 결과도 안 바뀐다.
#
# 키에 크기와 mtime(나노초)을 넣는다. 경로만으로 캐싱하면 파일이 바뀌었는데
# 옛 결과를 쓰고, 그 다음에 원본을 지운다 — 캐시가 데이터를 지운다.
#
# 그래도 구멍이 남는다: 크기가 같고 mtime 해상도 안에서 내용만 바뀌면
# 옛 결과를 쓴다. 그래서 **막 쓰인 파일은 캐시에 넣지 않는다.**
# 아카이브는 한 번 쓰고 안 고치는 파일이라, 조용해진 뒤부터 캐싱하면
# 그 창이 닫힌다.
_VERIFY_CACHE: dict[tuple[str, int, int], bool] = {}
_VERIFY_CACHE_MAX = 4096
CACHE_SETTLE_S = 10.0

# 내보낼 테이블과 컬럼. 시각 컬럼은 항상 ts 다.
ARCHIVE_TABLES = {
    "trades": ("ts", "venue", "symbol", "price", "qty", "side",
               "recv_ts", "latency_us", "seq"),
    "book_top": ("ts", "venue", "symbol", "bid", "bid_qty", "ask",
                 "ask_qty", "spread_bp"),
}
DAY_US = 86_400 * 1_000_000
# 한 번에 DB 에서 꺼내는 행 수. 하루치를 통째로 메모리에 올리면
# 950만 행 × 9열이라 GB 단위가 된다. 스트리밍으로 흘린다.
FETCH_CHUNK = 50_000


def day_bounds_us(day: dt.date) -> tuple[int, int]:
    """그 날 00:00:00 UTC 부터 다음 날 00:00:00 UTC 직전까지 (마이크로초)."""
    start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
    lo = int(start.timestamp() * 1_000_000)
    return lo, lo + DAY_US


def _name(table: str, day: dt.date) -> str:
    return f"{table}-{day.isoformat()}.csv.gz"


class Manifest(dict):
    """아카이브 한 조각의 검증 근거. 파일 옆에 .json 으로 같이 둔다.

    행수만 적으면 내용이 바뀐 걸 못 잡고, 해시만 적으면 몇 행인지 모른다.
    둘 다 적어야 "내가 넣은 그것"인지 확인할 수 있다.
    """

    @property
    def rows(self) -> int:
        return int(self.get("rows", 0))

    @property
    def sha256(self) -> str:
        return str(self.get("sha256", ""))


def export_day(storage, table: str, day: dt.date, out_dir: str) -> Manifest:
    """하루치를 .csv.gz 로 내보내고 매니페스트를 만든다.

    적재 락을 안 잡는다. SQLite 리더는 스레드마다 따로 열려 있고 WAL 이라
    쓰기와 안 부딪친다 — 여기서 락을 잡으면 하루치를 읽는 내내 적재가
    멈춘다(보존 삭제에서 이미 겪은 실패다).

    이미 있고 검증되면 다시 만들지 않는다(재실행 가능).
    """
    if table not in ARCHIVE_TABLES:
        raise ValueError(f"아카이브 대상이 아닌 테이블: {table}")
    cols = ARCHIVE_TABLES[table]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, _name(table, day))
    mpath = path + ".json"

    existing = read_manifest(mpath)
    if existing and verify_file(path, existing):
        log.info("[archive] %s 이미 있고 검증됨 — 건너뛴다", os.path.basename(path))
        existing["skipped"] = True
        return existing

    lo, hi = day_bounds_us(day)
    ph = getattr(storage, "placeholder", "?")
    sql = (f"SELECT {', '.join(cols)} FROM {table} "
           f"WHERE ts >= {ph} AND ts < {ph} ORDER BY ts")

    # 임시 이름으로 쓰고 다 끝난 뒤에 옮긴다. 도중에 죽으면 반쪽 파일이
    # 정상 이름으로 남고, 다음 실행이 그걸 완성본으로 착각한다.
    tmp = path + ".partial"
    # 앞선 실행이 중간에 죽으면 .partial 이 남는다. 완성본 행세는 못 하지만
    # (그래서 임시 이름을 쓴다) 그냥 두면 하루치 수백MB 가 목적지에 쌓인다.
    if os.path.exists(tmp):
        log.info("[archive] 앞선 실행이 남긴 %s 를 지우고 다시 만든다",
                 os.path.basename(tmp))
        with contextlib.suppress(OSError):
            os.remove(tmp)
    rows = 0
    started = time.time()
    digest = hashlib.sha256()
    # mtime=0 으로 고정한다. 같은 입력이 같은 바이트를 내야 해시가
    # 검증 수단이 된다 — gzip 은 기본으로 현재 시각을 헤더에 넣는다.
    with open(tmp, "wb") as fh:
        gz = gzip.GzipFile(fileobj=fh, mode="wb", compresslevel=6, mtime=0)
        try:
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow(cols)                       # 헤더는 행수에서 뺀다
            for chunk in _iter_rows(storage, sql, (lo, hi)):
                w.writerows(chunk)
                rows += len(chunk)
                data = buf.getvalue().encode()
                gz.write(data)
                buf.seek(0)
                buf.truncate(0)
            gz.write(buf.getvalue().encode())
        finally:
            gz.close()
    with open(tmp, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    os.replace(tmp, path)

    man = Manifest({
        "table": table,
        "day": day.isoformat(),
        "columns": list(cols),
        "rows": rows,
        "ts_from_us": lo,
        "ts_to_us": hi,
        "bytes": os.path.getsize(path),
        "sha256": digest.hexdigest(),
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "export_s": round(time.time() - started, 1),
        "format": "csv.gz (헤더 1줄 포함, UTF-8, ts 오름차순)",
    })
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(man, fh, ensure_ascii=False, indent=2)
    log.info("[archive] %s %d행 %.1fMB (%.1fs)", os.path.basename(path),
             rows, man["bytes"] / 1e6, man["export_s"])
    return man


def _iter_rows(storage, sql: str, params):
    """저장소에서 청크 단위로 흘린다. 통째로 받으면 GB 단위가 된다."""
    cursor = getattr(storage, "stream", None)
    if cursor is not None:
        yield from cursor(sql, params, FETCH_CHUNK)
        return
    # stream() 이 없으면 query() 로 떨어진다. 작은 저장소(시험용)용 경로다.
    rows = storage.query(sql, params)
    for i in range(0, len(rows), FETCH_CHUNK):
        chunk = rows[i:i + FETCH_CHUNK]
        yield [tuple(r.values()) if isinstance(r, dict) else tuple(r)
               for r in chunk]


def read_manifest(mpath: str) -> Manifest | None:
    try:
        with open(mpath, encoding="utf-8") as fh:
            return Manifest(json.load(fh))
    except (OSError, ValueError):
        return None


def verify_file(path: str, man: Manifest, *, use_cache: bool = True) -> bool:
    """매니페스트대로인지 **다시 읽어서** 확인한다.

    파일이 있는지가 아니라 내용이 맞는지를 본다. 있는지만 보면 0바이트
    파일이나 잘린 업로드를 통과시키고, 그 다음에 원본을 지운다.

    같은 파일을 반복해서 물어보면 캐시를 쓴다. 캐시 키에 크기와 mtime 이
    들어가므로 파일이 바뀌면 다시 읽는다. use_cache=False 로 강제할 수 있다.
    """
    key = None
    if use_cache:
        try:
            st = os.stat(path)
        except OSError:
            return False
        # 방금 쓰인 파일은 캐싱하지 않는다. mtime 해상도 안에서 내용이
        # 바뀌는 창을 닫는다.
        if time.time() - st.st_mtime >= CACHE_SETTLE_S:
            key = (path, st.st_size, st.st_mtime_ns)
            hit = _VERIFY_CACHE.get(key)
            if hit is not None:
                return hit
    ok = _verify_file_uncached(path, man)
    if key is not None:
        if len(_VERIFY_CACHE) >= _VERIFY_CACHE_MAX:
            _VERIFY_CACHE.clear()          # 단순하게 비운다. 다시 채워진다.
        _VERIFY_CACHE[key] = ok
    return ok


def _verify_file_uncached(path: str, man: Manifest) -> bool:
    try:
        if os.path.getsize(path) != man.get("bytes"):
            log.warning("[archive] %s 크기 불일치", os.path.basename(path))
            return False
        digest = hashlib.sha256()
        rows = 0
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                digest.update(block)
        if digest.hexdigest() != man.sha256:
            log.warning("[archive] %s 해시 불일치", os.path.basename(path))
            return False
        # 해시가 맞아도 gzip 이 실제로 풀리는지, 행수가 맞는지 본다.
        # 해시는 "내가 쓴 바이트 그대로"만 보증하지 내용이 온전한지는 모른다.
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            for i, _line in enumerate(fh):
                rows = i                       # 헤더 1줄을 빼고 세는 효과
        if rows != man.rows:
            log.warning("[archive] %s 행수 불일치: %d ≠ %d",
                        os.path.basename(path), rows, man.rows)
            return False
        return True
    except (OSError, EOFError, gzip.BadGzipFile) as e:
        log.warning("[archive] %s 검증 실패: %s: %s",
                    os.path.basename(path), type(e).__name__, e)
        return False


def upload(path: str, mpath: str, command: str) -> bool:
    """명령 틀로 올린다. `{file}` 자리에 파일 경로가 들어간다.

    예: MDFEED_ARCHIVE_UPLOAD='rclone copy {file} gdrive:mdfeed/'

    셸을 안 쓴다(shell=False). 파일명에 특수문자가 들어가면 셸이 그걸
    해석해 엉뚱한 걸 지울 수 있다. 명령은 shlex 로 쪼개서 그대로 넘긴다.
    """
    if "{file}" not in command:
        log.warning("[archive] MDFEED_ARCHIVE_UPLOAD 에 {file} 자리가 없다 — "
                    "무엇을 올릴지 알 수 없어 건너뛴다: %s", command)
        return False
    ok = True
    for f in (path, mpath):
        argv = [a.replace("{file}", f) for a in shlex.split(command)]
        try:
            r = subprocess.run(argv, capture_output=True, timeout=1800)
        except (OSError, subprocess.TimeoutExpired) as e:
            log.warning("[archive] 업로드 실패 %s: %s: %s",
                        os.path.basename(f), type(e).__name__, e)
            return False
        if r.returncode != 0:
            log.warning("[archive] 업로드 실패 %s: 종료코드 %d %s",
                        os.path.basename(f), r.returncode,
                        r.stderr.decode(errors="replace")[:200])
            ok = False
    return ok


def archived_days(archive_dir: str, table: str) -> list[dt.date]:
    """검증까지 통과한 날짜만 낸다. 삭제 허용 범위의 근거가 된다."""
    out = []
    try:
        names = sorted(os.listdir(archive_dir))
    except OSError:
        return out
    prefix = f"{table}-"
    for n in names:
        if not (n.startswith(prefix) and n.endswith(".csv.gz")):
            continue
        path = os.path.join(archive_dir, n)
        man = read_manifest(path + ".json")
        if not man or not verify_file(path, man):
            continue
        try:
            out.append(dt.date.fromisoformat(n[len(prefix):-len(".csv.gz")]))
        except ValueError:
            continue
    return out


def safe_delete_cutoff_us(archive_dir: str, tables=None) -> int:
    """여기 이전은 지워도 된다 — **검증된 아카이브가 연속으로 있는 구간**.

    구멍이 있으면 거기서 멈춘다. 3일치가 있고 4일째가 빠지고 5일째가 있어도
    5일째까지 지우면 4일째가 영원히 사라진다. 가장 이른 날부터 끊기지 않고
    이어지는 데까지만 허용한다.

    모든 대상 테이블이 함께 있는 날만 센다. trades 만 올리고 book_top 을
    안 올린 날을 지우면 호가가 사라진다.
    """
    tables = list(tables or ARCHIVE_TABLES)
    per = {t: set(archived_days(archive_dir, t)) for t in tables}
    common = set.intersection(*per.values()) if per else set()
    if not common:
        return 0
    day = min(common)
    while day in common:
        day += dt.timedelta(days=1)
    # day 는 처음으로 **없는** 날. 그 시작 시각 이전까지만 지운다.
    return day_bounds_us(day)[0]


def pending_days(storage, archive_dir: str, table: str,
                 lag_s: float = 3600.0, now: float | None = None) -> list[dt.date]:
    """아직 안 올린 날들. 오늘과 너무 최근인 날은 뺀다.

    끝나지 않은 날을 올리면 반쪽이 올라가고, 그 뒤에 온 체결은 영원히
    아카이브에 없다. 하루가 끝나고 lag_s 가 지나야 대상이 된다 —
    거래소 이벤트 시각 기준이라 늦게 도착하는 틱이 있다.
    """
    # MIN 과 MAX 를 한 쿼리에 같이 쓰면 SQLite 가 인덱스 최적화를 못 걸고
    # 전체를 훑는다 — 실측 7,400만 행에서 **49초**. 그동안 무슨 일이
    # 벌어지는지 헬스에는 아무 표시가 없다. 따로 물으면 인덱스로 각각
    # 한 행만 읽는다.
    try:
        lo_row = storage.query(f"SELECT MIN(ts) AS v FROM {table}")[0]
        hi_row = storage.query(f"SELECT MAX(ts) AS v FROM {table}")[0]
    except Exception:                                     # noqa: BLE001
        return []
    row = {"lo": lo_row["v"], "hi": hi_row["v"]}
    if not row["lo"]:
        return []
    now = now if now is not None else time.time()
    settled_before = now - lag_s
    have = set(archived_days(archive_dir, table))
    first = dt.datetime.fromtimestamp(row["lo"] / 1e6, dt.timezone.utc).date()
    last = dt.datetime.fromtimestamp(row["hi"] / 1e6, dt.timezone.utc).date()
    out = []
    day = first
    while day <= last:
        _lo, hi = day_bounds_us(day)
        if hi / 1e6 <= settled_before and day not in have:
            out.append(day)
        day += dt.timedelta(days=1)
    return out
