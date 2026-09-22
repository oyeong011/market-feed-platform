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
import pathlib
import re
import shlex
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass

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
REMOTE_RECEIPT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class RemoteArtifactReceipt:
    schema_version: int
    object_id: str
    artifact: str
    manifest: str
    table: str | None
    day: str | None
    local_sha256: str
    local_bytes: int
    local_rows: int
    remote_fetch_sha256: str
    remote_fetch_bytes: int
    remote_fetch_rows: int
    manifest_sha256: str
    remote_manifest_sha256: str
    source_table: str | None
    source_day: str | None
    source_from_us: int | None
    source_to_us: int | None
    source_rows: int | None
    source_content_sha256: str | None
    verified_at: str
    transport: str
    status: str

    def to_json(self) -> dict[str, str | int | None]:
        return {
            "schema_version": self.schema_version,
            "object_id": self.object_id,
            "artifact": self.artifact,
            "manifest": self.manifest,
            "table": self.table,
            "day": self.day,
            "local_sha256": self.local_sha256,
            "local_bytes": self.local_bytes,
            "local_rows": self.local_rows,
            "remote_fetch_sha256": self.remote_fetch_sha256,
            "remote_fetch_bytes": self.remote_fetch_bytes,
            "remote_fetch_rows": self.remote_fetch_rows,
            "manifest_sha256": self.manifest_sha256,
            "remote_manifest_sha256": self.remote_manifest_sha256,
            "source_table": self.source_table,
            "source_day": self.source_day,
            "source_from_us": self.source_from_us,
            "source_to_us": self.source_to_us,
            "source_rows": self.source_rows,
            "source_content_sha256": self.source_content_sha256,
            "verified_at": self.verified_at,
            "transport": self.transport,
            "status": self.status,
        }


class RemoteArtifactError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

    def __str__(self) -> str:
        return self.reason


def day_bounds_us(day: dt.date) -> tuple[int, int]:
    """그 날 00:00:00 UTC 부터 다음 날 00:00:00 UTC 직전까지 (마이크로초)."""
    start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
    lo = int(start.timestamp() * 1_000_000)
    return lo, lo + DAY_US


def _name(table: str, day: dt.date) -> str:
    return f"{table}-{day.isoformat()}.csv.gz"


class Manifest(dict[str, object]):
    """아카이브 한 조각의 검증 근거. 파일 옆에 .json 으로 같이 둔다.

    행수만 적으면 내용이 바뀐 걸 못 잡고, 해시만 적으면 몇 행인지 모른다.
    둘 다 적어야 "내가 넣은 그것"인지 확인할 수 있다.
    """

    @property
    def rows(self) -> int:
        value = self.get("rows", 0)
        return int(value) if isinstance(value, (int, float, str)) else 0

    @property
    def bytes(self) -> int:
        value = self.get("bytes", 0)
        return int(value) if isinstance(value, (int, float, str)) else 0

    @property
    def export_s(self) -> float:
        value = self.get("export_s", 0.0)
        return float(value) if isinstance(value, (int, float, str)) else 0.0

    @property
    def sha256(self) -> str:
        return str(self.get("sha256", ""))


def _query_bounds(storage, lo: int, hi: int) -> tuple[object, object]:
    if getattr(storage, "kind", "") == "postgres":
        from mdfeed.migration.time import epoch_us_to_datetime

        return epoch_us_to_datetime(lo), epoch_us_to_datetime(hi)
    return lo, hi


def _version_paths(out_dir: str, man: Manifest) -> tuple[str, str]:
    object_id = object_id_for_manifest(man)
    path = os.path.join(out_dir, object_id)
    return path, path + ".json"


def _restore_versioned_archive(out_dir: str, table: str, day: dt.date,
                               path: str, mpath: str) -> Manifest | None:
    root = pathlib.Path(out_dir) / "archive" / table / day.isoformat()
    try:
        manifests = sorted(root.glob("*.csv.gz.json"))
    except OSError:
        return None
    candidates: list[tuple[int, pathlib.Path, Manifest]] = []
    for manifest_path in manifests:
        artifact_path = pathlib.Path(str(manifest_path)[:-5])
        man = read_manifest(str(manifest_path))
        if man is None or not verify_file(str(artifact_path), man, use_cache=False):
            continue
        candidates.append((man.rows, manifest_path, man))
    if not candidates:
        return None
    _rows, manifest_path, man = max(candidates, key=lambda item: item[0])
    artifact_path = pathlib.Path(str(manifest_path)[:-5])
    shutil.copy2(artifact_path, path)
    shutil.copy2(manifest_path, mpath)
    restored = read_manifest(mpath)
    if restored is None or not verify_file(path, restored, use_cache=False):
        return None
    restored["skipped"] = True
    return restored


def _preserve_versioned_archive(out_dir: str, path: str, mpath: str, man: Manifest) -> None:
    version_path, version_manifest = _version_paths(out_dir, man)
    os.makedirs(os.path.dirname(version_path), exist_ok=True)
    if not os.path.exists(version_path):
        shutil.copy2(path, version_path)
    if not os.path.exists(version_manifest):
        shutil.copy2(mpath, version_manifest)


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
        _preserve_versioned_archive(out_dir, path, mpath, existing)
        log.info("[archive] %s 이미 있고 검증됨 — 건너뛴다", os.path.basename(path))
        existing["skipped"] = True
        return existing
    restored = _restore_versioned_archive(out_dir, table, day, path, mpath)
    if restored is not None:
        log.info("[archive] %s 버전 보관본 복구 — 건너뛴다", os.path.basename(path))
        return restored

    lo, hi = day_bounds_us(day)
    ph = getattr(storage, "placeholder", "?")
    order_cols = ", ".join(cols)
    sql = (f"SELECT {', '.join(cols)} FROM {table} "
           f"WHERE ts >= {ph} AND ts < {ph} ORDER BY {order_cols}")

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
    source_digest = hashlib.sha256()
    # mtime=0 으로 고정한다. 같은 입력이 같은 바이트를 내야 해시가
    # 검증 수단이 된다 — gzip 은 기본으로 현재 시각을 헤더에 넣는다.
    with open(tmp, "wb") as fh:
        gz = gzip.GzipFile(fileobj=fh, mode="wb", compresslevel=6, mtime=0)
        try:
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow(cols)                       # 헤더는 행수에서 뺀다
            for chunk in _iter_rows(storage, sql, _query_bounds(storage, lo, hi)):
                w.writerows(chunk)
                rows += len(chunk)
                data = buf.getvalue().encode()
                source_digest.update(data)
                gz.write(data)
                buf.seek(0)
                buf.truncate(0)
            tail = buf.getvalue().encode()
            source_digest.update(tail)
            gz.write(tail)
        finally:
            gz.close()
    with open(tmp, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            if not isinstance(block, bytes):
                raise TypeError("archive digest expected bytes")
            digest.update(block)
    os.replace(tmp, path)

    man = Manifest({
        "table": table,
        "day": day.isoformat(),
        "columns": list(cols),
        "rows": rows,
        "ts_from_us": lo,
        "ts_to_us": hi,
        "source_table": table,
        "source_day": day.isoformat(),
        "source_from_us": lo,
        "source_to_us": hi,
        "source_rows": rows,
        "source_content_sha256": source_digest.hexdigest(),
        "bytes": os.path.getsize(path),
        "sha256": digest.hexdigest(),
        "format": "csv.gz (헤더 1줄 포함, UTF-8, ts 오름차순)",
    })
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(man, fh, ensure_ascii=False, indent=2)
    _preserve_versioned_archive(out_dir, path, mpath, man)
    log.info("[archive] %s %d행 %.1fMB (%.1fs)", os.path.basename(path),
             rows, man.bytes / 1e6, round(time.time() - started, 1))
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
                if not isinstance(block, bytes):
                    raise TypeError("archive digest expected bytes")
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


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _redact(text: str) -> str:
    redacted = re.sub(r"(?i)(password|token|secret|key)=\S+", r"\1=<redacted>", text)
    redacted = re.sub(r"(?i)(--(?:password|token|secret|key))\s+\S+", r"\1 <redacted>", redacted)
    return redacted[:400]


def _transport_id(command: str) -> str:
    try:
        parts = shlex.split(command)
    except ValueError:
        return "invalid"
    if not parts:
        return "empty"
    return pathlib.Path(parts[0]).name


def _validate_object_id(object_id: str) -> None:
    if not object_id or object_id.startswith("/") or ".." in pathlib.PurePosixPath(object_id).parts:
        raise RemoteArtifactError("remote object id must be a relative immutable name")


def _run_transfer(command: str, *, source: str | None = None,
                  object_id: str, destination: str | None = None,
                  timeout_s: float = 1800.0) -> None:
    if not command.strip():
        raise RemoteArtifactError("remote transfer command is missing")
    _validate_object_id(object_id)
    required = {"{object}"}
    if source is not None:
        required.add("{source}")
    if destination is not None:
        required.add("{destination}")
    missing = [placeholder for placeholder in sorted(required) if placeholder not in command]
    if missing:
        raise RemoteArtifactError(f"remote command missing placeholders: {', '.join(missing)}")
    if "{file}" in command:
        raise RemoteArtifactError("remote command must use {source}, {object}, and {destination}")
    try:
        parts = shlex.split(command)
    except ValueError as exc:
        raise RemoteArtifactError(f"remote command parse failed: {_redact(str(exc))}") from exc
    values = {
        "{source}": source or "",
        "{object}": object_id,
        "{destination}": destination or "",
    }
    argv = [part.replace("{source}", values["{source}"])
            .replace("{object}", values["{object}"])
            .replace("{destination}", values["{destination}"])
            for part in parts]
    try:
        result = subprocess.run(argv, capture_output=True, timeout=timeout_s,
                                check=False)
    except OSError as exc:
        raise RemoteArtifactError(f"remote transfer failed: {type(exc).__name__}: {_redact(str(exc))}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RemoteArtifactError(f"remote transfer timed out after {timeout_s:g}s: {_redact(str(exc))}") from exc
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace")
        raise RemoteArtifactError(
            f"remote transfer exited {result.returncode}: {_redact(stderr)}")


def _write_receipt(receipt: RemoteArtifactReceipt, receipt_dir: str) -> None:
    os.makedirs(receipt_dir, exist_ok=True)
    safe_name = receipt.object_id.replace("/", "_") + ".json"
    path = os.path.join(receipt_dir, safe_name)
    fd, tmp = tempfile.mkstemp(prefix=safe_name, suffix=".partial", dir=receipt_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(receipt.to_json(), fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(OSError):
            os.remove(tmp)


def _manifest_int(man: Manifest, key: str) -> int | None:
    value = man.get(key)
    return value if isinstance(value, int) else None


def verify_remote_roundtrip(
    path: str,
    mpath: str,
    object_id: str,
    push_command: str,
    fetch_command: str,
    receipt_dir: str,
    *,
    table: str | None = None,
    day: dt.date | None = None,
    timeout_s: float = 1800.0,
    push_first: bool = True,
) -> RemoteArtifactReceipt:
    man = read_manifest(mpath)
    if man is None:
        raise RemoteArtifactError("local manifest is missing or invalid")
    if not verify_file(path, man, use_cache=False):
        raise RemoteArtifactError("local artifact does not match its manifest")
    manifest_sha = _sha256_file(mpath)
    manifest_object_id = f"{object_id}.json"
    if push_first:
        _run_transfer(push_command, source=path, object_id=object_id, timeout_s=timeout_s)
        _run_transfer(push_command, source=mpath, object_id=manifest_object_id, timeout_s=timeout_s)
    with tempfile.TemporaryDirectory(prefix="mdfeed-remote-fetch-") as tmpdir:
        fetched_artifact = os.path.join(tmpdir, pathlib.Path(path).name)
        fetched_manifest = fetched_artifact + ".json"
        _run_transfer(
            fetch_command,
            object_id=object_id,
            destination=fetched_artifact,
            timeout_s=timeout_s,
        )
        _run_transfer(
            fetch_command,
            object_id=manifest_object_id,
            destination=fetched_manifest,
            timeout_s=timeout_s,
        )
        fetched_manifest_sha = _sha256_file(fetched_manifest)
        if fetched_manifest_sha != manifest_sha:
            raise RemoteArtifactError("fetched manifest digest does not match local manifest")
        fetched_man = read_manifest(fetched_manifest)
        if fetched_man is None or not verify_file(fetched_artifact, fetched_man, use_cache=False):
            raise RemoteArtifactError("fetched artifact does not match fetched manifest")
        if (fetched_man.sha256 != man.sha256 or fetched_man.rows != man.rows
                or fetched_man.get("bytes") != man.get("bytes")):
            raise RemoteArtifactError("fetched manifest content does not match local manifest")
        receipt = RemoteArtifactReceipt(
            schema_version=REMOTE_RECEIPT_SCHEMA_VERSION,
            object_id=object_id,
            artifact=os.path.basename(path),
            manifest=os.path.basename(mpath),
            table=table,
            day=day.isoformat() if day is not None else None,
            local_sha256=man.sha256,
            local_bytes=man.bytes,
            local_rows=man.rows,
            remote_fetch_sha256=fetched_man.sha256,
            remote_fetch_bytes=fetched_man.bytes,
            remote_fetch_rows=fetched_man.rows,
            manifest_sha256=manifest_sha,
            remote_manifest_sha256=fetched_manifest_sha,
            source_table=str(man.get("source_table")) if man.get("source_table") is not None else None,
            source_day=str(man.get("source_day")) if man.get("source_day") is not None else None,
            source_from_us=_manifest_int(man, "source_from_us"),
            source_to_us=_manifest_int(man, "source_to_us"),
            source_rows=_manifest_int(man, "source_rows"),
            source_content_sha256=(str(man.get("source_content_sha256"))
                                   if man.get("source_content_sha256") is not None else None),
            verified_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            transport=_transport_id(fetch_command),
            status="verified",
        )
    _write_receipt(receipt, receipt_dir)
    return receipt


def object_id_for_manifest(man: Manifest) -> str:
    table = man.get("source_table") or man.get("table")
    day = man.get("source_day") or man.get("day")
    source_sha = man.get("source_content_sha256")
    if not isinstance(table, str) or not isinstance(day, str):
        raise RemoteArtifactError("archive manifest missing table/day for object id")
    if not isinstance(source_sha, str) or len(source_sha) != 64:
        raise RemoteArtifactError("archive manifest missing source content digest")
    return f"archive/{table}/{day}/{source_sha}.csv.gz"


def archive_day_remote(
    storage,
    table: str,
    day: dt.date,
    out_dir: str,
    push_command: str,
    fetch_command: str,
    *,
    receipt_dir: str | None = None,
    timeout_s: float = 1800.0,
) -> tuple[Manifest, RemoteArtifactReceipt]:
    man = export_day(storage, table, day, out_dir)
    path = os.path.join(out_dir, _name(table, day))
    object_id = object_id_for_manifest(man)
    receipt = verify_remote_roundtrip(
        path,
        path + ".json",
        object_id,
        push_command,
        fetch_command,
        receipt_dir or out_dir,
        table=table,
        day=day,
        timeout_s=timeout_s,
        push_first=True,
    )
    return man, receipt


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
            r = subprocess.run(argv, capture_output=True, timeout=1800,
                               check=False)
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


def _looks_like_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        ch in "0123456789abcdef" for ch in value.lower())


def remote_receipt_days(receipt_dir: str, table: str) -> set[dt.date]:
    days: set[dt.date] = set()
    try:
        names = os.listdir(receipt_dir)
    except OSError:
        return days
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(receipt_dir, name), encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        day_value = data.get("day")
        if data.get("schema_version") != REMOTE_RECEIPT_SCHEMA_VERSION:
            continue
        if data.get("status") != "verified" or data.get("table") != table:
            continue
        if not isinstance(day_value, str):
            continue
        if not all(_looks_like_sha256(data.get(key)) for key in (
                "local_sha256", "remote_fetch_sha256", "manifest_sha256",
                "remote_manifest_sha256", "source_content_sha256")):
            continue
        if data.get("local_sha256") != data.get("remote_fetch_sha256"):
            continue
        if data.get("manifest_sha256") != data.get("remote_manifest_sha256"):
            continue
        if data.get("source_table") != table or data.get("source_day") != day_value:
            continue
        try:
            days.add(dt.date.fromisoformat(day_value))
        except ValueError:
            continue
    return days


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
                 lag_s: float = 3600.0, now: float | None = None,
                 remote_receipt_dir: str | None = None) -> list[dt.date]:
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
    if remote_receipt_dir is not None:
        have = remote_receipt_days(remote_receipt_dir, table)
    else:
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
