"""프로세스 자원 자기 계측 — 누수는 오래 돌려야만 보인다.

왜 필요한가
-----------
이 프로젝트의 최장 연속 구동은 한 시간 남짓이었다. 그 정도로는 메모리 누수도
파일 디스크립터 누수도 안 보인다. 마켓데이터 서비스는 몇 주씩 돌아야 하고,
누수는 **천천히 자라다가 새벽에 터진다.**

누수가 생기기 쉬운 자리가 이 코드에 실제로 있다.

* 구독자별 큐 — 연결이 새면 큐도 샌다
* `snapshot` / `_last` / `symbols_seen` 같은 심볼 캐시 — 심볼이 늘기만 하고 안 준다
* `QualityMonitor.recent` — 상한을 안 두면 무한히 쌓인다 (테스트로 막아 뒀다)
* 소켓 — `close()` 를 빼먹으면 fd 가 샌다

그래서 각 프로세스가 **자기 RSS 와 fd 수를 스스로 보고**하게 한다.
문제는 사람이 볼 때가 아니라 자라는 동안 기록돼야 한다.

이식성
------
`/proc` 은 리눅스에만 있다. 개발은 macOS 에서 하므로 양쪽을 모두 다룬다.
psutil 을 쓰면 간단하지만 핵심 경로 의존성 0 원칙을 깬다 —
자원 계측 하나 때문에 배포 대상 전부에 패키지를 깔게 할 수는 없다.
"""

from __future__ import annotations

import os
import resource
import sys
import threading
import time

_IS_LINUX = sys.platform.startswith("linux")
_PAGE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def rss_bytes() -> int:
    """현재 상주 메모리(byte).

    리눅스는 /proc/self/statm 이 정확한 현재값을 준다.
    macOS 에는 없어서 getrusage 의 ru_maxrss(최대값)로 대신한다 —
    현재값이 아니라 **최고 수위**라 감소는 못 보지만 증가는 보인다.
    누수 탐지에는 그걸로 충분하다.
    """
    if _IS_LINUX:
        try:
            with open("/proc/self/statm") as fh:
                return int(fh.read().split()[1]) * _PAGE
        except Exception:                            # noqa: BLE001
            pass
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # 리눅스는 KB, macOS 는 byte 로 준다
    return ru if not _IS_LINUX else ru * 1024


def rss_is_peak() -> bool:
    """RSS 값이 현재값이 아니라 최고 수위인가 (macOS)."""
    return not _IS_LINUX


def fd_count() -> int:
    """열려 있는 파일 디스크립터 수. 소켓 누수가 여기 먼저 나타난다."""
    for d in ("/proc/self/fd", "/dev/fd"):
        try:
            return len(os.listdir(d))
        except Exception:                            # noqa: BLE001
            continue
    return -1


def fd_limit() -> int:
    try:
        return resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    except Exception:                                # noqa: BLE001
        return -1


class ResourceTracker:
    """자원 사용을 주기적으로 표본화하고 **증가 추세**를 낸다.

    절대값만 보면 판단이 안 된다. RSS 200MB 가 문제인지 아닌지는
    그게 자라고 있는지에 달렸다. 그래서 기울기를 낸다.

    최소제곱 직선의 기울기를 쓴다. 표본이 시간당 몇 개뿐이라 정교한 방법이
    필요 없고, 무엇보다 **읽는 사람이 계산을 검증할 수 있어야** 한다.

    짧은 관측에서는 기울기를 내지 않는다
    ------------------------------------
    기동 직후 10분 표본으로 "시간당" 기울기를 내면 한 번의 흔들림이 6배로 증폭된다.
    실제로 기동 3분 만에 `fd +16.4/h`, `RSS +13.4MB/h` 가 나와 Prometheus 알람이
    대기 상태로 들어갔다. 전부 노이즈였다.

    누수 알람이 기동 때마다 울리면 사람이 그 알람을 무시하게 되고, 그러면 진짜
    누수가 생겨도 무시한다. `MIN_TRACK_S` 를 넘기 전에는 0 을 보고한다.
    """

    __slots__ = ("_samples", "_max", "started_at", "_lock", "min_track_s")

    def __init__(self, max_samples: int = 720, min_track_s: float = 900.0):
        self._samples: list[tuple[float, int, int]] = []
        self._max = max_samples
        self.started_at = time.time()
        self.min_track_s = min_track_s
        self._lock = threading.Lock()

    def sample(self) -> tuple[float, int, int]:
        s = (time.time(), rss_bytes(), fd_count())
        with self._lock:
            self._samples.append(s)
            del self._samples[:-self._max]
        return s

    @staticmethod
    def _slope(xs: list[float], ys: list[float]) -> float:
        """최소제곱 기울기. 표본이 2개 미만이면 0."""
        n = len(xs)
        if n < 2:
            return 0.0
        mx = sum(xs) / n
        my = sum(ys) / n
        den = sum((x - mx) ** 2 for x in xs)
        if den == 0:
            return 0.0
        return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den

    def report(self) -> dict:
        with self._lock:
            samples = list(self._samples)
        cur_rss, cur_fd = rss_bytes(), fd_count()
        out = {
            "rss_mb": round(cur_rss / 1e6, 1),
            "rss_is_peak_not_current": rss_is_peak(),
            "fd_open": cur_fd,
            "fd_limit": fd_limit(),
            "samples": len(samples),
            "tracked_s": round(time.time() - self.started_at, 1),
            "threads": threading.active_count(),
        }
        # 표본이 부족해도 키는 항상 내보낸다. 없으면 알람이 평가되지 않는다.
        # 다만 관측이 짧으면 0 으로 둔다 — 노이즈를 시간 단위로 외삽하면
        # 기동 때마다 누수 알람이 울리고, 그러면 그 알람이 무의미해진다.
        out["rss_growth_mb_per_hour"] = 0.0
        out["fd_growth_per_hour"] = 0.0
        tracked = time.time() - self.started_at
        out["growth_valid"] = tracked >= self.min_track_s
        out["min_track_s"] = self.min_track_s
        if len(samples) >= 3 and tracked >= self.min_track_s:
            t0 = samples[0][0]
            xs = [(s[0] - t0) / 3600.0 for s in samples]        # 시간 단위
            out["rss_growth_mb_per_hour"] = round(
                self._slope(xs, [s[1] / 1e6 for s in samples]), 2)
            out["fd_growth_per_hour"] = round(
                self._slope(xs, [float(s[2]) for s in samples]), 2)
            out["growth_from_samples"] = len(samples)
            out["rss_min_mb"] = round(min(s[1] for s in samples) / 1e6, 1)
            out["rss_max_mb"] = round(max(s[1] for s in samples) / 1e6, 1)
            out["fd_max"] = max(s[2] for s in samples)
        return out
