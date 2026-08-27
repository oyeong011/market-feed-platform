"""서비스 공통 런타임 — 로깅, 시그널, PID 파일, 우아한 종료.

리눅스에서 서비스를 '운영 가능하게' 만드는 최소 조건을 여기 모았다.

* **SIGTERM 처리**: systemd 는 정지 시 SIGTERM 을 보내고 TimeoutStopSec 뒤에
  SIGKILL 한다. SIGTERM 을 안 잡으면 버퍼에 든 틱이 그대로 사라지고, DB 커넥션이
  half-open 으로 남는다. 여기서 잡아 flush/close 까지 하고 내려간다.
* **SIGHUP**: 로그 레벨 재적용. logrotate 후 파일 핸들 재개용 훅이기도 하다.
* **PID 파일**: 컨테이너 밖(systemd/직접 실행)에서 ops.sh 가 프로세스를 찾는 근거.
* **구조화 로그**: MDFEED_LOG_JSON=1 이면 JSON 한 줄씩 — 로그 수집기가 파싱한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import time

_START = time.time()


def make_tracker():
    """서비스마다 하나씩. httpd.health_routes 에 넘기면 /healthz·/metrics 에 실린다."""
    from .procstat import ResourceTracker
    return ResourceTracker()


async def sample_resources(tracker, stop, interval_s: float = 30.0) -> None:
    """자원 표본화 루프. 누수는 표본이 쌓여야 기울기가 나온다."""
    import asyncio
    while not stop.is_set():
        tracker.sample()
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            continue


class JSONFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created))
                  + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(service: str, level: str = "INFO", as_json: bool = False) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)
    h = logging.StreamHandler(sys.stdout)     # 컨테이너/journald 규약: stdout
    if as_json:
        h.setFormatter(JSONFormatter(service))
    else:
        h.setFormatter(logging.Formatter(
            f"%(asctime)s [%(levelname)-5s] [{service}] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
    root.addHandler(h)


def write_pidfile(run_dir: str, service: str) -> str:
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, f"{service}.pid")
    with open(path, "w") as fh:
        fh.write(str(os.getpid()))
    return path


def remove_pidfile(path: str) -> None:
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


def install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event,
                            service: str) -> None:
    log = logging.getLogger(service)

    def _term(sig):
        log.info("시그널 %s 수신 → 우아한 종료 시작", signal.Signals(sig).name)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _term, sig)

    def _hup():
        lvl = os.getenv("MDFEED_LOG_LEVEL", "INFO").upper()
        logging.getLogger().setLevel(getattr(logging, lvl, logging.INFO))
        log.info("SIGHUP: 로그 레벨을 %s 로 재적용", lvl)

    with contextlib.suppress(NotImplementedError, AttributeError):
        loop.add_signal_handler(signal.SIGHUP, _hup)


def run(service: str, main_coro_factory, cfg) -> int:
    """서비스 진입점 공통 래퍼. main(stop_event) 코루틴을 받아 굴린다."""
    setup_logging(service, cfg.log_level, cfg.log_json)
    log = logging.getLogger(service)
    pid = write_pidfile(cfg.run_dir, service)
    log.info("기동 pid=%d run_dir=%s", os.getpid(), cfg.run_dir)

    async def _wrap():
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        install_signal_handlers(loop, stop, service)
        await main_coro_factory(stop)

    try:
        asyncio.run(_wrap())
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception:                             # noqa: BLE001
        log.exception("치명적 오류로 종료")
        return 1
    finally:
        remove_pidfile(pid)
        log.info("종료 (가동 %.1fs)", time.time() - _START)
