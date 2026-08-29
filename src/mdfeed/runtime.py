"""서비스 공통 런타임 — 로깅, 시그널, PID 파일, 우아한 종료.

리눅스에서 서비스를 '운영 가능하게' 만드는 최소 조건을 여기 모았다.

* **SIGTERM 처리**: systemd 는 정지 시 SIGTERM 을 보내고 TimeoutStopSec 뒤에
  SIGKILL 한다. SIGTERM 을 안 잡으면 버퍼에 든 틱이 그대로 사라지고, DB 커넥션이
  half-open 으로 남는다. 여기서 잡아 flush/close 까지 하고 내려간다.
* **그 정리에 기한을 둔다**: 잡기만 하고 정리가 안 끝나면 결과는 안 잡은 것과
  같다 — SIGKILL 이 30초 뒤에 똑같이 버퍼를 날린다. 다른 게 있다면 그때는
  **왜 안 끝났는지 아무 기록도 안 남는다**는 것뿐이다.
  기한이 지나면 남은 태스크 이름을 찍고 우리가 먼저 내려간다.
  (2026-08-29: 어댑터 복구 경로에서 기한 없는 await 하나가 11.2시간 무음을
  만들었다. 종료 경로도 같은 모양이라 같이 막는다.)
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


EXIT_SHUTDOWN_TIMEOUT = 3


def _task_label(t: asyncio.Task) -> str:
    """태스크를 사람이 읽을 이름으로. 어느 코루틴이 안 끝났는지가 진단의 전부다."""
    coro = getattr(t, "get_coro", lambda: None)()
    name = getattr(coro, "__qualname__", None) or t.get_name()
    return str(name)


def _force_exit(code: int) -> None:
    """시험에서 갈아끼울 수 있게 분리해 둔다."""
    os._exit(code)


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
        main = asyncio.ensure_future(main_coro_factory(stop))
        waiter = asyncio.ensure_future(stop.wait())

        done, _ = await asyncio.wait({main, waiter},
                                     return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        if main in done:
            return main.result()                  # 정지 요청 없이 스스로 끝난 경우

        # 여기부터는 정지 요청을 받은 상태다. 정리에 기한을 잰다.
        grace = getattr(cfg, "shutdown_grace_s", 20.0)
        done2, _ = await asyncio.wait({main}, timeout=grace)
        if main in done2:
            return main.result()

        pending = sorted({_task_label(t) for t in asyncio.all_tasks()
                          if t is not asyncio.current_task() and not t.done()})
        log.error("정리가 %.1fs 안에 안 끝났다 — 남은 태스크 %d종: %s",
                  grace, len(pending), ", ".join(pending)[:400])
        log.error("여기서 안 내려가면 systemd 가 SIGKILL 한다. "
                  "같은 결과라면 이유를 남기고 우리가 내려간다")
        remove_pidfile(pid)
        logging.shutdown()
        # asyncio.run 의 teardown 은 남은 태스크를 취소하고 **기다린다**.
        # 취소를 안 받는 태스크가 하나라도 있으면 거기서 또 걸린다.
        # 그래서 루프를 정리하지 않고 나간다.
        _force_exit(EXIT_SHUTDOWN_TIMEOUT)

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
