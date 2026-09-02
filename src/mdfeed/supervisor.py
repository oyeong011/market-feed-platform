"""장수 태스크 감독 — 같은 사고가 네 번 난 뒤에 만든 계약.

왜 이게 필요한가
----------------
2026-08-28 부터 9-02 까지 **같은 가족의 사고가 네 번** 났다.
증상은 매번 같았다 — 업스트림 하나가 조용히 멎고, 헬스는 정상을 보고한다.
그런데 원인은 매번 달랐다.

    08-28  2.97시간  create_task 결과를 아무도 안 봐서 예외가 태스크 안에 갇혔다
    08-31  11.2시간  취소한 세션의 정리가 안 끝나 재접속이 그 뒤에 줄 섰다
    09-01  29.4시간  감독이 "정지 요청 없는 반환/취소"를 정상 종료로 보고 이탈했다
    09-02  30시간    태스크를 변수에 안 담아 GC 가 수거해 갔다

네 번 다 **개별적으로** 고쳤다. 다섯 번째 변종이 안 나온다는 보장이 없다.
실제로 코드를 세어 보니 장수 태스크 27개 중 감독되는 건 4개뿐이었고,
띄우는 방식이 서비스마다 제각각이었다(gather / 리스트 / 맨 create_task).

그래서 **진입점을 하나로 만든다.** 여기를 거치면 다음이 보장된다.

1. **강한 참조를 보관한다** — GC 가 수거해 갈 수 없다.
   ("Task was destroyed but it is pending!" 이 09-02 사고의 로그였다)
2. **어떤 이유로 끝나든 되살린다** — 정지 요청이 있을 때만 멈춘다.
   정상 반환도, 취소도, 예외도 정지 요청이 없으면 사건이다.
3. **태스크별 상태를 드러낸다** — 합쳐서 세면 하나가 죽어도 안 보인다.

이건 성능 장치가 아니라 **판정 장치**다. 되살리는 것보다 중요한 건
되살렸다는 사실이 남는 것이다 — 되살리기가 잦으면 원인을 고쳐야 한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

log = logging.getLogger("mdfeed.supervisor")

RESTART_BACKOFF_MAX_S = 30.0


class Supervisor:
    """한 서비스의 장수 태스크를 전부 들고 있는 곳."""

    def __init__(self, service: str, registry=None,
                 backoff_max_s: float = RESTART_BACKOFF_MAX_S):
        self.service = service
        self.registry = registry
        self.backoff_max_s = backoff_max_s
        # 이름 → 상태. **이 dict 가 강한 참조를 쥔다.**
        self.tasks: dict[str, dict] = {}
        if registry is not None:
            registry.declare_counters("task_restarts_total")

    def spawn(self, name: str, factory, stop: asyncio.Event, *,
              restart: bool = True) -> asyncio.Task:
        """장수 태스크를 띄운다.

        `factory` 는 코루틴을 **새로 만드는** 함수다. 코루틴 객체를 받으면
        한 번 쓰고 나서 되살릴 수 없다 — 되살리기가 있는 이상 팩토리여야 한다.
        """
        state = {"name": name, "restarts": 0, "started_at": time.time(),
                 "last_exit": None, "alive": True, "restart": restart}
        task = asyncio.ensure_future(self._run(name, factory, stop, state))
        state["task"] = task                       # 강한 참조. GC 수거 방지
        self.tasks[name] = state
        return task

    async def _run(self, name: str, factory, stop: asyncio.Event,
                   state: dict) -> None:
        backoff = 0.5
        while not stop.is_set():
            state["alive"] = True
            try:
                await factory()
                if stop.is_set():
                    state["last_exit"] = "stopped"
                    return
                # 정지 요청이 없는데 끝났다. 정상이 아니라 **사건**이다.
                state["last_exit"] = "returned"
                log.error("[%s] %s 가 정지 요청 없이 끝났다 — 되살린다", self.service, name)
            except asyncio.CancelledError:
                if stop.is_set():
                    state["last_exit"] = "cancelled(stop)"
                    raise
                state["last_exit"] = "cancelled"
                log.error("[%s] %s 가 정지 요청 없이 취소됐다 — 되살린다", self.service, name)
            except BaseException as e:             # noqa: BLE001
                state["last_exit"] = f"{type(e).__name__}: {e}"
                log.exception("[%s] %s 가 죽었다 — 되살린다: %s", self.service, name, e)

            state["alive"] = False
            if not state["restart"] or stop.is_set():
                return
            state["restarts"] += 1
            if self.registry is not None:
                self.registry.counter("task_restarts_total", task=name)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, self.backoff_max_s)

    def report(self) -> dict:
        """태스크별 상태. 합쳐서 세면 하나가 죽어도 안 보인다."""
        now = time.time()
        items = [{"name": s["name"], "alive": s["alive"],
                  "restarts": s["restarts"], "last_exit": s["last_exit"],
                  "uptime_s": round(now - s["started_at"], 1)}
                 for s in self.tasks.values()]
        return {"tasks": sorted(items, key=lambda x: -x["restarts"]),
                "task_restarts": sum(i["restarts"] for i in items),
                # 되살리기가 잦다는 건 고쳐야 할 원인이 있다는 뜻이다
                "unstable_tasks": [i["name"] for i in items if i["restarts"] > 3]}

    async def shutdown(self, timeout: float = 5.0) -> None:
        """전부 취소하고 기다린다. 안 끝나는 건 버린다 — 종료가 막히면 안 된다."""
        for s in self.tasks.values():
            t = s["task"]
            if not t.done():
                t.cancel()
        pending = [s["task"] for s in self.tasks.values() if not s["task"].done()]
        if pending:
            _done, still = await asyncio.wait(pending, timeout=timeout)
            for t in still:
                log.warning("[%s] 태스크가 %.0fs 안에 안 끝났다 — 버린다: %s",
                            self.service, timeout, t.get_name())
        for s in self.tasks.values():
            with contextlib.suppress(BaseException):
                s["task"].result()
