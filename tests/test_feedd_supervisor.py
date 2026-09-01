"""어댑터 루프가 죽었을 때 서비스가 그걸 알아채는지.

실측(2026-08-28): upbit 이 2.97시간 멎었는데 재접속은 8회에서 멈춰 있었다.
create_task 로 띄운 어댑터 태스크를 아무도 안 보고 있어서, run() 이 예외로
끝나면 태스크만 조용히 사라지고 서비스는 계속 healthy 를 보고했다.
"""
import asyncio
import contextlib

import pytest

from mdfeed.metrics import Registry


class _Svc:
    """_keep_running 만 떼어내 검증한다(전체 서비스 기동은 무겁다)."""

    def __init__(self):
        self.registry = Registry("feedd")

    # 실제 구현을 그대로 빌려온다 — 복제하면 원본이 바뀌어도 통과한다
    from mdfeed.services.feedd import FeedDaemon as _F
    _keep_running = _F._keep_running


class DyingAdapter:
    name = "dying"

    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.starts = 0

    async def run(self):
        self.starts += 1
        if self.starts <= self.fail_times:
            raise RuntimeError("루프가 터졌다")
        await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_어댑터_루프가_죽으면_되살린다():
    svc, a, stop = _Svc(), DyingAdapter(fail_times=2), asyncio.Event()
    task = asyncio.ensure_future(svc._keep_running(a, stop))
    await asyncio.sleep(0.8)          # 0.5s 백오프 뒤 재기동을 볼 수 있게
    stop.set()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    assert a.starts >= 2, "죽은 뒤 다시 띄우지 않았다"
    assert svc.registry.snapshot()["counters"].get(
        'adapter_task_deaths_total{venue="DYING"}', 0) >= 1


@pytest.mark.asyncio
async def test_죽음이_지표에_남는다():
    """조용히 죽으면 안 된다. 세어야 알람을 걸 수 있다."""
    svc, a, stop = _Svc(), DyingAdapter(fail_times=99), asyncio.Event()
    task = asyncio.ensure_future(svc._keep_running(a, stop))
    await asyncio.sleep(0.3)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    n = svc.registry.snapshot()["counters"].get(
        'adapter_task_deaths_total{venue="DYING"}', 0)
    assert n >= 1


@pytest.mark.asyncio
async def test_정지_요청에_의한_종료는_되살리지_않는다():
    """되살리기가 종료를 막으면 안 된다.

    **이 시험은 원래 반대를 못 박고 있었다.** 정지 요청이 없는데 run() 이
    돌아온 것을 "정상 종료"라 부르고 되살리지 않는다고 단언했다.
    그게 2026-09-01 에 upbit 을 18분간 멎게 한 계약이다 —
    정지 요청 없이 끝났다면 그건 정상이 아니라 사건이다.
    """
    class Clean:
        name = "clean"
        starts = 0

        async def run(self):
            Clean.starts += 1
            self_stop.set()          # 어댑터가 정지 요청을 받고 내려간 상황
            return

    svc, self_stop = _Svc(), asyncio.Event()
    await asyncio.wait_for(svc._keep_running(Clean(), self_stop), timeout=1.0)
    assert Clean.starts == 1


# ── 조용히 감독이 사라지는 경로 ────────────────────────────────────────────
# 실측(2026-09-01): 전 종목으로 스케일업한 뒤 upbit 이 18분째 stale 인데
# 재접속은 5에서 멈춰 있었다.
#     stale_restarts_total        1   ← 감시자가 15초마다 도는데
#     session_cancel_timeouts     0
#     adapter_task_deaths_total   0   ← 72번 동안 한 번도 안 울렸다
# 즉 run() 자체가 실행 중이 아니었는데 **어느 지표에도 남지 않았다.**
#
# _keep_running 이 run() 의 정상 반환과 취소를 "정지 요청"으로만 해석한 탓이다.
# 정지 요청이 없는데 끝났다면 그건 사건이다.

@pytest.mark.asyncio
async def test_정지_요청_없이_끝나면_되살린다():
    from mdfeed.config import Config
    from mdfeed.services.feedd import FeedDaemon

    calls = []

    class QuitsEarly:
        name = "quits"

        async def run(self):
            calls.append(1)
            if len(calls) < 3:
                return                      # 정지 요청 없이 그냥 돌아온다
            await asyncio.sleep(3600)

        def stop(self):
            pass

    d = FeedDaemon(Config())
    stop = asyncio.Event()
    task = asyncio.create_task(d._keep_running(QuitsEarly(), stop))
    for _ in range(200):
        await asyncio.sleep(0.02)
        if len(calls) >= 3:
            break
    stop.set()
    task.cancel()
    with contextlib.suppress(BaseException):
        await task

    assert len(calls) >= 3, f"되살리지 않았다 — {len(calls)}회만 돌았다"
    body = d.registry.prometheus()
    assert "adapter_silent_exits_total" in body


@pytest.mark.asyncio
async def test_정지_요청_없는_취소도_되살린다():
    """정지 요청 없이 취소되면 그대로 다시 던져 감독이 사라졌다."""
    from mdfeed.config import Config
    from mdfeed.services.feedd import FeedDaemon

    calls = []

    class CancelledOnce:
        name = "cancelled"

        async def run(self):
            calls.append(1)
            if len(calls) == 1:
                raise asyncio.CancelledError
            await asyncio.sleep(3600)

        def stop(self):
            pass

    d = FeedDaemon(Config())
    stop = asyncio.Event()
    task = asyncio.create_task(d._keep_running(CancelledOnce(), stop))
    for _ in range(200):
        await asyncio.sleep(0.02)
        if len(calls) >= 2:
            break
    stop.set()
    task.cancel()
    with contextlib.suppress(BaseException):
        await task

    assert len(calls) >= 2, "취소 뒤 되살리지 않았다"


@pytest.mark.asyncio
async def test_정지_요청이_있으면_그냥_끝낸다():
    """되살리기가 종료를 막으면 안 된다."""
    from mdfeed.config import Config
    from mdfeed.services.feedd import FeedDaemon

    calls = []

    class Normal:
        name = "normal"

        async def run(self):
            calls.append(1)
            await asyncio.sleep(0.05)

        def stop(self):
            pass

    d = FeedDaemon(Config())
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(d._keep_running(Normal(), stop), timeout=2)
    assert calls == [], "정지 상태인데 어댑터를 띄웠다"
