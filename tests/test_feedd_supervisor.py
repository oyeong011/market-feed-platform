"""어댑터 루프가 죽었을 때 서비스가 그걸 알아채는지.

실측(2026-08-28): upbit 이 2.97시간 멎었는데 재접속은 8회에서 멈춰 있었다.
create_task 로 띄운 어댑터 태스크를 아무도 안 보고 있어서, run() 이 예외로
끝나면 태스크만 조용히 사라지고 서비스는 계속 healthy 를 보고했다.
"""
import asyncio

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
async def test_정상_종료는_되살리지_않는다():
    class Clean:
        name = "clean"
        starts = 0

        async def run(self):
            Clean.starts += 1
            return

    svc, stop = _Svc(), asyncio.Event()
    await asyncio.wait_for(svc._keep_running(Clean(), stop), timeout=1.0)
    assert Clean.starts == 1
