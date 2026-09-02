"""감독 계약 — 네 번 난 사고를 하나씩 재현해 잠근다.

2026-08-28 ~ 09-02 에 같은 가족의 사고가 네 번 났다. 증상은 매번 같았고
(업스트림이 조용히 멎고 헬스는 정상) 원인은 매번 달랐다.

    08-28   create_task 결과를 아무도 안 봐 예외가 태스크 안에 갇혔다
    08-31   취소한 세션의 정리가 안 끝나 복구가 그 뒤에 줄 섰다
    09-01   "정지 요청 없는 반환/취소"를 정상 종료로 보고 이탈했다
    09-02   태스크를 변수에 안 담아 GC 가 수거해 갔다

아래 시험이 넷을 각각 재현한다. 하나라도 깨지면 그 사고가 돌아온 것이다.
"""
import asyncio
import gc

import pytest

from mdfeed.metrics import Registry
from mdfeed.supervisor import Supervisor


def _sup(**kw):
    return Supervisor("test", Registry("test"), backoff_max_s=0.05, **kw)


# ── 08-28: 예외가 태스크 안에 갇힌다 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_예외로_죽어도_되살린다():
    stop, runs = asyncio.Event(), []

    async def boom():
        runs.append(1)
        if len(runs) < 3:
            raise RuntimeError("터짐")
        await asyncio.sleep(3600)

    s = _sup()
    s.spawn("boom", boom, stop)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(runs) >= 3:
            break
    assert len(runs) >= 3, f"{len(runs)}회에서 멈췄다 — 예외가 갇혔다"
    assert s.tasks["boom"]["restarts"] >= 2
    stop.set(); await s.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_되살린_사실이_지표에_남는다():
    """되살리는 것보다 중요한 건 되살렸다는 사실이 남는 것이다."""
    stop, runs = asyncio.Event(), []

    async def boom():
        runs.append(1)
        raise RuntimeError("터짐")

    s = _sup()
    s.spawn("boom", boom, stop)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(runs) >= 3:
            break
    assert "task_restarts_total" in s.registry.prometheus()
    stop.set(); await s.shutdown(timeout=1)


# ── 09-01: 정지 요청 없는 반환·취소를 정상으로 봤다 ────────────────────────

@pytest.mark.asyncio
async def test_정지_요청_없이_반환하면_되살린다():
    stop, runs = asyncio.Event(), []

    async def quits():
        runs.append(1)
        if len(runs) < 3:
            return                      # 정지 요청 없이 그냥 돌아온다
        await asyncio.sleep(3600)

    s = _sup()
    s.spawn("quits", quits, stop)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(runs) >= 3:
            break
    assert len(runs) >= 3, "정상 반환을 종료로 보고 이탈했다"
    stop.set(); await s.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_정지_요청_없이_취소되면_되살린다():
    stop, runs = asyncio.Event(), []

    async def cancelled():
        runs.append(1)
        if len(runs) < 3:
            raise asyncio.CancelledError
        await asyncio.sleep(3600)

    s = _sup()
    s.spawn("c", cancelled, stop)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(runs) >= 3:
            break
    assert len(runs) >= 3, "취소를 종료로 보고 이탈했다"
    stop.set(); await s.shutdown(timeout=1)


# ── 09-02: GC 가 태스크를 수거해 갔다 ─────────────────────────────────────

@pytest.mark.asyncio
async def test_강한_참조를_쥐어_GC_가_못_가져간다():
    """'Task was destroyed but it is pending!' 이 09-02 사고의 로그였다."""
    stop, ticks = asyncio.Event(), []

    async def forever():
        while True:
            ticks.append(1)
            await asyncio.sleep(0.01)

    s = _sup()
    s.spawn("forever", forever, stop)
    await asyncio.sleep(0.05)
    gc.collect(); gc.collect()          # 참조가 없으면 여기서 사라진다
    n = len(ticks)
    await asyncio.sleep(0.1)
    assert len(ticks) > n, "GC 뒤에 태스크가 멈췄다 — 참조를 안 쥐고 있다"
    stop.set(); await s.shutdown(timeout=1)


# ── 08-31: 정리가 안 끝나 종료가 막힌다 ───────────────────────────────────

@pytest.mark.asyncio
async def test_취소를_삼키는_태스크가_종료를_막지_않는다():
    stop = asyncio.Event()

    swallowed = {"n": 0}

    async def swallows():
        # 넓게 잡는 정리 코드가 실제로 이렇게 생겼다.
        # 시험이 불멸의 태스크를 남기지 않도록 몇 번만 삼키고 놓아 준다.
        while swallowed["n"] < 3:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                swallowed["n"] += 1

    s = _sup()
    s.spawn("swallow", swallows, stop)
    await asyncio.sleep(0.05)
    stop.set()
    # 삼키는 태스크가 있어도 shutdown 이 기한 안에 돌아와야 한다
    await asyncio.wait_for(s.shutdown(timeout=0.2), timeout=3.0)
    assert swallowed["n"] >= 1, "취소가 배달되지 않았다"
    for _ in range(50):                 # 남은 태스크를 정리한다
        await asyncio.sleep(0.02)
        if s.tasks["swallow"]["task"].done():
            break
        s.tasks["swallow"]["task"].cancel()


# ── 판정 장치로서의 계약 ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_정지_요청이_오면_되살리지_않는다():
    stop, runs = asyncio.Event(), []

    async def once():
        runs.append(1)
        await asyncio.sleep(0.02)

    s = _sup()
    s.spawn("once", once, stop)
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.sleep(0.1)
    n = len(runs)
    await asyncio.sleep(0.1)
    assert len(runs) == n, "정지 요청 뒤에도 되살리고 있다"
    await s.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_불안정한_태스크를_지목한다():
    """되살리기가 잦다는 건 고쳐야 할 원인이 있다는 뜻이다."""
    stop, runs = asyncio.Event(), []

    async def flaky():
        runs.append(1)
        raise RuntimeError("계속 터짐")

    s = _sup()
    s.spawn("flaky", flaky, stop)
    for _ in range(300):
        await asyncio.sleep(0.01)
        if s.tasks["flaky"]["restarts"] > 3:
            break
    rep = s.report()
    assert "flaky" in rep["unstable_tasks"], rep
    assert rep["tasks"][0]["name"] == "flaky"      # 많이 죽은 순
    stop.set(); await s.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_왜_끝났는지_남는다():
    """되살렸다는 사실만으로는 원인을 못 찾는다."""
    stop, runs = asyncio.Event(), []

    async def boom():
        runs.append(1)
        raise ValueError("구체적인 이유")

    s = _sup()
    s.spawn("boom", boom, stop)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(runs) >= 2:
            break
    assert "ValueError" in (s.tasks["boom"]["last_exit"] or "")
    stop.set(); await s.shutdown(timeout=1)
