"""버스 소스가 여러 개일 때 하나만 죽으면 보이는가.

실측(2026-09-02): ws-gateway 가 크립토 버스 구독을 잃고 KRX 하트비트만
받는 상태로 **30시간** 돌았다.

    upstream_connected   True     ← 살아 있는 다른 소스가 갱신
    last_frame_age_s     5.0초    ← 그래서 정상으로 보였다
    BINANCE 시세         24초 동안 한 글자도 안 변함
    대시보드             낡은 값을 정상처럼 표시

원인은 `asyncio.create_task(...)` 결과를 아무 데도 안 담은 것이다.
파이썬이 그 태스크를 GC 로 수거해 갔고, 로그에
"Task was destroyed but it is pending!" 이 남아 있었다.

다른 소비자 넷은 전부 두 버스에 재연결했는데 ws-gateway 만 하나만 붙었다.
"""
import asyncio
import os
import tempfile
import time

import pytest

from mdfeed.bus import (SourceTracker, UDSPublisher, consume_forever)
from mdfeed.models import MSG_TRADE
from mdfeed.protocol import encode


def _sock(name="b.sock"):
    return os.path.join(tempfile.mkdtemp(prefix="mdfs", dir="/tmp"), name)


# ── 소스별로 본다 ─────────────────────────────────────────────────────────

def test_한_소스만_죽어도_저하로_잡는다():
    t = SourceTracker(["/x/crypto.sock", "/x/krx.sock"], stale_after_s=0.05)
    t.mark("/x/crypto.sock")
    t.mark("/x/krx.sock")
    assert t.report()["degraded_sources"] == []
    time.sleep(0.08)
    t.mark("/x/krx.sock")                       # 하나만 계속 들어온다
    rep = t.report()
    assert rep["degraded_sources"] == ["crypto.sock"], rep
    live = {s["source"]: s for s in rep["sources"]}
    assert live["crypto.sock"]["stale"] and not live["krx.sock"]["stale"]


def test_합계로는_구분되지_않는다():
    """이게 30시간 동안 정상으로 보인 이유다."""
    t = SourceTracker(["/x/a.sock", "/x/b.sock"], stale_after_s=0.05)
    t.mark("/x/a.sock")
    time.sleep(0.08)
    t.mark("/x/b.sock")
    # 합쳐서 "마지막 프레임" 만 보면 방금 왔으므로 정상이다
    newest = max(s["last_frame_age_s"] for s in t.report()["sources"]
                 if s["last_frame_age_s"] is not None)
    assert newest > 0.05                          # 하나는 낡았는데
    assert t.report()["degraded_sources"], "소스별로 안 보면 이걸 놓친다"


def test_한_번도_안_온_소스는_저하다():
    t = SourceTracker(["/x/a.sock"])
    assert t.report()["degraded_sources"] == ["a.sock"]


# ── 죽어도 되살아난다 ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_소비자가_죽어도_되살아난다():
    path = _sock()
    pub = UDSPublisher(path)
    await pub.start()
    got, boom = [], {"n": 0}

    def on_frame(f):
        boom["n"] += 1
        if boom["n"] == 1:
            raise RuntimeError("처리 중 예외")     # 예전엔 여기서 소비자가 죽었다
        got.append(f)

    tracker = SourceTracker([path])
    task = asyncio.create_task(consume_forever(path, on_frame, asyncio.Event(),
                                               tracker, name="t"))
    for _ in range(200):
        await asyncio.sleep(0.02)
        if pub.subscriber_count:
            break
    # 되살아나는 데 백오프가 있으므로 계속 발행한다. 한 번만 쏘고 기다리면
    # 재접속 구간에 발행분이 다 지나가 버려 시험이 엉뚱하게 실패한다.
    for i in range(300):
        pub.publish(encode(MSG_TRADE, i, b"x" * 8))
        await asyncio.sleep(0.02)
        if got:
            break
    task.cancel()
    try:
        await task
    except BaseException:
        pass
    await pub.close()

    assert got, "예외 한 번에 소비자가 영영 죽었다"
    assert tracker.state[path]["restarts"] >= 1, "죽은 사실이 안 세어졌다"


@pytest.mark.asyncio
async def test_정지_요청이_오면_되살리지_않는다():
    path = _sock()
    tracker = SourceTracker([path])
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(
        consume_forever(path, lambda f: None, stop, tracker, name="t"), timeout=2)


def test_ws게이트웨이가_소비자_태스크를_붙잡는다():
    """create_task 결과를 안 담으면 GC 가 수거해 간다.

    'Task was destroyed but it is pending!' 이 실제 로그에 있었다.
    """
    import inspect

    from mdfeed.services import ws_gateway

    src = inspect.getsource(ws_gateway.WSGateway._consume)
    assert "self._src_tasks" in src, "태스크를 어디에도 붙잡아 두지 않는다"
    assert "consume_forever" in src, "감독 없는 소비자를 쓰고 있다"
