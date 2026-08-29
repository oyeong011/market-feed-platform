"""정지 요청 뒤의 정리에도 기한이 있는가.

SIGTERM 을 잡는 이유는 버퍼에 든 틱을 flush 하기 위해서다. 그런데 잡기만 하고
정리가 안 끝나면 결과는 안 잡은 것과 같다 — systemd 가 TimeoutStopSec 뒤에
SIGKILL 하고 버퍼는 똑같이 날아간다. 다른 점은 **왜 안 끝났는지 아무 기록도
안 남는다**는 것뿐이다.

2026-08-29 에 어댑터 복구 경로에서 기한 없는 await 하나가 11.2시간 무음을
만들었다. 종료 경로도 같은 모양이라 같이 막는다.
"""
import asyncio

import pytest

from mdfeed import runtime
from mdfeed.config import Config


@pytest.fixture
def cfg(tmp_path):
    c = Config()
    c.run_dir = str(tmp_path)
    c.shutdown_grace_s = 0.3
    c.log_json = False
    return c


def test_정리가_끝나면_그대로_끝낸다(cfg, monkeypatch):
    forced = []
    monkeypatch.setattr(runtime, "_force_exit", lambda code: forced.append(code))
    flushed = []

    async def main(stop):
        stop.set()                       # 즉시 정지 요청
        await asyncio.sleep(0.01)
        flushed.append("flush")

    assert runtime.run("t", main, cfg) == 0
    assert flushed == ["flush"], "정상 정리를 중간에 자르면 안 된다"
    assert forced == [], "기한 안에 끝났는데 강제 종료했다"


def test_정리가_기한을_넘기면_강제로_내려간다(cfg, monkeypatch):
    forced = []
    monkeypatch.setattr(runtime, "_force_exit", lambda code: forced.append(code))

    async def main(stop):
        stop.set()
        await asyncio.sleep(3600)        # 정리가 안 끝나는 서비스

    runtime.run("t", main, cfg)
    assert forced == [runtime.EXIT_SHUTDOWN_TIMEOUT], "기한을 넘겼는데 계속 기다린다"


def test_안_끝난_태스크_이름을_남긴다(cfg, monkeypatch, capsys):
    """어느 코루틴이 안 끝났는지가 진단의 전부다."""
    monkeypatch.setattr(runtime, "_force_exit", lambda code: None)

    async def 절대_안_끝나는_정리():
        await asyncio.sleep(3600)

    async def main(stop):
        asyncio.ensure_future(절대_안_끝나는_정리())
        stop.set()
        await asyncio.sleep(3600)

    # setup_logging 이 루트 핸들러를 갈아끼우므로 caplog 가 아니라 실제 출력을 본다
    runtime.run("t", main, cfg)
    body = capsys.readouterr().out + capsys.readouterr().err
    assert "남은 태스크" in body
    assert "절대_안_끝나는_정리" in body, f"태스크 이름이 안 남았다: {body}"


def test_정지_요청이_없으면_기한을_안_건다(cfg, monkeypatch):
    """기한은 정지 이후에만 잰다. 평시 운전에 시한을 걸면 안 된다."""
    forced = []
    monkeypatch.setattr(runtime, "_force_exit", lambda code: forced.append(code))

    async def main(stop):
        await asyncio.sleep(cfg.shutdown_grace_s * 3)   # 기한보다 오래 돈다
        return None

    assert runtime.run("t", main, cfg) == 0
    assert forced == [], "정지 요청도 없는데 기한을 걸었다"
