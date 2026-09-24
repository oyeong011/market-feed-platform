"""버스 백엔드 선택이 **조용히 다른 걸 쓰지 않는가**.

예전엔 MDFEED_BUS_BACKEND=zmq 를 주면 UDS 로 폴백하면서 "pyzmq 없음" 이라고 경고했다.
두 가지가 틀렸다. bus_zmq 모듈이 저장소에 아예 없어서 pyzmq 를 설치해도 폴백했고(이유가 거짓),
여러 호스트로 흩어지려고 켠 사람이 실제로는 한 호스트 UDS 로 도는 걸 경고 한 줄로만 알 수 있었다.
"""
from __future__ import annotations

import pytest

from mdfeed import bus
from mdfeed.config import Config


def _cfg(backend: str) -> Config:
    c = Config()
    c.bus_backend = backend
    c.bus_path = "/tmp/mdfeed-test-bus.sock"
    return c


def test_uds_is_the_supported_backend():
    pub = bus.make_publisher(_cfg("uds"))
    sub = bus.make_subscriber(_cfg("uds"))
    assert isinstance(pub, bus.UDSPublisher) and isinstance(sub, bus.UDSSubscriber)


@pytest.mark.parametrize("backend", ["zmq", "kafka", ""])
def test_unsupported_backend_fails_loudly(backend):
    """폴백 금지. 설정과 실제가 갈리면 사람이 알 방법이 없다."""
    for make in (bus.make_publisher, bus.make_subscriber):
        with pytest.raises(bus.BusBackendError) as e:
            make(_cfg(backend))
        assert "UDS" in str(e.value)
        assert backend in str(e.value) or repr(backend) in str(e.value)


def test_repository_does_not_advertise_a_backend_it_lacks():
    """없는 기능을 설정·패키지에 광고해 두면 다음 사람이 그걸 믿는다."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assert not (root / "src/mdfeed/bus_zmq.py").exists()
    assert "pyzmq" not in (root / "pyproject.toml").read_text()
    # config 에도 zmq 엔드포인트 설정이 남아 있으면 안 된다
    assert "bus_zmq_endpoint" not in (root / "src/mdfeed/config.py").read_text()
