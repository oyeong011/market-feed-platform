"""어댑터 레지스트리. 새 거래소를 붙일 때 손대는 곳은 여기 한 줄뿐이다."""

from .base import Adapter
from .binance import BinanceAdapter
from .kis import KISAdapter
from .replay import ReplayAdapter
from .upbit import UpbitAdapter

REGISTRY = {
    "upbit": UpbitAdapter,
    "binance": BinanceAdapter,
    "kis": KISAdapter,
    "replay": ReplayAdapter,
}


def build(names, cfg, emit, registry=None):
    """설정된 이름 목록으로 어댑터 인스턴스를 만든다.

    비활성(자격증명 없음, 파일 없음) 어댑터는 만들되 기동하지 않고,
    그 사실을 /healthz 에 노출한다. 조용히 사라지는 업스트림이 가장 위험하다.
    """
    active, inactive = [], []
    for name in names:
        cls = REGISTRY.get(name.strip().lower())
        if cls is None:
            inactive.append({"venue": name, "reason": "알 수 없는 어댑터"})
            continue
        a = cls(cfg, emit, registry)
        (active if a.enabled() else inactive).append(
            a if a.enabled() else {"venue": name, "reason": a.disabled_reason()})
    return active, inactive


__all__ = ["Adapter", "REGISTRY", "build",
           "UpbitAdapter", "BinanceAdapter", "KISAdapter", "ReplayAdapter"]
