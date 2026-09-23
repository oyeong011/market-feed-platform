"""구독 권한(entitlement) — 누가 어떤 종목을 볼 수 있는가.

마켓데이터는 "받을 수 있는 사람"과 "받을 수 있는 종목"이 계약으로 정해진다. 거래소·벤더가
entitlement 라고 부르는 것이고, 이 플랫폼에 없던 개념이다. 지금까지는 배포 포트에 닿는 누구나
전 종목을 받았다.

파일 형식 (줄 단위, `#` 주석):

    # 토큰            허용 종목 (쉼표, * 는 전체)
    demo-readonly     UPBIT:KRW-BTC,BINANCE:BTCUSDT
    full-desk         *

**이것이 막는 것과 막지 못하는 것**을 분명히 적는다.
  * 막는 것: 권한 없는 구독자가 종목을 받아 가는 것. 요청하지 않아도, 필터를 비워도 못 받는다.
  * 막지 못하는 것: 도청. 토큰과 시세가 평문으로 흐른다. 전송 구간 보호는 사설망이나
    TLS 종단(스니펫은 RUNBOOK)이 맡는다. 토큰을 비밀번호처럼 쓰되 암호로 착각하지 않는다.

파일을 주지 않으면 권한 검사가 **꺼진다.** 그때는 기동 로그에 그렇게 적는다 —
"켜 둔 줄 알았는데 안 켜져 있었다"가 이 저장소에서 반복된 사고 유형이다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

ALL = "*"


@dataclass(frozen=True)
class Entitlements:
    """토큰 → 허용 종목. 비어 있으면 검사를 하지 않는다(개방)."""

    by_token: dict[str, frozenset[str]] = field(default_factory=dict)
    enabled: bool = False
    source: str = ""

    def allows_all(self, token: str) -> bool:
        return ALL in self.by_token.get(token, frozenset())

    def allowed(self, token: str) -> frozenset[str]:
        return self.by_token.get(token, frozenset())

    def known(self, token: str) -> bool:
        return token in self.by_token

    def resolve(self, token: str, requested: list[str] | None) -> tuple[set[str] | None, list[str], str]:
        """(적용할 필터, 거절된 종목, 사유) 를 돌려준다.

        * 검사가 꺼져 있으면 요청을 그대로 쓴다(None = 전체 구독).
        * 토큰이 없거나 모르는 토큰이면 **아무것도 주지 않는다**(빈 집합). 조용히 전체를 주지 않는다.
        * 전체 권한이면 요청대로.
        * 아니면 요청 ∩ 허용. 요청이 비어 있으면(전체 구독) 허용 집합으로 좁힌다.
        """
        if not self.enabled:
            return (set(requested) if requested else None), [], ""
        if not token:
            return set(), list(requested or []), "TOKEN_REQUIRED"
        if not self.known(token):
            return set(), list(requested or []), "UNKNOWN_TOKEN"
        if self.allows_all(token):
            return (set(requested) if requested else None), [], ""
        allow = self.allowed(token)
        if not requested:
            return set(allow), [], ""
        granted = {s for s in requested if s in allow}
        denied = [s for s in requested if s not in allow]
        return granted, denied, "NOT_ENTITLED" if denied else ""


def parse(text: str, source: str = "") -> Entitlements:
    by: dict[str, frozenset[str]] = {}
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split(None, 1)
        token = parts[0]
        syms = parts[1] if len(parts) > 1 else ""
        by[token] = frozenset(s.strip() for s in syms.split(",") if s.strip())
    return Entitlements(by_token=by, enabled=bool(by), source=source)


def load(path: str | None = None) -> Entitlements:
    path = path if path is not None else os.getenv("MDFEED_ENTITLEMENTS_FILE", "")
    if not path:
        return Entitlements()
    with open(path, encoding="utf-8") as fh:
        return parse(fh.read(), source=path)
