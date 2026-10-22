"""한국투자증권 실시간 체결통보 — 내 주문이 접수·체결되는 순간을 웹소켓으로 받는다.

합계 대사(ledger.apply_totals)는 일별 체결 합계를 주기적으로 물어 체결을 배분한다. 정확하지만 느리다
(대사 한 바퀴만큼 늦게 보인다). 체결통보는 증권사가 체결 즉시 밀어 준다. 원장은 둘을 이렇게 쓴다:

* **통보는 빠른 반영용**이다. 통보로 들어온 체결은 그 주문에 바로 붙는다(Ledger.apply_notice).
* **합계 대사가 최종 판정**이다. 통보가 실제보다 많으면 합계가 원장보다 작게 나와 불일치로 올라간다.
  통보를 놓치면 합계가 나머지를 채운다. 그래서 통보 중복 제거는 "덜 세는 쪽"으로 틀리게 짰다.

프로토콜 메모 (KIS 실시간 명세 기준 — **필드 순서는 실측 전**이다)
----------------------------------------------------------------------
* 접속키(approval_key) → ws://ops.koreainvestment.com (실전 21000, 모의 31000) — 시세(adapters/kis.py)와 같다.
* 구독 TR: 실전 H0STCNI0, 모의 H0STCNI9. tr_key 는 **HTS ID**(계좌번호가 아니다).
* 구독 응답 JSON 의 body.output 에 AES-256-CBC 키(key)와 IV(iv)가 온다.
* 통보 본문은 `1|H0STCNI0|001|<base64 암호문>` — 첫 필드 '1' 이 암호화 표시다.
  복호화하면 '^' 로 나뉜 필드다. 체결여부(CNTG_YN) '2' 가 체결, '1' 이 접수·정정·취소·거부 통보다.

이 저장소는 KIS 문서가 틀린 것을 이미 한 번 겪었다(체결구분 매도가 문서상 3, 실제 5 — README 결함 9).
그래서 필드 순서를 상수 한 곳에 두고, 실제 통보를 받으면 원문 필드 수와 값을 로그로 남겨 대조한다.

복호화에 `cryptography` 가 필요하다(선택 설치: pip install '.[notify]'). 없으면 통보를 끄고
합계 대사만으로 돈다 — 기동 로그와 /healthz 에 그 사실이 나온다.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import urllib.request
from dataclasses import dataclass

from .wsproto import WSClient

log = logging.getLogger("mdfeed.kis_notice")

TR_NOTICE = {"mock": "H0STCNI9", "real": "H0STCNI0"}
ENDPOINTS = {
    "mock": ("https://openapivts.koreainvestment.com:29443", "ws://ops.koreainvestment.com:31000"),
    "real": ("https://openapi.koreainvestment.com:9443", "ws://ops.koreainvestment.com:21000"),
}

# 복호화한 본문의 필드 위치 (KIS 명세 기준, 실측 전). 바뀌면 여기 한 곳만 고친다.
F_ODNO, F_SIDE, F_SYMBOL, F_QTY, F_PRICE, F_TIME = 2, 4, 8, 9, 10, 11
F_REFUSED, F_FILLED, F_ORDER_QTY = 12, 13, 16
MIN_FIELDS = F_ORDER_QTY + 1
SIDE_CODE = {"01": "sell", "02": "buy"}


def crypto_available() -> tuple[bool, str]:
    try:
        import cryptography.hazmat.primitives.ciphers  # noqa: F401
    except ImportError:
        return False, "cryptography 미설치 (pip install '.[notify]')"
    return True, ""


def decrypt(key: str, iv: str, b64: str) -> str:
    """AES-256-CBC + PKCS7. 키·IV 는 구독 응답에 문자열로 온다."""
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    dec = Cipher(algorithms.AES(key.encode()), modes.CBC(iv.encode())).decryptor()
    raw = dec.update(base64.b64decode(b64)) + dec.finalize()
    unpad = padding.PKCS7(128).unpadder()
    return (unpad.update(raw) + unpad.finalize()).decode("utf-8")


@dataclass(frozen=True)
class Notice:
    kind: str            # fill(체결) · ack(접수·정정·취소) · refused(거부)
    odno: str
    side: str
    symbol: str
    qty: int             # fill 이면 이번 체결 수량, ack 면 0
    price: int
    time: str            # HHMMSS
    order_qty: int

    @property
    def key(self) -> str:
        """중복 제거 키. 재접속 뒤 같은 통보가 다시 와도 한 번만 붙인다.

        같은 초에 같은 수량·가격으로 두 번 체결되면 하나로 합쳐져 **덜 센다** — 그 차이는 합계 대사가
        채운다. 더 세는 쪽(같은 통보를 두 번 붙임)은 원장을 틀리게 하므로 그쪽으로는 틀리지 않게 했다.
        """
        return f"{self.odno.lstrip('0')}|{self.time}|{self.qty}|{self.price}"


def parse(fields: list[str]) -> Notice | None:
    if len(fields) < MIN_FIELDS:
        return None
    try:
        filled = fields[F_FILLED] == "2"
        refused = fields[F_REFUSED].upper() == "Y"
        return Notice(kind="refused" if refused else "fill" if filled else "ack",
                      odno=fields[F_ODNO], side=SIDE_CODE.get(fields[F_SIDE], "?"),
                      symbol=fields[F_SYMBOL], qty=int(fields[F_QTY] or 0) if filled else 0,
                      price=int(float(fields[F_PRICE] or 0)), time=fields[F_TIME],
                      order_qty=int(fields[F_ORDER_QTY] or 0))
    except ValueError:
        return None


def get_approval_key(rest_base: str, app_key: str, app_secret: str, timeout: float = 10.0) -> str:
    body = json.dumps({"grant_type": "client_credentials", "appkey": app_key,
                       "secretkey": app_secret}).encode()
    req = urllib.request.Request(f"{rest_base}/oauth2/Approval", data=body,
                                 headers={"content-type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read())
    if not data.get("approval_key"):
        raise RuntimeError(f"approval_key 발급 실패 (응답 필드: {sorted(data)})")
    return data["approval_key"]


class NoticeClient:
    """체결통보 구독. 끊기면 다시 붙는다. 통보마다 on_notice(Notice) 를 부른다."""

    def __init__(self, env: str, app_key: str, app_secret: str, hts_id: str,
                 rest_base: str | None = None, ws_url: str | None = None):
        self.env, self.app_key, self.app_secret, self.hts_id = env, app_key, app_secret, hts_id
        self.rest_base = rest_base or ENDPOINTS[env][0]
        self.ws_url = ws_url or ENDPOINTS[env][1]
        self.tr_id = TR_NOTICE[env]
        self._key = self._iv = None
        self.connected = False
        self.subscribed = False
        self.counts: dict[str, int] = {}
        self.last_error = ""
        self.logged_raw = 0

    def _count(self, k: str) -> None:
        self.counts[k] = self.counts.get(k, 0) + 1

    async def run(self, on_notice, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                await self._session(on_notice, stop)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:                       # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"[:200]
                log.warning("[notice] 끊김 — %.0f초 뒤 다시 붙는다: %s", backoff, self.last_error)
            self.connected = self.subscribed = False
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, 30.0)

    async def _session(self, on_notice, stop: asyncio.Event) -> None:
        approval = await asyncio.to_thread(get_approval_key, self.rest_base, self.app_key, self.app_secret)
        ws = await WSClient.connect(self.ws_url)
        self.connected = True
        try:
            await ws.send_text(json.dumps({
                "header": {"approval_key": approval, "custtype": "P", "tr_type": "1",
                           "content-type": "utf-8"},
                "body": {"input": {"tr_id": self.tr_id, "tr_key": self.hts_id}}}))
            while not stop.is_set():
                try:
                    _op, raw = await ws.recv(timeout=60)
                except asyncio.TimeoutError:
                    continue                         # 장 밖에는 아무것도 안 온다. 끊긴 게 아니다
                await self._on_message(raw.decode("utf-8", "ignore"), ws, on_notice)
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    async def _on_message(self, text: str, ws, on_notice) -> None:
        if text.startswith("{"):
            msg = json.loads(text)
            header, body = msg.get("header") or {}, msg.get("body") or {}
            if header.get("tr_id") == "PINGPONG":
                await ws.pong(text.encode())         # 텍스트가 아니라 PONG 프레임(adapters/kis.py 와 같은 이유)
                return
            out = body.get("output") or {}
            if body.get("rt_cd") == "0" and out.get("key"):
                self._key, self._iv = out["key"], out["iv"]
                self.subscribed = True
                log.info("[notice] 체결통보 구독 %s (%s)", self.tr_id, (body.get("msg1") or "").strip())
            elif body.get("rt_cd") not in (None, "0"):
                # 조용히 넘기면 "붙어 있는데 통보가 안 오는" 상태가 된다
                self.last_error = f"구독 거절 {body.get('msg_cd')} {(body.get('msg1') or '').strip()}"
                log.error("[notice] %s", self.last_error)
            return
        parts = text.split("|", 3)
        if len(parts) < 4 or parts[1] != self.tr_id:
            return
        if parts[0] == "1":
            if not self._key:
                self._count("no_key")
                return
            payload = decrypt(self._key, self._iv, parts[3])
        else:
            payload = parts[3]
        fields = payload.split("^")
        if self.logged_raw < 3:
            # 필드 순서가 실측 전이다. 처음 몇 건은 위치·값을 남겨 명세와 대조한다(계좌번호·이름 칸은 가린다)
            shown = [f if i not in (0, 1, 17) else "***" for i, f in enumerate(fields)]
            log.info("[notice] 원문 필드 %d개: %s", len(fields), shown)
            self.logged_raw += 1
        n = parse(fields)
        if n is None:
            self._count("unparsed")
            return
        self._count(n.kind)
        on_notice(n)

    def status(self) -> dict:
        return {"connected": self.connected, "subscribed": self.subscribed, "counts": self.counts,
                "last_error": self.last_error, "tr_id": self.tr_id}
