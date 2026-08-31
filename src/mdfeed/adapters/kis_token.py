"""KIS access_token 을 프로세스·장비 단위로 한 번만 발급한다.

왜 필요한가
-----------
KIS 는 **1일 1회 발급을 원칙**으로 하고, 유효기간 안에 잦은 발급이 생기면
이용을 제한한다. 계정이 막히면 그날 KRX 수집이 통째로 멎는다.

그런데 `kis_rest` 와 `kis_macro` 는 같은 샤드 프로세스에 함께 뜨면서
**각자** `/oauth2/tokenP` 를 부르고 있었다. 둘 다 파일 캐시를 보긴 하지만,
캐시가 비었거나 만료된 콜드 스타트에서는 **동시에 miss 가 나서 둘 다 발급**한다.
재기동 한 번에 두 번씩 받는 셈이다. (2026-08-31 확인)

여기서 세 가지를 보장한다.

1. **락 안에서 캐시를 다시 본다.** 기다리는 동안 다른 쪽이 받아 왔을 수 있다.
   이 재확인이 없으면 락은 발급을 직렬화할 뿐 횟수를 줄이지 못한다.
2. **파일 락**으로 프로세스 사이도 막는다. 샤드를 나눠 띄우거나
   재기동이 겹치면 asyncio 락만으로는 부족하다.
3. **원자적 쓰기**(임시파일 + rename). 반쯤 쓰인 JSON 을 다른 쪽이 읽으면
   캐시 miss 로 판정해 또 발급한다.

approval_key(`/oauth2/Approval`, 실시간 WS 접속키)는 다른 자격이라
여기서 다루지 않는다.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import threading
import time
import urllib.request

log = logging.getLogger("mdfeed.kis_token")

# 이 시간 안에 만료될 토큰은 안 쓴다. 쓰다가 중간에 만료되면 재시도가 곧 재발급이다.
MIN_REMAIN_S = 600.0


class TokenStore:
    """appkey + 캐시경로 조합마다 하나. 프로세스 안에서 공유한다."""

    _instances: dict[tuple, "TokenStore"] = {}
    _registry_lock = threading.Lock()

    @classmethod
    def get(cls, app_key: str, app_secret: str, rest_base: str,
            cache_path: str) -> "TokenStore":
        path = os.path.expanduser(cache_path)
        key = (app_key, rest_base, path)
        with cls._registry_lock:
            inst = cls._instances.get(key)
            if inst is None:
                inst = cls(app_key, app_secret, rest_base, path)
                cls._instances[key] = inst
            return inst

    def __init__(self, app_key: str, app_secret: str, rest_base: str, path: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.rest_base = rest_base
        self.path = path
        self._lock = asyncio.Lock()
        self._token: str | None = None
        self._exp = 0.0
        self.issues = 0                 # 이 프로세스가 실제로 발급한 횟수

    # ── 파일 캐시 ─────────────────────────────────────────────────────────
    def _read_cache(self) -> tuple[str, float] | None:
        try:
            with open(self.path, encoding="utf-8") as fh:
                d = json.load(fh)
        except Exception:                            # noqa: BLE001
            return None
        exp = float(d.get("expires_at", 0))
        if exp > time.time() + MIN_REMAIN_S and d.get("access_token"):
            return d["access_token"], exp
        return None

    def _write_cache(self, tok: str, exp: float) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"access_token": tok, "expires_at": exp}, fh)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)                   # 원자적으로 갈아끼운다

    # ── 발급 ──────────────────────────────────────────────────────────────
    def _issue_locked(self) -> tuple[str, float]:
        """파일 락을 잡고, 다시 캐시를 본 뒤, 없으면 발급한다.

        다른 프로세스가 발급 중이면 여기서 기다렸다가 그 결과를 읽는다.
        """
        lock_path = f"{self.path}.lock"
        os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            got = self._read_cache()                 # 기다리는 동안 남이 받아 왔을 수 있다
            if got:
                log.info("[kis] 다른 쪽이 받아 둔 토큰을 쓴다 — 발급하지 않는다")
                return got
            body = json.dumps({"grant_type": "client_credentials",
                               "appkey": self.app_key,
                               "appsecret": self.app_secret}).encode()
            req = urllib.request.Request(
                f"{self.rest_base}/oauth2/tokenP", data=body,
                headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                d = json.loads(r.read())
            exp = time.time() + int(d.get("expires_in", 86400)) - 120
            self._write_cache(d["access_token"], exp)
            self.issues += 1
            log.warning("[kis] access_token 신규 발급 (이 프로세스 %d번째). "
                        "KIS 는 1일 1회 발급이 원칙이므로 잦으면 이용이 제한된다",
                        self.issues)
            return d["access_token"], exp
        finally:
            with __import__("contextlib").suppress(Exception):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    async def token(self) -> str:
        if self._token and self._exp > time.time() + 60:
            return self._token
        async with self._lock:
            # 락 안에서 다시 본다. 이게 없으면 직렬화만 하고 횟수는 그대로다.
            if self._token and self._exp > time.time() + 60:
                return self._token
            got = self._read_cache()
            if got:
                self._token, self._exp = got
                return self._token
            self._token, self._exp = await asyncio.to_thread(self._issue_locked)
            return self._token
