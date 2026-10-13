"""주문 데스크 로그인 — 비밀번호 해시 · 세션 · 실패 제한 (표준 라이브러리만).

사용자는 한 명이다. 그래서 사용자 테이블 대신 비밀번호 해시 하나를 환경변수로 받는다.

    python -m mdfeed.auth hash        # 비밀번호를 물어보고 해시 한 줄을 출력
    MDFEED_DESK_PASSWORD_HASH='pbkdf2_sha256$600000$<salt>$<hash>'

정한 것들
---------
* **비밀번호 원문은 어디에도 저장하지 않는다.** PBKDF2-SHA256 60만 회(OWASP 2023 권고).
* 비교는 hmac.compare_digest — 앞자리부터 비교하다 일찍 빠지면 시간으로 새어 나간다.
* 세션 토큰은 secrets.token_urlsafe(32). 서버 메모리에만 두므로 재기동하면 다시 로그인한다.
  주문 화면에서 이게 단점이 아니다 — 재기동 전 세션이 살아 있을 이유가 없다.
* 세션은 **유휴 30분 · 절대 12시간**에 만료. 유휴만 두면 탭을 켜 둔 채로 영원히 산다.
* 로그인 실패 5회(5분 창) → 60초 잠금. 사용자 한 명이라 IP 별이 아니라 전역이다.
  공격자가 일부러 틀려서 나를 잠글 수는 있다. 그 대가로 무차별 대입이 분당 5회로 묶인다.
"""

from __future__ import annotations

import getpass
import hashlib
import hmac
import secrets
import sys
import time

ITERATIONS = 600_000
IDLE_TTL_S = 30 * 60
ABSOLUTE_TTL_S = 12 * 3600
FAIL_WINDOW_S = 300
FAIL_LIMIT = 5
LOCK_S = 60


def hash_password(password: str, iterations: int = ITERATIONS) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iters, salt_hex, hash_hex = encoded.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), int(iters))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(dk.hex(), hash_hex)


class LoginThrottle:
    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._fails: list[float] = []
        self._locked_until = 0.0

    def locked_for(self) -> float:
        return max(0.0, self._locked_until - self._clock())

    def fail(self) -> None:
        now = self._clock()
        self._fails = [t for t in self._fails if now - t < FAIL_WINDOW_S] + [now]
        if len(self._fails) >= FAIL_LIMIT:
            self._locked_until = now + LOCK_S
            self._fails.clear()

    def success(self) -> None:
        self._fails.clear()


class SessionStore:
    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._sessions: dict[str, tuple[float, float]] = {}   # token → (생성, 마지막 사용)

    def create(self) -> str:
        tok = secrets.token_urlsafe(32)
        now = self._clock()
        self._sessions[tok] = (now, now)
        return tok

    def check(self, token: str | None) -> bool:
        """유효하면 마지막 사용 시각을 갱신한다. 만료된 것은 여기서 지운다."""
        if not token:
            return False
        got = self._sessions.get(token)
        if got is None:
            return False
        created, last = got
        now = self._clock()
        if now - last > IDLE_TTL_S or now - created > ABSOLUTE_TTL_S:
            del self._sessions[token]
            return False
        self._sessions[token] = (created, now)
        return True

    def revoke(self, token: str | None) -> None:
        self._sessions.pop(token or "", None)

    def __len__(self) -> int:
        return len(self._sessions)


def cookie_value(cookie_header: str, name: str) -> str | None:
    for part in (cookie_header or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == name:
            return v
    return None


def main(argv: list[str]) -> int:
    if argv[1:2] != ["hash"]:
        print("사용법: python -m mdfeed.auth hash", file=sys.stderr)
        return 2
    pw = getpass.getpass("주문 데스크 비밀번호: ")
    if len(pw) < 10:
        print("10자 이상으로 정하세요", file=sys.stderr)
        return 2
    if pw != getpass.getpass("한 번 더: "):
        print("두 입력이 다릅니다", file=sys.stderr)
        return 2
    print(hash_password(pw))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
