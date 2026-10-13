"""주문 데스크 로그인 부품 — 해시·세션 만료·잠금."""
from mdfeed import auth


class Clock:
    t = 0.0

    def __call__(self):
        return self.t


def test_hash_roundtrip_and_no_plaintext():
    h = auth.hash_password("pw-1234567890", iterations=1000)
    assert "pw-1234567890" not in h
    assert auth.verify_password("pw-1234567890", h)
    assert not auth.verify_password("pw-1234567891", h)
    assert not auth.verify_password("x", "garbage")


def test_session_idle_and_absolute_expiry():
    c = Clock()
    s = auth.SessionStore(clock=c)
    tok = s.create()
    c.t += auth.IDLE_TTL_S - 1
    assert s.check(tok)                       # 사용하면 유휴 시계가 다시 돈다
    c.t += auth.IDLE_TTL_S + 1
    assert not s.check(tok)
    tok = s.create()
    for _ in range(int(auth.ABSOLUTE_TTL_S // 600) + 1):
        c.t += 600
        s.check(tok)
    assert not s.check(tok)                   # 계속 써도 절대 만료는 온다


def test_throttle_locks_then_unlocks():
    c = Clock()
    t = auth.LoginThrottle(clock=c)
    for _ in range(auth.FAIL_LIMIT):
        t.fail()
    assert t.locked_for() > 0
    c.t += auth.LOCK_S + 1
    assert t.locked_for() == 0


def test_cookie_parse():
    assert auth.cookie_value("a=1; desk_session=xyz; b=2", "desk_session") == "xyz"
    assert auth.cookie_value("", "desk_session") is None
