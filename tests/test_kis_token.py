"""KIS access_token 을 몇 번 발급하는가.

KIS 는 1일 1회 발급이 원칙이고, 유효기간 안에 잦은 발급이 생기면 이용을
제한한다. 계정이 막히면 그날 KRX 수집이 통째로 멎는다.

예전엔 kis_rest 와 kis_macro 가 같은 프로세스에서 **각자** 발급했다.
파일 캐시를 보긴 했지만 콜드 스타트에서 동시에 miss 가 나서 둘 다 받았다 —
재기동 한 번에 두 번이다. (2026-08-31 확인)
"""
import asyncio
import json
import os
import time

import pytest

from mdfeed.adapters.kis_token import TokenStore


@pytest.fixture
def store(tmp_path, monkeypatch):
    TokenStore._instances.clear()
    s = TokenStore.get("KEY", "SECRET", "https://example.invalid",
                       str(tmp_path / "tok.json"))
    calls = {"n": 0}

    def fake_issue():
        calls["n"] += 1
        s.issues += 1
        exp = time.time() + 86400 - 120
        s._write_cache(f"TOK{calls['n']}", exp)
        return f"TOK{calls['n']}", exp

    # 네트워크는 타지 않는다. 파일 락과 재확인 로직만 시험한다.
    monkeypatch.setattr(s, "_issue_locked", fake_issue)
    s._calls = calls
    return s


@pytest.mark.asyncio
async def test_동시에_요청해도_한_번만_발급한다(store):
    """두 어댑터가 같이 뜨는 상황. 락 안에서 재확인이 없으면 두 번 받는다."""
    toks = await asyncio.gather(store.token(), store.token(), store.token())
    assert store._calls["n"] == 1, f"{store._calls['n']}회 발급 — 중복이다"
    assert len(set(toks)) == 1


@pytest.mark.asyncio
async def test_재기동하면_파일_캐시를_재사용한다(store, tmp_path):
    await store.token()
    assert store._calls["n"] == 1
    TokenStore._instances.clear()                    # 프로세스가 새로 뜬 셈
    fresh = TokenStore.get("KEY", "SECRET", "https://example.invalid",
                           str(tmp_path / "tok.json"))
    fresh._issue_locked = lambda: pytest.fail("캐시가 있는데 또 발급했다")
    assert await fresh.token() == "TOK1"


@pytest.mark.asyncio
async def test_두_어댑터가_같은_저장소를_공유한다(tmp_path):
    """get() 이 같은 키에 같은 인스턴스를 줘야 락이 의미가 있다."""
    TokenStore._instances.clear()
    a = TokenStore.get("K", "S", "https://x", str(tmp_path / "t.json"))
    b = TokenStore.get("K", "S", "https://x", str(tmp_path / "t.json"))
    assert a is b


@pytest.mark.asyncio
async def test_곧_만료될_토큰은_안_쓴다(store):
    """쓰다가 중간에 만료되면 재시도가 곧 재발급이다."""
    store._write_cache("OLD", time.time() + 60)      # 10분 미만 남음
    assert await store.token() != "OLD"
    assert store._calls["n"] == 1


def test_캐시를_원자적으로_쓴다(store):
    """반쯤 쓰인 JSON 을 다른 쪽이 읽으면 miss 로 보고 또 발급한다."""
    store._write_cache("T", time.time() + 86400)
    d = json.load(open(store.path, encoding="utf-8"))
    assert d["access_token"] == "T"
    leftovers = [f for f in os.listdir(os.path.dirname(store.path))
                 if f.endswith(".tmp")]
    assert not leftovers, f"임시파일이 남았다: {leftovers}"


def test_캐시_파일_권한이_600이다(store):
    store._write_cache("T", time.time() + 86400)
    assert oct(os.stat(store.path).st_mode)[-3:] == "600"
