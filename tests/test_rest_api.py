"""조회 API — 요청 경로에 누적 비용을 두지 않는다.

이 프로세스를 실시간 경로와 따로 뗀 이유는 "무거운 조회 한 방이 나머지를
막지 않게"다. 그런데 정작 프로세스 안에서 그 일이 벌어지고 있었다.
실측(560만 행): /api/v1/stats 4.6초가 도는 동안 /api/v1/quotes 가
0.01초 → 4.44초로 밀렸다. 444배다.

원인이 둘이었다.
1. 커넥션 하나를 락으로 직렬화했다 (WAL 은 읽기끼리 동시성이 있는데도)
2. COUNT(*) 를 요청마다 세었다 (누적 행수에 비례한다)
"""
import asyncio
import json
import socket
import time
import urllib.request

import pytest

from mdfeed.config import Config
from mdfeed.services.rest_api import RestAPI


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def get(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return json.loads(r.read())


@pytest.fixture
def api(tmp_path):
    """RestAPI 를 실제 포트에 띄우고, 체결 몇 건을 넣어 둔다."""
    cfg = Config()
    cfg.sqlite_path = str(tmp_path / "api.db")
    cfg.http_port = free_port()
    cfg.http_host = "127.0.0.1"
    cfg.stats_ttl_s = 60.0
    svc = RestAPI(cfg)

    from mdfeed.storage.db import SQLiteStorage
    seed = SQLiteStorage(cfg.sqlite_path)
    seed.ensure_schema()
    t = int(time.time() * 1e6)
    seed.insert_trades([(t + i, "UPBIT", "KRW-BTC", 100.0 + i, 1.0, 1, t, 10, i)
                        for i in range(50)])
    seed.close()

    stop = asyncio.Event()
    loop = asyncio.new_event_loop()
    task = None

    import threading
    ready = threading.Event()

    def run():
        nonlocal task
        asyncio.set_event_loop(loop)
        task = loop.create_task(svc.run(stop))
        loop.call_later(0.0, ready.set)
        loop.run_until_complete(task)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    ready.wait(5)
    for _ in range(100):                       # 서버가 포트를 열 때까지
        try:
            get(cfg.http_port, "/healthz")
            break
        except Exception:                      # noqa: BLE001
            time.sleep(0.05)
    yield svc, cfg.http_port
    loop.call_soon_threadsafe(stop.set)
    th.join(timeout=10)


def test_통계는_요청마다_다시_세지_않는다(api):
    """COUNT(*) 는 누적 행수에 비례한다. 요청 경로에 두면 안 된다."""
    _svc, port = api
    a = get(port, "/api/v1/stats")
    b = get(port, "/api/v1/stats")
    assert a["counts"]["trades"] == 50
    assert a["counts_as_of"] == b["counts_as_of"], "요청마다 다시 세고 있다"


def test_통계는_언제_잰_값인지_밝힌다(api):
    """몇 초 지난 값을 주는 건 괜찮다. 언제 잰 건지 안 밝히는 게 문제다."""
    _svc, port = api
    r = get(port, "/api/v1/stats")
    assert r["counts_as_of"] > 0
    assert r["counts_age_s"] >= 0
    assert "counts_took_ms" in r


def test_무거운_조회가_시세조회를_막지_않는다(api):
    """느린 DB 조회 하나가 도는 동안 가벼운 API 요청이 밀리지 않아야 한다.

    예전엔 커넥션 하나를 락으로 감싸서, /api/v1/stats 의 COUNT(*) 4.6초 동안
    /api/v1/quotes 가 4.44초로 밀렸다. 아래 SLOW 는 데이터가 아니라 재귀
    CTE 로 시간을 만든다 — 테스트가 560만 행을 만들지 않고 같은 상황을 낸다.
    """
    svc, port = api
    SLOW = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL "
            "SELECT x+1 FROM c WHERE x < 4000000) SELECT COUNT(*) AS n FROM c")

    import threading
    th = threading.Thread(target=lambda: svc.storage.query(SLOW), daemon=True)
    th.start()
    time.sleep(0.05)
    t = time.perf_counter()
    r = get(port, "/api/v1/quotes?symbol=KRW-BTC")
    fast_s = time.perf_counter() - t
    th.join(timeout=10)
    assert r["count"] == 1
    assert fast_s < 0.2, f"시세 조회가 {fast_s*1000:.0f}ms — 무거운 조회 뒤에 줄 서 있다"


def test_종목목록_기본값은_종목_수에_비례한다(api):
    """봉 통계는 누적 봉 수에 비례한다. 목록을 보려던 요청이 그 값을 물면 안 된다."""
    _svc, port = api
    r = get(port, "/api/v1/symbols")
    assert r["count"] == 1
    item = r["items"][0]
    assert item["symbol"] == "KRW-BTC" and "last_ts" in item
    assert "bars" not in item, "기본값에서 봉 통계를 세고 있다"


def test_봉_통계는_명시적으로_요청해야_나온다(api):
    _svc, port = api
    r = get(port, "/api/v1/symbols?bars=1")
    assert r["count"] == 0            # 봉이 아직 없다 (체결만 넣었다)
