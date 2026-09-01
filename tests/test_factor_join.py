"""참조 평면(재무제표) ↔ 실시간 평면(피드) 조인이 실제로 닿는가.

factor_screen.py 의 첫 문단이 이 프로젝트의 주장이다.

    실시간 평면 : 거래소 WS → feedd → 버스 → 1분봉
    참조 평면   : SEC EDGAR / OpenDART → 재무제표 487k건
    두 평면을 잇는 조인 키는 DART stock_code = KIS 종목코드다

그런데 국내 주식은 **두 경로**로 들어온다.

    KIS  실시간 WebSocket — 소켓당 등록 한도 때문에 3종목
    KRX  REST 스윕        — 전 종목, 2,035종목

조인은 venue="KIS" 하나만 봤다. **3종목에만 닿고 있었다.**
그리고 예전 문서가 그 증상을 미리 변명해 뒀다 —
"KIS 데이터가 없으면 전부 None 이고, 그게 정상이다". 정상이 아니었다.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "quant"))
from factor_screen import EQUITY_VENUES, join_with_feed   # noqa: E402


@pytest.fixture
def db(tmp_path):
    """KIS 3종목 · KRX 다수라는 실제 형태를 그대로 만든다."""
    p = tmp_path / "t.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE bars_1m (bucket INTEGER, venue TEXT, symbol TEXT, "
              "close REAL, volume REAL)")
    rows = [(1, "KIS", "005930", 268750.0, 10.0),
            (1, "KIS", "000660", 1754000.0, 5.0),
            (1, "KRX", "005930", 268000.0, 99.0),   # 같은 종목이 양쪽에 있다
            (1, "KRX", "000020", 5090.0, 3.0),
            (1, "KRX", "005380", 398000.0, 7.0),
            (1, "BINANCE", "BTCUSDT", 1.0, 1.0)]
    c.executemany("INSERT INTO bars_1m VALUES (?,?,?,?,?)", rows)
    c.commit(); c.close()
    return str(p)


def _rows(*keys):
    return [{"key": k} for k in keys]


def test_전_종목_스윕까지_봐야_조인이_닿는다(db):
    r = join_with_feed(_rows("005930", "000020", "005380"), db)
    assert all(x["feed_linked"] for x in r), (
        f"연결 {sum(x['feed_linked'] for x in r)}/3 — KIS 만 보고 있다")


def test_KIS_만_보면_대부분_놓친다(db):
    """옛 동작을 재현해 차이를 못 박는다."""
    r = join_with_feed(_rows("005930", "000020", "005380"), db, venues=("KIS",))
    assert sum(x["feed_linked"] for x in r) == 1


def test_양쪽에_있으면_실시간을_고른다(db):
    """KIS 는 실시간 체결, KRX 는 REST 스윕이다. 신선한 쪽이 낫다."""
    (r,) = join_with_feed(_rows("005930"), db)
    assert r["feed_source"] == "KIS"
    assert r["live_price"] == 268750.0


def test_어느_경로에서_붙었는지_밝힌다(db):
    """실시간 체결가와 스윕 값은 신선도가 다르다. 읽는 쪽이 구분할 수 있어야 한다."""
    r = {x["key"]: x for x in join_with_feed(_rows("005930", "000020"), db)}
    assert r["005930"]["feed_source"] == "KIS"
    assert r["000020"]["feed_source"] == "KRX"


def test_크립토는_조인_대상이_아니다(db):
    """대응하는 재무제표가 없다. 억지로 붙이지 않는다."""
    assert "BINANCE" not in EQUITY_VENUES
    (r,) = join_with_feed(_rows("BTCUSDT"), db)
    assert not r["feed_linked"]


def test_DB_가_없어도_필드는_항상_있다(tmp_path):
    """호출한 쪽이 KeyError 를 만나면 안 된다."""
    (r,) = join_with_feed(_rows("005930"), str(tmp_path / "없음.db"))
    assert r["feed_linked"] is False
    assert r["live_price"] is None and r["feed_source"] is None
