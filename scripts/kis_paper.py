#!/usr/bin/env python3
"""한국투자증권 **모의투자** 주문·잔고 CLI — 시세→전략→주문→체결 한 바퀴의 주문 쪽.

    python scripts/kis_paper.py balance              # 예수금·보유 종목
    python scripts/kis_paper.py buy 005930 1         # 삼성전자 1주 시장가 매수
    python scripts/kis_paper.py sell 005930 1        # 1주 시장가 매도
    python scripts/kis_paper.py fills                # 오늘 주문·체결 내역

자격증명은 ~/.mdfeed/kis.env (MDFEED_ENV_FILE 로 바꿀 수 있음) 에서 읽는다.
  KIS_MOCK_APP_KEY / KIS_MOCK_APP_SECRET   모의투자용 키 (실전 키와 다르다)
  KIS_MOCK_ACCOUNT="12345678-01"           모의투자 계좌번호 (앞 8자리-뒤 2자리)

**이 스크립트는 모의투자 서버(openapivts)에만 붙는다.** 실전 서버 주소는 코드에 없다.
TR ID 는 모의투자용 V 접두어만 쓴다 (VTTC0802U 매수 · VTTC0801U 매도 · VTTC8434R 잔고 · VTTC8001R 체결).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import stat
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자 전용. 실전은 이 파일에 없다.
TOKEN_CACHE = Path.home() / ".mdfeed" / "kis_vts_token.json"
ENV_FILE = Path(os.environ.get("MDFEED_ENV_FILE", Path.home() / ".mdfeed" / "kis.env"))

TR = {"buy": "VTTC0802U", "sell": "VTTC0801U", "balance": "VTTC8434R", "fills": "VTTC8001R"}


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k.startswith("KIS_")})
    return env


def die(msg: str, code: int = 2) -> None:
    print(f"오류: {msg}", file=sys.stderr)
    raise SystemExit(code)


def account(env: dict[str, str]) -> tuple[str, str]:
    raw = env.get("KIS_MOCK_ACCOUNT", "").replace(" ", "")
    if not raw:
        die("KIS_MOCK_ACCOUNT 가 없습니다. ~/.mdfeed/kis.env 에 KIS_MOCK_ACCOUNT=\"12345678-01\" 을 추가하세요 "
            "(KIS Developers 마이페이지 → 서비스 신청 내역 → 모의투자계좌 행의 계좌번호)")
    cano, _, prdt = raw.partition("-")
    if len(cano) != 8 or not cano.isdigit():
        die(f"계좌번호 앞자리는 숫자 8자리여야 합니다: {cano!r}")
    return cano, (prdt or "01")


def token(env: dict[str, str]) -> str:
    key, secret = env.get("KIS_MOCK_APP_KEY", ""), env.get("KIS_MOCK_APP_SECRET", "")
    if not key or not secret:
        die("KIS_MOCK_APP_KEY / KIS_MOCK_APP_SECRET 가 없습니다")
    try:
        d = json.loads(TOKEN_CACHE.read_text())
        if d.get("access_token") and d.get("expires_at", 0) > time.time() + 300:
            return d["access_token"]
    except (OSError, ValueError):
        pass
    body = json.dumps({"grant_type": "client_credentials", "appkey": key, "appsecret": secret}).encode()
    req = urllib.request.Request(f"{BASE}/oauth2/tokenP", data=body, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.load(r)
    tok = d["access_token"]
    exp = dt.datetime.strptime(d["access_token_token_expired"], "%Y-%m-%d %H:%M:%S").timestamp()
    TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE.write_text(json.dumps({"access_token": tok, "expires_at": exp}))
    TOKEN_CACHE.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return tok


def call(env: dict[str, str], method: str, path: str, tr_id: str, params: dict | None = None, body: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token(env)}",
        "appkey": env["KIS_MOCK_APP_KEY"], "appsecret": env["KIS_MOCK_APP_SECRET"],
        "tr_id": tr_id, "custtype": "P",
    }
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.load(r)
    except urllib.error.HTTPError as e:
        die(f"HTTP {e.code} {tr_id}: {e.read().decode(errors='replace')[:400]}")
    if d.get("rt_cd") != "0":
        die(f"{tr_id} 거절: [{d.get('msg_cd')}] {d.get('msg1')}")
    return d


def cmd_balance(env: dict[str, str]) -> None:
    cano, prdt = account(env)
    d = call(env, "GET", "/uapi/domestic-stock/v1/trading/inquire-balance", TR["balance"], {
        "CANO": cano, "ACNT_PRDT_CD": prdt, "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
        "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
    summary = (d.get("output2") or [{}])[0]
    print(f"[모의투자 {cano}-{prdt}] 예수금 {int(float(summary.get('dnca_tot_amt', 0))):,}원 · "
          f"총평가 {int(float(summary.get('tot_evlu_amt', 0))):,}원 · 평가손익 {int(float(summary.get('evlu_pfls_smtl_amt', 0))):,}원")
    holdings = [h for h in d.get("output1") or [] if int(float(h.get("hldg_qty", 0))) > 0]
    if not holdings:
        print("보유 종목 없음")
    for h in holdings:
        print(f"  {h['pdno']} {h['prdt_name']:<12} {int(float(h['hldg_qty'])):>6}주 · 평균 {float(h['pchs_avg_pric']):,.0f} · "
              f"현재 {float(h['prpr']):,.0f} · 손익 {float(h['evlu_pfls_amt']):,.0f}원 ({float(h['evlu_pfls_rt']):+.2f}%)")


def cmd_order(env: dict[str, str], side: str, symbol: str, qty: int, price: int = 0) -> None:
    cano, prdt = account(env)
    body = {"CANO": cano, "ACNT_PRDT_CD": prdt, "PDNO": symbol,
            "ORD_DVSN": "01" if price == 0 else "00",        # 01 시장가 · 00 지정가
            "ORD_QTY": str(qty), "ORD_UNPR": str(price)}
    d = call(env, "POST", "/uapi/domestic-stock/v1/trading/order-cash", TR[side], body=body)
    o = d.get("output", {})
    print(f"{'매수' if side == 'buy' else '매도'} 주문 접수: {symbol} {qty}주 {'시장가' if price == 0 else f'{price:,}원'} "
          f"· 주문번호 {o.get('ODNO')} · 접수시각 {o.get('ORD_TMD')} · {d.get('msg1')}")


def cmd_fills(env: dict[str, str]) -> None:
    cano, prdt = account(env)
    today = dt.date.today().strftime("%Y%m%d")
    d = call(env, "GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", TR["fills"], {
        "CANO": cano, "ACNT_PRDT_CD": prdt, "INQR_STRT_DT": today, "INQR_END_DT": today,
        "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00", "PDNO": "", "CCLD_DVSN": "00", "ORD_GNO_BRNO": "",
        "ODNO": "", "INQR_DVSN_3": "00", "INQR_DVSN_1": "", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
    rows = d.get("output1") or []
    if not rows:
        print(f"{today} 주문 없음")
    for r in rows:
        print(f"  {r.get('ord_tmd')} {r.get('sll_buy_dvsn_cd_name')} {r.get('pdno')} {r.get('prdt_name'):<12} "
              f"주문 {int(float(r.get('ord_qty', 0)))}주 · 체결 {int(float(r.get('tot_ccld_qty', 0)))}주 @ {float(r.get('avg_prvs', 0) or 0):,.0f} "
              f"· 주문번호 {r.get('odno')}")


def main(argv: list[str]) -> int:
    env = load_env()
    if len(argv) < 2 or argv[1] not in ("balance", "buy", "sell", "fills"):
        print(__doc__)
        return 2
    cmd = argv[1]
    if cmd == "balance":
        cmd_balance(env)
    elif cmd == "fills":
        cmd_fills(env)
    else:
        if len(argv) < 4:
            die(f"사용법: {argv[0]} {cmd} <종목코드> <수량> [지정가]")
        cmd_order(env, cmd, argv[2], int(argv[3]), int(argv[4]) if len(argv) > 4 else 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
