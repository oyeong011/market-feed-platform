"""한국투자증권(KIS) 국내주식 실시간 체결 어댑터.

크립토 어댑터와 달리 자격증명이 필요하다. 키가 없으면 스스로 비활성화되고
feedd 는 나머지 어댑터로 정상 기동한다 — 선택적 업스트림이 전체 서비스를
막지 않게 하는 게 운영 관점의 핵심이다.

프로토콜 메모
-------------
* 접속키(approval_key)를 HTTPS POST /oauth2/Approval 로 먼저 받는다.
  계좌 주문용 access_token 과는 다른 키이고, 계좌번호는 실시간 시세에 필요 없다.
* 실시간은 **평문 ws://** 다 (실전 21000, 모의 31000).
* 구독/해지 요청은 JSON, 그런데 **시세 본문은 파이프(|)와 캐럿(^) 구분 문자열**이다.
      0|H0STCNT0|001|005930^093015^70000^...
      [0]암호화여부 [1]TR_ID [2]데이터건수 [3]본문(^ 구분)
* 체결(H0STCNT0) 필드 인덱스는 KIS 실시간 시세 명세 기준으로 상수에 고정했다.
* 소켓당 등록 한도가 있다. 문서 기준값은 41이지만 **계좌마다 다르다** —
  실전 계좌로 5종목을 요청했더니 3개만 성공하고 나머지는 `MAX SUBSCRIBE OVER` 로
  거절됐다. 그래서 고정 상수를 믿지 않고, 거절을 만나면 그 시점의 성공 개수를
  실효 한도로 학습해 다음 접속부터는 그만큼만 요청한다.

실전에서 걸리는 것들
--------------------
1. **PINGPONG 을 되돌려주지 않으면 끊긴다.**
   KIS 는 주기적으로 `{"header":{"tr_id":"PINGPONG"}}` 를 **텍스트 메시지**로 보낸다.
   JSON 이라는 이유로 버리면 몇 분 뒤 세션이 죽고 재접속 루프만 도는 상태가 된다.
   원인이 로그에 안 남아 찾기 어려운 종류다.

   응답은 **텍스트 회신이 아니라 WebSocket PONG 제어 프레임**이다. 공식 예제가
   `await websocket.pong(data)` 를 쓴다. 텍스트로 되돌려주면 서버가 하트비트
   응답으로 인정하지 않을 수 있어, 같은 방식으로 맞췄다.
2. **등록 응답을 확인하지 않으면 구독 실패를 모른다.**
   종목코드가 틀리거나 한도를 넘으면 서버는 rt_cd != '0' 인 JSON 을 보내는데,
   이걸 무시하면 "연결은 됐는데 데이터가 안 오는" 상태가 된다.
3. **암호화 플래그**: 본문 첫 필드가 '1' 이면 암호화된 데이터다.
   체결통보(H0STCNI0)에만 해당하고 시세는 '0' 이지만, '1' 이 오면 파싱하지 않고 센다.
4. **체결시각이 HHMMSS 뿐이다.**
   날짜가 없어서 그대로는 epoch 시각을 만들 수 없다. 오늘 KST 날짜를 붙여 합성하되,
   합성 결과가 현재 시각과 크게 벌어지면(장 마감 직후 지연 전송, 시계 이상 등)
   그 값을 믿지 않고 수신시각으로 대체하고 그 횟수를 센다.
   조용히 틀린 타임스탬프를 만드는 것이 지연 지표를 못 재는 것보다 나쁘다.

5. **장 시간 밖에는 데이터가 없다.**
   국내주식 체결은 평일 09:00~15:30 KST 에만 흐른다. 이 구간 밖의 무데이터를
   정체(staleness)로 판정해 재접속하면, 밤새 의미 없는 재접속만 반복한다.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import urllib.request

from ..models import BookTop, Trade, SIDE_BUY, SIDE_SELL, SIDE_UNKNOWN, now_ns
from ..wsproto import WSClient
from .base import Adapter

log = logging.getLogger("mdfeed.adapter.kis")

ENDPOINTS = {
    # env      REST(approval_key 발급)                       WS(실시간)
    "real": ("https://openapi.koreainvestment.com:9443", "ws://ops.koreainvestment.com:21000"),
    "vts":  ("https://openapivts.koreainvestment.com:29443", "ws://ops.koreainvestment.com:31000"),
}

TR_TRADE = "H0STCNT0"
MAX_SUBSCRIPTIONS = 41

# H0STCNT0 본문 필드 인덱스 (KIS 실시간 시세 명세)
# 공식 저장소(koreainvestment/open-trading-api)의 menulist 46개 필드와 대조 확인함
F_CODE, F_TIME, F_PRICE = 0, 1, 2        # 종목코드 · 체결시간 · 현재가
F_ASK1, F_BID1, F_VOL = 10, 11, 12       # 매도호가1 · 매수호가1 · 체결거래량
F_SIGN = 21                              # 체결구분 (아래 SIDE_BY_SIGN 참고)
F_ASK_QTY, F_BID_QTY = 36, 37            # 매도호가잔량 · 매수호가잔량

# 체결구분(CCLD_DVSN) → 체결 방향.
#
# 문서에는 `1 매수 / 3 매도 / 5 장전` 으로 적혀 있으나, 실전 계좌로 300건을 받아
# 체결가를 호가와 대조한 결과는 달랐다.
#
#     체결구분  매도호가 이상 체결   매수호가 이하 체결   판정
#     1                    70                   3     매수
#     5                     0                 173     매도
#
# 즉 이 피드에서 매도는 `5` 로 온다. 문서대로 `3` 만 매도로 처리하면 매도 체결이
# 전부 UNKNOWN 이 되어 주문흐름 방향이 통째로 사라진다. 관측값을 기준으로 삼되,
# 다른 구간에서 `3` 이 올 가능성도 있으므로 함께 매도로 둔다.
SIDE_BY_SIGN = {"1": SIDE_BUY, "3": SIDE_SELL, "5": SIDE_SELL}

KST = dt.timezone(dt.timedelta(hours=9))
MARKET_OPEN = dt.time(9, 0)
MARKET_CLOSE = dt.time(15, 40)      # 동시호가 여유를 둔 값

# 합성한 체결시각이 현재와 이만큼 벌어지면 신뢰하지 않는다
MAX_TS_DRIFT_S = 600.0


def synth_event_ns(hhmmss: str, recv_ns: int) -> tuple[int, bool]:
    """`093015` 같은 체결시각에 오늘 KST 날짜를 붙여 epoch ns 로 만든다.

    반환값의 두 번째는 `합성에 성공했는가`. 실패하거나 현재와 10분 넘게 벌어지면
    수신시각을 그대로 돌려주고 False 를 준다.
    """
    if len(hhmmss) != 6 or not hhmmss.isdigit():
        return recv_ns, False
    try:
        h, m, sec = int(hhmmss[:2]), int(hhmmss[2:4]), int(hhmmss[4:6])
        now = dt.datetime.fromtimestamp(recv_ns / 1e9, KST)
        evt = now.replace(hour=h, minute=m, second=sec, microsecond=0)
    except ValueError:
        return recv_ns, False
    ns = int(evt.timestamp() * 1e9)
    if abs(recv_ns - ns) > MAX_TS_DRIFT_S * 1e9:
        return recv_ns, False
    return ns, True


def market_is_open(now: dt.datetime | None = None) -> bool:
    """국내 정규장 시간대인가. 공휴일은 반영하지 않는다(휴장일엔 그냥 데이터가 없다)."""
    n = now or dt.datetime.now(KST)
    if n.weekday() >= 5:                 # 토·일
        return False
    return MARKET_OPEN <= n.time() <= MARKET_CLOSE


class KISAdapter(Adapter):
    name = "kis"
    stale_after_s = 180.0

    def __init__(self, cfg, emit, registry=None):
        super().__init__(cfg, emit, registry)
        self.app_key = cfg.kis_app_key
        self.app_secret = cfg.kis_app_secret
        self.env = (getattr(cfg, "kis_env", "real") or "real").lower()
        if self.env not in ENDPOINTS:
            log.warning("알 수 없는 KIS_ENV=%s → real 로 처리", self.env)
            self.env = "real"
        self.rest_base, self.ws_url = ENDPOINTS[self.env]
        self.symbols = cfg.kis_symbols[:MAX_SUBSCRIPTIONS]
        if len(cfg.kis_symbols) > MAX_SUBSCRIPTIONS:
            log.warning("KIS 소켓당 등록 한도 %d 초과 → 앞의 %d개만 구독",
                        MAX_SUBSCRIPTIONS, MAX_SUBSCRIPTIONS)
        self._approval_key: str | None = None
        self.subscribed: list[str] = []
        self.rejected: list[dict] = []
        self.pingpongs = 0
        self.encrypted_skipped = 0
        self.unknown_signs: dict[str, int] = {}
        self.ts_synth_ok = 0
        self.ts_synth_fallback = 0
        # 거절을 만나기 전까지는 알 수 없다. 학습되면 다음 접속부터 적용한다.
        self.effective_limit: int | None = None

    # ── 활성화 조건 ───────────────────────────────────────────────────────
    def enabled(self) -> bool:
        return bool(self.app_key and self.app_secret)

    def disabled_reason(self) -> str:
        return "KIS_APP_KEY / KIS_APP_SECRET 미설정"

    def expects_data(self) -> bool:
        # 장이 닫혀 있으면 데이터가 없는 게 정상이다. 재접속을 유발하지 않는다.
        return market_is_open()

    # ── 접속키 ────────────────────────────────────────────────────────────
    async def _get_approval_key(self) -> str:
        """블로킹 HTTP 호출을 스레드로 밀어 이벤트 루프를 막지 않는다."""
        def _call() -> str:
            body = json.dumps({
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "secretkey": self.app_secret,
            }).encode()
            req = urllib.request.Request(
                f"{self.rest_base}/oauth2/Approval", data=body,
                headers={"content-type": "application/json; charset=utf-8"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            key = data.get("approval_key")
            if not key:
                # 응답 본문에 키가 없으면 사유가 들어 있다. 다만 자격증명이
                # 섞여 나올 수 있으므로 전체를 로그에 남기지 않는다.
                raise RuntimeError(
                    f"approval_key 발급 실패 (응답 필드: {sorted(data)})")
            return key
        return await asyncio.to_thread(_call)

    # ── 세션 ──────────────────────────────────────────────────────────────
    async def session(self) -> None:
        if not self.enabled():
            raise RuntimeError(self.disabled_reason())
        if not self._approval_key:
            self._approval_key = await self._get_approval_key()
            log.info("[kis] approval_key 발급 완료 (env=%s)", self.env)

        ws = await WSClient.connect(self.ws_url)
        self.subscribed, self.rejected = [], []
        want = self.symbols
        if self.effective_limit is not None and len(want) > self.effective_limit:
            want = want[:self.effective_limit]
            log.info("[kis] 지난 세션에서 학습한 실효 한도 %d 적용 → %s 만 요청",
                     self.effective_limit, want)
        try:
            for code in want:
                await ws.send_text(json.dumps({
                    "header": {"approval_key": self._approval_key, "custtype": "P",
                               "tr_type": "1", "content-type": "utf-8"},
                    "body": {"input": {"tr_id": TR_TRADE, "tr_key": code}},
                }))
                await asyncio.sleep(0.05)      # 등록 요청 폭주 방지

            if not market_is_open():
                log.info("[kis] 장 시간이 아니다 (KST %s). 연결은 유지하되 "
                         "데이터는 개장 후에 흐른다",
                         dt.datetime.now(KST).strftime("%m-%d %H:%M"))

            while True:
                _op, raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_after_s)
                await self._on_message(raw, ws)
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    # ── 수신 처리 ─────────────────────────────────────────────────────────
    async def _on_message(self, raw: bytes, ws) -> None:
        text = raw.decode("utf-8", "ignore")

        if text.startswith("{"):
            await self._on_control(text, ws)
            return

        parts = text.split("|")
        if len(parts) < 4:
            return
        if parts[0] == "1":
            # 암호화된 본문. 시세에는 오지 않지만 오면 파싱하지 않고 센다
            self.encrypted_skipped += 1
            return
        if parts[1] != TR_TRADE:
            return

        ts_recv = now_ns()
        f = parts[3].split("^")
        try:
            count = int(parts[2])
        except ValueError:
            count = 1
        if count <= 0:
            return
        stride = len(f) // count
        if stride <= F_SIGN:
            return

        for i in range(count):
            g = f[i * stride: (i + 1) * stride]
            if len(g) <= F_SIGN:
                continue
            try:
                price = float(g[F_PRICE])
                vol = float(g[F_VOL])
            except ValueError:
                continue
            side = SIDE_BY_SIGN.get(g[F_SIGN], SIDE_UNKNOWN)
            if side is SIDE_UNKNOWN:
                # 매핑에 없는 값이 오면 조용히 넘기지 말고 센다. 거래소가
                # 코드를 바꾸면 여기가 먼저 알려준다.
                self.unknown_signs[g[F_SIGN]] = self.unknown_signs.get(g[F_SIGN], 0) + 1
            # 체결시각(HHMMSS)에 오늘 날짜를 붙여 실제 수집 지연을 잴 수 있게 한다
            ts_event, ok = synth_event_ns(g[F_TIME], ts_recv)
            if ok:
                self.ts_synth_ok += 1
            else:
                self.ts_synth_fallback += 1
            self._mark(Trade(
                venue="KIS", symbol=g[F_CODE],
                ts_event_ns=ts_event, ts_recv_ns=ts_recv,
                price=price, qty=vol, side=side,
            ))
            try:
                bid, ask = float(g[F_BID1]), float(g[F_ASK1])
            except (ValueError, IndexError):
                continue
            # 호가잔량도 같은 메시지에 실려 온다. 없으면 0 으로 둔다.
            def _q(idx: int) -> float:
                try:
                    return float(g[idx])
                except (ValueError, IndexError):
                    return 0.0
            if bid > 0 and ask > 0:
                self._mark(BookTop(
                    venue="KIS", symbol=g[F_CODE], ts_event_ns=ts_event,
                    ts_recv_ns=ts_recv, bid=bid, bid_qty=_q(F_BID_QTY),
                    ask=ask, ask_qty=_q(F_ASK_QTY),
                ))

    async def _on_control(self, raw_text: str, ws) -> None:
        """등록 응답과 PINGPONG 처리."""
        try:
            msg = json.loads(raw_text)
        except json.JSONDecodeError:
            return
        header = msg.get("header") or {}
        tr_id = header.get("tr_id")

        if tr_id == "PINGPONG":
            # 공식 예제와 동일하게 PONG 제어 프레임으로 응답한다.
            await ws.pong(raw_text.encode())
            self.pingpongs += 1
            return

        body = msg.get("body") or {}
        rt_cd = body.get("rt_cd")
        msg1 = (body.get("msg1") or "").strip()
        tr_key = (header.get("tr_key")
                  or ((body.get("output") or {}).get("tr_key")))
        if rt_cd is None:
            return
        # 공식 예제 기준: rt_cd '0' 성공, '1' 오류.
        # 'ALREADY IN SUBSCRIBE' 는 재접속 직후 흔히 나오고 실제 구독은 살아 있다.
        if rt_cd == "0" or msg1.upper() == "ALREADY IN SUBSCRIBE":
            if tr_key and tr_key not in self.subscribed:
                self.subscribed.append(tr_key)
            log.info("[kis] 구독 등록 %s %s (%d/%d)",
                     tr_key or "?", msg1 or "OK",
                     len(self.subscribed), len(self.symbols))
        else:
            # 조용히 지나가면 "연결은 됐는데 데이터가 안 오는" 상태가 된다
            info = {"tr_key": tr_key, "rt_cd": rt_cd, "msg": msg1}
            self.rejected.append(info)
            if msg1.upper().startswith("MAX SUBSCRIBE"):
                if self.effective_limit is None or len(self.subscribed) < self.effective_limit:
                    self.effective_limit = len(self.subscribed)
                log.error("[kis] 구독 한도 도달 (%s). 이 계좌의 실효 한도를 %d 로 "
                          "학습했다. 더 필요하면 소켓을 나누거나 한도 상향이 필요하다.",
                          msg1, self.effective_limit)
            else:
                log.error("[kis] 구독 등록 실패 %s", info)

    # ── 헬스 ──────────────────────────────────────────────────────────────
    def health(self) -> dict:
        d = super().health()
        d.update({
            "env": self.env,
            "market_open": market_is_open(),
            "requested": len(self.symbols),
            "subscribed": len(self.subscribed),
            "rejected": self.rejected,
            "pingpongs": self.pingpongs,
            "encrypted_skipped": self.encrypted_skipped,
            "effective_limit": self.effective_limit,
            "unknown_signs": self.unknown_signs,
            "ts_synth_ok": self.ts_synth_ok,
            "ts_synth_fallback": self.ts_synth_fallback,
        })
        return d
