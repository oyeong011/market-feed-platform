# 운영 런북

장애가 났을 때 **무엇을 확인하고 무엇을 하는지**를 증상별로 적습니다.
새벽에 호출받은 사람이 이 문서만 보고 1차 조치를 끝낼 수 있어야 합니다.

---

## 0. 첫 3분: 무조건 이것부터

```bash
./ops/ops.sh diag
```

이 하나가 다음을 전부 훑습니다.

- 6개 서비스의 프로세스 생존 · HTTP 헬스 · 리스닝 포트
- UDS 버스 소켓 존재 여부와 권한
- 업스트림별 메시지 수 · 재접속 횟수 · **마지막 메시지 이후 경과 시간**
- 거래소별 시계 오프셋 (지연 지표를 믿어도 되는지)
- 시퀀스 갭 / 유실 / 중복 건수
- 적재 행수 · 대기 행수 · DB 오류
- 디스크 · 메모리 · NTP 동기화 상태

판정만 필요하면:

```bash
python3 ops/healthcheck.py        # 종료코드 0=OK / 1=WARN / 2=CRIT
python3 ops/healthcheck.py --json # 기계 판독용
```

---

## 서비스 지도

| 서비스 | 데이터 포트 | 관리 포트 | 역할 | 죽으면 |
|---|---|---|---|---|
| `feedd` | – | 9100 | 수집·정규화·버스 발행 | **전면 장애.** 모든 하류가 멈춤 |
| `tcp-gateway` | 9101 | 9111 | MDFP/1 바이너리 배포 | 외부 구독자 단절 |
| `ws-gateway` | 9102 | 9102 | WebSocket + 대시보드 | 브라우저 화면만 |
| `rest-api` | 9103 | 9103 | 과거 데이터 조회 | 조회만. 수집은 정상 |
| `writer` | – | 9104 | DB 적재 · 1분봉 | **데이터 영구 유실 시작** |
| `strategy` | – | 9105 | 지표 → 시그널 | 시그널만 |

```
systemctl status  mdfeed-feedd
systemctl restart mdfeed-tcp-gateway
systemctl start   mdfeed.target      # 전체
journalctl -u mdfeed-feedd -f
journalctl -u 'mdfeed-*' --since '10 min ago'
```

---

## 증상 1 — 시세가 안 움직인다 (프로세스는 살아 있음)

**가장 흔한 장애입니다.** 그리고 systemd가 절대 잡지 못하는 종류입니다.

### 진단

```bash
curl -s localhost:9100/healthz | python3 -m json.tool
```

`upstreams[].last_msg_age_s` 를 봅니다.

| 값 | 의미 |
|---|---|
| 0 ~ 5초 | 정상 |
| 30초 이상 | 의심. 새벽 한산한 시간이면 정상일 수 있음(Upbit는 분 단위로 비기도 함) |
| **120초 이상** | 장애. `stale: true` 로 표시됨 |

`stale: true` 인데 `reconnects` 가 안 늘어난다면 **TCP half-open** 입니다.
소켓은 `ESTABLISHED` 인데 데이터가 안 옵니다.

> [!warning] 이 조합이면 재시작하기 전에 지표부터 보세요 (2026-08-31 실측)
> ```
> stale                        1        ← 정체 판정은 정상
> stale_restarts_total         2        ← 감시도 발동
> adapter_task_deaths_total    0        ← 태스크는 살아 있음
> reconnects_total       3에서 멈춤     ← 그런데 재접속이 안 돎
> /healthz               healthy: true  ← 아무 표시 없음
> ```
> **감지·지표·경보가 전부 정상인데 복구만 안 되는 상태**입니다.
> upbit 이 이 상태로 11.2시간 멎어 있었습니다. 원인은 취소한 세션의
> 정리(죽은 소켓에 close → drain)가 안 끝나 재접속이 그 뒤에 서 있던 것입니다.
>
> 지금은 취소에 기한(5초 × 2회)이 있고, 못 끝내면 태스크를 버리고 재접속합니다.
> **버린 사실은 지표에 남습니다.**
> ```bash
> curl -s localhost:9100/metrics | grep session_cancel_timeouts_total
> ```
> 이 값이 오르면 정리가 안 끝나는 어댑터가 있다는 뜻입니다. 시세는 돌아오지만
> 태스크가 프로세스에 쌓이므로 `rss_mb` 와 `fd_open` 을 같이 보세요.
> 경보: `SessionCancelTimeout`

```bash
# 소켓 상태 확인 — Send-Q 가 쌓여 있으면 상대가 안 받는 것
ss -tnp | grep -E 'api.upbit|binance'

# 거래소 자체 상태
curl -s -o /dev/null -w '%{http_code}\n' https://api.upbit.com/v1/market/all
curl -s -o /dev/null -w '%{http_code}\n' https://api.binance.com/api/v3/ping
```

### 조치

1. 어댑터에 정체 감지가 있어 `stale_after_s`(Upbit 90초 / Binance 60초) 뒤 스스로
   끊고 재접속합니다. **2분은 기다려 봅니다.**
2. 그래도 안 되면 `systemctl restart mdfeed-feedd`
3. 거래소 점검 공지 확인. 점검 중이면 기다리는 것 외에 할 일이 없고,
   그 사실을 기록해 둡니다(사후 분석에서 "우리 문제"로 오인되는 걸 막습니다).
4. **`inactive_upstreams` 도 확인하세요.** 자격증명 누락으로 어댑터가 조용히 비활성화된
   상태일 수 있습니다. 사유가 그대로 노출됩니다.
5. **`degraded_upstreams` 를 보세요.** 거래소 하나만 죽으면 `healthy` 는 계속 `true`
   입니다(재시작해도 그 거래소는 안 살아나므로 생존 판정을 뒤집지 않습니다).
   대신 죽은 거래소가 이 목록에 이름으로 뜹니다.
   ```bash
   curl -s localhost:9100/healthz | python3 -c \
     'import json,sys; print(json.load(sys.stdin)["degraded_upstreams"])'
   ```

---

## 증상 2 — 구독자가 시퀀스 갭을 보고한다

### 진단

```bash
curl -s localhost:9104/healthz | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["sequence"])'
curl -s localhost:9111/subscribers | python3 -m json.tool
```

`subscribers[].dropped` 와 `backlog` 를 봅니다.

| 관찰 | 원인 | 조치 |
|---|---|---|
| 특정 구독자만 `dropped` 큼 | **그 구독자가 느림.** 백프레셔가 정상 동작한 것 | 구독자 측 문제. 심볼 필터를 좁히거나 처리 속도 개선 |
| 전 구독자가 `dropped` 큼 | 게이트웨이 자체가 못 따라감 | CPU 확인. `MDFEED_CLIENT_QUEUE` 상향 검토 |
| `writer` 만 갭 | 적재가 느림 | 증상 4로 |
| 갭이 있는데 아무도 안 느림 | **재넘버링 버그 의심** | 아래 참조 |

### 재넘버링을 확인하는 법

과거에 실제로 있었던 결함입니다. 게이트웨이는 구독자별로 seq를 다시 매겨야 하는데,
전역 seq를 흘리면 필터로 걸러진 번호가 구독자에겐 유실로 보입니다.

```bash
# 참조 클라이언트로 직접 확인. 무결성이 100% 여야 정상
python3 -m mdfeed.cli client --symbols UPBIT:KRW-BTC --duration 20 --quiet
```

`데이터 무결성: 100.0000%` 가 아니면 배포단 회귀입니다.
`tests/test_e2e.py::test_full_pipeline` 이 이를 고정하고 있으므로 CI도 확인하세요.

---

## 증상 3 — 지연시간이 이상하다 (음수이거나 튄다)

### 진단

```bash
curl -s localhost:9100/healthz | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print(d["clock"]); print(d["clock_warning"])'
```

`local_clock_behind: true` 또는 `offset_us` 가 음수로 크면 **로컬 시계가 뒤처진 것**입니다.

```bash
timedatectl status              # NTPSynchronized: yes 여야 함
chronyc tracking                # 또는
systemctl status systemd-timesyncd
```

### 조치

```bash
sudo timedatectl set-ntp true
sudo systemctl restart chronyd     # 또는 systemd-timesyncd
```

**보정 로직이 있으므로 서비스는 계속 정상 동작합니다.** 다만 오프셋이 크면
지연 지표의 신뢰도가 떨어지므로 시각을 맞추는 것이 맞습니다.
원시값은 `mdfeed_ingest_latency_raw_*` 로 따로 노출되니 보정 전후를 비교할 수 있습니다.

### p99만 튀는 경우

```bash
curl -s localhost:9100/metrics | grep ingest_latency
```

`p50`은 정상인데 `p99`만 크면 **꼬리 지연**입니다. 흔한 원인:

- GC 일시정지 → 프로세스 RSS 확인 (`./ops/ops.sh top`)
- 디스크 I/O 대기 → `iostat -x 1`
- 거래소 쪽 혼잡 (다른 venue는 멀쩡한지 비교)
- CPU 스로틀링 / 다른 프로세스와 경합

---

## 증상 4 — DB 적재가 밀린다

### 진단

```bash
curl -s localhost:9104/healthz | python3 -m json.tool
```

| 필드 | 임계 | 의미 |
|---|---|---|
| `pending_rows` | 50,000 초과 | DB 쓰기가 수집 속도를 못 따라감 |
| `db_errors` | 1 이상 | 연결/쿼리 실패 |
| `backend` | `sqlite` | **Postgres 연결에 실패해 폴백된 상태** |

### 조치

```bash
# Postgres 상태
pg_isready -h 127.0.0.1 -U mdfeed
psql "$DATABASE_URL" -c "SELECT count(*) FROM pg_stat_activity;"

# 디스크
df -h /var/lib/mdfeed
psql "$DATABASE_URL" -c "SELECT pg_size_pretty(pg_database_size('mdfeed'));"

# 느린 쿼리
psql "$DATABASE_URL" -c \
  "SELECT pid, now()-query_start AS dur, left(query,60) FROM pg_stat_activity
   WHERE state='active' ORDER BY dur DESC LIMIT 5;"
```

**폴백된 경우**: SQLite에 데이터가 계속 쌓이고 있으므로 급하지 않습니다.
Postgres를 복구한 뒤 `mdfeed-writer` 를 재시작하면 다시 붙습니다.
SQLite에 쌓인 구간은 별도 이관이 필요합니다(`data/mdfeed.db`).

**디스크가 찬 경우**: 틱 테이블이 원인입니다. TimescaleDB 보존정책이 걸려 있는지 확인:

```sql
SELECT * FROM timescaledb_information.jobs WHERE proc_name='policy_retention';
-- 없으면
SELECT add_retention_policy('trades', INTERVAL '30 days');
```

---

## 증상 5 — 서비스가 재시작을 반복한다

```bash
systemctl status mdfeed-feedd
journalctl -u mdfeed-feedd --since '30 min ago' | grep -E '치명적|Traceback|ERROR'
```

`start-limit-hit` 상태면 systemd가 **의도적으로 멈춘 것**입니다
(`StartLimitBurst=5` / 60초). 설정 오류로 무한 재시작하는 것보다 낫습니다.

```bash
systemctl reset-failed mdfeed-feedd
systemctl start mdfeed-feedd
```

**원인을 먼저 찾으세요.** 흔한 것:

| 로그 | 원인 |
|---|---|
| `UDS 소켓 경로가 너무 깁니다` | `MDFEED_RUN_DIR` 이 깊음. 104바이트 제한 |
| `Address already in use` | 이전 프로세스가 안 죽음. `ss -ltnp \| grep 910` |
| `Permission denied` (버스 소켓) | `/run/mdfeed` 소유권. `chown mdfeed:mdfeed` |
| `활성 어댑터가 없다` | 전 어댑터가 비활성. `inactive_upstreams` 사유 확인 |

워치독은 시간당 4회를 넘으면 **재시작을 포기하고 알립니다.**
재시작으로 못 고치는 문제라는 뜻이니 로그를 보세요.

---

## 증상 6 — 정지 명령이 안 먹는다 / 종료가 30초 걸린다

과거에 실제로 있던 결함입니다. Python 3.12+ 의 `Server.wait_closed()` 가 연결 핸들러
종료를 기다리는데, 핸들러는 큐에서 영원히 대기해 종료가 멈췄습니다.

지금은 **리스너를 닫기 전에 연결 핸들러부터 취소**하고, `wait_closed()` 에 5초
타임아웃을 겁니다. 그래도 30초가 걸린다면 회귀입니다.

```bash
# 어느 단계에서 멈추는지
journalctl -u mdfeed-feedd -n 50 | tail -20
# "종료 신호. 어댑터 정리 중..." 다음에 "정리 완료" 가 나와야 정상

# 강제 종료가 필요하면 (버퍼 손실을 감수)
systemctl kill -s SIGKILL mdfeed-feedd
```

`TimeoutStopSec=30` 은 버퍼 flush와 DB 커밋에 필요한 시간입니다.
**짧게 줄이지 마세요.** 줄이면 배포마다 데이터가 샙니다.

### 정리가 안 끝나면 우리가 먼저 내려갑니다 (2026-08-31 추가)

SIGTERM 을 잡는 이유는 버퍼를 flush 하기 위해서입니다. 잡기만 하고 정리가
안 끝나면 systemd 가 30초 뒤 SIGKILL 하고 버퍼는 똑같이 날아갑니다 —
**잡은 쪽이 나은 점이 하나도 없고, 왜 안 끝났는지 기록도 안 남습니다.**

정지 요청 뒤 `MDFEED_SHUTDOWN_GRACE_S`(기본 20초, TimeoutStopSec 보다 짧게)를
재고, 넘기면 **남은 태스크 이름을 찍고** 종료코드 3 으로 내려갑니다.

```
ERROR 정리가 20.0s 안에 안 끝났다 — 남은 태스크 2종: _flush_loop, _consume
ERROR 여기서 안 내려가면 systemd 가 SIGKILL 한다. 같은 결과라면 이유를 남기고 우리가 내려간다
```

이 줄이 보이면 **그 태스크 이름이 곧 원인**입니다. 종료코드 3 은 정상 종료가
아니므로 `Restart=` 정책이 어떻게 반응하는지 함께 확인하세요.

---

## 증상 7 — 대시보드가 비어 있다

```bash
curl -s localhost:9102/healthz | python3 -m json.tool
curl -s localhost:9102/api/clients | python3 -m json.tool
```

| 관찰 | 원인 |
|---|---|
| `upstream_connected: false` | ws-gateway가 버스에 못 붙음. `feedd` 먼저 확인 |
| `symbols: 0` | 데이터가 아직 안 흐름. 증상 1로 |
| `ws_clients: 0` 인데 브라우저는 열려 있음 | WebSocket 핸드셰이크 실패. 리버스 프록시 설정 확인 |

리버스 프록시(nginx) 뒤에 둔 경우 업그레이드 헤더를 통과시켜야 합니다:

```nginx
location /ws {
    proxy_pass http://127.0.0.1:9102;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;      # 무거래 구간에 끊기지 않게
}
```

---

## 증상 8 — 데이터는 흐르는데 값이 이상하다

가장 위험한 종류입니다. **아무 알람도 안 울립니다.**

### 확인

```bash
# 스프레드 이상 (음수이거나 비정상적으로 큼)
curl -s localhost:9100/snapshot | python3 -c '
import json,sys
for i in json.load(sys.stdin)["items"]:
    s = i.get("spread_bp")
    if s is not None and (s < 0 or s > 500):
        print("이상:", i["venue"], i["symbol"], "spread", s, "bp")'

# 가격 점프 (직전 봉 대비 10% 이상)
psql "$DATABASE_URL" -c "
SELECT venue, symbol, bucket, close,
       LAG(close) OVER (PARTITION BY venue,symbol ORDER BY bucket) AS prev
FROM bars_1m WHERE bucket > now() - interval '1 hour'
ORDER BY bucket DESC LIMIT 50;"

# 봉 무결성 (OHLC 관계 위반)
psql "$DATABASE_URL" -c "
SELECT * FROM bars_1m
WHERE low > open OR low > close OR high < open OR high < close
LIMIT 10;"
```

OHLC 위반이 나오면 집계 로직 버그입니다 — 즉시 에스컬레이션하세요.

### 품질 경보를 읽는 법 (2026-08-31 갱신)

```bash
curl -s localhost:9106/healthz | python3 -m json.tool | head -20
```

| 항목 | 뜻 | 어떻게 볼 것인가 |
|---|---|---|
| `price_jump:CRITICAL` | 한 틱에 임계(10%) 이상 이동 | **문구에 간격이 적혀 있습니다.** `(3,185 → 3,550, 82.6초 간격)` — 82초에 11%면 소형주에서 실제로 일어납니다. 간격이 짧을수록 데이터 오류 쪽입니다 |
| `price_jump:WARNING` + `초 만의 첫 틱` | 기준가가 낡은 상태에서의 큰 이동 | 수집이 끊겼다 돌아온 직후입니다. 데이터 문제가 아니라 **연결 문제**를 보세요 |
| `stale_value:WARNING` | 상류가 **같은 레코드**를 반복 | 가격만 같은 게 아니라 **체결시각까지 같습니다.** 진짜 상류 고장 신호입니다 |
| `price_ref_resets` | 기준가를 버린 횟수 (거래소별) | **바닥값이 거래소마다 다릅니다.** 실측(연속 체결 20만건, 간격 300초 초과 비율): KRX 4.95% · UPBIT 0.45% · BINANCE 0.09% · KIS 0.00%. KRX 는 비유동 종목의 정상 공백이 대부분이라 늘 높습니다. **합계가 아니라 거래소별 급증**을 보세요 — BINANCE 가 오르면 수집 중단을 의심할 만하지만 KRX 가 오르는 건 평시입니다 |

> `stale_value` 는 예전에 "같은 가격이 반복"으로 판정해 조용한 시장을 상류 고장이라
> 불렀습니다. 실측 400,000건에서 117건이 전부 오탐이었습니다. 지금은 체결시각까지
> 같아야 울립니다 — **울리면 진짜입니다.**

품질 검사 자체를 의심할 때는 실데이터 코퍼스로 회귀를 돌립니다.

```bash
.venv/bin/python -m pytest tests/test_quality_corpus.py -q
```

오탐(정상 데이터에 만건당 3건 이하)과 미탐(결함을 넣으면 잡는가)을 양방향으로 봅니다.

### 프레이밍 오류

```bash
python3 -m mdfeed.cli client --duration 30 --quiet | grep -E 'CRC오류|재동기화'
```

`CRC오류` 가 0이 아니면 네트워크 오염이거나 프레이밍 버그입니다.
CRC가 잡아주므로 **틀린 값이 배포되지는 않지만**, 계속 나면 원인을 찾아야 합니다.

---

## 증상 9 — 버스 드롭이 늘어난다

```bash
curl -s localhost:9100/healthz | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("합계", d["bus_dropped"])
for s in d["bus_subscribers"]:
    print("  %-14s 큐 %5d/%d 드롭 %8d" % (
        s["name"], s["backlog"], s["queue_size"], s["dropped"]))'
```

**드롭 많은 순으로 나옵니다.** 합계만 보면 다섯 구독자 중 누가 느린지 알 수 없습니다
(그래서 이름을 붙였습니다). 지목된 구독자를 먼저 보세요.

| 구독자 | 느릴 때 흔한 원인 |
|---|---|
| `writer` | 디스크 I/O. `증상 4` 로 |
| `tcp-gateway` | 구독자가 많거나 느림. `:9111/subscribers` 의 `wire_bytes` |
| `quality` | 검사 비용. 종목 수를 늘린 직후인지 확인 |

게이트웨이 재기동 직후에는 **한 번에 크게 튄 뒤 멈춥니다.** 그건 정상입니다 —
구독이 끊긴 동안 발행분이 버려진 것이고, 값이 더 안 오르면 진행 중인 문제가 아닙니다.

---

## 증상 10 — 조회 API 가 느리다

```bash
for u in /api/v1/stats /api/v1/quotes /api/v1/symbols; do
  /usr/bin/time -p curl -s -o /dev/null "localhost:9103$u" 2>&1 | awk -v u=$u '/real/{print $2"s", u}'
done
```

**순차로만 재면 안 됩니다.** 무거운 조회가 도는 동안 가벼운 조회를 같이 날려 보세요.

```bash
curl -s -o /dev/null "localhost:9103/api/v1/trades?venue=UPBIT&symbol=KRW-BTC&limit=2000" &
/usr/bin/time -p curl -s -o /dev/null "localhost:9103/api/v1/quotes?symbol=KRW-BTC"
```

동시에 냈을 때만 밀린다면 **직렬화 지점**이 생긴 것입니다. 예전에 조회 커넥션 하나를
락으로 감싸서, 5.4초짜리 통계 조회가 0.01초짜리 시세 조회를 4.44초로 밀었습니다
(444배). 지금은 스레드마다 조회 커넥션을 씁니다.

`/api/v1/stats` 의 `counts_took_ms` 와 `counts_age_s` 를 보세요. 집계는 요청 경로가
아니라 백그라운드에서 돌고, 재는 주기는 `max(STATS_TTL_S, 소요×10)` 입니다.
`counts_took_ms` 가 계속 커지면 테이블이 커진 것이니 **보존 정책을 확인하세요.**

> [!warning] 실측 (2026-09-01) — 이 값이 어떻게 커지는가
> ```
> 08-31 오전   584만 행   COUNT(*)   5.0초
> 09-01 오전  3,373만 행  COUNT(*) 112.9초    ← 하루 만에 6배
> DB 5.47GB · 여유 35.9GB
> ```
> 자기 제한 주기 덕에 요청은 여전히 즉시 응답하고 집계는 19분에 한 번만 돕니다.
> **막힌 건 없지만 방향이 잘못돼 있습니다.**
>
> 원인은 `MDFEED_RETENTION_DAYS=0`(무제한 보존)입니다. 기본값이 0인 이유는
> 구현을 붙이는 순간 기존 배포에서 데이터가 조용히 지워지지 않게 하려던 것이고,
> **켜는 건 명시적 결정**입니다.
> ```bash
> curl -s localhost:9104/healthz | python3 -c \
>   'import json,sys; print(json.load(sys.stdin)["storage"])'
> # retention_days 0.0 → 안 지웁니다. hours_until_full 이 None 이면 증가율 표본이
> # 아직 부족한 것이니 최소 1시간 뒤 다시 보세요.
> ```
> 켤 때는 값을 정하고 한 번에 지우지 않게 배치 크기를 확인하세요
> (`DELETE_BATCH`, 기본 50,000행).

> [!danger] 위 문장은 틀렸습니다 (2026-09-03 정정)
> 배치 크기를 확인해도 소용없었습니다. `DELETE_BATCH` 로 끊는 코드는 있었지만
> writer 가 **락을 루프 바깥에서** 잡고 있었습니다. 배치로 끊는 목적이
> "락을 오래 잡지 않으려고"라고 주석에 적혀 있었는데, 실제로는 첫 삭제가
> 다 끝날 때까지 적재가 통째로 멈춥니다. 버스는 drop-oldest 라 **그 시간만큼
> 틱이 버려집니다 — 보존을 켜는 행위 자체가 데이터 손실이었습니다.**
>
> 실측: 삭제 50,000행 배치 하나에 **0.63초**(300만 행·인덱스 2개 기준).
> 보존 3일이면 첫 실행이 557배치 = **5.9분간 적재 정지**.
>
> 지금은 락을 배치마다 잡았다 놓고(`guard`), 한 번에 도는 시간에 상한을
> 둡니다(`RETENTION_BUDGET_S`, 기본 30초). 남은 건 다음 주기가 이어서
> 지우고, 밀린 동안은 1시간이 아니라 60초 뒤에 다시 옵니다.
> 첫 삭제는 5.9분 정지 대신 약 12분에 걸쳐 나뉩니다.
>
> **A/B 로 확인했습니다** (`make bench-retention`, 50만 행·12배치·순서 뒤집어 2회):
> ```
> 락 범위            삭제 소요   적재 최대 공백   공백/소요
> 옛 판(루프 바깥)     4.66s       4.71s          101%   ← 삭제 전체에 묶임
> 옛 판(2번째)        2.03s       2.07s          102%
> 새 판(배치마다)      1.93s       0.29s           15%   ← 배치 하나에 묶임
> 새 판(2번째)        2.35s       0.57s           24%
> ```
> **배수는 쓰지 않습니다.** 기계 부하에 따라 22배에서 1배까지 흔들립니다.
> 흔들리지 않는 건 비율입니다 — 옛 판의 정지는 **삭제 총량**에 비례하고,
> 새 판은 **배치 하나**에 묶입니다. 데이터가 늘수록 격차가 벌어집니다.

### 보존을 켜는 절차

**1. 지우기 전에 무엇이 지워질지 봅니다.** 되돌릴 수 없는 결정입니다.

```bash
python3 -m mdfeed.cli retention --days 1 3 7 14     # 아무것도 안 지웁니다
```

실측 (2026-09-03, trades 7,444만 행 · 6.1일치 · DB 13.3GB):

| 보존일 | 지울 행 | 남길 행 | 배치 | 첫 삭제 총 작업시간 |
|---:|---:|---:|---:|---:|
| 1일 | 52,253,807 | 22,135,281 | 1,046 | 11.0분 |
| 2일 | 37,193,444 | 37,229,992 | 744 | 7.8분 |
| 3일 | 27,800,912 | 46,651,158 | 557 | 5.9분 |
| 5일 | 19,953,442 | 54,527,029 | 400 | 4.2분 |

맨 오른쪽은 **총 작업시간**이지 정지 시간이 아닙니다. 예산 30초씩 나뉘어
60초 주기로 도니, 3일 기준 벽시계로는 약 12분에 걸쳐 끝납니다.

**2. 켭니다.**

```bash
MDFEED_RETENTION_DAYS=3 make up-bg-shards
```

**3. 파일이 안 줄어드는 걸 사고로 오해하지 마세요.**

이 DB 는 `auto_vacuum=0` 입니다. SQLite 는 DELETE 한 페이지를 freelist 에
넣고 파일 크기는 그대로 둡니다. 다음 적재가 그 자리를 재사용하므로
**증가는 멈추지만 `db_bytes` 는 안 줄어듭니다.** 그게 정상입니다.

```bash
curl -s localhost:9104/healthz | python3 -c \
  'import json,sys; s=json.load(sys.stdin)["storage"]; \
   print(f"파일 {s[\"db_bytes\"]/1e9:.2f}GB · 빈 자리 {s[\"reclaimable_bytes\"]/1e9:.2f}GB")'
```

`reclaimable_bytes` 가 0 이 아니면 삭제가 실제로 돌고 있다는 뜻입니다.
파일을 진짜로 줄이려면 `VACUUM` 이 필요한데, 13GB 를 통째로 다시 쓰는
작업이라 적재를 세워야 합니다. 증가만 멈추면 되는 상황에선 하지 마세요.

**4. 삭제가 유입을 못 따라가면 알람이 뜹니다.**

`RetentionFallingBehind` — 2시간째 한 번도 다 못 끝냈다는 뜻입니다.
`prune_incomplete` 가 계속 `true` 면 보존 일수를 더 줄이거나
`RETENTION_BUDGET_S` 를 올리세요.

실측 여유: 예산 30초 = 주기당 약 47배치. 정상 주기(1시간)로 하루 24주기,
1,128배치 = 5,640만 행을 지울 수 있습니다. 현재 유입은 하루 약 1,200만 행
(240배치)이니 **4.7배 여유**입니다. 밀린 동안엔 60초 주기로 도니 여유는
더 큽니다 — 예산이 병목이 되는 건 유입이 20배쯤 늘었을 때입니다.

---

## 증상 11 — 대시보드가 낡은 값을 보여준다

WS 경로에는 시퀀스가 없습니다. 서버가 배치를 버리면 그 안에 있던 종목은
**다음 체결이 올 때까지** 낡은 값이 떠 있습니다.

지금은 버리면 다음 주기에 전체 스냅샷으로 되맞추고, 화면에
`갱신 N건 놓쳐 전체 재동기화` 가 뜹니다. 그 메시지가 자주 보이면:

```bash
curl -s localhost:9102/metrics | grep -E 'dropped_total|resyncs_total'
curl -s localhost:9102/api/clients | python3 -m json.tool | head -20
```

`resyncs` 가 특정 클라이언트에만 몰리면 그 브라우저/네트워크 문제입니다.
전체에 걸쳐 오르면 서버가 못 따라가는 것이니 `증상 9` 로 갑니다.

---

## 증상 12 — 경보가 떴는데 실제로는 아니다

경보를 끄기 전에 **그 지표가 무엇을 나누고 있는지** 확인하세요.

| 경보 | 오경보로 보인다면 |
|---|---|
| `ReconnectStorm` | 운영 기록의 시간당 환산은 **그 어댑터를 담은 프로세스**의 가동시간으로 나눕니다. 게이트웨이만 재기동했는데 수집기가 폭주로 찍힌다면 회귀입니다(실측: 2.6회/시간이 70회/시간으로 보고됐던 적 있음). 가동 10분 미만이면 판정 자체를 안 합니다 |
| `DataQualityCritical` | `증상 8` 의 표로 문구를 읽으세요. 간격이 길면 복구 직후일 수 있습니다 |
| `UpstreamStale` | 휴장 중이면 `expects_data: false` 이고 `stale` 은 false 여야 정상입니다. true 라면 장 시간 판정이 잘못된 것 |

**끄지 말고 고치세요.** 없는 사건으로 경보를 내면, 진짜일 때 아무도 안 봅니다.

---

## 정기 점검

### 매일

```bash
./ops/ops.sh diag                    # 전반
df -h /var/lib/mdfeed                # 디스크
journalctl -u 'mdfeed-*' --since yesterday -p warning

# 저장소 증가 — 보존을 껐다면 이걸 매일 봐야 합니다
curl -s localhost:9104/healthz | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["storage"])'

# 조용히 새는 것들 — 값이 오르면 원인을 찾습니다
curl -s localhost:9100/metrics | grep -E \
  'session_cancel_timeouts_total|adapter_task_deaths_total|symbol_truncated_kinds'
```

### 매주

```sql
-- 수집 품질: 분당 틱이 0으로 떨어진 구간 = 피드 끊김
SELECT venue, symbol, bucket, tick_count
FROM v_feed_gaps
WHERE tick_count = 0 AND bucket > now() - interval '7 days';

-- 스프레드 추이 (유동성 이상 탐지)
SELECT * FROM v_spread_hourly
WHERE hour > now() - interval '7 days' AND avg_spread_bp > 50
ORDER BY avg_spread_bp DESC LIMIT 20;
```

### 배포 전

```bash
make ci                              # lint + 111개 테스트
make bench                           # 성능 회귀 확인
MDFEED_ADAPTERS=replay make demo     # 오프라인 전 구간 재현
```

---

## 에스컬레이션 기준

| 상황 | 조치 |
|---|---|
| `feedd` 가 5분 이상 복구 안 됨 | **즉시 에스컬레이션.** 그 구간 데이터는 영구 유실 |
| OHLC 관계 위반이 DB에 있음 | **즉시.** 집계 로직 버그 = 하류 전부 오염 |
| 시퀀스 유실률 1% 초과가 지속 | 즉시. 배포단 설계 문제 가능성 |
| 워치독이 시간당 재시작 한도 도달 | 즉시. 재시작으로 못 고치는 문제 |
| 시계 오프셋 100ms 초과 | 당일 중. 서비스는 동작하나 지표 신뢰도 저하 |
| 특정 구독자만 드롭 | 구독자 측에 통보. 서비스 문제 아님 |
| 거래소 점검 | 기록만. 복구 대기 |

---

## 백업과 복구

```bash
# 틱 + 봉 백업 (Postgres)
pg_dump "$DATABASE_URL" -t trades -t bars_1m -Fc -f /backup/mdfeed_$(date +%F).dump

# 봉만 (용량이 수백 배 작음. 대부분 이걸로 충분)
pg_dump "$DATABASE_URL" -t bars_1m -Fc -f /backup/bars_$(date +%F).dump

# 복구
pg_restore -d "$DATABASE_URL" --clean /backup/mdfeed_2026-08-27.dump
```

**재수집이 불가능하다는 점을 기억하세요.** 거래소 실시간 피드는 과거를 다시 주지 않습니다.
틱을 잃으면 영구히 잃습니다. 그래서 `writer` 장애가 `rest-api` 장애보다 훨씬 심각합니다.

장애 구간을 재현해 분석해야 한다면 녹화 파일을 씁니다:

```bash
MDFEED_REPLAY_FILE=data/replay/incident_2026-08-27.mdf \
MDFEED_ADAPTERS=replay MDFEED_REPLAY_SPEED=1.0 make demo
```
