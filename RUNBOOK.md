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

### 프레이밍 오류

```bash
python3 -m mdfeed.cli client --duration 30 --quiet | grep -E 'CRC오류|재동기화'
```

`CRC오류` 가 0이 아니면 네트워크 오염이거나 프레이밍 버그입니다.
CRC가 잡아주므로 **틀린 값이 배포되지는 않지만**, 계속 나면 원인을 찾아야 합니다.

---

## 정기 점검

### 매일

```bash
./ops/ops.sh diag                    # 전반
df -h /var/lib/mdfeed                # 디스크
journalctl -u 'mdfeed-*' --since yesterday -p warning
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
make ci                              # lint + 109개 테스트
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
