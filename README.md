# MDFeed — 실시간 마켓데이터 FEED 플랫폼

[![CI](https://github.com/oyeong011/market-feed-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/oyeong011/market-feed-platform/actions/workflows/ci.yml)
[![Pages](https://github.com/oyeong011/market-feed-platform/actions/workflows/pages.yml/badge.svg)](https://oyeong011.github.io/market-feed-platform/)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![deps](https://img.shields.io/badge/핵심%20의존성-0-brightgreen)
![tests](https://img.shields.io/badge/tests-142%20passing-brightgreen)
![venues](https://img.shields.io/badge/거래소-3곳%20실연결-blue)

거래소 실시간 시세를 **수집 → 정규화 → 멀티프로토콜 배포**하는 마켓데이터 피드 서비스와
그것을 리눅스에서 운영하기 위한 자동화 스택.

**거래소 3곳 실연결 검증** — 업비트·바이낸스(24시간), 한국투자증권(국내 정규장).
국내 주식은 **코스피 1,783종목**을 계층형 피드로 덮습니다. 공개 대시보드에서 종목명으로 검색할 수 있습니다.

**▶ 데모 대시보드: https://oyeong011.github.io/market-feed-platform/**

```
거래소 WebSocket ──▶ feedd ──▶ UDS 버스 ──┬──▶ tcp-gateway  :9101  MDFP/1 바이너리
 (Upbit·Binance·KIS)  정규화·seq        ├──▶ ws-gateway   :9102  WebSocket + 대시보드
                      시계보정          ├──▶ writer              1분봉 · PostgreSQL/SQLite
                            └─▶ 공유메모리 링    ├──▶ strategy            지표 → SIGNAL ──┐
                                                 └──▶ rest-api    :9103  과거 조회        │
                                                        ▲                                 │
                                                        └──────── signals.sock ───────────┘
```

---

## 핵심 제약: 외부 의존성 0

수집·배포·적재의 **핵심 경로는 Python 표준 라이브러리만** 씁니다. 다음을 직접 구현했습니다.

| 구현물 | 무엇을 다루는가 |
|---|---|
| **WebSocket 클라이언트** (RFC 6455) | HTTP Upgrade 핸드셰이크, `Sec-WebSocket-Accept` 계산, 마스킹, 확장 길이(7/16/64비트), 단편화 재조립, ping/pong (평문 `ws://`·TLS `wss://` 모두) |
| **WebSocket 서버** | 핸드셰이크 응답, 서버측 비마스킹 규칙, 제어 프레임 |
| **HTTP/1.1 서버** | keep-alive, Content-Length(바이트 정확도), HEAD, 헤더 크기 상한, 경로 탈출 차단, 라우팅 |
| **MDFP/1 TCP 프로토콜** | 길이 프리픽스 프레이밍, CRC32 무결성, 시퀀스 갭 탐지, 재동기화, 스냅샷+증분 |
| **공유메모리 링버퍼** | SPSC 브로드캐스트, 논블로킹 생산자, 추월(lap) 감지, 찢긴 읽기 방지 |
| **UDS pub/sub 버스** | 프로세스 간 팬아웃, 구독자별 백프레셔, 지수 백오프 재접속 |

**왜 그렇게 했나.** 이 프로젝트가 증명하려는 것이 "네트워크 프로토콜에 대한 이해"인데,
라이브러리를 부르면 그 이해가 코드에 남지 않습니다. 부수 효과로 `python3` 만 있으면
어떤 리눅스 박스에서도 `git clone && make demo` 로 전 구간이 돕니다.

선택 의존성은 있습니다 — PostgreSQL(`psycopg2`), ZeroMQ 버스(`pyzmq`),
백테스트 교차검증(`pandas`·`backtesting`·`vectorbt`). **없으면 각각 SQLite·UDS·자체 엔진으로
자동 폴백**하며 서비스는 그대로 동작합니다.

---

## 30초 만에 돌려보기

```bash
git clone https://github.com/oyeong011/market-feed-platform
cd market-feed-platform

make demo        # 6개 프로세스 기동 + 대시보드 안내
                 # → http://localhost:9102/ 에서 실시간 시세

make status      # 서비스 상태 (프로세스 + HTTP 헬스 + 포트)
make client      # 참조 TCP 구독 클라이언트 (갭 탐지 포함)
make diag        # 장애 진단 원스톱
make test        # 142개 테스트 — 네트워크 불필요
```

인터넷이 없어도 됩니다. 저장소에 든 녹화 파일로 전 구간을 재현합니다:

```bash
MDFEED_ADAPTERS=replay make demo
```

---

## 국내 주식을 넓게 덮는 방법 — 계층형 피드

KIS 실시간 웹소켓은 **계정당 등록 한도**가 있습니다. 이 계정은 실측 결과 **3종목**이었습니다
(새 연결에서 한 종목씩 등록해도 4번째부터 `MAX SUBSCRIBE OVER`. 문서 기준값 41과 다르고
계정마다 다릅니다). 코스피 전체를 틱 단위로 받으려면 거래소와의 정식 시세 계약이 필요합니다.

그래서 실제 마켓데이터 시스템이 쓰는 **계층형 피드**로 우회했습니다.

| 계층 | 방식 | 종목 수 | 갱신 | 데이터 성격 |
|---|---|---:|---|---|
| 1 · 틱 | WebSocket `H0STCNT0` | 3 | 밀리초 | **실제 체결** (가격·수량·방향·호가잔량) |
| 2 · 활성 | 순위 API (30종목/요청) | ~60 | 10초 | 스냅샷 |
| 3 · 전체 | 유니버스 라운드로빈 | **1,783** | 약 15분 회전 | 스냅샷 |

2계층이 효율의 핵심입니다. 순위 API는 **한 번 호출에 30종목**을 주므로 같은 요청 예산으로
30배 넓게 덮고, 거래가 활발한 종목이 자동으로 상위에 올라 "지금 움직이는 종목"이
우선 갱신됩니다.

**스냅샷을 체결로 위조하지 않습니다.** REST 응답은 체결이 아니라 그 순간의 상태입니다.
직전 폴링 대비 **누적거래량이 늘어난 만큼만** 합성 체결로 발행하고, venue를 `KRX`로 따로 둬
웹소켓에서 온 진짜 체결(`KIS`)과 섞이지 않게 했습니다. 지연 지표에서도 제외합니다 —
폴링 주기가 곧 지연이라 네트워크 지연을 재는 의미가 없습니다.

**유량 제한은 스스로 찾아갑니다.** 실측 한도는 초당 5건이었고 4건에서도 실패가 났습니다.
고정 3건/초로 두니 60초에 15번 거절당했습니다. TCP 혼잡제어와 같은 방식(AIMD)으로
바꿔 — 거절하면 간격 1.15배, 8회 연속 성공하면 0.9배 — **2.0 req/s로 수렴**했습니다.
거절 한 번의 비용은 요청 슬롯 하나뿐이라, 거절을 피하는 것보다 성공 요청 수를
최대화하는 쪽이 맞습니다.

```bash
python scripts/fetch_krx_symbols.py      # 종목 마스터 갱신 (KOSPI 1,783 + KOSDAQ 1,771)
MDFEED_ADAPTERS=upbit,binance,kis,kis_rest make up
```

## 자격증명

한국투자증권 어댑터는 앱키가 필요합니다. **저장소에 넣지 마세요.**

```bash
cat > ~/.mdfeed/kis.env <<'EOF'
KIS_APP_KEY="..."
KIS_APP_SECRET="..."
KIS_ENV="real"          # real 실전 / vts 모의
EOF
chmod 600 ~/.mdfeed/kis.env

MDFEED_ENV_FILE=~/.mdfeed/kis.env MDFEED_ADAPTERS=upbit,binance,kis make up
```

`MDFEED_ENV_FILE` 은 docker 의 `--env-file`, systemd 의 `EnvironmentFile=` 과 같은 역할입니다.
이미 설정된 환경변수는 덮어쓰지 않고, 값은 어디에도 로그로 남기지 않습니다.
**키가 없으면 KIS 어댑터만 스스로 비활성화되고 나머지는 정상 기동합니다.**

## 실측 성능

`bench/latency_bench.py` 가 만든 수치입니다. 대시보드와 CI가 이 JSON을 그대로 읽으므로
**문서와 실제가 갈라지지 않습니다**.

| 계층 | 처리량 | 지연 p50 | 지연 p99 |
|---|---:|---:|---:|
| MDFP 인코딩 | 2,305,570 msg/s | 434 ns | – |
| MDFP 파싱 (재동기화 포함) | 759,786 msg/s | 1,316 ns | – |
| 공유메모리 링버퍼 push | 1,560,551 msg/s | 641 ns | – |
| **UDS 버스 (순수 IPC 지연)** | – | **47.3 µs** | **105.9 µs** |
| UDS 버스 (처리량 상한) | 97,562 msg/s | – | – |
| HTTP/1.1 서버 (keep-alive) | 15,237 req/s | 62.2 µs | 95.5 µs |
| **전체 파이프라인** | **49,235 msg/s** | – | – |

<sub>macOS 26.5 · Apple Silicon 10코어 · Python 3.14. 절대 성능이 아니라 **계층별 상대 비용**과
이 구조가 견디는 규모를 보기 위한 수치입니다. 프레임 88B = 페이로드 64B + 헤더 20B + CRC 4B (오버헤드 37.5%).</sub>

> **벤치마크를 한 번 틀렸다가 고쳤습니다.** 처음엔 최대 속도로 밀어 넣고 지연을 쟀더니
> p50이 105ms로 나왔습니다. 그건 IPC 지연이 아니라 **큐에 쌓여 기다린 시간**이었습니다.
> 지연은 큐를 비운 상태(페이싱)에서, 처리량은 버스트로 따로 재도록 분리했습니다.
> 벤치마크가 조용히 다른 걸 재고 있는 건 없는 것보다 나쁩니다.

---

## 개발 중 실제로 잡아낸 결함

전부 **참조 클라이언트와 통합 테스트가 잡아낸** 것이고, 원인과 조치를 코드 주석에 남겼습니다.

| # | 증상 | 진짜 원인 | 조치 |
|---|---|---|---|
| 1 | 구독자 데이터 무결성 **48%** | 게이트웨이가 심볼 필터링을 하는데 seq는 전역 번호를 그대로 씀 → 걸러진 번호가 구독자에겐 전부 유실로 보임. **프로토콜 설계 결함** | 구독자별 시퀀스 재넘버링 → **100.0000%** |
| 2 | DB 지연시간이 **−30,825 µs** | 거래소 시계가 로컬보다 앞섬. 음수 지연이 든 테이블은 어떤 SQL 집계를 해도 결론이 틀림 | NTP 방식 **최소값 필터**로 venue별 시계 오프셋 추정·보정. 원시값도 함께 보관 |
| 3 | SIGTERM 후 프로세스가 안 내려감 | Python 3.12+ `Server.wait_closed()` 가 핸들러 종료를 기다리는데, 핸들러는 큐에서 영원히 대기 | 리스너를 닫기 전에 연결 핸들러부터 취소 (bus·httpd·tcp-gateway 3곳) |
| 4 | 종료 중 **세그폴트** | `asyncio.to_thread` 는 await 를 취소해도 스레드가 안 멈춤. 그 스레드가 `executemany` 중일 때 DB 커넥션을 닫아 use-after-free | DB 접근 경로 전체를 단일 락으로 직렬화 |
| 5 | CRC 오류 후 세션 전체 정지 | 재동기화 직후 파서가 남은 버퍼를 더 안 읽고 반환 → 한 번 깨지면 영구히 멎음 | 재동기화 후 루프 계속 → 오염 프레임 **1개만** 폐기하고 나머지 복원 |
| 6 | UDS 소켓 연결 실패가 재시도 루프에 묻힘 | `sockaddr_un.sun_path` 104바이트 제한 초과 | 기동 시점에 검사해 조치 방법과 함께 즉시 실패 |
| 7 | CI에서 "시계 오프셋 **2,343,288 ms**" 거짓 알람 | 리플레이 데이터의 "녹화 시각 − 현재 시각"을 지연으로 계산. 그건 시계 오차도 네트워크 지연도 아니라 **언제 녹화했는가**일 뿐 | 어댑터에 `measures_latency` 플래그 추가. 실시간 배속 재생 시에는 체결 시각을 현재 기준으로 평행이동 |
| 8 | `make demo` 안내대로 열면 대시보드 대신 JSON | `health_routes` 의 `GET /` 가 정적 파일보다 우선 | 루트에 정적 마운트하면 그쪽이 이기도록. `/healthz`·`/metrics` 는 유지 |
| 9 | **국내 주식 매도 체결이 전부 방향 미상** | KIS 문서는 체결구분을 `1 매수 / 3 매도 / 5 장전` 으로 적었는데, 실계좌 300건을 체결가와 호가로 대조하니 **매도는 `5`** 로 왔다. 문서대로면 매도가 전량 UNKNOWN → 주문흐름 지표가 통째로 무의미 | 관측값 기준으로 매핑 정정. 미매핑 코드는 버리지 않고 세어 `/healthz` 에 노출 |
| 10 | 종목 5개를 요청했는데 3개만 데이터가 옴 | 소켓당 구독 한도가 **계좌마다 다르다**. 문서 기준값 41을 믿고 요청했는데 4번째부터 `MAX SUBSCRIBE OVER` | 거절을 만나면 그 시점 성공 개수를 **실효 한도로 학습**해 다음 접속부터 그만큼만 요청 |
| 11 | KIS 세션이 몇 분 뒤 끊김 | KIS 는 `PINGPONG` 을 텍스트로 보내고 **WebSocket PONG 제어 프레임**으로 답하기를 기대한다. JSON 이라는 이유로 버리면 세션이 죽고 원인이 로그에 안 남는다 | 공식 예제와 동일하게 PONG 프레임으로 응답 (`WSClient.pong()` 추가) |
| 12 | KIS 수집 지연이 항상 0 | 체결시각이 `HHMMSS` 뿐이라 날짜가 없다. 수신시각을 그대로 event 시각에 넣어 지연이 0으로 고정 → 전체 p50/p99 를 낮춰 왜곡 | 오늘 KST 날짜를 붙여 합성하고, 현재와 10분 넘게 벌어지면 믿지 않고 대체 후 카운트 |
| 13 | 고정 유량 제한이 요청의 10%를 버림 | 초당 3건으로 고정했더니 60초에 15번 `EGW00201`. 서버 한도는 문서값과 다르고 시간대에 따라서도 달라진다 | AIMD 적응형 제한기 — 거절 시 감속, 연속 성공 시 회복. 2.0 req/s 수렴, 거절 대비 성공 요청 수 최대화 |
| 14 | **`DATABASE_URL` 을 줬는데 조용히 SQLite 로 동작** | 컨테이너 이미지에 psycopg2 가 없었다. 폴백 자체는 의도한 동작이지만 **설정 오류와 DB 장애를 구분하지 않아** 한참 뒤에야 발견했다 | 이미지에 드라이버 설치. 드라이버 없음은 `ImportError` 로 따로 잡아 조치 방법과 함께 error 로그 |
| 15 | 컨테이너 5개가 영구 `unhealthy` | Dockerfile `HEALTHCHECK` 이 포트 9100 고정. tcp-gateway 컨테이너엔 9100 이 없어 영원히 실패 | `MDFEED_HEALTH_PORT` 로 서비스마다 지정, compose 에서 주입 |
| 16 | **feedd 재시작이 소비자 전체를 끌고 내려감** | systemd `Requires=` 는 재시작을 전파한다. 버스 구독자에 지수 백오프 자동 재접속을 넣어둔 이유가 무력화되고, 소비자의 배치 버퍼도 함께 날아간다 | `Wants=` + `After=` 로 변경 — 순서만 보장하고 재시작은 전파하지 않는다. 실측: feedd PID 427→460, 소비자 3개 PID 유지 |
| 17 | Postgres 조회 시각이 UTC | 국내 장 시간 09:00~15:30 이 00:00~06:30 으로 보인다. 장 시작 전인지 마감 후인지 눈으로 판단 불가 | 스키마에서 DB 기본 시간대를 `Asia/Seoul` 로 설정 (저장 값은 그대로, 표시만) |

---

## 채용 요구사항 ↔ 구현 매핑

| 요구사항 | 이 저장소의 어디에 |
|---|---|
| 금융 데이터 FEED 서비스 개발·운영 | 전체. `src/mdfeed/services/` 6개 서비스, 정규화 스키마 `models.py`, 배포 프로토콜 3종 |
| Linux 서비스·프로세스 점검 및 안정화 | `ops/systemd/` 유닛 6종 + target, `ops/ops.sh` (status/ports/top/conns/diag), `ops/watchdog.sh`, `ops/healthcheck.py` |
| 데이터 파이프라인·배포·점검 자동화 | `Makefile`, `Dockerfile`, `docker-compose.yml`, GitHub Actions CI(테스트·스모크·벤치·이미지) + Pages 자동 배포 |
| Python | 전량. 표준 라이브러리 중심 |
| SQL 기초 / 관계형 DB 이해 | `storage/schema.sql` (PostgreSQL + TimescaleDB 하이퍼테이블·보존정책·복합인덱스·뷰 4종), `schema_sqlite.sql`, upsert 병합 로직 |
| Linux 명령·프로세스·로그 확인 | `RUNBOOK.md` 장애 시나리오 8종, `ops.sh logs`(journalctl), `logrotate.conf`, JSON 구조화 로깅 |
| **프로세스 간 통신** | UDS pub/sub 버스, 공유메모리 SPSC 링버퍼, 다단 파이프라인(strategy가 소비자이자 발행자) |
| **네트워크 프로토콜 (TCP/HTTP)** | TCP 바이너리 프로토콜 직접 설계·구현, HTTP/1.1 서버 직접 구현, WebSocket 클라이언트·서버 직접 구현 |
| 자료구조·운영체제·네트워크 | 링버퍼·해시인덱스·로그버킷 히스토그램 / 공유메모리·시그널·프로세스 감독·파일디스크립터 / 프레이밍·백프레셔·TCP_NODELAY·half-open 탐지 |
| *(우대)* 파이프라인·인프라·자동화 경험 | 위와 동일 |
| *(우대)* Feed 기반 시세·마켓데이터 서비스 경험 | 이 프로젝트 자체. 스냅샷+증분, 시퀀스 갭 복구, 구독 필터, 느린 구독자 격리 |

---

## 데이터 평면 두 개

```
실시간 평면  거래소 WS ─▶ feedd ─▶ 1분봉        밀리초 단위 · 휘발성
참조 평면    SEC EDGAR / OpenDART ─▶ 재무제표    분기 단위 · 영속 (487,434건)
                                    └─ oyeong011/financial-database
```

조인 키는 **`DART stock_code` = `KIS 종목코드`** 입니다. 삼성전자는 참조 평면에서 `005930`,
실시간 평면에서 `KIS:005930` 입니다. 크립토 심볼은 대응하는 재무제표가 없으므로
**억지로 붙이지 않습니다**.

```bash
make factor    # DART 재무 팩터 스크리닝 (영업이익률·ROE·매출성장·FCF·부채비율)
```

결측을 0으로 채우지 않습니다. 채우는 순간 "부채가 없는 회사"와 "부채 데이터가 없는 회사"가
같아져 스크리닝 결과가 조용히 오염됩니다.

---

## 퀀트: 실시간과 백테스트가 같은 코드

`src/mdfeed/strategies.py` 의 전략 클래스를 **실시간 엔진과 백테스트가 공유**합니다.
백테스트만 따로 구현하면 두 코드가 미묘하게 갈리고, 그 차이는 실거래에서만 드러납니다.

정직하게 만들려고 넣은 장치:

- **미래 참조 차단** — `t` 봉 종가로 판단하고 **`t+1` 봉 시가**로 체결합니다.
  같은 봉 종가로 체결하면 "종가를 보고 종가에 산" 셈이 되어 성과가 부풀려집니다.
- **수수료 + 슬리피지** — 편도 0.05% + 5bp를 반영합니다.
- **오픈소스 교차검증** — `backtesting.py` / `vectorbt` / `pandas` 로 같은 데이터를 다시 돌려 대조합니다.

교차검증 실측:

```
SMA(30)   우리 구현 vs pandas rolling   최대 절대오차 7.28e-11   (부동소수 오차 수준)
RSI(14)   우리 구현 vs Wilder 공식 예제  소수 둘째 자리까지 일치   (tests/test_indicators.py)
백테스트  우리 엔진 vs backtesting.py    차이 0.012 %p           (동일 가정으로 맞춘 뒤)
```

> 처음 비교했을 땐 0.21%p가 벌어졌습니다. 우리는 슬리피지 5bp를 넣었고 상대는 수수료만
> 반영했기 때문이었습니다. **가정을 맞추자 0.012%p로 줄었습니다.** 사과 대 사과로 맞추기 전의
> 비교는 아무것도 검증하지 못합니다.

```bash
make backtest    # → docs/data/backtest.json (대시보드가 읽음)
```

**백테스트 결과에 대한 정직한 경계**: 저장소의 샘플은 20~30분 구간 단일 종목입니다.
통계적 유의성이 없습니다. 이 수치는 **파이프라인이 끝까지 동작한다는 증거**이지
전략의 우열이 아닙니다.

---

## 운영

### 실제 리눅스 systemd 에서 검증했습니다

유닛 파일을 "썼다"와 "돌려봤다"는 다릅니다. 개발은 macOS 에서 했으므로
systemd 가 실제로 도는 Ubuntu 22.04 컨테이너를 만들어 확인했습니다.

```bash
make systemd-test        # systemd 테스트베드에 설치·기동·검증
```

확인한 것:

| 항목 | 결과 |
|---|---|
| 6개 서비스 + target 기동 | 전부 `active (running)`, `User=mdfeed` |
| 보안 강화 | `ProtectSystem=strict` · `ProtectHome=yes` · `NoNewPrivileges=yes` · `LimitNOFILE=65536` 적용 확인 |
| 구조화 로그 | JSON 한 줄씩 journald 로 수집 |
| **SIGKILL 후 재시작** | feedd PID 133 → 253, `Restart=on-failure` 동작 |
| **소비자 생존** | feedd 재시작 시 소비자 3개 PID 유지, 백오프 1s→2s→4s 후 자동 재접속 |
| **우아한 종료** | `systemctl stop` 즉시 완료, 종료 flush 로 잔여 행 저장, 프로세스 잔류 없음 |

```bash
# 리눅스 서버 배포
sudo make install-systemd
systemctl start mdfeed.target

# 점검
./ops/ops.sh status     # 프로세스 + HTTP 헬스 + 포트를 한 화면에
./ops/ops.sh diag       # 업스트림·시퀀스 무결성·시계 동기화·디스크까지
./ops/ops.sh top        # 자원 사용량 + 파일 디스크립터
./ops/ops.sh conns      # 현재 구독자 연결
python3 ops/healthcheck.py    # 외부 감시용 (종료코드 0/1/2)
```

**왜 워치독이 따로 필요한가.** systemd의 `Restart=on-failure` 는 *프로세스가 죽어야* 동작합니다.
그런데 피드 서비스의 실제 장애는 대부분 프로세스가 멀쩡한 채로 옵니다 —
TCP half-open(소켓은 ESTABLISHED인데 데이터가 없음), 거래소의 조용한 구독 해지,
이벤트 루프가 블로킹 호출에 물림. 전부 `/healthz` 로만 탐지되므로
**헬스 판정 → 재시작** 경로를 따로 뒀습니다. 시간당 재시작 한도를 넘으면
재시작으로 못 고치는 문제로 보고 사람을 호출합니다.

모든 서비스가 동일한 점검 규약을 갖습니다: `/healthz`(liveness) · `/readyz`(readiness) ·
`/metrics`(Prometheus). liveness와 readiness를 나눈 이유는, 기동 직후 DB 연결 전 상태를
"죽었다"고 판정해 재시작하면 영원히 못 뜨기 때문입니다.

---

## 프로젝트 구조

```
src/mdfeed/
  models.py        정규화 스키마 (Trade 64B · BookTop 72B · Signal · Bar)
  protocol.py      MDFP/1 프레이밍 · CRC32 · 시퀀스 추적
  wsproto.py       RFC 6455 WebSocket 클라이언트/서버
  httpd.py         HTTP/1.1 서버 (라우팅 · keep-alive · 정적 · WS 업그레이드)
  bus.py           UDS pub/sub 버스 (백프레셔 · 재접속)
  ringbuffer.py    공유메모리 SPSC 링버퍼
  metrics.py       로그버킷 히스토그램 · Prometheus 노출
  clock.py         거래소 시계 오프셋 추정 (최소값 필터)
  indicators.py    증분 지표 (SMA·EMA·RSI·Bollinger·MACD·ATR)
  strategies.py    전략 정의 — 실시간·백테스트 공유
  runtime.py       시그널 처리 · PID · 구조화 로깅
  cli.py           프로세스 감독기 · 진단 도구
  client.py        MDFP/1 참조 구독 클라이언트
  adapters/        upbit · binance · kis · replay
  services/        feedd · tcp_gateway · ws_gateway · rest_api · writer · strategy
  storage/         schema.sql (PG+Timescale) · schema_sqlite.sql · db.py

ops/     systemd 유닛 6종 · ops.sh · watchdog.sh · healthcheck.py · logrotate
quant/   backtest.py · run_backtest.py · integrations.py · factor_screen.py
tests/   142개 (프로토콜 · 지표 · 링버퍼 · HTTP · WS · 저장소 · 백테스트 · E2E)
bench/   계층별 성능 측정 → docs/data/bench.json
docs/    GitHub Pages 대시보드 (정적/실시간 겸용)
```

---

## 문서

- **[DESIGN.md](DESIGN.md)** — 프로토콜 스펙, 설계 판단과 그 근거, 대안을 버린 이유
- **[RUNBOOK.md](RUNBOOK.md)** — 장애 시나리오 8종의 증상·진단·조치 절차

---

## 범위와 한계

정직하게 적습니다.

- **호가는 최우선(BBO)만** 다룹니다. 전체 depth 오더북 복원은 범위 밖입니다.
- **가격은 float64** 입니다. 거래소별 tick size 테이블 기반 정수 표현은 과설계라고 판단했습니다.
- **주문 실행 없음.** 시그널 생성까지입니다. 실주문은 전혀 다른 신뢰성 요구를 갖습니다.
- **장기 안정성은 미검증입니다.** 최장 연속 구동이 1시간 남짓입니다. 하루·일주일 단위의
  메모리 증가나 파일 디스크립터 누수는 확인하지 못했습니다.
- **PINGPONG 응답은 실측 검증하지 못했습니다.** 데이터가 흐르는 동안에는 KIS 가 ping 을
  보내지 않아 한 번도 받아보지 못했습니다. 코드는 공식 예제와 맞췄을 뿐입니다.
- **장 마감·개장 전환, 동시호가, VI 발동, 휴장일**을 겪어보지 못했습니다.
- **ZeroMQ 버스 백엔드는 코드만 있고 실행해본 적이 없습니다.**
- **시계 오프셋 보정은 상대값**입니다. 편도 지연의 절대값을 알려면 PTP 하드웨어
  타임스탬프가 필요합니다. 여기서 얻는 것은 "기준선 대비 얼마나 튀었는가"이고,
  운영 알람에는 그걸로 충분합니다.
- **국내 주식 틱은 3종목까지입니다.** 계정 한도이며 실측으로 확인했습니다.
  나머지 1,780종목은 REST 스냅샷(2·3계층)으로 덮습니다 — **체결 단위가 아닙니다.**
  코스피 전종목 틱은 거래소와의 정식 시세 계약 영역이고, 개인 API 로는 불가능합니다.
- **휴장일을 반영하지 않습니다.** 장 시간 판정은 요일과 시각만 봅니다.
  공휴일에는 데이터가 없는 것으로 자연히 처리되지만, 명시적 캘린더는 없습니다.
- **구독자별 seq 재넘버링은 구독자 수만큼 재인코딩**합니다. 수백 명 규모가 되면
  심볼 그룹을 고정 채널로 묶는 방식(거래소 멀티캐스트 피드의 표준)으로 바꿔야 합니다.

---

MIT License · 참조 데이터: [oyeong011/financial-database](https://github.com/oyeong011/financial-database)
