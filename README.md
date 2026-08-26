# MDFeed — 실시간 마켓데이터 FEED 플랫폼

[![CI](https://github.com/oyeong011/market-feed-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/oyeong011/market-feed-platform/actions/workflows/ci.yml)
[![Pages](https://github.com/oyeong011/market-feed-platform/actions/workflows/pages.yml/badge.svg)](https://oyeong011.github.io/market-feed-platform/)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![deps](https://img.shields.io/badge/핵심%20의존성-0-brightgreen)
![tests](https://img.shields.io/badge/tests-111%20passing-brightgreen)

거래소 실시간 시세를 **수집 → 정규화 → 멀티프로토콜 배포**하는 마켓데이터 피드 서비스와
그것을 리눅스에서 운영하기 위한 자동화 스택.

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
| **WebSocket 클라이언트** (RFC 6455) | HTTP Upgrade 핸드셰이크, `Sec-WebSocket-Accept` 계산, 마스킹, 확장 길이(7/16/64비트), 단편화 재조립, ping/pong |
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
make test        # 111개 테스트 — 네트워크 불필요
```

인터넷이 없어도 됩니다. 저장소에 든 녹화 파일로 전 구간을 재현합니다:

```bash
MDFEED_ADAPTERS=replay make demo
```

---

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
tests/   111개 (프로토콜 · 지표 · 링버퍼 · HTTP · WS · 저장소 · 백테스트 · E2E)
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
- **시계 오프셋 보정은 상대값**입니다. 편도 지연의 절대값을 알려면 PTP 하드웨어
  타임스탬프가 필요합니다. 여기서 얻는 것은 "기준선 대비 얼마나 튀었는가"이고,
  운영 알람에는 그걸로 충분합니다.
- **KIS 어댑터는 자격증명이 없어 실계좌로 검증하지 못했습니다.** 프로토콜 명세대로
  구현했고 키가 없으면 스스로 비활성화됩니다. 크립토 어댑터 2종은 실거래소로 검증했습니다.
- **구독자별 seq 재넘버링은 구독자 수만큼 재인코딩**합니다. 수백 명 규모가 되면
  심볼 그룹을 고정 채널로 묶는 방식(거래소 멀티캐스트 피드의 표준)으로 바꿔야 합니다.

---

MIT License · 참조 데이터: [oyeong011/financial-database](https://github.com/oyeong011/financial-database)
