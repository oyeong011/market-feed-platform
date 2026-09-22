# MDFeed C++ 데이터 평면

파이썬 구현(`src/mdfeed/`)과 **바이트 단위로 호환**되는 C++ 구현. 의존성은 컴파일러(C++20)뿐이다.

| 파일 | 역할 | 파이썬 대응 |
|---|---|---|
| `include/mdfp/protocol.hpp` | MDFP/1 인코더 · 스트리밍 파서(재동기화) · 갭 탐지 · Trade/BookTop | `protocol.py`, `models.py` |
| `include/mdfp/crc32.hpp` | CRC-32 (zlib 과 동일 결과), 슬라이싱-바이-8 | `zlib.crc32` |
| `src/tcp_gateway.cpp` | 배포 게이트웨이. 스냅샷→증분, 구독 필터, 구독자별 재번호, 백프레셔, conflate, `/healthz` `/metrics` | `services/tcp_gateway.py` |
| `tests/test_protocol.cpp` | `tests/test_protocol.py` 를 그대로 옮긴 테스트 | |
| `tests/conformance.cpp` | 파이썬↔C++ 양방향 교차 검증 도구 (`tests/test_cpp_conformance.py` 가 부른다) | |
| `bench/bench_protocol.cpp` | `bench/latency_bench.py::bench_protocol` 과 같은 방법 | |

```bash
make -C cpp all test        # 빌드 + C++ 단위 테스트
make cpp-test               # + 파이썬 교차 검증 + 게이트웨이 통합 시험 (저장소 루트에서)
make cpp-gateway            # 파이썬 tcp_gateway 대신 기동. 같은 환경변수(MDFEED_BUS_PATH, MDFEED_TCP_PORT, ...)
```

게이트웨이는 파이썬 참조 클라이언트(`python -m mdfeed client`), 부하 시험(`bench/load_test.py`),
운영 점검(`ops/healthcheck.py`) 이 그대로 붙는다. 첫 stdout 줄은 기계용 JSON 이다
(`{"event":"listening","tcp_port":..,"admin_port":..}`) — 포트 0 으로 띄우면 실제 포트를 여기서 읽는다.

설계 메모는 각 파일 상단 주석에 있다. 특히 `tcp_gateway.cpp` 의 "묶음 전송" 주석은 첫 측정에서
파이썬보다 느렸던 이유(README 결함 27)를 기록한다.

## 코드 리뷰에서 잡힌 것 (2026-09-22, 9건 전부 수정·회귀 테스트)

첫 구현을 리뷰에 넣었더니 아래가 나왔다. 성능 표가 나온 뒤였다 — **빠른 것과 안전한 것은 다른 문제다.**

| # | 지적 | 왜 위험한가 | 조치 · 테스트 |
|---|---|---|---|
| 1 | 관리 포트 요청 줄을 검사 없이 `substr` | `\r\n\r\n` 한 줄에 예외 → 프로세스 종료 → 구독자 전원 끊김 | 요청 줄 안전 파싱 + 이벤트 처리 전체를 try 로 감싸 해당 연결만 정리. `test_snapshot_filter_and_renumbering` 이 쓰레기 5종을 보내고 생존 확인 |
| 2 | conflate 모드에서 키 없는 프레임(하트비트)이 영영 안 나감 | 큐만 자라고, 넘치면 정상 대기 항목을 버리다 결국 강제 종료 | 순서 큐 하나로 통합, seq 는 보낼 때. `test_conflate_mode…` 가 하트비트 3건 도착 확인 |
| 3 | 관리 응답을 블로킹 `send` 로 | 안 읽는 관리 클라이언트 하나가 배포 전체를 멈춤 | 논블로킹 + POLLOUT + 5초 기한 |
| 4 | 호스트 해석 실패 시 조용히 0.0.0.0 | `localhost` 로 루프백만 열려던 관리 포트가 전 인터페이스에 노출 | `localhost` 만 매핑, 그 외는 기동 실패. `test_unresolvable_host_fails_fast`, `test_localhost_binds_loopback` |
| 5 | 깨진 구독 요청이 필터를 전체 구독으로 풂 | 부분 수신 한 번에 구독자가 전 종목을 받고 넘쳐서 끊김 | 파이썬과 동일하게 무시. mode 대소문자 무시 |
| 6 | 파서 콜백이 던지면 같은 프레임 재배달 | 잡고 계속 먹이는 호출자에서 무한 중복 | 소비 처리를 콜백 앞으로. C++ 테스트 |
| 7 | fd 고갈 시 accept 오류로 100% 스핀 | level-trigger 리스너가 매 poll 마다 깨움 | 200ms 백오프 + 로그 1회/초 |
| 8 | 헬스 JSON 고정 2KB 버퍼 | 버스 경로 10개 넘으면 잘린 JSON → 헬스체크 실패 → 재시작 루프 | `std::string` 으로 |
| 9 | 한글 심볼을 바이트 단위로 절단 | 파이썬과 바이트·CRC 가 달라져 호환 주장이 깨짐 | 파이썬 `_fix` 와 같은 UTF-8 경계 절단. C++ 테스트 + 교차 검증에 한글 심볼 포함 |
