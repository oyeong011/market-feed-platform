# MDFeed — 개발·운영 공통 진입점.
# 사람이 외워야 할 명령을 줄이는 게 목적이다. CI 도 이 타깃들을 그대로 부른다.

PY      ?= python3
VENV    := .venv
BIN     := $(VENV)/bin
PYTHONPATH := src
export PYTHONPATH

.DEFAULT_GOAL := help
.PHONY: help venv install test lint bench up down status health logs \
        demo record replay backtest factor clean docker-build docker-up docker-down \
        ci docs schema

help:  ## 사용 가능한 명령
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "; printf "\nMDFeed 실시간 마켓데이터 FEED 플랫폼\n\n"} \
	       {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""

$(VENV):
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -q --upgrade pip

venv: $(VENV)  ## 가상환경 생성

install: venv  ## 개발 의존성 설치 (핵심 경로는 표준 라이브러리만 씀)
	$(BIN)/pip install -q pytest
	@echo "설치 완료. 퀀트 교차검증까지 원하면: $(BIN)/pip install -e '.[quant]'"

test: venv  ## 테스트 (외부 네트워크 불필요)
	$(BIN)/pip install -q pytest
	$(BIN)/python -m pytest tests/ -q

test-verbose: venv  ## 테스트 상세 출력
	$(BIN)/python -m pytest tests/ -v

lint:  ## 문법·임포트 점검 (외부 린터 없이)
	@$(PY) -m compileall -q src quant bench tests ops/healthcheck.py && echo "컴파일 OK"
	@$(PY) -c "import sys; sys.path.insert(0,'src'); \
	  import mdfeed.cli, mdfeed.services.feedd, mdfeed.services.tcp_gateway, \
	         mdfeed.services.ws_gateway, mdfeed.services.rest_api, \
	         mdfeed.services.writer, mdfeed.services.strategy; print('임포트 OK')"
	@bash -n ops/ops.sh ops/watchdog.sh && echo "셸 문법 OK"
	@$(PY) scripts/find_unused_params.py --check && echo "안 쓰는 인자 OK"

bench: venv  ## 성능 벤치마크 → docs/data/bench.json
	$(BIN)/python bench/latency_bench.py --out docs/data/bench.json

up:  ## 전체 스택 기동 (6개 프로세스, 포그라운드 감독)
	$(PY) -m mdfeed.cli up

up-shards:  ## feedd 를 venue 그룹별로 쪼개 기동 (단일 장애점 제거)
	$(PY) -m mdfeed.cli up --shards

MINUTES ?= 60
soak: venv  ## 장시간 감시 (MINUTES=60) → 누수 임계 초과 시 실패
	$(BIN)/python bench/soak.py --minutes $(MINUTES) --interval 30 \
	  --out docs/data/soak.json

chaos:  ## 장애 주입 — 복구 경로가 실제로 도는지 확인
	@bash ops/chaos.sh all

obs-up:  ## Prometheus + Grafana 기동
	docker compose -f docker-compose.observability.yml up -d
	@echo "  Prometheus http://localhost:9090"
	@echo "  Grafana    http://localhost:3000 (admin/admin)"

obs-down:  ## 관측 스택 종료
	docker compose -f docker-compose.observability.yml down

verify-alerts:  ## 알람이 실재하는 지표를 참조하는지 검증
	$(PY) scripts/verify_alerts.py

load:  venv  ## 배포단 부하 시험 → docs/data/load.json
	@echo "리플레이를 고정 속도로 돌린 뒤 실행하세요: MDFEED_ADAPTERS=replay make up"
	$(BIN)/python bench/load_test.py --subscribers 1 10 25 50 100 \
	  --seconds 10 --out docs/data/load.json

up-bg:  ## 전체 스택 백그라운드 기동
	@$(PY) -m mdfeed.cli up > /tmp/mdfeed-stack.log 2>&1 & \
	 echo "기동 중... (로그: /tmp/mdfeed-stack.log)"; sleep 8; $(MAKE) status

down:  ## 전체 스택 종료
	@pkill -f "mdfeed.cli up" 2>/dev/null || true
	@pkill -f "mdfeed.services" 2>/dev/null || true
	@# 패턴을 [m] 으로 쪼개는 건 pgrep 이 **레시피를 실행 중인 셸 자신**을
	@# 잡지 않게 하려는 것이다. sh -c 의 명령줄에 패턴 문자열이 그대로 들어
	@# 있어서, 그냥 쓰면 프로세스가 하나도 없어도 항상 자기를 세어 실패한다.
	@# 감독 프로세스는 자식 종료에 10초 기한을 준다. 1초 뒤에 "완료"를 찍으면
	@# 아직 살아 있는 프로세스를 종료됐다고 보고하는 것이다. 실제로 확인한다.
	@for i in $$(seq 1 15); do \
	  pgrep -f "[m]dfeed.cli up|[m]dfeed.services" >/dev/null 2>&1 || break; \
	  sleep 1; \
	done; \
	left=$$(pgrep -f "[m]dfeed.cli up|[m]dfeed.services" 2>/dev/null | wc -l | tr -d " "); \
	if [ "$$left" = "0" ]; then echo "종료 완료"; \
	else echo "종료 미완: $$left개 남음"; pgrep -af "[m]dfeed.cli up|[m]dfeed.services"; exit 1; fi

status:  ## 서비스 상태
	@bash ops/ops.sh status

health:  ## 상세 헬스 JSON
	@bash ops/ops.sh health

diag:  ## 장애 진단 원스톱
	@bash ops/ops.sh diag

logs:  ## 로그 tail
	@bash ops/ops.sh logs

check:  ## 외부 감시용 헬스체크 (종료코드 0/1/2)
	@$(PY) ops/healthcheck.py

client:  ## 참조 TCP 구독 클라이언트로 30초 구독
	$(PY) -m mdfeed.cli client --duration 30

record:  ## 실시간 피드를 녹화 (Ctrl-C 로 종료)
	MDFEED_RECORD_FILE=data/replay/sample.mdf $(PY) -m mdfeed.services.feedd

replay:  ## 녹화 파일로 오프라인 데모 (네트워크 불필요)
	MDFEED_ADAPTERS=replay $(PY) -m mdfeed.cli up

demo:  ## 원클릭 데모 — 스택 기동 + 대시보드 안내
	@bash scripts/demo.sh

backtest: venv  ## 백테스트 → docs/data/backtest.json
	$(BIN)/python quant/run_backtest.py --source replay \
	  --symbol BINANCE:BTCUSDT --interval 60 --out docs/data/backtest.json

factor: venv  ## 재무 팩터 스크리닝 → docs/data/factors.json
	$(BIN)/python quant/factor_screen.py --market dart --top 20 \
	  --out docs/data/factors.json

schema:  ## PostgreSQL 스키마 적용 (DATABASE_URL 필요)
	psql "$$DATABASE_URL" -f src/mdfeed/storage/schema.sql

quality-snapshot: ## 품질 검사 스냅샷 → docs/data/quality.json
	$(PY) scripts/capture_quality.py --out docs/data/quality.json

docs: bench backtest quality-snapshot  ## 대시보드용 데이터 갱신
	@echo "docs/data/ 갱신 완료"

docker-build:  ## 컨테이너 이미지 빌드
	docker build -t mdfeed:latest .

docker-up:  ## docker compose 로 전체 스택 + Postgres 기동
	docker compose up -d --build
	@sleep 6; docker compose ps

docker-down:  ## docker compose 종료
	docker compose down -v

systemd-test:  ## systemd 가 실제로 도는 리눅스 컨테이너에서 유닛 검증
	@bash ops/testbed/verify_systemd.sh

install-systemd:  ## 리눅스 서버에 systemd 설치 (root 필요)
	sudo bash ops/ops.sh install

ci: lint test  ## CI 가 실행하는 것

clean:  ## 산출물 정리 (녹화 파일은 남긴다)
	rm -rf .pytest_cache **/__pycache__ src/**/__pycache__ .venv
	rm -f data/mdfeed.db data/mdfeed.db-wal data/mdfeed.db-shm
	@echo "정리 완료"
