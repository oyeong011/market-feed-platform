"""환경변수 기반 설정. 12-factor 원칙대로 코드에 상수를 박지 않는다.

systemd unit / docker-compose / CI 가 전부 같은 환경변수를 쓰므로,
설정이 어디서 왔는지 추적할 곳이 이 파일 하나뿐이다.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, asdict


def load_env_file(path: str) -> int:
    """KEY="value" 형식 파일을 환경변수로 올린다 (이미 있는 값은 덮지 않는다).

    자격증명을 리포지터리 밖에 두기 위한 최소 장치다. docker 의 --env-file 과
    같은 역할이고, systemd 배포에서는 EnvironmentFile= 이 같은 일을 한다.
    값은 어디에도 로그로 남기지 않는다.
    """
    n = 0
    try:
        with open(os.path.expanduser(path), encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and k not in os.environ:
                    os.environ[k] = v
                    n += 1
    except FileNotFoundError:
        return 0
    return n


def _env(key: str, default: str) -> str:
    return os.getenv(f"MDFEED_{key}", default)


def _int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


def _bool(key: str, default: bool = False) -> bool:
    return _env(key, "1" if default else "0").lower() in ("1", "true", "yes", "on")


def _list(key: str, default: str) -> list[str]:
    return [x.strip() for x in _env(key, default).split(",") if x.strip()]


@dataclass
class Config:
    # ── 실행 환경 ──────────────────────────────────────────────────────────
    run_dir: str = field(default_factory=lambda: _env("RUN_DIR", "/tmp/mdfeed"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    log_json: bool = field(default_factory=lambda: _bool("LOG_JSON", False))

    # ── 업스트림 ───────────────────────────────────────────────────────────
    # replay 는 네트워크 없이 녹화 파일을 재생한다 → CI/오프라인 데모 기본값
    adapters: list[str] = field(default_factory=lambda: _list("ADAPTERS", "upbit,binance"))
    upbit_symbols: list[str] = field(
        default_factory=lambda: _list("UPBIT_SYMBOLS", "KRW-BTC,KRW-ETH,KRW-XRP,KRW-SOL"))
    binance_symbols: list[str] = field(
        default_factory=lambda: _list("BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt"))
    kis_symbols: list[str] = field(
        default_factory=lambda: _list("KIS_SYMBOLS", "005930,000660,373220"))
    # 암호화폐 유니버스. KRX 와 달리 웹소켓 구독이라 종목을 늘리면 유량이
    # 실제로 늘어난다(폴링처럼 회전 주기가 길어지는 게 아니다).
    # 한도 0 = 위의 명시 목록을 쓴다. >0 이면 마스터에서 앞의 N개.
    crypto_universe_path: str = field(
        default_factory=lambda: _env("CRYPTO_UNIVERSE", "data/reference/crypto_symbols.csv"))
    upbit_universe_limit: int = field(
        default_factory=lambda: _int("UPBIT_UNIVERSE_LIMIT", 0))
    binance_universe_limit: int = field(
        default_factory=lambda: _int("BINANCE_UNIVERSE_LIMIT", 0))
    kis_app_key: str = field(default_factory=lambda: os.getenv("KIS_APP_KEY", ""))
    kis_app_secret: str = field(default_factory=lambda: os.getenv("KIS_APP_SECRET", ""))
    # real = 실전투자, vts = 모의투자. 실시간 시세는 실전 계좌 키를 쓴다.
    kis_env: str = field(default_factory=lambda: _env("KIS_ENV", "real"))
    # KRX 광역 스냅샷 (웹소켓 종목 한도를 REST 로 우회)
    krx_universe_path: str = field(
        default_factory=lambda: _env("KRX_UNIVERSE", "data/reference/krx_symbols.csv"))
    krx_markets: list[str] = field(default_factory=lambda: _list("KRX_MARKETS", "KOSPI"))
    krx_universe_limit: int = field(default_factory=lambda: _int("KRX_UNIVERSE_LIMIT", 0))
    # 실측 한도는 초당 5건. 4건에서도 실패가 나와 3건으로 둔다.
    kis_rest_rate: float = field(default_factory=lambda: float(_env("KIS_REST_RATE", "3.0")))
    # AIMD 가 올라갈 수 있는 상한. 0 = kis_rest_rate 와 같음(탐색 안 함).
    # 문서·실측 한도는 5 req/s 다. 3 에 묶어 두면 1,783종목 한 바퀴가 594초,
    # 5 까지 올라가면 357초다 — 종목당 갱신 주기가 그만큼 짧아진다.
    # 요청을 더 보내는 변경이라 기본으로 켜지 않는다.
    kis_rest_rate_max: float = field(
        default_factory=lambda: float(_env("KIS_REST_RATE_MAX", "0")))
    kis_rank_interval_s: float = field(
        default_factory=lambda: float(_env("KIS_RANK_INTERVAL_S", "10")))
    kis_index_interval_s: float = field(default_factory=lambda: float(_env("KIS_INDEX_INTERVAL_S", "10")))
    kis_rate_interval_s: float = field(default_factory=lambda: float(_env("KIS_RATE_INTERVAL_S", "300")))
    kis_token_cache: str = field(
        default_factory=lambda: _env("KIS_TOKEN_CACHE", "~/.mdfeed/kis_token.json"))
    replay_file: str = field(default_factory=lambda: _env("REPLAY_FILE", "data/replay/sample.mdf"))
    replay_speed: float = field(default_factory=lambda: float(_env("REPLAY_SPEED", "1.0")))
    replay_loop: bool = field(default_factory=lambda: _bool("REPLAY_LOOP", True))
    # 재생 시 체결 시각을 현재 기준으로 평행이동한다. 장애 재현에는 0 으로 끈다.
    replay_restamp: bool = field(default_factory=lambda: _bool("REPLAY_RESTAMP", True))

    # ── IPC ────────────────────────────────────────────────────────────────
    bus_backend: str = field(default_factory=lambda: _env("BUS_BACKEND", "uds"))  # uds|zmq
    # ── 샤딩 ──────────────────────────────────────────────────────────────
    # feedd 하나가 모든 업스트림을 들면 단일 장애점이 된다. 거래소 하나가
    # 프로토콜을 바꾸거나 어댑터가 죽으면 나머지 전부가 함께 내려간다.
    # 샤드 이름을 주면 그 샤드 전용 버스 소켓·관리 포트를 쓴다.
    #
    #   MDFEED_SHARD=crypto  MDFEED_ADAPTERS=upbit,binance
    #   MDFEED_SHARD=krx     MDFEED_ADAPTERS=kis,kis_rest,kis_macro
    #
    # 소비자는 MDFEED_BUS_PATHS 로 여러 샤드를 한꺼번에 구독한다.
    shard: str = field(default_factory=lambda: _env("SHARD", ""))
    bus_path: str = field(default_factory=lambda: _env("BUS_PATH", "/tmp/mdfeed/bus.sock"))
    bus_zmq_endpoint: str = field(default_factory=lambda: _env("BUS_ZMQ", "tcp://127.0.0.1:5599"))
    # 소비자가 구독할 버스 소켓 목록 (샤드 여러 개를 한꺼번에)
    bus_paths: list[str] = field(default_factory=lambda: _list("BUS_PATHS", ""))
    shard_port_offset: int = field(default_factory=lambda: _int("SHARD_PORT_OFFSET", 0))
    bus_queue_size: int = field(default_factory=lambda: _int("BUS_QUEUE", 4096))
    ring_name: str = field(default_factory=lambda: _env("RING_NAME", "mdfeed_ring"))
    ring_capacity: int = field(default_factory=lambda: _int("RING_CAPACITY", 65536))
    ring_slot_size: int = field(default_factory=lambda: _int("RING_SLOT", 128))
    ring_enabled: bool = field(default_factory=lambda: _bool("RING_ENABLED", True))

    # ── 배포(다운스트림) ───────────────────────────────────────────────────
    tcp_host: str = field(default_factory=lambda: _env("TCP_HOST", "0.0.0.0"))
    tcp_port: int = field(default_factory=lambda: _int("TCP_PORT", 9101))
    ws_host: str = field(default_factory=lambda: _env("WS_HOST", "0.0.0.0"))
    ws_port: int = field(default_factory=lambda: _int("WS_PORT", 9102))
    http_host: str = field(default_factory=lambda: _env("HTTP_HOST", "0.0.0.0"))
    http_port: int = field(default_factory=lambda: _int("HTTP_PORT", 9103))
    # 서비스별 관리 포트. 프로세스마다 /healthz /metrics 가 따로 떠야
    # "어느 프로세스가 아픈지"를 구분할 수 있다.
    feedd_admin_port: int = field(default_factory=lambda: _int("FEEDD_ADMIN_PORT", 9100))
    tcp_admin_port: int = field(default_factory=lambda: _int("TCP_ADMIN_PORT", 9111))
    writer_admin_port: int = field(default_factory=lambda: _int("WRITER_ADMIN_PORT", 9104))
    strategy_admin_port: int = field(default_factory=lambda: _int("STRATEGY_ADMIN_PORT", 9105))
    quality_admin_port: int = field(default_factory=lambda: _int("QUALITY_ADMIN_PORT", 9106))
    # 품질 검사 임계값. 오탐이 잦으면 사람이 알람을 무시하게 되므로 보수적으로.
    qc_jump_abs_pct: float = field(default_factory=lambda: float(_env("QC_JUMP_ABS_PCT", "10")))
    qc_jump_sigma: float = field(default_factory=lambda: float(_env("QC_JUMP_SIGMA", "8")))
    qc_max_spread_bp: float = field(default_factory=lambda: float(_env("QC_MAX_SPREAD_BP", "1000")))
    qc_stale_after_s: float = field(default_factory=lambda: float(_env("QC_STALE_AFTER_S", "120")))
    qc_divergence_pct: float = field(default_factory=lambda: float(_env("QC_DIVERGENCE_PCT", "3")))
    signal_bus_path: str = field(default_factory=lambda: _env("SIGNAL_BUS_PATH", "/tmp/mdfeed/signals.sock"))
    record_file: str = field(default_factory=lambda: _env("RECORD_FILE", ""))
    heartbeat_s: float = field(default_factory=lambda: float(_env("HEARTBEAT_S", "5")))
    client_queue_size: int = field(default_factory=lambda: _int("CLIENT_QUEUE", 2048))

    # ── 저장소 ─────────────────────────────────────────────────────────────
    # DSN 이 비면 SQLite 로 떨어진다. 로컬/CI 에서 Postgres 없이도 전 구간이 돈다.
    pg_dsn: str = field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    sqlite_path: str = field(default_factory=lambda: _env("SQLITE_PATH", "data/mdfeed.db"))
    bar_interval_s: int = field(default_factory=lambda: _int("BAR_INTERVAL_S", 60))
    write_batch: int = field(default_factory=lambda: _int("WRITE_BATCH", 500))
    write_flush_s: float = field(default_factory=lambda: float(_env("WRITE_FLUSH_S", "2.0")))
    # 원시 체결·호가 보존 일수. 0 = 끄기.
    #
    # 이 설정은 원래 선언만 있고 아무 데서도 쓰이지 않았다. 운영자가
    # RETENTION_DAYS=7 로 두면 7일 뒤 지워진다고 믿는데 실제로는 계속 쌓였다.
    # **아무것도 안 하는 설정은 없는 것보다 나쁘다** — 디스크가 찰 때까지
    # 아무도 이상을 못 느낀다.
    #
    # 이제 writer 가 실제로 지운다(mdfeed/retention.py). 그래서 기본값을
    # 30 에서 0 으로 내렸다. 구현이 붙는 순간 기존 배포에서 30일 넘은 데이터가
    # 조용히 삭제되기 때문이다. 지우는 기능은 명시적으로 켜야 한다.
    retention_days: float = field(
        default_factory=lambda: float(_env("RETENTION_DAYS", "0")))
    retention_interval_s: float = field(
        default_factory=lambda: float(_env("RETENTION_INTERVAL_S", "3600")))
    # 이 시간 안에 디스크가 찰 것으로 보이면 경고한다. 보존을 꺼 뒀어도
    # 디스크 감시는 항상 돈다 — 재는 것과 지우는 것은 별개다.
    disk_warn_hours: float = field(
        default_factory=lambda: float(_env("DISK_WARN_HOURS", "24")))

    # ── 전략엔진 ───────────────────────────────────────────────────────────
    strategies: list[str] = field(default_factory=lambda: _list("STRATEGIES", "sma_cross,rsi_revert"))
    signal_cooldown_s: float = field(default_factory=lambda: float(_env("SIGNAL_COOLDOWN_S", "30")))

    def to_dict(self) -> dict:
        d = asdict(self)
        for secret in ("kis_app_key", "kis_app_secret", "pg_dsn"):
            if d.get(secret):
                d[secret] = "***"          # 로그/헬스체크에 자격증명이 새지 않게
        return d


def shard_bus_path(base: str, shard: str) -> str:
    """샤드 이름을 소켓 경로에 끼워 넣는다. `/run/mdfeed/bus.sock` → `bus-krx.sock`"""
    if not shard:
        return base
    d, name = os.path.split(base)
    stem, _, ext = name.rpartition(".")
    return os.path.join(d, f"{stem or name}-{shard}" + (f".{ext}" if stem else ""))


def load() -> Config:
    # MDFEED_ENV_FILE 이 지정돼 있으면 먼저 읽어 환경변수로 올린다.
    # Config 필드가 os.getenv 를 default_factory 에서 읽으므로 순서가 중요하다.
    env_file = os.getenv("MDFEED_ENV_FILE")
    if env_file:
        n = load_env_file(env_file)
        logging.getLogger("mdfeed.config").info(
            "환경파일에서 %d개 변수 로드: %s", n, env_file)
    cfg = Config()
    os.makedirs(cfg.run_dir, exist_ok=True)

    if cfg.shard:
        # 발행자는 자기 샤드 소켓에만 쓴다
        cfg.bus_path = shard_bus_path(cfg.bus_path, cfg.shard)
        # 샤드마다 관리 포트가 달라야 어느 샤드가 아픈지 구분된다
        off = cfg.shard_port_offset
        for f in ("feedd_admin_port", "tcp_admin_port", "writer_admin_port",
                  "strategy_admin_port", "quality_admin_port",
                  "tcp_port", "ws_port", "http_port"):
            setattr(cfg, f, getattr(cfg, f) + off)
        if not os.getenv("MDFEED_RING_NAME"):
            cfg.ring_name = f"{cfg.ring_name}_{cfg.shard}"

    # 소비자가 구독할 버스 목록. 지정이 없으면 자기 bus_path 하나.
    if not cfg.bus_paths:
        cfg.bus_paths = [cfg.bus_path]
    return cfg
