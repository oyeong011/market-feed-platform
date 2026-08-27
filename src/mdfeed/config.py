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
    kis_app_key: str = field(default_factory=lambda: os.getenv("KIS_APP_KEY", ""))
    kis_app_secret: str = field(default_factory=lambda: os.getenv("KIS_APP_SECRET", ""))
    # real = 실전투자, vts = 모의투자. 실시간 시세는 실전 계좌 키를 쓴다.
    kis_env: str = field(default_factory=lambda: _env("KIS_ENV", "real"))
    replay_file: str = field(default_factory=lambda: _env("REPLAY_FILE", "data/replay/sample.mdf"))
    replay_speed: float = field(default_factory=lambda: float(_env("REPLAY_SPEED", "1.0")))
    replay_loop: bool = field(default_factory=lambda: _bool("REPLAY_LOOP", True))
    # 재생 시 체결 시각을 현재 기준으로 평행이동한다. 장애 재현에는 0 으로 끈다.
    replay_restamp: bool = field(default_factory=lambda: _bool("REPLAY_RESTAMP", True))

    # ── IPC ────────────────────────────────────────────────────────────────
    bus_backend: str = field(default_factory=lambda: _env("BUS_BACKEND", "uds"))  # uds|zmq
    bus_path: str = field(default_factory=lambda: _env("BUS_PATH", "/tmp/mdfeed/bus.sock"))
    bus_zmq_endpoint: str = field(default_factory=lambda: _env("BUS_ZMQ", "tcp://127.0.0.1:5599"))
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
    retention_days: int = field(default_factory=lambda: _int("RETENTION_DAYS", 30))

    # ── 전략엔진 ───────────────────────────────────────────────────────────
    strategies: list[str] = field(default_factory=lambda: _list("STRATEGIES", "sma_cross,rsi_revert"))
    signal_cooldown_s: float = field(default_factory=lambda: float(_env("SIGNAL_COOLDOWN_S", "30")))

    def to_dict(self) -> dict:
        d = asdict(self)
        for secret in ("kis_app_key", "kis_app_secret", "pg_dsn"):
            if d.get(secret):
                d[secret] = "***"          # 로그/헬스체크에 자격증명이 새지 않게
        return d


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
    return cfg
