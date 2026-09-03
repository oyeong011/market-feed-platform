"""mdfeed CLI — 서비스 기동, 프로세스 감독, 진단 도구.

`mdfeed up` 은 5개 서비스를 각각 **별도 프로세스**로 띄우고 감독한다.
systemd 가 없는 환경(맥, 컨테이너 한 개, 개발 노트북)에서도 같은 다중 프로세스
토폴로지를 그대로 재현하기 위해서다. 리눅스 서버에서는 ops/systemd/*.service 를
쓰는 쪽이 맞고, 그쪽이 정본이다.

감독기가 하는 일
----------------
* 자식이 죽으면 지수 백오프로 재시작 (crash loop 방지)
* SIGTERM 을 자식에게 전파하고 유예시간 뒤 SIGKILL
* 기동 순서: feedd 를 먼저 띄워 버스 소켓을 만든 뒤 소비자들을 올린다
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

SERVICES = [
    ("feedd", "mdfeed.services.feedd", 9100),
    ("tcp-gateway", "mdfeed.services.tcp_gateway", 9111),
    ("ws-gateway", "mdfeed.services.ws_gateway", 9102),
    ("rest-api", "mdfeed.services.rest_api", 9103),
    ("writer", "mdfeed.services.writer", 9104),
    ("strategy", "mdfeed.services.strategy", 9105),
    ("quality", "mdfeed.services.quality", 9106),
]


def _get(url: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:                            # noqa: BLE001
            return e.code, {}
    except Exception as e:                           # noqa: BLE001
        return None, {"error": str(e)}


# 샤드 구성. feedd 하나가 모든 업스트림을 들면 단일 장애점이 된다.
# 거래소 하나가 프로토콜을 바꾸거나 어댑터가 죽으면 나머지도 함께 내려간다.
SHARDS = [
    ("crypto", "upbit,binance", 0),
    ("krx", "kis,kis_rest,kis_macro", 100),
]


def cmd_up(args) -> int:
    """모든 서비스를 자식 프로세스로 띄우고 감독한다."""
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", os.path.join(os.getcwd(), "src"))
    env.setdefault("PYTHONUNBUFFERED", "1")

    procs: dict[str, subprocess.Popen] = {}
    shard_env: dict[str, dict] = {}
    restarts: dict[str, int] = {}
    last_start: dict[str, float] = {}
    stopping = False

    def spawn(name: str, module: str) -> None:
        p = subprocess.Popen([sys.executable, "-m", module],
                             env=shard_env.get(name, env))
        procs[name] = p
        last_start[name] = time.time()
        print(f"[supervisor] {name} 기동 pid={p.pid}", flush=True)

    # signal.signal 콜백 규약이라 받는다. 값은 안 쓴다
    def shutdown(_signum=None, _frame=None):
        nonlocal stopping
        if stopping:
            return
        stopping = True
        print("\n[supervisor] 종료 신호 → 자식에게 SIGTERM 전파", flush=True)
        # 소비자부터 내리고 feedd 를 마지막에 내린다(역순 종료)
        for name in reversed(list(procs)):
            p = procs[name]
            if p.poll() is None:
                p.terminate()
        deadline = time.time() + 10
        for name, p in procs.items():
            remain = max(0.0, deadline - time.time())
            try:
                p.wait(timeout=remain)
                print(f"[supervisor] {name} 정상 종료", flush=True)
            except subprocess.TimeoutExpired:
                print(f"[supervisor] {name} 응답 없음 → SIGKILL", flush=True)
                p.kill()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    if args.shards:
        # 샤드 모드: feedd 를 venue 그룹별로 쪼개 띄우고, 소비자는 전부 구독한다
        run_dir = env.get("MDFEED_RUN_DIR", "/tmp/mdfeed")
        bus_paths = []
        for shard, adapters, offset in SHARDS:
            senv = dict(env)
            senv["MDFEED_SHARD"] = shard
            senv["MDFEED_SHARD_PORT_OFFSET"] = str(offset)
            senv["MDFEED_ADAPTERS"] = adapters
            path = os.path.join(run_dir, f"bus-{shard}.sock")
            bus_paths.append(path)
            p = subprocess.Popen([sys.executable, "-m", "mdfeed.services.feedd"], env=senv)
            procs[f"feedd:{shard}"] = p
            last_start[f"feedd:{shard}"] = time.time()
            shard_env[f"feedd:{shard}"] = senv
            print(f"[supervisor] feedd:{shard} 기동 pid={p.pid} "
                  f"어댑터={adapters} 포트={9100 + offset}", flush=True)
        env["MDFEED_BUS_PATHS"] = ",".join(bus_paths)
        time.sleep(2.0)
        selected = [s for s in SERVICES if s[0] != "feedd"
                    and (not args.only or s[0] in args.only)]
        for name, module, _port in selected:
            spawn(name, module)
    else:
        selected = [s for s in SERVICES if not args.only or s[0] in args.only]
        for i, (name, module, _port) in enumerate(selected):
            spawn(name, module)
            if i == 0:
                time.sleep(1.5)      # feedd 가 버스 소켓을 만들 시간을 준다

    watch = [(n, "mdfeed.services.feedd") for n in shard_env] + \
            [(n, m) for n, m, _ in selected]
    try:
        while not stopping:
            time.sleep(1.0)
            for name, module in watch:
                p = procs.get(name)
                if p is None or p.poll() is None:
                    continue
                code = p.returncode
                n = restarts[name] = restarts.get(name, 0) + 1
                uptime = time.time() - last_start[name]
                if uptime < 5 and n >= 5:
                    print(f"[supervisor] {name} 이 5초 내 {n}회 재시작. "
                          f"crash loop 로 판단해 재시작 중단", flush=True)
                    continue
                backoff = min(2 ** min(n, 5), 30)
                print(f"[supervisor] {name} 종료(코드 {code}) → {backoff}s 후 재시작",
                      flush=True)
                time.sleep(backoff)
                if not stopping:
                    spawn(name, module)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()
    return 0


def cmd_status(_args) -> int:
    """모든 서비스의 /healthz 를 훑는다."""
    print(f"{'SERVICE':<14} {'HTTP':<6} {'HEALTHY':<8} 요약")
    print("-" * 78)
    bad = 0
    for name, _mod, port in SERVICES:
        st, body = _get(f"http://127.0.0.1:{port}/healthz")
        if st is None:
            print(f"{name:<14} {'-':<6} {'DOWN':<8} {body.get('error','')[:45]}")
            bad += 1
            continue
        healthy = body.get("healthy", False)
        if not healthy:
            bad += 1
        bits = []
        for k in ("frames_in", "rows_written", "subscribers", "ws_clients",
                  "signals_emitted", "seq", "symbols"):
            if k in body:
                bits.append(f"{k}={body[k]}")
        print(f"{name:<14} {st:<6} {'OK' if healthy else 'UNHEALTHY':<8} {' '.join(bits[:4])}")
    print("-" * 78)
    print(f"{len(SERVICES) - bad}/{len(SERVICES)} 정상")
    return 1 if bad else 0


def cmd_health(args) -> int:
    for name, _mod, port in SERVICES:
        if args.service and name != args.service:
            continue
        st, body = _get(f"http://127.0.0.1:{port}/healthz")
        print(f"=== {name} (:{port}) → {st} ===")
        print(json.dumps(body, ensure_ascii=False, indent=2))
    return 0


def cmd_client(args) -> int:
    from .client import run_client
    return run_client(args.host, args.port, args.symbols, args.duration, args.quiet)


def cmd_record(args) -> int:
    os.environ["MDFEED_RECORD_FILE"] = args.output
    from .services.feedd import main as feedd_main
    print(f"[record] {args.output} 에 녹화. Ctrl-C 로 종료")
    return feedd_main()


def cmd_retention(args) -> int:
    """지우기 전에 무엇이 지워질지 보여 준다.

    보존 일수는 되돌릴 수 없다. 3일이 얼마인지는 테이블마다 다르고,
    쌓인 양에 따라 첫 삭제 비용도 다르다. 켜기 전에 숫자를 본다.
    """
    from .config import Config
    from .retention import DiskWatch, prune_plan
    from .storage.db import open_storage

    cfg = Config()
    store = open_storage(cfg)
    disk = DiskWatch(cfg.sqlite_path)
    r = disk.report()
    print(f"DB {r['db_bytes']/1e9:.2f}GB "
          f"(빈 자리 {r['reclaimable_bytes']/1e9:.2f}GB) · "
          f"디스크 여유 {r['disk_free_bytes']/1e9:.1f}GB")
    print(f"현재 설정: RETENTION_DAYS={cfg.retention_days} "
          f"({'켜짐' if cfg.retention_days > 0 else '꺼짐 — 아무것도 안 지운다'})")
    print()
    print(f"{'보존일':>5} {'테이블':<9} {'전체행':>13} {'지울행':>13} "
          f"{'남길행':>13} {'배치':>6}")
    print("─" * 68)
    for days in args.days:
        plan = prune_plan(store, days)
        for table, t in plan["tables"].items():
            if "error" in t:
                print(f"{days:>5g} {table:<9} {t['error']}")
                continue
            print(f"{days:>5g} {table:<9} {t['rows']:>13,} "
                  f"{t['delete_rows']:>13,} {t['keep_rows']:>13,} "
                  f"{t['batches']:>6,}")
    print()
    print("배치 하나는 50,000행이다. 락은 배치마다 놓으므로 적재는 안 멈추지만,")
    print(f"한 번에 도는 시간은 RETENTION_BUDGET_S={cfg.retention_budget_s:g}초로 "
          "끊긴다 — 남은 건 다음 주기가 이어서 지운다.")
    print("SQLite auto_vacuum=0 이라 파일 크기는 안 줄고 빈 자리로 재사용된다.")
    print("증가는 멈추지만 db_bytes 는 그대로다. 그게 정상이다.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("mdfeed", description="MDFeed 마켓데이터 FEED 플랫폼")
    sub = p.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("up", help="모든 서비스를 띄우고 감독")
    up.add_argument("--only", nargs="*", help="일부 서비스만")
    up.add_argument("--shards", action="store_true",
                    help="feedd 를 venue 그룹별로 쪼개 띄운다 (단일 장애점 제거)")
    up.set_defaults(fn=cmd_up)

    st = sub.add_parser("status", help="서비스 상태 한눈에 보기")
    st.set_defaults(fn=cmd_status)

    he = sub.add_parser("health", help="상세 헬스 JSON")
    he.add_argument("service", nargs="?")
    he.set_defaults(fn=cmd_health)

    cl = sub.add_parser("client", help="TCP 피드 구독 클라이언트 (참조 구현)")
    cl.add_argument("--host", default="127.0.0.1")
    cl.add_argument("--port", type=int, default=9101)
    cl.add_argument("--symbols", nargs="*")
    cl.add_argument("--duration", type=float, default=0, help="0 이면 무한")
    cl.add_argument("--quiet", action="store_true", help="틱 출력 없이 통계만")
    cl.set_defaults(fn=cmd_client)

    rt = sub.add_parser("retention",
                        help="보존 일수별로 무엇이 지워질지 미리 본다 (안 지움)")
    rt.add_argument("--days", type=float, nargs="*", default=[1, 3, 7, 14],
                    help="비교할 보존 일수 (기본 1 3 7 14)")
    rt.set_defaults(fn=cmd_retention)

    rc = sub.add_parser("record", help="피드를 파일로 녹화 (리플레이용)")
    rc.add_argument("-o", "--output", default="data/replay/sample.mdf")
    rc.set_defaults(fn=cmd_record)

    for name, module, _ in SERVICES:
        s = sub.add_parser(name, help=f"{name} 단일 프로세스 실행")
        s.set_defaults(fn=lambda a, m=module: _run_module(m))
    return p


def _run_module(module: str) -> int:
    import importlib
    return importlib.import_module(module).main()


def main() -> int:
    args = build_parser().parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
