"""장시간 구동 감시 — 누수는 오래 돌려야만 보인다.

이 프로젝트의 최장 연속 구동은 한 시간 남짓이었다. 그 정도로는 메모리 누수도
파일 디스크립터 누수도 안 보인다. 마켓데이터 서비스는 몇 주씩 돌아야 하고,
누수는 천천히 자라다가 새벽에 터진다.

이 하네스는 모든 서비스의 `/healthz` 를 주기적으로 훑어 RSS·fd·처리량을 기록하고,
**증가 추세**를 낸다. 절대값만 보면 판단이 안 된다 — RSS 200MB 가 문제인지는
그게 자라고 있는지에 달렸다.

판정 기준
---------
| 항목 | 임계 | 근거 |
| --- | --- | --- |
| RSS 증가 | 5 MB/h 초과 | 하루면 120MB. 한 주면 컨테이너 한도를 넘는다 |
| fd 증가 | 1 개/h 초과 **그리고** 절대 증가 2개 이상 | 아래 참고 |
| 처리량 감소 | 시작 대비 50% 미만 | 무언가 쌓여 느려지고 있다 |

**워밍업 구간을 기울기에 넣으면 정상적인 시동을 누수로 읽는다.**
프로세스는 뜨자마자 캐시·아레나·버퍼를 채우며 RSS 가 한 번 오른 뒤 평평해진다. 그 구간을
포함해 직선을 맞추면 기울기가 실제보다 가파르다. 첫 리눅스 25분 관측(2026-09-25)에서
feedd +8.4MB/h · writer +5.7MB/h · mcast-publisher +7.1MB/h 로 임계를 넘었는데, 같은 창의
**실제 증가는 2.3~3.4MB** 였다. 25분짜리 시동을 한 시간으로 외삽한 값이다.
그래서 앞쪽 `--warmup-minutes`(기본 5분)를 기울기 계산에서 뺀다. 판정 창의 시작은 시동이
끝난 뒤여야 한다.

**짧은 관측에서 시간당 기울기를 내면 노이즈가 증폭된다.**
처음 4분을 돌렸을 때 `feedd-krx` 가 "fd +4.4/h" 로 걸렸다. 그런데 시작도 21개,
끝도 21개였다. 중간에 한 번 22로 튄 것을 시간 단위로 외삽한 결과였다.
REST 어댑터가 매 호출마다 연결을 열고 닫으므로 fd 가 순간적으로 하나 더 잡힌다 —
정상 동작이다.

그래서 두 가지를 고쳤다.

1. **최소 관측 시간**(기본 20분) 미만이면 기울기로 판정하지 않는다.
   관측이 짧다는 사실을 결과에 남긴다.
2. fd 는 기울기와 **절대 증가량**을 함께 본다. 시작보다 실제로 늘어 있어야 한다.
3. RSS 도 같다. 기울기와 **절대 증가량**(`RSS_ABSOLUTE_MIN_MB`)을 함께 본다.
   fd 에는 이 보호를 넣어 뒀으면서 RSS 에는 안 넣어 둔 탓에 첫 자동 실행이 거짓 양성으로 실패했다.

   **그런데 이 보호는 관측 창이 짧으면 판정 자체를 불가능하게 만든다.** 임계 5MB/h 짜리 누수가
   절대 하한 10MB 를 넘으려면 최소 2시간이 필요하다. 25분 관측에서 기울기가 +8.6MB/h 로 나와도
   실제 증가는 3.4MB 라 하한에 안 걸리고 통과한다 — 그건 "누수가 없다"가 아니라 **"이 길이로는
   모른다"** 이다. 그래서 창이 임계에 도달할 수 없으면 결과에 그렇게 적는다(`floor_reachable`).
4. 워밍업 구간(기본 앞 5분)은 기울기 계산에서 뺀다.

측정 도구가 거짓 양성을 내면 사람이 결과를 안 믿게 된다. 검사기의 오탐과 같은 문제다.

**아무것도 관측 못 했으면 실패다.**
2026-09-25 에 스택이 안 뜬 채로 이 하네스를 돌렸더니 "0개 서비스 전부 임계 이내" 를 찍고
종료 코드 0 으로 나갔다. 감시가 아무것도 안 봤는데 통과라고 말한 것이다. 이 저장소가
반복해서 싸운 유형(결함 26: 장애 주입 테스트가 안 돌고 통과)과 같다.
관측한 서비스가 하나도 없으면 실패로 끝낸다.

임계를 넘으면 종료 코드 1 로 나간다. CI 야간 작업이나 배포 전 검증에 쓴다.

    python bench/soak.py --minutes 60 --interval 30 --out docs/data/soak.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request

SERVICES = [
    ("feedd", 9100), ("feedd-krx", 9200), ("tcp-gateway", 9111),
    ("ws-gateway", 9102), ("rest-api", 9103), ("writer", 9104),
    ("strategy", 9105), ("quality", 9106),
    # 멀티캐스트 발행자는 선택 서비스다. 안 떠 있으면 아래에서 그냥 건너뛴다.
    ("mcast-publisher", 9132),
]

# 이 목록에 없는 프로세스는 감시 밖이다. C++ 서비스(tcp-gateway 의 C++ 판, mcast-publisher)는
# 한동안 /healthz 에 resources 를 안 내고 있어서, 목록에 있어도 값이 0 으로만 읽혔다.
# 누수가 가장 보기 어려운 쪽(GC 가 없는 쪽)이 감시 밖이었던 셈이다. 2026-09-24 에 메웠다.

RSS_GROWTH_LIMIT_MB_H = 5.0
FD_GROWTH_LIMIT_H = 1.0
FD_ABSOLUTE_MIN = 2          # 기울기만으로 판정하지 않는다 (docstring 참고)
# RSS 도 마찬가지다. 25분 관측에서 3MB 오른 것을 "시간당 7MB" 로 외삽해 실패시키면
# 사람이 결과를 안 믿게 된다. 기울기와 절대 증가가 **둘 다** 넘어야 누수로 본다.
RSS_ABSOLUTE_MIN_MB = 10.0
THROUGHPUT_FLOOR = 0.5
MIN_MINUTES_FOR_SLOPE = 20.0  # 이보다 짧으면 시간당 외삽이 노이즈를 증폭한다
WARMUP_MINUTES = 5.0          # 시동 구간. 기울기 계산에서 뺀다 (docstring 참고)


def poll(port: int) -> dict | None:
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/healthz", timeout=4) as r:
            return json.loads(r.read())
    except Exception:                                # noqa: BLE001
        return None


def slope(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def main() -> int:
    ap = argparse.ArgumentParser("soak")
    ap.add_argument("--minutes", type=float, default=60.0)
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--out", default="")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--warmup-minutes", type=float, default=WARMUP_MINUTES,
                    help="앞쪽 이 시간은 기울기 계산에서 뺀다 (시동 구간)")
    ap.add_argument("--min-minutes-for-slope", type=float, default=MIN_MINUTES_FOR_SLOPE,
                    help="이보다 짧은 관측에서는 기울기로 판정하지 않는다")
    args = ap.parse_args()
    slope_valid = args.minutes >= args.min_minutes_for_slope

    series: dict[str, list[tuple[float, float, int, int]]] = {}
    t0 = time.time()
    deadline = t0 + args.minutes * 60
    tick = 0

    print(f"장시간 감시 {args.minutes:.0f}분 · {args.interval:.0f}초 간격\n")
    while time.time() < deadline:
        tick += 1
        now = time.time()
        row = []
        for name, port in SERVICES:
            d = poll(port)
            if not d:
                continue
            r = d.get("resources") or {}
            if not r:
                # resources 를 안 내는 서비스는 "0MB/0fd" 로 기록되어 누수가 안 보인다.
                # 조용히 0 으로 세지 말고 건너뛴다 — 없는 값을 있는 값처럼 다루면 안 된다.
                continue
            frames = d.get("frames_in") or d.get("seq") or 0
            series.setdefault(name, []).append(
                (now, r.get("rss_mb", 0.0), r.get("fd_open", 0), frames))
            row.append(f"{name} {r.get('rss_mb', 0):.0f}M/{r.get('fd_open', 0)}fd")
        if not args.quiet:
            print(f"  [{(now - t0) / 60:5.1f}분] " + "  ".join(row[:5]))
        remain = min(args.interval, deadline - time.time())
        if remain > 0:
            time.sleep(remain)

    # ── 판정 ──────────────────────────────────────────────────────────────
    results, failed = [], []
    print(f"\n{'SERVICE':<13} {'RSS 시작→끝':>16} {'MB/h':>8} {'fd':>10} {'/h':>7} {'처리량':>10}")
    print("-" * 74)
    for name, rows in series.items():
        if len(rows) < 4:
            continue
        base = rows[0][0]
        xs = [(r[0] - base) / 3600.0 for r in rows]
        # 워밍업 이후 구간으로만 기울기를 낸다. 시동을 누수로 읽지 않기 위해서다.
        t0 = rows[0][0]
        judged = [r for r in rows if (r[0] - t0) / 60.0 >= args.warmup_minutes] or rows
        jxs = [(r[0] - judged[0][0]) / 3600.0 for r in judged]
        rss_slope = slope(jxs, [r[1] for r in judged])
        fd_slope = slope(jxs, [float(r[2]) for r in judged])
        # 처리량: 마지막 절반 구간의 초당 프레임 vs 첫 절반
        half = len(rows) // 2
        def rate(seg):
            dt = seg[-1][0] - seg[0][0]
            return (seg[-1][3] - seg[0][3]) / dt if dt > 0 else 0.0
        r_early, r_late = rate(rows[:half + 1]), rate(rows[half:])
        retained = (r_late / r_early) if r_early > 0 else 1.0

        fd_delta = rows[-1][2] - rows[0][2]
        rss_delta = rows[-1][1] - rows[0][1]

        bad = []
        if slope_valid:
            if rss_slope > RSS_GROWTH_LIMIT_MB_H and rss_delta >= RSS_ABSOLUTE_MIN_MB:
                bad.append(f"RSS +{rss_slope:.1f}MB/h (실제 +{rss_delta:.1f}MB)")
            # 기울기만으로 판정하지 않는다 — 짧은 흔들림이 외삽되면 거짓 양성이 난다
            if fd_slope > FD_GROWTH_LIMIT_H and fd_delta >= FD_ABSOLUTE_MIN:
                bad.append(f"fd +{fd_slope:.1f}/h (실제 +{fd_delta})")
        if r_early > 1 and retained < THROUGHPUT_FLOOR:
            bad.append(f"처리량 {retained * 100:.0f}%")
        if bad:
            failed.append((name, bad))

        results.append({
            "service": name, "samples": len(rows),
            # **원시 표본을 남긴다.** 예전엔 개수만 적었다. 그러면 판정 기준이 바뀌었을 때
            # 과거 실행을 다시 볼 수 없고, 보고서를 그냥 믿는 수밖에 없다. 실제로 2026-09-25 에
            # 워밍업 제외·RSS 절대 하한을 넣고 나서 직전 실행을 재판정할 수 없었다.
            # (t 는 관측 시작 기준 초, rss 는 MB)
            "rows": [{"t": round(r[0] - rows[0][0], 1), "rss_mb": round(r[1], 2),
                      "fd": r[2], "frames": r[3]} for r in rows],
            "rss_start_mb": rows[0][1], "rss_end_mb": rows[-1][1],
            "rss_growth_mb_per_hour": round(rss_slope, 2),
            "fd_start": rows[0][2], "fd_end": rows[-1][2],
            "fd_growth_per_hour": round(fd_slope, 2),
            "throughput_retained": round(retained, 3),
            "rss_delta_mb": round(rss_delta, 1), "fd_delta": fd_delta,
            "slope_judged": slope_valid,
            "verdict": "FAIL" if bad else ("OK" if slope_valid else "OK(관측 부족)"),
            "issues": bad,
        })
        mark = "!" if bad else " "
        print(f"{mark}{name:<12} {rows[0][1]:>6.1f} → {rows[-1][1]:<6.1f}M "
              f"{rss_slope:>+8.2f} {rows[0][2]:>4} → {rows[-1][2]:<3} "
              f"{fd_slope:>+6.2f} {retained * 100:>9.0f}%")

    print("-" * 74)
    print(f"임계: RSS {RSS_GROWTH_LIMIT_MB_H}MB/h (절대 +{RSS_ABSOLUTE_MIN_MB:.0f}MB 이상 동반) · "
          f"fd {FD_GROWTH_LIMIT_H}/h (절대 +{FD_ABSOLUTE_MIN} 이상 동반) · 처리량 {THROUGHPUT_FLOOR * 100:.0f}%")
    print(f"기울기는 앞 {args.warmup_minutes:.0f}분(시동)을 뺀 구간으로 낸다.")
    # 이 관측 창에서 임계(기울기)가 절대 하한에 도달할 수 있는가.
    # 도달할 수 없으면 "통과" 는 "누수 없음" 이 아니라 "이 길이로는 모름" 이다.
    hours = args.minutes / 60.0
    floor_reachable = RSS_GROWTH_LIMIT_MB_H * max(hours - args.warmup_minutes / 60.0, 0) >= RSS_ABSOLUTE_MIN_MB
    if not floor_reachable:
        need_h = RSS_ABSOLUTE_MIN_MB / RSS_GROWTH_LIMIT_MB_H + args.warmup_minutes / 60.0
        print(f"\n주의: 관측 {args.minutes:.0f}분으로는 RSS 누수 판정이 성립하지 않는다.\n"
              f"  임계 {RSS_GROWTH_LIMIT_MB_H:.0f}MB/h 짜리 누수가 절대 하한 {RSS_ABSOLUTE_MIN_MB:.0f}MB 를 넘으려면 "
              f"최소 {need_h * 60:.0f}분이 필요하다.\n"
              f"  이 실행이 보증하는 것은 fd 누수 없음·처리량 유지·급격한 메모리 증가 없음까지다.")
    if not slope_valid:
        print(f"관측 {args.minutes:.0f}분 < {args.min_minutes_for_slope:.0f}분 — "
              f"시간당 기울기는 참고용이고 판정에 쓰지 않았다.\n"
              f"짧은 표본을 시간 단위로 외삽하면 한 번의 흔들림이 거짓 양성이 된다.")
    if failed:
        print("\n임계 초과:")
        for name, bad in failed:
            print(f"  {name}: {', '.join(bad)}")
    elif not results:
        # 감시가 아무것도 안 봤는데 "통과" 라고 말하면 안 된다.
        print("\n관측된 서비스가 없다 — 스택이 떠 있는지, 포트가 맞는지 확인한다.\n"
              "아무것도 안 본 감시는 통과가 아니다.")
    else:
        note = "" if slope_valid else " — 단 관측이 짧아 기울기 판정은 보류"
        print(f"\n{len(results)}개 서비스 전부 임계 이내 "
              f"({args.minutes:.0f}분 관측){note}")

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump({
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "duration_minutes": args.minutes,
                "interval_s": args.interval,
                "thresholds": {
                    "rss_growth_mb_per_hour": RSS_GROWTH_LIMIT_MB_H,
                    "rss_absolute_min_mb": RSS_ABSOLUTE_MIN_MB,
                    "warmup_minutes": args.warmup_minutes,
                    "fd_growth_per_hour": FD_GROWTH_LIMIT_H,
                    "throughput_retained": THROUGHPUT_FLOOR},
                "min_minutes_for_slope": args.min_minutes_for_slope,
                # 이 창에서 RSS 누수 판정이 성립하는가. false 면 "통과 = 누수 없음" 이 아니다.
                "rss_verdict_reachable": floor_reachable,
                "slope_judged": slope_valid,
                "services": results,
                "observed_services": len(results),
                "passed": bool(results) and not failed,
            }, fh, ensure_ascii=False, indent=1)
        print(f"저장: {args.out}")
    if not results:
        return 2      # 임계 초과(1)와 구분한다 — 원인이 다르면 종료 코드도 달라야 한다
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
