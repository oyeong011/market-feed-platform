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

**짧은 관측에서 시간당 기울기를 내면 노이즈가 증폭된다.**
처음 4분을 돌렸을 때 `feedd-krx` 가 "fd +4.4/h" 로 걸렸다. 그런데 시작도 21개,
끝도 21개였다. 중간에 한 번 22로 튄 것을 시간 단위로 외삽한 결과였다.
REST 어댑터가 매 호출마다 연결을 열고 닫으므로 fd 가 순간적으로 하나 더 잡힌다 —
정상 동작이다.

그래서 두 가지를 고쳤다.

1. **최소 관측 시간**(기본 20분) 미만이면 기울기로 판정하지 않는다.
   관측이 짧다는 사실을 결과에 남긴다.
2. fd 는 기울기와 **절대 증가량**을 함께 본다. 시작보다 실제로 늘어 있어야 한다.

측정 도구가 거짓 양성을 내면 사람이 결과를 안 믿게 된다. 검사기의 오탐과 같은 문제다.

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
]

RSS_GROWTH_LIMIT_MB_H = 5.0
FD_GROWTH_LIMIT_H = 1.0
FD_ABSOLUTE_MIN = 2          # 기울기만으로 판정하지 않는다 (docstring 참고)
THROUGHPUT_FLOOR = 0.5
MIN_MINUTES_FOR_SLOPE = 20.0  # 이보다 짧으면 시간당 외삽이 노이즈를 증폭한다


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
        rss_slope = slope(xs, [r[1] for r in rows])
        fd_slope = slope(xs, [float(r[2]) for r in rows])
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
            if rss_slope > RSS_GROWTH_LIMIT_MB_H and rss_delta > 0:
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
    print(f"임계: RSS {RSS_GROWTH_LIMIT_MB_H}MB/h · fd {FD_GROWTH_LIMIT_H}/h "
          f"(절대 +{FD_ABSOLUTE_MIN} 이상 동반) · 처리량 {THROUGHPUT_FLOOR * 100:.0f}%")
    if not slope_valid:
        print(f"관측 {args.minutes:.0f}분 < {args.min_minutes_for_slope:.0f}분 — "
              f"시간당 기울기는 참고용이고 판정에 쓰지 않았다.\n"
              f"짧은 표본을 시간 단위로 외삽하면 한 번의 흔들림이 거짓 양성이 된다.")
    if failed:
        print("\n임계 초과:")
        for name, bad in failed:
            print(f"  {name}: {', '.join(bad)}")
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
                    "fd_growth_per_hour": FD_GROWTH_LIMIT_H,
                    "throughput_retained": THROUGHPUT_FLOOR},
                "min_minutes_for_slope": args.min_minutes_for_slope,
                "slope_judged": slope_valid,
                "services": results,
                "passed": not failed,
            }, fh, ensure_ascii=False, indent=1)
        print(f"저장: {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
