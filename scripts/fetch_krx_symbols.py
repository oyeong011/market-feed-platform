#!/usr/bin/env python3
"""KRX 상장종목 마스터 내려받기 — 실시간 피드의 종목 유니버스.

한국투자증권이 공개하는 종목 마스터(.mst)를 받아 코드·종목명·시장을 뽑는다.
상장폐지 종목이 섞이지 않은 **현재 상장 목록**이라, 재무데이터 저장소의
기업 목록보다 실시간 시세용으로 정확하다.

파일 형식은 고정폭이다. 앞쪽 9바이트가 단축코드, 그다음 12바이트가 표준코드,
**끝에서 228바이트**가 속성 블록이고 그 사이가 한글 종목명이다.
바이트 기준이라 cp949 로 디코딩하기 전에 잘라야 한다 — 한글이 2바이트이므로
문자 기준으로 자르면 종목명 끝에 속성코드가 딸려 들어온다.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import urllib.request
import zipfile

URLS = {
    "KOSPI": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
    "KOSDAQ": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
}
TRAIL_BYTES = 228          # 끝쪽 고정 속성 블록 길이


def parse(raw: bytes) -> list[tuple[str, str]]:
    rows = []
    for line in raw.split(b"\n"):
        line = line.rstrip(b"\r")
        if len(line) < 21 + TRAIL_BYTES:
            continue
        code = line[0:9].decode("cp949", "ignore").strip()
        if len(code) != 6 or not code.isdigit():
            continue
        name = line[21:len(line) - TRAIL_BYTES].decode("cp949", "ignore").strip()
        rows.append((code, name))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser("fetch_krx_symbols")
    ap.add_argument("--out", default="data/reference/krx_symbols.csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    total = 0
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["market", "code", "name"])
        for market, url in URLS.items():
            blob = urllib.request.urlopen(url, timeout=30).read()
            z = zipfile.ZipFile(io.BytesIO(blob))
            rows = parse(z.read(z.namelist()[0]))
            for c, n in rows:
                w.writerow([market, c, n])
            total += len(rows)
            print(f"  {market:<7} {len(rows):>5,}종목  예: "
                  + ", ".join(f"{c} {n}" for c, n in rows[:3]))
    print(f"저장: {args.out} ({total:,}종목)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
