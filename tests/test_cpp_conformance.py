"""파이썬 MDFP/1 구현과 C++ 구현(cpp/include/mdfp/protocol.hpp)이 바이트 단위로 같은지 양방향 검증.

C++ 컴파일러가 없으면 스킵한다. 있으면 cpp/tests/conformance.cpp 를 빌드해
  (1) 파이썬이 쓴 스트림을 C++ 가 파싱한 결과가 파이썬 파서·Trade.unpack 과 같은지
  (2) C++ 가 쓴 스트림을 파이썬 파서가 그대로 읽는지
  (3) 오염된 스트림에서 두 구현의 재동기화 카운터와 살아남는 프레임이 같은지
를 본다. 이 테스트가 있어야 C++ 데이터 평면이 파이썬 수집기와 섞여 돌 수 있다.
"""
from __future__ import annotations

import os
import random
import shutil
import struct
import subprocess
import sys
from pathlib import Path

import pytest

from mdfeed.models import MSG_HEARTBEAT, MSG_TRADE, Trade
from mdfeed.protocol import FLAG_SNAPSHOT, HEADER_SIZE, FrameParser, encode, heartbeat

ROOT = Path(__file__).resolve().parents[1]
CPP = ROOT / "cpp"


def _compiler() -> str | None:
    for c in (os.environ.get("CXX"), "c++", "clang++", "g++"):
        if c and shutil.which(c):
            return c
    return None


@pytest.fixture(scope="session")
def conformance_bin(tmp_path_factory) -> Path:
    cxx = _compiler()
    if cxx is None:
        pytest.skip("C++ compiler not found (c++/clang++/g++)")
    out = tmp_path_factory.mktemp("cpp") / "conformance"
    cmd = [cxx, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", f"-I{CPP / 'include'}",
           str(CPP / "tests" / "conformance.cpp"), "-o", str(out)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.fail(f"C++ build failed:\n{proc.stderr}")
    return out


def _cpp_parse(binary: Path, path: Path) -> tuple[list[str], dict[str, int]]:
    proc = subprocess.run([str(binary), "parse", str(path)], capture_output=True, text=True, check=True)
    lines = proc.stdout.rstrip("\n").split("\n")
    end = lines.pop()
    assert end.startswith("END ")
    counters = {k: int(v) for k, v in (kv.split("=") for kv in end.split()[1:])}
    return lines, counters


def _py_lines(stream: bytes) -> tuple[list[str], FrameParser]:
    p = FrameParser()
    out = []
    for f in p.feed(stream):
        line = f"{f.seq} {f.msg_type} {f.flags} {len(f.payload)}"
        if f.msg_type == MSG_TRADE:
            t = Trade.unpack(f.payload)
            line += f" {t.venue} {t.symbol} {t.ts_event_ns} {t.ts_recv_ns} {t.price!r} {t.qty!r} {t.side}"
        elif f.msg_type == MSG_HEARTBEAT:
            line += f" hb {struct.unpack('!Q', f.payload)[0]}"
        out.append(line)
    return out, p


def _norm(line: str) -> str:
    # C++ 는 %.17g, 파이썬은 repr — 둘 다 왕복 정확하므로 float 로 되읽어 비교한다
    parts = line.split()
    if len(parts) >= 11 and parts[1] == str(MSG_TRADE):
        parts[8] = repr(float(parts[8]))
        parts[9] = repr(float(parts[9]))
    return " ".join(parts)


def _python_stream(n: int, seed: int = 7) -> bytes:
    rnd = random.Random(seed)
    frames = []
    for i in range(n):
        if i % 7 == 6:
            frames.append(heartbeat(i, 1_000_000_000 + i))
            continue
        sym = "코스피대형주우" if i % 11 == 10 else "KRW-BTC"     # 16B 를 넘는 한글 심볼 — 문자 경계 절단이 양쪽에서 같아야 한다
        t = Trade("UPBIT", sym, i * 1000, i * 1000 + 5, rnd.uniform(1, 1e8), rnd.uniform(0, 10), i % 3)
        frames.append(encode(MSG_TRADE, i, t.pack(), FLAG_SNAPSHOT if i % 5 == 0 else 0))
    return b"".join(frames)


def test_cpp_parses_python_stream(conformance_bin, tmp_path):
    stream = _python_stream(500)
    path = tmp_path / "py.mdf"
    path.write_bytes(stream)
    cpp_lines, counters = _cpp_parse(conformance_bin, path)
    py_lines, _ = _py_lines(stream)
    assert [_norm(l) for l in cpp_lines] == [_norm(l) for l in py_lines]
    assert counters == {"resync": 0, "crc_errors": 0}
    assert len(py_lines) == 500


def test_python_parses_cpp_stream(conformance_bin, tmp_path):
    path = tmp_path / "cpp.mdf"
    subprocess.run([str(conformance_bin), "gen", "300", str(path)], check=True)
    lines, p = _py_lines(path.read_bytes())
    assert p.resync_count == 0 and p.crc_error_count == 0
    assert [int(l.split()[0]) for l in lines] == list(range(300))
    trades = [l for l in lines if l.split()[1] == str(MSG_TRADE)]
    assert trades[0].split()[4:6] == ["UPBIT", "KRW-BTC"]
    # C++ 가 자른 한글 심볼이 파이썬 _fix/_unfix 결과와 같다
    from mdfeed.models import _fix, _unfix
    ko = [l.split()[5] for l in trades if int(l.split()[0]) % 11 == 10]
    assert ko and all(k == _unfix(_fix("코스피대형주우", 16)) for k in ko), ko[:2]
    # C++ 가 붙인 스냅샷 플래그가 파이썬 쪽에서 같은 자리로 읽힌다
    assert all(int(l.split()[2]) == (FLAG_SNAPSHOT if int(l.split()[0]) % 5 == 0 else 0) for l in trades)


def test_both_recover_identically_from_corruption(conformance_bin, tmp_path):
    stream = bytearray(_python_stream(40))
    stream[HEADER_SIZE + 5] ^= 0xFF           # 첫 프레임 페이로드 오염 (test_protocol.py 와 동일)
    stream[88 * 20 + 4] = 99                   # 21번째 프레임 버전 필드 오염
    stream[88 * 30 : 88 * 30 + 3] = b"\x00\x00\x00"   # 31번째 프레임 매직 오염
    path = tmp_path / "bad.mdf"
    path.write_bytes(bytes(stream))
    cpp_lines, counters = _cpp_parse(conformance_bin, path)
    py_lines, p = _py_lines(bytes(stream))
    assert [_norm(l) for l in cpp_lines] == [_norm(l) for l in py_lines]
    assert counters == {"resync": p.resync_count, "crc_errors": p.crc_error_count}
    assert p.crc_error_count >= 1 and p.resync_count >= 3
