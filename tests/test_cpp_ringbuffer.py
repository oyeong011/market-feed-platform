"""파이썬 링버퍼(src/mdfeed/ringbuffer.py)와 C++ 링버퍼(cpp/include/mdfp/ringbuffer.hpp)가 같은 공유메모리를 읽고 쓰는가.

  (1) 파이썬이 만들고 push → C++ 가 붙어서 읽는다
  (2) 파이썬이 만들고 → C++ 가 push → 파이썬 리더가 읽는다 (수집은 파이썬, 소비는 C++ 인 실제 배치의 반대 방향까지)
  (3) C++ 가 만들고 push → 파이썬이 붙어서 읽는다
컴파일러가 없으면 스킵.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import time
from multiprocessing import shared_memory
from pathlib import Path

import pytest

from mdfeed.ringbuffer import RingBuffer, RingReader

from test_cpp_gateway import _compiler  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
CPP = ROOT / "cpp"


@pytest.fixture(scope="session")
def ring_conform_bin(tmp_path_factory) -> Path:
    cxx = _compiler()
    if cxx is None:
        pytest.skip("C++ compiler not found")
    out = tmp_path_factory.mktemp("cpp") / "ring_conform"
    proc = subprocess.run([cxx, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", f"-I{CPP / 'include'}",
                           str(CPP / "tests" / "ring_conform.cpp"), "-o", str(out)], capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.fail(f"C++ build failed:\n{proc.stderr}")
    return out


def _name() -> str:
    return f"mdfp_xring_{os.getpid()}_{int(time.time() * 1000) % 100000}"


def test_cpp_reads_python_ring(ring_conform_bin):
    name = _name()
    ring = RingBuffer(name, capacity=256, slot_size=64, create=True)
    try:
        for i in range(100):
            ring.push(f"m{i}".encode())
        out = subprocess.run([str(ring_conform_bin), "read", name, "100"], capture_output=True, text=True, check=True).stdout.splitlines()
        assert out[-1] == "END skipped=0 torn=0"
        assert out[:-1] == [f"m{i}" for i in range(100)]
    finally:
        ring.close()


def test_cpp_writes_into_python_ring_and_python_reads(ring_conform_bin):
    name = _name()
    ring = RingBuffer(name, capacity=256, slot_size=64, create=True)
    try:
        reader = ring.reader()                        # 현재 쓰기 위치(0)부터
        res = subprocess.run([str(ring_conform_bin), "write", name, "150"], capture_output=True, text=True, check=True)
        assert res.stdout.strip() == "write_seq=150"
        assert ring.write_seq == 150                  # C++ 가 올린 write_seq 를 파이썬이 본다
        got = []
        while len(got) < 150:
            items = reader.poll(64)
            if not items:
                break
            got += [b.decode() for b in items]
        assert got == [f"m{i}" for i in range(150)]
        assert reader.stats()["torn"] == 0 and reader.stats()["skipped"] == 0
    finally:
        ring.close()


def test_python_reads_cpp_created_ring(ring_conform_bin):
    name = _name()
    proc = subprocess.Popen([str(ring_conform_bin), "create", name, "512", "128", "300"], stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == "ready write_seq=300"
        # 파이썬은 C++ 가 만든 세그먼트에 붙는다. track=False: 리소스 트래커가 남의 세그먼트를 지우지 않게
        shm = shared_memory.SharedMemory(name=name, track=False)
        try:
            ring = RingBuffer.__new__(RingBuffer)
            ring.shm = shm
            ring._buf = shm.buf
            ring._owner = False
            import struct
            magic, ss, cap, *_ = struct.unpack_from("!4sIIIQQQ", shm.buf, 0)
            assert magic == b"MDRB" and (ss, cap) == (128, 512)
            ring.slot_size, ring.capacity = ss, cap
            ring.payload_max = ss - 12 - 8
            rd = RingReader(ring, start=0)
            got = []
            while len(got) < 300:
                items = rd.poll(64)
                if not items:
                    break
                got += [b.decode() for b in items]
            assert got == [f"m{i}" for i in range(300)]
            assert rd.stats()["torn"] == 0
        finally:
            shm.close()
    finally:
        proc.kill()
