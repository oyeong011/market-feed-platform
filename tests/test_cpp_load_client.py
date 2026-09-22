"""C++ 부하 클라이언트(cpp/bench/load_client.cpp)가 파이썬 도구와 같은 정의로 재는지.

파이썬 발행자 → C++ 게이트웨이 → C++ 부하 클라이언트. 지연은 Trade.ts_recv_ns 기준이므로
발행 시 ts_recv_ns 를 '지금' 으로 찍어 보내면 클라이언트가 잰 p50 은 수 ms 아래여야 한다.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from mdfeed.bus import UDSPublisher
from mdfeed.models import MSG_TRADE, Trade, now_ns
from mdfeed.protocol import encode

from test_cpp_gateway import Gateway, _compiler, bus_dir, _wait, gateway_bin  # noqa: F401  (fixture 재사용)

ROOT = Path(__file__).resolve().parents[1]
CPP = ROOT / "cpp"


@pytest.fixture(scope="session")
def load_client_bin(tmp_path_factory) -> Path:
    cxx = _compiler()
    if cxx is None:
        pytest.skip("C++ compiler not found")
    out = tmp_path_factory.mktemp("cpp") / "load_client"
    proc = subprocess.run([cxx, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", f"-I{CPP / 'include'}",
                           str(CPP / "bench" / "load_client.cpp"), "-o", str(out)], capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.fail(f"C++ build failed:\n{proc.stderr}")
    return out


def test_load_client_measures_like_python_tool(gateway_bin, load_client_bin, tmp_path):
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")
    out = tmp_path / "load.json"

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        gw = Gateway(gateway_bin, bus_path)
        try:
            await _wait(lambda: pub.subscriber_count == 1, 5, "gateway on bus")
            proc = subprocess.Popen([str(load_client_bin), "--port", str(gw.port), "--admin", str(gw.admin),
                                     "--subscribers", "5", "20", "--seconds", "2", "--out", str(out)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            seq = 0
            deadline = time.time() + 6.5
            while time.time() < deadline and proc.poll() is None:
                for _ in range(20):                    # 회차 중 계속 발행. ts_recv_ns = 지금
                    t = Trade("TEST", "AAA", now_ns(), now_ns(), 100.0, 1.0, 1)
                    pub.publish(encode(MSG_TRADE, seq, t.pack())); seq += 1
                await asyncio.sleep(0.01)
            stdout, stderr = proc.communicate(timeout=10)
            return proc.returncode, stderr, seq
        finally:
            gw.proc.kill()
            await pub.close()

    rc, stderr, published = asyncio.run(main())
    assert rc == 0, stderr
    doc = json.loads(out.read_text())
    assert doc["client"].startswith("c++") and len(doc["rounds"]) == 2
    for r, n in zip(doc["rounds"], (5, 20)):
        assert r["subscribers"] == n and r["connected"] == n and r["connect_failed"] == 0
        assert r["total_messages"] > 0 and r["lost_messages"] == 0 and r["crc_errors"] == 0 and r["resyncs"] == 0
        assert r["per_sub_min"] > 0 and r["gateway_samples"] >= 3
        assert 0 <= r["latency_p50_us"] < 5_000, r          # 같은 기계, 발행 직후 도착 — 수 ms 아래
        assert r["latency_p99_us"] >= r["latency_p50_us"] and r["latency_max_us"] >= r["latency_p99_us"]
        assert r["upstream_msg_per_s"] > 0
    assert published > 0
