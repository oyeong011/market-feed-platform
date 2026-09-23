"""UDP 멀티캐스트 피드 + 복구 채널 — 유실이 정상인 전송에서 수신자가 순서를 복원하는가.

파이썬 발행자(UDS 버스) → C++ mcast_publisher → 파이썬 참조 구독자(mcast_client).
발행자의 결정적 유실 주입(MDFEED_MCAST_DROP_EVERY)으로 복구 경로가 실제로 도는지 고정한다.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from mdfeed.bus import UDSPublisher
from mdfeed.mcast_client import McastSubscriber
from mdfeed.models import MSG_TRADE, Trade, now_ns
from mdfeed.protocol import encode, heartbeat

from test_cpp_gateway import _compiler, bus_dir, _wait  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
CPP = ROOT / "cpp"


@pytest.fixture(scope="session")
def publisher_bin(tmp_path_factory) -> Path:
    cxx = _compiler()
    if cxx is None:
        pytest.skip("C++ compiler not found")
    out = tmp_path_factory.mktemp("cpp") / "mcast_publisher"
    proc = subprocess.run([cxx, "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", f"-I{CPP / 'include'}",
                           str(CPP / "src" / "mcast_publisher.cpp"), "-o", str(out)], capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.fail(f"C++ build failed:\n{proc.stderr}")
    return out


def free_udp_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class McastPub:
    def __init__(self, binary: Path, bus_path: str, group: str, udp_port: int, **overrides):
        env = {k: v for k, v in os.environ.items() if not k.startswith("MDFEED_")}
        env.update({"MDFEED_BUS_PATH": bus_path, "MDFEED_HTTP_HOST": "127.0.0.1", "MDFEED_MCAST_GROUP": group,
                    "MDFEED_MCAST_PORT": str(udp_port), "MDFEED_MCAST_RECOVERY_PORT": "0", "MDFEED_MCAST_ADMIN_PORT": "0"})
        env.update({k: str(v) for k, v in overrides.items()})
        self.proc = subprocess.Popen([str(binary)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        info = json.loads(self.proc.stdout.readline())
        assert info["event"] == "listening" and info["service"] == "mcast-publisher"
        self.recovery, self.admin, self.multicast = info["recovery_port"], info["admin_port"], info["multicast"]
        self._stderr: list[str] = []
        threading.Thread(target=lambda: self._stderr.extend(self.proc.stderr), daemon=True).start()

    def health(self) -> dict:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.admin}/healthz", timeout=3) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return json.loads(e.read())

    def kill(self):
        self.proc.kill()


def trade(i: int) -> bytes:
    return Trade("TEST", "AAA", now_ns(), now_ns(), 100.0 + i * 0.01, 1.0, 1).pack()


def _run_scenario(publisher_bin, group: str, n_frames: int, pace_every: int, pace_s: float, wait_udp_s: float = 0.0,
                  sub_iface: str = "", burst_before_client: bool = False, gap_fill_delay_s: float | None = None, **overrides):
    """구독자를 먼저 붙이고(그룹 가입 + 스냅샷) 발행한다. 배달된 seq 목록과 통계, 발행자 헬스를 돌려준다."""
    run = bus_dir()
    bus_path = os.path.join(run, "bus.sock")
    udp_port = free_udp_port()
    delivered_seqs: list[int] = []

    async def main():
        pub = UDSPublisher(bus_path, queue_size=65536)
        await pub.start()
        mp = McastPub(publisher_bin, bus_path, group, udp_port, **overrides)
        sub = None
        try:
            # 발행자 프로세스가 "연결됨"이라 해도 파이썬 버스 서버가 그 연결을 등록하기 전엔 발행이 아무에게도
            # 안 간다 (버스는 등록된 구독자에게만 큐잉한다). 양쪽 다 기다린다 — 첫 프레임을 잃는 원인이었다.
            await _wait(lambda: pub.subscriber_count == 1 and mp.health().get("sources", [{}])[0].get("connected") is True, 5, "publisher on bus")
            sub = McastSubscriber(group, udp_port, "127.0.0.1", mp.recovery, iface=sub_iface,
                                  gap_fill_delay_s=gap_fill_delay_s,
                                  on_frame=lambda f: delivered_seqs.append(f.seq) if not (f.flags & 1) else None)
            # 스냅샷 메타(next_seq) 를 받을 때까지 구독자 루프를 잠깐 돌린다
            await asyncio.to_thread(sub.run, 3.0, lambda s: s.expected is not None)
            assert sub.expected is not None, "스냅샷 메타를 못 받음"
            if wait_udp_s:
                # 멀티캐스트가 실제로 도착하는지 먼저 본다 (안 되는 호스트면 스킵)
                pub.publish(heartbeat(0, 1))
                await asyncio.sleep(0.05)
                await asyncio.to_thread(sub.run, wait_udp_s, lambda s: s.stats.datagrams > 0)
                if sub.stats.datagrams == 0:
                    return None, sub.stats, mp.health()
            seq = 0

            async def produce():
                nonlocal seq
                for i in range(n_frames):
                    pub.publish(encode(MSG_TRADE, seq, trade(i))); seq += 1
                    if pace_every and i % pace_every == 0:
                        await asyncio.sleep(pace_s)
                pub.publish(heartbeat(seq, 2)); seq += 1

            want = n_frames
            if burst_before_client:
                # 클라이언트가 한 건도 읽기 전에 전부 쏜다. 그래야 재전송 버퍼가 확실히 돌아
                # 앞쪽 유실이 복구 불가가 된다. 동시에 돌리면 빠른 기계에선 다 복구돼 시험이 우연에 기댄다
                # (CI 3.10 러너에서 실제로 그랬다).
                await produce()
                await asyncio.sleep(0.3)
                await asyncio.to_thread(sub.run, 15.0, lambda s: s.stats.trades + s.stats.unrecoverable >= want)
            else:
                producer = asyncio.create_task(produce())
                await asyncio.to_thread(sub.run, 15.0, lambda s: s.stats.trades >= want)
                await producer
            # 마지막 구간 복구가 끝나도록 잠깐 더
            await asyncio.to_thread(sub.run, 1.0, lambda s: s.stats.trades >= want and not s.pending)
            return delivered_seqs, sub.stats, mp.health()
        finally:
            if sub:
                sub.close()
            mp.kill()
            await pub.close()

    return asyncio.run(main())


def test_unicast_drops_are_recovered_in_order(publisher_bin):
    """5번째 데이터그램마다 버려도 구독자는 전부, 순서대로, 중복 없이 받는다."""
    n = 3000
    seqs, st, health = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=50, pace_s=0.002, MDFEED_MCAST_DROP_EVERY=5)
    assert health["injected_drops"] > 0, health
    assert st.trades == n, st.to_dict()
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), "배달 순서가 연속이 아니다"
    assert st.gaps_detected > 0 and st.retrans_frames > 0, st.to_dict()
    assert st.duplicates == 0 and st.unrecoverable == 0
    assert health["retrans_requests"] == st.retrans_requests and health["retrans_frames_sent"] == st.retrans_frames
    assert health["healthy"] is True and health["multicast"] is False


def _probe_multicast(publisher_bin, group: str):
    """되는 인터페이스 조합을 찾는다. 루프백 명시(맥에서 필요) → 커널 기본(리눅스 eth0) 순.
    둘 다 데이터그램이 안 돌아오면 None — 그 호스트는 멀티캐스트를 못 돌린다."""
    for pub_if, sub_if in (("127.0.0.1", "127.0.0.1"), ("", "")):
        ov = {"MDFEED_MCAST_IF": pub_if} if pub_if else {}
        seqs, st, health = _run_scenario(publisher_bin, group, 5, pace_every=1, pace_s=0.02, wait_udp_s=1.5, sub_iface=sub_if, **ov)
        if seqs is not None and health.get("send_errors", 1) == 0:
            return pub_if, sub_if
    return None


def test_real_multicast_group(publisher_bin):
    """실제 멀티캐스트 그룹. 호스트가 멀티캐스트를 못 돌리면(인터페이스 없음) 스킵한다."""
    combo = _probe_multicast(publisher_bin, "239.192.7.1")
    if combo is None:
        pytest.skip("이 호스트에서는 멀티캐스트 데이터그램이 돌아오지 않는다 (루프백·기본 인터페이스 모두 실패)")
    pub_if, sub_if = combo
    n = 1500
    seqs, st, health = _run_scenario(publisher_bin, "239.192.7.1", n, pace_every=50, pace_s=0.002, sub_iface=sub_if,
                                     MDFEED_MCAST_DROP_EVERY=7, **({"MDFEED_MCAST_IF": pub_if} if pub_if else {}))
    assert health["multicast"] is True and health["send_errors"] == 0
    assert health["injected_drops"] > 0 and st.retrans_frames > 0     # 진짜 멀티캐스트에서도 복구 경로가 돈다
    assert st.trades == n and seqs == list(range(seqs[0], seqs[0] + len(seqs)))
    assert st.unrecoverable == 0 and st.duplicates == 0


def test_gap_beyond_retrans_buffer_is_counted_not_hidden(publisher_bin):
    """재전송 버퍼(32프레임) 밖으로 밀린 구간은 복구할 수 없다 — 숨기지 말고 세고 계속 간다."""
    n = 4000
    seqs, st, health = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=0, pace_s=0.0, burst_before_client=True,
                                     MDFEED_MCAST_DROP_EVERY=3, MDFEED_MCAST_RETRANS_BUFFER=32)
    assert health["injected_drops"] > 0
    assert st.unrecoverable > 0, st.to_dict()                         # 잃은 걸 잃었다고 말한다
    assert health["retrans_unavailable"] > 0
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)       # 배달된 것은 순서대로, 중복 없이
    assert st.trades + st.unrecoverable >= n * 0.9                    # 잃은 것 + 받은 것이 발행량을 설명한다


def test_reordering_does_not_trigger_spurious_retransmission(publisher_bin):
    """순서가 잠깐 뒤바뀌는 건 유실이 아니다. 기다리면 오는 것을 다시 달라고 하면 안 된다.

    발행자가 5번째 데이터그램마다 붙들었다 다음 것 뒤에 보낸다(재배열 주입).
    갭필 지연이 없으면 수신자는 그때마다 재전송을 부른다 — 요청 대부분이 헛일이 된다.
    """
    n = 3000
    seqs, st, health = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=50, pace_s=0.002,
                                     MDFEED_MCAST_REORDER_EVERY=5)
    assert health["injected_reorders"] > 100, health          # 재배열이 실제로 들어갔다
    assert health["injected_drops"] == 0                      # 유실은 안 넣었다
    assert st.trades == n, st.to_dict()
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), "배달 순서가 연속이 아니다"
    assert st.reordered > 100, st.to_dict()                   # 뒤바뀜은 관측됐고
    assert st.unrecoverable == 0
    # 핵심: 뒤바뀜 수에 비해 재전송 요청이 거의 없어야 한다 (갭필 지연이 흡수)
    assert st.retrans_requests <= st.reordered // 10, (
        f"재배열 {st.reordered}건에 재전송 요청 {st.retrans_requests}건 — 갭필 지연이 안 먹는다")


def test_duplicates_are_counted_and_discarded(publisher_bin):
    """같은 데이터그램이 두 번 와도 두 번 배달하지 않는다. 그리고 온 사실은 센다."""
    n = 2000
    seqs, st, health = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=50, pace_s=0.002,
                                     MDFEED_MCAST_DUPLICATE_EVERY=3)
    assert health["injected_duplicates"] > 100, health
    assert st.trades == n and st.unrecoverable == 0
    assert len(seqs) == len(set(seqs)), "같은 seq 를 두 번 배달했다"
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))
    assert st.duplicates >= health["injected_duplicates"] * 0.5, st.to_dict()


def test_loss_reorder_and_duplication_together(publisher_bin):
    """실제 네트워크는 셋을 함께 준다. 그래도 순서대로·한 번씩·전부 배달돼야 한다."""
    n = 3000
    seqs, st, health = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=40, pace_s=0.002,
                                     MDFEED_MCAST_DROP_EVERY=7, MDFEED_MCAST_REORDER_EVERY=5,
                                     MDFEED_MCAST_DUPLICATE_EVERY=11)
    assert health["injected_drops"] > 0 and health["injected_reorders"] > 0 and health["injected_duplicates"] > 0
    assert st.trades == n, st.to_dict()
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))
    assert len(seqs) == len(set(seqs))
    assert st.unrecoverable == 0
    assert st.retrans_frames > 0                              # 진짜 유실은 재전송으로 메웠다


def test_without_gap_fill_delay_reordering_floods_retransmission_requests(publisher_bin):
    """갭필 지연이 없으면 뒤바뀜마다 헛요청이 나간다 — 이 시험이 그 값의 존재 이유다.

    같은 재배열 주입을 지연 0 으로 돌리면 재전송 요청이 쏟아지고, 기본값(20ms)이면 0 이 된다.
    실측(2026-09-23, 3,000프레임·5번째마다 재배열): 지연 0 → 요청 167건, 20ms → 0건.
    """
    n = 2000
    _, st0, h0 = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=50, pace_s=0.002,
                               gap_fill_delay_s=0.0, MDFEED_MCAST_REORDER_EVERY=5)
    assert h0["injected_reorders"] > 50 and h0["injected_drops"] == 0
    assert st0.retrans_requests > 20, st0.to_dict()      # 지연이 없으면 헛요청이 나간다
    assert st0.trades == n                                # 그래도 데이터는 다 온다 — 비용 문제다

    _, st1, h1 = _run_scenario(publisher_bin, "127.0.0.1", n, pace_every=50, pace_s=0.002,
                               MDFEED_MCAST_REORDER_EVERY=5)
    assert h1["injected_reorders"] > 50
    assert st1.trades == n
    assert st1.retrans_requests * 5 < st0.retrans_requests, (st0.to_dict(), st1.to_dict())
