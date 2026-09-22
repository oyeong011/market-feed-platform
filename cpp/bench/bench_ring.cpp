// bench/latency_bench.py::bench_ringbuffer 와 같은 방법: 64B 페이로드 n 번 push, 그 뒤 poll 로 다 읽는다.
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>
#include <unistd.h>

#include "mdfp/ringbuffer.hpp"
using namespace mdfp; using clk = std::chrono::steady_clock;
int main(int argc, char** argv) {
    const size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 300000;
    RingBuffer r("mdfp_ring_bench_" + std::to_string(getpid()), 65536, 128, true); auto rd = r.reader();
    std::vector<uint8_t> payload(64, 0x5a);
    auto s = clk::now(); for (size_t i = 0; i < n; ++i) r.push(payload.data(), payload.size()); const double push = std::chrono::duration<double>(clk::now() - s).count();
    std::vector<std::vector<uint8_t>> out; out.reserve(65536);
    s = clk::now(); size_t got = 0; while (got < 65536) { size_t k = rd.poll(out, 4096); if (!k) break; got += k; } const double poll = std::chrono::duration<double>(clk::now() - s).count();
    std::printf("{\"lang\": \"c++\", \"push_ns_per_msg\": %.0f, \"push_msg_per_s\": %.0f, \"poll_msg_per_s\": %.0f, \"capacity\": 65536, \"consumed\": %zu, \"skipped_by_lap\": %llu, \"torn_reads\": %llu}\n",
        push / n * 1e9, n / push, got / poll, got, (unsigned long long)rd.skipped, (unsigned long long)rd.torn);
    return 0;
}
