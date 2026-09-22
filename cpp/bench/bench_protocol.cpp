// bench/latency_bench.py::bench_protocol 과 같은 방법으로 잰다. 출력은 JSON.
//   encode: 64B Trade 페이로드를 n 번 인코딩 (재사용 버퍼 / 매번 할당 둘 다)
//   parse : n 프레임짜리 스트림을 한 번에 / 1460B(MTU) 청크로 파싱
//   unpack: Trade 페이로드 n 번 디코딩
#include <chrono>
#include <cstdio>
#include <vector>

#include "mdfp/protocol.hpp"

using namespace mdfp;
using clk = std::chrono::steady_clock;
static double secs(clk::time_point a, clk::time_point b) { return std::chrono::duration<double>(b - a).count(); }
template <class T> static inline void keep(const T& v) { asm volatile("" : : "g"(&v) : "memory"); }

int main(int argc, char** argv) {
    const size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 300000;
    Trade t; t.venue = "BINANCE"; t.symbol = "BTCUSDT"; t.ts_event_ns = 1; t.ts_recv_ns = 2; t.price = 68123.45; t.qty = 0.01; t.side = 1;
    uint8_t payload[Trade::SIZE]; t.pack(payload);

    std::vector<uint8_t> buf; buf.reserve(128);
    auto s = clk::now();
    for (size_t i = 0; i < n; ++i) { buf.clear(); encode_into(buf, MSG_TRADE, i, payload, Trade::SIZE); keep(buf); }
    const double enc_reuse = secs(s, clk::now());

    s = clk::now();
    for (size_t i = 0; i < n; ++i) { auto v = encode(MSG_TRADE, i, payload, Trade::SIZE); keep(v); }
    const double enc_alloc = secs(s, clk::now());

    std::vector<uint8_t> stream; stream.reserve(n * 88);
    for (size_t i = 0; i < n; ++i) encode_into(stream, MSG_TRADE, i, payload, Trade::SIZE);

    FrameParser p; size_t count = 0;
    s = clk::now();
    p.feed(stream.data(), stream.size(), [&](const FrameView& f) { ++count; keep(f.seq); });
    const double dec_whole = secs(s, clk::now());

    FrameParser p2; size_t count2 = 0;
    s = clk::now();
    for (size_t i = 0; i < stream.size(); i += 1460)
        p2.feed(stream.data() + i, std::min<size_t>(1460, stream.size() - i), [&](const FrameView& f) { ++count2; keep(f.seq); });
    const double dec_mtu = secs(s, clk::now());

    s = clk::now();
    for (size_t i = 0; i < n; ++i) { auto tr = Trade::unpack(payload, Trade::SIZE); keep(tr.price); }
    const double unp = secs(s, clk::now());

    std::printf("{\n  \"lang\": \"c++\", \"n\": %zu,\n", n);
    std::printf("  \"encode_ns_per_msg\": %.0f, \"encode_msg_per_s\": %.0f,\n", enc_reuse / n * 1e9, n / enc_reuse);
    std::printf("  \"encode_alloc_ns_per_msg\": %.0f,\n", enc_alloc / n * 1e9);
    std::printf("  \"parse_ns_per_msg\": %.0f, \"parse_msg_per_s\": %.0f,\n", dec_whole / count * 1e9, count / dec_whole);
    std::printf("  \"parse_mtu_chunks_ns_per_msg\": %.0f,\n", dec_mtu / count2 * 1e9);
    std::printf("  \"unpack_ns_per_msg\": %.0f,\n", unp / n * 1e9);
    std::printf("  \"frame_bytes\": %zu, \"payload_bytes\": %zu, \"parsed\": %zu\n}\n", HEADER_SIZE + Trade::SIZE + CRC_SIZE, Trade::SIZE, count);
    return count == n && count2 == n ? 0 : 1;
}
