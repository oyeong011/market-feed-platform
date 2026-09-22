// 파이썬 ↔ C++ 교차 검증 도구. tests/test_cpp_conformance.py 가 부른다.
//   gen   N OUT   : C++ 가 프레임 N 개를 파일로 쓴다 (파이썬이 파싱)
//   parse IN      : 파이썬이 쓴 파일을 의사난수 청크로 파싱해 한 줄씩 출력 (파이썬이 대조)
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "mdfp/protocol.hpp"

using namespace mdfp;

int main(int argc, char** argv) {
    if (argc >= 4 && std::strcmp(argv[1], "gen") == 0) {
        const size_t n = std::strtoull(argv[2], nullptr, 10);
        std::vector<uint8_t> out;
        for (size_t i = 0; i < n; ++i) {
            if (i % 7 == 6) { auto h = heartbeat(i, 1'000'000'000ull + i); out.insert(out.end(), h.begin(), h.end()); continue; }
            Trade t; t.venue = "UPBIT"; t.symbol = (i % 11 == 10) ? "\xEC\xBD\x94\xEC\x8A\xA4\xED\x94\xBC\xEB\x8C\x80\xED\x98\x95\xEC\xA3\xBC\xEC\x9A\xB0" : "KRW-BTC";   // 매 11번째는 한글 심볼(절단 호환)
            t.ts_event_ns = i * 1000; t.ts_recv_ns = i * 1000 + 5;
            t.price = 100.0 + 0.5 * double(i); t.qty = 0.1; t.side = uint8_t(i % 3);
            uint8_t p[Trade::SIZE]; t.pack(p);
            encode_into(out, MSG_TRADE, i, p, Trade::SIZE, (i % 5 == 0) ? FLAG_SNAPSHOT : 0);
        }
        std::ofstream(argv[3], std::ios::binary).write(reinterpret_cast<const char*>(out.data()), std::streamsize(out.size()));
        return 0;
    }
    if (argc >= 3 && std::strcmp(argv[1], "parse") == 0) {
        std::ifstream in(argv[2], std::ios::binary);
        std::vector<uint8_t> data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        FrameParser p; uint32_t lcg = 12345; size_t i = 0;
        while (i < data.size()) {
            lcg = lcg * 1103515245u + 12345u; size_t n = 1 + (lcg >> 16) % 97; n = std::min(n, data.size() - i);
            p.feed(data.data() + i, n, [&](const FrameView& f) {
                std::printf("%llu %u %u %u", (unsigned long long)f.seq, f.msg_type, f.flags, f.length);
                if (f.msg_type == MSG_TRADE) { auto t = Trade::unpack(f.payload, f.length);
                    std::printf(" %s %s %llu %llu %.17g %.17g %u", t.venue.c_str(), t.symbol.c_str(),
                        (unsigned long long)t.ts_event_ns, (unsigned long long)t.ts_recv_ns, t.price, t.qty, t.side); }
                else if (f.msg_type == MSG_HEARTBEAT) std::printf(" hb %llu", (unsigned long long)get_be64(f.payload));
                std::printf("\n");
            });
            i += n;
        }
        std::printf("END resync=%llu crc_errors=%llu\n", (unsigned long long)p.resync_count, (unsigned long long)p.crc_error_count);
        return 0;
    }
    std::fprintf(stderr, "usage: %s gen N OUT | parse IN\n", argv[0]);
    return 2;
}
