// tests/test_protocol.py 를 그대로 옮긴 C++ 테스트. 테스트 프레임워크 없이 CHECK 매크로만 쓴다.
#include <cstdio>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "mdfp/protocol.hpp"

using namespace mdfp;
static int failures = 0;
#define CHECK(cond) do { if (!(cond)) { std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); ++failures; } } while (0)

static std::vector<uint8_t> mk(uint64_t seq, double price = 100.0) {
    Trade t; t.venue = "UPBIT"; t.symbol = "KRW-BTC"; t.ts_event_ns = 1; t.ts_recv_ns = 2; t.price = price; t.qty = 0.1; t.side = 0;
    uint8_t p[Trade::SIZE]; t.pack(p);
    return encode(MSG_TRADE, seq, p, Trade::SIZE);
}
static std::vector<uint64_t> seqs_of(FrameParser& p, const std::vector<uint8_t>& s, size_t chunk) {
    std::vector<uint64_t> out;
    for (size_t i = 0; i < s.size(); i += chunk)
        p.feed(s.data() + i, std::min(chunk, s.size() - i), [&](const FrameView& f) { out.push_back(f.seq); });
    return out;
}
static std::vector<uint8_t> cat(size_t n) { std::vector<uint8_t> s; for (size_t i = 0; i < n; ++i) { auto f = mk(i); s.insert(s.end(), f.begin(), f.end()); } return s; }
static std::vector<uint64_t> iota(size_t n) { std::vector<uint64_t> v; for (size_t i = 0; i < n; ++i) v.push_back(i); return v; }

int main() {
    // crc32 알려진 벡터: "123456789" → 0xCBF43926 (zlib.crc32 와 동일)
    CHECK(crc32(reinterpret_cast<const uint8_t*>("123456789"), 9) == 0xCBF43926u);

    { // test_frame_roundtrip
        auto raw = mk(42); FrameParser p; int n = 0;
        p.feed(raw.data(), raw.size(), [&](const FrameView& f) {
            ++n; CHECK(f.seq == 42); CHECK(f.msg_type == MSG_TRADE);
            CHECK(Trade::unpack(f.payload, f.length).symbol == "KRW-BTC");
        });
        CHECK(n == 1);
    }
    { // test_header_size_is_fixed
        CHECK(HEADER_SIZE == 20); CHECK(mk(0).size() == HEADER_SIZE + Trade::SIZE + CRC_SIZE);
    }
    { // test_partial_reads_reassemble — TCP 는 메시지 경계를 지켜주지 않는다
        auto s = cat(20);
        for (size_t chunk : {1, 2, 3, 7, 19, 64, 1000}) { FrameParser p; CHECK(seqs_of(p, s, chunk) == iota(20)); }
    }
    { // test_random_chunking_fuzz
        auto s = cat(200); std::mt19937 rnd(1234); FrameParser p; std::vector<uint64_t> out; size_t i = 0;
        while (i < s.size()) { size_t n = 1 + rnd() % 97; n = std::min(n, s.size() - i);
            p.feed(s.data() + i, n, [&](const FrameView& f) { out.push_back(f.seq); }); i += n; }
        CHECK(out == iota(200));
    }
    { // test_crc_detects_corruption_and_recovers — 오염된 프레임 하나만 버리고 나머지는 살아야 한다
        auto s = cat(5); s[HEADER_SIZE + 5] ^= 0xFF; FrameParser p; auto got = seqs_of(p, s, s.size());
        CHECK(p.crc_error_count == 1); CHECK(p.resync_count == 1); CHECK((got == std::vector<uint64_t>{1, 2, 3, 4}));
    }
    { // test_garbage_prefix_resyncs
        std::vector<uint8_t> s = {0x00, 'r', 'u', 'b', 'b', 'i', 's', 'h', 0xFF, 0xFE}; auto f = mk(7); s.insert(s.end(), f.begin(), f.end());
        FrameParser p; CHECK((seqs_of(p, s, s.size()) == std::vector<uint64_t>{7}));
    }
    { // test_bad_version_rejected
        auto s = mk(1); s[4] = 99; FrameParser p; CHECK(seqs_of(p, s, s.size()).empty()); CHECK(p.resync_count >= 1);
    }
    { // test_oversized_payload_refused
        bool threw = false; std::vector<uint8_t> big((size_t{1} << 21), 'x');
        try { encode(MSG_TRADE, 0, big.data(), big.size()); } catch (const ProtocolError&) { threw = true; }
        CHECK(threw);
    }
    { // test_heartbeat_carries_timestamp
        auto raw = heartbeat(9, 1234567890123ull); FrameParser p; int n = 0;
        p.feed(raw.data(), raw.size(), [&](const FrameView& f) { ++n; CHECK(f.msg_type == MSG_HEARTBEAT); CHECK(get_be64(f.payload) == 1234567890123ull); });
        CHECK(n == 1);
    }
    { // MAGIC 이 청크 경계에 걸쳐도 놓치지 않는다 (꼬리 3바이트 보존)
        auto f = mk(3); std::vector<uint8_t> s = {0xAA, 0xBB}; s.insert(s.end(), f.begin(), f.end());
        FrameParser p; std::vector<uint64_t> out;
        p.feed(s.data(), 4, [&](const FrameView& v) { out.push_back(v.seq); });          // "\xAA\xBB" + "MD"
        p.feed(s.data() + 4, s.size() - 4, [&](const FrameView& v) { out.push_back(v.seq); });
        CHECK((out == std::vector<uint64_t>{3}));
    }
    { // SequenceTracker
        SequenceTracker t; bool ok = true; for (uint64_t i = 0; i < 10; ++i) ok &= (t.observe(i) == 0);
        CHECK(ok); CHECK(t.lost_messages == 0);
        SequenceTracker g; g.observe(0); CHECK(g.observe(5) == 4); CHECK(g.gap_count == 1); CHECK(g.lost_messages == 4);
        SequenceTracker d; d.observe(0); d.observe(1); d.observe(1); CHECK(d.duplicate_count == 1); CHECK(d.gap_count == 0);
    }
    { // 긴 스트림에서 compact 가 프레임을 잃지 않는다
        auto s = cat(5000); FrameParser p; CHECK(seqs_of(p, s, 1460) == iota(5000)); CHECK(p.buffered() == 0);
    }
    { // 콜백이 던져도 같은 프레임이 다시 배달되지 않는다 (리뷰 지적 6)
        auto s = cat(3); FrameParser p; std::vector<uint64_t> seen; int calls = 0;
        auto cb = [&](const FrameView& f) { ++calls; if (calls == 1) throw std::runtime_error("boom"); seen.push_back(f.seq); };
        bool threw = false;
        try { p.feed(s.data(), s.size(), cb); } catch (const std::runtime_error&) { threw = true; }
        CHECK(threw); CHECK(calls == 1);
        p.feed(nullptr, 0, cb);                       // 남은 버퍼 이어서 처리
        CHECK((seen == std::vector<uint64_t>{1, 2})); // seq 0 은 콜백이 던졌으므로 사라지고, 재배달되지 않는다
        CHECK(p.buffered() == 0);
    }
    { // fix(): UTF-8 문자 경계 절단 — 파이썬 _fix 와 같은 바이트 (리뷰 지적 9)
        const std::string ko = "\xEC\xBD\x94\xEC\x8A\xA4\xED\x94\xBC\xEB\x8C\x80\xED\x98\x95\xEC\xA3\xBC\xEC\x9A\xB0"; // 코스피대형주우 (21B)
        uint8_t buf[16]; fix(buf, 16, ko);
        CHECK(unfix(buf, 16).size() == 15);          // 16번째 바이트는 잘린 선두 바이트 → 제거
        CHECK(unfix(buf, 16) == ko.substr(0, 15));
        uint8_t ascii[8]; fix(ascii, 8, "BINANCE-LONG"); CHECK(unfix(ascii, 8) == "BINANCE-");
    }
    if (failures) { std::fprintf(stderr, "%d failure(s)\n", failures); return 1; }
    std::puts("cpp protocol tests: all passed");
    return 0;
}
