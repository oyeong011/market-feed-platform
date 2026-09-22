// MDFP/1 — src/mdfeed/protocol.py 와 바이트 단위로 호환되는 C++ 구현 (헤더 전용, 의존성 0).
//
// 프레임 레이아웃 (빅엔디언, 헤더 20바이트) 은 파이썬 쪽 docstring 이 원본이다:
//     +0  magic 4B "MDF1" | +4 version 1B | +5 msg_type 1B | +6 flags 2B
//     +8  seq 8B          | +16 length 4B | +20 payload NB  | crc32 4B (헤더+페이로드)
//
// 파이썬 구현과 **동작이 같아야 하는 것들** (tests/test_cpp_conformance.py 가 양방향으로 고정):
//   * 파서 상태 전이: 덜 왔으면 기다림 / MAGIC·버전·길이·CRC 가 깨지면 다음 MAGIC 까지 건너뛰어 재동기화
//   * 재동기화 직후 멈추지 않고 계속 파싱 (파이썬 _RETRY 분기 — CRC 후 영구 정지 결함의 수정)
//   * MAGIC 을 못 찾으면 꼬리 3바이트만 남김 (다음 청크에 걸친 MAGIC 을 놓치지 않기 위해)
//   * 카운터: resync_count, crc_error_count
//
// 파이썬과 다른 점은 API 뿐이다. 프레임은 파서 내부 버퍼를 가리키는 뷰로 콜백에 전달되며
// 콜백이 끝나면 무효가 된다. 복사가 필요하면 콜백 안에서 한다.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "crc32.hpp"

namespace mdfp {

inline constexpr std::array<uint8_t, 4> MAGIC{'M', 'D', 'F', '1'};
inline constexpr uint8_t VERSION = 1;
inline constexpr size_t HEADER_SIZE = 20;
inline constexpr size_t CRC_SIZE = 4;
inline constexpr size_t MAX_PAYLOAD = size_t{1} << 20;   // 1MB. 넘으면 스트림이 깨진 것으로 본다

enum MsgType : uint8_t {
    MSG_HEARTBEAT = 1, MSG_TRADE = 2, MSG_BOOK = 3, MSG_SIGNAL = 4,
    MSG_SNAPSHOT = 5, MSG_SUBSCRIBE = 6, MSG_ACK = 7,
};
inline constexpr uint16_t FLAG_SNAPSHOT = 1u << 0;
inline constexpr uint16_t FLAG_COMPRESSED = 1u << 1;

struct ProtocolError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// ── 빅엔디언 도우미 ─────────────────────────────────────────────────────────
inline void put_be16(uint8_t* p, uint16_t v) { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v); }
inline void put_be32(uint8_t* p, uint32_t v) { for (int i = 0; i < 4; ++i) p[i] = uint8_t(v >> (24 - 8 * i)); }
inline void put_be64(uint8_t* p, uint64_t v) { for (int i = 0; i < 8; ++i) p[i] = uint8_t(v >> (56 - 8 * i)); }
inline uint16_t get_be16(const uint8_t* p) { return uint16_t((uint16_t(p[0]) << 8) | p[1]); }
inline uint32_t get_be32(const uint8_t* p) { uint32_t v = 0; for (int i = 0; i < 4; ++i) v = (v << 8) | p[i]; return v; }
inline uint64_t get_be64(const uint8_t* p) { uint64_t v = 0; for (int i = 0; i < 8; ++i) v = (v << 8) | p[i]; return v; }
inline double get_be_f64(const uint8_t* p) { uint64_t u = get_be64(p); double d; std::memcpy(&d, &u, 8); return d; }
inline void put_be_f64(uint8_t* p, double d) { uint64_t u; std::memcpy(&u, &d, 8); put_be64(p, u); }

// ── 인코딩 ──────────────────────────────────────────────────────────────────
// out 뒤에 프레임 하나를 덧붙인다. hot path 에서는 out 을 재사용해 할당을 피한다.
inline size_t encode_into(std::vector<uint8_t>& out, uint8_t msg_type, uint64_t seq,
                          const uint8_t* payload, size_t n, uint16_t flags = 0) {
    if (n > MAX_PAYLOAD) throw ProtocolError("payload too large: " + std::to_string(n));
    const size_t start = out.size();
    out.resize(start + HEADER_SIZE + n + CRC_SIZE);
    uint8_t* h = out.data() + start;
    std::memcpy(h, MAGIC.data(), 4);
    h[4] = VERSION;
    h[5] = msg_type;
    put_be16(h + 6, flags);
    put_be64(h + 8, seq);
    put_be32(h + 16, uint32_t(n));
    if (n) std::memcpy(h + HEADER_SIZE, payload, n);
    put_be32(h + HEADER_SIZE + n, crc32(h, HEADER_SIZE + n));
    return HEADER_SIZE + n + CRC_SIZE;
}

inline std::vector<uint8_t> encode(uint8_t msg_type, uint64_t seq,
                                   const uint8_t* payload = nullptr, size_t n = 0, uint16_t flags = 0) {
    std::vector<uint8_t> out;
    out.reserve(HEADER_SIZE + n + CRC_SIZE);
    encode_into(out, msg_type, seq, payload, n, flags);
    return out;
}

inline std::vector<uint8_t> heartbeat(uint64_t seq, uint64_t ts_ns) {
    uint8_t p[8];
    put_be64(p, ts_ns);
    return encode(MSG_HEARTBEAT, seq, p, 8);
}

// ── 프레임 뷰 ───────────────────────────────────────────────────────────────
struct FrameView {
    uint8_t msg_type = 0;
    uint16_t flags = 0;
    uint64_t seq = 0;
    const uint8_t* payload = nullptr;   // 파서 버퍼를 가리킨다. 콜백 밖에서는 무효.
    uint32_t length = 0;
};

// ── 스트리밍 파서 ───────────────────────────────────────────────────────────
class FrameParser {
public:
    uint64_t resync_count = 0;
    uint64_t crc_error_count = 0;
    std::function<void()> on_resync;

    // 완성된 프레임마다 on_frame(const FrameView&) 를 부른다. 남은 조각은 내부에 보관.
    template <class F>
    void feed(const uint8_t* data, size_t n, F&& on_frame) {
        buf_.insert(buf_.end(), data, data + n);
        for (;;) {
            FrameView f;
            const St st = try_one(f);
            if (st == St::NeedMore) break;
            if (st == St::Retry) continue;   // 재동기화 직후. 여기서 멈추면 세션이 영구히 멎는다
            // 콜백보다 먼저 소비 처리한다. 콜백이 던져도 같은 프레임이 다시 배달되지 않는다.
            // 뷰는 compact() 전까지 유효하므로 콜백 안에서는 안전하다.
            rd_ += HEADER_SIZE + f.length + CRC_SIZE;
            try { on_frame(static_cast<const FrameView&>(f)); }
            catch (...) { compact(); throw; }
        }
        compact();
    }

    size_t buffered() const { return buf_.size() - rd_; }

private:
    enum class St { Ok, NeedMore, Retry };
    std::vector<uint8_t> buf_;
    size_t rd_ = 0;   // 논리적 버퍼 시작. 매 프레임마다 앞을 지우면 O(n) memmove 라 지연시킨다

    St try_one(FrameView& out) {
        const size_t avail = buf_.size() - rd_;
        if (avail < HEADER_SIZE) return St::NeedMore;
        const uint8_t* h = buf_.data() + rd_;
        if (std::memcmp(h, MAGIC.data(), 4) != 0) return resync();
        const uint8_t ver = h[4];
        const uint32_t length = get_be32(h + 16);
        if (ver != VERSION || length > MAX_PAYLOAD) return resync();
        const size_t total = HEADER_SIZE + length + CRC_SIZE;
        if (avail < total) return St::NeedMore;   // 아직 덜 왔다
        const uint32_t want = get_be32(h + HEADER_SIZE + length);
        if (want != crc32(h, HEADER_SIZE + length)) {
            ++crc_error_count;
            return resync();
        }
        out.msg_type = h[5];
        out.flags = get_be16(h + 6);
        out.seq = get_be64(h + 8);
        out.length = length;
        out.payload = h + HEADER_SIZE;
        return St::Ok;
    }

    // 다음 MAGIC 경계로 점프. 못 찾으면 꼬리 3바이트만 남긴다 (파이썬 _resync 와 동일).
    St resync() {
        const size_t avail = buf_.size() - rd_;
        const uint8_t* base = buf_.data() + rd_;
        size_t idx = npos;
        for (size_t i = 1; i + 4 <= avail; ++i) {
            if (std::memcmp(base + i, MAGIC.data(), 4) == 0) { idx = i; break; }
        }
        if (idx == npos) {
            const size_t keep = MAGIC.size() - 1;
            if (avail > keep) rd_ += avail - keep;
        } else {
            rd_ += idx;
        }
        ++resync_count;
        if (on_resync) on_resync();
        return idx != npos ? St::Retry : St::NeedMore;
    }

    void compact() {
        if (rd_ == 0) return;
        if (rd_ == buf_.size()) { buf_.clear(); rd_ = 0; return; }
        if (rd_ >= 4096 && rd_ * 2 >= buf_.size()) {
            buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(rd_));
            rd_ = 0;
        }
    }

    static constexpr size_t npos = static_cast<size_t>(-1);
};

// ── 구독자 측 갭 탐지 ───────────────────────────────────────────────────────
class SequenceTracker {
public:
    std::optional<uint64_t> expected;
    uint64_t gap_count = 0;
    uint64_t lost_messages = 0;
    uint64_t duplicate_count = 0;

    // 이번 프레임에서 유실된 개수 (0 이면 정상)
    uint64_t observe(uint64_t seq) {
        if (!expected) { expected = seq + 1; return 0; }
        if (seq == *expected) { ++*expected; return 0; }
        if (seq < *expected) { ++duplicate_count; return 0; }
        const uint64_t lost = seq - *expected;
        ++gap_count;
        lost_messages += lost;
        expected = seq + 1;
        return lost;
    }
};

// ── 페이로드 모델 (models.py 의 struct 포맷과 동일) ─────────────────────────
inline std::string unfix(const uint8_t* p, size_t n) {
    size_t len = n;
    while (len && p[len - 1] == 0) --len;
    return std::string(reinterpret_cast<const char*>(p), len);
}
inline void fix(uint8_t* dst, size_t n, std::string_view s) {
    // 파이썬 _fix 와 동일: n 바이트에서 자르되 UTF-8 문자 경계까지 물러난다.
    // (KRX 업종명 같은 한글 심볼에서 두 구현의 바이트·CRC 가 같아야 한다)
    size_t len = s.size() < n ? s.size() : n;
    if (len < s.size()) {
        while (len && (uint8_t(s[len - 1]) & 0xC0) == 0x80) --len;   // 이어지는 바이트 제거
        if (len && (uint8_t(s[len - 1]) & 0xC0) == 0xC0) --len;      // 잘린 선두 바이트 제거
    }
    std::memset(dst, 0, n);
    std::memcpy(dst, s.data(), len);
}

struct Trade {   // "!16s8sQQddB7x" = 64B
    static constexpr size_t SIZE = 64;
    std::string venue, symbol;
    uint64_t ts_event_ns = 0, ts_recv_ns = 0;
    double price = 0, qty = 0;
    uint8_t side = 0;

    static Trade unpack(const uint8_t* p, size_t n) {
        if (n < SIZE) throw ProtocolError("Trade payload too short");
        Trade t;
        t.symbol = unfix(p, 16);
        t.venue = unfix(p + 16, 8);
        t.ts_event_ns = get_be64(p + 24);
        t.ts_recv_ns = get_be64(p + 32);
        t.price = get_be_f64(p + 40);
        t.qty = get_be_f64(p + 48);
        t.side = p[56];
        return t;
    }
    void pack(uint8_t* p) const {
        fix(p, 16, symbol);
        fix(p + 16, 8, venue);
        put_be64(p + 24, ts_event_ns);
        put_be64(p + 32, ts_recv_ns);
        put_be_f64(p + 40, price);
        put_be_f64(p + 48, qty);
        p[56] = side;
        std::memset(p + 57, 0, 7);
    }
};

struct BookTop {   // "!16s8sQQdddd" = 72B
    static constexpr size_t SIZE = 72;
    std::string venue, symbol;
    uint64_t ts_event_ns = 0, ts_recv_ns = 0;
    double bid = 0, bid_qty = 0, ask = 0, ask_qty = 0;

    static BookTop unpack(const uint8_t* p, size_t n) {
        if (n < SIZE) throw ProtocolError("BookTop payload too short");
        BookTop b;
        b.symbol = unfix(p, 16);
        b.venue = unfix(p + 16, 8);
        b.ts_event_ns = get_be64(p + 24);
        b.ts_recv_ns = get_be64(p + 32);
        b.bid = get_be_f64(p + 40);
        b.bid_qty = get_be_f64(p + 48);
        b.ask = get_be_f64(p + 56);
        b.ask_qty = get_be_f64(p + 64);
        return b;
    }
};

}  // namespace mdfp
