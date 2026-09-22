// CRC-32 (IEEE 802.3, zlib.crc32 와 동일 결과) — 슬라이싱-바이-8, 표는 컴파일 시점에 생성.
//
// 왜 zlib 을 안 부르나: 파이썬 쪽과 같은 이유다. 핵심 경로 의존성 0 을 C++ 에서도 지킨다.
// 88바이트 프레임에서는 바이트 단위 표 방식도 충분하지만, 8바이트씩 처리하면
// 파서 비용에서 CRC 가 차지하는 몫이 거의 사라진다.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace mdfp {
namespace detail {

constexpr std::array<std::array<uint32_t, 256>, 8> make_crc_tables() {
    std::array<std::array<uint32_t, 256>, 8> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        t[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; ++i)
        for (int s = 1; s < 8; ++s)
            t[s][i] = (t[s - 1][i] >> 8) ^ t[0][t[s - 1][i] & 0xFFu];
    return t;
}

inline constexpr auto CRC_TABLES = make_crc_tables();

inline uint32_t load_le32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    v = __builtin_bswap32(v);
#endif
    return v;
}

}  // namespace detail

inline uint32_t crc32(const uint8_t* p, size_t n, uint32_t crc = 0) {
    const auto& T = detail::CRC_TABLES;
    crc = ~crc;
    while (n >= 8) {
        const uint32_t one = crc ^ detail::load_le32(p);
        const uint32_t two = detail::load_le32(p + 4);
        crc = T[7][one & 0xFF] ^ T[6][(one >> 8) & 0xFF] ^ T[5][(one >> 16) & 0xFF] ^ T[4][one >> 24]
            ^ T[3][two & 0xFF] ^ T[2][(two >> 8) & 0xFF] ^ T[1][(two >> 16) & 0xFF] ^ T[0][two >> 24];
        p += 8;
        n -= 8;
    }
    while (n--) crc = T[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

}  // namespace mdfp
