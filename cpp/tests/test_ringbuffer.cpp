// tests/test_ringbuffer.py 를 옮긴 C++ 테스트.
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "mdfp/ringbuffer.hpp"

using namespace mdfp;
static int failures = 0;
#define CHECK(cond) do { if (!(cond)) { std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); ++failures; } } while (0)
static std::vector<uint8_t> msg(int i) { std::string s = "m" + std::to_string(i); return {s.begin(), s.end()}; }

int main() {
    const std::string name = "mdfp_ring_test_" + std::to_string(getpid());
    { // push/pop 순서
        RingBuffer r(name, 64, 64, true); auto rd = r.reader();
        for (int i = 0; i < 10; ++i) r.push(msg(i).data(), msg(i).size());
        std::vector<std::vector<uint8_t>> out; rd.poll(out);
        CHECK(out.size() == 10); CHECK(out[0] == msg(0)); CHECK(out[9] == msg(9)); CHECK(rd.backlog() == 0);
    }
    { // 리더는 현재 쓰기 위치에서 시작한다 (과거는 안 본다)
        RingBuffer r(name, 64, 64, true);
        for (int i = 0; i < 5; ++i) r.push(msg(i).data(), msg(i).size());
        auto rd = r.reader(); r.push(msg(99).data(), msg(99).size());
        std::vector<std::vector<uint8_t>> out; rd.poll(out); CHECK(out.size() == 1); CHECK(out[0] == msg(99));
    }
    { // 생산자는 절대 블로킹하지 않고 느린 소비자를 추월한다 — 소비자는 건너뛴 수를 안다
        RingBuffer r(name, 16, 64, true); auto rd = r.reader();
        for (int i = 0; i < 40; ++i) r.push(msg(i).data(), msg(i).size());
        std::vector<std::vector<uint8_t>> out; rd.poll(out, 1000);
        CHECK(rd.skipped == 24); CHECK(out.size() == 16); CHECK(out[0] == msg(24)); CHECK(out[15] == msg(39));
    }
    { // 여러 리더가 독립적이다
        RingBuffer r(name, 64, 64, true); auto a = r.reader(), b = r.reader();
        for (int i = 0; i < 3; ++i) r.push(msg(i).data(), msg(i).size());
        std::vector<std::vector<uint8_t>> oa, ob; a.poll(oa); CHECK(oa.size() == 3); b.poll(ob); CHECK(ob.size() == 3);
    }
    { // 슬롯보다 큰 페이로드는 거부
        RingBuffer r(name, 8, 64, true); std::vector<uint8_t> big(100, 'x'); bool threw = false;
        try { r.push(big.data(), big.size()); } catch (const std::invalid_argument&) { threw = true; }
        CHECK(threw);
    }
    { // 기존 세그먼트에 붙기 + 다른 프로세스처럼 스레드에서 읽기
        RingBuffer w(name, 1024, 128, true); RingBuffer a(name); CHECK(a.capacity() == 1024); CHECK(a.slot_size() == 128);
        auto rd = a.reader(); size_t got = 0;
        std::thread t([&] { std::vector<std::vector<uint8_t>> out; for (int k = 0; k < 2000 && got < 500; ++k) { got += rd.poll(out, 64); std::this_thread::yield(); } });
        for (int i = 0; i < 500; ++i) w.push(msg(i).data(), msg(i).size());
        t.join(); CHECK(got == 500); CHECK(rd.torn == 0);
    }
    if (failures) { std::fprintf(stderr, "%d failure(s)\n", failures); return 1; }
    std::puts("cpp ringbuffer tests: all passed"); return 0;
}
