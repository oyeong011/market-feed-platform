// 파이썬 ↔ C++ 링버퍼 교차 검증 도구. tests/test_cpp_ringbuffer.py 가 부른다.
//   write NAME N            : 파이썬이 만든 세그먼트에 붙어 "m0".."m{N-1}" 를 push 한다
//   read  NAME N            : 파이썬이 push 한 항목을 처음(seq 0)부터 N 개 읽어 한 줄씩 출력
//   create NAME CAP SLOT N  : 세그먼트를 만들고 N 개 push 한 뒤 60초 대기 (파이썬이 읽는다)
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "mdfp/ringbuffer.hpp"

using namespace mdfp;
int main(int argc, char** argv) {
    if (argc < 4) { std::fprintf(stderr, "usage\n"); return 2; }
    const std::string mode = argv[1], name = argv[2];
    if (mode == "write") {
        RingBuffer r(name); const int n = std::atoi(argv[3]);
        for (int i = 0; i < n; ++i) { std::string s = "m" + std::to_string(i); r.push(reinterpret_cast<const uint8_t*>(s.data()), s.size()); }
        std::printf("write_seq=%llu\n", (unsigned long long)r.write_seq()); return 0;
    }
    if (mode == "read") {
        RingBuffer r(name); RingBuffer::Reader rd(r, 0); const size_t n = size_t(std::atoi(argv[3]));
        std::vector<std::vector<uint8_t>> out; while (out.size() < n && rd.poll(out, 4096)) {}
        for (auto& v : out) std::printf("%s\n", std::string(v.begin(), v.end()).c_str());
        std::printf("END skipped=%llu torn=%llu\n", (unsigned long long)rd.skipped, (unsigned long long)rd.torn); return 0;
    }
    if (mode == "create" && argc >= 6) {
        RingBuffer r(name, uint32_t(std::atoi(argv[3])), uint32_t(std::atoi(argv[4])), true); const int n = std::atoi(argv[5]);
        for (int i = 0; i < n; ++i) { std::string s = "m" + std::to_string(i); r.push(reinterpret_cast<const uint8_t*>(s.data()), s.size()); }
        std::printf("ready write_seq=%llu\n", (unsigned long long)r.write_seq()); std::fflush(stdout);
        std::this_thread::sleep_for(std::chrono::seconds(60)); return 0;
    }
    return 2;
}
