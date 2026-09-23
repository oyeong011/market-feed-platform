// 멀티캐스트 부하 클라이언트 — "구독자 수가 늘어도 발행 비용이 같은가" 를 재는 도구.
//
// TCP 게이트웨이는 구독자마다 send() 를 부른다(실측: 1,000명에서 초당 15만 회, 365MB/s).
// 멀티캐스트는 한 번 쏘면 끝이라는 게 설계상의 주장인데, 주장은 재야 사실이 된다.
// 이 도구는 같은 그룹에 소켓 N 개를 가입시키고, 발행자의 /healthz 에서 **발행 측 비용**을 읽는다.
//   * 발행 측: datagrams_sent, bytes_sent (구독자 수와 무관해야 한다)
//   * 수신 측: 구독자별 도착 프레임·유실·지연 (여기는 커널이 N 번 복제하므로 N 에 비례한다)
//
// 정직하게 적어 둘 것: 한 호스트에서 N 개 소켓이 같은 그룹에 가입하면 커널이 N 번 복제한다.
// 실제 배포에서 이 복제는 스위치가 한다 — 발행 호스트의 비용은 그대로 1회다.
// 이 도구가 증명하는 건 **발행 측 비용의 평탄함**이고, 수신 측 N 배는 로컬 측정의 성질이다.
//
// 스냅샷·복구 채널은 쓰지 않는다. 순수 UDP 팬아웃 비용만 보려는 것이고, 갭은 seq 로 센다.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "mdfp/protocol.hpp"

using namespace mdfp;
namespace {

double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
uint64_t sys_now_ns() { return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

std::string http_get(const std::string& host, int port, const std::string& path) {
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return "";
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(uint16_t(port)); inet_pton(AF_INET, host.c_str(), &a.sin_addr);
    timeval tv{3, 0}; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv); setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    if (connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0) { close(fd); return ""; }
    std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
    if (send(fd, req.data(), req.size(), 0) < 0) { close(fd); return ""; }
    std::string out; char buf[8192]; ssize_t n;
    while ((n = recv(fd, buf, sizeof buf, 0)) > 0) out.append(buf, size_t(n));
    close(fd);
    auto p = out.find("\r\n\r\n"); return p == std::string::npos ? "" : out.substr(p + 4);
}
double json_num(const std::string& j, const std::string& key, double dflt = -1) {
    auto k = j.find("\"" + key + "\""); if (k == std::string::npos) return dflt;
    auto c = j.find(':', k); if (c == std::string::npos) return dflt;
    size_t i = c + 1; while (i < j.size() && j[i] == ' ') ++i;
    if (i < j.size() && j.compare(i, 4, "null") == 0) return dflt;
    return std::strtod(j.c_str() + i, nullptr);
}

struct Sub { int fd = -1; FrameParser parser; SequenceTracker track; uint64_t frames = 0, datagrams = 0, bytes = 0; };

double pct(std::vector<double>& xs, double q) {
    if (xs.empty()) return 0.0;
    size_t i = std::min(size_t(double(xs.size()) * q / 100.0), xs.size() - 1);
    return xs[i];
}

}  // namespace

int main(int argc, char** argv) {
    std::string group = "239.192.0.1", iface = "", host = "127.0.0.1", out;
    int port = 9130, admin = 9132, gap_s = 2; double seconds = 8; std::vector<int> counts;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--group") group = next(); else if (a == "--port") port = std::atoi(next().c_str());
        else if (a == "--admin") admin = std::atoi(next().c_str()); else if (a == "--admin-host") host = next();
        else if (a == "--iface") iface = next(); else if (a == "--seconds") seconds = std::atof(next().c_str());
        else if (a == "--gap") gap_s = std::atoi(next().c_str()); else if (a == "--out") out = next();
        else if (a == "--subscribers") { while (i + 1 < argc && argv[i + 1][0] != '-') counts.push_back(std::atoi(argv[++i])); }
        else { std::fprintf(stderr, "usage: %s [--group G] [--port P] [--admin A] [--iface IP] [--subscribers N...] [--seconds S] [--out FILE]\n", argv[0]); return 2; }
    }
    if (counts.empty()) counts = {1, 10, 100, 500};

    std::fprintf(stderr, "그룹 %s:%d · 회차당 %.0f초 · 발행 측 비용은 %s:%d/healthz 에서 읽는다\n\n", group.c_str(), port, seconds, host.c_str(), admin);
    std::fprintf(stderr, "%7s %7s %12s %12s %12s %10s %10s %7s\n", "구독자", "가입", "발행 dgram/s", "발행 MB/s", "수신 frame/s", "p50", "p99", "유실");
    std::string rounds;
    for (size_t ci = 0; ci < counts.size(); ++ci) {
        const int n = counts[ci];
        std::vector<Sub> subs; subs.resize(size_t(n));
        int joined = 0;
        for (auto& s : subs) {
            int fd = socket(AF_INET, SOCK_DGRAM, 0); if (fd < 0) continue;
            int one = 1;
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
#ifdef SO_REUSEPORT
            setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);   // 같은 포트에 소켓 N 개
#endif
            int rb = 4 * 1024 * 1024; setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rb, sizeof rb);
            sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(uint16_t(port)); a.sin_addr.s_addr = htonl(INADDR_ANY);
            if (bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0) { close(fd); continue; }
            ip_mreq mreq{}; inet_pton(AF_INET, group.c_str(), &mreq.imr_multiaddr);
            inet_pton(AF_INET, iface.empty() ? "0.0.0.0" : iface.c_str(), &mreq.imr_interface);
            if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof mreq) < 0) { close(fd); continue; }
            int fl = fcntl(fd, F_GETFL, 0); fcntl(fd, F_SETFL, fl | O_NONBLOCK);
            s.fd = fd; ++joined;
        }

        const std::string h0 = http_get(host, admin, "/healthz");
        const double d0 = json_num(h0, "datagrams_sent"), b0 = json_num(h0, "bytes_sent");

        std::vector<pollfd> pfds; std::vector<size_t> idx;
        for (size_t i = 0; i < subs.size(); ++i) if (subs[i].fd >= 0) { pfds.push_back(pollfd{subs[i].fd, POLLIN, 0}); idx.push_back(i); }
        std::vector<double> lat; lat.reserve(200000);
        std::vector<uint8_t> buf(65536);
        const double t0 = mono(), deadline = t0 + seconds;
        while (mono() < deadline) {
            int k = poll(pfds.data(), pfds.size(), 100);
            if (k <= 0) continue;
            for (size_t p = 0; p < pfds.size(); ++p) {
                if (!(pfds[p].revents & POLLIN)) continue;
                Sub& s = subs[idx[p]];
                for (;;) {
                    ssize_t got = recv(s.fd, buf.data(), buf.size(), 0);
                    if (got <= 0) break;
                    ++s.datagrams; s.bytes += uint64_t(got);
                    const uint64_t now = sys_now_ns();
                    const bool sample = (idx[p] == 0);   // 지연은 대표 구독자 하나에서만 — 전원에서 재면 이 도구가 병목이 된다
                    s.parser.feed(buf.data(), size_t(got), [&](const FrameView& f) {
                        ++s.frames; s.track.observe(f.seq);
                        if (sample && f.msg_type == MSG_TRADE && f.length >= Trade::SIZE)
                            lat.push_back(double(int64_t(now - get_be64(f.payload + 32))) / 1000.0);
                    });
                }
            }
        }
        const double elapsed = mono() - t0;
        const std::string h1 = http_get(host, admin, "/healthz");
        const double d1 = json_num(h1, "datagrams_sent"), b1 = json_num(h1, "bytes_sent");
        const double dgram_s = (d1 - d0) / elapsed, mb_s = (b1 - b0) / elapsed / 1e6;

        uint64_t frames = 0, lost = 0, dgrams = 0, bytes = 0;
        for (auto& s : subs) { if (s.fd < 0) continue; frames += s.frames; lost += s.track.lost_messages; dgrams += s.datagrams; bytes += s.bytes; close(s.fd); }
        std::sort(lat.begin(), lat.end());
        const double p50 = pct(lat, 50), p99 = pct(lat, 99), lmax = lat.empty() ? 0 : lat.back();
        std::fprintf(stderr, "%7d %7d %12.0f %12.1f %12.0f %9.0fµ %9.0fµ %7llu\n", n, joined, dgram_s, mb_s, frames / elapsed, p50, p99, (unsigned long long)lost);

        char b[1024];
        std::snprintf(b, sizeof b,
            "{\"subscribers\": %d, \"joined\": %d, \"elapsed_s\": %.2f, \"publish_datagrams_per_s\": %.0f, \"publish_mb_per_s\": %.2f, "
            "\"receive_frames_per_s\": %.0f, \"receive_datagrams_per_s\": %.0f, \"receive_mb_per_s\": %.2f, "
            "\"latency_p50_us\": %.1f, \"latency_p99_us\": %.1f, \"latency_max_us\": %.1f, \"lost_messages\": %llu}",
            n, joined, elapsed, dgram_s, mb_s, frames / elapsed, dgrams / elapsed, double(bytes) / elapsed / 1e6, p50, p99, lmax, (unsigned long long)lost);
        rounds += (ci ? ",\n  " : "  ") + std::string(b);
        if (ci + 1 < counts.size()) std::this_thread::sleep_for(std::chrono::seconds(gap_s));
    }
    char ts[32]; std::time_t t = std::time(nullptr); std::strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%S%z", std::localtime(&t));
    std::string doc = "{\n \"generated_at\": \"" + std::string(ts) + "\",\n \"tool\": \"cpp/bench/mcast_load_client.cpp\",\n"
        " \"note\": \"발행 측 비용(publish_*)은 구독자 수와 무관해야 한다. 수신 측(receive_*)은 한 호스트에서 커널이 N 번 복제하므로 N 에 비례한다 — 실제 배포에서는 스위치가 복제한다.\",\n"
        " \"group\": \"" + group + ":" + std::to_string(port) + "\",\n \"seconds_per_round\": " + std::to_string(seconds) + ",\n \"rounds\": [\n" + rounds + "\n ]\n}\n";
    if (!out.empty()) { std::ofstream(out) << doc; std::fprintf(stderr, "저장: %s\n", out.c_str()); } else std::fputs(doc.c_str(), stdout);
    return 0;
}
