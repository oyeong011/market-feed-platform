// C++ 부하 클라이언트 — bench/load_test.py 와 같은 측정 정의, 같은 출력 키.
//
// 왜 필요한가: 파이썬 도구는 구독자마다 스레드 하나라 200명이면 스레드 200개가 GIL 을 다툰다.
// 200명 회차에서 C++ 게이트웨이 p99 가 7~284ms 로 요동쳤는데, 그동안 서버 큐는 0 이었다.
// 측정 도구가 병목이면 결론이 안 선다. 이 도구는 단일 스레드 poll() 로 소켓 수백 개를 받는다.
//
// 측정 정의(파이썬과 동일):
//   * 스냅샷 프레임은 제외. 증분 프레임의 seq 로 갭·유실을 센다.
//   * 지연 = 지금(system clock) − Trade.ts_recv_ns (feedd 가 소켓에서 읽은 시각). 배포단이 더한 지연.
//   * messages = 체결 + 호가. 지연은 체결만.
//   * 백분위 = 정렬 후 index = min(int(n*q/100), n-1) (파이썬 pct 와 동일)
//   * 상류 msg/s = 회차 동안 게이트웨이 /healthz 의 frames_in 증가분 / 경과. 큐·전송버퍼 최대는 0.5초 간격 표본.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
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

uint64_t sys_now_ns() { return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }
double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

// ── 아주 작은 HTTP GET + JSON 숫자 추출 (관리 포트 /healthz 용) ────────────────
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

struct Sub {
    int fd = -1; FrameParser parser; SequenceTracker track;
    uint64_t messages = 0, bytes = 0, gaps = 0, lost = 0; bool connect_failed = false; std::string err;
    // 구독자별 지연. 팬아웃이 공평한지 보려면 전체 분포가 아니라 **구독자마다** 재야 한다.
    // 게이트웨이가 늘 같은 순서로 쓰면 뒤쪽 구독자가 계속 손해를 보는데, 전부 한 통에 넣고
    // p99 를 내면 그 편향이 평균에 묻힌다.
    std::vector<double> lat;
};

double pct(std::vector<double>& xs, double q) {   // xs 는 정렬돼 있어야 한다
    if (xs.empty()) return 0.0;
    size_t i = std::min(size_t(double(xs.size()) * q / 100.0), xs.size() - 1);
    return xs[i];
}

struct Round {
    int subscribers = 0, connected = 0, connect_failed = 0; std::string connect_error;
    double elapsed = 0; uint64_t total_messages = 0, bytes_total = 0, gaps = 0, lost = 0, crc_errors = 0, resyncs = 0;
    uint64_t per_sub_min = 0, per_sub_max = 0;
    double p50 = 0, p95 = 0, p99 = 0, p999 = 0, lmax = 0, per_sub_avg = 0;
    // 공평성: 구독자별 p99 의 퍼짐. first/last 는 접속 순서(게이트웨이의 쓰기 순서)와 같다.
    double fair_p99_min = 0, fair_p99_med = 0, fair_p99_max = 0, fair_p99_first = 0, fair_p99_last = 0, fair_spread = 0;
    double g_dropped = -1, g_subs = -1, up_frames_before = -1, up_frames_after = -1, up_symbols = -1;
    double peak_backlog = 0, peak_wire = 0; int samples = 0;
};

Round run_round(const std::string& host, int port, int admin, int n, double seconds, const std::string& subscribe_json) {
    Round r; r.subscribers = n;
    std::vector<Sub> subs{}; subs.resize(size_t(n));
    for (auto& s : subs) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(uint16_t(port)); inet_pton(AF_INET, host.c_str(), &a.sin_addr);
        timeval tv{10, 0}; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        if (connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0) { s.connect_failed = true; s.err = std::strerror(errno); close(fd); ++r.connect_failed; if (r.connect_error.empty()) r.connect_error = s.err; continue; }
        int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        int fl = fcntl(fd, F_GETFL, 0); fcntl(fd, F_SETFL, fl | O_NONBLOCK);
        if (!subscribe_json.empty()) { auto f = encode(MSG_SUBSCRIBE, 0, reinterpret_cast<const uint8_t*>(subscribe_json.data()), subscribe_json.size()); send(fd, f.data(), f.size(), 0); }
        s.fd = fd; ++r.connected;
    }
    std::string h0 = http_get(host, admin, "/healthz");
    r.up_frames_before = json_num(h0, "frames_in");

    // 게이트웨이 큐·전송버퍼 표본은 별도 스레드 — 수신 루프에 HTTP 왕복을 섞지 않는다
    std::atomic<bool> stop{false}; double peak_backlog = 0, peak_wire = 0; int samples = 0;
    std::thread sampler([&] {
        while (!stop) { std::string h = http_get(host, admin, "/healthz"); if (!h.empty()) { peak_backlog = std::max(peak_backlog, json_num(h, "max_backlog", 0)); peak_wire = std::max(peak_wire, json_num(h, "max_wire_bytes", 0)); ++samples; }
            for (int i = 0; i < 5 && !stop; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    });

    std::vector<pollfd> pfds; std::vector<size_t> idx;
    for (size_t i = 0; i < subs.size(); ++i) if (subs[i].fd >= 0) { pfds.push_back(pollfd{subs[i].fd, POLLIN, 0}); idx.push_back(i); }
    std::vector<double> lat; lat.reserve(size_t(n) * 8000);
    constexpr size_t PER_SUB_CAP = 20000;   // 구독자 1,000명 × 2만 = 메모리 상한 안
    std::vector<uint8_t> buf(1 << 16);
    const double t0 = mono(), deadline = t0 + seconds;
    size_t read_rotor = 0;
    while (mono() < deadline) {
        if (!pfds.empty()) read_rotor = (read_rotor + 1) % pfds.size();
        int k = poll(pfds.data(), pfds.size(), 100);
        if (k <= 0) continue;
        // **읽는 순서를 매번 돌린다.** 늘 0번부터 읽으면 뒤쪽 소켓은 앞쪽을 다 처리한 뒤에야
        // 시각이 찍힌다 — 그 차이가 "뒤 구독자가 느리다"로 보인다. 서버가 아니라 이 도구가
        // 만든 편향이다. 공평성을 재는 도구가 편향을 갖고 있으면 잴 수가 없다.
        for (size_t off = 0; off < pfds.size(); ++off) {
            const size_t p = (off + read_rotor) % pfds.size();
            if (!(pfds[p].revents & (POLLIN | POLLHUP | POLLERR))) continue;
            Sub& s = subs[idx[p]];
            for (;;) {
                ssize_t got = recv(s.fd, buf.data(), buf.size(), 0);
                if (got > 0) {
                    s.bytes += uint64_t(got);
                    const uint64_t now = sys_now_ns();
                    s.parser.feed(buf.data(), size_t(got), [&](const FrameView& f) {
                        if (f.flags & FLAG_SNAPSHOT) return;
                        uint64_t l = s.track.observe(f.seq); if (l) { ++s.gaps; s.lost += l; }
                        if (f.msg_type == MSG_TRADE && f.length >= Trade::SIZE) {
                            const uint64_t ts_recv = get_be64(f.payload + 32);           // Trade.ts_recv_ns
                            const double us = double(int64_t(now - ts_recv)) / 1000.0;    // µs. 청크 수신 시각 기준
                            lat.push_back(us);
                            if (s.lat.size() < PER_SUB_CAP) s.lat.push_back(us);
                            ++s.messages;
                        } else if (f.msg_type == MSG_BOOK) ++s.messages;
                    });
                    if (size_t(got) < buf.size()) break;
                    continue;
                }
                if (got == 0) { pfds[p].fd = -1; break; }                                  // 서버가 끊음
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                pfds[p].fd = -1; break;
            }
        }
    }
    r.elapsed = mono() - t0;
    stop = true; sampler.join();
    std::string h1 = http_get(host, admin, "/healthz");
    r.up_frames_after = json_num(h1, "frames_in"); r.g_dropped = json_num(h1, "total_dropped"); r.g_subs = json_num(h1, "subscribers"); r.up_symbols = json_num(h1, "cached_symbols");
    r.peak_backlog = peak_backlog; r.peak_wire = peak_wire; r.samples = samples;
    for (auto& s : subs) { if (s.fd >= 0) close(s.fd); if (s.connect_failed) continue;
        r.total_messages += s.messages; r.bytes_total += s.bytes; r.gaps += s.gaps; r.lost += s.lost; r.crc_errors += s.parser.crc_error_count; r.resyncs += s.parser.resync_count;
        r.per_sub_min = r.per_sub_min == 0 && r.per_sub_max == 0 ? s.messages : std::min(r.per_sub_min, s.messages); r.per_sub_max = std::max(r.per_sub_max, s.messages); }
    if (r.connected) r.per_sub_avg = double(r.total_messages) / r.connected;
    std::sort(lat.begin(), lat.end());
    r.p50 = pct(lat, 50); r.p95 = pct(lat, 95); r.p99 = pct(lat, 99); r.p999 = pct(lat, 99.9); r.lmax = lat.empty() ? 0 : lat.back();

    // 구독자별 p99 를 모아 퍼짐을 본다. 한 통에 넣고 낸 p99 는 편향을 숨긴다.
    std::vector<double> per99; per99.reserve(subs.size());
    for (auto& s : subs) { if (s.lat.empty()) continue; std::sort(s.lat.begin(), s.lat.end()); per99.push_back(pct(s.lat, 99)); }
    if (!per99.empty()) {
        r.fair_p99_first = per99.front(); r.fair_p99_last = per99.back();   // 접속(=쓰기) 순서
        std::vector<double> sorted99 = per99; std::sort(sorted99.begin(), sorted99.end());
        r.fair_p99_min = sorted99.front(); r.fair_p99_max = sorted99.back(); r.fair_p99_med = pct(sorted99, 50);
        r.fair_spread = r.fair_p99_min > 0 ? r.fair_p99_max / r.fair_p99_min : 0;
    }
    return r;
}

std::string round_json(const Round& r, double seconds) {
    char b[2048];
    const double up = (r.up_frames_before >= 0 && r.up_frames_after >= 0 && r.elapsed > 0) ? (r.up_frames_after - r.up_frames_before) / r.elapsed : -1;
    std::snprintf(b, sizeof b,
        "{\"subscribers\": %d, \"client\": \"c++\", \"connected\": %d, \"connect_failed\": %d, \"connect_error_sample\": %s, \"elapsed_s\": %.2f, "
        "\"total_messages\": %llu, \"msg_per_s_total\": %.0f, \"msg_per_s_per_sub\": %.1f, \"per_sub_min\": %llu, \"per_sub_max\": %llu, \"bytes_total\": %llu, "
        "\"latency_p50_us\": %.1f, \"latency_p95_us\": %.1f, \"latency_p99_us\": %.1f, \"latency_p999_us\": %.1f, \"latency_max_us\": %.1f, "
        "\"gaps\": %llu, \"lost_messages\": %llu, \"crc_errors\": %llu, \"resyncs\": %llu, "
        "\"gateway_dropped\": %.0f, \"gateway_subscribers\": %.0f, \"upstream_frames_in\": %.0f, \"upstream_msg_per_s\": %.1f, \"upstream_symbols\": %.0f, "
        "\"throughput_retained_pct\": %.1f, \"gateway_max_backlog\": %.0f, \"gateway_max_wire_bytes\": %.0f, \"gateway_samples\": %d, "
        "\"fairness\": {\"per_sub_p99_min_us\": %.1f, \"per_sub_p99_median_us\": %.1f, \"per_sub_p99_max_us\": %.1f, "
        "\"first_connected_p99_us\": %.1f, \"last_connected_p99_us\": %.1f, \"max_over_min\": %.2f}}",
        r.subscribers, r.connected, r.connect_failed, r.connect_error.empty() ? "null" : ("\"" + r.connect_error + "\"").c_str(), r.elapsed,
        (unsigned long long)r.total_messages, r.elapsed > 0 ? double(r.total_messages) / r.elapsed : 0.0, r.per_sub_avg / seconds,
        (unsigned long long)r.per_sub_min, (unsigned long long)r.per_sub_max, (unsigned long long)r.bytes_total,
        r.p50, r.p95, r.p99, r.p999, r.lmax, (unsigned long long)r.gaps, (unsigned long long)r.lost, (unsigned long long)r.crc_errors, (unsigned long long)r.resyncs,
        r.g_dropped, r.g_subs, r.up_frames_after, up, r.up_symbols,
        up > 0 ? (r.per_sub_avg / r.elapsed) / up * 100.0 : -1.0, r.peak_backlog, r.peak_wire, r.samples,
        r.fair_p99_min, r.fair_p99_med, r.fair_p99_max, r.fair_p99_first, r.fair_p99_last, r.fair_spread);
    return b;
}

}  // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1", out, subscribe; int port = 9101, admin = 9111, gap_s = 3; double seconds = 12; std::vector<int> subs;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--host") host = next(); else if (a == "--port") port = std::atoi(next().c_str()); else if (a == "--admin") admin = std::atoi(next().c_str());
        else if (a == "--seconds") seconds = std::atof(next().c_str()); else if (a == "--gap") gap_s = std::atoi(next().c_str()); else if (a == "--out") out = next(); else if (a == "--subscribe") subscribe = next();
        else if (a == "--subscribers") { while (i + 1 < argc && argv[i + 1][0] != '-') subs.push_back(std::atoi(argv[++i])); }
        else { std::fprintf(stderr, "usage: %s [--host H] [--port P] [--admin A] [--subscribers N...] [--seconds S] [--gap SECONDS_BETWEEN_ROUNDS] [--subscribe JSON] [--out FILE]\n", argv[0]); return 2; }
    }
    if (subs.empty()) subs = {1, 10, 50, 100, 200};
    std::fprintf(stderr, "대상 %s:%d · 회차당 %.0f초 · 클라이언트 C++ 단일 스레드 poll\n\n", host.c_str(), port, seconds);
    std::fprintf(stderr, "%6s %6s %9s %9s %10s %10s %6s %6s %9s %9s %7s\n", "구독자", "접속", "msg/s/sub", "상류/s", "p50", "p99", "유실", "드롭", "첫p99", "끝p99", "퍼짐");
    std::string rounds;
    for (size_t i = 0; i < subs.size(); ++i) {
        Round r = run_round(host, port, admin, subs[i], seconds, subscribe);
        const double up = (r.up_frames_after - r.up_frames_before) / (r.elapsed > 0 ? r.elapsed : 1);
        std::fprintf(stderr, "%6d %6d %9.1f %9.1f %9.0fµ %9.0fµ %6llu %6.0f %8.0fµ %8.0fµ %6.1fx\n", r.subscribers, r.connected, r.per_sub_avg / seconds,
            up, r.p50, r.p99, (unsigned long long)r.lost, r.g_dropped, r.fair_p99_first, r.fair_p99_last, r.fair_spread);
        rounds += (i ? ",\n  " : "  ") + round_json(r, seconds);
        // 회차 사이 대기: 직전 회차의 소켓이 TIME_WAIT 로 남아 있으면 다음 회차의 접속이 커널 자원에 막힌다.
        // 실측(2026-09-22): 500명 회차 1초 뒤 1,000명을 붙이자 클라이언트가 조용히 죽었다. 단독으로는 정상.
        if (i + 1 < subs.size()) std::this_thread::sleep_for(std::chrono::seconds(gap_s));
    }
    char ts[32]; std::time_t t = std::time(nullptr); std::strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%S%z", std::localtime(&t));
    std::string doc = "{\n \"generated_at\": \"" + std::string(ts) + "\",\n \"client\": \"c++ (cpp/bench/load_client.cpp, 단일 스레드 poll)\",\n \"target\": \"" + host + ":" + std::to_string(port) + "\",\n \"seconds_per_round\": " + std::to_string(seconds) + ",\n \"rounds\": [\n" + rounds + "\n ]\n}\n";
    if (!out.empty()) { std::ofstream(out) << doc; std::fprintf(stderr, "저장: %s\n", out.c_str()); } else std::fputs(doc.c_str(), stdout);
    return 0;
}
