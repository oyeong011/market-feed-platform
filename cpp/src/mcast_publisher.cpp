// mcast_publisher (C++) — UDP 멀티캐스트 증분 피드 + TCP 복구 채널 (스냅샷·재전송).
//
//     UDS 버스 ─▶ 채널 seq 부여 ─▶ MDFP 인코딩 ─┬─▶ UDP 멀티캐스트 (증분, 구독자 수와 무관한 비용)
//                                              └─▶ 재전송 버퍼 (최근 N 프레임)
//     TCP 복구 채널: MSG_SUBSCRIBE → 스냅샷 + MSG_SNAPSHOT{next_seq}
//                    MSG_RETRANS{from,to} → 버퍼의 프레임 재전송 + MSG_ACK{sent, oldest_available}
//
// 거래소 피드의 표준 구조다(ITCH/MoldUDP64, CME MDP 3.0 계열). TCP 게이트웨이는 구독자마다
// send() 를 부르니 비용이 구독자 수에 비례하지만, 멀티캐스트는 한 번 쏘면 끝이다. 대신 UDP 는
// 유실·순서 뒤바뀜이 정상이라, **수신자가 seq 로 갭을 잡고 복구 채널로 메운다.** 그 경로가 실제로
// 도는지 확인하려고 발행자에 결정적 유실 주입(MDFEED_MCAST_DROP_EVERY=N: N번째 데이터그램마다 버림)을 둔다.
//
// 설정(환경변수): MDFEED_BUS_PATH(S), MDFEED_MCAST_GROUP(239.192.0.1) MDFEED_MCAST_PORT(9130)
//   MDFEED_MCAST_IF(송신 인터페이스 IPv4, 기본 커널 선택) MDFEED_MCAST_TTL(1) MDFEED_MCAST_LOOP(1)
//   MDFEED_MCAST_RECOVERY_PORT(9131) MDFEED_MCAST_ADMIN_PORT(9132) MDFEED_HTTP_HOST(바인드, 0.0.0.0)
//   MDFEED_MCAST_RETRANS_BUFFER(65536 프레임) MDFEED_MCAST_MAX_DATAGRAM(1400B) MDFEED_MCAST_DROP_EVERY(0)
//   MDFEED_MCAST_HEARTBEAT_MS(250): 유휴 시 자체 하트비트. 마지막 데이터그램이 유실되면 뒤에 오는 게 없어
//   수신자가 갭을 알 길이 없다 — 꼬리 유실은 하트비트가 있어야 드러난다 (MoldUDP64 도 같은 이유로 하트비트를 쏜다).
//   MDFEED_MCAST_REORDER_EVERY(0) / MDFEED_MCAST_DUPLICATE_EVERY(0): 순서 뒤바뀜·중복 주입.
//   UDP 는 유실만 정상인 게 아니다. 경로가 갈리면 순서가 바뀌고, 재전송·멀티캐스트 경로가 겹치면 같은
//   데이터그램이 두 번 온다. 수신자에 그 처리 코드가 있어도 시험이 없으면 도는지 알 수 없다.
// 그룹이 멀티캐스트 주소가 아니면(예: 127.0.0.1) 유니캐스트 UDP 로 보낸다 — 같은 코드 경로를 어디서나 시험할 수 있다.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "mdfp/bus_subscriber.hpp"
#include "mdfp/protocol.hpp"

#ifdef MSG_NOSIGNAL
#define MSG_NOSIGNAL_COMPAT MSG_NOSIGNAL
#else
#define MSG_NOSIGNAL_COMPAT 0
#endif

using namespace mdfp;
namespace {

std::string env_str(const char* k, const char* d) { const char* v = std::getenv(k); return (v && *v) ? v : d; }
long env_int(const char* k, long d) { const char* v = std::getenv(k); return (v && *v) ? std::atol(v) : d; }
std::vector<std::string> split_csv(const std::string& s) { std::vector<std::string> o; std::string c; for (char ch : s) { if (ch == ',') { if (!c.empty()) o.push_back(c); c.clear(); } else if (ch != ' ') c += ch; } if (!c.empty()) o.push_back(c); return o; }
double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
uint64_t now_ns() { return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }
void logf(const char* level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
void logf(const char* level, const char* fmt, ...) { char b[1024]; va_list ap; va_start(ap, fmt); std::vsnprintf(b, sizeof b, fmt, ap); va_end(ap); std::fprintf(stderr, "%.3f %s mdfeed.mcast_publisher: %s\n", mono(), level, b); }
void set_nonblock(int fd) { int fl = fcntl(fd, F_GETFL, 0); fcntl(fd, F_SETFL, fl | O_NONBLOCK); }
std::string json_escape(const std::string& s) { std::string o; for (char c : s) { if (c == '"' || c == '\\') { o += '\\'; o += c; } else if (uint8_t(c) < 0x20) o += ' '; else o += c; } return o; }

struct Cfg {
    std::vector<std::string> bus_paths; std::string group, iface, bind_host; int udp_port, recovery_port, admin_port, ttl, loop;
    size_t retrans_buffer, max_datagram; uint64_t drop_every, reorder_every, duplicate_every; int heartbeat_ms;
    Cfg() {
        bus_paths = split_csv(env_str("MDFEED_BUS_PATHS", "")); if (bus_paths.empty()) bus_paths.push_back(env_str("MDFEED_BUS_PATH", "/tmp/mdfeed/bus.sock"));
        group = env_str("MDFEED_MCAST_GROUP", "239.192.0.1"); udp_port = int(env_int("MDFEED_MCAST_PORT", 9130)); iface = env_str("MDFEED_MCAST_IF", "");
        ttl = int(env_int("MDFEED_MCAST_TTL", 1)); loop = int(env_int("MDFEED_MCAST_LOOP", 1));
        recovery_port = int(env_int("MDFEED_MCAST_RECOVERY_PORT", 9131)); admin_port = int(env_int("MDFEED_MCAST_ADMIN_PORT", 9132)); bind_host = env_str("MDFEED_HTTP_HOST", "0.0.0.0");
        retrans_buffer = size_t(env_int("MDFEED_MCAST_RETRANS_BUFFER", 65536)); max_datagram = size_t(env_int("MDFEED_MCAST_MAX_DATAGRAM", 1400)); drop_every = uint64_t(env_int("MDFEED_MCAST_DROP_EVERY", 0));
        reorder_every = uint64_t(env_int("MDFEED_MCAST_REORDER_EVERY", 0));
        duplicate_every = uint64_t(env_int("MDFEED_MCAST_DUPLICATE_EVERY", 0));
        heartbeat_ms = int(env_int("MDFEED_MCAST_HEARTBEAT_MS", 250));
    }
};

struct Conn { int fd = -1; std::string peer; FrameParser in; std::string out; size_t off = 0; double deadline = 0; bool admin = false; uint64_t sent_frames = 0; };

class Publisher {
public:
    explicit Publisher(Cfg c) : cfg_(std::move(c)), ring_(cfg_.retrans_buffer) {}
    static volatile sig_atomic_t g_stop;

    int run() {
        for (auto& p : cfg_.bus_paths) sources_.emplace_back(p, "mcast-publisher-cpp");
        if (!open_udp()) return 1;
        rec_fd_ = listen_tcp(cfg_.bind_host, cfg_.recovery_port, rec_port_); adm_fd_ = listen_tcp(cfg_.bind_host, cfg_.admin_port, adm_port_);
        if (rec_fd_ < 0 || adm_fd_ < 0) return 1;
        std::printf("{\"event\":\"listening\",\"service\":\"mcast-publisher\",\"impl\":\"c++\",\"group\":\"%s\",\"udp_port\":%d,\"recovery_port\":%d,\"admin_port\":%d,\"multicast\":%s}\n",
                    cfg_.group.c_str(), cfg_.udp_port, rec_port_, adm_port_, multicast_ ? "true" : "false");
        std::fflush(stdout);
        logf("INFO", "증분 → %s:%d (%s) · 복구 TCP :%d · admin :%d · 재전송 버퍼 %zu · 유실 주입 %llu", cfg_.group.c_str(), cfg_.udp_port, multicast_ ? "multicast" : "unicast", rec_port_, adm_port_, cfg_.retrans_buffer, (unsigned long long)cfg_.drop_every);
        started_ = mono();
        while (!g_stop) {
            const double now = mono(); std::string err;
            for (auto& s : sources_) if (s.due()) { s.try_connect(err); if (!err.empty()) { logf("WARNING", "bus %s: %s", s.path.c_str(), err.c_str()); err.clear(); } }
            pfds_.clear(); owners_.clear();
            add(rec_fd_, POLLIN, {Kind::RecListen, 0}); add(adm_fd_, POLLIN, {Kind::AdmListen, 0});
            for (size_t i = 0; i < sources_.size(); ++i) if (sources_[i].fd >= 0) add(sources_[i].fd, sources_[i].connected ? POLLIN : POLLOUT, {Kind::Bus, i});
            for (auto& [fd, c] : conns_) add(fd, (c.off < c.out.size()) ? (POLLIN | POLLOUT) : POLLIN, {Kind::Conn, uint64_t(fd)});
            int n = poll(pfds_.data(), pfds_.size(), std::min(500, std::max(10, cfg_.heartbeat_ms / 2)));
            if (n < 0) { if (errno == EINTR) continue; logf("ERROR", "poll: %s", std::strerror(errno)); break; }
            // 유휴 하트비트: 마지막 전송 뒤 heartbeat_ms 가 지났으면 자체 하트비트를 쏜다.
            // 꼬리 유실(마지막 데이터그램이 사라진 경우)은 이게 있어야 수신자가 알아챈다.
            if (cfg_.heartbeat_ms > 0 && mono() - last_send_at_ >= cfg_.heartbeat_ms / 1000.0) {
                uint8_t p[8]; put_be64(p, now_ns());
                FrameView hb; hb.msg_type = MSG_HEARTBEAT; hb.flags = 0; hb.seq = 0; hb.payload = p; hb.length = 8;
                on_frame(hb, false); ++own_heartbeats_; flush_datagram();
            }
            for (size_t i = 0; i < pfds_.size(); ++i) {
                if (!pfds_[i].revents) continue;
                const Owner o = owners_[i]; const short re = pfds_[i].revents;
                try {
                    switch (o.kind) {
                        case Kind::RecListen: accept_conn(rec_fd_, false); break;
                        case Kind::AdmListen: accept_conn(adm_fd_, true); break;
                        case Kind::Bus: on_bus(sources_[o.id], re); break;
                        case Kind::Conn: on_conn(int(o.id), re); break;
                    }
                } catch (const std::exception& e) { logf("ERROR", "이벤트 처리 예외: %s", e.what()); if (o.kind == Kind::Conn) drop_conn(int(o.id)); }
            }
            for (auto it = conns_.begin(); it != conns_.end();) { if (it->second.admin && now > it->second.deadline) { close(it->first); it = conns_.erase(it); } else ++it; }
        }
        for (auto& [fd, c] : conns_) close(fd);
        for (auto& s : sources_) if (s.fd >= 0) close(s.fd);
        close(udp_fd_); close(rec_fd_); close(adm_fd_);
        logf("INFO", "종료. 수신 %llu · 데이터그램 %llu · 재전송 요청 %llu", (unsigned long long)frames_in_, (unsigned long long)datagrams_, (unsigned long long)retrans_requests_);
        return 0;
    }

private:
    enum class Kind { RecListen, AdmListen, Bus, Conn };
    struct Owner { Kind kind; uint64_t id; };
    Cfg cfg_;
    std::vector<BusSubscriber> sources_;
    std::vector<std::vector<uint8_t>> ring_;      // seq % cap → 인코딩된 프레임
    std::unordered_map<std::string, std::pair<uint8_t, std::vector<uint8_t>>> last_;
    std::map<int, Conn> conns_;
    std::vector<pollfd> pfds_; std::vector<Owner> owners_;
    int udp_fd_ = -1, rec_fd_ = -1, adm_fd_ = -1, rec_port_ = 0, adm_port_ = 0; bool multicast_ = false; sockaddr_in dst_{};
    uint64_t seq_ = 0, frames_in_ = 0, datagrams_ = 0, bytes_sent_ = 0, injected_drops_ = 0, retrans_requests_ = 0, retrans_frames_ = 0, retrans_unavailable_ = 0, snapshots_ = 0, send_errors_ = 0;
    std::vector<uint8_t> dgram_, held_; size_t dgram_frames_ = 0;
    uint64_t injected_reorders_ = 0, injected_duplicates_ = 0;
    double started_ = 0, last_frame_at_ = 0, last_send_at_ = 0; uint64_t own_heartbeats_ = 0;

    void add(int fd, short ev, Owner o) { pfds_.push_back(pollfd{fd, ev, 0}); owners_.push_back(o); }

    bool open_udp() {
        udp_fd_ = socket(AF_INET, SOCK_DGRAM, 0); if (udp_fd_ < 0) { logf("ERROR", "udp socket: %s", std::strerror(errno)); return false; }
        dst_.sin_family = AF_INET; dst_.sin_port = htons(uint16_t(cfg_.udp_port));
        if (inet_pton(AF_INET, cfg_.group.c_str(), &dst_.sin_addr) != 1) { logf("ERROR", "그룹 주소를 해석할 수 없습니다: %s", cfg_.group.c_str()); return false; }
        const uint8_t first = uint8_t(ntohl(dst_.sin_addr.s_addr) >> 24);
        multicast_ = first >= 224 && first <= 239;
        if (multicast_) {
            uint8_t ttl = uint8_t(cfg_.ttl), loop = uint8_t(cfg_.loop);
            setsockopt(udp_fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof ttl);
            setsockopt(udp_fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof loop);   // 같은 호스트의 수신자도 받게
            if (!cfg_.iface.empty()) { in_addr ifa{}; if (inet_pton(AF_INET, cfg_.iface.c_str(), &ifa) == 1) setsockopt(udp_fd_, IPPROTO_IP, IP_MULTICAST_IF, &ifa, sizeof ifa); }
        }
        int sz = 4 * 1024 * 1024; setsockopt(udp_fd_, SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);
        return true;
    }
    static int listen_tcp(const std::string& host, int port, int& bound) {
        int fd = socket(AF_INET, SOCK_STREAM, 0); int one = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(uint16_t(port));
        if (host.empty()) a.sin_addr.s_addr = htonl(INADDR_ANY); else if (host == "localhost") a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        else if (inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) { logf("ERROR", "호스트를 해석할 수 없습니다: %s", host.c_str()); close(fd); return -1; }
        if (bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0 || listen(fd, 128) < 0) { logf("ERROR", "bind/listen %s:%d: %s", host.c_str(), port, std::strerror(errno)); close(fd); return -1; }
        socklen_t l = sizeof a; getsockname(fd, reinterpret_cast<sockaddr*>(&a), &l); bound = ntohs(a.sin_port); set_nonblock(fd); return fd;
    }

    // ── 버스 → seq 부여 → 데이터그램 묶음 ─────────────────────────────
    void on_bus(BusSubscriber& s, short re) {
        if (!s.connected) { if (re & (POLLERR | POLLHUP)) { s.schedule_retry(); return; } if (s.finish_connect()) logf("INFO", "bus subscriber connected to %s", s.path.c_str()); return; }
        s.drain([&](const FrameView& f) { on_frame(f); });
        flush_datagram();
    }
    void on_frame(const FrameView& f, bool from_bus = true) {
        if (from_bus) { ++frames_in_; last_frame_at_ = mono(); }
        if (f.msg_type == MSG_TRADE && f.length >= Trade::SIZE) last_[unfix(f.payload + 16, 8) + ":" + unfix(f.payload, 16)] = {f.msg_type, std::vector<uint8_t>(f.payload, f.payload + f.length)};
        else if (f.msg_type == MSG_BOOK && f.length >= BookTop::SIZE) last_[unfix(f.payload + 16, 8) + ":" + unfix(f.payload, 16)] = {f.msg_type, std::vector<uint8_t>(f.payload, f.payload + f.length)};
        auto& slot = ring_[seq_ % ring_.size()]; slot.clear();
        encode_into(slot, f.msg_type, seq_, f.payload, f.length, f.flags);
        ++seq_;
        if (!dgram_.empty() && dgram_.size() + slot.size() > cfg_.max_datagram) flush_datagram();
        dgram_.insert(dgram_.end(), slot.begin(), slot.end()); ++dgram_frames_;
    }
    void flush_datagram() {
        if (dgram_.empty()) return;
        ++datagrams_;
        last_send_at_ = mono();
        if (cfg_.drop_every && datagrams_ % cfg_.drop_every == 0) { ++injected_drops_; }   // 결정적 유실 주입 — 복구 경로 시험용
        else if (cfg_.reorder_every && datagrams_ % cfg_.reorder_every == 0 && held_.empty()) {
            // 재배열 주입: 이 데이터그램을 붙들었다가 **다음 것 뒤에** 보낸다. 실제 경로가
            // 갈렸을 때 일어나는 일이다. 수신자는 갭으로 보고 재전송을 부르지 말아야 한다 —
            // 잠깐 기다리면 오는 것이므로.
            held_ = dgram_; ++injected_reorders_;
        } else {
            ssize_t n = ::sendto(udp_fd_, dgram_.data(), dgram_.size(), 0, reinterpret_cast<sockaddr*>(&dst_), sizeof dst_);
            if (n < 0) {
                ++send_errors_;
                if (send_errors_ == 1 || send_errors_ % 1000 == 0) {
                    // 조용히 세기만 하면 "발행은 되는데 아무도 못 받는" 상태를 아무도 모른다.
                    // macOS 에서 en0 에 주소가 없거나 멀티캐스트 경로가 없으면 ENETUNREACH/EADDRNOTAVAIL 이 난다.
                    logf("ERROR", "UDP 전송 실패 #%llu (%s). 멀티캐스트 경로가 없으면 MDFEED_MCAST_IF 로 송신 인터페이스를 지정하세요 (같은 호스트 시험은 127.0.0.1, 수신자도 같은 인터페이스로 가입)",
                         (unsigned long long)send_errors_, std::strerror(errno));
                }
            } else bytes_sent_ += uint64_t(n);
            if (cfg_.duplicate_every && datagrams_ % cfg_.duplicate_every == 0) {
                // 중복 주입: 같은 데이터그램을 한 번 더. 수신자는 두 번째를 중복으로 세고 버려야 한다.
                if (::sendto(udp_fd_, dgram_.data(), dgram_.size(), 0, reinterpret_cast<sockaddr*>(&dst_), sizeof dst_) > 0) ++injected_duplicates_;
            }
            if (!held_.empty()) {   // 붙들어 둔 것을 지금 내보낸다 → 수신자에게는 순서가 뒤바뀌어 보인다
                if (::sendto(udp_fd_, held_.data(), held_.size(), 0, reinterpret_cast<sockaddr*>(&dst_), sizeof dst_) > 0) bytes_sent_ += uint64_t(held_.size());
                held_.clear();
            }
        }
        dgram_.clear(); dgram_frames_ = 0;
    }

    // ── 복구 채널 / 관리 ───────────────────────────────────────────────
    void accept_conn(int lfd, bool admin) {
        for (;;) {
            sockaddr_in a{}; socklen_t l = sizeof a; int fd = accept(lfd, reinterpret_cast<sockaddr*>(&a), &l);
            if (fd < 0) return;
            set_nonblock(fd); int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            char ip[64]; inet_ntop(AF_INET, &a.sin_addr, ip, sizeof ip);
            Conn c; c.fd = fd; c.peer = std::string(ip) + ":" + std::to_string(ntohs(a.sin_port)); c.admin = admin; c.deadline = mono() + 5.0;
            conns_[fd] = std::move(c);
        }
    }
    void drop_conn(int fd) { auto it = conns_.find(fd); if (it == conns_.end()) return; close(fd); conns_.erase(it); }
    void flush_conn(Conn& c) {
        while (c.off < c.out.size()) {
            ssize_t n = ::send(c.fd, c.out.data() + c.off, c.out.size() - c.off, MSG_NOSIGNAL_COMPAT);
            if (n > 0) { c.off += size_t(n); continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            if (n < 0 && errno == EINTR) continue;
            c.off = c.out.size(); c.out.clear(); throw std::runtime_error("send failed");
        }
        c.out.clear(); c.off = 0;
    }
    void on_conn(int fd, short re) {
        auto it = conns_.find(fd); if (it == conns_.end()) return;
        Conn& c = it->second;
        if (re & (POLLERR | POLLNVAL)) { drop_conn(fd); return; }
        if (re & POLLIN) {
            uint8_t buf[8192]; ssize_t n = ::recv(fd, buf, sizeof buf, 0);
            if (n == 0) { drop_conn(fd); return; }
            if (n < 0) { if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) { drop_conn(fd); return; } }
            else if (c.admin) { on_admin_bytes(c, buf, size_t(n)); if (conns_.find(fd) == conns_.end()) return; }
            else c.in.feed(buf, size_t(n), [&](const FrameView& f) { on_recovery_request(c, f); });
        }
        if (c.off < c.out.size() || (re & POLLOUT)) { flush_conn(c); if (c.admin && c.out.empty() && c.off == 0 && c.deadline < 0) { drop_conn(fd); return; } }
        if (c.out.size() > 8u * 1024 * 1024) { logf("WARNING", "복구 클라이언트 %s 송신 버퍼 8MB 초과 → 끊음", c.peer.c_str()); drop_conn(fd); }
    }
    void on_recovery_request(Conn& c, const FrameView& f) {
        if (f.msg_type == MSG_SUBSCRIBE) {   // 스냅샷: 최신값 전체 + 메타(next_seq = 이 시점 이후 증분의 첫 seq)
            std::vector<uint8_t> out; size_t n = 0;
            for (auto& [k, v] : last_) { encode_into(out, v.first, 0, v.second.data(), v.second.size(), FLAG_SNAPSHOT); ++n; }
            const std::string meta = "{\"snapshot_count\": " + std::to_string(n) + ", \"ts_ns\": " + std::to_string(now_ns()) + ", \"next_seq\": " + std::to_string(seq_) + "}";
            encode_into(out, MSG_SNAPSHOT, 0, reinterpret_cast<const uint8_t*>(meta.data()), meta.size(), FLAG_SNAPSHOT);
            c.out.append(reinterpret_cast<const char*>(out.data()), out.size()); ++snapshots_; c.sent_frames += n + 1;
        } else if (f.msg_type == MSG_RETRANS && f.length >= 16) {
            ++retrans_requests_;
            uint64_t from = get_be64(f.payload), to = get_be64(f.payload + 8);
            const uint64_t oldest = seq_ > ring_.size() ? seq_ - ring_.size() : 0;   // 버퍼가 보관하는 가장 오래된 seq
            if (to >= seq_) to = seq_ ? seq_ - 1 : 0;
            if (to - from + 1 > 10000) to = from + 9999;                               // 요청 하나당 상한
            std::vector<uint8_t> out; uint64_t sent = 0;
            for (uint64_t s = std::max(from, oldest); seq_ && s <= to && s < seq_; ++s) { auto& fr = ring_[s % ring_.size()]; out.insert(out.end(), fr.begin(), fr.end()); ++sent; }
            const uint64_t unavailable = (from < oldest) ? std::min(oldest, to + 1) - from : 0;
            retrans_frames_ += sent; retrans_unavailable_ += unavailable;
            const std::string ack = "{\"type\": \"retrans\", \"from\": " + std::to_string(from) + ", \"to\": " + std::to_string(to) + ", \"sent\": " + std::to_string(sent) +
                                    ", \"unavailable\": " + std::to_string(unavailable) + ", \"oldest_available\": " + std::to_string(oldest) + ", \"next_seq\": " + std::to_string(seq_) + "}";
            encode_into(out, MSG_ACK, 0, reinterpret_cast<const uint8_t*>(ack.data()), ack.size());
            c.out.append(reinterpret_cast<const char*>(out.data()), out.size()); c.sent_frames += sent + 1;
        }
    }
    void on_admin_bytes(Conn& c, const uint8_t* buf, size_t n) {
        static std::map<int, std::string> inbuf;   // 관리 요청은 짧다
        std::string& in = inbuf[c.fd]; in.append(reinterpret_cast<const char*>(buf), n);
        if (in.size() > 16 * 1024) { inbuf.erase(c.fd); drop_conn(c.fd); return; }
        if (in.find("\r\n\r\n") == std::string::npos) return;
        const std::string line = in.substr(0, in.find("\r\n")); inbuf.erase(c.fd);
        const size_t sp1 = line.find(' '), sp2 = sp1 == std::string::npos ? std::string::npos : line.find(' ', sp1 + 1);
        std::string method, target; if (sp1 != std::string::npos && sp2 != std::string::npos && sp2 > sp1 + 1) { method = line.substr(0, sp1); target = line.substr(sp1 + 1, sp2 - sp1 - 1); }
        auto q = target.find('?'); if (q != std::string::npos) target = target.substr(0, q);
        std::string body, ctype = "application/json; charset=utf-8"; int status = 200;
        if (method.empty() || target.empty()) { status = 400; body = "bad request"; ctype = "text/plain"; }
        else if (method != "GET") { status = 405; body = "method not allowed"; ctype = "text/plain"; }
        else if (target == "/healthz") { body = health_json(); status = healthy() ? 200 : 503; }
        else if (target == "/readyz") body = "{\"ready\": true}";
        else if (target == "/metrics") { body = metrics_text(); ctype = "text/plain; version=0.0.4; charset=utf-8"; }
        else { status = 404; body = "not found"; ctype = "text/plain"; }
        const char* reason = status == 200 ? "OK" : status == 400 ? "Bad Request" : status == 404 ? "Not Found" : status == 405 ? "Method Not Allowed" : "Service Unavailable";
        c.out = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\nContent-Type: " + ctype + "\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
        c.off = 0; c.deadline = -1;   // 다 쓰면 닫는다
    }
    bool healthy() const { const double age = last_frame_at_ ? mono() - last_frame_at_ : -1; bool any = false; for (auto& s : sources_) any |= s.connected; return any && (age < 0 || age < 30.0); }
    std::string health_json() const {
        const double age = last_frame_at_ ? mono() - last_frame_at_ : -1; size_t rec = 0; for (auto& [fd, c] : conns_) if (!c.admin) ++rec;
        std::string srcs; for (auto& s : sources_) srcs += (srcs.empty() ? "" : ", ") + std::string("{\"source\": \"") + json_escape(s.path.substr(s.path.rfind('/') + 1)) + "\", \"connected\": " + (s.connected ? "true" : "false") + ", \"frames\": " + std::to_string(s.frames) + ", \"restarts\": " + std::to_string(s.restarts) + "}";
        std::string o = "{\"service\": \"mcast-publisher\", \"impl\": \"c++\", \"healthy\": "; o += healthy() ? "true" : "false";
        o += ", \"uptime_s\": " + std::to_string(mono() - started_) + ", \"group\": \"" + cfg_.group + "\", \"udp_port\": " + std::to_string(cfg_.udp_port) + ", \"multicast\": " + (multicast_ ? "true" : "false");
        o += ", \"last_frame_age_s\": " + (age < 0 ? std::string("null") : std::to_string(age)) + ", \"frames_in\": " + std::to_string(frames_in_) + ", \"seq\": " + std::to_string(seq_);
        o += ", \"datagrams_sent\": " + std::to_string(datagrams_ - injected_drops_) + ", \"bytes_sent\": " + std::to_string(bytes_sent_) + ", \"injected_drops\": " + std::to_string(injected_drops_) + ", \"send_errors\": " + std::to_string(send_errors_);
        o += ", \"retrans_requests\": " + std::to_string(retrans_requests_) + ", \"retrans_frames_sent\": " + std::to_string(retrans_frames_) + ", \"retrans_unavailable\": " + std::to_string(retrans_unavailable_) + ", \"snapshots_served\": " + std::to_string(snapshots_);
        o += ", \"injected_reorders\": " + std::to_string(injected_reorders_) + ", \"injected_duplicates\": " + std::to_string(injected_duplicates_);
        o += ", \"own_heartbeats\": " + std::to_string(own_heartbeats_) + ", \"retrans_buffer\": " + std::to_string(ring_.size()) + ", \"recovery_clients\": " + std::to_string(rec) + ", \"cached_symbols\": " + std::to_string(last_.size()) + ", \"sources\": [" + srcs + "], \"tasks\": {}}";
        return o;
    }
    std::string metrics_text() const {
        auto line = [](const char* k, double v) { char b[160]; std::snprintf(b, sizeof b, "mdfeed_%s{service=\"mcast-publisher\"} %g\n", k, v); return std::string(b); };
        size_t rec = 0; for (auto& [fd, c] : conns_) if (!c.admin) ++rec;
        return line("uptime_seconds", mono() - started_) + line("frames_in_total", double(frames_in_)) + line("mcast_datagrams_total", double(datagrams_ - injected_drops_)) + line("mcast_bytes_total", double(bytes_sent_)) +
               line("mcast_injected_drops_total", double(injected_drops_)) + line("mcast_injected_reorders_total", double(injected_reorders_)) + line("mcast_injected_duplicates_total", double(injected_duplicates_)) + line("mcast_send_errors_total", double(send_errors_)) + line("mcast_retrans_requests_total", double(retrans_requests_)) +
               line("mcast_retrans_frames_total", double(retrans_frames_)) + line("mcast_retrans_unavailable_total", double(retrans_unavailable_)) + line("mcast_snapshots_total", double(snapshots_)) + line("mcast_own_heartbeats_total", double(own_heartbeats_)) + line("mcast_recovery_clients", double(rec)) + line("mcast_seq", double(seq_));
    }
};
volatile sig_atomic_t Publisher::g_stop = 0;
void on_signal(int) { Publisher::g_stop = 1; }

}  // namespace

int main() {
    signal(SIGPIPE, SIG_IGN); signal(SIGTERM, on_signal); signal(SIGINT, on_signal);
    return Publisher(Cfg{}).run();
}
