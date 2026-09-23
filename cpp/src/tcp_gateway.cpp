// tcp_gateway (C++) — src/mdfeed/services/tcp_gateway.py 의 데이터 평면 대체 구현.
//
//     UDS 버스 ─▶ [구독 필터] ─▶ 구독자별 유한 큐 ─▶ TCP 소켓 (MDFP/1)
//
// 파이썬 게이트웨이와 **배선 규약이 같다.** 그래서 파이썬 참조 클라이언트(client.py),
// 부하 시험(bench/load_test.py), 운영 점검(/healthz /metrics) 이 그대로 붙는다.
//   1. 접속 즉시 최신값 스냅샷(seq 0, FLAG_SNAPSHOT) → MSG_SNAPSHOT 메타 → 증분
//   2. MSG_SUBSCRIBE {"symbols":[...],"mode":"stream|conflate"} 로 구독 필터
//   3. 구독자별로 seq 를 다시 매긴다 (필터로 걸러진 번호가 갭으로 보이지 않게)
//   4. 구독자별 유한 큐. 차면 오래된 것부터 버리고 센다. DROP_LIMIT 넘으면 끊는다
//   5. 하트비트는 전원에게 전달
//
// 왜 C++ 인가: 파이썬 게이트웨이는 구독자 100명에서 p99 27.9ms 였고 병목의 59% 가
// 소켓 쓰기였다 (DESIGN.md). 인터프리터가 시스템 콜 사이에서 쓰는 시간이 그 대부분이다.
// 이 구현은 단일 스레드 kqueue/epoll 루프(mdfp/event_loop.hpp)에 논블로킹 소켓이고, 의존성은 컴파일러뿐이다.
// 처음엔 poll() 이었다 — 구독자 1,000명이면 이벤트 하나에 fd 1,000개를 커널이 훑는다. 관심 집합을 등록해
// 두고 일어난 것만 받는 쪽으로 바꿨다. 구독자별 '쓸 게 있는가' 는 바뀔 때만 mod 한다.
//
// 설정은 파이썬과 같은 환경변수를 읽는다 (MDFEED_BUS_PATH, MDFEED_TCP_PORT, ...).
// 포트 0 을 주면 OS 가 고른 포트를 기동 로그 첫 줄(JSON)에 찍는다 — 테스트용.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mdfp/event_loop.hpp"
#include "mdfp/protocol.hpp"

#include <cstdarg>
#include <cctype>
#ifdef MSG_NOSIGNAL
#define MSG_NOSIGNAL_COMPAT MSG_NOSIGNAL
#else
#define MSG_NOSIGNAL_COMPAT 0
#endif

using namespace mdfp;
namespace {

// ── 설정 ────────────────────────────────────────────────────────────────────
std::string env_str(const char* k, const char* d) { const char* v = std::getenv(k); return (v && *v) ? v : d; }
long env_int(const char* k, long d) { const char* v = std::getenv(k); return (v && *v) ? std::atol(v) : d; }
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out; std::string cur;
    for (char c : s) { if (c == ',') { if (!cur.empty()) out.push_back(cur); cur.clear(); } else if (c != ' ') cur += c; }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

struct Cfg {
    std::vector<std::string> bus_paths;
    std::string tcp_host, http_host;
    int tcp_port, admin_port;
    size_t client_queue;
    uint64_t drop_limit;
    int tcp_sndbuf;   // 구독자 소켓 SO_SNDBUF (0 = 커널 기본). 리눅스는 자동조정으로 수 MB 까지 키운다
    Cfg() {
        bus_paths = split_csv(env_str("MDFEED_BUS_PATHS", ""));
        if (bus_paths.empty()) bus_paths.push_back(env_str("MDFEED_BUS_PATH", "/tmp/mdfeed/bus.sock"));
        tcp_host = env_str("MDFEED_TCP_HOST", "0.0.0.0");
        tcp_port = int(env_int("MDFEED_TCP_PORT", 9101));
        http_host = env_str("MDFEED_HTTP_HOST", "0.0.0.0");
        admin_port = int(env_int("MDFEED_TCP_ADMIN_PORT", 9111));
        client_queue = size_t(env_int("MDFEED_CLIENT_QUEUE", 2048));
        drop_limit = uint64_t(env_int("MDFEED_DROP_LIMIT", 5000));
        tcp_sndbuf = int(env_int("MDFEED_TCP_SNDBUF", 0));
    }
};

// ── 시간·로그 ──────────────────────────────────────────────────────────────
double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
uint64_t now_ns() { return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }
void logf(const char* level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
void logf(const char* level, const char* fmt, ...) {
    char buf[1024]; va_list ap; va_start(ap, fmt); std::vsnprintf(buf, sizeof buf, fmt, ap); va_end(ap);
    std::fprintf(stderr, "%.3f %s mdfeed.tcp_gateway.cpp: %s\n", mono(), level, buf);
}

void set_nonblock(int fd) { int fl = fcntl(fd, F_GETFL, 0); fcntl(fd, F_SETFL, fl | O_NONBLOCK); }
void set_nodelay(int fd) { int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one); }

// 소켓 송신 버퍼에 아직 못 나간 바이트. 파이썬 Subscriber.wire_bytes 와 같은 뜻 —
// 큐 깊이만 보면 밀림을 놓친다 (실측: 큐 0 인데 클라이언트 p99 363ms).
size_t wire_bytes(int fd) {
#if defined(__APPLE__)
    int n = 0; socklen_t len = sizeof n;
    return getsockopt(fd, SOL_SOCKET, SO_NWRITE, &n, &len) == 0 ? size_t(n) : 0;
#elif defined(__linux__)
    int n = 0;
    return ioctl(fd, TIOCOUTQ, &n) == 0 ? size_t(n) : 0;
#else
    (void)fd; return 0;
#endif
}

std::string json_escape(const std::string& s) {
    std::string o; o.reserve(s.size() + 2);
    for (char c : s) { if (c == '"' || c == '\\') { o += '\\'; o += c; } else if (uint8_t(c) < 0x20) o += ' '; else o += c; }
    return o;
}

// 최소 JSON 추출 — {"symbols":["A","B"],"mode":"conflate"} 만 다룬다.
std::optional<std::vector<std::string>> json_string_array(const std::string& j, const std::string& key) {
    auto k = j.find("\"" + key + "\""); if (k == std::string::npos) return std::nullopt;
    auto lb = j.find('[', k); if (lb == std::string::npos) return std::nullopt;
    auto rb = j.find(']', lb); if (rb == std::string::npos) return std::nullopt;
    std::vector<std::string> out; size_t i = lb + 1;
    while (i < rb) { auto q1 = j.find('"', i); if (q1 == std::string::npos || q1 >= rb) break;
        auto q2 = j.find('"', q1 + 1); if (q2 == std::string::npos || q2 > rb) break;
        out.push_back(j.substr(q1 + 1, q2 - q1 - 1)); i = q2 + 1; }
    return out;
}
std::optional<std::string> json_string(const std::string& j, const std::string& key) {
    auto k = j.find("\"" + key + "\""); if (k == std::string::npos) return std::nullopt;
    auto c = j.find(':', k); if (c == std::string::npos) return std::nullopt;
    auto q1 = j.find('"', c); if (q1 == std::string::npos) return std::nullopt;
    auto q2 = j.find('"', q1 + 1); if (q2 == std::string::npos) return std::nullopt;
    return j.substr(q1 + 1, q2 - q1 - 1);
}

// ── 구독자 ──────────────────────────────────────────────────────────────────
struct Cached { uint8_t msg_type; std::vector<uint8_t> payload; };

struct Subscriber {
    int fd = -1; uint64_t id = 0; std::string peer; double connected_at = 0;
    std::optional<std::unordered_set<std::string>> symbols;   // nullopt = 전체
    bool conflate = false;
    FrameParser in;                                             // 구독 요청 파서
    // stream: 인코딩된 프레임(seq 는 넣을 때 매김).
    // conflate: 순서 큐. key 가 있으면 pending 에서 보내는 순간의 최신값을 꺼내고,
    //           key 가 비면(하트비트·시그널) 항목이 든 페이로드를 그대로 보낸다. seq 는 보낼 때 매김.
    std::deque<std::vector<uint8_t>> queue;
    struct CItem { std::string key; uint8_t msg_type; uint16_t flags; std::vector<uint8_t> payload; };
    std::deque<CItem> key_queue;
    std::unordered_map<std::string, std::tuple<uint8_t, std::vector<uint8_t>, uint16_t>> pending;
    std::vector<uint8_t> wbuf; size_t woff = 0;                // 부분 전송 중인 버퍼
    uint64_t wframes = 0;                                       // wbuf 에 모인 프레임 수 (sent 집계용)
    bool armed_write = false;                                   // 이벤트 루프에 쓰기 관심을 등록해 뒀는가
    // [계측] 배치 시작(버스에서 묶음을 다 받은 시각) → 이 구독자에게 send() 가 끝난 시각.
    // 구독자별 지연 격차가 게이트웨이 **안**에 있는지 보려고 잰다.
    double send_delay_sum = 0; uint64_t send_delay_n = 0;
    uint64_t dropped = 0, sent = 0, out_seq = 0, conflated = 0;
    bool wants(const std::string& key) const { return !symbols || symbols->count(key) > 0; }
    size_t backlog() const { return conflate ? key_queue.size() : queue.size(); }
};

struct BusSource {
    std::string path; int fd = -1; bool connected = false; double next_try = 0, backoff = 1.0, last_at = 0;
    uint64_t frames = 0, restarts = 0; FrameParser parser;
};

// ── 게이트웨이 ──────────────────────────────────────────────────────────────
class Gateway {
public:
    explicit Gateway(Cfg c) : cfg_(std::move(c)) {}

    int run() {
        for (auto& p : cfg_.bus_paths) { BusSource s; s.path = p; sources_.push_back(std::move(s)); }
        listen_fd_ = listen_tcp(cfg_.tcp_host, cfg_.tcp_port, tcp_port_);
        admin_fd_ = listen_tcp(cfg_.http_host, cfg_.admin_port, admin_port_);
        if (listen_fd_ < 0 || admin_fd_ < 0) return 1;
        // 첫 줄은 기계가 읽는다 (테스트가 포트 0 으로 띄우고 실제 포트를 알아낸다)
        std::printf("{\"event\":\"listening\",\"service\":\"tcp-gateway\",\"impl\":\"c++\",\"event_loop\":\"%s\",\"tcp_port\":%d,\"admin_port\":%d}\n", EventLoop::backend(), tcp_port_, admin_port_);
        std::fflush(stdout);
        logf("INFO", "MDFP/1 배포 서버 listening on %s:%d (admin %d)", cfg_.tcp_host.c_str(), tcp_port_, admin_port_);
        started_ = mono();

        loop_.add(listen_fd_, true, false, tag(Kind::Listen, 0)); listen_armed_ = true;
        loop_.add(admin_fd_, true, false, tag(Kind::Admin, 0));
        while (!g_stop) {
            const double now = mono();
            for (auto& s : sources_) if (!s.connected && s.fd < 0 && now >= s.next_try) connect_bus(s);
            // fd 고갈 백오프: 리스너를 관심 집합에서 잠시 뺀다
            const bool want_listen = now >= accept_backoff_until_;
            if (want_listen != listen_armed_) { loop_.mod(listen_fd_, want_listen, false); listen_armed_ = want_listen; }

            int n = loop_.wait(500, [&](const Event& e) {
                const Owner o = untag(e.tag);
                short re = short((e.readable ? POLLIN : 0) | (e.writable ? POLLOUT : 0) | (e.error ? POLLERR : 0) | (e.hangup ? POLLHUP : 0));
                try {
                    switch (o.kind) {
                        case Kind::Listen: accept_sub(); break;
                        case Kind::Admin: accept_admin(); break;
                        case Kind::Bus: on_bus(sources_[o.id], re); break;
                        case Kind::Sub: { auto it = subs_.find(o.id); if (it != subs_.end()) { on_sub(it->second, re); if (subs_.count(o.id)) arm_sub(it->second); } break; }
                        case Kind::AdminConn: on_admin(int(o.id), re); break;
                    }
                } catch (const std::exception& ex) {
                    // 한 연결의 예외가 배포 전체를 죽이면 안 된다. 그 연결만 정리한다.
                    logf("ERROR", "이벤트 처리 예외 (kind=%d id=%llu): %s", int(o.kind), (unsigned long long)o.id, ex.what());
                    if (o.kind == Kind::Sub) to_close_.push_back(o.id);
                    else if (o.kind == Kind::AdminConn) { auto it = admin_conns_.find(int(o.id)); if (it != admin_conns_.end()) { loop_.del(it->first); close(it->first); admin_conns_.erase(it); } }
                    else if (o.kind == Kind::Bus) schedule_retry(sources_[o.id], ex.what());
                }
            });
            if (n < 0 && errno != EINTR) { logf("ERROR", "event loop: %s", std::strerror(errno)); break; }
            // 구독자 종료는 이벤트 처리 뒤에 한꺼번에 (순회 중 삭제 방지)
            for (uint64_t id : to_close_) close_sub(id);
            to_close_.clear();
            // 관리 연결 기한: 안 읽는 클라이언트가 fd 를 붙들고 있지 못하게
            for (auto it = admin_conns_.begin(); it != admin_conns_.end();) {
                if (mono() > it->second.deadline) { loop_.del(it->first); close(it->first); it = admin_conns_.erase(it); } else ++it;
            }
        }
        shutdown();
        return 0;
    }

    static volatile sig_atomic_t g_stop;

private:
    enum class Kind { Listen, Admin, Bus, Sub, AdminConn };
    struct Owner { Kind kind; uint64_t id; };

    Cfg cfg_;
    std::vector<BusSource> sources_;
    int listen_fd_ = -1, admin_fd_ = -1, tcp_port_ = 0, admin_port_ = 0;
    std::map<uint64_t, Subscriber> subs_;
    struct AdminConn { std::string in, out; size_t off = 0; double deadline = 0; };
    std::map<int, AdminConn> admin_conns_;
    std::vector<uint64_t> to_close_;
    EventLoop loop_; bool listen_armed_ = false;
    uint64_t rotate_cursor_ = 0;   // 팬아웃 시작 구독자. 배치마다 한 칸씩 민다 (for_each_sub_rotated)
    std::unordered_map<std::string, Cached> last_;
    uint64_t next_id_ = 0, frames_in_ = 0, connections_ = 0, dropped_total_ = 0, sent_total_ = 0, conflated_total_ = 0;
    uint64_t send_calls_ = 0, send_bytes_ = 0, send_eagain_ = 0, bus_reads_ = 0;   // 시스템 콜 비용을 보이게
    double started_ = 0, last_frame_at_ = 0; bool upstream_ok_ = false;
    double accept_backoff_until_ = 0, accept_log_after_ = 0;
    std::vector<uint8_t> scratch_;

    // 구독자를 매번 **다른 지점부터** 훑는다.
    //
    // 순차 팬아웃은 늘 같은 순서로 돌면 접속이 이른 구독자가 구조적으로 유리하다. 게이트웨이
    // 안에서 재보니 기울기가 완벽한 직선이었다(2026-09-23, 구독자 100명·상류 3,900 msg/s):
    // 배치 시작 → send() 완료가 첫 구독자 5.4µs, 마지막 357µs, id 와의 상관 +1.000.
    // 같은 값을 파는 피드에서 접속 순서가 지연 우선순위를 정하면 안 된다. 배치마다 시작점을 민다.
    template <class F>
    void for_each_sub_rotated(F&& fn) {
        if (subs_.empty()) return;
        auto it = subs_.lower_bound(rotate_cursor_);
        if (it == subs_.end()) it = subs_.begin();
        const auto start = it;
        do {
            fn(it->second);
            if (++it == subs_.end()) it = subs_.begin();
        } while (it != start);
        auto nxt = subs_.upper_bound(rotate_cursor_);
        rotate_cursor_ = (nxt == subs_.end()) ? subs_.begin()->first : nxt->first;
    }

    static uint64_t tag(Kind k, uint64_t id) { return (uint64_t(k) << 56) | (id & ((uint64_t(1) << 56) - 1)); }
    static Owner untag(uint64_t t) { return Owner{Kind(t >> 56), t & ((uint64_t(1) << 56) - 1)}; }
    // 구독자의 관심(읽기는 항상, 쓰기는 보낼 게 있을 때만)을 현재 상태에 맞춘다. 바뀔 때만 커널을 부른다.
    void arm_sub(Subscriber& s) {
        const bool wr = s.woff < s.wbuf.size() || s.backlog() > 0;
        if (wr != s.armed_write) { loop_.mod(s.fd, true, wr); s.armed_write = wr; }
    }

    static int listen_tcp(const std::string& host, int port, int& bound_port) {
        int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) { logf("ERROR", "socket: %s", std::strerror(errno)); return -1; }
        int one = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(uint16_t(port));
        if (host.empty()) a.sin_addr.s_addr = htonl(INADDR_ANY);
        else if (host == "localhost") a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        else if (inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) {
            // 조용히 0.0.0.0 으로 떨어지면 루프백만 열려던 관리 포트가 전 인터페이스에 노출된다. 실패가 맞다.
            logf("ERROR", "호스트를 해석할 수 없습니다: %s (IPv4 주소, localhost, 또는 빈 값)", host.c_str()); close(fd); return -1;
        }
        if (bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0 || listen(fd, 256) < 0) {
            logf("ERROR", "bind/listen %s:%d: %s", host.c_str(), port, std::strerror(errno)); close(fd); return -1; }
        socklen_t len = sizeof a; getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len); bound_port = ntohs(a.sin_port);
        set_nonblock(fd);
        return fd;
    }

    // ── 업스트림(버스) ────────────────────────────────────────────────
    void connect_bus(BusSource& s) {
        sockaddr_un a{}; a.sun_family = AF_UNIX;
        if (s.path.size() >= sizeof a.sun_path) { logf("ERROR", "UDS 소켓 경로가 너무 깁니다 (%zu): %s", s.path.size(), s.path.c_str()); s.next_try = mono() + 15; return; }
        std::strncpy(a.sun_path, s.path.c_str(), sizeof a.sun_path - 1);
        int fd = socket(AF_UNIX, SOCK_STREAM, 0); set_nonblock(fd);
        int r = connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
        if (r < 0 && errno != EINPROGRESS) { close(fd); schedule_retry(s, std::strerror(errno)); return; }
        s.fd = fd; s.connected = false;
        const size_t idx = size_t(&s - sources_.data());
        loop_.add(fd, r == 0, r != 0, tag(Kind::Bus, idx));   // 진행 중이면 쓰기(연결 완료), 됐으면 읽기
        if (r == 0) bus_connected(s);
    }
    void schedule_retry(BusSource& s, const char* why) {
        if (s.connected) ++s.restarts;
        s.connected = false; if (s.fd >= 0) { loop_.del(s.fd); close(s.fd); s.fd = -1; }
        logf("WARNING", "bus 연결 끊김/실패(%s): %s. %.1fs 후 재시도", s.path.c_str(), why, s.backoff);
        s.next_try = mono() + s.backoff; s.backoff = std::min(s.backoff * 2, 15.0);
        s.parser = FrameParser();
    }
    void bus_connected(BusSource& s) {
        s.connected = true; s.backoff = 1.0;
        loop_.mod(s.fd, true, false);
        const std::string hello = "{\"name\": \"tcp-gateway-cpp\"}\n";   // 발행자가 드롭을 이름으로 귀속시킨다
        ::send(s.fd, hello.data(), hello.size(), 0);
        logf("INFO", "bus subscriber connected to %s", s.path.c_str());
    }
    void on_bus(BusSource& s, short re) {
        if (!s.connected) {
            int err = 0; socklen_t l = sizeof err; getsockopt(s.fd, SOL_SOCKET, SO_ERROR, &err, &l);
            if (err || (re & (POLLERR | POLLHUP))) { schedule_retry(s, std::strerror(err ? err : ECONNRESET)); return; }
            bus_connected(s); return;
        }
        uint8_t buf[65536];
        for (;;) {
            ssize_t n = ::recv(s.fd, buf, sizeof buf, 0);
            if (n > 0) { ++bus_reads_; s.parser.feed(buf, size_t(n), [&](const FrameView& f) { on_frame(s, f); }); if (size_t(n) < sizeof buf) break; continue; }
            if (n == 0) { schedule_retry(s, "publisher closed"); return; }
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            schedule_retry(s, std::strerror(errno)); return;
        }
        // 버스에서 받은 묶음을 다 나눠 담은 뒤 구독자마다 한 번 쓴다. 프레임마다 쓰면
        // 버스트 때 send() 가 프레임 수 × 구독자 수만큼 나간다 (200명·30프레임 = 6,000회).
        // 파이썬 게이트웨이는 배치가 공평성을 해쳐 되돌렸지만(_send_loop 주석), 그건
        // 이벤트 루프 양보 문제였고 여기서는 구독자 순회 한 바퀴가 곧 공평한 분배다.
        // 구독자 순회 순서는 std::map 그대로(= 접속 순서)다. 한때 "늘 같은 순서로 쓰면 먼저
        // 접속한 구독자가 유리하다"고 보고 시작점을 배치마다 돌려 봤다. **아니었다.**
        // 구독자 100명·상류 3,900 msg/s 에서 3회씩 재니 첫/끝 구독자 p99 격차가
        // 고정 순서 1.40배, 회전 1.48배로 차이가 없었다(docs/data/fanout_fairness.json).
        // 효과가 없는 복잡도는 넣지 않는다. 격차의 원인은 아직 모른다 — README 결함 36.
        const double batch_t0 = mono();
        for_each_sub_rotated([&](Subscriber& sub) {
            if (sub.backlog() || sub.woff < sub.wbuf.size()) {
                flush(sub);
                sub.send_delay_sum += (mono() - batch_t0) * 1e6;   // µs
                ++sub.send_delay_n;
            }
            arm_sub(sub);
        });
    }
    static std::optional<std::string> key_of(const FrameView& f) {
        if (f.msg_type == MSG_TRADE && f.length >= Trade::SIZE) return unfix(f.payload + 16, 8) + ":" + unfix(f.payload, 16);
        if (f.msg_type == MSG_BOOK && f.length >= BookTop::SIZE) return unfix(f.payload + 16, 8) + ":" + unfix(f.payload, 16);
        return std::nullopt;   // 하트비트 등 — 필터 없이 전원에게
    }
    void on_frame(BusSource& s, const FrameView& f) {
        ++frames_in_; ++s.frames; s.last_at = last_frame_at_ = mono(); upstream_ok_ = true;
        auto key = key_of(f);
        if (key) { auto& c = last_[*key]; c.msg_type = f.msg_type; c.payload.assign(f.payload, f.payload + f.length); }
        fanout(f, key);
    }

    // ── 팬아웃 ────────────────────────────────────────────────────────
    bool drop_overflowed(Subscriber& s) {   // 넘친 구독자에서 오래된 것 하나를 버린다. 끊어야 하면 true
        if (s.conflate) { if (!s.key_queue.empty()) { if (!s.key_queue.front().key.empty()) s.pending.erase(s.key_queue.front().key); s.key_queue.pop_front(); } }
        else if (!s.queue.empty()) s.queue.pop_front();
        ++s.dropped; ++dropped_total_;
        if (s.dropped > cfg_.drop_limit) {
            logf("WARNING", "구독자 #%llu 드롭 %llu 초과 → 강제 종료", (unsigned long long)s.id, (unsigned long long)cfg_.drop_limit);
            to_close_.push_back(s.id); return true;
        }
        return false;
    }
    void fanout(const FrameView& f, const std::optional<std::string>& key) {
        for (auto& [id, s] : subs_) {
            if (std::find(to_close_.begin(), to_close_.end(), id) != to_close_.end()) continue;
            if (key && !s.wants(*key)) continue;
            if (s.conflate) {
                if (key) {
                    auto it = s.pending.find(*key);
                    if (it != s.pending.end()) {   // 같은 종목이 큐에 있으면 새 값으로 덮는다. seq 는 보낼 때 매긴다
                        it->second = {f.msg_type, std::vector<uint8_t>(f.payload, f.payload + f.length), f.flags};
                        ++s.conflated; ++conflated_total_; continue;
                    }
                }
                if (s.key_queue.size() >= cfg_.client_queue && drop_overflowed(s)) continue;
                if (key) {
                    s.pending[*key] = {f.msg_type, std::vector<uint8_t>(f.payload, f.payload + f.length), f.flags};
                    s.key_queue.push_back(Subscriber::CItem{*key, f.msg_type, f.flags, {}});
                } else {   // 하트비트 등은 합치지 않고 순서대로 보낸다 (파이썬과 동일)
                    s.key_queue.push_back(Subscriber::CItem{"", f.msg_type, f.flags, std::vector<uint8_t>(f.payload, f.payload + f.length)});
                }
            } else {
                scratch_.clear();
                encode_into(scratch_, f.msg_type, s.out_seq++, f.payload, f.length, f.flags);   // 구독자별 재번호
                if (s.queue.size() >= cfg_.client_queue && drop_overflowed(s)) continue;
                s.queue.push_back(scratch_);
            }
        }
    }

    // ── 다운스트림(TCP 구독자) ─────────────────────────────────────────
    void accept_sub() {
        for (;;) {
            sockaddr_in a{}; socklen_t l = sizeof a;
            int fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&a), &l);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                // fd 고갈(EMFILE/ENFILE/ENOBUFS): 대기 연결이 그대로 남아 poll 이 즉시 되돌아온다.
                // 리스너를 잠시 폴링에서 빼고, 로그는 한 번만 남긴다.
                if (mono() >= accept_log_after_) { logf("ERROR", "accept: %s — 200ms 동안 신규 접속을 받지 않는다", std::strerror(errno)); accept_log_after_ = mono() + 1.0; }
                accept_backoff_until_ = mono() + 0.2;
                return;
            }
            set_nonblock(fd); set_nodelay(fd);
            if (cfg_.tcp_sndbuf > 0) {
                // 커널 송신 버퍼가 크면 밀림이 우리 큐가 아니라 커널에 쌓여 백프레셔가 늦게 보인다.
                // 리눅스 루프백은 수 MB 까지 자동조정된다 — 시험이 커널 크기에 의존하지 않게 하는 조절값.
                setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &cfg_.tcp_sndbuf, sizeof cfg_.tcp_sndbuf);
            }
            char ip[64]; inet_ntop(AF_INET, &a.sin_addr, ip, sizeof ip);
            Subscriber s; s.fd = fd; s.id = next_id_++; s.peer = std::string(ip) + ":" + std::to_string(ntohs(a.sin_port)); s.connected_at = mono();
            ++connections_;
            send_snapshot(s);
            auto [it, _] = subs_.emplace(s.id, std::move(s));
            logf("INFO", "구독자 #%llu 접속 (%s). 현재 %zu명", (unsigned long long)it->second.id, it->second.peer.c_str(), subs_.size());
            loop_.add(it->second.fd, true, false, tag(Kind::Sub, it->second.id));
            flush(it->second); arm_sub(it->second);
        }
    }
    void send_snapshot(Subscriber& s) {   // 접속 즉시 최신값 전체 + 종료 메타. 증분 seq 는 next_seq 부터
        size_t n = 0;
        for (auto& [key, c] : last_) { if (!s.wants(key)) continue; encode_into(s.wbuf, c.msg_type, 0, c.payload.data(), c.payload.size(), FLAG_SNAPSHOT); ++n; }
        const std::string meta = "{\"snapshot_count\": " + std::to_string(n) + ", \"ts_ns\": " + std::to_string(now_ns()) + ", \"next_seq\": " + std::to_string(s.out_seq) + "}";
        encode_into(s.wbuf, MSG_SNAPSHOT, 0, reinterpret_cast<const uint8_t*>(meta.data()), meta.size(), FLAG_SNAPSHOT);
        s.woff = 0; s.wframes = n + 1;
    }
    static constexpr size_t COALESCE_BYTES = 64 * 1024;
    void flush(Subscriber& s) {
        for (;;) {
            if (s.woff >= s.wbuf.size()) {
                s.wbuf.clear(); s.woff = 0; s.wframes = 0;
                // 큐에 쌓인 프레임을 하나의 버퍼로 모은다 — 구독자당 send 한 번
                if (s.conflate) {
                    while (!s.key_queue.empty() && s.wbuf.size() < COALESCE_BYTES) {
                        auto item = std::move(s.key_queue.front()); s.key_queue.pop_front();
                        if (item.key.empty()) {   // 키 없는 프레임: 그대로
                            encode_into(s.wbuf, item.msg_type, s.out_seq++, item.payload.data(), item.payload.size(), item.flags); ++s.wframes; continue;
                        }
                        auto it = s.pending.find(item.key); if (it == s.pending.end()) continue;
                        auto& [t, p, fl] = it->second;
                        encode_into(s.wbuf, t, s.out_seq++, p.data(), p.size(), fl);   // 보내는 순간의 최신값에 seq
                        s.pending.erase(it); ++s.wframes;
                    }
                } else {
                    while (!s.queue.empty() && s.wbuf.size() < COALESCE_BYTES) {
                        auto& f = s.queue.front(); s.wbuf.insert(s.wbuf.end(), f.begin(), f.end()); s.queue.pop_front(); ++s.wframes;
                    }
                }
                if (s.wbuf.empty()) return;
            }
            ssize_t n = ::send(s.fd, s.wbuf.data() + s.woff, s.wbuf.size() - s.woff, MSG_NOSIGNAL_COMPAT);
            ++send_calls_;
            if (n > 0) { send_bytes_ += uint64_t(n); s.woff += size_t(n); if (s.woff >= s.wbuf.size()) { s.sent += s.wframes; sent_total_ += s.wframes; s.wframes = 0; } continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { ++send_eagain_; return; }   // 커널 버퍼가 찼다. POLLOUT 대기
            if (n < 0 && errno == EINTR) continue;
            to_close_.push_back(s.id); return;
        }
    }
    void on_sub(Subscriber& s, short re) {
        if (re & (POLLERR | POLLHUP | POLLNVAL)) { to_close_.push_back(s.id); return; }
        if (re & POLLIN) {
            uint8_t buf[4096];
            ssize_t n = ::recv(s.fd, buf, sizeof buf, 0);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) { to_close_.push_back(s.id); return; }
            if (n > 0) s.in.feed(buf, size_t(n), [&](const FrameView& f) { if (f.msg_type == MSG_SUBSCRIBE) apply_subscribe(s, std::string(reinterpret_cast<const char*>(f.payload), f.length)); });
        }
        if (re & POLLOUT) flush(s);
    }
    static bool looks_like_json_object(const std::string& j) {
        size_t a = j.find_first_not_of(" \t\r\n"), b = j.find_last_not_of(" \t\r\n");
        return a != std::string::npos && j[a] == '{' && j[b] == '}';
    }
    void apply_subscribe(Subscriber& s, const std::string& json) {
        if (!looks_like_json_object(json)) return;   // 파이썬: JSONDecodeError → 무시. 깨진 요청이 필터를 풀면 안 된다
        auto syms = json_string_array(json, "symbols");
        if (syms && !syms->empty()) s.symbols = std::unordered_set<std::string>(syms->begin(), syms->end()); else s.symbols.reset();
        auto mode = json_string(json, "mode");
        if (mode) for (auto& c : *mode) c = char(std::tolower(uint8_t(c)));
        if (mode && (*mode == "stream" || *mode == "conflate")) {
            const bool want = (*mode == "conflate");
            if (want != s.conflate) { s.queue.clear(); s.key_queue.clear(); s.pending.clear(); s.conflate = want; }   // 구독 변경 자체가 재동기 지점
        }
        logf("INFO", "구독자 #%llu 구독 변경 → %s (%s)", (unsigned long long)s.id, s.symbols ? std::to_string(s.symbols->size()).append(" symbols").c_str() : "ALL", s.conflate ? "conflate" : "stream");
    }
    void close_sub(uint64_t id) {
        auto it = subs_.find(id); if (it == subs_.end()) return;
        auto& s = it->second; loop_.del(s.fd); close(s.fd);
        logf("INFO", "구독자 #%llu 종료 (전송 %llu, 드롭 %llu). 남은 %zu명", (unsigned long long)id, (unsigned long long)s.sent, (unsigned long long)s.dropped, subs_.size() - 1);
        subs_.erase(it);
    }

    // ── 관리 HTTP (/healthz /readyz /metrics /subscribers) ─────────────
    void accept_admin() {
        for (;;) { int fd = accept(admin_fd_, nullptr, nullptr); if (fd < 0) return; set_nonblock(fd); admin_conns_[fd] = AdminConn{"", "", 0, mono() + 5.0}; loop_.add(fd, true, false, tag(Kind::AdminConn, uint64_t(fd))); }
    }
    void on_admin(int fd, short re) {
        auto it = admin_conns_.find(fd); if (it == admin_conns_.end()) return;
        AdminConn& ac = it->second;
        auto drop = [&] { loop_.del(fd); close(fd); admin_conns_.erase(it); };
        if (re & (POLLERR | POLLNVAL)) { drop(); return; }
        if (!ac.out.empty()) {   // 응답 쓰는 중 — 논블로킹. 막히면 다음 POLLOUT 에 이어 쓴다
            ssize_t n = ::send(fd, ac.out.data() + ac.off, ac.out.size() - ac.off, MSG_NOSIGNAL_COMPAT);
            if (n > 0) { ac.off += size_t(n); if (ac.off >= ac.out.size()) drop(); return; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
            drop(); return;
        }
        char buf[4096]; ssize_t n = ::recv(fd, buf, sizeof buf, 0);
        if (n == 0) { drop(); return; }
        if (n < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return; drop(); return; }
        ac.in.append(buf, size_t(n));
        if (ac.in.size() > 16 * 1024) { drop(); return; }   // 헤더 상한. 없으면 느린 클라이언트 하나로 메모리가 샌다
        auto end = ac.in.find("\r\n\r\n"); if (end == std::string::npos) return;
        // 요청 줄 파싱 — 어떤 입력에도 던지지 않는다. 한 줄의 쓰레기가 배포 전체를 죽이면 안 된다.
        const std::string line = ac.in.substr(0, ac.in.find("\r\n"));
        const size_t sp1 = line.find(' ');
        const size_t sp2 = sp1 == std::string::npos ? std::string::npos : line.find(' ', sp1 + 1);
        std::string method, target;
        if (sp1 != std::string::npos && sp2 != std::string::npos && sp2 > sp1 + 1) { method = line.substr(0, sp1); target = line.substr(sp1 + 1, sp2 - sp1 - 1); }
        auto q = target.find('?'); if (q != std::string::npos) target = target.substr(0, q);
        std::string body, ctype = "application/json; charset=utf-8"; int status = 200;
        if (method.empty() || target.empty() || target[0] != '/') { status = 400; body = "bad request"; ctype = "text/plain; charset=utf-8"; }
        else if (method != "GET" && method != "HEAD") { status = 405; body = "method not allowed"; ctype = "text/plain; charset=utf-8"; }
        else if (target == "/healthz") { body = health_json(); status = upstream_healthy() ? 200 : 503; }
        else if (target == "/readyz") { body = "{\"ready\": true}"; }
        else if (target == "/metrics") { body = metrics_text(); ctype = "text/plain; version=0.0.4; charset=utf-8"; }
        else if (target == "/subscribers") { body = subscribers_json(); }
        else { status = 404; body = "not found"; ctype = "text/plain; charset=utf-8"; }
        const char* reason = status == 200 ? "OK" : status == 400 ? "Bad Request" : status == 404 ? "Not Found" : status == 405 ? "Method Not Allowed" : "Service Unavailable";
        ac.out = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\nContent-Type: " + ctype + "\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n";
        if (method != "HEAD") ac.out += body;
        ac.off = 0; ac.in.clear();
        loop_.mod(fd, false, true);
        on_admin(fd, POLLOUT);   // 바로 한 번 써 본다
    }
    bool upstream_healthy() const { const double age = last_frame_at_ ? mono() - last_frame_at_ : -1; return upstream_ok_ && (age < 0 || age < 30.0); }
    std::string health_json() const {
        const double age = last_frame_at_ ? mono() - last_frame_at_ : -1;
        uint64_t total_dropped = 0; size_t max_backlog = 0, max_wire = 0;
        for (auto& [id, s] : subs_) { total_dropped += s.dropped; max_backlog = std::max(max_backlog, s.backlog()); max_wire = std::max(max_wire, wire_bytes(s.fd)); }
        std::string srcs, degraded; const double now = mono();
        for (auto& s : sources_) {
            const double a = s.last_at ? now - s.last_at : -1; const bool stale = s.frames > 0 && a >= 0 && a > 60.0;
            std::string name = s.path.substr(s.path.rfind('/') + 1);
            if (stale || !s.connected) degraded += (degraded.empty() ? "" : ", ") + ("\"" + json_escape(name) + "\"");
            srcs += (srcs.empty() ? "" : ", ") + std::string("{\"source\": \"") + json_escape(name) + "\", \"connected\": " + (s.connected ? "true" : "false") +
                ", \"frames\": " + std::to_string(s.frames) + ", \"restarts\": " + std::to_string(s.restarts) +
                ", \"last_frame_age_s\": " + (a < 0 ? "null" : std::to_string(a)) + ", \"stale\": " + (stale ? "true" : "false") + "}";
        }
        std::string out = "{\"service\": \"tcp-gateway\", \"impl\": \"c++\", \"event_loop\": \"" + std::string(EventLoop::backend()) + "\", \"healthy\": ";
        out += upstream_healthy() ? "true" : "false";
        out += ", \"uptime_s\": " + std::to_string(mono() - started_) + ", \"upstream_connected\": " + (upstream_ok_ ? "true" : "false");
        out += ", \"last_frame_age_s\": " + (age < 0 ? std::string("null") : std::to_string(age));
        out += ", \"frames_in\": " + std::to_string(frames_in_) + ", \"subscribers\": " + std::to_string(subs_.size()) + ", \"cached_symbols\": " + std::to_string(last_.size());
        out += ", \"total_dropped\": " + std::to_string(total_dropped) + ", \"max_backlog\": " + std::to_string(max_backlog) + ", \"max_wire_bytes\": " + std::to_string(max_wire);
        out += ", \"sources\": [" + srcs + "], \"degraded_sources\": [" + degraded + "], \"tasks\": {}}";
        return out;
    }
    // 팬아웃 공평성 지표: 구독자별 "배치 시작 → send() 완료" 평균의 최대/최소 비.
    // 1 에 가까워야 한다. 순차 팬아웃을 늘 같은 순서로 돌면 이 값이 수십 배가 된다
    // (실측: 구독자 100명에서 62배). 접속 순서가 지연 우선순위가 되지 않는지 보는 값이다.
    std::pair<double, double> fanout_delay_spread() const {
        double lo = 0, hi = 0; bool first = true;
        for (auto& [id, s] : subs_) {
            if (!s.send_delay_n) continue;
            const double m = s.send_delay_sum / double(s.send_delay_n);
            if (first) { lo = hi = m; first = false; } else { lo = std::min(lo, m); hi = std::max(hi, m); }
        }
        return {hi, (lo > 0) ? hi / lo : (first ? 0.0 : 1.0)};
    }
    std::string metrics_text() const {   // Prometheus text v0.0.4 — 파이썬 Registry.prometheus() 와 같은 이름
        size_t max_backlog = 0, max_wire = 0;
        for (auto& [id, s] : subs_) { max_backlog = std::max(max_backlog, s.backlog()); max_wire = std::max(max_wire, wire_bytes(s.fd)); }
        const auto [fan_max_us, fan_spread] = fanout_delay_spread();
        auto line = [](const char* name, double v) { char b[160]; std::snprintf(b, sizeof b, "mdfeed_%s{service=\"tcp-gateway\"} %g\n", name, v); return std::string(b); };
        return line("uptime_seconds", mono() - started_) + line("conflated_total", double(conflated_total_)) + line("connections_total", double(connections_)) +
            line("dropped_total", double(dropped_total_)) + line("frames_in_total", double(frames_in_)) + line("sent_total", double(sent_total_)) +
            line("max_backlog", double(max_backlog)) + line("max_wire_bytes", double(max_wire)) + line("subscribers", double(subs_.size())) +
            line("send_calls_total", double(send_calls_)) + line("send_bytes_total", double(send_bytes_)) + line("send_eagain_total", double(send_eagain_)) + line("bus_reads_total", double(bus_reads_)) +
            line("fanout_delay_max_us", fan_max_us) + line("fanout_delay_spread", fan_spread);
    }
    std::string subscribers_json() const {
        std::string items;
        for (auto& [id, s] : subs_) {
            std::string syms = "\"ALL\"";
            if (s.symbols) { std::vector<std::string> v(s.symbols->begin(), s.symbols->end()); std::sort(v.begin(), v.end()); syms = "["; for (size_t i = 0; i < v.size(); ++i) syms += (i ? ", \"" : "\"") + json_escape(v[i]) + "\""; syms += "]"; }
            items += (items.empty() ? "" : ", ") + std::string("{\"id\": ") + std::to_string(id) + ", \"peer\": \"" + json_escape(s.peer) + "\", \"symbols\": " + syms +
                ", \"sent\": " + std::to_string(s.sent) + ", \"dropped\": " + std::to_string(s.dropped) + ", \"mode\": \"" + (s.conflate ? "conflate" : "stream") +
                "\", \"out_seq\": " + std::to_string(s.out_seq) + ", \"backlog\": " + std::to_string(s.backlog()) + ", \"conflated\": " + std::to_string(s.conflated) +
                ", \"mean_send_delay_us\": " + std::to_string(s.send_delay_n ? s.send_delay_sum / double(s.send_delay_n) : 0.0) +
                ", \"wire_bytes\": " + std::to_string(wire_bytes(s.fd)) + ", \"uptime_s\": " + std::to_string(mono() - s.connected_at) + "}";
        }
        return "{\"count\": " + std::to_string(subs_.size()) + ", \"items\": [" + items + "]}";
    }

    void shutdown() {
        for (auto& [id, s] : subs_) close(s.fd);
        subs_.clear();
        for (auto& [fd, r] : admin_conns_) close(fd);
        for (auto& s : sources_) if (s.fd >= 0) close(s.fd);
        close(listen_fd_); close(admin_fd_);
        logf("INFO", "종료. 수신 %llu 프레임", (unsigned long long)frames_in_);
    }
};
volatile sig_atomic_t Gateway::g_stop = 0;
void on_signal(int) { Gateway::g_stop = 1; }

}  // namespace

int main() {
    signal(SIGPIPE, SIG_IGN);          // 끊긴 구독자에게 쓰다 프로세스가 죽으면 안 된다
    signal(SIGTERM, on_signal); signal(SIGINT, on_signal);
    return Gateway(Cfg{}).run();
}
