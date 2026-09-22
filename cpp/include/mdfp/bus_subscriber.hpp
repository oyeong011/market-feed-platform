// UDS 버스 구독자 (논블로킹, 자동 재접속) — bus.py 의 UDSSubscriber 와 같은 배선.
// 접속 직후 {"name": ...}\n 으로 자기를 밝힌다. 발행자가 드롭을 이름으로 귀속시킨다.
#pragma once
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>

#include "protocol.hpp"

namespace mdfp {

class BusSubscriber {
public:
    std::string path, name;
    int fd = -1; bool connected = false;
    double next_try = 0, backoff = 1.0, last_at = 0;
    uint64_t frames = 0, restarts = 0;
    FrameParser parser;

    BusSubscriber(std::string p, std::string n) : path(std::move(p)), name(std::move(n)) {}

    static double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

    // 접속 시도. 성공/진행중이면 fd >= 0. 실패면 백오프 예약.
    void try_connect(std::string& err) {
        sockaddr_un a{}; a.sun_family = AF_UNIX;
        if (path.size() >= sizeof a.sun_path) { err = "UDS 경로가 너무 깁니다"; next_try = mono() + 15; return; }
        std::strncpy(a.sun_path, path.c_str(), sizeof a.sun_path - 1);
        int s = socket(AF_UNIX, SOCK_STREAM, 0);
        int fl = fcntl(s, F_GETFL, 0); fcntl(s, F_SETFL, fl | O_NONBLOCK);
        int r = connect(s, reinterpret_cast<sockaddr*>(&a), sizeof a);
        if (r < 0 && errno != EINPROGRESS) { err = std::strerror(errno); close(s); schedule_retry(); return; }
        fd = s; connected = false;
        if (r == 0) on_connected();
    }
    bool due() const { return !connected && fd < 0 && mono() >= next_try; }
    void schedule_retry() {
        if (connected) ++restarts;
        connected = false; if (fd >= 0) { close(fd); fd = -1; }
        next_try = mono() + backoff; backoff = std::min(backoff * 2, 15.0);
        parser = FrameParser();
    }
    void on_connected() {
        connected = true; backoff = 1.0;
        const std::string hello = "{\"name\": \"" + name + "\"}\n";
        ::send(fd, hello.data(), hello.size(), 0);
    }
    // 진행 중이던 connect 완료 확인. 실패면 false.
    bool finish_connect() {
        int err = 0; socklen_t l = sizeof err; getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &l);
        if (err) { schedule_retry(); return false; }
        on_connected(); return true;
    }
    // 읽을 수 있는 만큼 읽어 프레임마다 on_frame. 연결이 끊기면 재접속 예약하고 false.
    template <class F> bool drain(F&& on_frame) {
        uint8_t buf[65536];
        for (;;) {
            ssize_t n = ::recv(fd, buf, sizeof buf, 0);
            if (n > 0) { last_at = mono(); parser.feed(buf, size_t(n), [&](const FrameView& f) { ++frames; on_frame(f); }); if (size_t(n) < sizeof buf) return true; continue; }
            if (n == 0) { schedule_retry(); return false; }
            if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
            if (errno == EINTR) continue;
            schedule_retry(); return false;
        }
    }
};

}  // namespace mdfp
