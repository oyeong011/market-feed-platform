// 이벤트 다중화 추상 — macOS kqueue / 리눅스 epoll / 그 외 poll (헤더 전용, 의존성 0).
//
// 왜 poll() 을 버리나: poll 은 부를 때마다 fd 배열 전체를 커널에 복사하고 커널은 전부 훑는다.
// 구독자 1,000명이면 이벤트 하나에 1,000개를 검사한다 — O(n) per wakeup. kqueue/epoll 은 관심 집합을
// 커널에 등록해 두고 **일어난 것만** 돌려준다 — O(활성 fd). 배포 게이트웨이처럼 fd 는 많고 한 번에
// 깨는 건 몇 개인 워크로드에서 차이가 난다.
//
// 인터페이스는 일부러 좁다. add(fd, want_read, want_write, tag) / mod / del / wait(timeout_ms, cb).
// 게이트웨이는 구독자별 "쓸 게 있는가" 가 자주 바뀌므로 mod 가 싸야 한다 — kqueue 는 EV_ADD 로 덮어쓰기,
// epoll 은 EPOLL_CTL_MOD 한 번이다. poll 폴백은 배열을 유지한다.
#pragma once
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

#if defined(__APPLE__) || defined(__FreeBSD__)
#define MDFP_KQUEUE 1
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#elif defined(__linux__)
#define MDFP_EPOLL 1
#include <sys/epoll.h>
#include <unistd.h>
#else
#define MDFP_POLL 1
#include <poll.h>
#endif

namespace mdfp {

struct Event { int fd; uint64_t tag; bool readable, writable, error, hangup; };

class EventLoop {
public:
    static const char* backend() {
#if defined(MDFP_KQUEUE)
        return "kqueue";
#elif defined(MDFP_EPOLL)
        return "epoll";
#else
        return "poll";
#endif
    }
    EventLoop() {
#if defined(MDFP_KQUEUE)
        kq_ = kqueue();
#elif defined(MDFP_EPOLL)
        ep_ = epoll_create1(EPOLL_CLOEXEC);
#endif
    }
    ~EventLoop() {
#if defined(MDFP_KQUEUE)
        if (kq_ >= 0) close(kq_);
#elif defined(MDFP_EPOLL)
        if (ep_ >= 0) close(ep_);
#endif
    }
    EventLoop(const EventLoop&) = delete; EventLoop& operator=(const EventLoop&) = delete;

    void add(int fd, bool rd, bool wr, uint64_t tag) { tags_[fd] = tag; apply(fd, rd, wr, true); }
    void mod(int fd, bool rd, bool wr) { apply(fd, rd, wr, false); }
    void del(int fd) {
        tags_.erase(fd);
#if defined(MDFP_KQUEUE)
        struct kevent ch[2]; EV_SET(&ch[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr); EV_SET(&ch[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, ch, 2, nullptr, 0, nullptr);   // 없는 필터의 삭제 오류는 무시
#elif defined(MDFP_EPOLL)
        epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
#else
        for (size_t i = 0; i < pfds_.size(); ++i) if (pfds_[i].fd == fd) { pfds_.erase(pfds_.begin() + long(i)); break; }
#endif
    }
    // 일어난 이벤트마다 cb(Event). 반환값은 이벤트 수(0 = 타임아웃, -1 = 오류/EINTR).
    template <class F> int wait(int timeout_ms, F&& cb) {
#if defined(MDFP_KQUEUE)
        struct kevent evs[512]; timespec ts{timeout_ms / 1000, (timeout_ms % 1000) * 1000000L};
        int n = kevent(kq_, nullptr, 0, evs, 512, &ts);
        if (n < 0) return -1;
        // kqueue 는 같은 fd 의 READ/WRITE 를 별개 항목으로 돌려준다. fd 별로 한 번만 콜백하도록 병합한다.
        merged_.clear();
        for (int i = 0; i < n; ++i) {
            const int fd = int(evs[i].ident); auto it = tags_.find(fd); if (it == tags_.end()) continue;
            auto [m, inserted] = merged_.try_emplace(fd, Event{fd, it->second, false, false, false, false});
            Event& e = m->second;
            if (evs[i].filter == EVFILT_READ) e.readable = true;
            if (evs[i].filter == EVFILT_WRITE) e.writable = true;
            if (evs[i].flags & EV_EOF) e.hangup = true;
            if (evs[i].flags & EV_ERROR) e.error = true;
            if (inserted) order_.push_back(fd);
        }
        for (int fd : order_) cb(merged_[fd]);
        order_.clear();
        return n;
#elif defined(MDFP_EPOLL)
        epoll_event evs[512];
        int n = epoll_wait(ep_, evs, 512, timeout_ms);
        if (n < 0) return -1;
        for (int i = 0; i < n; ++i) {
            const int fd = int(evs[i].data.u64 & 0xffffffffu); auto it = tags_.find(fd); if (it == tags_.end()) continue;
            cb(Event{fd, it->second, bool(evs[i].events & EPOLLIN), bool(evs[i].events & EPOLLOUT), bool(evs[i].events & EPOLLERR), bool(evs[i].events & (EPOLLHUP | EPOLLRDHUP))});
        }
        return n;
#else
        int n = ::poll(pfds_.data(), pfds_.size(), timeout_ms);
        if (n < 0) return -1;
        for (auto& p : pfds_) {
            if (!p.revents) continue; auto it = tags_.find(p.fd); if (it == tags_.end()) continue;
            cb(Event{p.fd, it->second, bool(p.revents & POLLIN), bool(p.revents & POLLOUT), bool(p.revents & (POLLERR | POLLNVAL)), bool(p.revents & POLLHUP)});
        }
        return n;
#endif
    }

private:
    std::unordered_map<int, uint64_t> tags_;
#if defined(MDFP_KQUEUE)
    int kq_ = -1; std::unordered_map<int, Event> merged_; std::vector<int> order_;
    void apply(int fd, bool rd, bool wr, bool /*is_add*/) {
        struct kevent ch[2];
        EV_SET(&ch[0], fd, EVFILT_READ, rd ? (EV_ADD | EV_ENABLE) : (EV_ADD | EV_DISABLE), 0, 0, nullptr);
        EV_SET(&ch[1], fd, EVFILT_WRITE, wr ? (EV_ADD | EV_ENABLE) : (EV_ADD | EV_DISABLE), 0, 0, nullptr);
        kevent(kq_, ch, 2, nullptr, 0, nullptr);
    }
#elif defined(MDFP_EPOLL)
    int ep_ = -1;
    void apply(int fd, bool rd, bool wr, bool is_add) {
        epoll_event ev{}; ev.events = (rd ? EPOLLIN : 0u) | (wr ? EPOLLOUT : 0u) | EPOLLRDHUP; ev.data.u64 = uint64_t(uint32_t(fd));
        if (epoll_ctl(ep_, is_add ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &ev) < 0 && is_add) epoll_ctl(ep_, EPOLL_CTL_MOD, fd, &ev);
    }
#else
    std::vector<pollfd> pfds_;
    void apply(int fd, bool rd, bool wr, bool /*is_add*/) {
        short ev = short((rd ? POLLIN : 0) | (wr ? POLLOUT : 0));
        for (auto& p : pfds_) if (p.fd == fd) { p.events = ev; return; }
        pfds_.push_back(pollfd{fd, ev, 0});
    }
#endif
};

}  // namespace mdfp
