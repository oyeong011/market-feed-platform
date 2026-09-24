// 이 프로세스의 자원 사용 — RSS 와 열린 fd 수. (헤더 전용, 의존성 0)
//
// 왜 필요한가: 누수는 오래 돌려야 보이고, 보려면 **모든 서비스가 같은 자리에 같은 이름으로**
// 자기 자원을 보고해야 한다. 파이썬 서비스는 /healthz 의 resources 로 내고 있었는데 C++ 서비스는
// 아무것도 안 냈다. 그래서 장시간 감시(bench/soak.py)가 C++ 프로세스를 건너뛰고 있었다 —
// 누수가 가장 보기 어려운 쪽(GC 가 없는 쪽)이 감시 밖에 있었던 셈이다.
//
// 값의 의미를 정확히 적는다.
//   * rss_mb: **현재** 상주 메모리. 리눅스는 /proc/self/statm, macOS 는 mach task_info 로 읽는다.
//     둘 다 실패하면 getrusage 의 peak RSS 로 떨어지는데, peak 은 줄지 않으므로 회복을 못 본다.
//     그때는 rss_is_peak=true 로 표시한다 — 모르는 값을 아는 척하지 않는다.
//   * fd_open: 열린 파일 디스크립터 수. /proc/self/fd (리눅스) 또는 /dev/fd (macOS) 를 센다.
#pragma once
#include <dirent.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstdio>
#include <string>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

namespace mdfp {

struct ProcStat { double rss_mb = 0; long fd_open = 0; bool rss_is_peak = false; };

inline ProcStat proc_stat() {
    ProcStat s;
#if defined(__linux__)
    if (FILE* f = std::fopen("/proc/self/statm", "r")) {
        long pages_total = 0, pages_rss = 0;
        if (std::fscanf(f, "%ld %ld", &pages_total, &pages_rss) == 2)
            s.rss_mb = double(pages_rss) * double(sysconf(_SC_PAGESIZE)) / 1e6;
        std::fclose(f);
    }
#elif defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS)
        s.rss_mb = double(info.resident_size) / 1e6;
#endif
    if (s.rss_mb <= 0) {   // 못 읽었으면 peak 으로 떨어지되, peak 이라고 말한다
        rusage ru{};
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
#if defined(__APPLE__)
            s.rss_mb = double(ru.ru_maxrss) / 1e6;      // macOS: 바이트
#else
            s.rss_mb = double(ru.ru_maxrss) / 1e3;      // 리눅스: KB
#endif
            s.rss_is_peak = true;
        }
    }
    const char* fd_dir =
#if defined(__linux__)
        "/proc/self/fd";
#else
        "/dev/fd";
#endif
    if (DIR* d = opendir(fd_dir)) {
        while (dirent* e = readdir(d)) {
            const std::string n = e->d_name;
            if (n != "." && n != "..") ++s.fd_open;
        }
        closedir(d);
        if (s.fd_open > 0) --s.fd_open;   // opendir 자신의 fd 는 뺀다
    }
    return s;
}

// /healthz 의 resources 블록. 파이썬 서비스와 **같은 키 이름**을 쓴다 —
// 이름이 다르면 같은 감시 도구가 못 읽는다.
inline std::string proc_stat_json() {
    const ProcStat s = proc_stat();
    char b[256];
    std::snprintf(b, sizeof b, "{\"rss_mb\": %.1f, \"fd_open\": %ld, \"rss_is_peak\": %s}",
                  s.rss_mb, s.fd_open, s.rss_is_peak ? "true" : "false");
    return b;
}

}  // namespace mdfp
