// 공유메모리 SPSC 링버퍼 — src/mdfeed/ringbuffer.py 와 바이트 단위로 호환 (헤더 전용, 의존성 0).
//
// 레이아웃(빅엔디언, 파이썬 struct "!4sIIIQQQ" 그대로):
//   헤더 64B: +0 magic "MDRB" | +4 slot_size u32 | +8 capacity u32 | +12 reserved | +16 write_seq u64 | +24 read_hint | +32 dropped
//   슬롯:     +0 seq_head u64 | +8 length u32 | +12 payload | slot_size-8 seq_tail u64
//
// 정책도 파이썬과 같다. **생산자는 절대 블로킹하지 않는다.** 소비자가 느리면 덮어쓰고 지나가고(lap),
// 소비자는 write_seq 와 자기 커서의 차이로 건너뛴 수를 센다. 락이 없으니 복사 도중 덮어쓰일 수 있어
// 슬롯 앞뒤의 seq(head/tail)가 같을 때만 그 슬롯을 믿는다 — Disruptor 계열의 고전 기법.
//
// 파이썬과 다른 점: 쓰기 순서에 메모리 펜스를 둔다(payload → tail → head 순으로 쓰고 write_seq 는 release).
// 파이썬 쪽은 GIL 과 memoryview 복사가 사실상 순서를 보장했지만, C++ 컴파일러와 CPU 는 재배치한다.
// 이름 규칙: 파이썬 multiprocessing.shared_memory 는 POSIX 에서 이름 앞에 '/' 를 붙인다. 여기서도 같다.
#pragma once
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "protocol.hpp"   // get_be*/put_be*

namespace mdfp {

class RingBuffer {
public:
    static constexpr size_t HDR_SIZE = 64, SLOT_HDR = 12, SLOT_TAIL = 8;
    static constexpr char MAGIC[4] = {'M', 'D', 'R', 'B'};

    // create=true: 새로 만든다(이전 세그먼트가 남아 있으면 지운다). false: 기존 세그먼트에 붙는다.
    RingBuffer(const std::string& name, uint32_t capacity = 8192, uint32_t slot_size = 128, bool create = false)
        : name_(name[0] == '/' ? name : "/" + name), owner_(create) {
        if (create) {
            shm_unlink(name_.c_str());
            int fd = shm_open(name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
            if (fd < 0) throw std::runtime_error("shm_open(create) failed: " + std::string(std::strerror(errno)));
            total_ = HDR_SIZE + size_t(capacity) * slot_size;
            if (ftruncate(fd, off_t(total_)) < 0) { close(fd); throw std::runtime_error("ftruncate failed"); }
            map(fd);
            std::memcpy(buf_, MAGIC, 4); put_be32(buf_ + 4, slot_size); put_be32(buf_ + 8, capacity); put_be32(buf_ + 12, 0);
            put_be64(buf_ + 16, 0); put_be64(buf_ + 24, 0); put_be64(buf_ + 32, 0);
            capacity_ = capacity; slot_size_ = slot_size;
        } else {
            int fd = shm_open(name_.c_str(), O_RDWR, 0600);
            if (fd < 0) throw std::runtime_error("shm_open failed: " + std::string(std::strerror(errno)));
            struct stat st{}; fstat(fd, &st); total_ = size_t(st.st_size);
            map(fd);
            if (std::memcmp(buf_, MAGIC, 4) != 0) throw std::runtime_error("공유메모리 " + name_ + " 가 링버퍼가 아니다");
            slot_size_ = get_be32(buf_ + 4); capacity_ = get_be32(buf_ + 8);
        }
        if (slot_size_ <= SLOT_HDR + SLOT_TAIL) throw std::runtime_error("slot_size가 슬롯 헤더보다 작다");
        payload_max_ = slot_size_ - SLOT_HDR - SLOT_TAIL;
    }
    ~RingBuffer() { if (buf_) munmap(buf_, total_); if (owner_) shm_unlink(name_.c_str()); }
    RingBuffer(const RingBuffer&) = delete; RingBuffer& operator=(const RingBuffer&) = delete;

    uint32_t capacity() const { return capacity_; }
    uint32_t slot_size() const { return slot_size_; }
    size_t payload_max() const { return payload_max_; }
    uint64_t write_seq() const { std::atomic_thread_fence(std::memory_order_acquire); return get_be64(buf_ + 16); }
    uint64_t dropped() const { return get_be64(buf_ + 32); }

    // ── 생산자: 블로킹 없이 슬롯 하나를 쓴다. 반환값은 부여된 seq ──
    uint64_t push(const uint8_t* payload, size_t n) {
        if (n > payload_max_) throw std::invalid_argument("payload " + std::to_string(n) + "B > slot payload " + std::to_string(payload_max_) + "B");
        const uint64_t seq = get_be64(buf_ + 16);
        uint8_t* slot = buf_ + HDR_SIZE + size_t(seq % capacity_) * slot_size_;
        put_be64(slot, seq); put_be32(slot + 8, uint32_t(n));
        if (n) std::memcpy(slot + SLOT_HDR, payload, n);
        put_be64(slot + slot_size_ - SLOT_TAIL, seq);
        std::atomic_thread_fence(std::memory_order_release);   // 슬롯 내용이 write_seq 보다 먼저 보이게
        put_be64(buf_ + 16, seq + 1);
        return seq;
    }

    // ── 소비자: 독립 커서. 여러 개가 같은 버퍼를 병렬로 읽는다 ──
    class Reader {
    public:
        uint64_t cursor, skipped = 0, torn = 0;
        explicit Reader(RingBuffer& r, int64_t start = -1) : cursor(start < 0 ? r.write_seq() : uint64_t(start)), ring_(r) {}
        // 완성된 항목을 out 에 넣는다 (각 항목은 payload 복사본). 반환값은 넣은 개수.
        size_t poll(std::vector<std::vector<uint8_t>>& out, size_t max_items = 256) {
            RingBuffer& r = ring_; const uint64_t write_seq = r.write_seq();
            const uint64_t behind = write_seq - cursor;
            if (behind > r.capacity_) { skipped += behind - r.capacity_; cursor = write_seq - r.capacity_; }   // 추월당했다
            size_t got = 0;
            while (cursor < write_seq && got < max_items) {
                const uint64_t seq = cursor;
                const uint8_t* slot = r.buf_ + HDR_SIZE + size_t(seq % r.capacity_) * r.slot_size_;
                const uint64_t head = get_be64(slot); const uint32_t n = get_be32(slot + 8);
                if (head != seq || n > r.payload_max_) { ++torn; ++cursor; continue; }
                std::vector<uint8_t> data(slot + SLOT_HDR, slot + SLOT_HDR + n);
                std::atomic_thread_fence(std::memory_order_acquire);
                if (get_be64(slot + r.slot_size_ - SLOT_TAIL) != seq) { ++torn; ++cursor; continue; }   // 복사 도중 덮어쓰였다
                out.push_back(std::move(data)); ++cursor; ++got;
            }
            return got;
        }
        uint64_t backlog() const { return ring_.write_seq() - cursor; }
    private:
        RingBuffer& ring_;
    };
    Reader reader() { return Reader(*this); }

private:
    std::string name_; bool owner_; uint8_t* buf_ = nullptr; size_t total_ = 0; uint32_t capacity_ = 0, slot_size_ = 0; size_t payload_max_ = 0;
    void map(int fd) {
        void* p = mmap(nullptr, total_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); close(fd);
        if (p == MAP_FAILED) throw std::runtime_error("mmap failed: " + std::string(std::strerror(errno)));
        buf_ = static_cast<uint8_t*>(p);
    }
};

}  // namespace mdfp
