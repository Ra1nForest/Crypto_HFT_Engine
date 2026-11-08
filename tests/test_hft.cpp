//
// test_hft.cpp
// HFT 基础库测试：SPSC 队列、对象池、开放寻址哈希表、HDR 直方图、计时工具与 LatencyTracker。
// 多线程用例（SPSC 压力测试、直方图读写并发）需要在 TSan 构建下也能通过。
// Tests for the HFT core library: SPSC queue, object pool, open-addressing hash map, HDR histogram,
// timing utilities and LatencyTracker.
// The multithreaded cases (SPSC stress tests, concurrent histogram reads and writes) must also pass in
// a TSan build.
//

// 启用库内调试断言（必须在包含任何 hft 头文件之前定义）
// Enable the library's debug assertions (must be defined before including any hft header)
#define HFT_ENABLE_ASSERTS

#include "test_framework.h"

#include "hft/common.h"
#include "hft/flat_hash_map.h"
#include "hft/latency_histogram.h"
#include "hft/object_pool.h"
#include "hft/spsc_queue.h"
#include "hft/timing.h"
#include "latency_tracker.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace {

constexpr std::uint64_t kU64Max = std::numeric_limits<std::uint64_t>::max();

// ============================================================================
// 通用辅助类型
// Common helper types
// ============================================================================

// 忙等退避：先 pause 自旋，再让出 CPU（共享机器上避免两个自旋线程互相饿死）
// Busy-wait backoff: spin with pause first, then yield the CPU (keeps two spinning threads from
// starving each other on a shared machine)
void backoff(unsigned& spins) {
    if (++spins < 64) {
        hft::cpuRelax();
    } else {
        spins = 0;
        std::this_thread::yield();
    }
}

// 统计存活实例数的类型（单线程用例使用）
// A type that counts its live instances (used by single-threaded cases)
struct Tracked {
    static inline int live = 0;
    int value;
    explicit Tracked(int v) : value(v) { ++live; }
    Tracked(const Tracked& o) : value(o.value) { ++live; }
    Tracked(Tracked&& o) noexcept : value(o.value) { ++live; }
    Tracked& operator=(const Tracked&) = default;
    Tracked& operator=(Tracked&&) noexcept = default;
    ~Tracked() { --live; }
    bool operator==(const Tracked& o) const { return value == o.value; }
};

// 没有默认构造函数的类型
// A type without a default constructor
struct NoDefault {
    NoDefault(int a, int b) : sum(a + b) {}
    int sum;
};
static_assert(!std::is_default_constructible_v<NoDefault>);

// 构造时可按需抛异常的类型
// A type whose constructor throws on demand
struct ThrowOnFlag {
    ThrowOnFlag(int v, bool throw_now) : value(v) {
        if (throw_now) throw std::runtime_error("ThrowOnFlag");
    }
    int value;
};

// 超对齐类型
// An over-aligned type
struct alignas(128) Wide {
    std::uint64_t v;
};

// 最近秩分位数（与 LatencyHistogram 使用同一定义：rank = ceil(q * n)，钳制到 [1, n]）
// Nearest-rank percentile (same definition as LatencyHistogram: rank = ceil(q * n), clamped to [1, n])
std::uint64_t exactPercentile(const std::vector<std::uint64_t>& sorted, double q) {
    const std::size_t n = sorted.size();
    if (n == 0) return 0;
    if (!(q > 0.0)) q = 0.0;
    if (q > 1.0) q = 1.0;
    const double r = std::ceil(q * static_cast<double>(n));
    std::size_t rank = r < 1.0 ? 1 : static_cast<std::size_t>(r);
    if (rank > n) rank = n;
    return sorted[rank - 1];
}

std::uint64_t absDiff(std::uint64_t a, std::uint64_t b) { return a > b ? a - b : b - a; }

}  // namespace

// ============================================================================
// common.h
// ============================================================================

TEST_CASE("common: nextPow2, isPow2, branch hints and cpuRelax") {
    static_assert(hft::kCacheLineSize == 64);
    static_assert(hft::nextPow2(0) == 1);
    static_assert(hft::nextPow2(1) == 1);
    static_assert(hft::nextPow2(2) == 2);
    static_assert(hft::nextPow2(3) == 4);
    static_assert(hft::nextPow2(1000) == 1024);
    const std::size_t top = std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1);
    const std::pair<std::size_t, std::size_t> cases[] = {
        {5, 8}, {1023, 1024}, {1024, 1024}, {1025, 2048}, {top - 1, top}, {top, top},
        {top + 1, top}, {std::numeric_limits<std::size_t>::max(), top}};  // 超过 top 的值不可表示：饱和为 top
                                                                          // values above top are not representable:
                                                                          // they saturate to top
    for (const auto& [n, want] : cases) CHECK_EQ(hft::nextPow2(n), want);
    for (std::size_t n = 1; n < 5000; ++n) {
        const std::size_t p = hft::nextPow2(n);
        REQUIRE(hft::isPow2(p));
        REQUIRE(p >= n);
        REQUIRE(p / 2 < n);
    }
    CHECK(!hft::isPow2(0));
    CHECK(hft::isPow2(1));
    CHECK(!hft::isPow2(6));
    CHECK(hft::isPow2(top));

    int taken = 0;
    for (int i = 0; i < 10; ++i) {
        if (HFT_LIKELY(i < 9)) ++taken;
        if (HFT_UNLIKELY(i == 9)) taken += 100;
        hft::cpuRelax();
    }
    CHECK_EQ(taken, 109);
}

// ============================================================================
// SpscQueue
// ============================================================================

TEST_CASE("spsc: capacity rounds up to a power of two with a minimum of 2") {
    const std::pair<std::size_t, std::size_t> cases[] = {{0, 2},    {1, 2},       {2, 2},       {3, 4},
                                                         {1000, 1024}, {1024, 1024}, {1025, 2048}};
    for (const auto& [requested, actual] : cases) CHECK_EQ(hft::SpscQueue<int>(requested).capacity(), actual);
    CHECK_THROWS(hft::SpscQueue<int>(std::numeric_limits<std::size_t>::max()));
    // 只读字段 + 两个下标 + 两个缓存副本，各占一个缓存行
    // Read-only fields + two indices + two cached copies, each on its own cache line
    static_assert(alignof(hft::SpscQueue<int>) == hft::kCacheLineSize);
    static_assert(sizeof(hft::SpscQueue<int>) == 5 * hft::kCacheLineSize);
}

TEST_CASE("spsc: full and empty boundaries keep FIFO order") {
    hft::SpscQueue<int> q(4);
    REQUIRE_EQ(q.capacity(), std::size_t{4});
    CHECK(q.empty());
    CHECK_EQ(q.size(), std::size_t{0});
    CHECK(q.front() == nullptr);
    int out = -1;
    CHECK(!q.tryPop(out));
    CHECK_EQ(out, -1);

    for (int i = 0; i < 4; ++i) CHECK(q.tryPush(i));
    CHECK_EQ(q.size(), std::size_t{4});
    CHECK(!q.tryPush(99));  // 满：不浪费槽位，容量 4 恰好放 4 个
                            // full: no slot is wasted, capacity 4 holds exactly 4
    CHECK(!q.tryEmplace(98));
    CHECK_EQ(q.size(), std::size_t{4});

    for (int i = 0; i < 4; ++i) {
        int* f = q.front();
        REQUIRE(f != nullptr);
        CHECK_EQ(*f, i);
        CHECK(q.front() == f);  // front 不消费
                                // front does not consume
        q.pop();
        CHECK_EQ(q.size(), static_cast<std::size_t>(3 - i));
    }
    CHECK(q.empty());
    CHECK(q.front() == nullptr);

    // 排空后容量可再次完整使用，且跨越了环形数组末尾
    // After draining, the full capacity can be used again, wrapping across the end of the ring array
    for (int i = 0; i < 3; ++i) CHECK(q.tryPush(10 + i));
    CHECK(q.tryPop(out));
    CHECK_EQ(out, 10);
    CHECK(q.tryPush(13));
    CHECK(q.tryPush(14));
    CHECK(!q.tryPush(15));
    for (int i = 11; i <= 14; ++i) {
        CHECK(q.tryPop(out));
        CHECK_EQ(out, i);
    }
    CHECK(!q.tryPop(out));
}

TEST_CASE("spsc: randomized single-thread push, reserving push, tryPop and front/pop match std::deque") {
    for (std::size_t cap : {std::size_t{2}, std::size_t{4}, std::size_t{8}, std::size_t{16}, std::size_t{64}}) {
        hft::SpscQueue<std::uint64_t> q(cap);
        REQUIRE_EQ(q.capacity(), cap);
        std::deque<std::uint64_t> model;
        std::mt19937_64 rng(cap);
        std::uint64_t next = 0, pushed = 0;
        for (int iter = 0; iter < 300000; ++iter) {
            const unsigned op = static_cast<unsigned>(rng() % 4);
            if (op < 2) {
                // op 0：普通推入；op 1：带预留的推入（reserve 含 >= 容量的情况）
                // op 0: plain push; op 1: push with a reserve (including reserve >= capacity)
                const std::size_t reserve = op == 0 ? 0 : static_cast<std::size_t>(rng() % (cap + 2));
                const bool ok = op == 0 ? q.tryPush(next) : q.tryPushReserving(next, reserve);
                REQUIRE_EQ(ok, reserve < cap && model.size() < cap - reserve);
                if (ok) {
                    model.push_back(next);
                    ++pushed;
                }
                ++next;
            } else if (op == 2) {
                std::uint64_t v = 0;
                const bool ok = q.tryPop(v);
                REQUIRE_EQ(ok, !model.empty());
                if (ok) {
                    REQUIRE_EQ(v, model.front());
                    model.pop_front();
                }
            } else {
                std::uint64_t* f = q.front();
                REQUIRE_EQ(f != nullptr, !model.empty());
                if (f != nullptr) {
                    REQUIRE_EQ(*f, model.front());
                    q.pop();
                    model.pop_front();
                }
            }
            REQUIRE_EQ(q.size(), model.size());
            REQUIRE_EQ(q.empty(), model.empty());
        }
        CHECK_GT(pushed / cap, std::uint64_t{1000});  // 下标绕环形数组至少上千圈
                                                      // indices lap the ring at least a thousand times
    }
}

TEST_CASE("spsc: move-only element type") {
    hft::SpscQueue<std::unique_ptr<int>> q(2);
    CHECK(q.tryPush(std::make_unique<int>(1)));
    CHECK(q.tryEmplace(std::make_unique<int>(2)));
    auto extra = std::make_unique<int>(3);
    CHECK(!q.tryPush(std::move(extra)));
    CHECK(extra != nullptr);  // 推入失败不得消费参数
                              // a failed push must not consume the argument
    std::unique_ptr<int> out;
    REQUIRE(q.tryPop(out));
    REQUIRE(out != nullptr);
    CHECK_EQ(*out, 1);
    std::unique_ptr<int>* f = q.front();
    REQUIRE(f != nullptr);
    CHECK_EQ(**f, 2);
    q.pop();
    CHECK(q.empty());
    CHECK(q.tryPush(std::move(extra)));
    CHECK(extra == nullptr);
    REQUIRE(q.tryPop(out));
    CHECK_EQ(*out, 3);
}

TEST_CASE("spsc: element type without a default constructor") {
    hft::SpscQueue<NoDefault> q(4);
    CHECK(q.tryEmplace(1, 2));
    CHECK(q.tryPush(NoDefault(3, 4)));
    const NoDefault copy(5, 6);
    CHECK(q.tryPush(copy));
    NoDefault out(0, 0);
    REQUIRE(q.tryPop(out));
    CHECK_EQ(out.sum, 3);
    REQUIRE(q.front() != nullptr);
    CHECK_EQ(q.front()->sum, 7);
    q.pop();
    REQUIRE(q.tryPop(out));
    CHECK_EQ(out.sum, 11);
    CHECK(q.empty());
}

TEST_CASE("spsc: pop and the destructor destroy elements, including ones straddling the ring end") {
    Tracked::live = 0;
    {
        hft::SpscQueue<Tracked> q(8);
        CHECK_EQ(Tracked::live, 0);  // 构造队列不构造任何元素
                                     // constructing the queue constructs no elements
        for (int i = 0; i < 6; ++i) REQUIRE(q.tryEmplace(i));
        CHECK_EQ(Tracked::live, 6);
        q.pop();
        CHECK_EQ(Tracked::live, 5);
        Tracked out(-1);
        CHECK_EQ(Tracked::live, 6);
        REQUIRE(q.tryPop(out));
        CHECK_EQ(out.value, 1);
        CHECK_EQ(Tracked::live, 5);  // 队列中 4 个 + out
                                     // 4 in the queue + out
        for (int i = 6; i < 10; ++i) REQUIRE(q.tryEmplace(i));  // 剩余元素跨越数组末尾
                                                                // the remaining elements straddle the array end
        CHECK_EQ(Tracked::live, 9);
        CHECK(!q.tryEmplace(99));
        CHECK_EQ(Tracked::live, 9);
    }
    CHECK_EQ(Tracked::live, 0);
}

TEST_CASE("spsc: a throwing constructor leaves the queue unchanged") {
    hft::SpscQueue<ThrowOnFlag> q(2);
    REQUIRE(q.tryEmplace(1, false));
    CHECK_THROWS(q.tryEmplace(2, true));
    CHECK_EQ(q.size(), std::size_t{1});
    REQUIRE(q.tryEmplace(3, false));
    CHECK(!q.tryEmplace(4, false));
    REQUIRE(q.front() != nullptr);
    CHECK_EQ(q.front()->value, 1);
    q.pop();
    REQUIRE(q.front() != nullptr);
    CHECK_EQ(q.front()->value, 3);
    q.pop();
    CHECK(q.empty());
}

TEST_CASE("spsc: over-aligned elements are stored aligned") {
    hft::SpscQueue<Wide> q(4);
    for (std::uint64_t i = 0; i < 11; ++i) {
        REQUIRE(q.tryPush(Wide{i}));
        Wide* f = q.front();
        REQUIRE(f != nullptr);
        CHECK_EQ(reinterpret_cast<std::uintptr_t>(f) % alignof(Wide), std::uintptr_t{0});
        CHECK_EQ(f->v, i);
        q.pop();
    }
}

namespace {

// 两线程压力测试：生产者按序号推入，消费者交替使用 tryPop 与 front/pop 并逐个校验
// Two-thread stress test: the producer pushes in sequence order, the consumer alternates between
// tryPop and front/pop and verifies each item
template <class T, class Make, class Verify>
std::uint64_t runSpscStress(std::size_t capacity, std::uint64_t items, Make make, Verify verify) {
    hft::SpscQueue<T> q(capacity);
    std::thread producer([&q, &make, items] {
        for (std::uint64_t i = 0; i < items; ++i) {
            T v = make(i);
            unsigned spins = 0;
            while (!q.tryPush(std::move(v))) backoff(spins);  // 失败时 v 未被移走，可重试
                                                              // on failure v is not moved from,
                                                              // so it can be retried
        }
    });
    std::uint64_t expected = 0, mismatches = 0;
    unsigned spins = 0;
    T out = make(0);
    while (expected < items) {
        T* f = nullptr;
        const bool got = (expected & 1) == 0 ? q.tryPop(out) : (f = q.front()) != nullptr;
        if (!got) {
            backoff(spins);
            continue;
        }
        mismatches += verify(f != nullptr ? *f : out, expected) ? 0 : 1;
        if (f != nullptr) q.pop();
        ++expected;
        spins = 0;
    }
    producer.join();
    if (!q.empty()) ++mismatches;
    return mismatches;
}

// 带堆内存的负载：让 TSan/ASan 检查元素内容（不只是下标）的发布是否正确
// Payload that owns heap memory: lets TSan/ASan check that the element contents (not just the
// indices) are published correctly
struct Payload {
    std::uint64_t seq = 0;
    std::string text;
    std::vector<std::uint64_t> data;
};

std::string payloadText(std::uint64_t i) { return "payload-" + std::to_string(i) + "-with-enough-text-to-heap-allocate"; }

}  // namespace

TEST_CASE("spsc: two-thread stress keeps sequence integrity (10M items at capacity 1024, 1M at capacity 2)") {
    const struct {
        std::size_t capacity;
        std::uint64_t items, mult;
    } runs[] = {{1024, 10'000'000, 1}, {2, 1'000'000, 2654435761u}};
    for (const auto& run : runs) {
        const std::uint64_t mult = run.mult;
        const std::uint64_t mismatches = runSpscStress<std::uint64_t>(
            run.capacity, run.items, [mult](std::uint64_t i) { return i * mult; },
            [mult](std::uint64_t v, std::uint64_t i) { return v == i * mult; });
        CHECK_EQ(mismatches, std::uint64_t{0});
    }
}

TEST_CASE("spsc: two-thread stress with heap-owning payloads (300k items)") {
    const std::uint64_t mismatches = runSpscStress<Payload>(
        16, 300'000,
        [](std::uint64_t i) { return Payload{i, payloadText(i), std::vector<std::uint64_t>(i % 7, i)}; },
        [](const Payload& p, std::uint64_t i) {
            return p.seq == i && p.text == payloadText(i) && p.data == std::vector<std::uint64_t>(i % 7, i);
        });
    CHECK_EQ(mismatches, std::uint64_t{0});
}

TEST_CASE("spsc: size is approximate but bounded while both sides run") {
    hft::SpscQueue<std::uint64_t> q(64);
    std::atomic<bool> done{false};
    std::thread producer([&q, &done] {
        for (std::uint64_t i = 0; i < 200'000; ++i) {
            unsigned spins = 0;
            while (!q.tryPush(i)) backoff(spins);
        }
        done.store(true, std::memory_order_release);
    });
    std::uint64_t bad = 0, popped = 0;
    std::uint64_t v = 0;
    while (!done.load(std::memory_order_acquire) || !q.empty()) {
        if (q.size() > q.capacity()) ++bad;
        if (q.tryPop(v)) {
            if (v != popped) ++bad;
            ++popped;
        }
    }
    producer.join();
    CHECK_EQ(bad, std::uint64_t{0});
    CHECK_EQ(popped, std::uint64_t{200'000});
}

TEST_CASE("spsc: tryPushReserving keeps reserve slots free; reserve 0 is tryPush; reserve >= capacity always fails") {
    hft::SpscQueue<int> q(4);
    CHECK(!q.tryPushReserving(1, 4));  // 预留不少于容量：空队列也推不进
                                       // reserve not below capacity: cannot push even into an empty queue
    CHECK(!q.tryPushReserving(1, 5));
    CHECK(!q.tryPushReserving(1, std::numeric_limits<std::size_t>::max()));
    CHECK(q.empty());
    CHECK(q.tryPushReserving(10, 3));  // reserve = capacity - 1：只有空队列能推入
                                       // reserve = capacity - 1: only an empty queue accepts a push
    CHECK(!q.tryPushReserving(11, 3));
    CHECK(q.tryPushReserving(11, 1));
    CHECK(q.tryPushReserving(12, 1));
    CHECK(!q.tryPushReserving(13, 1));  // 再推就只剩 0 个空位
                                        // one more push would leave 0 free slots
    CHECK_EQ(q.size(), std::size_t{3});
    CHECK(q.tryPushReserving(13, 0));  // 预留的最后一个槽位
                                       // the last slot, which was reserved
    CHECK(!q.tryPushReserving(14, 0));
    CHECK(!q.tryPush(14));
    int out = -1;
    REQUIRE(q.tryPop(out));
    CHECK_EQ(out, 10);
    CHECK(!q.tryPushReserving(14, 1));  // 消费者腾出一个：推入后会剩 0 个，仍然不行
                                        // the consumer frees one: the push would leave 0, still refused
    CHECK(q.tryPushReserving(14, 0));
    for (int want = 11; want <= 14; ++want) {
        REQUIRE(q.tryPop(out));
        CHECK_EQ(out, want);  // FIFO 不受预留影响
                              // reserving does not affect FIFO order
    }
    CHECK(q.empty());
}

TEST_CASE("spsc: with a consumer running, a reserve-0 push right after a successful reserve-1 push never fails") {
    // MarketDataFeed 依赖的性质：普通快照用 reserve = 1，紧接着的关键快照（空簿）用 reserve = 0，一定能推入
    // The property MarketDataFeed relies on: an ordinary snapshot uses reserve = 1, and the critical
    // snapshot (empty book) right after it uses reserve = 0 and always gets in
    for (std::size_t cap : {std::size_t{2}, std::size_t{8}}) {
        hft::SpscQueue<std::uint64_t> q(cap);
        constexpr std::uint64_t kPairs = 300'000;
        std::atomic<std::uint64_t> failures{0};
        std::thread producer([&q, &failures] {
            for (std::uint64_t i = 0; i < kPairs; ++i) {
                unsigned spins = 0;
                while (!q.tryPushReserving(2 * i, 1)) backoff(spins);
                if (!q.tryPushReserving(2 * i + 1, 0)) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    while (!q.tryPush(2 * i + 1)) backoff(spins);
                }
            }
        });
        std::uint64_t expected = 0, mismatches = 0;
        unsigned spins = 0;
        std::uint64_t v = 0;
        while (expected < 2 * kPairs) {
            if (q.tryPop(v)) {
                if (v != expected) ++mismatches;
                ++expected;
                spins = 0;
            } else {
                backoff(spins);
            }
        }
        producer.join();
        CHECK_EQ(failures.load(), std::uint64_t{0});
        CHECK_EQ(mismatches, std::uint64_t{0});
        CHECK(q.empty());
    }
}

// ============================================================================
// ObjectPool
// ============================================================================

TEST_CASE("pool: LIFO reuse of the most recently destroyed slot") {
    hft::ObjectPool<std::uint64_t> pool(8, false);
    std::uint64_t* a = pool.create(1);
    std::uint64_t* b = pool.create(2);
    std::uint64_t* c = pool.create(3);
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(c != nullptr);
    CHECK(a != b);
    CHECK(b != c);
    CHECK(a != c);
    pool.destroy(b);
    std::uint64_t* d = pool.create(4);
    CHECK(d == b);
    CHECK_EQ(*d, std::uint64_t{4});
    pool.destroy(a);
    pool.destroy(c);
    CHECK(pool.create(5) == c);  // 最后释放的最先复用
                                 // last freed is reused first
    CHECK(pool.create(6) == a);
    CHECK_EQ(*a, std::uint64_t{6});
    CHECK_EQ(*b, std::uint64_t{4});
    CHECK_EQ(*c, std::uint64_t{5});
}

TEST_CASE("pool: fresh slots are handed out in increasing address order") {
    hft::ObjectPool<std::uint64_t> pool(32, false);
    std::uintptr_t prev = reinterpret_cast<std::uintptr_t>(pool.create(0));
    for (std::uint64_t i = 1; i < 32; ++i) {
        const std::uintptr_t p = reinterpret_cast<std::uintptr_t>(pool.create(i));
        CHECK_GT(p, prev);
        prev = p;
    }
    CHECK(pool.create(99) == nullptr);
}

TEST_CASE("pool: growth keeps existing objects at stable addresses") {
    hft::ObjectPool<std::uint64_t> pool(4, true);
    std::vector<std::uint64_t*> ptrs;
    for (std::uint64_t i = 0; i < 5000; ++i) {
        std::uint64_t* p = pool.create(i * 7);
        REQUIRE(p != nullptr);
        ptrs.push_back(p);
    }
    CHECK_EQ(pool.size(), std::size_t{5000});
    CHECK_GE(pool.capacity(), std::size_t{5000});
    const std::set<std::uint64_t*> unique(ptrs.begin(), ptrs.end());
    CHECK_EQ(unique.size(), ptrs.size());
    for (std::size_t i = 0; i < ptrs.size(); ++i) {
        REQUIRE_EQ(*ptrs[i], static_cast<std::uint64_t>(i) * 7);  // 扩容未移动任何对象
                                                                  // growth moved no object
        REQUIRE(pool.owns(ptrs[i]));
    }
    std::uint64_t not_from_pool = 0;
    CHECK(!pool.owns(&not_from_pool));
    for (std::uint64_t* p : ptrs) pool.destroy(p);
    CHECK_EQ(pool.size(), std::size_t{0});
}

TEST_CASE("pool: a non-growable pool returns nullptr when exhausted; zero initial capacity") {
    hft::ObjectPool<int> pool(3, false);
    int* a = pool.create(1);
    int* b = pool.create(2);
    int* c = pool.create(3);
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(c != nullptr);
    CHECK(pool.create(4) == nullptr);
    CHECK_EQ(pool.size(), std::size_t{3});
    CHECK_EQ(pool.capacity(), std::size_t{3});
    CHECK_EQ(pool.available(), std::size_t{0});
    pool.destroy(b);
    int* d = pool.create(5);
    CHECK(d == b);
    CHECK_EQ(*d, 5);
    CHECK(pool.create(6) == nullptr);
    pool.reserve(5);  // 显式 reserve 不受 growable 限制
                      // an explicit reserve ignores growable
    CHECK_EQ(pool.capacity(), std::size_t{5});
    CHECK(pool.create(7) != nullptr);
    CHECK(pool.create(8) != nullptr);
    CHECK(pool.create(9) == nullptr);
    CHECK_EQ(pool.size(), std::size_t{5});

    hft::ObjectPool<int> fixed(0, false);
    CHECK_EQ(fixed.capacity(), std::size_t{0});
    CHECK(fixed.create(1) == nullptr);
    hft::ObjectPool<int> growing(0, true);
    int* p = growing.create(42);
    REQUIRE(p != nullptr);
    CHECK_EQ(*p, 42);
    CHECK_GE(growing.capacity(), std::size_t{1});
    growing.destroy(p);
}

TEST_CASE("pool: size accounting, reserve and nullptr destroy") {
    hft::ObjectPool<double> pool(10, true);
    CHECK_EQ(pool.size(), std::size_t{0});
    CHECK_EQ(pool.capacity(), std::size_t{10});
    pool.reserve(4);  // 已满足，无操作
                      // already satisfied, no-op
    CHECK_EQ(pool.capacity(), std::size_t{10});
    pool.reserve(100);
    CHECK_EQ(pool.capacity(), std::size_t{100});
    std::vector<double*> v;
    for (int i = 0; i < 100; ++i) v.push_back(pool.create(i * 0.5));
    CHECK_EQ(pool.size(), std::size_t{100});
    CHECK_EQ(pool.capacity(), std::size_t{100});  // reserve 之后不需要扩容
                                                  // no growth needed after reserve
    for (int i = 0; i < 40; ++i) pool.destroy(v[static_cast<std::size_t>(i)]);
    CHECK_EQ(pool.size(), std::size_t{60});
    CHECK_EQ(pool.available(), std::size_t{40});
    pool.destroy(nullptr);  // 空操作
                            // no-op
    CHECK_EQ(pool.size(), std::size_t{60});
    for (int i = 40; i < 100; ++i) CHECK_EQ(*v[static_cast<std::size_t>(i)], i * 0.5);
}

TEST_CASE("pool: constructor arguments are forwarded and destroy runs the destructor") {
    Tracked::live = 0;
    hft::ObjectPool<Tracked> pool(4);
    Tracked* t = pool.create(17);
    REQUIRE(t != nullptr);
    CHECK_EQ(t->value, 17);
    CHECK_EQ(Tracked::live, 1);
    pool.destroy(t);
    CHECK_EQ(Tracked::live, 0);
    hft::ObjectPool<NoDefault> nd(2, false);
    NoDefault* n = nd.create(20, 22);
    REQUIRE(n != nullptr);
    CHECK_EQ(n->sum, 42);
    nd.destroy(n);
}

TEST_CASE("pool: the destructor destroys objects that are still live") {
    Tracked::live = 0;
    {
        hft::ObjectPool<Tracked> pool(4, true);
        std::vector<Tracked*> objs;
        for (int i = 0; i < 100; ++i) objs.push_back(pool.create(i));  // 多次扩容，多个块
                                                                       // several growths, several blocks
        pool.reserve(300);                                             // 再加一个只有空闲槽位的块
                                                                       // plus one block with only free slots
        for (std::size_t i = 0; i < objs.size(); i += 3) pool.destroy(objs[i]);
        CHECK_EQ(Tracked::live, 66);
        CHECK_EQ(pool.size(), std::size_t{66});
    }
    CHECK_EQ(Tracked::live, 0);
    // 持有堆内存的对象：未 destroy 也不会泄漏（ASan 构建下由 LeakSanitizer 验证）
    // Objects that own heap memory: no leak even without destroy (checked by LeakSanitizer in ASan builds)
    {
        hft::ObjectPool<std::string> pool(2, true);
        for (int i = 0; i < 10; ++i) pool.create("a string that is long enough to live on the heap #" + std::to_string(i));
    }
}

TEST_CASE("pool: a throwing constructor returns the slot to the pool") {
    hft::ObjectPool<ThrowOnFlag> pool(2, false);
    ThrowOnFlag* a = pool.create(1, false);
    REQUIRE(a != nullptr);
    CHECK_THROWS(pool.create(2, true));
    CHECK_EQ(pool.size(), std::size_t{1});
    ThrowOnFlag* b = pool.create(3, false);
    REQUIRE(b != nullptr);
    CHECK(b != a);
    CHECK_EQ(b->value, 3);
    CHECK(pool.create(4, false) == nullptr);
    CHECK_EQ(a->value, 1);
}

TEST_CASE("pool: over-aligned objects are aligned") {
    hft::ObjectPool<Wide> pool(3, true);
    std::vector<Wide*> v;
    for (std::uint64_t i = 0; i < 200; ++i) {
        Wide* w = pool.create(Wide{i});
        REQUIRE(w != nullptr);
        REQUIRE_EQ(reinterpret_cast<std::uintptr_t>(w) % alignof(Wide), std::uintptr_t{0});
        v.push_back(w);
    }
    for (std::uint64_t i = 0; i < 200; ++i) CHECK_EQ(v[i]->v, i);
}

TEST_CASE("pool: randomized create/destroy keeps live objects intact and unique") {
    using Obj = std::array<std::uint64_t, 3>;
    hft::ObjectPool<Obj> pool(16, true);
    std::vector<std::pair<Obj*, std::uint64_t>> live;
    std::unordered_set<Obj*> addresses;
    std::mt19937_64 rng(7);
    for (int iter = 0; iter < 300000; ++iter) {
        if (live.empty() || rng() % 100 < 52) {
            const std::uint64_t tag = rng();
            Obj* p = pool.create(Obj{tag, ~tag, tag ^ 0x5555555555555555ULL});
            REQUIRE(p != nullptr);
            REQUIRE(addresses.insert(p).second);  // 存活对象的地址互不相同
                                                  // live objects have distinct addresses
            live.emplace_back(p, tag);
        } else {
            const std::size_t idx = static_cast<std::size_t>(rng() % live.size());
            auto [p, tag] = live[idx];
            REQUIRE_EQ((*p)[0], tag);
            REQUIRE_EQ((*p)[1], ~tag);
            REQUIRE_EQ((*p)[2], tag ^ 0x5555555555555555ULL);
            pool.destroy(p);
            addresses.erase(p);
            live[idx] = live.back();
            live.pop_back();
        }
        REQUIRE_EQ(pool.size(), live.size());
    }
    for (auto& [p, tag] : live) {
        REQUIRE_EQ((*p)[0], tag);
        pool.destroy(p);
    }
    CHECK_EQ(pool.size(), std::size_t{0});
}

// ============================================================================
// FlatHashMap
// ============================================================================

namespace {

enum class Side : std::uint8_t { Buy = 0, Sell = 1, Other = 255 };

// 值类型是外围类里带默认成员初始化器的嵌套类型（OkxAdapter::LevelText 的用法）：表在外围类完整之前
// 就作为成员被实例化，operator[] 仍须能值初始化新元素，哨兵键也一样（clang 曾因此编译失败）
// The value type is a nested type with default member initializers inside the enclosing class (as
// used by OkxAdapter::LevelText): the map is instantiated as a member before the enclosing class is
// complete, and operator[] must still be able to value-initialize new elements, for the sentinel key
// too (clang once failed to compile this)
struct NestedValueOwner {
    struct Text {
        int len = 7;
    };
    hft::FlatHashMap<std::uint64_t, Text> map{4};
};

// 用三种方式之一删除 key（how % 3：erase、extract、find + eraseAt）；返回被删除的值，键不存在时为 nullopt
// Erases key in one of three ways (how % 3: erase, extract, find + eraseAt); returns the erased value,
// or nullopt if the key is absent
template <class K, class V>
std::optional<V> eraseVia(hft::FlatHashMap<K, V>& m, K key, std::uint64_t how) {
    if (how % 3 == 1) return m.extract(key);
    const V* p = m.find(key);
    std::optional<V> value;
    if (p != nullptr) value.emplace(*p);
    if (how % 3 == 0) {
        REQUIRE_EQ(m.erase(key), value.has_value());
    } else if (p != nullptr) {
        m.eraseAt(p);
    }
    return value;
}

// 逐项比较 FlatHashMap 与 std::unordered_map，并校验内部不变量
// Compares FlatHashMap with std::unordered_map entry by entry and checks the internal invariants
template <class Map, class Ref>
void requireSameContents(const Map& m, const Ref& ref) {
    REQUIRE_EQ(m.size(), ref.size());
    std::size_t visited = 0;
    bool all_match = true;
    m.forEach([&](auto k, const auto& v) {
        ++visited;
        const auto it = ref.find(k);
        if (it == ref.end() || !(it->second == v)) all_match = false;
    });
    REQUIRE_EQ(visited, ref.size());
    REQUIRE(all_match);
    for (const auto& kv : ref) {
        const auto* p = m.find(kv.first);
        REQUIRE(p != nullptr);
        REQUIRE(*p == kv.second);
    }
    std::string why;
    if (!m.checkInvariants(&why)) FAIL(why);
}

// 一步操作中插入与删除所占的百分比（其余：5% operator[]、5% 先预留再填写或撤销、剩下的是查找）
// Percentages of insert and erase operations per step (the rest: 5% operator[], 5% reserve then fill
// or undo, the remainder lookups)
struct OpMix {
    int insert_pct, erase_pct;
};

auto constantMix(int insert_pct, int erase_pct) {
    return [=](std::size_t) { return OpMix{insert_pct, erase_pct}; };
}

// 差分测试：同一随机操作序列同时作用于 FlatHashMap 与 std::unordered_map，每一步比较结果。
// mix(i) 给出第 i 步的操作比例；删除轮流使用 erase / extract / find + eraseAt。
// Differential test: the same random operation sequence is applied to both FlatHashMap and
// std::unordered_map, comparing the results at every step.
// mix(i) gives the operation mix of step i; erases take turns using erase / extract / find + eraseAt.
template <class K, class KeyFn, class MixFn>
void runDifferential(std::uint64_t seed, std::size_t ops, std::size_t initial_capacity, KeyFn next_key, MixFn mix) {
    hft::FlatHashMap<K, std::uint64_t> m(initial_capacity);
    std::unordered_map<K, std::uint64_t> ref;
    std::mt19937_64 rng(seed);
    for (std::size_t i = 0; i < ops; ++i) {
        const K key = next_key(rng);
        const OpMix p = mix(i);
        const int r = static_cast<int>(rng() % 100);
        if (r < p.insert_pct) {
            const std::uint64_t val = rng();
            const auto res = m.insert(key, val);
            const auto ref_res = ref.emplace(key, val);
            REQUIRE_EQ(res.second, ref_res.second);
            REQUIRE(res.first != nullptr);
            REQUIRE_EQ(*res.first, ref_res.first->second);
        } else if (r < p.insert_pct + p.erase_pct) {
            const std::optional<std::uint64_t> got = eraseVia(m, key, rng());
            const auto it = ref.find(key);
            REQUIRE_EQ(got.has_value(), it != ref.end());
            if (got.has_value()) {
                REQUIRE_EQ(*got, it->second);
                ref.erase(it);
            }
        } else if (r < p.insert_pct + p.erase_pct + 5) {
            m[key] += 1;  // operator[]：不存在时值初始化为 0
                          // operator[]: value-initialized to 0 when absent
            ref[key] += 1;
            REQUIRE_EQ(*m.find(key), ref[key]);
        } else if (r < p.insert_pct + p.erase_pct + 10) {
            // 一次探测完成"查重 + 预留"：插入占位值，之后经指针写入真正的值，或用 eraseAt 撤销预留
            // One probe does both "duplicate check + reservation": insert a placeholder value, then either write
            // the real value through the pointer or undo the reservation with eraseAt
            const auto [slot, fresh] = m.insert(key, 0);
            REQUIRE_EQ(fresh, ref.count(key) == 0);
            if (fresh && rng() % 2 == 0) {
                *slot = rng();
                ref.emplace(key, *slot);
            } else if (fresh) {
                m.eraseAt(slot);
            }
        } else {
            const std::uint64_t* found = m.find(key);
            const auto it = ref.find(key);
            REQUIRE_EQ(found != nullptr, it != ref.end());
            if (found != nullptr) REQUIRE_EQ(*found, it->second);
            REQUIRE_EQ(m.contains(key), it != ref.end());
        }
        REQUIRE_EQ(m.size(), ref.size());
        if ((i & 0x3FFF) == 0) requireSameContents(m, ref);
    }
    requireSameContents(m, ref);
    // 通过 forEach 收集剩余键并全部删除
    // Collect the remaining keys through forEach and erase them all
    std::vector<K> keys;
    m.forEach([&keys](K k, const std::uint64_t&) { keys.push_back(k); });
    REQUIRE_EQ(keys.size(), ref.size());
    for (std::size_t i = 0; i < keys.size(); ++i) {
        REQUIRE(eraseVia(m, keys[i], i).has_value());
        ref.erase(keys[i]);
    }
    REQUIRE(m.empty());
    requireSameContents(m, ref);
}

// 找一个桶下标（在 bucket_count 个桶的表中）为 home 的键，从 start 开始搜索
// Finds a key whose bucket index (in a table of bucket_count buckets) is home, searching from start
template <class Map>
std::uint64_t keyWithHome(std::size_t home, std::size_t bucket_count, std::uint64_t start) {
    for (std::uint64_t k = start;; ++k) {
        if ((Map::hash(k) & (bucket_count - 1)) == home) return k;
    }
}

}  // namespace

TEST_CASE("flat_map: basic insert/find/erase/operator[]") {
    hft::FlatHashMap<std::uint64_t, int> m;
    CHECK(m.empty());
    CHECK_EQ(m.size(), std::size_t{0});
    CHECK_GE(m.capacity(), std::size_t{16});
    CHECK(m.find(10) == nullptr);
    CHECK(!m.erase(10));

    const auto r1 = m.insert(10, 100);
    CHECK(r1.second);
    REQUIRE(r1.first != nullptr);
    CHECK_EQ(*r1.first, 100);
    const auto r2 = m.insert(10, 200);  // 不覆盖
                                        // does not overwrite
    CHECK(!r2.second);
    CHECK(r2.first == r1.first);
    CHECK_EQ(*r2.first, 100);
    CHECK(m.contains(10));
    CHECK(!m.contains(11));

    m[11] = 5;
    CHECK_EQ(m[11], 5);
    CHECK_EQ(m[12], 0);  // 值初始化
                         // value-initialized
    CHECK_EQ(m.size(), std::size_t{3});
    *m.find(10) = 7;
    CHECK_EQ(*m.find(10), 7);

    CHECK(m.erase(10));
    CHECK(!m.erase(10));
    CHECK(!m.contains(10));
    CHECK_EQ(m.size(), std::size_t{2});
    const auto& cm = m;
    REQUIRE(cm.find(11) != nullptr);
    CHECK_EQ(*cm.find(11), 5);
    CHECK(cm.contains(12));

    const std::size_t buckets = m.bucketCount();
    m.clear();
    CHECK(m.empty());
    CHECK_EQ(m.bucketCount(), buckets);  // clear 保留内存
                                         // clear keeps the memory
    CHECK(m.find(11) == nullptr);
    CHECK(m.insert(11, 1).second);
    std::string why;
    CHECK(m.checkInvariants(&why));
}

TEST_CASE("flat_map: extract, eraseAt and reserve-then-fill on ordinary and sentinel keys") {
    using Map = hft::FlatHashMap<std::uint64_t, std::string>;
    Map m(0);
    for (const std::uint64_t k : {std::uint64_t{1}, kU64Max}) {  // 普通键与哨兵键（表外专用槽位）
                                                                 // an ordinary key and the sentinel key
                                                                 // (dedicated slot outside the table)
        const std::string text = "value-" + std::to_string(k);
        CHECK(!m.extract(k).has_value());
        const auto [slot, fresh] = m.insert(k, std::string());  // 先插入占位值预留槽位
                                                                // first insert a placeholder to reserve the slot
        REQUIRE(fresh);
        *slot = text;  // 之后经指针写入真正的值
                       // then write the real value through the pointer
        REQUIRE(m.find(k) != nullptr);
        CHECK_EQ(*m.find(k), text);
        const std::optional<std::string> got = m.extract(k);
        REQUIRE(got.has_value());
        CHECK_EQ(*got, text);
        CHECK(!m.contains(k));
        const auto [again, inserted] = m.insert(k, "placeholder");
        REQUIRE(inserted);
        m.eraseAt(again);  // 撤销预留
                           // undo the reservation
        CHECK(!m.contains(k));
        m.insert(k, text);
        m.eraseAt(m.find(k));
        CHECK(m.empty());
    }
    CHECK(m.checkInvariants());
}

TEST_CASE("flat_map: zero initial capacity allocates on first insert") {
    using Map = hft::FlatHashMap<std::uint32_t, int>;
    Map m(0);
    CHECK_EQ(m.capacity(), std::size_t{0});
    CHECK_EQ(m.bucketCount(), std::size_t{1});
    CHECK(m.find(3) == nullptr);
    CHECK(!m.erase(3));
    CHECK(!m.extract(3).has_value());
    m.clear();
    CHECK(m.checkInvariants());
    CHECK(m.insert(3, 9).second);
    CHECK_EQ(m.bucketCount(), Map::kMinBuckets);
    CHECK_EQ(*m.find(3), 9);
}

TEST_CASE("flat_map: the sentinel key value works as an ordinary key") {
    using Map = hft::FlatHashMap<std::uint64_t, std::string>;
    static_assert(Map::kEmptyKey == kU64Max);
    Map m(0);
    CHECK(m.find(kU64Max) == nullptr);
    CHECK(!m.erase(kU64Max));
    CHECK(m.insert(kU64Max, "max").second);
    CHECK(!m.insert(kU64Max, "again").second);
    REQUIRE(m.find(kU64Max) != nullptr);
    CHECK_EQ(*m.find(kU64Max), std::string("max"));
    CHECK_EQ(m.size(), std::size_t{1});
    CHECK_EQ(m.bucketCount(), std::size_t{1});  // 存放在表外，不分配桶数组
                                                // stored outside the table, no bucket array allocated
    m.insert(1, "one");
    int visits = 0;
    bool saw_max = false;
    m.forEach([&](std::uint64_t k, const std::string& v) {
        ++visits;
        if (k == kU64Max) saw_max = (v == "max");
    });
    CHECK_EQ(visits, 2);
    CHECK(saw_max);

    CHECK(m.erase(kU64Max));
    CHECK(!m.contains(kU64Max));
    CHECK_EQ(m.size(), std::size_t{1});
    m[kU64Max] += "x";
    CHECK_EQ(*m.find(kU64Max), std::string("x"));
    m.clear();
    CHECK(m.empty());
    CHECK(!m.contains(kU64Max));

    NestedValueOwner owner;
    CHECK_EQ(owner.map[1].len, 7);
    CHECK_EQ(owner.map[kU64Max].len, 7);
    CHECK_EQ(owner.map.size(), std::size_t{2});
}

TEST_CASE("flat_map: signed, narrow and enum keys") {
    hft::FlatHashMap<std::int64_t, std::int64_t> m(4);
    for (std::int64_t k = -1000; k <= 1000; ++k) m[k] = k * 2;
    m[std::numeric_limits<std::int64_t>::min()] = 1;
    m[std::numeric_limits<std::int64_t>::max()] = 2;  // 哨兵值
                                                      // sentinel value
    CHECK_EQ(m.size(), std::size_t{2003});
    for (std::int64_t k = -1000; k <= 1000; ++k) {
        REQUIRE(m.find(k) != nullptr);
        REQUIRE_EQ(*m.find(k), k * 2);
    }
    CHECK_EQ(*m.find(std::numeric_limits<std::int64_t>::min()), std::int64_t{1});
    CHECK_EQ(*m.find(std::numeric_limits<std::int64_t>::max()), std::int64_t{2});
    for (std::int64_t k = -1000; k <= 1000; k += 2) REQUIRE(m.erase(k));
    for (std::int64_t k = -1000; k <= 1000; ++k) REQUIRE_EQ(m.contains(k), (k & 1) != 0);
    CHECK(m.checkInvariants());

    hft::FlatHashMap<Side, int> e(2);
    static_assert(hft::FlatHashMap<Side, int>::kEmptyKey == Side::Other);
    e[Side::Buy] = 1;
    e[Side::Sell] = 2;
    e[Side::Other] = 3;
    CHECK_EQ(e.size(), std::size_t{3});
    CHECK_EQ(*e.find(Side::Other), 3);
    CHECK(e.erase(Side::Buy));
    CHECK(!e.contains(Side::Buy));
    CHECK_EQ(e.size(), std::size_t{2});

    hft::FlatHashMap<std::int8_t, int> tiny(0);
    for (int k = -128; k <= 127; ++k) tiny[static_cast<std::int8_t>(k)] = k;
    CHECK_EQ(tiny.size(), std::size_t{256});
    for (int k = -128; k <= 127; ++k) REQUIRE_EQ(*tiny.find(static_cast<std::int8_t>(k)), k);
    CHECK(tiny.checkInvariants());
}

TEST_CASE("flat_map: exhaustive 16-bit key space with growth and complete erase") {
    hft::FlatHashMap<std::uint16_t, std::uint32_t> m(0);
    for (std::uint32_t k = 0; k <= 0xFFFF; ++k) REQUIRE(m.insert(static_cast<std::uint16_t>(k), k * 3).second);
    CHECK_EQ(m.size(), std::size_t{65536});
    CHECK_LE(m.size(), m.bucketCount() / 2);
    std::string why;
    if (!m.checkInvariants(&why)) FAIL(why);
    for (std::uint32_t k = 0; k <= 0xFFFF; ++k) {
        const std::uint32_t* p = m.find(static_cast<std::uint16_t>(k));
        REQUIRE(p != nullptr);
        REQUIRE_EQ(*p, k * 3);
    }
    for (std::uint32_t k = 1; k <= 0xFFFF; k += 2) REQUIRE(m.erase(static_cast<std::uint16_t>(k)));
    if (!m.checkInvariants(&why)) FAIL(why);
    for (std::uint32_t k = 0; k <= 0xFFFF; ++k) REQUIRE_EQ(m.contains(static_cast<std::uint16_t>(k)), (k & 1) == 0);
    for (std::uint32_t k = 0; k <= 0xFFFF; k += 2) REQUIRE(m.erase(static_cast<std::uint16_t>(k)));
    CHECK(m.empty());
    if (!m.checkInvariants(&why)) FAIL(why);
}

TEST_CASE("flat_map: differential vs std::unordered_map, sequential order ids (1M ops)") {
    std::uint64_t next_id = 1'000'000;
    runDifferential<std::uint64_t>(
        11, 1'000'000, 16,
        [&next_id](std::mt19937_64& rng) {
            // 一半是新 id，一半是最近的 id（查找/撤单命中率高）
            // Half new ids, half recent ids (high hit rate for lookups/cancels)
            if (rng() % 2 == 0) return next_id++;
            return next_id - 1 - rng() % 5000;
        },
        constantMix(45, 35));
}

TEST_CASE("flat_map: differential vs std::unordered_map, random 64-bit keys (1M ops)") {
    std::mt19937_64 gen(12);
    std::vector<std::uint64_t> universe(100'000);
    for (auto& k : universe) k = gen();
    universe[0] = kU64Max;  // 混入哨兵值
                            // mix in the sentinel value
    runDifferential<std::uint64_t>(
        13, 1'000'000, 0,
        [&universe](std::mt19937_64& rng) { return universe[static_cast<std::size_t>(rng() % universe.size())]; },
        constantMix(40, 30));
}

TEST_CASE("flat_map: differential vs std::unordered_map, clustered drifting keys (1M ops)") {
    std::uint64_t base = 1'000'000'000'000ULL;
    std::uint64_t step = 0;
    runDifferential<std::uint64_t>(
        14, 1'000'000, 16,
        [&](std::mt19937_64& rng) {
            if (++step % 1000 == 0) base += 64;  // 窗口缓慢漂移
                                                 // the window drifts slowly
            return base + rng() % 512;
        },
        constantMix(40, 40));
}

TEST_CASE("flat_map: differential vs std::unordered_map, high-bit-only keys (1M ops)") {
    // 只有高位不同的键：检验哈希把高位混入低位
    // Keys that differ only in their high bits: checks that the hash mixes the high bits into the low bits
    runDifferential<std::uint64_t>(
        15, 1'000'000, 16, [](std::mt19937_64& rng) { return (rng() % 40'000) << 40; }, constantMix(45, 30));
}

TEST_CASE("flat_map: differential vs std::unordered_map, erase-heavy churn with growth phases (1M ops)") {
    // 交替的增长期与删除期：表反复增至约 3 万个元素再删到约 1 万（删除发生在已扩容的大表中）
    // Alternating growth and erase phases: the map repeatedly grows to about 30,000 elements and is then
    // erased down to about 10,000 (the erases happen in a large, already grown table)
    runDifferential<std::uint32_t>(
        17, 1'000'000, 0, [](std::mt19937_64& rng) { return static_cast<std::uint32_t>(rng() % 40'000); },
        [](std::size_t op) { return (op / 100'000) % 2 == 0 ? OpMix{65, 20} : OpMix{10, 75}; });
}

TEST_CASE("flat_map: differential vs std::unordered_map, signed keys around zero incl. sentinel (1M ops)") {
    runDifferential<std::int32_t>(
        18, 1'000'000, 8,
        [](std::mt19937_64& rng) {
            const unsigned r = static_cast<unsigned>(rng() % 1000);
            if (r == 0) return std::numeric_limits<std::int32_t>::max();  // 哨兵值
                                                                          // sentinel value
            if (r == 1) return std::numeric_limits<std::int32_t>::min();
            return static_cast<std::int32_t>(static_cast<std::int64_t>(rng() % 6001) - 3000);
        },
        constantMix(40, 40));
}

TEST_CASE("flat_map: colliding keys in probe runs that wrap around the table end") {
    using Map = hft::FlatHashMap<std::uint64_t, std::uint64_t>;
    constexpr std::size_t kBuckets = 64;  // reserve(32) => 64 个桶，32 个元素以内不扩容
                                          // reserve(32) => 64 buckets, no growth up to 32 elements
    std::vector<std::uint64_t> keys;
    for (std::uint64_t k = 1; keys.size() < 48; ++k) {
        const std::size_t home = static_cast<std::size_t>(Map::hash(k) & (kBuckets - 1));
        if (home >= 61 || home <= 1) keys.push_back(k);  // home 在表尾 3 个桶或表头 2 个桶
                                                         // home in the last 3 or the first 2 buckets
    }
    for (std::uint64_t seed = 1; seed <= 25; ++seed) {
        Map m(32);
        REQUIRE_EQ(m.bucketCount(), kBuckets);
        std::unordered_map<std::uint64_t, std::uint64_t> ref;
        std::mt19937_64 rng(seed);
        for (int op = 0; op < 8000; ++op) {
            const std::uint64_t k = keys[static_cast<std::size_t>(rng() % keys.size())];
            const unsigned r = static_cast<unsigned>(rng() % 100);
            if (r < 55 && ref.size() < 32) {
                const std::uint64_t v = rng();
                REQUIRE_EQ(m.insert(k, v).second, ref.emplace(k, v).second);
            } else if (r < 90) {
                REQUIRE_EQ(eraseVia(m, k, rng()).has_value(), ref.erase(k) == 1);
            } else {
                const std::uint64_t* p = m.find(k);
                const auto it = ref.find(k);
                REQUIRE_EQ(p != nullptr, it != ref.end());
                if (p != nullptr) REQUIRE_EQ(*p, it->second);
            }
            REQUIRE_EQ(m.bucketCount(), kBuckets);  // 始终在同一张拥挤的表中操作
                                                    // always operating in the same crowded table
            std::string why;
            if (!m.checkInvariants(&why)) FAIL(why);
            REQUIRE_EQ(m.size(), ref.size());
        }
        requireSameContents(m, ref);
    }
}

TEST_CASE("flat_map: keys sharing one home bucket at the table end, erased in random orders") {
    using Map = hft::FlatHashMap<std::uint64_t, std::uint64_t>;
    constexpr std::size_t kBuckets = 64;
    std::vector<std::uint64_t> keys;
    for (std::uint64_t k = 1; keys.size() < 32; ++k) {
        if ((Map::hash(k) & (kBuckets - 1)) == kBuckets - 1) keys.push_back(k);  // home 全是最后一个桶
                                                                                 // every home is the last bucket
    }
    std::mt19937_64 rng(99);
    for (int round = 0; round < 200; ++round) {
        Map m(32);
        REQUIRE_EQ(m.bucketCount(), kBuckets);
        std::vector<std::uint64_t> order = keys;
        std::shuffle(order.begin(), order.end(), rng);
        for (std::size_t i = 0; i < order.size(); ++i) REQUIRE(m.insert(order[i], order[i] * 3).second);
        // 32 个键占据桶 63, 0, 1, ..., 30：一条跨越表尾的探测链
        // The 32 keys occupy buckets 63, 0, 1, ..., 30: one probe chain that wraps around the table end
        REQUIRE_EQ(m.bucketCount(), kBuckets);
        std::shuffle(order.begin(), order.end(), rng);
        for (std::size_t i = 0; i < order.size(); ++i) {
            REQUIRE(eraseVia(m, order[i], round + i) == std::optional<std::uint64_t>(order[i] * 3));
            std::string why;
            if (!m.checkInvariants(&why)) FAIL(why);
            for (std::size_t j = 0; j < order.size(); ++j) {
                const std::uint64_t* p = m.find(order[j]);
                if (j <= i) {
                    REQUIRE(p == nullptr);
                } else {
                    REQUIRE(p != nullptr);
                    REQUIRE_EQ(*p, order[j] * 3);
                }
            }
        }
        CHECK(m.empty());
    }
}

TEST_CASE("flat_map: backward-shift deletion moves exactly the entries that may move") {
    using Map = hft::FlatHashMap<std::uint64_t, int>;
    constexpr std::size_t kBuckets = 64;
    // A、B 的 home 为 62，C 的 home 为 0，D 的 home 为 63：
    // 插入后 A@62 B@63 C@0 D@1。删除 A：B 前移到 62；C 不能移（home 0 在空位之后）；
    // D（home 63）前移到 63。最终 62:B 63:D 0:C。
    // A and B have home 62, C has home 0, D has home 63:
    // after the inserts A@62 B@63 C@0 D@1. Erasing A: B shifts back to 62; C cannot move (home 0 lies
    // after the hole); D (home 63) shifts back to 63. Final layout 62:B 63:D 0:C.
    const std::uint64_t a = keyWithHome<Map>(62, kBuckets, 1);
    const std::uint64_t b = keyWithHome<Map>(62, kBuckets, a + 1);
    const std::uint64_t c = keyWithHome<Map>(0, kBuckets, 1);
    const std::uint64_t d = keyWithHome<Map>(63, kBuckets, 1);
    Map m(32);
    REQUIRE_EQ(m.bucketCount(), kBuckets);
    m.insert(a, 1);
    m.insert(b, 2);
    m.insert(c, 3);
    m.insert(d, 4);
    REQUIRE(m.checkInvariants());
    REQUIRE(m.erase(a));
    std::string why;
    if (!m.checkInvariants(&why)) FAIL(why);
    CHECK(m.find(a) == nullptr);
    REQUIRE(m.find(b) != nullptr);
    REQUIRE(m.find(c) != nullptr);
    REQUIRE(m.find(d) != nullptr);
    CHECK_EQ(*m.find(b), 2);
    CHECK_EQ(*m.find(c), 3);
    CHECK_EQ(*m.find(d), 4);
    // 再删 C（位于表头）：没有后继可移动；再删 B：D 留在 63
    // Then erase C (at the table start): there is no successor to move; then erase B: D stays at 63
    REQUIRE(m.erase(c));
    if (!m.checkInvariants(&why)) FAIL(why);
    REQUIRE(m.erase(b));
    if (!m.checkInvariants(&why)) FAIL(why);
    REQUIRE(m.find(d) != nullptr);
    CHECK_EQ(*m.find(d), 4);
    CHECK_EQ(m.size(), std::size_t{1});
}

TEST_CASE("flat_map: non-trivial values survive growth and backward shifts") {
    hft::FlatHashMap<std::uint32_t, std::string> m(0);
    std::unordered_map<std::uint32_t, std::string> ref;
    std::mt19937_64 rng(21);
    for (int op = 0; op < 300000; ++op) {
        const std::uint32_t k = static_cast<std::uint32_t>(rng() % 5000);
        const unsigned r = static_cast<unsigned>(rng() % 100);
        if (r < 45) {
            std::string v = "value-" + std::to_string(rng()) + "-long-enough-for-heap-allocation";
            REQUIRE_EQ(m.insert(k, v).second, ref.emplace(k, v).second);
        } else if (r < 85) {
            const std::optional<std::string> got = eraseVia(m, k, r);  // extract 把字符串移出表外
                                                                       // extract moves the string out of the table
            const auto it = ref.find(k);
            REQUIRE_EQ(got.has_value(), it != ref.end());
            if (got.has_value()) {
                REQUIRE(*got == it->second);
                ref.erase(it);
            }
        } else {
            m[k] += "+";
            ref[k] += "+";
        }
        if (op % 20000 == 0) requireSameContents(m, ref);
    }
    requireSameContents(m, ref);
}

TEST_CASE("flat_map: value constructions and destructions balance") {
    Tracked::live = 0;
    {
        hft::FlatHashMap<std::uint64_t, Tracked> m(0);
        std::mt19937_64 rng(31);
        for (int op = 0; op < 200000; ++op) {
            const std::uint64_t k = rng() % 3000;
            const std::uint64_t key = k == 0 ? kU64Max : k;  // 混入哨兵值（表外专用槽位）
                                                             // mix in the sentinel value
                                                             // (dedicated slot outside the table)
            const unsigned r = static_cast<unsigned>(rng() % 1000);
            if (r < 500) {
                m.insert(key, Tracked(static_cast<int>(k)));
            } else if (r < 995) {
                const std::optional<Tracked> got = eraseVia(m, key, r);
                REQUIRE_EQ(static_cast<std::size_t>(Tracked::live), m.size() + (got.has_value() ? 1 : 0));
            } else {
                m.clear();
            }
            REQUIRE_EQ(static_cast<std::size_t>(Tracked::live), m.size());
        }
        m.insert(kU64Max, Tracked(-1));
        hft::FlatHashMap<std::uint64_t, Tracked> moved(std::move(m));
        REQUIRE_EQ(static_cast<std::size_t>(Tracked::live), moved.size());
        CHECK(m.empty());
    }
    CHECK_EQ(Tracked::live, 0);
}

TEST_CASE("flat_map: reserve prevents rehash until size exceeds n") {
    const std::size_t sizes[] = {1, 5, 16, 100, 1000, 4097};
    for (std::size_t n : sizes) {
        hft::FlatHashMap<std::uint64_t, std::uint64_t> m(0);
        m.reserve(n);
        CHECK_GE(m.capacity(), n);
        const std::size_t buckets = m.bucketCount();
        REQUIRE(hft::isPow2(buckets));
        m.insert(0, 12345);
        const std::uint64_t* first = m.find(0);
        for (std::uint64_t i = 1; i < n; ++i) m.insert(i * 977, i);
        CHECK_EQ(m.size(), n);
        CHECK_EQ(m.bucketCount(), buckets);  // 未扩容
                                             // no growth
        CHECK(m.find(0) == first);          // 未扩容时插入不移动已有元素
                                            // without growth, inserts do not move existing elements
        // 继续插入直到第一次扩容：扩容恰好发生在 size() 首次超过 reserve 得到的容量时
        // Keep inserting until the first growth: it happens exactly when size() first exceeds the capacity
        // obtained from reserve
        const std::size_t reserved = m.capacity();
        std::uint64_t extra = 1;
        while (m.bucketCount() == buckets) {
            REQUIRE_LE(m.size(), reserved);
            m.insert(kU64Max - extra++, 0);
        }
        CHECK_EQ(m.size(), reserved + 1);
        CHECK_EQ(m.bucketCount(), buckets * 2);
        CHECK(m.checkInvariants());
        REQUIRE(m.find(0) != nullptr);
        CHECK_EQ(*m.find(0), std::uint64_t{12345});
    }
}

TEST_CASE("flat_map: move construction transfers the table and leaves the source empty") {
    using Map = hft::FlatHashMap<std::uint64_t, std::string>;
    Map a;
    for (std::uint64_t i = 0; i < 1000; ++i) a.insert(i * 31, std::to_string(i));
    a.insert(kU64Max, "max");
    const std::size_t buckets = a.bucketCount();
    Map b(std::move(a));
    CHECK_EQ(b.size(), std::size_t{1001});
    CHECK_EQ(b.bucketCount(), buckets);
    for (std::uint64_t i = 0; i < 1000; ++i) {
        const std::string* p = b.find(i * 31);
        REQUIRE(p != nullptr);
        REQUIRE_EQ(*p, std::to_string(i));
    }
    REQUIRE(b.find(kU64Max) != nullptr);
    CHECK_EQ(*b.find(kU64Max), std::string("max"));
    CHECK(b.checkInvariants());
    CHECK(a.empty());
    CHECK_EQ(a.bucketCount(), std::size_t{1});  // 不再持有内存
                                                // no longer holds memory
    CHECK(!a.contains(kU64Max));
    CHECK(a.checkInvariants());
    a.insert(5, "five");  // 被移走的对象可继续使用
                          // a moved-from object can still be used
    CHECK_EQ(*a.find(5), std::string("five"));
}

// ============================================================================
// LatencyHistogram
// ============================================================================

namespace {

// 用 samples 建直方图，逐项与精确统计比较：分位数误差 <= 1/64（中点实现实际 <= 1/128），
// countBelow 与"值 < 桶下界"的精确计数相等
// Builds a histogram from samples and compares it item by item with the exact statistics: percentile
// error <= 1/64 (in practice <= 1/128 with the midpoint implementation), and countBelow equals the
// exact count of "value < bucket lower bound"
void checkHistogramAgainstExact(const std::vector<std::uint64_t>& samples, std::uint64_t seed) {
    hft::LatencyHistogram h;
    for (std::uint64_t v : samples) h.record(v);
    std::vector<std::uint64_t> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const std::size_t n = sorted.size();
    REQUIRE_EQ(h.count(), static_cast<std::uint64_t>(n));
    REQUIRE_EQ(h.min(), sorted.front());
    REQUIRE_EQ(h.max(), sorted.back());

    // 均值：总和未溢出时与精确值一致
    // Mean: equals the exact value when the sum does not overflow
    unsigned __int128 sum = 0;
    for (std::uint64_t v : sorted) sum += v;
    if (sum <= kU64Max) {
        const long double exact_mean = static_cast<long double>(static_cast<std::uint64_t>(sum)) / n;
        CHECK_NEAR(h.mean(), static_cast<double>(exact_mean), static_cast<double>(exact_mean) * 1e-12);
    } else {
        CHECK_GE(h.mean(), static_cast<double>(sorted.front()));
        CHECK_LE(h.mean(), static_cast<double>(sorted.back()));
    }

    std::vector<double> qs = {0.0, 1e-6, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99,
                              0.995, 0.999, 0.9999, 0.99999, 1.0};
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> uq(0.0, 1.0);
    for (int i = 0; i < 300; ++i) qs.push_back(uq(rng));
    for (double q : qs) {
        const std::uint64_t exact = exactPercentile(sorted, q);
        const std::uint64_t got = h.percentile(q);
        const std::uint64_t err = absDiff(got, exact);
        if (err > exact / 64) {
            FAIL("percentile(" + std::to_string(q) + ") = " + std::to_string(got) + ", exact " +
                 std::to_string(exact) + ": relative error above 1/64");
        }
        REQUIRE_LE(err, exact / 128);  // 中点代表值的更紧上界
                                       // tighter upper bound with midpoint representatives
        REQUIRE_GE(got, sorted.front());
        REQUIRE_LE(got, sorted.back());
    }

    // countBelow：随机阈值 + 样本值本身 + 样本值 +1
    // countBelow: random thresholds + the sample values themselves + sample values +1
    std::vector<std::uint64_t> thresholds = {0, 1, kU64Max};
    for (int i = 0; i < 300; ++i) {
        const std::uint64_t s = sorted[static_cast<std::size_t>(rng() % n)];
        thresholds.push_back(s);
        thresholds.push_back(s + 1);
        thresholds.push_back(rng() >> (rng() % 64));
    }
    for (std::uint64_t v : thresholds) {
        const std::uint64_t got = h.countBelow(v);
        const std::uint64_t low = hft::LatencyHistogram::bucketLow(hft::LatencyHistogram::bucketIndex(v));
        const auto exact_below_low = static_cast<std::uint64_t>(
            std::lower_bound(sorted.begin(), sorted.end(), low) - sorted.begin());
        const auto exact_below_v = static_cast<std::uint64_t>(
            std::lower_bound(sorted.begin(), sorted.end(), v) - sorted.begin());
        REQUIRE_EQ(got, exact_below_low);  // 恰好是"值 < v 所在桶下界"的样本数
                                           // exactly the count of samples with
                                           // "value < lower bound of v's bucket"
        REQUIRE_LE(got, exact_below_v);    // 从不多算
                                           // never overcounts
    }
}

}  // namespace

TEST_CASE("histogram: bucket mapping covers the full uint64 range with width <= 1/64") {
    using H = hft::LatencyHistogram;
    static_assert(H::kBucketCount == 3776);
    static_assert(H::bucketIndex(0) == 0);
    static_assert(H::bucketIndex(127) == 127);
    static_assert(H::bucketIndex(128) == 128);
    static_assert(H::bucketIndex(kU64Max) == H::kBucketCount - 1);
    static_assert(H::bucketHigh(H::kBucketCount - 1) == kU64Max);
    for (std::size_t i = 0; i < H::kBucketCount; ++i) {
        REQUIRE_EQ(H::bucketIndex(H::bucketLow(i)), i);
        REQUIRE_EQ(H::bucketIndex(H::bucketHigh(i)), i);
        if (i + 1 < H::kBucketCount) REQUIRE_EQ(H::bucketLow(i + 1), H::bucketHigh(i) + 1);  // 桶连续无缝
                                                                                             // buckets are contiguous
                                                                                             // with no gaps
        const std::uint64_t width = H::bucketHigh(i) - H::bucketLow(i) + 1;
        if (i < H::kExactLimit) {
            REQUIRE_EQ(width, std::uint64_t{1});  // 精确区
                                                  // exact range
        } else {
            REQUIRE_LE(width * 64, H::bucketLow(i));  // 桶宽 <= 下界 / 64
                                                      // bucket width <= lower bound / 64
        }
        REQUIRE(H::bucketMid(i) >= H::bucketLow(i));
        REQUIRE(H::bucketMid(i) <= H::bucketHigh(i));
    }
    std::mt19937_64 rng(1);
    for (int i = 0; i < 1'000'000; ++i) {
        const unsigned bits = static_cast<unsigned>(rng() % 65);
        const std::uint64_t v = bits == 0 ? 0 : rng() >> (64 - bits);  // 各数量级均匀覆盖
                                                                       // covers every order of magnitude evenly
        const std::size_t idx = H::bucketIndex(v);
        REQUIRE(idx < H::kBucketCount);
        REQUIRE(H::bucketLow(idx) <= v);
        REQUIRE(v <= H::bucketHigh(idx));
        REQUIRE_LE(absDiff(H::bucketMid(idx), v), v / 128);
        REQUIRE_LE(absDiff(H::bucketLow(idx), v), v / 64);
        REQUIRE_LE(absDiff(H::bucketHigh(idx), v), v / 64);
    }
}

TEST_CASE("histogram: empty state and the first sample") {
    hft::LatencyHistogram h;
    CHECK_EQ(h.count(), std::uint64_t{0});
    CHECK(h.empty());
    CHECK_EQ(h.min(), std::uint64_t{0});
    CHECK_EQ(h.max(), std::uint64_t{0});
    CHECK_EQ(h.mean(), 0.0);
    for (double q : {0.0, 0.5, 1.0}) CHECK_EQ(h.percentile(q), std::uint64_t{0});
    CHECK_EQ(h.countBelow(0), std::uint64_t{0});
    CHECK_EQ(h.countBelow(kU64Max), std::uint64_t{0});
    for (std::size_t i = 0; i < hft::LatencyHistogram::kBucketCount; ++i) REQUIRE_EQ(h.bucketCountAt(i), std::uint64_t{0});
    h.record(42);
    CHECK(!h.empty());
    CHECK_EQ(h.count(), std::uint64_t{1});
    CHECK_EQ(h.min(), std::uint64_t{42});
    CHECK_EQ(h.max(), std::uint64_t{42});
    CHECK_EQ(h.percentile(0.5), std::uint64_t{42});
    CHECK_EQ(h.mean(), 42.0);
}

TEST_CASE("histogram: percentiles and countBelow vs exact sorted data on random distributions") {
    std::mt19937_64 rng(5);
    constexpr std::size_t kN = 100'000;
    std::vector<std::uint64_t> s;

    s.clear();  // [0, 128) 精确区：每个值一个桶，分位数与 countBelow 都没有误差
                // [0, 128) exact range: one bucket per value, percentiles and countBelow are both error-free
    for (std::size_t i = 0; i < kN; ++i) s.push_back(rng() % 128);
    checkHistogramAgainstExact(s, 0);
    for (std::uint64_t v = 0; v < 128; ++v) {
        hft::LatencyHistogram one;
        for (int k = 0; k < 3; ++k) one.record(v);
        REQUIRE_EQ(one.percentile(0.5), v);
        REQUIRE_EQ(one.bucketCountAt(static_cast<std::size_t>(v)), std::uint64_t{3});
    }

    s.clear();  // 均匀小值
                // uniform small values
    for (std::size_t i = 0; i < kN; ++i) s.push_back(rng() % 1000);
    checkHistogramAgainstExact(s, 1);

    s.clear();  // 均匀中等值
                // uniform medium values
    for (std::size_t i = 0; i < kN; ++i) s.push_back(rng() % (1u << 20));
    checkHistogramAgainstExact(s, 2);

    s.clear();  // 对数均匀 [1, 2^40)
                // log-uniform [1, 2^40)
    {
        std::uniform_real_distribution<double> e(0.0, 40.0);
        for (std::size_t i = 0; i < kN; ++i) s.push_back(static_cast<std::uint64_t>(std::exp2(e(rng))));
    }
    checkHistogramAgainstExact(s, 3);

    s.clear();  // 类延迟分布：对数正态主体 + 1% 的 100 倍尖刺
                // latency-like distribution: log-normal body + 1% spikes 100 times larger
    {
        std::lognormal_distribution<double> ln(std::log(5000.0), 0.8);
        for (std::size_t i = 0; i < kN; ++i) {
            double v = ln(rng);
            if (rng() % 100 == 0) v *= 100.0;
            s.push_back(static_cast<std::uint64_t>(v));
        }
    }
    checkHistogramAgainstExact(s, 4);

    s.clear();  // 2 的幂附近（桶边界）
                // around powers of 2 (bucket boundaries)
    for (unsigned k = 0; k < 64; ++k) {
        const std::uint64_t p = std::uint64_t{1} << k;
        for (int rep = 0; rep < 50; ++rep) {
            s.push_back(p - 1);
            s.push_back(p);
            s.push_back(p + 1);
        }
    }
    checkHistogramAgainstExact(s, 5);

    s.clear();  // 完整 64 位随机值（总和溢出，均值饱和后仍钳制在 [min, max]）
                // full 64-bit random values (the sum overflows; after it saturates,
                // the mean is still clamped to [min, max])
    for (std::size_t i = 0; i < kN; ++i) s.push_back(rng());
    checkHistogramAgainstExact(s, 6);

    s.assign(kN, 123'456'789);  // 常数：所有分位数都精确
                                // constant: every percentile is exact
    checkHistogramAgainstExact(s, 7);
    {
        hft::LatencyHistogram h;
        for (std::uint64_t v : s) h.record(v);
        for (double q : {0.0, 0.3, 0.5, 0.99, 1.0}) CHECK_EQ(h.percentile(q), std::uint64_t{123'456'789});
    }

    s.clear();  // 相距很远的两簇
                // two clusters far apart
    for (std::size_t i = 0; i < kN; ++i) s.push_back(i % 3 == 0 ? 1'000 + rng() % 10 : 9'000'000'000ULL + rng() % 1000);
    checkHistogramAgainstExact(s, 8);
}

TEST_CASE("histogram: percentile clamps q and its result into [min, max]") {
    hft::LatencyHistogram h;
    h.record(1000);
    h.record(2000);
    h.record(3000);
    CHECK_EQ(h.percentile(-1.0), std::uint64_t{1000});
    CHECK_EQ(h.percentile(2.0), std::uint64_t{3000});
    CHECK_EQ(h.percentile(std::nan("")), std::uint64_t{1000});
    CHECK_EQ(h.percentile(0.0), std::uint64_t{1000});
    CHECK_EQ(h.percentile(1.0), std::uint64_t{3000});
    CHECK_LE(absDiff(h.percentile(0.5), 2000), std::uint64_t{2000 / 128});

    // 同一个桶内的样本：结果被钳制在 [min, max] 内
    // Samples within one bucket: the result is clamped into [min, max]
    hft::LatencyHistogram same;
    same.record(1'000'001);
    same.record(1'000'002);
    same.record(1'000'003);
    for (double q : {0.0, 0.4, 0.5, 0.6, 1.0}) {
        const std::uint64_t p = same.percentile(q);
        CHECK_GE(p, std::uint64_t{1'000'001});
        CHECK_LE(p, std::uint64_t{1'000'003});
    }
    hft::LatencyHistogram one;
    one.record(123'457);
    for (double q : {0.0, 0.01, 0.5, 0.99, 1.0}) CHECK_EQ(one.percentile(q), std::uint64_t{123'457});
}

TEST_CASE("histogram: extreme values across the full uint64 range") {
    using H = hft::LatencyHistogram;
    H h;
    h.record(0);
    h.record(kU64Max);
    h.record(kU64Max - 1);
    h.record(std::uint64_t{1} << 63);
    CHECK_EQ(h.count(), std::uint64_t{4});
    CHECK_EQ(h.min(), std::uint64_t{0});
    CHECK_EQ(h.max(), kU64Max);
    CHECK_EQ(h.percentile(0.0), std::uint64_t{0});
    CHECK_EQ(h.percentile(1.0), kU64Max);
    CHECK_LE(absDiff(h.percentile(0.5), std::uint64_t{1} << 63), (std::uint64_t{1} << 63) / 64);
    CHECK_EQ(h.bucketCountAt(H::kBucketCount - 1), std::uint64_t{2});
    CHECK_EQ(h.countBelow(kU64Max), std::uint64_t{2});
    CHECK_EQ(h.countBelow(1), std::uint64_t{1});
    // 总和已饱和：均值仍在 [min, max] 内
    // The sum has saturated: the mean is still within [min, max]
    CHECK_GT(h.mean(), 0.0);
    CHECK_LE(h.mean(), static_cast<double>(kU64Max));
    H top;
    top.record(kU64Max);
    top.record(kU64Max);
    CHECK_EQ(top.mean(), static_cast<double>(kU64Max));
}

TEST_CASE("histogram: countBelow at hand-computed bucket boundaries") {
    hft::LatencyHistogram h;
    h.record(100);  // 精确区
                    // exact range
    h.record(200);  // 桶 [200, 201]
                    // bucket [200, 201]
    h.record(300);  // 桶 [300, 303]
                    // bucket [300, 303]
    CHECK_EQ(h.countBelow(100), std::uint64_t{0});
    CHECK_EQ(h.countBelow(101), std::uint64_t{1});
    CHECK_EQ(h.countBelow(200), std::uint64_t{1});
    CHECK_EQ(h.countBelow(201), std::uint64_t{1});  // 200 与 201 同桶：桶分辨率，不计入
                                                    // 200 and 201 share a bucket: bucket resolution, not counted
    CHECK_EQ(h.countBelow(202), std::uint64_t{2});
    CHECK_EQ(h.countBelow(300), std::uint64_t{2});
    CHECK_EQ(h.countBelow(304), std::uint64_t{3});
    CHECK_EQ(h.countBelow(kU64Max), std::uint64_t{3});
}

TEST_CASE("histogram: merge equals recording every sample into one histogram") {
    std::mt19937_64 rng(8);
    hft::LatencyHistogram a, b, all;
    for (int i = 0; i < 50'000; ++i) {
        const std::uint64_t v = rng() % 100'000;
        a.record(v);
        all.record(v);
    }
    for (int i = 0; i < 30'000; ++i) {
        const std::uint64_t v = 50'000 + rng() % 10'000'000;
        b.record(v);
        all.record(v);
    }
    hft::LatencyHistogram merged;
    merged.merge(a);
    merged.merge(b);
    CHECK_EQ(merged.count(), all.count());
    CHECK_EQ(merged.min(), all.min());
    CHECK_EQ(merged.max(), all.max());
    CHECK_EQ(merged.mean(), all.mean());
    for (std::size_t i = 0; i < hft::LatencyHistogram::kBucketCount; ++i) {
        REQUIRE_EQ(merged.bucketCountAt(i), all.bucketCountAt(i));
    }
    for (double q : {0.0, 0.1, 0.5, 0.9, 0.99, 0.999, 1.0}) CHECK_EQ(merged.percentile(q), all.percentile(q));

    hft::LatencyHistogram copy, empty;
    copy.merge(a);      // 合并进空直方图：与 a 相同
                        // merged into an empty histogram: same as a
    copy.merge(empty);  // 合并空直方图：不变
                        // merging an empty histogram: unchanged
    CHECK_EQ(empty.count(), std::uint64_t{0});
    CHECK_EQ(copy.count(), a.count());
    CHECK_EQ(copy.min(), a.min());
    CHECK_EQ(copy.max(), a.max());
    CHECK_EQ(copy.percentile(0.5), a.percentile(0.5));

    copy.merge(copy);  // 与自身合并：计数翻倍，分布不变
                       // merged with itself: counts double, distribution unchanged
    CHECK_EQ(copy.count(), 2 * a.count());
    CHECK_EQ(copy.min(), a.min());
    CHECK_EQ(copy.max(), a.max());
    CHECK_EQ(copy.mean(), a.mean());
    for (std::size_t i = 0; i < hft::LatencyHistogram::kBucketCount; ++i) {
        REQUIRE_EQ(copy.bucketCountAt(i), 2 * a.bucketCountAt(i));
    }
}

TEST_CASE("histogram: concurrent readers and one writer stay consistent (run under TSan)") {
    hft::LatencyHistogram h;
    constexpr std::uint64_t kSamples = 1'000'000;
    constexpr std::uint64_t kLo = 1'000, kHi = 2'000'000;
    std::atomic<bool> done{false};
    std::atomic<std::uint64_t> violations{0};
    std::atomic<std::uint64_t> reads{0};
    auto reader = [&] {
        std::uint64_t last_count = 0, local_violations = 0, iterations = 0;
        do {
            const std::uint64_t n = h.count();
            if (n < last_count) ++local_violations;  // 计数单调不减
                                                     // the count never decreases
            last_count = n;
            if (n > 0) {
                const std::uint64_t lo = h.min(), hi = h.max();
                if (lo > hi || lo < kLo || hi > kHi) ++local_violations;
                for (double q : {0.0, 0.5, 0.99, 1.0}) {
                    const std::uint64_t p = h.percentile(q);
                    if (p < kLo || p > kHi) ++local_violations;
                }
                const double m = h.mean();
                if (m < static_cast<double>(kLo) || m > static_cast<double>(kHi)) ++local_violations;
                const std::uint64_t c1 = h.countBelow(10'000);
                const std::uint64_t c2 = h.countBelow(1'000'000);
                if (c1 > c2) ++local_violations;  // 阈值更大、读取更晚，不可能更少
                                                  // larger threshold, read later, so it cannot be fewer
                hft::LatencyHistogram snapshot;   // 并发合并：源正被其写者更新
                                                  // concurrent merge: the source is being updated by its writer
                snapshot.merge(h);
                if (snapshot.count() < n || snapshot.percentile(0.5) > kHi) ++local_violations;
            }
            ++iterations;
        } while (!done.load(std::memory_order_acquire));
        violations.fetch_add(local_violations);
        reads.fetch_add(iterations);
    };
    std::thread r1(reader);
    std::thread r2(reader);
    std::thread writer([&h, &done] {
        for (std::uint64_t i = 0; i < kSamples; ++i) h.record(kLo + (i * 7919) % (kHi - kLo + 1));
        done.store(true, std::memory_order_release);
    });
    writer.join();
    r1.join();
    r2.join();
    CHECK_EQ(violations.load(), std::uint64_t{0});
    CHECK_GT(reads.load(), std::uint64_t{0});
    CHECK_EQ(h.count(), kSamples);
    CHECK_EQ(h.min(), kLo);
    std::uint64_t total = 0;
    for (std::size_t i = 0; i < hft::LatencyHistogram::kBucketCount; ++i) total += h.bucketCountAt(i);
    CHECK_EQ(total, kSamples);
}

// ============================================================================
// timing.h
// ============================================================================

TEST_CASE("timing: clocks are monotonic and plausible") {
    std::int64_t prev = hft::nowNs();
    for (int i = 0; i < 10000; ++i) {
        const std::int64_t t = hft::nowNs();
        REQUIRE_GE(t, prev);
        prev = t;
    }
    const std::int64_t wall = hft::wallClockMs();
    CHECK_GT(wall, std::int64_t{1'577'836'800'000});  // 2020-01-01 之后
                                                      // after 2020-01-01
    const std::int64_t sys = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    CHECK_LE(std::llabs(sys - wall), 1000LL);
    std::uint64_t tprev = hft::rdtsc();
    for (int i = 0; i < 10000; ++i) {
        const std::uint64_t t = hft::rdtscp();
        REQUIRE_GE(t, tprev);  // 同一线程上 TSC 单调
                               // TSC is monotonic on the same thread
        tprev = hft::rdtsc();
    }
}

TEST_CASE("timing: TscClock agrees with steady_clock") {
    const hft::TscClock& clock = hft::TscClock::instance();
    CHECK(&clock == &hft::TscClock::instance());
    CHECK_GT(clock.nsPerTick(), 0.0);
    CHECK_EQ(clock.toNs(0), std::int64_t{0});
    CHECK_LT(clock.toNs(static_cast<std::uint64_t>(-100000)), std::int64_t{0});  // 回绕的"负"间隔
                                                                                 // wrapped "negative" interval
    // 多次测量取最好的一次，容忍共享机器上的调度抖动
    // Measure several times and keep the best, tolerating scheduling jitter on shared machines
    double best_ratio_error = 1.0;
    for (int attempt = 0; attempt < 5 && best_ratio_error > 0.02; ++attempt) {
        const std::uint64_t t0 = hft::rdtsc();
        const std::int64_t n0 = hft::nowNs();
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        const std::uint64_t t1 = hft::rdtscp();
        const std::int64_t n1 = hft::nowNs();
        const double ratio = static_cast<double>(clock.toNs(t1 - t0)) / static_cast<double>(n1 - n0);
        best_ratio_error = std::min(best_ratio_error, std::fabs(ratio - 1.0));
    }
    CHECK_LT(best_ratio_error, 0.02);
}

TEST_CASE("timing: thread pinning and naming") {
    // 在独立线程中进行，避免改变主测试线程（及其后创建的线程）的 CPU 亲和性
    // Done in a separate thread to avoid changing the CPU affinity of the main test thread (and of the
    // threads created after it)
    std::thread th([] {
        CHECK(!hft::pinThreadToCpu(-1));
        CHECK(!hft::pinThreadToCpu(1 << 20));
#if defined(__linux__)
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        const bool got = pthread_getaffinity_np(pthread_self(), sizeof(allowed), &allowed) == 0;
        CHECK(got);
        int cpu = -1;
        for (int c = 0; got && c < CPU_SETSIZE; ++c) {
            if (CPU_ISSET(c, &allowed)) {
                cpu = c;
                break;
            }
        }
        CHECK_GE(cpu, 0);
        if (cpu >= 0) {
            CHECK(hft::pinThreadToCpu(cpu));
            CHECK_EQ(sched_getcpu(), cpu);
        }
        hft::setThreadName("a-very-long-thread-name-for-testing");
        char name[32] = {};
        CHECK_EQ(pthread_getname_np(pthread_self(), name, sizeof(name)), 0);
        CHECK_EQ(std::string(name), std::string("a-very-long-thr"));  // 截断到 15 个字符
                                                                      // truncated to 15 characters
        hft::setThreadName("short");
        CHECK_EQ(pthread_getname_np(pthread_self(), name, sizeof(name)), 0);
        CHECK_EQ(std::string(name), std::string("short"));
        hft::setThreadName(nullptr);  // 空操作
                                      // no-op
#endif
    });
    th.join();
}

// ============================================================================
// LatencyTracker
// ============================================================================

TEST_CASE("latency_tracker: empty state, nanosecond statistics and microsecond percentiles") {
    LatencyTracker t("OKX");
    CHECK_EQ(t.getName(), std::string("OKX"));
    CHECK_EQ(t.count(), std::uint64_t{0});  // 空：所有统计量为 0
                                            // empty: every statistic is 0
    CHECK_EQ(t.percentileNs(0.5), std::uint64_t{0});
    CHECK_EQ(t.minNs(), std::uint64_t{0});
    CHECK_EQ(t.maxNs(), std::uint64_t{0});
    CHECK_EQ(t.meanNs(), 0.0);
    CHECK_EQ(t.getP50Us(), 0.0);
    CHECK_EQ(t.getP99Us(), 0.0);
    CHECK_EQ(t.fractionBelowNs(1000), 0.0);
    for (std::uint64_t us = 1; us <= 100; ++us) t.recordNs(us * 1000);
    CHECK_EQ(t.count(), std::uint64_t{100});
    CHECK_EQ(t.minNs(), std::uint64_t{1000});
    CHECK_EQ(t.maxNs(), std::uint64_t{100000});
    CHECK_NEAR(t.meanNs(), 50500.0, 1e-6);
    CHECK_EQ(t.percentileNs(0.0), std::uint64_t{1000});
    CHECK_EQ(t.percentileNs(1.0), std::uint64_t{100000});
    CHECK_NEAR(t.getP50Us(), 50.0, 50.0 / 64);
    CHECK_NEAR(t.getP99Us(), 99.0, 99.0 / 64);
    t.recordNs(10);  // < 128 ns 的样本精确记录
                     // samples < 128 ns are recorded exactly
    CHECK_EQ(t.count(), std::uint64_t{101});
    CHECK_EQ(t.minNs(), std::uint64_t{10});
    CHECK_EQ(t.percentileNs(0.0), std::uint64_t{10});
}

TEST_CASE("latency_tracker: fractionBelowNs at the display bucket edges") {
    LatencyTracker t("f");
    for (int i = 0; i < 50; ++i) t.recordNs(3000);
    for (int i = 0; i < 30; ++i) t.recordNs(7000);
    for (int i = 0; i < 20; ++i) t.recordNs(200000);
    CHECK_EQ(t.fractionBelowNs(0), 0.0);
    CHECK_NEAR(t.fractionBelowNs(5000), 0.5, 1e-12);
    CHECK_NEAR(t.fractionBelowNs(10000), 0.8, 1e-12);
    CHECK_NEAR(t.fractionBelowNs(20000), 0.8, 1e-12);
    CHECK_NEAR(t.fractionBelowNs(100000), 0.8, 1e-12);
    CHECK_NEAR(t.fractionBelowNs(kU64Max), 1.0, 1e-12);
}

TEST_CASE("latency_tracker: concurrent writer and dashboard reader (run under TSan)") {
    LatencyTracker t("feed");
    std::atomic<bool> done{false};
    std::thread writer([&t, &done] {
        for (std::uint64_t i = 0; i < 300'000; ++i) t.recordNs(i % 150'000);
        done.store(true, std::memory_order_release);
    });
    std::uint64_t bad = 0, iterations = 0;
    do {
        if (t.percentileNs(0.99) > 150'000 || t.getP50Us() > 150.0) ++bad;
        const double f = t.fractionBelowNs(50'000);
        if (f < 0.0 || f > 1.0) ++bad;
        if (t.meanNs() < 0.0 || t.meanNs() > 150'000.0) ++bad;
        const std::uint64_t lo = t.minNs();  // 先读 min 再读 max：min 只减不增、max 只增不减
                                             // read min before max: min only decreases, max only increases
        if (lo > t.maxNs()) ++bad;
        ++iterations;
    } while (!done.load(std::memory_order_acquire));
    writer.join();
    CHECK_EQ(bad, std::uint64_t{0});
    CHECK_GT(iterations, std::uint64_t{0});
    CHECK_EQ(t.count(), std::uint64_t{300'000});
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
