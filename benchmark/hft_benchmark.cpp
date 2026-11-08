//
// hft_benchmark.cpp
// HFT 基础库基准测试。注意：本程序直接包含并测试 include/hft/ 下的真实头文件（不是另写的副本），
// 对比 SPSC 无锁队列 vs std::mutex + std::deque、ObjectPool vs new/delete、
// FlatHashMap vs std::unordered_map（另测单次探测的组合操作），以及 LatencyHistogram vs 旧版"复制 + 排序采样"的分位数统计。
// 用法：./build/hft_benchmark [--quick] [--pin]
//   --quick  规模缩小 10 倍（冒烟运行）
//   --pin    把两个基准线程分别绑定到 CPU 0 / 1
//

#include "hft/common.h"
#include "hft/flat_hash_map.h"
#include "hft/latency_histogram.h"
#include "hft/object_pool.h"
#include "hft/spsc_queue.h"
#include "hft/timing.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

// 防止编译器把被测代码当作无用代码删除（同 Google Benchmark 的 DoNotOptimize）
template <class T>
inline void doNotOptimize(const T& value) {
    __asm__ __volatile__("" : : "r,m"(value) : "memory");
}

// 忙等：先自旋，长时间等不到再让出 CPU（共享机器上避免两个线程互相饿死）
inline void spinWait(unsigned& spins) {
    if (++spins < 4096) {
        hft::cpuRelax();
    } else {
        spins = 0;
        std::this_thread::yield();
    }
}

double nsSince(std::int64_t start_ns) { return static_cast<double>(hft::nowNs() - start_ns); }

void printHeader(const char* title) { std::printf("\n%s\n", title); }

void printRow(const char* name, double ns_per_op, const char* unit_note = "") {
    std::printf("  %-38s %12.2f ns/op %14.2f Mops/s %s\n", name, ns_per_op, 1e3 / ns_per_op, unit_note);
}

void printSpeedup(const char* what, double baseline_ns, double ours_ns) {
    std::printf("  %-38s %12.1fx\n", what, baseline_ns / ours_ns);
}

// 对照组：互斥锁 + std::deque 的有界队列（接口与 SpscQueue 的 tryPush/tryPop 一致）
template <class T>
class MutexQueue {
public:
    explicit MutexQueue(std::size_t capacity) : capacity_(capacity) {}
    bool tryPush(const T& v) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= capacity_) return false;
        queue_.push_back(v);
        return true;
    }
    bool tryPop(T& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        out = queue_.front();
        queue_.pop_front();
        return true;
    }

private:
    std::mutex mutex_;
    std::deque<T> queue_;
    std::size_t capacity_;
};

// ---------------------------------------------------------------------------
// 1. 队列吞吐：一个生产者线程、一个消费者线程，传递 items 个 uint64
// ---------------------------------------------------------------------------
template <class Queue>
double queueThroughputNsPerItem(Queue& q, std::uint64_t items, bool pin) {
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::uint64_t checksum = 0;
    std::thread consumer([&] {
        if (pin) hft::pinThreadToCpu(1);
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) hft::cpuRelax();
        std::uint64_t v = 0, sum = 0;
        for (std::uint64_t i = 0; i < items; ++i) {
            unsigned spins = 0;
            while (!q.tryPop(v)) spinWait(spins);
            sum += v;
        }
        checksum = sum;
    });
    std::thread producer([&] {
        if (pin) hft::pinThreadToCpu(0);
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) hft::cpuRelax();
        for (std::uint64_t i = 0; i < items; ++i) {
            unsigned spins = 0;
            while (!q.tryPush(i)) spinWait(spins);
        }
    });
    while (ready.load() != 2) std::this_thread::yield();
    const std::int64_t t0 = hft::nowNs();
    go.store(true, std::memory_order_release);
    producer.join();
    consumer.join();
    const double ns = nsSince(t0);
    if (checksum != items * (items - 1) / 2) {
        std::fprintf(stderr, "queue checksum mismatch\n");
        std::exit(1);
    }
    return ns / static_cast<double>(items);
}

// 往返延迟：主线程推入 TSC 时间戳，回声线程原样送回，主线程记录 RTT
template <class Queue>
void queuePingPong(Queue& to_echo, Queue& from_echo, std::uint64_t rounds, bool pin, hft::LatencyHistogram& rtt) {
    std::thread echo([&] {
        if (pin) hft::pinThreadToCpu(1);
        std::uint64_t v = 0;
        for (std::uint64_t i = 0; i < rounds; ++i) {
            unsigned spins = 0;
            while (!to_echo.tryPop(v)) spinWait(spins);
            spins = 0;
            while (!from_echo.tryPush(v)) spinWait(spins);
        }
    });
    std::thread pinger([&] {
        if (pin) hft::pinThreadToCpu(0);
        const hft::TscClock& clock = hft::TscClock::instance();
        std::uint64_t v = 0;
        for (std::uint64_t i = 0; i < rounds; ++i) {
            const std::uint64_t t0 = hft::rdtsc();
            unsigned spins = 0;
            while (!to_echo.tryPush(t0)) spinWait(spins);
            spins = 0;
            while (!from_echo.tryPop(v)) spinWait(spins);
            const std::int64_t ns = clock.toNs(hft::rdtscp() - v);
            if (i >= rounds / 10) rtt.record(ns > 0 ? static_cast<std::uint64_t>(ns) : 0);  // 前 10% 预热
        }
    });
    pinger.join();
    echo.join();
}

void benchQueues(std::uint64_t scale, bool pin) {
    printHeader("[1] SPSC queue vs std::mutex + std::deque (2 threads, uint64 items, capacity 4096)");
    const std::uint64_t spsc_items = 20'000'000 / scale;
    const std::uint64_t mutex_items = 2'000'000 / scale;
    double best_spsc = 1e30, best_mutex = 1e30;
    for (int run = 0; run < 3; ++run) {
        hft::SpscQueue<std::uint64_t> spsc(4096);
        best_spsc = std::min(best_spsc, queueThroughputNsPerItem(spsc, spsc_items, pin));
        MutexQueue<std::uint64_t> mq(4096);
        best_mutex = std::min(best_mutex, queueThroughputNsPerItem(mq, mutex_items, pin));
    }
    printRow("hft::SpscQueue throughput", best_spsc, "(best of 3)");
    printRow("mutex + deque throughput", best_mutex, "(best of 3)");
    printSpeedup("throughput speedup", best_mutex, best_spsc);

    const std::uint64_t rounds = 200'000 / scale;
    hft::LatencyHistogram spsc_rtt, mutex_rtt;
    {
        hft::SpscQueue<std::uint64_t> a(16), b(16);
        queuePingPong(a, b, rounds, pin, spsc_rtt);
    }
    {
        MutexQueue<std::uint64_t> a(16), b(16);
        queuePingPong(a, b, rounds, pin, mutex_rtt);
    }
    std::printf("  %-38s %10s %10s %10s %10s\n", "round trip (ns, TSC)", "p50", "p90", "p99", "p99.9");
    for (auto [name, h] : {std::pair<const char*, hft::LatencyHistogram*>{"hft::SpscQueue ping-pong", &spsc_rtt},
                           std::pair<const char*, hft::LatencyHistogram*>{"mutex + deque ping-pong", &mutex_rtt}}) {
        std::printf("  %-38s %10llu %10llu %10llu %10llu\n", name,
                    static_cast<unsigned long long>(h->percentile(0.50)),
                    static_cast<unsigned long long>(h->percentile(0.90)),
                    static_cast<unsigned long long>(h->percentile(0.99)),
                    static_cast<unsigned long long>(h->percentile(0.999)));
    }
}

// ---------------------------------------------------------------------------
// 2. 对象池 vs new/delete（64 字节的类订单对象）
// ---------------------------------------------------------------------------
struct BenchOrder {
    std::uint64_t id, account, price, qty, timestamp;
    BenchOrder* prev;
    BenchOrder* next;
    std::uint64_t flags;
};
static_assert(sizeof(BenchOrder) == 64, "BenchOrder should be one cache line");

void benchPool(std::uint64_t scale) {
    printHeader("[2] hft::ObjectPool vs new/delete (64-byte order objects; one op = create + destroy)");
    const std::uint64_t n = 20'000'000 / scale;
    double pool_lifo = 0, heap_lifo = 0, pool_rand = 0, heap_rand = 0;
    {
        hft::ObjectPool<BenchOrder> pool(1024);
        const std::int64_t t0 = hft::nowNs();
        for (std::uint64_t i = 0; i < n; ++i) {
            BenchOrder* o = pool.create(BenchOrder{i, 1, 100, 5, 0, nullptr, nullptr, 0});
            doNotOptimize(o);
            pool.destroy(o);
        }
        pool_lifo = nsSince(t0) / static_cast<double>(n);
    }
    {
        const std::int64_t t0 = hft::nowNs();
        for (std::uint64_t i = 0; i < n; ++i) {
            BenchOrder* o = new BenchOrder{i, 1, 100, 5, 0, nullptr, nullptr, 0};
            doNotOptimize(o);
            delete o;
        }
        heap_lifo = nsSince(t0) / static_cast<double>(n);
    }
    // 随机工作集：10 万个存活对象，每次随机销毁一个再创建一个（malloc 的碎片化场景）
    const std::size_t live_count = 100'000;
    std::vector<std::uint32_t> picks(1u << 20);
    std::mt19937_64 rng(42);
    for (auto& p : picks) p = static_cast<std::uint32_t>(rng() % live_count);
    const std::uint64_t m = 10'000'000 / scale;
    {
        hft::ObjectPool<BenchOrder> pool(live_count);
        std::vector<BenchOrder*> live(live_count);
        for (std::size_t i = 0; i < live_count; ++i) live[i] = pool.create(BenchOrder{i, 0, 0, 0, 0, nullptr, nullptr, 0});
        const std::int64_t t0 = hft::nowNs();
        for (std::uint64_t i = 0; i < m; ++i) {
            const std::uint32_t k = picks[i & (picks.size() - 1)];
            pool.destroy(live[k]);
            live[k] = pool.create(BenchOrder{i, 0, 0, 0, 0, nullptr, nullptr, 0});
            doNotOptimize(live[k]);
        }
        pool_rand = nsSince(t0) / static_cast<double>(m);
        for (BenchOrder* o : live) pool.destroy(o);
    }
    {
        std::vector<BenchOrder*> live(live_count);
        for (std::size_t i = 0; i < live_count; ++i) live[i] = new BenchOrder{i, 0, 0, 0, 0, nullptr, nullptr, 0};
        const std::int64_t t0 = hft::nowNs();
        for (std::uint64_t i = 0; i < m; ++i) {
            const std::uint32_t k = picks[i & (picks.size() - 1)];
            delete live[k];
            live[k] = new BenchOrder{i, 0, 0, 0, 0, nullptr, nullptr, 0};
            doNotOptimize(live[k]);
        }
        heap_rand = nsSince(t0) / static_cast<double>(m);
        for (BenchOrder* o : live) delete o;
    }
    printRow("ObjectPool LIFO create+destroy", pool_lifo);
    printRow("new/delete LIFO", heap_lifo);
    printSpeedup("LIFO speedup", heap_lifo, pool_lifo);
    printRow("ObjectPool random (100k live)", pool_rand);
    printRow("new/delete random (100k live)", heap_rand);
    printSpeedup("random working-set speedup", heap_rand, pool_rand);
}

// ---------------------------------------------------------------------------
// 3. FlatHashMap vs std::unordered_map（订单 id 索引场景）
// ---------------------------------------------------------------------------
template <class Map>
struct MapOps;

template <class V>
struct MapOps<hft::FlatHashMap<std::uint64_t, V>> {
    using M = hft::FlatHashMap<std::uint64_t, V>;
    static void insert(M& m, std::uint64_t k, V v) { m.insert(k, v); }
    static const V* find(const M& m, std::uint64_t k) { return m.find(k); }
    static void erase(M& m, std::uint64_t k) { m.erase(k); }
};

template <class V>
struct MapOps<std::unordered_map<std::uint64_t, V>> {
    using M = std::unordered_map<std::uint64_t, V>;
    static void insert(M& m, std::uint64_t k, V v) { m.emplace(k, v); }
    static const V* find(const M& m, std::uint64_t k) {
        const auto it = m.find(k);
        return it == m.end() ? nullptr : &it->second;
    }
    static void erase(M& m, std::uint64_t k) { m.erase(k); }
};

struct MapResult {
    double mixed = 1e30, find_hit = 1e30, find_miss = 1e30, insert = 1e30, erase = 1e30;

    // 逐项保留更快的一次
    void keepBest(const MapResult& r) {
        mixed = std::min(mixed, r.mixed);
        find_hit = std::min(find_hit, r.find_hit);
        find_miss = std::min(find_miss, r.find_miss);
        insert = std::min(insert, r.insert);
        erase = std::min(erase, r.erase);
    }
};

template <class Map>
MapResult benchMap(std::uint64_t scale, const std::vector<std::uint64_t>& random_keys) {
    using Ops = MapOps<Map>;
    MapResult r;
    const std::uint64_t window = 100'000;  // 存活订单数
    const std::uint64_t iters = 4'000'000 / scale;
    std::mt19937_64 rng(7);
    std::vector<std::uint32_t> offsets(1u << 20);
    for (auto& o : offsets) o = static_cast<std::uint32_t>(rng() % window);
    // 混合负载：每轮插入一个新 id，命中查找一次、未命中查找一次，撤销最老的订单（滑动窗口）
    {
        Map m;
        m.reserve(window * 2);
        std::uint64_t next_id = 1'000'000;
        for (std::uint64_t i = 0; i < window; ++i) Ops::insert(m, next_id++, i);
        std::uint64_t sink = 0;
        const std::int64_t t0 = hft::nowNs();
        for (std::uint64_t i = 0; i < iters; ++i) {
            Ops::insert(m, next_id, i);
            const std::uint64_t hit = next_id - offsets[i & (offsets.size() - 1)];
            const auto* p = Ops::find(m, hit);
            sink += p != nullptr ? *p : 0;
            const auto* q = Ops::find(m, random_keys[i & (random_keys.size() - 1)]);
            sink += q != nullptr ? 1 : 0;
            Ops::erase(m, next_id - window);
            ++next_id;
        }
        r.mixed = nsSince(t0) / static_cast<double>(iters * 4);
        doNotOptimize(sink);
    }
    // 单项操作：100 万个随机 64 位键
    const std::size_t n = static_cast<std::size_t>(1'000'000 / scale);
    {
        Map m;
        m.reserve(n);
        const std::int64_t t0 = hft::nowNs();
        for (std::size_t i = 0; i < n; ++i) Ops::insert(m, random_keys[i], i);
        r.insert = nsSince(t0) / static_cast<double>(n);
        std::uint64_t sink = 0;
        const std::int64_t t1 = hft::nowNs();
        for (std::size_t rep = 0; rep < 4; ++rep) {
            for (std::size_t i = 0; i < n; ++i) {
                const auto* p = Ops::find(m, random_keys[(i * 7919 + rep) % n]);
                sink += p != nullptr ? *p : 0;
            }
        }
        r.find_hit = nsSince(t1) / static_cast<double>(4 * n);
        const std::int64_t t2 = hft::nowNs();
        for (std::size_t i = 0; i < n; ++i) {
            const auto* p = Ops::find(m, random_keys[n + i]);  // 不在表中的键
            sink += p != nullptr ? 1 : 0;
        }
        r.find_miss = nsSince(t2) / static_cast<double>(n);
        const std::int64_t t3 = hft::nowNs();
        for (std::size_t i = 0; i < n; ++i) Ops::erase(m, random_keys[i]);
        r.erase = nsSince(t3) / static_cast<double>(n);
        doNotOptimize(sink);
    }
    return r;
}

// 单次探测的组合操作（撮合引擎订单索引 id -> 订单指针的挂单 / 撤单用法）：n 个随机键，表预留到 n，
// 先建表（不计时）再计时。挂单：contains 查重 + insert（两次探测） vs 插入占位值查重并预留、再经指针填写（一次）；
// 撤单：find 取值 + erase（两次探测） vs extract（一次） vs find + eraseAt（一次）。删除顺序与插入顺序无关。
// 值类型是指针（与订单索引相同），也让这一节的实例化与第 3 节的 <uint64, uint64> 互不影响内联决策
using ProbeMap = hft::FlatHashMap<std::uint64_t, const std::uint64_t*>;

template <class Add>
double timeAdds(const std::vector<std::uint64_t>& keys, std::size_t n, Add add) {
    ProbeMap m(n);
    const std::int64_t t0 = hft::nowNs();
    for (std::size_t i = 0; i < n; ++i) add(m, keys[i], &keys[i]);
    const double ns = nsSince(t0) / static_cast<double>(n);
    doNotOptimize(m.size());
    return ns;
}

template <class Remove>
double timeRemoves(const std::vector<std::uint64_t>& keys, const std::vector<std::uint64_t>& order, Remove remove) {
    ProbeMap m(order.size());
    for (std::size_t i = 0; i < order.size(); ++i) m.insert(keys[i], &keys[i]);
    std::uint64_t sink = 0;
    const std::int64_t t0 = hft::nowNs();
    for (std::uint64_t k : order) sink += remove(m, k) != nullptr ? 1 : 0;
    const double ns = nsSince(t0) / static_cast<double>(order.size());
    doNotOptimize(sink);
    return ns;
}

void benchSingleProbe(std::uint64_t scale, const std::vector<std::uint64_t>& keys) {
    using Ptr = const std::uint64_t*;
    const std::size_t n = static_cast<std::size_t>(1'000'000 / scale);
    std::vector<std::uint64_t> order(keys.begin(), keys.begin() + static_cast<std::ptrdiff_t>(n));
    std::shuffle(order.begin(), order.end(), std::mt19937_64(3));
    const auto contains_insert = [](ProbeMap& m, std::uint64_t k, Ptr v) {
        if (!m.contains(k)) m.insert(k, v);
    };
    const auto reserve_fill = [](ProbeMap& m, std::uint64_t k, Ptr v) {
        const auto [slot, fresh] = m.insert(k, nullptr);
        if (fresh) *slot = v;
    };
    const auto find_erase = [](ProbeMap& m, std::uint64_t k) {
        const Ptr* p = m.find(k);
        const Ptr v = p != nullptr ? *p : nullptr;
        m.erase(k);
        return v;
    };
    const auto extract = [](ProbeMap& m, std::uint64_t k) { return m.extract(k).value_or(nullptr); };
    const auto find_erase_at = [](ProbeMap& m, std::uint64_t k) {
        const Ptr* p = m.find(k);
        if (p == nullptr) return Ptr{nullptr};
        const Ptr v = *p;
        m.eraseAt(p);
        return v;
    };
    double best[5] = {1e30, 1e30, 1e30, 1e30, 1e30};
    for (int run = 0; run < 2; ++run) {
        best[0] = std::min(best[0], timeAdds(keys, n, contains_insert));
        best[1] = std::min(best[1], timeAdds(keys, n, reserve_fill));
        best[2] = std::min(best[2], timeRemoves(keys, order, find_erase));
        best[3] = std::min(best[3], timeRemoves(keys, order, extract));
        best[4] = std::min(best[4], timeRemoves(keys, order, find_erase_at));
    }
    std::printf("  %-38s %14s %14s\n", "single-probe primitives (best of 2)", "ns/op", "vs 2 probes");
    const struct {
        const char* name;
        double ns, two_probe_ns;
    } rows[] = {
        {"add: contains + insert (2 probes)", best[0], best[0]},
        {"add: insert placeholder + fill", best[1], best[0]},
        {"cancel: find + erase (2 probes)", best[2], best[2]},
        {"cancel: extract", best[3], best[2]},
        {"cancel: find + eraseAt", best[4], best[2]},
    };
    for (const auto& row : rows) std::printf("  %-38s %14.2f %13.2fx\n", row.name, row.ns, row.two_probe_ns / row.ns);
}

void benchMaps(std::uint64_t scale) {
    printHeader("[3] hft::FlatHashMap vs std::unordered_map (uint64 keys)");
    std::mt19937_64 rng(99);
    std::vector<std::uint64_t> keys(1u << 21);  // 前一半插入，后一半用作未命中查找
    for (auto& k : keys) k = rng();
    MapResult flat, std_map;
    for (int run = 0; run < 2; ++run) {
        flat.keepBest(benchMap<hft::FlatHashMap<std::uint64_t, std::uint64_t>>(scale, keys));
        std_map.keepBest(benchMap<std::unordered_map<std::uint64_t, std::uint64_t>>(scale, keys));
    }
    std::printf("  %-38s %14s %14s %9s\n", "operation (ns/op, best of 2)", "FlatHashMap", "unordered_map", "speedup");
    const struct {
        const char* name;
        double flat, std_map;
    } rows[] = {
        {"order-id mix (insert/hit/miss/erase)", flat.mixed, std_map.mixed},
        {"insert 1M random keys", flat.insert, std_map.insert},
        {"find hit (1M keys)", flat.find_hit, std_map.find_hit},
        {"find miss (1M keys)", flat.find_miss, std_map.find_miss},
        {"erase 1M random keys", flat.erase, std_map.erase},
    };
    for (const auto& row : rows) {
        std::printf("  %-38s %14.2f %14.2f %8.1fx\n", row.name, row.flat, row.std_map, row.std_map / row.flat);
    }
    benchSingleProbe(scale, keys);
}

// ---------------------------------------------------------------------------
// 4. LatencyHistogram vs 旧 LatencyTracker 的"复制 10000 个样本 + 排序"求分位数
// ---------------------------------------------------------------------------
void benchHistogram(std::uint64_t scale) {
    printHeader("[4] hft::LatencyHistogram vs old sorted-reservoir percentile");
    std::mt19937_64 rng(5);
    std::lognormal_distribution<double> ln(std::log(5000.0), 0.7);
    std::vector<std::uint64_t> values(1u << 16);
    for (auto& v : values) v = static_cast<std::uint64_t>(ln(rng));
    hft::LatencyHistogram h;
    const std::uint64_t n = 50'000'000 / scale;
    const std::int64_t t0 = hft::nowNs();
    for (std::uint64_t i = 0; i < n; ++i) h.record(values[i & (values.size() - 1)]);
    const double record_ns = nsSince(t0) / static_cast<double>(n);

    const int calls = static_cast<int>(200'000 / scale);
    std::uint64_t sink = 0;
    const std::int64_t t1 = hft::nowNs();
    for (int i = 0; i < calls; ++i) sink += h.percentile(i % 2 == 0 ? 0.99 : 0.5);
    const double pct_ns = nsSince(t1) / calls;

    // 旧实现：std::vector<long> 采样池（10000 个），每次求分位数复制并排序
    std::vector<long> reservoir(10'000);
    for (std::size_t i = 0; i < reservoir.size(); ++i) reservoir[i] = static_cast<long>(values[i] / 1000);
    const int old_calls = static_cast<int>(2'000 / scale) + 1;
    const std::int64_t t2 = hft::nowNs();
    for (int i = 0; i < old_calls; ++i) {
        std::vector<long> sorted = reservoir;
        std::sort(sorted.begin(), sorted.end());
        sink += static_cast<std::uint64_t>(sorted[static_cast<std::size_t>(std::ceil(0.99 * sorted.size())) - 1]);
    }
    const double old_ns = nsSince(t2) / old_calls;
    doNotOptimize(sink);

    printRow("LatencyHistogram::record", record_ns);
    printRow("LatencyHistogram::percentile", pct_ns);
    printRow("old copy+sort percentile (10k samples)", old_ns);
    printSpeedup("percentile speedup", old_ns, pct_ns);
    std::printf("  (histogram p50=%llu ns p99=%llu ns over %llu samples)\n",
                static_cast<unsigned long long>(h.percentile(0.5)), static_cast<unsigned long long>(h.percentile(0.99)),
                static_cast<unsigned long long>(h.count()));
}

}  // namespace

int main(int argc, char** argv) {
    bool quick = false, pin = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--quick") == 0) {
            quick = true;
        } else if (std::strcmp(argv[i], "--pin") == 0) {
            pin = true;
        } else {
            std::printf("usage: %s [--quick] [--pin]\n", argv[0]);
            return std::strcmp(argv[i], "--help") == 0 ? 0 : 1;
        }
    }
    const std::uint64_t scale = quick ? 10 : 1;
    std::printf("HFT core benchmark (real headers from include/hft/)%s%s\n", quick ? " [quick]" : "",
                pin ? " [pinned to CPU 0/1]" : "");
    std::printf("TSC: %.4f ns/tick (%.3f GHz), hardware threads: %u\n", hft::TscClock::instance().nsPerTick(),
                1.0 / hft::TscClock::instance().nsPerTick(), std::thread::hardware_concurrency());
    benchQueues(scale, pin);
    benchPool(scale);
    benchMaps(scale);
    benchHistogram(scale);
    std::printf("\nDone.\n");
    return 0;
}
