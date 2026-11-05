//
// matching_engine_benchmark.cpp
// 撮合引擎基准测试。注意：本程序直接包含并测试 include/matching/ 下的真实优化引擎（LimitOrderBook，
// 不是另写的副本），对照组是 tests/reference_order_book.h 中的参考模型（std::map + std::list + unordered_map）。
// 三类负载：(a) 盘口附近的被动挂单 + 撤单；(b) 随机生成器的混合真实流量（含拒单、STP、FOK、改单）；
// (c) 一次扫过多个价位的主动单。每类给出 ns/op 均值、p50/p99/p99.9（rdtsc 逐条计时、hft::TscClock 换算）、
// Mops/s、每操作的堆分配次数（计数版全局 operator new，只统计计时区间）。两个实现在同一命令流上的
// 事件流必须逐条一致（FNV-1a 哈希校验，不一致则以非零码退出）。
// 用法：./build/matching_engine_benchmark [--quick] [--pin] [--reps N]
//   --quick  规模缩小 10 倍（冒烟运行）
//   --pin    把基准线程绑定到 CPU 0
//   --reps   吞吐量测量的重复次数（取中位数，默认 3）
//

#include "hft/alloc_counter.h"
#include "hft/latency_histogram.h"
#include "hft/timing.h"
#include "matching/order_book.h"
#include "matching/types.h"

#include "matching_test_util.h"      // tests/（CMake 为本目标加入的 include 目录）
#include "reference_order_book.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <random>
#include <string>
#include <vector>

// 堆分配计数见 hft/alloc_counter.h（计数版全局 operator new/delete，本程序唯一包含它的翻译单元）
namespace {
std::atomic<std::uint64_t>& g_allocations = hft::alloc_counter::g_total;
}  // namespace

namespace {

using namespace hft::matching;
using hft::matching::test::Command;

// 编译器屏障：不让被测代码跨过计时点重排
inline void compilerBarrier() { __asm__ __volatile__("" : : : "memory"); }

// 计时用的监听器：只计数（与真实监听器一样有一点工作量，但不分配、不哈希）
struct CountingListener {
    std::uint64_t events = 0;
    std::uint64_t trades = 0;
    std::uint64_t traded_qty = 0;
    void onAccepted(const OrderRequest&) { ++events; }
    void onRejected(OrderId, RequestType, RejectReason) { ++events; }
    void onTrade(const Trade& t) {
        ++events;
        ++trades;
        traded_qty += static_cast<std::uint64_t>(t.qty);
    }
    void onCancelled(OrderId, Qty, CancelReason) { ++events; }
    void onModified(OrderId, Price, Qty) { ++events; }
};

// 校验用的监听器：对每个事件的全部字段做 FNV-1a 哈希（不计时）
struct HashListener {
    std::uint64_t hash = 1469598103934665603ULL;
    std::uint64_t events = 0;
    void mix(std::uint64_t v) { hash = (hash ^ v) * 1099511628211ULL; }
    template <class E>
    std::uint64_t u(E v) {
        return static_cast<std::uint64_t>(v);
    }
    void onAccepted(const OrderRequest& r) {
        ++events;
        mix(1);
        mix(r.id);
    }
    void onRejected(OrderId id, RequestType t, RejectReason r) {
        ++events;
        mix(2);
        mix(id);
        mix(u(t));
        mix(u(r));
    }
    void onTrade(const Trade& t) {
        ++events;
        mix(3);
        mix(t.trade_id);
        mix(t.maker_id);
        mix(t.taker_id);
        mix(t.maker_account);
        mix(t.taker_account);
        mix(u(t.taker_side));
        mix(u(t.price));
        mix(u(t.qty));
        mix(u(t.maker_remaining));
        mix(u(t.taker_remaining));
    }
    void onCancelled(OrderId id, Qty q, CancelReason r) {
        ++events;
        mix(4);
        mix(id);
        mix(u(q));
        mix(u(r));
    }
    void onModified(OrderId id, Price p, Qty q) {
        ++events;
        mix(5);
        mix(id);
        mix(u(p));
        mix(u(q));
    }
};

// 一个负载：setup 不计时；steps 中 timed == false 的步骤（例如扫单之后补回流动性）也不计时
struct Step {
    Command cmd;
    bool timed;
};

struct Workload {
    std::string name;
    std::string unit;  // 每个计时操作是什么
    std::vector<Command> setup;
    std::vector<Step> steps;
    std::size_t reserve_orders = 4096;
    bool all_timed = true;
};

// ---------------------------------------------------------------------------
// (a) 盘口附近的被动挂单 + 撤单：两侧各 20 档底仓（价差 6 tick，永不撤），新单落在离盘口 -2..+4 tick 内
//     （价差内的价位会被反复新建与删除），活动新单维持在几百到两千之间，撤单随机挑一个活动新单。
// ---------------------------------------------------------------------------
Workload makePassive(std::size_t ops) {
    Workload w;
    w.name = "(a) passive add/cancel near the touch";
    w.unit = "one add or one cancel";
    constexpr Price kMid = 1'000'000;
    OrderId next_id = 1;
    for (Price d = 3; d < 23; ++d) {
        for (int k = 0; k < 4; ++k) {
            w.setup.push_back(Command::newOrder(test::buy(next_id++, kMid - d, 10).account(1)));
            w.setup.push_back(Command::newOrder(test::sell(next_id++, kMid + d, 10).account(2)));
        }
    }
    std::mt19937_64 rng(11);
    std::vector<OrderId> live;
    live.reserve(4096);
    for (std::size_t i = 0; i < ops; ++i) {
        const bool add = live.size() < 300 || (live.size() < 2000 && rng() % 2 == 0);
        if (add) {
            const bool is_buy = rng() % 2 == 0;
            const Price offset = static_cast<Price>(rng() % 7) - 2;  // -2..4：负数 = 价差内
            const Price price = is_buy ? kMid - 3 - offset : kMid + 3 + offset;
            const Qty qty = 1 + static_cast<Qty>(rng() % 10);
            const AccountId account = static_cast<AccountId>(3 + rng() % 8);
            const OrderRequest r = is_buy ? test::buy(next_id, price, qty).account(account)
                                          : test::sell(next_id, price, qty).account(account);
            w.steps.push_back({Command::newOrder(r), true});
            live.push_back(next_id++);
        } else {
            const std::size_t k = static_cast<std::size_t>(rng() % live.size());
            w.steps.push_back({Command::cancel(live[k]), true});
            live[k] = live.back();
            live.pop_back();
        }
    }
    return w;
}

// ---------------------------------------------------------------------------
// (b) 混合真实流量：tests/matching_test_util.h 的随机命令生成器（默认参数：中间价随机游走、
//     盘口附近成簇的被动单、两成主动单、全部订单类型/TIF/post-only/STP、撤单改单命中活动与失效 id、
//     少量非法请求）。先不计时地跑 20k 条进入稳态。
// ---------------------------------------------------------------------------
Workload makeMixed(std::size_t ops) {
    Workload w;
    w.name = "(b) mixed flow (random generator)";
    w.unit = "one command";
    test::RandomCommandGenerator gen(20'260'000);
    for (int i = 0; i < 20'000; ++i) w.setup.push_back(gen.next());
    for (std::size_t i = 0; i < ops; ++i) w.steps.push_back({gen.next(), true});
    return w;
}

// ---------------------------------------------------------------------------
// (c) 主动扫单：两侧各 200 档、每档 2 单。每次扫单是一张 IOC 限价单，恰好吃光最优的 kSweepLevels 档
//     （2 * kSweepLevels 笔成交），然后不计时地按原样补回这些价位；买卖两侧交替扫。
// ---------------------------------------------------------------------------
constexpr Price kSweepLevels = 20;

Workload makeSweeps(std::size_t sweeps) {
    Workload w;
    w.name = "(c) aggressive sweep of 20 levels";
    w.unit = "one sweep (40 fills)";
    w.all_timed = false;
    constexpr Price kMid = 1'000'000;
    constexpr Price kDepth = 200;
    constexpr Qty kLot = 5;
    OrderId next_id = 1;
    auto maker = [&](Side side, Price price) {
        const AccountId account = static_cast<AccountId>(1 + next_id % 4);
        const OrderRequest r = side == Side::Buy ? test::buy(next_id, price, kLot).account(account)
                                                 : test::sell(next_id, price, kLot).account(account);
        ++next_id;
        return Command::newOrder(r);
    };
    for (Price d = kDepth; d >= 1; --d) {
        for (int k = 0; k < 2; ++k) {
            w.setup.push_back(maker(Side::Buy, kMid - d));
            w.setup.push_back(maker(Side::Sell, kMid + d));
        }
    }
    for (std::size_t s = 0; s < sweeps; ++s) {
        const bool buy_side_taker = s % 2 == 0;  // 买单扫卖盘 / 卖单扫买盘
        const Qty qty = 2 * kLot * kSweepLevels;
        const OrderRequest taker = buy_side_taker
                                       ? test::buy(next_id++, kMid + kSweepLevels, qty).ioc().account(9)
                                       : test::sell(next_id++, kMid - kSweepLevels, qty).ioc().account(9);
        w.steps.push_back({Command::newOrder(taker), true});
        // 由远及近补回被吃掉的价位：每次新建的都是新的最优档
        for (Price d = kSweepLevels; d >= 1; --d) {
            for (int k = 0; k < 2; ++k) {
                w.steps.push_back({buy_side_taker ? maker(Side::Sell, kMid + d) : maker(Side::Buy, kMid - d), false});
            }
        }
    }
    return w;
}

struct Result {
    double mean_ns = 0;
    double p50_ns = 0;
    double p99_ns = 0;
    double p999_ns = 0;
    double allocs_per_op = 0;
    double events_per_op = 0;
    std::uint64_t timed_ops = 0;
};

template <class Book>
void execute(Book& book, const Command& c) {
    hft::matching::test::apply(book, c);
}

// 吞吐量：整段循环只在两端取时间，没有逐条计时开销（仅用于全部步骤都计时的负载）
template <template <class> class BookT>
void throughputPass(const Workload& w, double& mean_ns, double& allocs_per_op) {
    CountingListener l;
    BookT<CountingListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) execute(book, c);
    const std::uint64_t a0 = g_allocations;
    compilerBarrier();
    const std::int64_t t0 = hft::nowNs();
    for (const Step& s : w.steps) execute(book, s.cmd);
    const std::int64_t t1 = hft::nowNs();
    compilerBarrier();
    allocs_per_op = static_cast<double>(g_allocations - a0) / static_cast<double>(w.steps.size());
    mean_ns = static_cast<double>(t1 - t0) / static_cast<double>(w.steps.size());
}

// 逐条计时：rdtsc 开始、rdtscp 结束，记入直方图（单位 tick）；同时统计计时区间内的分配次数
template <template <class> class BookT>
Result latencyPass(const Workload& w) {
    CountingListener l;
    BookT<CountingListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) execute(book, c);
    hft::LatencyHistogram hist;
    std::uint64_t allocs = 0;
    std::uint64_t ticks = 0;
    std::uint64_t timed = 0;
    std::uint64_t events = 0;
    for (const Step& s : w.steps) {
        if (!s.timed) {
            execute(book, s.cmd);
            continue;
        }
        const std::uint64_t a0 = g_allocations;
        const std::uint64_t e0 = l.events;
        compilerBarrier();
        const std::uint64_t start = hft::rdtsc();
        compilerBarrier();
        execute(book, s.cmd);
        compilerBarrier();
        const std::uint64_t end = hft::rdtscp();
        compilerBarrier();
        const std::uint64_t dt = end > start ? end - start : 0;
        allocs += g_allocations - a0;
        events += l.events - e0;
        hist.record(dt);
        ticks += dt;
        ++timed;
    }
    const double ns_per_tick = hft::TscClock::instance().nsPerTick();
    Result r;
    r.timed_ops = timed;
    r.mean_ns = static_cast<double>(ticks) * ns_per_tick / static_cast<double>(timed);
    r.p50_ns = static_cast<double>(hist.percentile(0.50)) * ns_per_tick;
    r.p99_ns = static_cast<double>(hist.percentile(0.99)) * ns_per_tick;
    r.p999_ns = static_cast<double>(hist.percentile(0.999)) * ns_per_tick;
    r.allocs_per_op = static_cast<double>(allocs) / static_cast<double>(timed);
    r.events_per_op = static_cast<double>(events) / static_cast<double>(timed);
    return r;
}

template <template <class> class BookT>
Result measure(const Workload& w, int reps) {
    Result r = latencyPass<BookT>(w);
    if (w.all_timed) {
        // 均值与分配次数改用无逐条计时开销的吞吐量测量（重复 reps 次取中位数）
        std::vector<double> means;
        double allocs = 0;
        for (int i = 0; i < reps; ++i) {
            double mean = 0;
            throughputPass<BookT>(w, mean, allocs);
            means.push_back(mean);
        }
        std::sort(means.begin(), means.end());
        r.mean_ns = means[means.size() / 2];
        r.allocs_per_op = std::max(r.allocs_per_op, allocs);
    }
    return r;
}

// 事件流校验：两个实现在同一命令流上的全部事件哈希必须相同
template <template <class> class BookT>
HashListener eventDigest(const Workload& w) {
    HashListener l;
    BookT<HashListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) execute(book, c);
    for (const Step& s : w.steps) execute(book, s.cmd);
    return l;
}

// 计时器自身开销（空区间）
double timerOverheadNs() {
    hft::LatencyHistogram hist;
    for (int i = 0; i < 200'000; ++i) {
        compilerBarrier();
        const std::uint64_t start = hft::rdtsc();
        compilerBarrier();
        const std::uint64_t end = hft::rdtscp();
        compilerBarrier();
        hist.record(end > start ? end - start : 0);
    }
    return static_cast<double>(hist.percentile(0.5)) * hft::TscClock::instance().nsPerTick();
}

void printRow(const char* workload, const char* impl, const Result& r) {
    std::printf("%-38s %-10s %9llu %9.1f %8.0f %8.0f %8.0f %9.2f %10.3f %9.2f\n", workload, impl,
                static_cast<unsigned long long>(r.timed_ops), r.mean_ns, r.p50_ns, r.p99_ns, r.p999_ns,
                1e3 / r.mean_ns, r.allocs_per_op, r.events_per_op);
}

}  // namespace

int main(int argc, char** argv) {
    bool quick = false;
    bool pin = false;
    int reps = 3;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--quick") == 0) {
            quick = true;
        } else if (std::strcmp(argv[i], "--pin") == 0) {
            pin = true;
        } else if (std::strcmp(argv[i], "--reps") == 0 && i + 1 < argc) {
            reps = std::max(1, std::atoi(argv[++i]));
        } else {
            std::printf("usage: %s [--quick] [--pin] [--reps N]\n", argv[0]);
            return std::strcmp(argv[i], "--help") == 0 ? 0 : 2;
        }
    }
    if (pin && !hft::pinThreadToCpu(0)) std::fprintf(stderr, "warning: could not pin to CPU 0\n");
    const std::size_t scale = quick ? 10 : 1;

    const double ns_per_tick = hft::TscClock::instance().nsPerTick();
    std::printf("Matching engine benchmark: optimized LimitOrderBook (include/matching/order_book.h)\n");
    std::printf("vs ReferenceOrderBook (tests/reference_order_book.h). TSC %.3f GHz; timer overhead p50 %.1f ns\n",
                1.0 / ns_per_tick, timerOverheadNs());
    std::printf("(included in the percentiles; ns/op mean of (a)/(b) is the median of %d untimed-loop runs).\n\n",
                reps);

    const std::vector<Workload> workloads = {
        makePassive(2'000'000 / scale),
        makeMixed(2'000'000 / scale),
        makeSweeps(100'000 / scale),
    };

    std::printf("%-38s %-10s %9s %9s %8s %8s %8s %9s %10s %9s\n", "workload", "impl", "ops", "ns/op", "p50",
                "p99", "p99.9", "Mops/s", "allocs/op", "events/op");
    std::printf("%s\n", std::string(130, '-').c_str());
    bool identical = true;
    for (const Workload& w : workloads) {
        const HashListener opt_digest = eventDigest<LimitOrderBook>(w);
        const HashListener ref_digest = eventDigest<ReferenceOrderBook>(w);
        const bool same = opt_digest.hash == ref_digest.hash && opt_digest.events == ref_digest.events;
        identical = identical && same;
        const Result opt = measure<LimitOrderBook>(w, reps);
        const Result ref = measure<ReferenceOrderBook>(w, reps);
        printRow(w.name.c_str(), "optimized", opt);
        printRow("", "reference", ref);
        std::printf("%-38s %-10s %9s %8.1fx %7.1fx %7.1fx %7.1fx   unit: %s; events %s\n", "", "speedup", "",
                    ref.mean_ns / opt.mean_ns, ref.p50_ns / opt.p50_ns, ref.p99_ns / opt.p99_ns,
                    ref.p999_ns / opt.p999_ns, w.unit.c_str(), same ? "identical" : "DIFFER");
    }
    std::printf("\nevent streams identical (FNV-1a over every field of every event): %s\n",
                identical ? "yes" : "NO");
    return identical ? 0 : 1;
}
