//
// matching_engine_benchmark.cpp
// 撮合引擎基准测试。注意：本程序直接包含并测试 include/matching/ 下的真实优化引擎（LimitOrderBook，
// 不是另写的副本），对照组是 tests/reference_order_book.h 中的参考模型（std::map + std::list + unordered_map）。
// 四类负载：(a) 盘口附近的被动挂单 + 撤单；(b) 随机生成器的混合真实流量（含拒单、STP、FOK、改单）；
// (c) 一次扫过多个价位的主动单；(d) 做市商式的被动改价（改价/加量失去时间优先级、减量原地修改）。
// 每类给出 ns/op 均值、p50/p99/p99.9（rdtsc 逐条计时、hft::TscClock 换算）、Mops/s、每操作的堆分配次数
// （计数版全局 operator new，只统计计时区间）。两个实现在同一命令流上的事件流必须逐条一致
// （FNV-1a 哈希校验，不一致则以非零码退出）。
// 用法：./build/matching_engine_benchmark [--quick] [--pin] [--reps N]
//   --quick  规模缩小 10 倍（冒烟运行）
//   --pin    把基准线程绑定到 CPU 0
//   --reps   吞吐量测量的重复次数（取中位数，默认 3）
// Matching engine benchmark. Note: this program directly includes and tests the real optimized engine
// under include/matching/ (LimitOrderBook, not a separately written copy); the baseline is the reference
// model in tests/reference_order_book.h (std::map + std::list + unordered_map).
// Four workloads: (a) passive adds + cancels near the touch; (b) realistic mixed flow from the random
// generator (including rejects, STP, FOK, modifies);
// (c) aggressive orders that sweep several price levels at once; (d) market-maker-style passive repricing
// (a reprice/quantity increase loses time priority, a quantity decrease modifies in place).
// Each reports the mean ns/op, p50/p99/p99.9 (per-operation rdtsc timing, converted with hft::TscClock),
// Mops/s and heap allocations per operation (counting global operator new, counted only inside the timed
// regions). The event streams of the two implementations on the same command stream must match event by
// event (checked with an FNV-1a hash; on a mismatch the program exits with a non-zero code).
// Usage: ./build/matching_engine_benchmark [--quick] [--pin] [--reps N]
//   --quick  10 times smaller scale (smoke run)
//   --pin    pin the benchmark thread to CPU 0
//   --reps   repetitions of the throughput measurement (the median is taken, default 3)
//

#include "hft/alloc_counter.h"
#include "hft/latency_histogram.h"
#include "hft/timing.h"
#include "matching/order_book.h"
#include "matching/types.h"

#include "matching_test_util.h"      // tests/（CMake 为本目标加入的 include 目录）
                                     // tests/ (an include directory CMake adds for this target)
#include "reference_order_book.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

using namespace hft::matching;
using hft::matching::test::apply;
using hft::matching::test::Command;
using hft::matching::test::CountingListener;
using hft::alloc_counter::total;  // 堆分配计数（计数版全局 operator new，本程序唯一包含它的翻译单元）
                                  // heap allocation count (counting global operator new; the only
                                  // translation unit of this program that includes it)

// 编译器屏障：不让被测代码跨过计时点重排
// Compiler barrier: keeps the code under test from being reordered across the timing points
inline void compilerBarrier() { __asm__ __volatile__("" : : : "memory"); }

// 校验用的监听器：对每个事件的全部字段做 FNV-1a 哈希（不计时）
// Listener for verification: an FNV-1a hash over all fields of every event (not timed)
struct HashListener {
    std::uint64_t hash = 1469598103934665603ULL;
    std::uint64_t events = 0;
    template <class... Fields>
    void mix(std::uint64_t kind, Fields... fields) {
        ++events;
        for (const std::uint64_t v : {kind, static_cast<std::uint64_t>(fields)...}) hash = (hash ^ v) * 1099511628211ULL;
    }
    void onAccepted(const OrderRequest& r) { mix(1, r.id); }
    void onRejected(OrderId id, RequestType t, RejectReason r) { mix(2, id, t, r); }
    void onTrade(const Trade& t) {
        mix(3, t.trade_id, t.maker_id, t.taker_id, t.maker_account, t.taker_account, t.taker_side, t.price, t.qty,
            t.maker_remaining, t.taker_remaining);
    }
    void onCancelled(OrderId id, Qty q, CancelReason r) { mix(4, id, q, r); }
    void onModified(OrderId id, Price p, Qty q) { mix(5, id, p, q); }
};

// 一个负载：setup 不计时；steps 中 timed == false 的步骤（例如扫单之后补回流动性）也不计时
// A workload: setup is not timed; steps with timed == false (e.g. replenishing liquidity after a sweep)
// are not timed either
struct Step {
    Command cmd;
    bool timed;
};

struct Workload {
    std::string name;
    std::string unit;  // 每个计时操作是什么
                       // what each timed operation is
    std::vector<Command> setup;
    std::vector<Step> steps;
    std::size_t reserve_orders = 4096;
    bool all_timed = true;
};

constexpr Price kMid = 1'000'000;

OrderRequest limitOrder(OrderId id, bool is_buy, Price price, Qty qty, AccountId account) {
    return is_buy ? test::buy(id, price, qty).account(account) : test::sell(id, price, qty).account(account);
}

// ---------------------------------------------------------------------------
// (a) 盘口附近的被动挂单 + 撤单：两侧各 20 档底仓（价差 6 tick，永不撤），新单落在离盘口 -2..+4 tick 内
//     （价差内的价位会被反复新建与删除），活动新单维持在几百到两千之间，撤单随机挑一个活动新单。
// (a) Passive adds + cancels near the touch: 20 levels of base liquidity on each side (6-tick spread,
//     never cancelled), new orders land within -2..+4 ticks of the touch (price levels inside the
//     spread are created and deleted over and over), live new orders stay between a few hundred and
//     two thousand, and a cancel picks a random live new order.
// ---------------------------------------------------------------------------
Workload makePassive(std::size_t ops) {
    Workload w;
    w.name = "(a) passive add/cancel near the touch";
    w.unit = "one add or one cancel";
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
                                                                     // -2..4: negative = inside the spread
            const Price price = is_buy ? kMid - 3 - offset : kMid + 3 + offset;
            const Qty qty = 1 + static_cast<Qty>(rng() % 10);
            const AccountId account = static_cast<AccountId>(3 + rng() % 8);
            w.steps.push_back({Command::newOrder(limitOrder(next_id, is_buy, price, qty, account)), true});
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
// (b) Realistic mixed flow: the random command generator of tests/matching_test_util.h (default
//     parameters: random-walk mid price, passive orders clustered near the touch, 20% aggressive
//     orders, all order types/TIF/post-only/STP, cancels and modifies hitting both live and stale ids,
//     a few invalid requests). First runs 20k commands untimed to reach steady state.
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
// (c) Aggressive sweeps: 200 levels on each side, 2 orders per level. Each sweep is one IOC limit order
//     that exactly clears the best kSweepLevels levels (2 * kSweepLevels fills), after which those price
//     levels are replenished as they were, untimed; the buy and sell sides take turns sweeping.
// ---------------------------------------------------------------------------
constexpr Price kSweepLevels = 20;

Workload makeSweeps(std::size_t sweeps) {
    Workload w;
    w.name = "(c) aggressive sweep of 20 levels";
    w.unit = "one sweep (40 fills)";
    w.all_timed = false;
    constexpr Price kDepth = 200;
    constexpr Qty kLot = 5;
    OrderId next_id = 1;
    auto maker = [&](bool is_buy, Price price) {
        const OrderRequest r = limitOrder(next_id, is_buy, price, kLot, static_cast<AccountId>(1 + next_id % 4));
        ++next_id;
        return Command::newOrder(r);
    };
    for (Price d = kDepth; d >= 1; --d) {
        for (int k = 0; k < 2; ++k) {
            w.setup.push_back(maker(true, kMid - d));
            w.setup.push_back(maker(false, kMid + d));
        }
    }
    for (std::size_t s = 0; s < sweeps; ++s) {
        const bool buy_side_taker = s % 2 == 0;  // 买单扫卖盘 / 卖单扫买盘
                                                 // a buy sweeps the asks / a sell sweeps the bids
        const Qty qty = 2 * kLot * kSweepLevels;
        const OrderRequest taker = buy_side_taker
                                       ? test::buy(next_id++, kMid + kSweepLevels, qty).ioc().account(9)
                                       : test::sell(next_id++, kMid - kSweepLevels, qty).ioc().account(9);
        w.steps.push_back({Command::newOrder(taker), true});
        // 由远及近补回被吃掉的价位：每次新建的都是新的最优档
        // Replenish the consumed price levels from far to near: each one created is the new best level
        for (Price d = kSweepLevels; d >= 1; --d) {
            for (int k = 0; k < 2; ++k) {
                w.steps.push_back({buy_side_taker ? maker(false, kMid + d) : maker(true, kMid - d), false});
            }
        }
    }
    return w;
}

// ---------------------------------------------------------------------------
// (d) 被动改价：两侧各 1000 张挂单分布在离中间价 1..20 tick 内；每次改单随机挑一张，改到本侧
//     1..20 tick 内的随机价位、数量 1..10（改价或加量失去时间优先级并重新挂单，同价减量原地修改），
//     永不穿价——做市商挪动报价的典型流量。
// (d) Passive repricing: 1000 resting orders per side spread within 1..20 ticks of the mid price; each
//     modify picks one at random and moves it to a random price within 1..20 ticks on its own side,
//     quantity 1..10 (a reprice or quantity increase loses time priority and re-rests, a quantity
//     decrease at the same price modifies in place), never crossing — the typical flow of a market
//     maker moving its quotes.
// ---------------------------------------------------------------------------
Workload makeReprice(std::size_t ops) {
    Workload w;
    w.name = "(d) passive reprice near the touch";
    w.unit = "one modify";
    std::mt19937_64 rng(13);
    auto price = [&rng](bool is_buy) {
        const Price offset = 1 + static_cast<Price>(rng() % 20);
        return is_buy ? kMid - offset : kMid + offset;
    };
    constexpr OrderId kOrders = 2000;  // 奇数 id 为买单
                                       // odd ids are buy orders
    for (OrderId id = 1; id <= kOrders; ++id) {
        const bool is_buy = id % 2 == 1;
        w.setup.push_back(Command::newOrder(limitOrder(id, is_buy, price(is_buy), 5, is_buy ? 1 : 2)));
    }
    for (std::size_t i = 0; i < ops; ++i) {
        const OrderId id = 1 + rng() % kOrders;
        const Price p = price(id % 2 == 1);
        const Qty q = 1 + static_cast<Qty>(rng() % 10);
        w.steps.push_back({Command::modify(id, p, q), true});
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

// 吞吐量：整段循环只在两端取时间，没有逐条计时开销（仅用于全部步骤都计时的负载）
// Throughput: the time is taken only at the two ends of the whole loop, with no per-operation timing
// overhead (only for workloads whose steps are all timed)
template <template <class> class BookT>
void throughputPass(const Workload& w, double& mean_ns, double& allocs_per_op) {
    CountingListener l;
    BookT<CountingListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) apply(book, c);
    const std::uint64_t a0 = total();
    compilerBarrier();
    const std::int64_t t0 = hft::nowNs();
    for (const Step& s : w.steps) apply(book, s.cmd);
    const std::int64_t t1 = hft::nowNs();
    compilerBarrier();
    allocs_per_op = static_cast<double>(total() - a0) / static_cast<double>(w.steps.size());
    mean_ns = static_cast<double>(t1 - t0) / static_cast<double>(w.steps.size());
}

// 逐条计时：rdtsc 开始、rdtscp 结束，记入直方图（单位 tick）；同时统计计时区间内的分配次数
// Per-operation timing: rdtsc at the start, rdtscp at the end, recorded in a histogram (in ticks); also
// counts the allocations inside the timed regions
template <template <class> class BookT>
Result latencyPass(const Workload& w) {
    CountingListener l;
    BookT<CountingListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) apply(book, c);
    hft::LatencyHistogram hist;
    std::uint64_t allocs = 0;
    std::uint64_t ticks = 0;
    std::uint64_t timed = 0;
    std::uint64_t events = 0;
    for (const Step& s : w.steps) {
        if (!s.timed) {
            apply(book, s.cmd);
            continue;
        }
        const std::uint64_t a0 = total();
        const std::uint64_t e0 = l.events;
        compilerBarrier();
        const std::uint64_t start = hft::rdtsc();
        compilerBarrier();
        apply(book, s.cmd);
        compilerBarrier();
        const std::uint64_t end = hft::rdtscp();
        compilerBarrier();
        const std::uint64_t dt = end > start ? end - start : 0;
        allocs += total() - a0;
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
        // 均值改用无逐条计时开销的吞吐量测量（重复 reps 次取中位数）；
        // 分配次数取逐条计时与最后一次吞吐量测量中的较大值
        // The mean comes from the throughput measurement instead, which has no per-operation timing overhead
        // (median of reps repetitions); the allocation count is the larger of the counts from the
        // per-operation timing and the last throughput measurement
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
// Event stream check: the hashes of all events of the two implementations on the same command stream
// must be identical
template <template <class> class BookT>
HashListener eventDigest(const Workload& w) {
    HashListener l;
    BookT<HashListener> book(l, 0, w.reserve_orders);
    for (const Command& c : w.setup) apply(book, c);
    for (const Step& s : w.steps) apply(book, s.cmd);
    return l;
}

// 计时器自身开销（空区间）
// Overhead of the timer itself (empty region)
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
    std::printf("(included in the percentiles; ns/op mean of (a)/(b)/(d) is the median of %d untimed-loop runs).\n\n",
                reps);

    const std::vector<Workload> workloads = {
        makePassive(2'000'000 / scale),
        makeMixed(2'000'000 / scale),
        makeSweeps(100'000 / scale),
        makeReprice(2'000'000 / scale),
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
