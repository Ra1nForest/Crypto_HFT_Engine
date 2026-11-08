//
// market_data_benchmark.cpp
// 行情模块基准测试。注意：本程序直接包含并测试 include/ 下的真实头文件（exchanges/*、orderbook.h、hft/crc32.h），
// 不是另写的副本；"旧实现"一栏是升级前 market_data_feed.h 的处理流程（buffers_to_string + 每条消息构造
// padded_string + 旧的 parseBinanceDepth），原样复制在本文件中作为对照。
// 内容：
//   1. 各适配器每条消息的处理耗时（adapter.onMessage，TSC 逐条计时；精确的 p50/p90/p99、均值、超过 100 µs 的比例）
//      a. 模拟交易所的行情流：与离线演示中监控程序收到的报文以同样的方式生成——tools/mock_market.h 的市场仿真驱动
//         tools/mock_publishers.h 的发布状态机（种子、预热、窗口与推送周期同 tools/mock_exchange.h 的默认值）：
//         Binance depth20 与 1000 档增量、OKX books 400 档增量与快照（含校验和）/ books5 / bbo-tbt、Bybit orderbook.50。
//         增量平均上百档变化、分布在整个窗口上，大部分是插入与删除。引用"每条消息的耗时"时用这一组。
//      b. 同一批增量只看簿的应用：逐档 setLevel（0ec0622 之前适配器的做法）vs 批量 applyLevels，A/B 交错计时，
//         最终簿必须逐位相同；同时打印实测的簿深度、变化构成与"最优在前"的比例。
//      c. 合成的小增量（原来的第 1 节，保留作微基准）：只改动最优 30 档，预热后几乎全是原地改量，
//         碰不到插入/删除的 memmove，远低于真实行情的每条消息耗时，不要当作每条消息的成本引用。
//      --cold：a 组再测一遍冷缓存（每条消息之前遍历一块大于 LLC 的缓冲区）。真实行情每 10-100 ms 才来一条，
//      处理时缓存多半是冷的；默认的热缓存、背靠背计时是下限。
//   2. 旧流程 vs 新流程：同一批 Binance depth20 消息，端到端耗时与每条消息的堆分配次数；以及只看缓冲区处理的耗时。
//      两种流程在同一条消息上交替计时（A/B 交错，奇偶轮换先后），共享 CPU 上的噪声对两者影响相同。
//      旧流程使用的是升级后的 OrderBook（尾部快路径），因此差异只来自消息处理方式。
//   3. OrderBook upsert 微基准：400 档簿上的顶部更新（原地改量、顶部插入/删除、随机档位、旧式整簿重建）
//   4. 一条增量应用到深簿：逐档 setLevel vs 批量 applyLevels（最优在前 / 打乱两种顺序）。生成器保持簿深度等于标签、
//      插入/删除/改量各约 1/3，实测的平均深度与变化构成打印在每行旁边。
// 注意：机器上的 CPU 由多个进程共享，单次运行的绝对值可能有 ±50% 的波动；比较请看同一次运行内的相对值，
// 或把新旧两个二进制交替运行多次。代码布局也会让个别小消息的行（如 bbo-tbt）在不同构建间相差 10-20%。
// 自检（行情流失去同步或校验和失败、簿不一致、深度或构成偏离标签等）失败时打印 "!!"，程序以非零状态退出。
// 用法：./build/market_data_benchmark [--quick] [--cold]
//

#include "hft/alloc_counter.h"
#include "book_snapshot.h"
#include "exchanges/adapters.h"
#include "hft/crc32.h"
#include "hft/latency_histogram.h"
#include "hft/timing.h"
#include "mock_publishers.h"  // tools/（CMake 为本目标加入的 include 目录）
#include "orderbook.h"

#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <new>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 堆分配计数见 hft/alloc_counter.h（计数版全局 operator new/delete，本程序唯一包含它的翻译单元）
namespace {
std::atomic<std::uint64_t>& g_allocs = hft::alloc_counter::g_total;
}  // namespace

namespace {

// 防止编译器把被测代码当作无用代码删除
template <class T>
inline void doNotOptimize(const T& value) {
    __asm__ __volatile__("" : : "r,m"(value) : "memory");
}

using Level = std::pair<std::string, std::string>;
using Levels = std::vector<Level>;

std::string fixedTicks(std::int64_t ticks, int decimals) {
    std::int64_t scale = 1;
    for (int i = 0; i < decimals; ++i) scale *= 10;
    std::string frac = std::to_string(ticks % scale);
    while (static_cast<int>(frac.size()) < decimals) frac.insert(frac.begin(), '0');
    return std::to_string(ticks / scale) + (decimals > 0 ? "." + frac : "");
}

std::string levelsJson(const Levels& levels, bool okx_extra = false) {
    std::string s = "[";
    for (std::size_t i = 0; i < levels.size(); ++i) {
        if (i != 0) s += ',';
        s += "[\"" + levels[i].first + "\",\"" + levels[i].second + "\"";
        if (okx_extra) s += ",\"0\",\"4\"";
        s += ']';
    }
    return s + "]";
}

std::string qty8(std::mt19937_64& rng) { return fixedTicks(1 + static_cast<std::int64_t>(rng() % 300000000), 8); }

// ---------------------------------------------------------------------------
// 第 2 节的计时：每条消息 rdtsc/rdtscp，结果按 TSC 频率换算为 ns 记入直方图
// ---------------------------------------------------------------------------
struct Result {
    const char* name;
    std::size_t messages;
    double bytes_per_msg;
    double levels_per_msg;
    std::uint64_t p50, p99;
    double mean;
    double allocs_per_msg;
};

// A/B 交替计时：同一条消息上紧挨着分别跑 A 与 B（奇偶轮换先后顺序），两者经历相同的机器状态，
// 共享 CPU 上的噪声对比较的影响最小
template <class FnA, class FnB>
std::pair<Result, Result> measureAB(const char* name_a, const char* name_b, const std::vector<std::string>& msgs,
                                    double levels_per_msg, FnA&& fa, FnB&& fb) {
    hft::LatencyHistogram ha, hb;
    const hft::TscClock& clock = hft::TscClock::instance();
    std::uint64_t allocs_a = 0, allocs_b = 0;
    double bytes = 0;
    auto timed = [&](auto& fn, const std::string& m, hft::LatencyHistogram& h, std::uint64_t& allocs) {
        const std::uint64_t a0 = g_allocs;
        const std::uint64_t t0 = hft::rdtsc();
        fn(m);
        const std::uint64_t t1 = hft::rdtscp();
        allocs += g_allocs - a0;
        h.record(static_cast<std::uint64_t>(std::max<std::int64_t>(0, clock.toNs(t1 - t0))));
    };
    for (std::size_t i = 0; i < msgs.size(); ++i) {
        if ((i & 1) == 0) {
            timed(fa, msgs[i], ha, allocs_a);
            timed(fb, msgs[i], hb, allocs_b);
        } else {
            timed(fb, msgs[i], hb, allocs_b);
            timed(fa, msgs[i], ha, allocs_a);
        }
        bytes += static_cast<double>(msgs[i].size());
    }
    const double n = static_cast<double>(msgs.size());
    return {Result{name_a, msgs.size(), bytes / n, levels_per_msg, ha.percentile(0.50), ha.percentile(0.99),
                   ha.mean(), static_cast<double>(allocs_a) / n},
            Result{name_b, msgs.size(), bytes / n, levels_per_msg, hb.percentile(0.50), hb.percentile(0.99),
                   hb.mean(), static_cast<double>(allocs_b) / n}};
}

void printHeader() {
    std::printf("  %-44s %7s %9s %7s %8s %8s %8s %8s %9s\n", "scenario", "msgs", "bytes", "levels", "p50 ns",
                "p99 ns", "mean ns", "ns/lvl", "allocs/m");
}

void printResult(const Result& r) {
    std::printf("  %-44s %7zu %9.0f %7.1f %8llu %8llu %8.0f %8.1f %9.2f\n", r.name, r.messages, r.bytes_per_msg,
                r.levels_per_msg, static_cast<unsigned long long>(r.p50), static_cast<unsigned long long>(r.p99), r.mean,
                r.levels_per_msg > 0 ? r.mean / r.levels_per_msg : 0.0, r.allocs_per_msg);
}

AdapterConfig config(Exchange ex) {
    AdapterConfig c;
    c.name = exchangeName(ex);
    c.symbol = "BTCUSDT";
    if (ex == Exchange::OKX) c.subscribe_msg = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
    if (ex == Exchange::BYBIT) c.subscribe_msg = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";
    return c;
}

// ---------------------------------------------------------------------------
// 消息生成（模拟真实行情：BTCUSDT，0.01/0.1 跳价，顶部附近变化最多）
// ---------------------------------------------------------------------------

// Binance depth20@100ms：每条 20 + 20 档
std::vector<std::string> binanceDepth20(std::size_t n, std::mt19937_64& rng) {
    std::vector<std::string> out;
    std::int64_t mid = 6500000;
    for (std::size_t i = 0; i < n; ++i) {
        mid += static_cast<std::int64_t>(rng() % 5) - 2;
        Levels b, a;
        std::int64_t pb = mid, pa = mid + 1;
        for (int k = 0; k < 20; ++k) {
            b.push_back({fixedTicks(pb, 2), qty8(rng)});
            a.push_back({fixedTicks(pa, 2), qty8(rng)});
            pb -= 1 + static_cast<std::int64_t>(rng() % 3);
            pa += 1 + static_cast<std::int64_t>(rng() % 3);
        }
        out.push_back("{\"lastUpdateId\":" + std::to_string(70000000000ULL + i) + ",\"bids\":" + levelsJson(b) +
                      ",\"asks\":" + levelsJson(a) + "}");
    }
    return out;
}

// 交易所侧的簿：tick -> 数量字符串（买盘 tick < mid < 卖盘 tick）
struct SimBook {
    std::map<std::int64_t, std::string> bids, asks;
    std::int64_t bestBid() const { return bids.rbegin()->first; }
    std::int64_t bestAsk() const { return asks.begin()->first; }
};

// 随机改动一档：改量 / 删除 / 在顶部附近插入（保持不交叉、每侧档位数大致稳定）
template <class QtyFn>
void randomChange(SimBook& book, std::mt19937_64& rng, std::size_t target_levels, QtyFn&& qty, bool& bid,
                  std::int64_t& tick, std::string& q) {
    bid = (rng() & 1u) != 0;
    auto& side = bid ? book.bids : book.asks;
    const unsigned kind = static_cast<unsigned>(rng() % 10);
    const std::size_t depth = std::min<std::size_t>(side.size(), 30);
    auto near = [&]() {
        auto it = bid ? std::prev(side.end()) : side.begin();
        for (std::size_t s = rng() % depth; s > 0; --s) it = bid ? std::prev(it) : std::next(it);
        return it;
    };
    if (kind < 5 || (kind < 8 && side.size() <= target_levels / 2)) {
        auto it = near();
        tick = it->first;
        q = qty(rng);
        it->second = q;
    } else if (kind < 8 || side.size() > target_levels) {
        auto it = near();
        tick = it->first;
        q = "0";
        side.erase(it);
    } else {
        tick = bid ? book.bestAsk() - 1 - static_cast<std::int64_t>(rng() % 20)
                   : book.bestBid() + 1 + static_cast<std::int64_t>(rng() % 20);
        q = qty(rng);
        side[tick] = q;
    }
}

Levels sideLevels(const std::map<std::int64_t, std::string>& side, bool bid, int decimals, std::size_t max_levels,
                  std::string (*fmt)(std::int64_t, int)) {
    Levels out;
    if (bid) {
        for (auto it = side.rbegin(); it != side.rend() && out.size() < max_levels; ++it)
            out.push_back({fmt(it->first, decimals), it->second});
    } else {
        for (auto it = side.begin(); it != side.end() && out.size() < max_levels; ++it)
            out.push_back({fmt(it->first, decimals), it->second});
    }
    return out;
}

// 一条随机增量：k 次 randomChange（同一档位以最后一次为准），按价格排好分成 {买盘, 卖盘}；
// 删除的档位数量写作 zero
template <class QtyFn>
std::pair<Levels, Levels> randomDiff(SimBook& book, std::mt19937_64& rng, std::size_t target_levels, std::size_t k,
                                     QtyFn&& qty, const char* zero, std::string (*fmt)(std::int64_t, int), int decimals) {
    std::map<std::pair<bool, std::int64_t>, std::string> agg;
    for (std::size_t c = 0; c < k; ++c) {
        bool bid;
        std::int64_t tick;
        std::string q;
        randomChange(book, rng, target_levels, qty, bid, tick, q);
        agg[{bid, tick}] = q == "0" ? zero : q;
    }
    std::pair<Levels, Levels> out;
    for (const auto& kv : agg) {
        (kv.first.first ? out.first : out.second).push_back({fmt(kv.first.second, decimals), kv.second});
    }
    return out;
}

std::string okxPx(std::int64_t ticks, int) {
    if (ticks % 10 == 0) return std::to_string(ticks / 10);
    return std::to_string(ticks / 10) + "." + std::to_string(ticks % 10);
}

std::string okxChecksumOf(const SimBook& book) {
    const Levels b = sideLevels(book.bids, true, 1, 25, okxPx);
    const Levels a = sideLevels(book.asks, false, 1, 25, okxPx);
    std::string s;
    for (std::size_t i = 0; i < 25; ++i) {
        if (i < b.size()) s += (s.empty() ? "" : ":") + b[i].first + ":" + b[i].second;
        if (i < a.size()) s += (s.empty() ? "" : ":") + a[i].first + ":" + a[i].second;
    }
    return std::to_string(hft::crc32Signed(s));
}

std::string okxQty(std::mt19937_64& rng) {
    std::string s = fixedTicks(1 + static_cast<std::int64_t>(rng() % 500000000), 8);
    while (s.back() == '0') s.pop_back();
    if (s.back() == '.') s.pop_back();
    return s;
}

std::string bybitQty(std::mt19937_64& rng) { return fixedTicks(1 + static_cast<std::int64_t>(rng() % 5000000), 6); }

// ---------------------------------------------------------------------------
// 自检：失败时打印 "!!"，程序最后以非零状态退出
// ---------------------------------------------------------------------------
int g_check_failures = 0;

void check(bool ok, const std::string& what) {
    if (ok) return;
    ++g_check_failures;
    std::printf("  !! %s\n", what.c_str());
}

// 两个簿的档位逐位相同
bool sameBook(const OrderBook& x, const OrderBook& y) {
    auto eq = [](const std::vector<PriceLevel>& a, const std::vector<PriceLevel>& b) {
        if (a.size() != b.size()) return false;
        for (std::size_t i = 0; i < a.size(); ++i) {
            if (a[i].price != b[i].price || a[i].quantity != b[i].quantity) return false;
        }
        return true;
    };
    return eq(x.getBids(), y.getBids()) && eq(x.getAsks(), y.getAsks());
}

// 实测的变化构成：把变化逐档 setLevel 到参考簿上，按档位数的变化分类；同时累计每条消息之前的簿深度
struct ChangeMix {
    std::uint64_t inserts = 0, deletes = 0, qty_changes = 0, noops = 0;
    double depth_sum = 0;       // 每条消息之前的每侧平均档位数之和
    std::size_t messages = 0;

    std::uint64_t total() const { return inserts + deletes + qty_changes + noops; }
    double pct(std::uint64_t x) const {
        return total() == 0 ? 0.0 : 100.0 * static_cast<double>(x) / static_cast<double>(total());
    }
    double meanDepth() const { return messages == 0 ? 0.0 : depth_sum / static_cast<double>(messages); }
};

void applyClassified(OrderBook& book, BookSide side, const std::vector<PriceLevel>& levels, ChangeMix& mix) {
    for (const PriceLevel& l : levels) {
        const int before = side == BookSide::Bid ? book.getBidCount() : book.getAskCount();
        const bool changed = book.setLevel(side, l.price, l.quantity);
        const int after = side == BookSide::Bid ? book.getBidCount() : book.getAskCount();
        if (after > before) {
            ++mix.inserts;
        } else if (after < before) {
            ++mix.deletes;
        } else if (changed) {
            ++mix.qty_changes;
        } else {
            ++mix.noops;
        }
    }
}

// ---------------------------------------------------------------------------
// 第 4 节的工作负载：深度平稳的增量生成器
// ---------------------------------------------------------------------------
// 交易所侧的买盘有 2 × depth 个价位槽（价格 65000 - 0.25 × s），开始时偶数槽有档位，即起始簿为
// 65000 - 0.5 × i（i < depth）。每个变化以各 1/3 的概率删除一个已有档位、在一个空槽插入新档位、改一个已有档位的数量，
// 目标在整个簿上均匀抽取；档位数偏离 depth 时插入/删除改为往回拉，所以深度保持在 depth ± 1，构成仍是各约 1/3。
// 同一价格在一条消息里最多出现一次（与交易所的增量一致）。每条消息给出两种顺序：最优在前（Binance / OKX / Bybit
// 的发送顺序）与随机打乱。
struct DeepWorkload {
    OrderBook start;
    std::vector<std::vector<PriceLevel>> best_first;
    std::vector<std::vector<PriceLevel>> shuffled;
};

DeepWorkload makeDeepWorkload(std::size_t depth, std::size_t changes, std::size_t msgs, std::uint64_t seed) {
    const std::size_t slots = 2 * depth;
    const auto price = [](std::size_t s) { return 65000.0 - 0.25 * static_cast<double>(s); };
    std::mt19937_64 r(seed);
    std::vector<std::size_t> present, absent, where(slots);
    DeepWorkload w;
    w.start.reserve(slots);
    for (std::size_t s = 0; s < slots; ++s) {
        std::vector<std::size_t>& pool = s % 2 == 0 ? present : absent;
        where[s] = pool.size();
        pool.push_back(s);
        if (s % 2 == 0) w.start.updateBid(price(s), 1.0 + static_cast<double>(s / 2));
    }
    // 把槽 s 从 from 移到 to（与末尾交换后删除，O(1)）
    auto move = [&](std::vector<std::size_t>& from, std::vector<std::size_t>& to, std::size_t s) {
        const std::size_t i = where[s];
        from[i] = from.back();
        where[from[i]] = i;
        from.pop_back();
        where[s] = to.size();
        to.push_back(s);
    };
    std::vector<std::size_t> stamp(slots, SIZE_MAX);  // 槽最近一次被改动的消息序号
    w.best_first.resize(msgs);
    w.shuffled.resize(msgs);
    for (std::size_t m = 0; m < msgs; ++m) {
        std::vector<PriceLevel>& batch = w.best_first[m];
        batch.reserve(changes);
        for (std::size_t k = 0; k < changes; ++k) {
            unsigned op = static_cast<unsigned>(r() % 3);  // 0 删除，1 插入，2 改量
            if (op != 2) {
                if (present.size() > depth) op = 0;
                if (present.size() < depth) op = 1;
            }
            const std::vector<std::size_t>& pool = op == 1 ? absent : present;
            std::size_t s = 0;
            do {
                s = pool[r() % pool.size()];
            } while (stamp[s] == m);
            stamp[s] = m;
            if (op == 0) {
                move(present, absent, s);
                batch.push_back({price(s), 0.0});
            } else {
                if (op == 1) move(absent, present, s);
                batch.push_back({price(s), 1.0 + static_cast<double>(r() % 100) / 10.0});
            }
        }
        std::vector<PriceLevel>& shuffled = w.shuffled[m];
        shuffled = batch;
        for (std::size_t i = shuffled.size(); i > 1; --i) std::swap(shuffled[i - 1], shuffled[r() % i]);  // Fisher-Yates
        std::sort(batch.begin(), batch.end(), [](const PriceLevel& a, const PriceLevel& b) { return a.price > b.price; });
    }
    return w;
}

// ---------------------------------------------------------------------------
// 第 1 节：逐条计时的统计（精确分位数：样本排序后按最近秩取值，单位 µs）
// ---------------------------------------------------------------------------
struct SampleStats {
    std::size_t n = 0;
    double p50 = 0, p90 = 0, p99 = 0, mean = 0;
    double above_100us = 0;  // 超过 100 µs 的消息比例（0..1）
};

SampleStats summarize(std::vector<double> ns) {
    SampleStats s;
    s.n = ns.size();
    if (ns.empty()) return s;
    std::sort(ns.begin(), ns.end());
    const auto rank = [&](double q) {
        const double r = std::ceil(q * static_cast<double>(ns.size()));
        const std::size_t i = std::clamp<std::size_t>(static_cast<std::size_t>(r), 1, ns.size());
        return ns[i - 1] / 1000.0;
    };
    s.p50 = rank(0.50);
    s.p90 = rank(0.90);
    s.p99 = rank(0.99);
    double sum = 0;
    std::size_t above = 0;
    for (const double x : ns) {
        sum += x;
        if (x > 100000.0) ++above;
    }
    s.mean = sum / static_cast<double>(ns.size()) / 1000.0;
    s.above_100us = static_cast<double>(above) / static_cast<double>(ns.size());
    return s;
}

double ticksToNs(std::uint64_t t0, std::uint64_t t1) {
    return static_cast<double>(std::max<std::int64_t>(0, hft::TscClock::instance().toNs(t1 - t0)));
}

// 冷缓存模式：每条消息之前读改写一遍大于 LLC 的缓冲区，把被测代码的数据挤出各级缓存
constexpr std::size_t kEvictBytes = std::size_t{64} << 20;

void evictCaches(std::vector<unsigned char>& buf) {
    if (buf.size() < kEvictBytes) buf.assign(kEvictBytes, 1);
    unsigned sum = 0;
    for (std::size_t i = 0; i < buf.size(); i += 64) {
        buf[i] = static_cast<unsigned char>(buf[i] + 1);
        sum += buf[i];
    }
    doNotOptimize(sum);
}

// ---------------------------------------------------------------------------
// 报文里的买卖档位（预解析；数值与适配器完全相同：同一个 adapter_detail::parseLevels）
// ---------------------------------------------------------------------------
struct ParsedLevels {
    std::vector<PriceLevel> bids, asks;
};

// 取出 Binance "b"/"a" 或 "bids"/"asks"、OKX data[].bids/asks、Bybit data.b/a，不做协议检查
bool collectLevels(adapter_detail::od::object obj, std::vector<adapter_detail::RawLevel>& bids,
                   std::vector<adapter_detail::RawLevel>& asks, bool top) {
    namespace od = adapter_detail::od;
    for (auto field_result : obj) {
        od::field field;
        if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
        const std::string_view key = field.escaped_key();
        od::value& v = field.value();
        if (key == "b" || key == "bids") {
            if (!adapter_detail::parseLevels(v, bids)) return false;
        } else if (key == "a" || key == "asks") {
            if (!adapter_detail::parseLevels(v, asks)) return false;
        } else if (key == "data" && top) {
            od::json_type type;
            if (v.type().get(type) != simdjson::SUCCESS) return false;
            if (type == od::json_type::array) {
                od::array arr;
                if (v.get_array().get(arr) != simdjson::SUCCESS) return false;
                for (auto element : arr) {
                    od::object inner;
                    if (element.get_object().get(inner) != simdjson::SUCCESS) return false;
                    if (!collectLevels(inner, bids, asks, false)) return false;
                }
            } else {
                od::object inner;
                if (v.get_object().get(inner) != simdjson::SUCCESS) return false;
                if (!collectLevels(inner, bids, asks, false)) return false;
            }
        }
    }
    return true;
}

std::vector<ParsedLevels> parseMessages(const std::vector<std::string>& msgs) {
    adapter_detail::JsonReader reader;
    std::vector<adapter_detail::RawLevel> b, a;
    std::vector<ParsedLevels> out;
    out.reserve(msgs.size());
    for (const std::string& m : msgs) {
        b.clear();
        a.clear();
        adapter_detail::od::document doc;
        adapter_detail::od::object obj;
        const bool ok = reader.load(m, doc) == simdjson::SUCCESS && doc.get_object().get(obj) == simdjson::SUCCESS &&
                        collectLevels(obj, b, a, true);
        check(ok, "cannot extract the levels of a benchmark message");
        ParsedLevels p;
        for (const adapter_detail::RawLevel& l : b) p.bids.push_back({l.price, l.quantity});
        for (const adapter_detail::RawLevel& l : a) p.asks.push_back({l.price, l.quantity});
        out.push_back(std::move(p));
    }
    return out;
}

double averageLevels(const std::vector<ParsedLevels>& msgs, std::size_t begin, std::size_t end) {
    double total = 0;
    for (std::size_t i = begin; i < end; ++i) total += static_cast<double>(msgs[i].bids.size() + msgs[i].asks.size());
    return end > begin ? total / static_cast<double>(end - begin) : 0.0;
}

// 与适配器相同：每侧超过 kMaxBookLevels 时裁掉远端档位
void trimLikeAdapters(OrderBook& book) { book.truncate(adapter_detail::kMaxBookLevels); }

// ---------------------------------------------------------------------------
// 模拟交易所的行情流：与离线演示中监控程序收到的报文以同样的方式生成
// （tools/mock_market.h 的市场仿真 + tools/mock_publishers.h 的发布状态机，参数同 tools/mock_exchange.h 的默认值）
// ---------------------------------------------------------------------------
namespace sim = mock_exchange::sim;
namespace pub = mock_exchange::pub;

constexpr std::uint64_t kMockSeed = 1;     // tools/run_mock_demo.sh 默认的 --seed
constexpr double kMockWarmupS = 5.0;       // MockExchangeOptions::warmup_s 的默认值
// 推送周期（毫秒）：同 tools/mock_exchange.h 的 kBinanceFastMs、kOkxBooksMs、kOkxBboMs 与 kBybitPeriodsMs（50 档）
constexpr int kMockBinanceMs = 100;
constexpr int kMockOkxBooksMs = 100;
constexpr int kMockOkxBboMs = 10;
constexpr int kMockBybit50Ms = 20;
constexpr int kMockOkxSnapshotMs = 500;    // 每 0.5 s 取一个 books 快照（新订阅时收到的那种），各不相同
constexpr std::size_t kMockBybitCapture = 200;  // mock_exchange 取 Bybit 最深的 200 档窗口，各深度的发布者再取前 N 档

struct MockStreams {
    std::string binance_rest;                  // GET /api/v3/depth?limit=1000，在第一条增量之后取得
    std::vector<std::string> binance_diffs;    // <symbol>@depth@100ms（第 0 条被 REST 快照覆盖）
    std::vector<std::string> binance_depth20;  // <symbol>@depth20@100ms
    std::string okx_snapshot;                  // books 订阅时的快照
    std::vector<std::string> okx_updates;      // books 增量（带 checksum）
    std::vector<std::string> okx_snapshots;    // 不同时刻的 books 快照
    std::vector<std::string> okx_books5;       // books5 全量推送
    std::vector<std::string> okx_bbo;          // bbo-tbt 全量推送
    std::string bybit_snapshot;                // orderbook.50 订阅时的快照
    std::vector<std::string> bybit_deltas;     // orderbook.50 delta
};

MockStreams generateMockStreams(double seconds) {
    using sim::Side;
    MockStreams s;
    sim::Market market(sim::defaultMarketParams(), kMockSeed);
    market.advance(kMockWarmupS);
    const sim::Venue& vb = market.venue(0);
    const sim::Venue& vo = market.venue(1);
    const sim::Venue& vy = market.venue(2);
    pub::BinanceDepthPublisher binance;  // 1000 档窗口
    pub::OkxBooksPublisher books;        // 400 档窗口
    pub::OkxTopPublisher books5("books5", 5);
    pub::OkxTopPublisher bbo("bbo-tbt", 1);
    pub::BybitBookPublisher bybit(50);
    const std::int64_t t0_ms = 1700000000000;
    // 预热结束时以当前深度初始化全部发布者（同 MockExchange::resetHubs）
    binance.reset(vb.depth(Side::Buy, binance.window()), vb.depth(Side::Sell, binance.window()), vb.bookEvents());
    {
        const pub::Levels b = vo.depth(Side::Buy, books.window());
        const pub::Levels a = vo.depth(Side::Sell, books.window());
        const auto seq = static_cast<std::int64_t>(vo.bookEvents());
        books.reset(b, a, seq, t0_ms);
        books5.reset(b, a, seq);
        bbo.reset(b, a, seq);
    }
    bybit.reset(vy.depth(Side::Buy, kMockBybitCapture), vy.depth(Side::Sell, kMockBybitCapture), vy.bookEvents());
    s.okx_snapshot = books.snapshot(t0_ms);
    s.bybit_snapshot = bybit.snapshot(t0_ms, t0_ms);
    const double step_ms = market.stepSeconds() * 1000.0;
    const auto every = [&](int period_ms) {
        return static_cast<std::uint64_t>(std::max(1.0, std::round(static_cast<double>(period_ms) / step_ms)));
    };
    const std::uint64_t binance_every = every(kMockBinanceMs), books_every = every(kMockOkxBooksMs),
                        bbo_every = every(kMockOkxBboMs), bybit_every = every(kMockBybit50Ms),
                        snapshot_every = every(kMockOkxSnapshotMs);
    const auto steps = static_cast<std::uint64_t>(std::llround(seconds * 1000.0 / step_ms));
    std::string msg;
    for (std::uint64_t k = 1; k <= steps; ++k) {
        market.step();
        const std::int64_t now = t0_ms + std::llround(static_cast<double>(k) * step_ms);
        if (k % binance_every == 0) {
            if (binance.publish(vb.depth(Side::Buy, binance.window()), vb.depth(Side::Sell, binance.window()),
                                vb.bookEvents(), now, msg)) {
                s.binance_diffs.push_back(msg);
                // 客户端缓冲第一条增量后拉取 REST 快照（limit 同 tools/mock_config.json）
                if (s.binance_diffs.size() == 1) s.binance_rest = binance.restSnapshot(1000);
            }
            s.binance_depth20.push_back(binance.partialDepth(20));
        }
        const bool books_due = k % books_every == 0;
        const bool bbo_due = k % bbo_every == 0;
        if (books_due || bbo_due) {
            const pub::Levels b = vo.depth(Side::Buy, books.window());
            const pub::Levels a = vo.depth(Side::Sell, books.window());
            const auto seq = static_cast<std::int64_t>(vo.bookEvents());
            if (books_due) {
                if (books.publish(b, a, seq, now, msg)) s.okx_updates.push_back(msg);
                if (books5.publish(b, a, seq, now, msg)) s.okx_books5.push_back(msg);
            }
            if (bbo_due && bbo.publish(b, a, seq, now, msg)) s.okx_bbo.push_back(msg);
        }
        if (k % snapshot_every == 0) s.okx_snapshots.push_back(books.snapshot(now));
        if (k % bybit_every == 0 && bybit.publish(vy.depth(Side::Buy, kMockBybitCapture),
                                                  vy.depth(Side::Sell, kMockBybitCapture), vy.bookEvents(), now, now,
                                                  msg)) {
            s.bybit_deltas.push_back(msg);
        }
    }
    return s;
}

// ---------------------------------------------------------------------------
// 第 1 节的一行：一段报文经真实适配器（onMessage）逐条计时
// ---------------------------------------------------------------------------
struct StreamRow {
    std::string name;
    SampleStats stats;
    double bytes = 0;    // 平均每条字节数
    double levels = 0;   // 平均每条档位数（两侧合计）
    double allocs = 0;   // 平均每条堆分配次数
};

// msgs[begin, end) 逐条计时；evict 非空时每条之前冲掉缓存（冷缓存模式）
StreamRow timeAdapter(ExchangeAdapter& a, const std::vector<std::string>& msgs, std::size_t begin, std::size_t end,
                      std::vector<unsigned char>* evict) {
    StreamRow row;
    std::vector<double> ns;
    ns.reserve(end - begin);
    if (evict != nullptr) evictCaches(*evict);  // 先分配好缓冲区
    AdapterResult out;
    double bytes = 0;
    std::uint64_t allocs = 0;
    for (std::size_t i = begin; i < end; ++i) {
        if (evict != nullptr) evictCaches(*evict);
        const std::uint64_t a0 = g_allocs;
        const std::uint64_t t0 = hft::rdtsc();
        out.reset();
        a.onMessage(msgs[i], out);
        const std::uint64_t t1 = hft::rdtscp();
        allocs += g_allocs - a0;
        ns.push_back(ticksToNs(t0, t1));
        bytes += static_cast<double>(msgs[i].size());
    }
    const double n = static_cast<double>(std::max<std::size_t>(1, end - begin));
    row.stats = summarize(std::move(ns));
    row.bytes = bytes / n;
    row.allocs = static_cast<double>(allocs) / n;
    return row;
}

// 适配器统计里不该出现的问题（缺口、重同步、校验和失败、格式错误、未同步）；空串 = 正常
std::string adapterProblems(const ExchangeAdapter& a) {
    const AdapterStats& s = a.stats();
    std::string why;
    if (!a.synced()) why += " not synced;";
    if (s.parse_errors != 0) why += " parse_errors=" + std::to_string(s.parse_errors) + ";";
    if (s.gaps != 0) why += " gaps=" + std::to_string(s.gaps) + ";";
    if (s.resyncs != 0) why += " resyncs=" + std::to_string(s.resyncs) + ";";
    if (s.checksum_failures != 0) why += " checksum_failures=" + std::to_string(s.checksum_failures) + ";";
    if (a.exchange() == Exchange::OKX && !static_cast<const OkxAdapter&>(a).checksumEnabled()) {
        why += " checksum verification disabled;";
    }
    return why;
}

void printStreamHeader() {
    std::printf("  %-48s %6s %7s %7s %8s %8s %8s %8s %7s %8s\n", "scenario", "msgs", "bytes", "levels", "p50 us",
                "p90 us", "p99 us", "mean us", ">100us", "allocs/m");
}

void printStreamRow(const StreamRow& r) {
    std::printf("  %-48s %6zu %7.0f %7.1f %8.2f %8.2f %8.2f %8.2f %6.1f%% %8.2f\n", r.name.c_str(), r.stats.n, r.bytes,
                r.levels, r.stats.p50, r.stats.p90, r.stats.p99, r.stats.mean, 100.0 * r.stats.above_100us, r.allocs);
}

// ---------------------------------------------------------------------------
// 第 1b 节：同一批增量只看簿的应用——逐档 setLevel（0ec0622 之前的适配器）vs applyLevels，A/B 交错
// ---------------------------------------------------------------------------
struct ApplyComparison {
    SampleStats per_level, batched;
    ChangeMix mix;
    double levels = 0;                 // 平均每条档位数
    std::size_t batched_sides = 0;     // 超过 kBatchThreshold 档、走批量路径的一侧
    std::size_t best_first_sides = 0;  // 其中已严格按最优在前排好的
    bool same = false;
};

bool strictlyBestFirst(BookSide side, const std::vector<PriceLevel>& v) {
    for (std::size_t i = 1; i < v.size(); ++i) {
        const bool ordered = side == BookSide::Bid ? v[i - 1].price > v[i].price : v[i - 1].price < v[i].price;
        if (!ordered) return false;
    }
    return true;
}

// start：起始快照；msgs[first, ...)：依次应用的增量，前 warm 条不计时
ApplyComparison compareApply(const ParsedLevels& start, const std::vector<ParsedLevels>& msgs, std::size_t first,
                             std::size_t warm) {
    ApplyComparison c;
    OrderBook per_level, batched, ref;  // ref：不计时的参考簿，用于分类变化与核对
    for (OrderBook* b : {&per_level, &batched, &ref}) {
        b->reserve(2048);
        for (const PriceLevel& l : start.bids) b->setLevel(BookSide::Bid, l.price, l.quantity);
        for (const PriceLevel& l : start.asks) b->setLevel(BookSide::Ask, l.price, l.quantity);
        trimLikeAdapters(*b);
    }
    LevelBatchScratch scratch;
    std::vector<double> t_per_level, t_batched;
    t_per_level.reserve(msgs.size());
    t_batched.reserve(msgs.size());
    double levels = 0;
    for (std::size_t i = first; i < msgs.size(); ++i) {
        const ParsedLevels& m = msgs[i];
        const bool timed = i >= first + warm;
        if (timed) {
            c.mix.depth_sum += static_cast<double>(ref.getBidCount() + ref.getAskCount()) / 2.0;
            ++c.mix.messages;
            levels += static_cast<double>(m.bids.size() + m.asks.size());
            for (BookSide side : {BookSide::Bid, BookSide::Ask}) {
                const std::vector<PriceLevel>& v = side == BookSide::Bid ? m.bids : m.asks;
                if (v.size() <= OrderBook::kBatchThreshold) continue;
                ++c.batched_sides;
                if (strictlyBestFirst(side, v)) ++c.best_first_sides;
            }
        }
        ChangeMix scratch_mix;
        applyClassified(ref, BookSide::Bid, m.bids, timed ? c.mix : scratch_mix);
        applyClassified(ref, BookSide::Ask, m.asks, timed ? c.mix : scratch_mix);
        trimLikeAdapters(ref);
        const auto runPerLevel = [&] {
            const std::uint64_t t0 = hft::rdtsc();
            for (const PriceLevel& l : m.bids) per_level.setLevel(BookSide::Bid, l.price, l.quantity);
            for (const PriceLevel& l : m.asks) per_level.setLevel(BookSide::Ask, l.price, l.quantity);
            trimLikeAdapters(per_level);
            return ticksToNs(t0, hft::rdtscp());
        };
        const auto runBatched = [&] {
            const std::uint64_t t0 = hft::rdtsc();
            batched.applyLevels(BookSide::Bid, m.bids.data(), m.bids.size(), scratch);
            batched.applyLevels(BookSide::Ask, m.asks.data(), m.asks.size(), scratch);
            trimLikeAdapters(batched);
            return ticksToNs(t0, hft::rdtscp());
        };
        double a = 0, b = 0;
        if ((i & 1) == 0) {
            a = runPerLevel();
            b = runBatched();
        } else {
            b = runBatched();
            a = runPerLevel();
        }
        if (timed) {
            t_per_level.push_back(a);
            t_batched.push_back(b);
        }
    }
    c.levels = c.mix.messages == 0 ? 0.0 : levels / static_cast<double>(c.mix.messages);
    c.per_level = summarize(std::move(t_per_level));
    c.batched = summarize(std::move(t_batched));
    c.same = sameBook(per_level, batched) && sameBook(per_level, ref);
    return c;
}

}  // namespace

// ---------------------------------------------------------------------------
// 升级前的处理流程（原样复制自旧 market_data_feed.h，作为对照）
// ---------------------------------------------------------------------------
namespace legacy {

double fastParseDouble(std::string_view sv) {
    double val = 0.0;
    std::from_chars(sv.data(), sv.data() + sv.size(), val);
    return val;
}

bool parseBinanceDepth(simdjson::ondemand::document& doc, OrderBook& orderbook_, std::mutex& mutex_) {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        orderbook_.clearBids();
        orderbook_.clearAsks();

        auto bids = doc["bids"].get_array();
        for (auto bid : bids) {
            auto arr = bid.get_array();
            auto iter = arr.begin();
            std::string_view price_str = (*iter).get_string();
            ++iter;
            std::string_view qty_str = (*iter).get_string();
            double price = fastParseDouble(price_str);
            double qty = fastParseDouble(qty_str);
            if (qty > 0.0) orderbook_.updateBid(price, qty);
        }

        auto asks = doc["asks"].get_array();
        for (auto ask : asks) {
            auto arr = ask.get_array();
            auto iter = arr.begin();
            std::string_view price_str = (*iter).get_string();
            ++iter;
            std::string_view qty_str = (*iter).get_string();
            double price = fastParseDouble(price_str);
            double qty = fastParseDouble(qty_str);
            if (qty > 0.0) orderbook_.updateAsk(price, qty);
        }
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace legacy

int main(int argc, char** argv) {
    bool quick = false;
    bool cold = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--quick") == 0) {
            quick = true;
        } else if (std::strcmp(argv[i], "--cold") == 0) {
            cold = true;
        } else {
            std::fprintf(stderr, "usage: %s [--quick] [--cold]\n", argv[0]);
            return 2;
        }
    }
    const std::size_t n = quick ? 2000 : 20000;
    std::mt19937_64 rng(20261104);
    hft::TscClock::instance();  // 先校准

    std::printf("Market data benchmark (real headers from include/)\n");
    // stage 1（结构索引）在运行时按 CPU 选择内核；On-Demand 的内联迭代代码使用编译期的 builtin 内核
    std::printf("  simdjson %s: stage-1 kernel %s (runtime dispatch), On-Demand inline kernel %s (compile flags)\n",
                SIMDJSON_VERSION, simdjson::get_active_implementation()->name().c_str(),
                simdjson::builtin_implementation()->name().c_str());
    std::printf("  TSC: %.3f ns/tick, %zu messages per synthetic scenario\n", hft::TscClock::instance().nsPerTick(), n);

    // ======================= 1. 各适配器每条消息耗时 =======================
    // 1a / 1b：模拟交易所的行情流（与离线演示中监控程序收到的报文相同），每个流的前 20% 只预热、不计时
    const double mock_seconds = quick ? 30.0 : 180.0;
    const MockStreams ms = generateMockStreams(mock_seconds);
    const std::vector<ParsedLevels> p_binance_diffs = parseMessages(ms.binance_diffs);
    const std::vector<ParsedLevels> p_binance_depth20 = parseMessages(ms.binance_depth20);
    const std::vector<ParsedLevels> p_okx_updates = parseMessages(ms.okx_updates);
    const std::vector<ParsedLevels> p_okx_snapshots = parseMessages(ms.okx_snapshots);
    const std::vector<ParsedLevels> p_okx_books5 = parseMessages(ms.okx_books5);
    const std::vector<ParsedLevels> p_okx_bbo = parseMessages(ms.okx_bbo);
    const std::vector<ParsedLevels> p_bybit_deltas = parseMessages(ms.bybit_deltas);

    struct MockCase {
        const char* name;
        Exchange ex;
        const char* subscribe;                          // nullptr = config() 的默认订阅
        const std::vector<std::string>* msgs;
        const std::vector<ParsedLevels>* parsed;
        std::size_t first;                              // 第一条要处理的报文（之前的已被 prime 覆盖）
        std::function<void(ExchangeAdapter&)> prime;    // 进入已同步状态：订阅快照 / REST 快照
    };
    const auto feedOne = [](ExchangeAdapter& a, const std::string& m) {
        AdapterResult r;
        a.onMessage(m, r);
    };
    const char* const kOkxBooks5Sub = R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})";
    const char* const kOkxBboSub = R"({"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT"}]})";
    const std::vector<MockCase> mock_cases = {
        {"Binance depth20@100ms (20+20 snapshot)", Exchange::BINANCE, nullptr, &ms.binance_depth20,
         &p_binance_depth20, 0, nullptr},
        {"Binance diff@100ms (1000-level window)", Exchange::BINANCE, nullptr, &ms.binance_diffs, &p_binance_diffs, 1,
         [&](ExchangeAdapter& a) {
             feedOne(a, ms.binance_diffs[0]);  // 缓冲并请求快照；REST 快照覆盖这一条（u <= lastUpdateId）
             AdapterResult r;
             a.onRestSnapshot(ms.binance_rest, r);
         }},
        {"OKX books update (400 window, checksum)", Exchange::OKX, nullptr, &ms.okx_updates, &p_okx_updates, 0,
         [&](ExchangeAdapter& a) { feedOne(a, ms.okx_snapshot); }},
        {"OKX books snapshot (400+400, checksum)", Exchange::OKX, nullptr, &ms.okx_snapshots, &p_okx_snapshots, 0, nullptr},
        {"OKX books5 push (5+5, no checksum)", Exchange::OKX, kOkxBooks5Sub, &ms.okx_books5, &p_okx_books5, 0, nullptr},
        {"OKX bbo-tbt push (1+1, no checksum)", Exchange::OKX, kOkxBboSub, &ms.okx_bbo, &p_okx_bbo, 0, nullptr},
        {"Bybit orderbook.50 delta", Exchange::BYBIT, nullptr, &ms.bybit_deltas, &p_bybit_deltas, 0,
         [&](ExchangeAdapter& a) { feedOne(a, ms.bybit_snapshot); }},
    };
    // 一条比流中所有报文都大、会被适配器忽略的消息：先把 simdjson parser 与报文缓冲区撑到稳态容量
    // （否则每遇到一条"史上最大"的报文都会扩容一次，allocs/m 量到的是预热而不是稳态）
    const auto growBuffers = [&](ExchangeAdapter& a, const std::vector<std::string>& msgs) {
        std::size_t largest = 0;
        for (const std::string& m : msgs) largest = std::max(largest, m.size());
        const std::string pad(largest + 1024, 'x');
        switch (a.exchange()) {
            case Exchange::BINANCE: feedOne(a, R"({"result":null,"id":1,"pad":")" + pad + "\"}"); break;
            case Exchange::OKX: feedOne(a, R"({"event":"subscribe","pad":")" + pad + "\"}"); break;
            case Exchange::BYBIT:
                feedOne(a, R"({"success":true,"ret_msg":"subscribe","op":"subscribe","pad":")" + pad + "\"}");
                break;
        }
    };
    // 一个流经真实适配器：prime 同步，再预热前 20%（不计时），其余（最多 max_timed 条）逐条计时
    const auto runMock = [&](const MockCase& mc, std::vector<unsigned char>* evict, std::size_t max_timed) {
        AdapterConfig cfg = config(mc.ex);
        if (mc.subscribe != nullptr) cfg.subscribe_msg = mc.subscribe;
        auto a = makeAdapter(mc.ex, cfg);
        a->onConnected();
        growBuffers(*a, *mc.msgs);
        if (mc.prime) mc.prime(*a);
        const std::vector<std::string>& msgs = *mc.msgs;
        check(msgs.size() > mc.first, std::string(mc.name) + ": the mock stream produced no messages");
        const std::size_t first = std::min(mc.first, msgs.size());
        const std::size_t warm_end = first + (msgs.size() - first) / 5;
        for (std::size_t i = first; i < warm_end; ++i) feedOne(*a, msgs[i]);
        const std::size_t end = warm_end + std::min(max_timed, msgs.size() - warm_end);
        StreamRow row = timeAdapter(*a, msgs, warm_end, end, evict);
        row.name = std::string(evict != nullptr ? "cold: " : "") + mc.name;
        row.levels = averageLevels(*mc.parsed, warm_end, end);
        const std::string problems = adapterProblems(*a);
        check(problems.empty(), row.name + ":" + problems);
        return row;
    };

    std::printf("\n1. Per-message processing through the real adapters (adapter.onMessage, TSC-timed per message)\n");
    std::printf("  1a. Mock-exchange streams = what the monitor receives in the offline demo (tools/mock_market.h driving\n"
                "      tools/mock_publishers.h; seed %llu, %.0f s warm-up, %.0f s of market at the mock exchange's cadence:\n"
                "      Binance and OKX every 100 ms, Bybit 50 every 20 ms, bbo-tbt every 10 ms). Warm caches, back to back;\n"
                "      the first 20%% of each stream is applied untimed. These are the per-message costs to quote.\n"
                "      allocs/m counts reusable buffers growing for a message larger than any before it (warm-up).\n",
                static_cast<unsigned long long>(kMockSeed), kMockWarmupS, mock_seconds);
    printStreamHeader();
    for (const MockCase& mc : mock_cases) printStreamRow(runMock(mc, nullptr, SIZE_MAX));

    // 1b：只看簿的应用。setLevel 一栏是 0ec0622 之前适配器应用增量的方式，applyLevels 是现在的方式
    std::printf("\n  1b. Book apply only, the same mock diffs pre-parsed: per-level setLevel (how the adapters applied a diff\n"
                "      before 0ec0622) vs OrderBook::applyLevels (now), A/B interleaved per message, books must end\n"
                "      identical (us/msg). depth = mean levels per side; mix measured; sorted = share of the sides with more\n"
                "      than %zu levels that arrive strictly best-first.\n",
                OrderBook::kBatchThreshold);
    std::printf("  %-28s %6s %7s %6s %19s | %31s | %31s | %7s %6s %4s\n", "stream", "msgs", "levels", "depth",
                "ins/del/qty/noop %", "setLevel p50 / p90 / p99 / mean", "applyLevels p50 / p90 / p99 / mean", "speedup",
                "sorted", "same");
    {
        struct ApplyCase {
            const char* name;
            const std::string* snapshot;
            const std::vector<ParsedLevels>* msgs;
            std::size_t first;
        };
        const ApplyCase apply_cases[] = {
            {"Binance diff (1000 window)", &ms.binance_rest, &p_binance_diffs, 1},
            {"OKX books update (400 window)", &ms.okx_snapshot, &p_okx_updates, 0},
            {"Bybit orderbook.50 delta", &ms.bybit_snapshot, &p_bybit_deltas, 0},
        };
        for (const ApplyCase& ac : apply_cases) {
            const std::vector<ParsedLevels> start = parseMessages({*ac.snapshot});
            check(ac.msgs->size() > ac.first, std::string(ac.name) + ": the mock stream produced no messages");
            const std::size_t warm = (ac.msgs->size() - std::min(ac.first, ac.msgs->size())) / 5;
            const ApplyComparison c = compareApply(start[0], *ac.msgs, ac.first, warm);
            char shares[48];
            std::snprintf(shares, sizeof shares, "%.1f/%.1f/%.1f/%.1f", c.mix.pct(c.mix.inserts), c.mix.pct(c.mix.deletes),
                          c.mix.pct(c.mix.qty_changes), c.mix.pct(c.mix.noops));
            const double sorted = c.batched_sides == 0 ? 0.0
                                                       : 100.0 * static_cast<double>(c.best_first_sides) /
                                                             static_cast<double>(c.batched_sides);
            std::printf("  %-28s %6zu %7.1f %6.0f %19s | %7.2f %7.2f %7.2f %7.2f | %7.2f %7.2f %7.2f %7.2f | %6.1fx %5.0f%% %4s\n",
                        ac.name, c.per_level.n, c.levels, c.mix.meanDepth(), shares, c.per_level.p50, c.per_level.p90,
                        c.per_level.p99, c.per_level.mean, c.batched.p50, c.batched.p90, c.batched.p99, c.batched.mean,
                        c.per_level.mean / std::max(1e-9, c.batched.mean), sorted, c.same ? "yes" : "NO");
            check(c.same, std::string(ac.name) + ": setLevel and applyLevels produced different books");
        }
    }

    if (cold) {
        std::printf("\n  1a, cold caches: the same streams with %zu MiB (more than a typical LLC) walked before every message;\n"
                    "      real feeds deliver a message every 10-100 ms, so processing usually starts cold. Up to 300 per stream.\n",
                    kEvictBytes >> 20);
        printStreamHeader();
        std::vector<unsigned char> evict;
        for (const MockCase& mc : mock_cases) printStreamRow(runMock(mc, &evict, 300));
    }

    // 1c：旧的第 1 节，保留作微基准
    std::printf("\n  1c. Synthetic small diffs (the former section 1; micro-benchmarks, NOT per-message costs of real streams):\n"
                "      changes only touch the best 30 levels and are in-place quantity changes once warmed up, so they never\n"
                "      reach the insert/erase path that dominates real diffs. Book depth and qty share are measured.\n");
    printStreamHeader();
    // 一行微基准：计时后把同样的报文逐档重放到计时前的簿上，得到实测的簿深度与变化构成（写进行名）
    const auto microRow = [&](const char* what, ExchangeAdapter& a, const std::vector<std::string>& msgs) {
        const OrderBook before = a.book();
        StreamRow row = timeAdapter(a, msgs, 0, msgs.size(), nullptr);
        const std::vector<ParsedLevels> parsed = parseMessages(msgs);
        ChangeMix mix;
        OrderBook ref = before;
        for (const ParsedLevels& p : parsed) {
            applyClassified(ref, BookSide::Bid, p.bids, mix);
            applyClassified(ref, BookSide::Ask, p.asks, mix);
            trimLikeAdapters(ref);
        }
        row.levels = averageLevels(parsed, 0, parsed.size());
        char label[96];
        std::snprintf(label, sizeof label, "micro: %s (%d+%d book, %.0f%% qty)", what, before.getBidCount(),
                      before.getAskCount(), mix.pct(mix.qty_changes));
        row.name = label;
        printStreamRow(row);
        check(sameBook(ref, a.book()), row.name + ": the replay does not match the adapter's book");
        const std::string problems = adapterProblems(a);
        check(problems.empty(), row.name + ":" + problems);
    };

    // Binance diff depth（已与 1000 档 REST 快照同步）
    {
        SimBook book;
        const std::int64_t mid = 6500000;
        for (std::int64_t t = 1; t <= 1000; ++t) {
            book.bids[mid - t] = qty8(rng);
            book.asks[mid + t] = qty8(rng);
        }
        std::uint64_t id = 50000000000ULL;
        auto snapshotNow = [&]() {
            return "{\"lastUpdateId\":" + std::to_string(id) +
                   ",\"bids\":" + levelsJson(sideLevels(book.bids, true, 2, 1000, fixedTicks)) +
                   ",\"asks\":" + levelsJson(sideLevels(book.asks, false, 2, 1000, fixedTicks)) + "}";
        };
        auto makeEvents = [&](std::size_t count) {
            std::vector<std::string> out;
            for (std::size_t i = 0; i < count; ++i) {
                const std::size_t k = 10 + rng() % 30;
                const std::uint64_t first = id + 1;
                const auto [b, a] = randomDiff(book, rng, 1000, k, qty8, "0.00000000", fixedTicks, 2);
                id += k;
                out.push_back("{\"e\":\"depthUpdate\",\"E\":1700000000000,\"s\":\"BTCUSDT\",\"U\":" + std::to_string(first) +
                              ",\"u\":" + std::to_string(id) + ",\"b\":" + levelsJson(b) + ",\"a\":" + levelsJson(a) + "}");
            }
            return out;
        };
        auto a = makeAdapter(Exchange::BINANCE, config(Exchange::BINANCE));
        a->onConnected();
        AdapterResult out;
        const auto first = makeEvents(1);  // 第一条触发缓冲 + 快照请求
        a->onMessage(first[0], out);
        out.reset();
        a->onRestSnapshot(snapshotNow(), out);  // 快照在第一条事件之后取得：L >= U，第一条被丢弃
        const auto warm = makeEvents(2000);
        const auto msgs = makeEvents(n);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        microRow("Binance diff", *a, msgs);
    }

    // OKX books 增量（含校验和）
    {
        SimBook book;
        const std::int64_t mid = 300000;
        for (std::int64_t t = 1; t <= 400; ++t) {
            book.bids[mid - t] = okxQty(rng);
            book.asks[mid + t] = okxQty(rng);
        }
        std::int64_t seq = 1000;
        auto snapshot = [&]() {
            ++seq;
            return "{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":" +
                   levelsJson(sideLevels(book.asks, false, 1, 400, okxPx), true) +
                   ",\"bids\":" + levelsJson(sideLevels(book.bids, true, 1, 400, okxPx), true) +
                   ",\"ts\":\"1597026383085\",\"checksum\":" + okxChecksumOf(book) +
                   ",\"prevSeqId\":-1,\"seqId\":" + std::to_string(seq) + "}]}";
        };
        auto makeUpdates = [&](std::size_t count) {
            std::vector<std::string> v;
            for (std::size_t i = 0; i < count; ++i) {
                const auto [b, as] = randomDiff(book, rng, 400, 2 + rng() % 12, okxQty, "0", okxPx, 1);
                const std::int64_t prev = seq++;
                v.push_back("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"update\",\"data\":[{\"asks\":" +
                            levelsJson(as, true) + ",\"bids\":" + levelsJson(b, true) +
                            ",\"ts\":\"1597026383085\",\"checksum\":" + okxChecksumOf(book) +
                            ",\"prevSeqId\":" + std::to_string(prev) + ",\"seqId\":" + std::to_string(seq) + "}]}");
            }
            return v;
        };
        auto a = makeAdapter(Exchange::OKX, config(Exchange::OKX));
        a->onConnected();
        AdapterResult out;
        a->onMessage(snapshot(), out);
        const auto warm = makeUpdates(2000);
        const auto msgs = makeUpdates(n);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        microRow("OKX books update", *a, msgs);
    }

    // Bybit orderbook.50 delta
    {
        SimBook book;
        const std::int64_t mid = 3024800;
        for (std::int64_t t = 1; t <= 50; ++t) {
            book.bids[mid - t] = bybitQty(rng);
            book.asks[mid + t] = bybitQty(rng);
        }
        std::uint64_t u = 177400000;
        auto msg = [&](const char* type, const Levels& b, const Levels& as) {
            return std::string("{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"") + type +
                   "\",\"ts\":1687940967466,\"data\":{\"s\":\"BTCUSDT\",\"b\":" + levelsJson(b) + ",\"a\":" + levelsJson(as) +
                   ",\"u\":" + std::to_string(u) + ",\"seq\":66544703342},\"cts\":1687940967464}";
        };
        auto a = makeAdapter(Exchange::BYBIT, config(Exchange::BYBIT));
        a->onConnected();
        AdapterResult out;
        a->onMessage(msg("snapshot", sideLevels(book.bids, true, 2, 50, fixedTicks), sideLevels(book.asks, false, 2, 50, fixedTicks)),
                     out);
        auto makeDeltas = [&](std::size_t count) {
            std::vector<std::string> v;
            for (std::size_t i = 0; i < count; ++i) {
                const auto [b, as] = randomDiff(book, rng, 50, 3 + rng() % 10, bybitQty, "0", fixedTicks, 2);
                ++u;
                v.push_back(msg("delta", b, as));
            }
            return v;
        };
        const auto warm = makeDeltas(2000);
        const auto msgs = makeDeltas(n);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        microRow("Bybit delta", *a, msgs);
    }

    // ======================= 2. 旧流程 vs 新流程 =======================
    std::printf("\n2. Old feed pipeline vs adapter (same Binance depth20 messages, from a filled beast::flat_buffer)\n");
    printHeader();
    {
        const auto msgs = binanceDepth20(n, rng);
        boost::beast::flat_buffer buffer;
        auto fill = [&](const std::string& m) {
            buffer.clear();
            auto mb = buffer.prepare(m.size());
            std::memcpy(mb.data(), m.data(), m.size());
            buffer.commit(m.size());
        };
        // 旧流程：buffers_to_string -> msg.assign -> padded_string -> iterate -> parseBinanceDepth（含旧的互斥锁）
        simdjson::ondemand::parser parser;
        OrderBook legacy_book("Binance:BTCUSDT");
        std::mutex legacy_mutex;
        std::string msg;
        msg.reserve(4096);
        auto oldPipeline = [&](const std::string& m) {
            fill(m);  // 模拟 ws.read 填好缓冲区（两种流程都包含这一步）
            msg.assign(boost::beast::buffers_to_string(buffer.data()));
            simdjson::padded_string padded_msg(msg);
            simdjson::ondemand::document doc;
            if (parser.iterate(padded_msg).get(doc)) return;
            legacy::parseBinanceDepth(doc, legacy_book, legacy_mutex);
        };
        auto a = makeAdapter(Exchange::BINANCE, config(Exchange::BINANCE));
        a->onConnected();
        AdapterResult out;
        auto newPipeline = [&](const std::string& m) {
            fill(m);
            out.reset();
            a->onMessage(std::string_view(static_cast<const char*>(buffer.data().data()), buffer.size()), out);
        };
        for (std::size_t i = 0; i < 2000 && i < msgs.size(); ++i) {
            oldPipeline(msgs[i]);
            newPipeline(msgs[i]);
        }
        const auto [old_r, new_r] = measureAB("old: buffers_to_string+padded_string+parse",
                                              "new: BinanceAdapter (reusable padded buffer)", msgs, 40.0, oldPipeline,
                                              newPipeline);
        printResult(old_r);
        printResult(new_r);
        // 只看缓冲区处理：每条消息两次分配 + 两次复制 vs 一次复制、零分配
        adapter_detail::PaddedBuffer pbuf;
        auto oldBuffer = [&](const std::string& m) {
            fill(m);
            msg.assign(boost::beast::buffers_to_string(buffer.data()));
            simdjson::padded_string padded_msg(msg);
            doNotOptimize(padded_msg.data());
        };
        auto newBuffer = [&](const std::string& m) {
            fill(m);
            auto view = pbuf.assign(std::string_view(static_cast<const char*>(buffer.data().data()), buffer.size()));
            doNotOptimize(view.data());
        };
        const auto [old_buf, new_buf] =
            measureAB("old buffer handling only", "new buffer handling only", msgs, 0.0, oldBuffer, newBuffer);
        printResult(old_buf);
        printResult(new_buf);
        const bool same = legacy_book.getBidCount() == a->book().getBidCount() &&
                          legacy_book.getBids()[0].price == a->book().getBids()[0].price &&
                          legacy_book.getAsks().back().quantity == a->book().getAsks().back().quantity;
        std::printf("  old -> new end-to-end: p50 %llu -> %llu ns (%.2fx), p99 %llu -> %llu ns, allocs/msg %.1f -> %.1f; "
                    "books identical: %s\n",
                    static_cast<unsigned long long>(old_r.p50), static_cast<unsigned long long>(new_r.p50),
                    static_cast<double>(old_r.p50) / static_cast<double>(std::max<std::uint64_t>(1, new_r.p50)),
                    static_cast<unsigned long long>(old_r.p99), static_cast<unsigned long long>(new_r.p99),
                    old_r.allocs_per_msg, new_r.allocs_per_msg, same ? "yes" : "NO");
    }

    // ======================= 3. OrderBook upsert 微基准 =======================
    std::printf("\n3. OrderBook upsert on a 400-level book (ns/op, mean over the batch)\n");
    {
        const std::size_t ops = quick ? 200000 : 2000000;
        auto build = [](OrderBook& b) {
            b.clear();
            for (int i = 0; i < 400; ++i) {
                b.updateBid(65000.0 - 0.5 * i, 1.0 + i);
                b.updateAsk(65000.5 + 0.5 * i, 1.0 + i);
            }
        };
        auto timeIt = [&](const char* name, auto&& op) {
            OrderBook b;
            b.reserve(1024);
            build(b);
            std::mt19937_64 r(7);
            std::vector<std::uint32_t> rnd(4096);
            for (auto& x : rnd) x = static_cast<std::uint32_t>(r());
            const std::int64_t t0 = hft::nowNs();
            for (std::size_t i = 0; i < ops; ++i) op(b, i, rnd[i & 4095]);
            const double ns = static_cast<double>(hft::nowNs() - t0) / static_cast<double>(ops);
            doNotOptimize(b.getBids()[0].quantity);
            std::printf("  %-58s %8.1f ns/op   (book %d+%d levels, valid=%s)\n", name, ns, b.getBidCount(), b.getAskCount(),
                        b.validate() ? "yes" : "NO");
        };
        timeIt("qty change at levels 0..4 (in place, head/binary search)", [](OrderBook& b, std::size_t i, std::uint32_t x) {
            const int lvl = static_cast<int>(x % 5);
            b.setLevel((i & 1) ? BookSide::Bid : BookSide::Ask, (i & 1) ? 65000.0 - 0.5 * lvl : 65000.5 + 0.5 * lvl,
                       1.0 + static_cast<double>(x % 1000));
        });
        timeIt("new best price inserted then removed (memmove 400 levels)", [](OrderBook& b, std::size_t i, std::uint32_t x) {
            const bool bid = (i & 2) != 0;
            const double p = bid ? 65000.25 : 65000.25;  // 价差中间
            b.setLevel(bid ? BookSide::Bid : BookSide::Ask, p, (i & 1) ? 0.0 : 1.0 + static_cast<double>(x % 7));
        });
        timeIt("qty change at a random level of 400 (binary search)", [](OrderBook& b, std::size_t i, std::uint32_t x) {
            const int lvl = static_cast<int>(x % 400);
            b.setLevel((i & 1) ? BookSide::Bid : BookSide::Ask, (i & 1) ? 65000.0 - 0.5 * lvl : 65000.5 + 0.5 * lvl,
                       2.0 + static_cast<double>(x % 1000));
        });
        timeIt("erase + re-insert a random level of 400", [](OrderBook& b, std::size_t i, std::uint32_t x) {
            const int lvl = static_cast<int>(x % 400);
            const double p = 65000.0 - 0.5 * lvl;
            b.setLevel(BookSide::Bid, p, (i & 1) ? 1.0 : 0.0);
        });
        {
            // 旧 feed 的整簿重建：clearBids + 按交易所顺序 20 次 updateBid（尾部快路径，每档 O(1)）
            OrderBook b;
            std::vector<double> prices(20), qtys(20);
            for (int k = 0; k < 20; ++k) {
                prices[static_cast<std::size_t>(k)] = 65000.0 - 0.5 * k;
                qtys[static_cast<std::size_t>(k)] = 1.0 + k;
            }
            const std::size_t rounds = ops / 20;
            const std::int64_t t0 = hft::nowNs();
            for (std::size_t i = 0; i < rounds; ++i) {
                b.clearBids();
                for (int k = 0; k < 20; ++k) b.updateBid(prices[static_cast<std::size_t>(k)], qtys[static_cast<std::size_t>(k)] + static_cast<double>(i & 7));
                doNotOptimize(b.getBids()[19].quantity);
            }
            const double ns = static_cast<double>(hft::nowNs() - t0) / static_cast<double>(rounds * 20);
            std::printf("  %-58s %8.1f ns/level\n", "legacy rebuild: clearBids + 20 appends (tail fast path)", ns);
        }
        {
            // 对照：std::map<double, double> 的同类顶部改量
            std::map<double, double, std::greater<double>> m;
            for (int i = 0; i < 400; ++i) m[65000.0 - 0.5 * i] = 1.0 + i;
            std::mt19937_64 r(7);
            const std::int64_t t0 = hft::nowNs();
            for (std::size_t i = 0; i < ops; ++i) {
                const int lvl = static_cast<int>(r() % 5);
                m[65000.0 - 0.5 * lvl] = 1.0 + static_cast<double>(i % 1000);
            }
            const double ns = static_cast<double>(hft::nowNs() - t0) / static_cast<double>(ops);
            doNotOptimize(m.begin()->second);
            std::printf("  %-58s %8.1f ns/op\n", "reference: std::map qty change at levels 0..4", ns);
        }
        {
            // BookSnapshot 发布与消费（feed 每条消息一次 assignFrom，主线程一次 copyTo）
            OrderBook b;
            build(b);
            OrderBook dst;
            BookSnapshot s;
            const std::size_t rounds = ops / 10;
            const std::int64_t t0 = hft::nowNs();
            for (std::size_t i = 0; i < rounds; ++i) {
                s.assignFrom(b);
                s.seq = i;
                s.copyTo(dst);
                doNotOptimize(dst.getAsks()[0].price);
            }
            const double ns = static_cast<double>(hft::nowNs() - t0) / static_cast<double>(rounds);
            std::printf("  %-58s %8.1f ns/op\n", "BookSnapshot assignFrom(20 levels) + copyTo", ns);
        }
    }

    // ======================= 4. 一条增量消息应用到深簿：逐档 setLevel vs 批量 applyLevels =======================
    // 增量消息（Binance 100 ms diff、OKX books update）的档位变化分布在整个深度上：逐档 upsert 每次插入/删除
    // 都要 memmove 半个簿；applyLevels 一次线性归并（输入不是最优在前时先排序）。三种方式应用完全相同的变化，
    // 最终簿必须逐位相同。生成器见 makeDeepWorkload：深度平稳在标签值上，插入/删除/改量各约 1/3；
    // 每行旁边打印实测的平均深度与变化构成，偏离标签时自检失败。最深一档的标签等于适配器的簿上限 kMaxBookLevels。
    std::printf("\n4. One diff message applied to a deep book: per-level setLevel vs batched applyLevels (us/msg)\n");
    std::printf("   Depth-stationary generator: each change deletes an existing level, inserts a new one or changes a\n"
                "   level's quantity (1/3 each, uniform over the book); depth held at the label +-1; each price at most\n"
                "   once per message. applyLevels runs on the batch sorted best-first (exchange order) and shuffled.\n");
    std::printf("  %-26s %10s %19s %10s %10s %10s %10s %10s  %s\n", "book depth x changes/msg", "mean depth",
                "ins/del/qty/noop %", "setLevel", "best-first", "shuffled", "x best-1st", "x shuffled", "same book");
    {
        struct Case {
            std::size_t depth;
            std::size_t changes;
        };
        const Case cases[] = {{50, 7}, {400, 30}, {1000, 30}, {1000, 130}, {5000, 130}, {5000, 300}};
        for (const Case& c : cases) {
            const std::size_t msgs = quick ? 200 : 2000;
            const DeepWorkload w = makeDeepWorkload(c.depth, c.changes, msgs, 11);
            // mode 0：逐档 setLevel（最优在前的顺序）；1：applyLevels（最优在前）；2：applyLevels（打乱）
            auto run = [&](int mode, OrderBook& book) {
                book = w.start;
                book.reserve(2 * c.depth);
                LevelBatchScratch scratch;
                const auto& batches = mode == 2 ? w.shuffled : w.best_first;
                std::int64_t total = 0;
                for (const auto& batch : batches) {
                    const std::int64_t t0 = hft::nowNs();
                    if (mode == 0) {
                        for (const PriceLevel& l : batch) book.setLevel(BookSide::Bid, l.price, l.quantity);
                    } else {
                        book.applyLevels(BookSide::Bid, batch.data(), batch.size(), scratch);
                    }
                    total += hft::nowNs() - t0;
                }
                doNotOptimize(book.getBids()[0].quantity);
                return static_cast<double>(total) / static_cast<double>(msgs) / 1000.0;
            };
            OrderBook seq_book, sorted_book, shuffled_book;
            const double seq_us = run(0, seq_book);
            const double sorted_us = run(1, sorted_book);
            const double shuffled_us = run(2, shuffled_book);
            // 不计时的核对：实测深度与变化构成
            ChangeMix mix;
            OrderBook ref = w.start;
            for (const auto& batch : w.best_first) {
                mix.depth_sum += ref.getBidCount();
                ++mix.messages;
                applyClassified(ref, BookSide::Bid, batch, mix);
            }
            const bool same = sameBook(seq_book, sorted_book) && sameBook(seq_book, shuffled_book) && sameBook(seq_book, ref);
            char label[64];
            std::snprintf(label, sizeof label, "%zu levels x %zu changes", c.depth, c.changes);
            char shares[48];
            std::snprintf(shares, sizeof shares, "%.1f/%.1f/%.1f/%.1f", mix.pct(mix.inserts), mix.pct(mix.deletes),
                          mix.pct(mix.qty_changes), mix.pct(mix.noops));
            std::printf("  %-26s %10.1f %19s %10.2f %10.2f %10.2f %9.1fx %9.1fx  %s\n", label, mix.meanDepth(), shares,
                        seq_us, sorted_us, shuffled_us, seq_us / sorted_us, seq_us / shuffled_us, same ? "yes" : "NO");
            check(same, std::string(label) + ": the three apply methods produced different books");
            const double depth = static_cast<double>(c.depth);
            check(std::abs(mix.meanDepth() - depth) <= std::max(1.0, 0.02 * depth),
                  std::string(label) + ": mean depth is not the labelled depth");
            for (std::uint64_t part : {mix.inserts, mix.deletes, mix.qty_changes}) {
                check(std::abs(mix.pct(part) - 100.0 / 3.0) <= 5.0,
                      std::string(label) + ": change mix is not about 1/3 insert, 1/3 delete, 1/3 quantity change");
            }
        }
    }
    if (g_check_failures != 0) {
        std::printf("\n%d self-check(s) failed\n", g_check_failures);
        return 1;
    }
    return 0;
}
