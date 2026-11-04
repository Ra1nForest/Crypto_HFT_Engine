//
// market_data_benchmark.cpp
// 行情模块基准测试。注意：本程序直接包含并测试 include/ 下的真实头文件（exchanges/*、orderbook.h、hft/crc32.h），
// 不是另写的副本；"旧实现"一栏是升级前 market_data_feed.h 的处理流程（buffers_to_string + 每条消息构造
// padded_string + 旧的 parseBinanceDepth），原样复制在本文件中作为对照。
// 内容：
//   1. 各适配器每条消息的处理耗时（p50/p99/均值，TSC 计时）：Binance depth20 / diff、OKX books 快照 / 增量（含校验和）、Bybit delta
//   2. 旧流程 vs 新流程：同一批 Binance depth20 消息，端到端耗时与每条消息的堆分配次数；以及只看缓冲区处理的耗时。
//      两种流程在同一条消息上交替计时（A/B 交错，奇偶轮换先后），共享 CPU 上的噪声对两者影响相同。
//      旧流程使用的是升级后的 OrderBook（尾部快路径），因此差异只来自消息处理方式。
// 注意：机器上的 CPU 由多个进程共享，单次运行的绝对值可能有 ±50% 的波动；比较请看同一次运行内的相对值。
//   3. OrderBook upsert 微基准：400 档簿上的顶部更新（原地改量、顶部插入/删除、随机档位、旧式整簿重建）
// 用法：./build/market_data_benchmark [--quick]
//

#include "book_snapshot.h"
#include "exchanges/adapters.h"
#include "hft/crc32.h"
#include "hft/latency_histogram.h"
#include "hft/timing.h"
#include "orderbook.h"

#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <random>
#include <string>
#include <string_view>
#include <vector>

// ---------------------------------------------------------------------------
// 堆分配计数（单线程基准，普通计数器即可）
// ---------------------------------------------------------------------------
namespace {
std::uint64_t g_allocs = 0;
}
__attribute__((noinline)) void* operator new(std::size_t n) {
    ++g_allocs;
    void* p = std::malloc(n == 0 ? 1 : n);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
__attribute__((noinline)) void* operator new[](std::size_t n) { return ::operator new(n); }
__attribute__((noinline)) void operator delete(void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::size_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
// nothrow 版本也必须替换：否则（例如在 ASan 下）simdjson 的 new (std::nothrow) 来自另一个分配器，
// 却由上面的 operator delete[] 用 free 释放
__attribute__((noinline)) void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    ++g_allocs;
    return std::malloc(n == 0 ? 1 : n);
}
__attribute__((noinline)) void* operator new[](std::size_t n, const std::nothrow_t& t) noexcept {
    return ::operator new(n, t);
}
__attribute__((noinline)) void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }

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
// 计时：每条消息 rdtsc/rdtscp，结果按 TSC 频率换算为 ns 记入直方图
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

template <class Fn>
Result measure(const char* name, const std::vector<std::string>& msgs, double levels_per_msg, Fn&& fn) {
    hft::LatencyHistogram hist;
    const hft::TscClock& clock = hft::TscClock::instance();
    double bytes = 0;
    const std::uint64_t allocs0 = g_allocs;
    for (const std::string& m : msgs) {
        const std::uint64_t t0 = hft::rdtsc();
        fn(m);
        const std::uint64_t t1 = hft::rdtscp();
        hist.record(static_cast<std::uint64_t>(std::max<std::int64_t>(0, clock.toNs(t1 - t0))));
        bytes += static_cast<double>(m.size());
    }
    const double n = static_cast<double>(msgs.size());
    return {name, msgs.size(), bytes / n, levels_per_msg, hist.percentile(0.50), hist.percentile(0.99), hist.mean(),
            static_cast<double>(g_allocs - allocs0) / n};
}

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
    const bool quick = argc > 1 && std::strcmp(argv[1], "--quick") == 0;
    const std::size_t n = quick ? 2000 : 20000;
    std::mt19937_64 rng(20261104);
    hft::TscClock::instance();  // 先校准

    std::printf("Market data benchmark (real headers from include/)\n");
    // stage 1（结构索引）在运行时按 CPU 选择内核；On-Demand 的内联迭代代码使用编译期的 builtin 内核
    std::printf("  simdjson %s: stage-1 kernel %s (runtime dispatch), On-Demand inline kernel %s (compile flags)\n",
                SIMDJSON_VERSION, simdjson::get_active_implementation()->name().c_str(),
                simdjson::builtin_implementation()->name().c_str());
    std::printf("  TSC: %.3f ns/tick, %zu messages per scenario\n", hft::TscClock::instance().nsPerTick(), n);

    // ======================= 1. 各适配器每条消息耗时 =======================
    std::printf("\n1. Per-message processing (adapter.onMessage, TSC-timed, warmed up)\n");
    printHeader();
    std::vector<Result> results;

    // Binance depth20
    {
        auto a = makeAdapter(Exchange::BINANCE, config(Exchange::BINANCE));
        a->onConnected();
        const auto warm = binanceDepth20(2000, rng);
        const auto msgs = binanceDepth20(n, rng);
        AdapterResult out;
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        results.push_back(measure("Binance depth20 (partial snapshot)", msgs, 40.0, [&](const std::string& m) {
            out.reset();
            a->onMessage(m, out);
        }));
        printResult(results.back());
    }

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
        auto makeEvents = [&](std::size_t count, double& levels) {
            std::vector<std::string> out;
            double total = 0;
            for (std::size_t i = 0; i < count; ++i) {
                std::map<std::pair<bool, std::int64_t>, std::string> agg;
                const std::size_t k = 10 + rng() % 30;  // 100ms 增量通常十几到几十档
                const std::uint64_t first = id + 1;
                for (std::size_t c = 0; c < k; ++c) {
                    bool bid;
                    std::int64_t tick;
                    std::string q;
                    randomChange(book, rng, 1000, qty8, bid, tick, q);
                    agg[{bid, tick}] = q == "0" ? "0.00000000" : q;
                    ++id;
                }
                Levels b, a;
                for (const auto& kv : agg) (kv.first.first ? b : a).push_back({fixedTicks(kv.first.second, 2), kv.second});
                total += static_cast<double>(b.size() + a.size());
                out.push_back("{\"e\":\"depthUpdate\",\"E\":1700000000000,\"s\":\"BTCUSDT\",\"U\":" + std::to_string(first) +
                              ",\"u\":" + std::to_string(id) + ",\"b\":" + levelsJson(b) + ",\"a\":" + levelsJson(a) + "}");
            }
            levels = total / static_cast<double>(count);
            return out;
        };
        auto a = makeAdapter(Exchange::BINANCE, config(Exchange::BINANCE));
        a->onConnected();
        AdapterResult out;
        double warm_levels = 0, levels = 0;
        const auto first = makeEvents(1, warm_levels);  // 第一条触发缓冲 + 快照请求
        a->onMessage(first[0], out);
        out.reset();
        a->onRestSnapshot(snapshotNow(), out);  // 快照在第一条事件之后取得：L >= U，第一条被丢弃
        if (!a->synced()) std::printf("  !! binance diff stream did not sync\n");
        const auto warm = makeEvents(2000, warm_levels);
        const auto msgs = makeEvents(n, levels);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        results.push_back(measure("Binance diff depth (synced, 1000-level book)", msgs, levels, [&](const std::string& m) {
            out.reset();
            a->onMessage(m, out);
        }));
        printResult(results.back());
        if (!a->synced() || a->stats().gaps != 0) std::printf("  !! binance diff stream lost sync\n");
    }

    // OKX books：400 档快照（含校验和）与增量（含校验和）
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
        auto a = makeAdapter(Exchange::OKX, config(Exchange::OKX));
        a->onConnected();
        AdapterResult out;
        const std::vector<std::string> snaps(n / 10, snapshot());
        for (int i = 0; i < 200; ++i) {
            out.reset();
            a->onMessage(snaps[0], out);
        }
        results.push_back(measure("OKX books snapshot (400+400, checksum)", snaps, 800.0, [&](const std::string& m) {
            out.reset();
            a->onMessage(m, out);
        }));
        printResult(results.back());
        auto makeUpdates = [&](std::size_t count, double& levels) {
            std::vector<std::string> v;
            double total = 0;
            for (std::size_t i = 0; i < count; ++i) {
                std::map<std::pair<bool, std::int64_t>, std::string> agg;
                const std::size_t k = 2 + rng() % 12;
                for (std::size_t c = 0; c < k; ++c) {
                    bool bid;
                    std::int64_t tick;
                    std::string q;
                    randomChange(book, rng, 400, okxQty, bid, tick, q);
                    agg[{bid, tick}] = q;
                }
                Levels b, as;
                for (const auto& kv : agg) (kv.first.first ? b : as).push_back({okxPx(kv.first.second, 1), kv.second});
                total += static_cast<double>(b.size() + as.size());
                const std::int64_t prev = seq++;
                v.push_back("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"update\",\"data\":[{\"asks\":" +
                            levelsJson(as, true) + ",\"bids\":" + levelsJson(b, true) +
                            ",\"ts\":\"1597026383085\",\"checksum\":" + okxChecksumOf(book) +
                            ",\"prevSeqId\":" + std::to_string(prev) + ",\"seqId\":" + std::to_string(seq) + "}]}");
            }
            levels = total / static_cast<double>(count);
            return v;
        };
        out.reset();
        a->onMessage(snapshot(), out);
        double warm_levels = 0, levels = 0;
        const auto warm = makeUpdates(2000, warm_levels);
        const auto msgs = makeUpdates(n, levels);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        results.push_back(measure("OKX books update (checksum verified)", msgs, levels, [&](const std::string& m) {
            out.reset();
            a->onMessage(m, out);
        }));
        printResult(results.back());
        if (!a->synced() || a->stats().checksum_failures != 0) std::printf("  !! okx stream lost sync\n");
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
        auto makeDeltas = [&](std::size_t count, double& levels) {
            std::vector<std::string> v;
            double total = 0;
            for (std::size_t i = 0; i < count; ++i) {
                std::map<std::pair<bool, std::int64_t>, std::string> agg;
                const std::size_t k = 3 + rng() % 10;
                for (std::size_t c = 0; c < k; ++c) {
                    bool bid;
                    std::int64_t tick;
                    std::string q;
                    randomChange(book, rng, 50, bybitQty, bid, tick, q);
                    agg[{bid, tick}] = q;
                }
                Levels b, as;
                for (const auto& kv : agg) (kv.first.first ? b : as).push_back({fixedTicks(kv.first.second, 2), kv.second});
                total += static_cast<double>(b.size() + as.size());
                ++u;
                v.push_back(msg("delta", b, as));
            }
            levels = total / static_cast<double>(count);
            return v;
        };
        double warm_levels = 0, levels = 0;
        const auto warm = makeDeltas(2000, warm_levels);
        const auto msgs = makeDeltas(n, levels);
        for (const auto& m : warm) {
            out.reset();
            a->onMessage(m, out);
        }
        results.push_back(measure("Bybit orderbook.50 delta", msgs, levels, [&](const std::string& m) {
            out.reset();
            a->onMessage(m, out);
        }));
        printResult(results.back());
        if (!a->synced() || a->stats().gaps != 0) std::printf("  !! bybit stream lost sync\n");
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
    return 0;
}
