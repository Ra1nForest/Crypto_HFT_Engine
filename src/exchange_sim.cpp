//
// exchange_sim.cpp
// 多线程模拟交易所：G 个网关线程（做市商 / 吃单者，各自独立种子）经无锁 SPSC 队列驱动单个撮合引擎线程，
// 测量往返延迟（命令发出 → 收到该命令的第一个事件）与吞吐量，结束时校验订单簿不变量与客户端/交易所状态一致性。
// 用法：./build/exchange_sim [--orders N] [--gateways G] [--symbols S] [--seed X] [--pin] [--help]（其余选项见 --help）
//

#include "hft/alloc_counter.h"
#include "cli_util.h"
#include "hft/common.h"
#include "hft/latency_histogram.h"
#include "hft/timing.h"
#include "matching/engine_runner.h"
#include "matching/types.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif

// 堆分配计数见 hft/alloc_counter.h（计数版全局 operator new/delete，本程序唯一包含它的翻译单元）
namespace {
std::atomic<std::uint64_t>& g_heap_allocations = hft::alloc_counter::g_total;
}  // namespace

namespace {

using namespace hft::matching;
using Runner = EngineRunner<>;

// ============================================================================
// 命令行
// ============================================================================

struct Options {
    std::uint64_t orders = 2'000'000;  // 全部网关发送的命令总数
    std::size_t gateways = 2;
    std::size_t symbols = 1;
    std::uint64_t seed = 1;
    bool pin = false;
    std::size_t makers = SIZE_MAX;  // 做市网关数（SIZE_MAX = 自动：一半，向上取整）
    std::size_t window = 8;     // 每个网关的最大在途命令数
    std::size_t levels = 10;    // 每个做市商在每个交易对每一侧报价的档数
    std::size_t batch = 32;     // 引擎每轮每个入站队列最多处理的命令数
    long long yield_after = -1; // 连续空转多少次后 yield（-1 = 自动）
};

constexpr std::size_t kMaxSimGateways = 4096;  // 订单 id 编码用 16 位网关字段（见 makeOrderId）
constexpr std::size_t kMaxSlots = 0xFFFF;      // 订单 id 编码用 16 位报价槽位字段（0xFFFF 留给吃单）

void printUsage(std::FILE* out) {
    std::fprintf(out,
                 "Usage: exchange_sim [options]\n"
                 "Multi-threaded exchange simulator: G gateway threads (seeded market makers and takers)\n"
                 "drive one matching-engine thread through lock-free SPSC queues, then report throughput,\n"
                 "round-trip latency, event counts and final book state, and verify book invariants.\n"
                 "\n"
                 "  --orders N       total commands sent by all gateways (default 2000000)\n"
                 "  --gateways G     gateway threads (default 2, max %zu)\n"
                 "  --symbols S      symbols (default 1)\n"
                 "  --seed X         RNG seed (default 1)\n"
                 "  --pin            pin the engine thread and each gateway thread to its own CPU\n"
                 "  --makers M       market-maker gateways; the others are takers (default: half,\n"
                 "                   rounded up; a single gateway runs a hybrid maker/taker trader)\n"
                 "  --window W       max in-flight commands per gateway (default 8)\n"
                 "  --levels L       quote levels per side and symbol for each market maker (default 10)\n"
                 "  --batch K        commands the engine drains per gateway queue per turn (default 32)\n"
                 "  --yield-after N  idle spins before yielding the CPU; 0 = pure busy-spin\n"
                 "                   (default: 0 when every thread gets its own CPU, else 64)\n"
                 "  --help           show this help\n"
                 "\n"
                 "Exit status: 0 = all checks passed, 1 = a check failed, 2 = bad command line.\n",
                 kMaxSimGateways);
}

using cli_util::parseUnsigned;

// 返回 0 = 继续运行，1 = 已打印帮助（正常退出），2 = 参数错误
int parseOptions(int argc, char** argv, Options& o) {
    auto bad = [](const std::string& msg) {
        std::fprintf(stderr, "exchange_sim: %s\n(try --help)\n", msg.c_str());
        return 2;
    };
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(stdout);
            return 1;
        }
        if (arg == "--pin") {
            o.pin = true;
            continue;
        }
        auto value = [&](std::uint64_t lo, std::uint64_t hi, std::uint64_t& out) -> bool {
            if (i + 1 >= argc) return false;
            std::uint64_t v = 0;
            if (!parseUnsigned(argv[++i], v) || v < lo || v > hi) return false;
            out = v;
            return true;
        };
        std::uint64_t v = 0;
        if (arg == "--orders") {
            if (!value(1, std::uint64_t{1} << 40, v)) return bad("--orders needs an integer in [1, 2^40]");
            o.orders = v;
        } else if (arg == "--gateways") {
            if (!value(1, kMaxSimGateways, v)) return bad("--gateways needs an integer in [1, 4096]");
            o.gateways = static_cast<std::size_t>(v);
        } else if (arg == "--symbols") {
            if (!value(1, 4096, v)) return bad("--symbols needs an integer in [1, 4096]");
            o.symbols = static_cast<std::size_t>(v);
        } else if (arg == "--seed") {
            if (!value(0, UINT64_MAX, v)) return bad("--seed needs a non-negative integer");
            o.seed = v;
        } else if (arg == "--makers") {
            if (!value(0, kMaxSimGateways, v)) return bad("--makers needs an integer in [0, 4096]");
            o.makers = static_cast<std::size_t>(v);
        } else if (arg == "--window") {
            if (!value(1, 1 << 16, v)) return bad("--window needs an integer in [1, 65536]");
            o.window = static_cast<std::size_t>(v);
        } else if (arg == "--levels") {
            if (!value(1, 1000, v)) return bad("--levels needs an integer in [1, 1000]");
            o.levels = static_cast<std::size_t>(v);
        } else if (arg == "--batch") {
            if (!value(1, 1 << 20, v)) return bad("--batch needs an integer in [1, 1048576]");
            o.batch = static_cast<std::size_t>(v);
        } else if (arg == "--yield-after") {
            if (!value(0, UINT32_MAX, v)) return bad("--yield-after needs a non-negative 32-bit integer");
            o.yield_after = static_cast<long long>(v);
        } else {
            return bad("unknown option '" + arg + "'");
        }
    }
    if (o.makers != SIZE_MAX && o.makers > o.gateways) return bad("--makers cannot exceed --gateways");
    if (o.symbols * 2 * o.levels >= kMaxSlots) return bad("--symbols x --levels x 2 must stay below 65535");
    if (o.orders < o.gateways) return bad("--orders must be at least --gateways");
    return 0;
}

// ============================================================================
// 工具
// ============================================================================

// splitmix64：快速、可复现的伪随机数（每个线程各自一个实例）
class Rng {
public:
    explicit Rng(std::uint64_t seed) noexcept : state_(seed) {}
    std::uint64_t next() noexcept {
        std::uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        return z ^ (z >> 31);
    }
    std::uint64_t below(std::uint64_t n) noexcept { return n == 0 ? 0 : next() % n; }  // 取模偏差可忽略
    std::int64_t range(std::int64_t lo, std::int64_t hi) noexcept {
        return lo + static_cast<std::int64_t>(below(static_cast<std::uint64_t>(hi - lo) + 1));
    }

private:
    std::uint64_t state_;
};

std::string withCommas(std::uint64_t v) {
    std::string digits = std::to_string(v);
    std::string out;
    out.reserve(digits.size() + digits.size() / 3);
    const std::size_t lead = digits.size() % 3 == 0 ? 3 : digits.size() % 3;
    for (std::size_t i = 0; i < digits.size(); ++i) {
        if (i >= lead && (i - lead) % 3 == 0) out += ',';
        out += digits[i];
    }
    return out;
}

std::string priceText(std::optional<Price> p) { return p ? withCommas(static_cast<std::uint64_t>(*p)) : "-"; }

std::vector<int> allowedCpus() {
    std::vector<int> cpus;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int c = 0; c < CPU_SETSIZE; ++c) {
            if (CPU_ISSET(c, &set)) cpus.push_back(c);
        }
    }
#endif
    if (cpus.empty()) {
        const unsigned n = std::thread::hardware_concurrency();
        for (unsigned c = 0; c < (n == 0 ? 1u : n); ++c) cpus.push_back(static_cast<int>(c));
    }
    return cpus;
}

const char* kSymbolNames[] = {"BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT",
                              "XRP-USDT", "DOGE-USDT", "ADA-USDT", "AVAX-USDT"};

std::string symbolName(std::size_t s) {
    if (s < sizeof(kSymbolNames) / sizeof(kSymbolNames[0])) return kSymbolNames[s];
    return "SYM-" + std::to_string(s);
}

// 订单 id 编码：[63:48] 网关 + 1，[47:32] 报价槽位（吃单为 0xFFFF），[31:0] 该网关的新订单计数。
// 各网关的 id 空间互不相交；同一槽位的新订单 id 也各不相同（计数递增）。
OrderId makeOrderId(GatewayId g, std::size_t slot, std::uint64_t n) {
    return ((static_cast<OrderId>(g) + 1) << 48) | ((static_cast<OrderId>(slot) & 0xFFFF) << 32) |
           (n & 0xFFFF'FFFFULL);
}

// ============================================================================
// 市场：每个交易对一条预先生成的中间价随机游走（只读，全部线程共享）。
// 每个网关按自己的进度（已发命令数 / 配额）索引它，因此各网关看到的中间价同步移动、且与线程调度无关。
// ============================================================================
class Market {
public:
    static constexpr std::size_t kSteps = std::size_t{1} << 14;
    static constexpr Price kInitialMid = 100'000;  // tick

    Market(std::size_t symbols, std::uint64_t seed) : walks_(symbols) {
        Rng rng(seed ^ 0x5eed'0f'3a1c'e7ULL);
        for (std::size_t s = 0; s < symbols; ++s) {
            std::vector<Price>& w = walks_[s];
            w.resize(kSteps);
            Price mid = kInitialMid + static_cast<Price>(s) * 1000;
            for (std::size_t i = 0; i < kSteps; ++i) {
                const std::uint64_t r = rng.below(8);  // 每步 1/8 上涨、1/8 下跌
                if (r == 0) ++mid;
                else if (r == 1) --mid;
                w[i] = std::max<Price>(mid, 1000);
            }
        }
    }

    Price mid(SymbolId s, std::size_t step) const noexcept { return walks_[s][step]; }
    std::size_t symbols() const noexcept { return walks_.size(); }

private:
    std::vector<std::vector<Price>> walks_;
};

// ============================================================================
// 模拟交易者（每个网关线程一个）
// - Maker：在每个交易对两侧各挂 levels 档 post-only 报价（中间价 ± 半价差 ± 档位）；中间价移动后用改单
//   （cancel/replace）把报价挪到新价位；价位没变时随机撤单或原地减量，制造真实的撤改流量。
//   每个报价槽位同时最多一条在途命令；槽位状态完全由事件驱动（Accepted/Modified/Fill/Cancelled/Rejected）。
// - Taker：随机方向、以小单为主的 IOC 限价单（越过盘口 0~2 tick）和市价单。
// - Hybrid：只有一个网关时使用，75% 做市动作 + 25% 吃单动作（做市与吃单用不同账户，避免自成交保护）。
// 流量控制：在途命令数不超过 window；每个命令的第一个事件（一定发回本网关、且按发送顺序到达）
// 即为确认，用于测量往返延迟，并核对回显的 client_ts 与最早的在途命令一致。
// ============================================================================
enum class Role : std::uint8_t { Maker, Taker, Hybrid };

const char* roleName(Role r) {
    switch (r) {
        case Role::Maker: return "maker";
        case Role::Taker: return "taker";
        case Role::Hybrid: return "hybrid";
    }
    return "?";
}

struct QuoteSlot {
    OrderId id = 0;       // 该槽位当前（或最近一次）的订单 id；0 = 从未使用
    Price price = 0;
    Qty remaining = 0;
    bool live = false;    // 交易所已确认挂在簿上
    bool pending = false; // 有一条针对该槽位的在途命令
};

struct TraderStats {
    std::uint64_t sent = 0;
    std::uint64_t acked = 0;
    std::uint64_t sent_by_type[3] = {};  // New / Cancel / Modify
    std::uint64_t quotes = 0;            // New 中的 post-only 报价
    std::uint64_t iocs = 0;              // New 中的 IOC 限价单
    std::uint64_t markets = 0;           // New 中的市价单
    std::uint64_t events_by_type[5] = {};
    std::uint64_t maker_fills = 0;
    std::uint64_t taker_fills = 0;
    std::uint64_t maker_qty = 0;
    std::uint64_t taker_qty = 0;
    std::uint64_t reject_reasons[8] = {};
    std::uint64_t cancel_reasons[5] = {};
    std::uint64_t echo_mismatches = 0;   // 首个事件回显的 client_ts 与最早在途命令不符（恒应为 0）
    std::uint64_t stale_events = 0;      // 本网关订单 id 但不是任何槽位当前订单的事件（恒应为 0）
};

struct StartGate {
    std::atomic<std::size_t> ready{0};
    std::atomic<bool> go{false};
    std::atomic<std::size_t> done{0};
    std::atomic<bool> stopped{false};
};

struct SimSettings {
    std::size_t symbols;
    std::size_t levels;
    std::size_t window;
    std::uint32_t yield_after;
};

class Trader {
public:
    Trader(GatewayId id, Role role, std::uint64_t quota, std::uint64_t seed, const SimSettings& settings,
           const Market& market, Runner& runner, int cpu)
        : id_(id),
          role_(role),
          quota_(quota),
          settings_(settings),
          market_(market),
          in_(runner.inbound(id)),
          out_(runner.outbound(id)),
          rng_(seed),
          cpu_(cpu),
          tsc_(hft::TscClock::instance()),
          maker_account_(static_cast<AccountId>(2 * static_cast<std::uint32_t>(id) + 1)),
          taker_account_(static_cast<AccountId>(2 * static_cast<std::uint32_t>(id) + 2)),
          half_spread_(1 + static_cast<Price>(id % 2)) {
        if (role_ != Role::Taker) slots_.resize(settings_.symbols * 2 * settings_.levels);
        ring_mask_ = hft::nextPow2(settings_.window) - 1;
        sent_ts_.assign(ring_mask_ + 1, 0);
    }

    // 网关线程主体
    void run(StartGate& gate) {
        char name[16];
        std::snprintf(name, sizeof(name), "gw-%u", static_cast<unsigned>(id_));
        hft::setThreadName(name);
        if (cpu_ >= 0) pinned_ = hft::pinThreadToCpu(cpu_);
        gate.ready.fetch_add(1, std::memory_order_release);
        while (!gate.go.load(std::memory_order_acquire)) std::this_thread::yield();

        std::uint32_t spins = 0;
        bool have = false;     // cmd 已生成但尚未成功入队（入站队列满时下一轮重试）
        EngineCommand cmd{};
        while (stats_.acked < quota_) {
            bool progress = drain();
            while (stats_.sent < quota_ && stats_.sent - stats_.acked < settings_.window) {
                if (!have) {
                    if (!buildCommand(cmd)) break;  // 所有报价槽位都有在途命令：等确认
                    have = true;
                }
                cmd.client_ts = hft::rdtsc();
                if (!in_.tryPush(cmd)) break;
                have = false;
                sent_ts_[stats_.sent & ring_mask_] = cmd.client_ts;
                ++stats_.sent;
                progress = true;
            }
            if (progress) spins = 0;
            else idlePause(spins);
        }
        finish_ns_ = hft::nowNs();
        gate.done.fetch_add(1, std::memory_order_release);
        // 自己的命令都已确认，但其他网关的命令仍可能让本网关的报价成交：消费到引擎停止为止
        while (!gate.stopped.load(std::memory_order_acquire)) {
            if (!drain()) idlePause(spins);
        }
        drain();
    }

    // 引擎停止后核对客户端状态与交易所状态：每个"在簿"报价都在订单簿里且价格/剩余量/归属一致，
    // 每个"不在簿"报价都确实不在；没有遗留的在途命令。返回在簿报价数。
    bool verify(const Runner& runner, std::size_t& live, std::string* why) const {
        live = 0;
        for (std::size_t i = 0; i < slots_.size(); ++i) {
            const QuoteSlot& q = slots_[i];
            const SymbolId s = static_cast<SymbolId>(i / (2 * settings_.levels));
            if (q.pending) return fail(why, "slot " + std::to_string(i) + " still has a command in flight");
            if (q.id == 0) continue;
            const std::optional<OrderInfo> o = runner.engine().book(s).findOrder(q.id);
            if (!q.live) {
                if (o) return fail(why, "order " + std::to_string(q.id) + " rests in the book but the trader thinks it is gone");
                continue;
            }
            ++live;
            if (!o) return fail(why, "order " + std::to_string(q.id) + " is live for the trader but not in the book");
            if (o->price != q.price || o->remaining != q.remaining)
                return fail(why, "order " + std::to_string(q.id) + ": trader has " + std::to_string(q.remaining) + " @ " +
                                     std::to_string(q.price) + ", book has " + std::to_string(o->remaining) + " @ " +
                                     std::to_string(o->price));
            if (runner.ownerOf(s, q.id) != std::optional<GatewayId>(id_))
                return fail(why, "order " + std::to_string(q.id) + " is not owned by gateway " + std::to_string(id_));
        }
        return true;
    }

    GatewayId id() const noexcept { return id_; }
    Role role() const noexcept { return role_; }
    const TraderStats& stats() const noexcept { return stats_; }
    const hft::LatencyHistogram& latency() const noexcept { return latency_; }
    std::int64_t finishNs() const noexcept { return finish_ns_; }
    bool pinned() const noexcept { return pinned_; }
    int cpu() const noexcept { return cpu_; }

private:
    static bool fail(std::string* why, std::string msg) {
        if (why != nullptr) *why = std::move(msg);
        return false;
    }

    void idlePause(std::uint32_t& spins) const noexcept {
        if (settings_.yield_after != 0 && ++spins >= settings_.yield_after) {
            spins = 0;
            std::this_thread::yield();
        } else {
            hft::cpuRelax();
        }
    }

    // 当前中间价：按本网关的进度索引随机游走
    Price midOf(SymbolId s) const noexcept {
        // sent < 2^40、kSteps < 2^15：乘积不会溢出 64 位
        const std::size_t step = static_cast<std::size_t>(stats_.sent * (Market::kSteps - 1) / quota_);
        return market_.mid(s, step);
    }

    Price targetPrice(SymbolId s, Side side, std::size_t level) const noexcept {
        const Price mid = midOf(s);
        const Price offset = half_spread_ + static_cast<Price>(level);
        return side == Side::Buy ? mid - offset : mid + offset;
    }

    bool buildCommand(EngineCommand& out) {
        switch (role_) {
            case Role::Maker: return buildQuote(out);
            case Role::Taker: return buildTake(out);
            case Role::Hybrid: return rng_.below(4) == 0 ? buildTake(out) : (buildQuote(out) || buildTake(out));
        }
        return false;
    }

    // 做市：从随机槽位开始找第一个可操作的槽位
    bool buildQuote(EngineCommand& out) {
        const std::size_t n = slots_.size();
        const std::size_t per_symbol = 2 * settings_.levels;
        std::size_t idx = static_cast<std::size_t>(rng_.below(n));
        for (std::size_t k = 0; k < n; ++k, idx = idx + 1 == n ? 0 : idx + 1) {
            QuoteSlot& q = slots_[idx];
            if (q.pending) continue;
            const SymbolId s = static_cast<SymbolId>(idx / per_symbol);
            const std::size_t within = idx % per_symbol;
            const Side side = within < settings_.levels ? Side::Buy : Side::Sell;
            const std::size_t level = within % settings_.levels;
            const Price target = targetPrice(s, side, level);
            const std::uint64_t seq = stats_.sent + 1;
            if (!q.live) {  // 空槽位：挂新报价
                OrderRequest r;
                r.id = makeOrderId(id_, idx, ++order_counter_);
                r.account = maker_account_;
                r.side = side;
                r.type = OrderType::Limit;
                r.tif = TimeInForce::GTC;
                r.price = target;
                r.qty = rng_.range(5, 50);
                r.post_only = true;
                q.id = r.id;
                q.pending = true;
                out = EngineCommand::newOrder(id_, seq, s, r);
                ++stats_.sent_by_type[0];
                ++stats_.quotes;
                return true;
            }
            if (q.price != target) {  // 中间价移动：改单挪到新价位（cancel/replace，同时刷新数量）
                q.pending = true;
                out = EngineCommand::modify(id_, seq, s, q.id, target, rng_.range(5, 50));
                ++stats_.sent_by_type[2];
                return true;
            }
            const std::uint64_t roll = rng_.below(8);
            if (roll == 0) {  // 撤单（之后会在新价位重新报价）
                q.pending = true;
                out = EngineCommand::cancel(id_, seq, s, q.id);
                ++stats_.sent_by_type[1];
                return true;
            }
            if (roll == 1 && q.remaining > 1) {  // 原地减量（保留时间优先级）
                q.pending = true;
                out = EngineCommand::modify(id_, seq, s, q.id, q.price, rng_.range(1, q.remaining - 1));
                ++stats_.sent_by_type[2];
                return true;
            }
        }
        return false;
    }

    // 吃单：IOC 限价（越过对手最优价 0~2 tick）或市价单
    bool buildTake(EngineCommand& out) {
        const SymbolId s = static_cast<SymbolId>(rng_.below(settings_.symbols));
        const Price mid = midOf(s);
        OrderRequest r;
        r.id = makeOrderId(id_, 0xFFFF, ++order_counter_);
        r.account = taker_account_;
        r.side = (rng_.next() & 1) != 0 ? Side::Buy : Side::Sell;
        const std::uint64_t size_roll = rng_.below(100);
        r.qty = size_roll < 70 ? rng_.range(1, 10) : size_roll < 95 ? rng_.range(11, 40) : rng_.range(41, 150);
        if (rng_.below(100) < 70) {
            const Price reach = 1 + static_cast<Price>(rng_.below(3));
            r.type = OrderType::Limit;
            r.tif = TimeInForce::IOC;
            r.price = r.side == Side::Buy ? mid + reach : mid - reach;
            ++stats_.iocs;
        } else {
            r.type = OrderType::Market;
            r.tif = TimeInForce::IOC;
            r.price = 0;
            ++stats_.markets;
        }
        out = EngineCommand::newOrder(id_, stats_.sent + 1, s, r);
        ++stats_.sent_by_type[0];
        return true;
    }

    // 本网关某个报价槽位的当前订单；不是（吃单、旧订单）则返回 nullptr
    QuoteSlot* slotFor(OrderId id) noexcept {
        if ((id >> 48) != static_cast<OrderId>(id_) + 1) return nullptr;
        const std::size_t slot = static_cast<std::size_t>((id >> 32) & 0xFFFF);
        if (slot >= slots_.size()) return nullptr;
        QuoteSlot& q = slots_[slot];
        if (q.id != id) {
            ++stats_.stale_events;
            return nullptr;
        }
        return &q;
    }

    bool drain() {
        bool any = false;
        while (const EngineEvent* ev = out_.front()) {
            onEvent(*ev);
            out_.pop();
            any = true;
        }
        return any;
    }

    void onEvent(const EngineEvent& ev) {
        const std::size_t type = static_cast<std::size_t>(ev.type);
        if (type < 5) ++stats_.events_by_type[type];
        if (ev.gateway == id_ && ev.engine_seq != last_acked_seq_) {
            // 本网关下一条命令的第一个事件：确认 + 往返延迟
            const std::uint64_t now = hft::rdtsc();
            last_acked_seq_ = ev.engine_seq;
            if (ev.client_ts != sent_ts_[stats_.acked & ring_mask_]) ++stats_.echo_mismatches;
            // toNs 把差值按有符号数解释：跨核 TSC 的微小偏差得到小负数，钳制为 0
            const std::int64_t ns = tsc_.toNs(now - ev.client_ts);
            latency_.record(ns <= 0 ? 0 : static_cast<std::uint64_t>(ns));
            ++stats_.acked;
        }
        QuoteSlot* q = slots_.empty() ? nullptr : slotFor(ev.order_id);
        switch (ev.type) {
            case EventType::Accepted:
                if (q != nullptr) {
                    q->live = true;
                    q->pending = false;
                    q->price = ev.accepted.price;
                    q->remaining = ev.accepted.qty;
                }
                break;
            case EventType::Rejected: {
                const std::size_t reason = static_cast<std::size_t>(ev.rejected.reason);
                if (reason < 8) ++stats_.reject_reasons[reason];
                if (q != nullptr) {
                    q->pending = false;
                    // 新报价被拒（例如 PostOnlyWouldCross）；撤 / 改单 UnknownOrder：订单已先一步成交
                    if (ev.rejected.request == RequestType::New || ev.rejected.reason == RejectReason::UnknownOrder)
                        q->live = false;
                }
                break;
            }
            case EventType::Fill:
                if (ev.liquidity == Liquidity::Maker) {
                    ++stats_.maker_fills;
                    stats_.maker_qty += static_cast<std::uint64_t>(ev.fill.qty);
                    if (q != nullptr) {
                        q->remaining = ev.fill.remaining;
                        if (q->remaining == 0) q->live = false;
                    }
                } else {
                    ++stats_.taker_fills;
                    stats_.taker_qty += static_cast<std::uint64_t>(ev.fill.qty);
                }
                break;
            case EventType::Cancelled: {
                const std::size_t reason = static_cast<std::size_t>(ev.cancelled.reason);
                if (reason < 5) ++stats_.cancel_reasons[reason];
                if (q != nullptr) {
                    q->live = false;
                    if (ev.gateway == id_) q->pending = false;  // 对本网关撤单命令的回应
                }
                break;
            }
            case EventType::Modified:
                if (q != nullptr) {
                    q->pending = false;
                    q->price = ev.modified.price;
                    q->remaining = ev.modified.qty;
                }
                break;
        }
    }

    const GatewayId id_;
    const Role role_;
    const std::uint64_t quota_;
    const SimSettings settings_;
    const Market& market_;
    Runner::CommandQueue& in_;
    Runner::EventQueue& out_;
    Rng rng_;
    const int cpu_;
    const hft::TscClock& tsc_;
    const AccountId maker_account_;
    const AccountId taker_account_;
    const Price half_spread_;
    std::vector<QuoteSlot> slots_;      // [symbol][side][level]
    std::vector<std::uint64_t> sent_ts_;  // 在途命令的 client_ts（环形，容量 >= window）
    std::uint64_t ring_mask_ = 0;
    std::uint64_t order_counter_ = 0;
    std::uint64_t last_acked_seq_ = 0;
    std::int64_t finish_ns_ = 0;
    bool pinned_ = false;
    TraderStats stats_;
    hft::LatencyHistogram latency_;
};

// ============================================================================
// 报告
// ============================================================================

void printLatencyHeader() {
    std::printf("  %-8s %-7s %12s %7s %7s %7s %7s %8s %9s %8s\n", "gateway", "role", "samples", "min", "p50", "p90",
                "p99", "p99.9", "max", "mean");
}

void printLatencyRow(const std::string& label, const char* role, const hft::LatencyHistogram& h) {
    std::printf("  %-8s %-7s %12s %7" PRIu64 " %7" PRIu64 " %7" PRIu64 " %7" PRIu64 " %8" PRIu64 " %9" PRIu64
                " %8.0f\n",
                label.c_str(), role, withCommas(h.count()).c_str(), h.min(), h.percentile(0.50), h.percentile(0.90),
                h.percentile(0.99), h.percentile(0.999), h.max(), h.mean());
}

double perSecond(std::uint64_t n, double seconds) { return seconds > 0 ? static_cast<double>(n) / seconds : 0.0; }

std::string rate(std::uint64_t n, double seconds) {
    char buf[32];
    const double r = perSecond(n, seconds);
    if (r >= 1e6) std::snprintf(buf, sizeof(buf), "%.2f M/s", r / 1e6);
    else if (r >= 1e3) std::snprintf(buf, sizeof(buf), "%.1f k/s", r / 1e3);
    else std::snprintf(buf, sizeof(buf), "%.0f /s", r);
    return buf;
}

// "PostOnlyWouldCross 123, UnknownOrder 45"（只列非零项）
template <class Enum, std::size_t N>
std::string breakdown(const std::uint64_t (&counts)[N]) {
    std::string s;
    for (std::size_t i = 0; i < N; ++i) {
        if (counts[i] == 0) continue;
        if (!s.empty()) s += ", ";
        s += std::string(toString(static_cast<Enum>(i))) + " " + withCommas(counts[i]);
    }
    return s.empty() ? "none" : s;
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    const int parsed = parseOptions(argc, argv, opt);
    if (parsed == 1) return 0;
    if (parsed != 0) return 2;

    const std::size_t G = opt.gateways;
    std::size_t makers = opt.makers != SIZE_MAX ? opt.makers : (G + 1) / 2;
    if (G == 1) makers = 1;  // 单网关：hybrid
    const std::vector<int> cpus = allowedCpus();
    const std::uint32_t yield_after =
        opt.yield_after >= 0 ? static_cast<std::uint32_t>(opt.yield_after) : (G + 1 <= cpus.size() ? 0u : 64u);

    // 预留容量：只有报价会挂单（吃单都是 IOC / 市价），每个交易对的活动订单不超过 做市网关数 x 2 x levels。
    // 留 4 倍余量并取 2 的幂（至少 4096），保证撮合热路径零分配（运行结束时报告实际分配次数）。
    const std::size_t live_bound = makers * 2 * opt.levels;
    const std::size_t reserve = std::max<std::size_t>(4096, hft::nextPow2(4 * live_bound));
    const std::size_t inbound_capacity = std::max<std::size_t>(64, hft::nextPow2(2 * opt.window));
    const std::size_t outbound_capacity = 8192;

    hft::TscClock::instance();  // 校准 TSC（约 20 ms），在任何计时之前完成

    EngineRunnerConfig cfg;
    cfg.gateways = G;
    cfg.inbound_capacity = inbound_capacity;
    cfg.outbound_capacity = outbound_capacity;
    cfg.batch = opt.batch;
    cfg.idle_spins_before_yield = yield_after;
    cfg.engine_cpu = opt.pin ? cpus[0] : -1;
    Runner runner(cfg);
    for (std::size_t s = 0; s < opt.symbols; ++s) runner.addSymbol(symbolName(s), 0, reserve);

    const Market market(opt.symbols, opt.seed);
    const SimSettings settings{opt.symbols, opt.levels, opt.window, yield_after};
    std::vector<std::unique_ptr<Trader>> traders;
    traders.reserve(G);
    Rng seeder(opt.seed);
    for (std::size_t g = 0; g < G; ++g) {
        const Role role = G == 1 ? Role::Hybrid : (g < makers ? Role::Maker : Role::Taker);
        const std::uint64_t quota = opt.orders / G + (g < opt.orders % G ? 1 : 0);
        const int cpu = opt.pin ? cpus[(g + 1) % cpus.size()] : -1;
        traders.push_back(std::make_unique<Trader>(static_cast<GatewayId>(g), role, quota, seeder.next(), settings,
                                                   market, runner, cpu));
    }

    auto plural = [](std::size_t n, const char* word) {
        return std::to_string(n) + " " + word + (n == 1 ? "" : "s");
    };
    std::size_t n_makers = 0, n_takers = 0;
    for (const auto& t : traders) {
        if (t->role() == Role::Maker) ++n_makers;
        if (t->role() == Role::Taker) ++n_takers;
    }
    const std::string roles =
        G == 1 ? std::string("hybrid maker/taker") : plural(n_makers, "maker") + ", " + plural(n_takers, "taker");
    std::printf("exchange_sim: %s (%s), %s, %s commands, seed %" PRIu64 "\n", plural(G, "gateway").c_str(),
                roles.c_str(), plural(opt.symbols, "symbol").c_str(), withCommas(opt.orders).c_str(), opt.seed);
    std::printf("  window %zu in flight/gateway, engine batch %zu, queues in %zu / out %zu, reserve %s orders/symbol\n",
                opt.window, opt.batch, runner.inbound(0).capacity(), runner.outbound(0).capacity(),
                withCommas(reserve).c_str());
    std::printf("  %s, %s, %zu CPU%s available, TSC %.4f ns/tick\n",
                yield_after == 0 ? "busy-spin" : ("yield after " + std::to_string(yield_after) + " idle spins").c_str(),
                opt.pin ? "threads pinned" : "no pinning", cpus.size(), cpus.size() == 1 ? "" : "s",
                hft::TscClock::instance().nsPerTick());
    if (G + 1 > cpus.size())
        std::printf("  note: %zu busy threads share %zu CPUs; latency tails include scheduler preemption\n", G + 1,
                    cpus.size());

    // ---- 运行 ----
    runner.start();
    StartGate gate;
    std::vector<std::thread> threads;
    threads.reserve(G);
    for (auto& t : traders) {
        Trader* tp = t.get();
        threads.emplace_back([tp, &gate] { tp->run(gate); });
    }
    while (gate.ready.load(std::memory_order_acquire) < G) std::this_thread::yield();
    const std::uint64_t allocations_before = g_heap_allocations.load(std::memory_order_relaxed);
    const std::int64_t t0 = hft::nowNs();
    gate.go.store(true, std::memory_order_release);
    while (gate.done.load(std::memory_order_acquire) < G) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const std::uint64_t allocations = g_heap_allocations.load(std::memory_order_relaxed) - allocations_before;
    runner.stop();  // 排空（此时没有在途命令）
    gate.stopped.store(true, std::memory_order_release);
    for (std::thread& th : threads) th.join();

    std::int64_t t1 = t0;
    for (const auto& t : traders) t1 = std::max(t1, t->finishNs());
    const double seconds = static_cast<double>(t1 - t0) * 1e-9;

    // ---- 汇总 ----
    TraderStats total;
    hft::LatencyHistogram all_latency;
    for (const auto& t : traders) {
        const TraderStats& s = t->stats();
        total.sent += s.sent;
        total.acked += s.acked;
        for (int i = 0; i < 3; ++i) total.sent_by_type[i] += s.sent_by_type[i];
        total.quotes += s.quotes;
        total.iocs += s.iocs;
        total.markets += s.markets;
        for (int i = 0; i < 5; ++i) total.events_by_type[i] += s.events_by_type[i];
        total.maker_fills += s.maker_fills;
        total.taker_fills += s.taker_fills;
        total.maker_qty += s.maker_qty;
        total.taker_qty += s.taker_qty;
        for (int i = 0; i < 8; ++i) total.reject_reasons[i] += s.reject_reasons[i];
        for (int i = 0; i < 5; ++i) total.cancel_reasons[i] += s.cancel_reasons[i];
        total.echo_mismatches += s.echo_mismatches;
        total.stale_events += s.stale_events;
        all_latency.merge(t->latency());
    }
    std::uint64_t events_received = 0;
    for (int i = 0; i < 5; ++i) events_received += total.events_by_type[i];
    std::uint64_t book_trades = 0;
    std::size_t book_orders = 0;
    for (SymbolId s = 0; s < opt.symbols; ++s) {
        book_trades += runner.engine().book(s).lastTradeId();
        book_orders += runner.engine().book(s).orderCount();
    }
    const EngineRunnerStats es = runner.stats();

    std::printf("\nThroughput (%.3f s from start until every gateway had all its commands acknowledged)\n", seconds);
    std::printf("  commands  %14s   %s\n", withCommas(total.sent).c_str(), rate(total.sent, seconds).c_str());
    std::printf("  events    %14s   %s\n", withCommas(events_received).c_str(), rate(events_received, seconds).c_str());
    std::printf("  trades    %14s   %s   (%s lots)\n", withCommas(book_trades).c_str(), rate(book_trades, seconds).c_str(),
                withCommas(total.taker_qty).c_str());

    std::printf("\nRound-trip latency, ns (command sent -> first event for it received by the same gateway)\n");
    printLatencyHeader();
    for (const auto& t : traders) printLatencyRow(std::to_string(t->id()), roleName(t->role()), t->latency());
    if (G > 1) printLatencyRow("all", "", all_latency);

    std::printf("\nCommands sent\n");
    std::printf("  New       %14s   (post-only quotes %s, IOC %s, market %s)\n", withCommas(total.sent_by_type[0]).c_str(),
                withCommas(total.quotes).c_str(), withCommas(total.iocs).c_str(), withCommas(total.markets).c_str());
    std::printf("  Cancel    %14s\n", withCommas(total.sent_by_type[1]).c_str());
    std::printf("  Modify    %14s\n", withCommas(total.sent_by_type[2]).c_str());

    std::printf("\nEvents received by type\n");
    std::printf("  Accepted  %14s\n", withCommas(total.events_by_type[0]).c_str());
    std::printf("  Rejected  %14s   (%s)\n", withCommas(total.events_by_type[1]).c_str(),
                breakdown<RejectReason>(total.reject_reasons).c_str());
    std::printf("  Fill      %14s   (maker %s, taker %s)\n", withCommas(total.events_by_type[2]).c_str(),
                withCommas(total.maker_fills).c_str(), withCommas(total.taker_fills).c_str());
    std::printf("  Cancelled %14s   (%s)\n", withCommas(total.events_by_type[3]).c_str(),
                breakdown<CancelReason>(total.cancel_reasons).c_str());
    std::printf("  Modified  %14s\n", withCommas(total.events_by_type[4]).c_str());

    std::printf("\nEngine\n");
    std::printf("  commands processed %s, events published %s, outbound-full stalls %s, idle polls %s\n",
                withCommas(es.commands).c_str(), withCommas(es.events).c_str(), withCommas(es.outbound_full).c_str(),
                withCommas(es.idle_polls).c_str());
    std::printf("  heap allocations while trading (all threads): %s\n", withCommas(allocations).c_str());
    if (opt.pin) {
        std::printf("  pinning: engine -> CPU %d (%s)", cfg.engine_cpu, runner.pinned() ? "ok" : "failed");
        for (const auto& t : traders)
            std::printf(", gw %u -> CPU %d (%s)", static_cast<unsigned>(t->id()), t->cpu(), t->pinned() ? "ok" : "failed");
        std::printf("\n");
    }

    std::printf("\nFinal books\n");
    std::printf("  %-10s %10s %10s %7s %7s %7s %8s %10s %10s %12s\n", "symbol", "best bid", "best ask", "spread",
                "bid lvl", "ask lvl", "orders", "bid qty", "ask qty", "trades");
    for (SymbolId s = 0; s < opt.symbols; ++s) {
        const Runner::Book& book = runner.engine().book(s);
        const std::optional<Price> bid = book.bestBid();
        const std::optional<Price> ask = book.bestAsk();
        Qty bid_qty = 0, ask_qty = 0;
        for (const LevelInfo& l : book.depth(Side::Buy)) bid_qty += l.qty;
        for (const LevelInfo& l : book.depth(Side::Sell)) ask_qty += l.qty;
        const std::string spread = bid && ask ? std::to_string(*ask - *bid) : "-";
        std::printf("  %-10s %10s %10s %7s %7zu %7zu %8zu %10s %10s %12s\n", runner.engine().symbolName(s).c_str(),
                    priceText(bid).c_str(), priceText(ask).c_str(), spread.c_str(), book.levelCount(Side::Buy),
                    book.levelCount(Side::Sell), book.orderCount(), withCommas(static_cast<std::uint64_t>(bid_qty)).c_str(),
                    withCommas(static_cast<std::uint64_t>(ask_qty)).c_str(), withCommas(book.lastTradeId()).c_str());
    }

    // ---- 校验 ----
    std::printf("\nChecks\n");
    bool ok = true;
    auto check = [&ok](const char* what, bool pass, const std::string& detail = std::string()) {
        std::printf("  %-58s %s%s%s\n", what, pass ? "OK" : "FAILED", detail.empty() ? "" : "  ", detail.c_str());
        ok = ok && pass;
    };
    {
        std::string why;
        const bool inv = runner.checkInvariants(&why);
        check("book invariants + owner map == resting orders", inv, why);
    }
    check("engine processed every command, no sequence gaps",
          es.commands == opt.orders && total.sent == opt.orders && es.sequence_gaps == 0 && es.orphan_events == 0,
          es.commands == opt.orders ? "" : withCommas(es.commands) + " of " + withCommas(opt.orders));
    check("every command acknowledged in send order (client_ts echo)",
          total.acked == total.sent && total.echo_mismatches == 0 && total.stale_events == 0,
          total.echo_mismatches == 0 ? "" : withCommas(total.echo_mismatches) + " mismatches");
    check("events published == events received", es.events == events_received,
          withCommas(es.events) + " vs " + withCommas(events_received));
    check("maker fills == taker fills == book trades (and quantities)",
          total.maker_fills == total.taker_fills && total.taker_fills == book_trades && total.maker_qty == total.taker_qty);
    {
        bool pass = true;
        std::string why;
        std::size_t live_total = 0;
        for (const auto& t : traders) {
            std::size_t live = 0;
            if (!t->verify(runner, live, &why)) {
                why = "gateway " + std::to_string(t->id()) + ": " + why;
                pass = false;
                break;
            }
            live_total += live;
        }
        if (pass && live_total != book_orders) {
            pass = false;
            why = std::to_string(live_total) + " live quotes vs " + std::to_string(book_orders) + " resting orders";
        }
        check("client order state matches the books", pass, why);
    }
    std::printf("RESULT: %s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
