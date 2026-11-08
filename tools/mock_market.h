//
// mock_market.h
// 模拟交易所的市场仿真：共享的随机游走公允价 + 各交易所独立的噪声与偶发错位；每家交易所一个真实的
// hft::matching::MatchingEngine，由做市商（盘口阶梯报价）、深度提供者（远端流动性）与吃单者驱动。
// Market simulation for the mock exchange: a shared random-walk fair price + independent per-venue noise
// and occasional dislocations; each venue runs a real hft::matching::MatchingEngine, driven by market
// makers (laddered quotes at the top of book), depth providers (distant liquidity) and takers.
//

#pragma once

#include "matching/matching_engine.h"
#include "matching/types.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

// ============================================================================
// 模型概要（全部确定性：同一种子 + 同一步进序列 => 完全相同的订单流与盘口）
// Model overview (fully deterministic: same seed + same step sequence => identical order flow and books)
//
// - 公允价 F：所有交易所共享的算术随机游走（波动率以"每 √秒 多少基点"给出），带极弱的向起始价回归，
//   长时间运行价格也不会漂走。
// - 交易所 v 的本地公允价 = F × (1 + (噪声_v + 错位_v) × 1e-4)：噪声是 OU 过程（几个美元的常态价差）；
//   错位是泊松到达的跳跃（小跳几个基点、大跳 0.28%~0.45%），按指数衰减，并经一阶滤波在约 0.3 s 内展开。
//   大跳超过 0.1% 双边手续费（约 0.2%）的盈亏平衡点，因此 fee_rate = 0.001 时监控也能看到套利信号；
//   fee_rate = 0 时常态噪声本身就足以让跨所盘口经常交叉。
// - 每家交易所的代理（各自独立账户；同账户订单之间启用 STP CancelTaker）：
//   * 做市商 MarketMaker：每侧 L 档阶梯报价（离公允价 half_spread + k × level_gap + 抖动），
//     按泊松频率重新报价；深档更"粘"（容忍 k × gap / 2 的偏离），被吃掉的档位按概率补回。
//     价格上行时先挪卖单、下行时先挪买单，避免新报价撞上自己尚未挪走的旧报价。
//   * 深度提供者 DepthProvider：每侧维持 N 张远端挂单（离公允价 [inner, outer] tick，越靠内越密），
//     越界（带滞回）即撤，按每步上限补足，另有随机改量/撤单，给深档带来持续的增量变化。
//   * 吃单者 Taker：泊松到达的 IOC 限价单 / 市价单，方向偏向"让盘口回到本地公允价"，偶有大单扫簿。
// - 订单 id 编码"代理 / 槽位 / 序号"：撮合引擎的监听器（Venue 本身）据此在 O(1) 内把成交、撤单、改单回报
//   同步到槽位，槽位因此总与引擎一致（测试逐个核对）。监听器只更新槽位与统计，从不在回调里调用
//   submit/cancel/modify（引擎的要求）。
// - Fair price F: an arithmetic random walk shared by all venues (volatility given as "basis points per
//   √second"), with a very weak reversion toward the start price, so the price does not drift away even
//   over long runs.
// - Local fair price of venue v = F × (1 + (noise_v + dislocation_v) × 1e-4): the noise is an OU process
//   (a baseline spread of a few dollars); dislocations are Poisson-arriving jumps (small jumps of a few
//   basis points, large jumps of 0.28%~0.45%) that decay exponentially and unfold over about 0.3 s through
//   a first-order filter. Large jumps exceed the break-even point of the 0.1% fee on both sides (about
//   0.2%), so the monitor also sees arbitrage signals with fee_rate = 0.001; with fee_rate = 0 the baseline
//   noise alone is enough to make the tops of book of different venues cross often.
// - Agents of each venue (each with its own account; STP CancelTaker between orders of the same account):
//   * Market maker (MarketMaker): laddered quotes of L levels per side (half_spread + k × level_gap +
//     jitter away from the fair price), requoted at a Poisson rate; deeper levels are "stickier" (they
//     tolerate a deviation of k × gap / 2), and levels that were taken are refilled with some probability.
//     On a rising price it moves the asks first, on a falling price the bids first, so that a new quote
//     does not run into its own old quote that has not been moved yet.
//   * Depth provider (DepthProvider): keeps N distant resting orders per side ([inner, outer] ticks from
//     the fair price, denser toward the inside), cancels them once out of range (with hysteresis), tops
//     them up within a per-step limit, and also changes quantities / cancels at random, giving the deep
//     levels a steady stream of incremental changes.
//   * Taker: Poisson-arriving IOC limit / market orders, with the direction biased toward "bringing the top
//     of book back to the local fair price"; now and then a large order sweeps the book.
// - Order ids encode "agent / slot / sequence number": from them the matching engine's listener (the Venue
//   itself) syncs fill, cancel and modify reports to the slots in O(1), so the slots always agree with the
//   engine (the tests check them one by one). The listener only updates slots and stats and never calls
//   submit/cancel/modify in a callback (an engine requirement).
// ============================================================================
namespace mock_exchange::sim {

namespace mt = hft::matching;
using mt::AccountId;
using mt::OrderId;
using mt::Price;
using mt::Qty;
using mt::Side;

// 价格、数量都是整数：价格单位 tick = 0.01 USDT，数量单位 lot = 0.000001 BTC
// Prices and quantities are both integers: price unit tick = 0.01 USDT, quantity unit lot = 0.000001 BTC
inline constexpr int kPriceDecimals = 2;
inline constexpr Price kTicksPerUnit = 100;
inline constexpr int kQtyDecimals = 6;
inline constexpr Qty kLotsPerUnit = 1000000;

// splitmix64：由主种子派生各子种子（相邻种子也得到互不相关的序列）
// splitmix64: derives the sub-seeds from the master seed (adjacent seeds also give uncorrelated sequences)
constexpr std::uint64_t splitmix64(std::uint64_t x) noexcept {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// 确定性随机数：std::mt19937_64 的输出序列由标准规定；分布变换自己实现
// （标准库各分布的算法由实现决定，换标准库就会得到不同的序列）
// Deterministic random numbers: the standard specifies the output sequence of std::mt19937_64; the
// distribution transforms are implemented here
// (the algorithms of the standard library's distributions are implementation-defined, so switching
// standard libraries would give different sequences)
class Random {
public:
    explicit Random(std::uint64_t seed) : engine_(seed) {}

    // [0, 1)
    double uniform() { return static_cast<double>(engine_() >> 11) * 0x1.0p-53; }
    double uniform(double lo, double hi) { return lo + (hi - lo) * uniform(); }

    // [lo, hi] 上的均匀整数（取模偏差 < 2^-40，可忽略）
    // Uniform integer on [lo, hi] (modulo bias < 2^-40, negligible)
    std::int64_t uniformInt(std::int64_t lo, std::int64_t hi) {
        if (hi <= lo) return lo;
        const std::uint64_t span = static_cast<std::uint64_t>(hi - lo) + 1;
        return lo + static_cast<std::int64_t>(engine_() % span);
    }

    bool chance(double p) { return uniform() < p; }

    // 标准正态（Box-Muller，成对生成，第二个值缓存到下一次调用）
    // Standard normal (Box-Muller, generated in pairs, the second value cached for the next call)
    double normal() {
        if (has_spare_) {
            has_spare_ = false;
            return spare_;
        }
        double u1 = uniform();
        while (u1 <= 0.0) u1 = uniform();
        const double u2 = uniform();
        const double r = std::sqrt(-2.0 * std::log(u1));
        const double theta = 6.283185307179586 * u2;
        spare_ = r * std::sin(theta);
        has_spare_ = true;
        return r * std::cos(theta);
    }

private:
    std::mt19937_64 engine_;
    double spare_ = 0.0;
    bool has_spare_ = false;
};

// ---------------------------------------------------------------------------
// 参数
// Parameters
// ---------------------------------------------------------------------------

struct FairValueParams {
    double start_price = 65000.0;            // USDT
    double volatility_bps = 0.4;             // 每 √秒 的标准差（基点；0.4 bp/√s 约合日波动 1.2%）
                                             // standard deviation per √second (basis points;
                                             // 0.4 bp/√s is about 1.2% daily volatility)
    double reversion_per_s = 1.0 / 1800.0;   // 向起始价的弱均值回归
                                             // weak mean reversion toward the start price
};

struct OffsetParams {
    double noise_bps = 0.4;                  // 常态噪声（OU 过程）的平稳标准差
                                             // stationary standard deviation of the baseline noise (OU process)
    double noise_reversion_per_s = 0.8;
    double small_jump_rate_per_s = 0.12;     // 小错位的泊松到达率
                                             // Poisson arrival rate of small dislocations
    double small_jump_min_bps = 1.5;
    double small_jump_max_bps = 6.0;
    double large_jump_rate_per_s = 1.0 / 45.0;   // 大错位：扣除 0.1% 双边手续费后仍有利润
                                                 // large dislocations: still profitable after
                                                 // the 0.1% fee on both sides
    double large_jump_min_bps = 28.0;
    double large_jump_max_bps = 45.0;
    double jump_decay_s = 1.6;               // 错位目标的指数衰减时间常数
                                             // exponential decay time constant of the dislocation target
    double jump_rise_s = 0.15;               // 实际错位追随目标的时间常数
                                             // time constant of the actual dislocation following the target
};

struct MakerParams {
    int levels = 8;                 // 每侧档数
                                    // levels per side
    Price half_spread = 1;          // 第 0 档离公允价的 tick 数（>= 1，保证自己的买卖报价不交叉）
                                    // ticks between level 0 and the fair price
                                    // (>= 1, so its own bid and ask never cross)
    Price level_gap = 3;            // 相邻档的间距（tick）
                                    // spacing between adjacent levels (ticks)
    Price jitter = 2;               // 每次挪单时随机外移 [0, jitter] tick
                                    // random outward shift of [0, jitter] ticks on each move
    Qty base_size = 20000;          // 第 0 档的典型数量（lot）
                                    // typical quantity at level 0 (lots)
    double size_growth = 0.5;       // 每深一档数量增加的比例
                                    // fraction by which the quantity grows per level deeper
    double requote_rate_per_s = 12; // 重新报价的泊松频率
                                    // Poisson rate of requotes
    double refill_prob = 0.8;       // 被吃光的档位在一次重新报价中补回的概率
                                    // probability that a fully taken level is refilled in one requote
    double resize_prob = 0.05;      // 未挪动的档位随机改量的概率
                                    // probability of a random quantity change for a level that is not moved
};

struct DepthParams {
    int orders_per_side = 1000;     // 每侧维持的挂单数
                                    // resting orders kept per side
    Price inner = 30;               // 离公允价最近的距离（tick）
                                    // closest distance from the fair price (ticks)
    Price outer = 3500;             // 最远的距离（tick）
                                    // farthest distance (ticks)
    double inner_bias = 1.5;        // 距离 = inner + (outer - inner) × u^bias：越大越集中在内侧
                                    // distance = inner + (outer - inner) × u^bias:
                                    // larger values concentrate more on the inside
    Qty base_size = 40000;          // 典型数量（lot），越深越大
                                    // typical quantity (lots), growing with depth
    int max_adds_per_step = 40;     // 每步每侧最多补单数
                                    // maximum orders added per side per step
    double churn_per_s = 40;        // 每秒随机改量/撤单次数
                                    // random quantity changes/cancels per second
};

struct TakerParams {
    double rate_per_s = 12;         // 主动单的泊松频率
                                    // Poisson rate of aggressive orders
    Qty median_size = 6000;         // 数量中位数（lot），对数正态
                                    // median quantity (lots), log-normal
    double size_sigma = 1.1;
    double whale_prob = 0.02;       // 大单概率
                                    // probability of a large order
    Qty whale_min = 300000;
    Qty whale_max = 2500000;
    double market_prob = 0.1;       // 市价单比例（其余为 IOC 限价单）
                                    // share of market orders (the rest are IOC limit orders)
    Price max_slippage = 200;       // IOC 限价最多越过对手最优价的 tick 数
                                    // max ticks an IOC limit price goes past the opposite best price
    double pressure = 0.7;          // 方向偏置强度：0 = 随机，1 = 完全由"本地公允价 - 盘口中价"决定
                                    // direction bias strength: 0 = random,
                                    // 1 = decided entirely by "local fair price - top-of-book mid"
    double pressure_scale = 30;     // 偏置的价差尺度（tick）
                                    // price-gap scale of the bias (ticks)
};

struct VenueParams {
    std::string name;
    OffsetParams offset;
    std::vector<MakerParams> makers;
    DepthParams depth;
    TakerParams taker;
};

struct MarketParams {
    FairValueParams fair;
    std::vector<VenueParams> venues;
    double step_s = 0.005;               // 仿真步长（秒）
                                         // simulation step (seconds)
};

// 默认的三家交易所（顺序固定：Binance、OKX、Bybit，与 Exchange 枚举一致）。
// "性格"各不相同：Binance 价差最窄、深度最深；OKX 居中；Bybit 价差稍宽、深度较浅、错位更频繁。
// 深度提供者的挂单数保证每侧档位数超过各自的发布窗口（1000 / 400 / 50），窗口边界上的进出也被覆盖。
// The three default venues (fixed order: Binance, OKX, Bybit, matching the Exchange enum).
// Their "personalities" differ: Binance has the tightest spread and the deepest book; OKX sits in the
// middle; Bybit has a slightly wider spread, a shallower book and more frequent dislocations.
// The depth providers' order counts keep the levels per side above each venue's publish window
// (1000 / 400 / 50), so levels entering and leaving at the window edge are covered too.
inline MarketParams defaultMarketParams() {
    MarketParams m;
    auto maker = [](int levels, Price half_spread, Price gap, Qty size, double rate) {
        MakerParams p;
        p.levels = levels;
        p.half_spread = half_spread;
        p.level_gap = gap;
        p.base_size = size;
        p.requote_rate_per_s = rate;
        return p;
    };

    VenueParams binance;
    binance.name = "Binance";
    binance.makers = {maker(10, 1, 2, 8000, 14), maker(10, 2, 3, 25000, 10), maker(8, 4, 5, 60000, 6)};
    binance.depth.orders_per_side = 1500;
    binance.depth.inner = 25;
    binance.depth.outer = 3500;
    binance.taker.rate_per_s = 18;

    VenueParams okx;
    okx.name = "OKX";
    okx.makers = {maker(8, 1, 3, 10000, 12), maker(8, 3, 5, 40000, 8), maker(6, 5, 6, 80000, 5)};
    okx.depth.orders_per_side = 900;
    okx.depth.inner = 35;
    okx.depth.outer = 4000;
    okx.taker.rate_per_s = 12;

    VenueParams bybit;
    bybit.name = "Bybit";
    bybit.makers = {maker(10, 2, 2, 12000, 12), maker(8, 3, 4, 50000, 7)};
    bybit.depth.orders_per_side = 500;
    bybit.depth.inner = 40;
    bybit.depth.outer = 3000;
    bybit.taker.rate_per_s = 10;
    bybit.offset.noise_bps = 0.5;
    bybit.offset.small_jump_rate_per_s = 0.18;
    bybit.offset.large_jump_rate_per_s = 1.0 / 35.0;

    m.venues = {binance, okx, bybit};
    return m;
}

// ---------------------------------------------------------------------------
// 价格过程
// Price process
// ---------------------------------------------------------------------------

class FairValue {
public:
    explicit FairValue(const FairValueParams& p) : p_(p), price_(p.start_price) {}

    void step(double dt, Random& rng) {
        const double sigma = p_.start_price * p_.volatility_bps * 1e-4;  // USDT / √s
        price_ += -p_.reversion_per_s * (price_ - p_.start_price) * dt + sigma * std::sqrt(dt) * rng.normal();
    }

    double price() const noexcept { return price_; }

private:
    FairValueParams p_;
    double price_;
};

// 交易所偏移（基点）：OU 噪声 + 泊松跳跃错位（目标值指数衰减，实际值一阶追随目标）
// Venue offset (basis points): OU noise + Poisson-jump dislocation (the target decays exponentially, and
// the actual value follows the target as a first-order lag)
class OffsetProcess {
public:
    explicit OffsetProcess(const OffsetParams& p) : p_(p) {}

    void step(double dt, Random& rng) {
        const double a = std::exp(-p_.noise_reversion_per_s * dt);
        noise_ = noise_ * a + p_.noise_bps * std::sqrt(1.0 - a * a) * rng.normal();
        target_ *= std::exp(-dt / p_.jump_decay_s);
        if (rng.chance(p_.small_jump_rate_per_s * dt)) {
            target_ += signedMagnitude(rng, p_.small_jump_min_bps, p_.small_jump_max_bps);
        }
        if (rng.chance(p_.large_jump_rate_per_s * dt)) {
            target_ += signedMagnitude(rng, p_.large_jump_min_bps, p_.large_jump_max_bps);
            ++large_jumps_;
        }
        disloc_ += (target_ - disloc_) * (1.0 - std::exp(-dt / p_.jump_rise_s));
    }

    double bps() const noexcept { return noise_ + disloc_; }
    std::uint64_t largeJumps() const noexcept { return large_jumps_; }

private:
    static double signedMagnitude(Random& rng, double lo, double hi) {
        const double m = rng.uniform(lo, hi);
        return rng.chance(0.5) ? m : -m;
    }

    OffsetParams p_;
    double noise_ = 0.0;
    double target_ = 0.0;
    double disloc_ = 0.0;
    std::uint64_t large_jumps_ = 0;
};

// ---------------------------------------------------------------------------
// 订单 id 编码：代理（8 位）| 槽位（20 位）| 序号（36 位）
// Order id encoding: agent (8 bits) | slot (20 bits) | sequence number (36 bits)
// ---------------------------------------------------------------------------

inline constexpr unsigned kSerialBits = 36;
inline constexpr unsigned kSlotBits = 20;
inline constexpr std::uint32_t kNoSlot = (1u << kSlotBits) - 1;  // 不占槽位的订单（吃单者）
                                                                 // orders without a slot (takers)

constexpr OrderId makeOrderId(std::uint32_t agent, std::uint32_t slot, std::uint64_t serial) noexcept {
    return (OrderId{agent} << (kSlotBits + kSerialBits)) | (OrderId{slot & kNoSlot} << kSerialBits) |
           (serial & ((OrderId{1} << kSerialBits) - 1));
}
constexpr std::uint32_t agentOf(OrderId id) noexcept {
    return static_cast<std::uint32_t>(id >> (kSlotBits + kSerialBits));
}
constexpr std::uint32_t slotOf(OrderId id) noexcept {
    return static_cast<std::uint32_t>((id >> kSerialBits) & kNoSlot);
}

// 一个报价槽位：同一时刻最多一张活动订单
// A quote slot: at most one live order at a time
struct Slot {
    OrderId id = 0;      // 当前订单（live 时有效）
                         // current order (valid while live)
    Price price = 0;
    Qty qty = 0;         // 剩余数量（成交回报同步）
                         // remaining quantity (synced from fill reports)
    Side side = Side::Buy;
    bool live = false;
};

struct VenueStats {
    std::uint64_t book_events = 0;       // 受理 / 成交 / 撤单 / 改单回调总数（推进各协议的更新号）
                                         // total accept / fill / cancel / modify callbacks
                                         // (advances each protocol's update ids)
    std::uint64_t trades = 0;
    std::uint64_t rejects = 0;           // 引擎拒单（代理只发合法请求，正常应为 0）
                                         // engine rejects (agents only send valid requests, so normally 0)
    Qty volume = 0;                      // 成交量（lot）
                                         // traded volume (lots)
};

class Venue;

// ---------------------------------------------------------------------------
// 代理
// Agents
// ---------------------------------------------------------------------------

class Agent {
public:
    Agent(std::uint32_t index, AccountId account, std::size_t slots)
        : index_(index), account_(account), slots_(slots) {}
    virtual ~Agent() = default;
    Agent(const Agent&) = delete;
    Agent& operator=(const Agent&) = delete;

    virtual void step(Venue& venue, double dt) = 0;

    // 监听器：订单的剩余量变化（<= 0 表示订单已不在簿上）
    // Listener: an order's remaining quantity changed (<= 0 means the order is no longer in the book)
    void onRemaining(std::uint32_t slot, OrderId id, Qty remaining) noexcept {
        if (Slot* s = liveSlot(slot, id)) {
            if (remaining <= 0) {
                s->live = false;
            } else {
                s->qty = remaining;
            }
        }
    }

    void onModified(std::uint32_t slot, OrderId id, Price price, Qty qty) noexcept {
        if (Slot* s = liveSlot(slot, id)) {
            s->price = price;
            s->qty = qty;
        }
    }

    const std::vector<Slot>& slots() const noexcept { return slots_; }
    AccountId account() const noexcept { return account_; }

protected:
    // 下单 / 改单 / 撤单（调用方保证槽位状态与参数合法）；槽位随后由监听器按引擎回报更新
    // Place / modify / cancel (the caller ensures the slot state and arguments are valid); the listener then
    // updates the slot from the engine's reports
    inline void place(Venue& v, std::uint32_t slot, Side side, Price price, Qty qty);
    inline void move(Venue& v, std::uint32_t slot, Price price, Qty qty);
    inline void cancel(Venue& v, std::uint32_t slot);
    OrderId nextTakerId() noexcept { return makeOrderId(index_, kNoSlot, ++serial_); }

    std::uint32_t index_;
    AccountId account_;
    std::vector<Slot> slots_;   // 构造后大小固定（引用稳定）
                                // size fixed after construction (stable references)
    std::uint64_t serial_ = 0;

private:
    Slot* liveSlot(std::uint32_t slot, OrderId id) noexcept {
        return slot < slots_.size() && slots_[slot].live && slots_[slot].id == id ? &slots_[slot] : nullptr;
    }
};

class MarketMaker final : public Agent {
public:
    MarketMaker(std::uint32_t index, AccountId account, const MakerParams& p)
        : Agent(index, account, static_cast<std::size_t>(p.levels) * 2), p_(p), jitter_(slots_.size(), 0) {}

    inline void step(Venue& v, double dt) override;

private:
    std::uint32_t slotIndex(Side side, int level) const noexcept {
        return static_cast<std::uint32_t>(side == Side::Buy ? level : p_.levels + level);
    }

    inline Price target(Side side, int level, double fair) const noexcept;
    inline Qty size(int level, Random& rng) const;
    inline void quoteSide(Venue& v, Side side, double fair);

    MakerParams p_;
    std::vector<Price> jitter_;   // 每个槽位当前的随机外移
                                  // current random outward shift of each slot
    double last_fair_ = 0.0;
};

class DepthProvider final : public Agent {
public:
    DepthProvider(std::uint32_t index, AccountId account, const DepthParams& p)
        : Agent(index, account, static_cast<std::size_t>(p.orders_per_side) * 2), p_(p) {
        free_.reserve(slots_.size());
    }

    inline void step(Venue& v, double dt) override;

private:
    inline Price placementPrice(Side side, Price floor_fair, Price ceil_fair, Random& rng) const;

    DepthParams p_;
    std::vector<std::uint32_t> free_;   // 每步重建的空闲槽位表（复用容量）
                                        // free slot list rebuilt every step (capacity reused)
};

class Taker final : public Agent {
public:
    Taker(std::uint32_t index, AccountId account, const TakerParams& p) : Agent(index, account, 0), p_(p) {}

    inline void step(Venue& v, double dt) override;

private:
    TakerParams p_;
};

// ---------------------------------------------------------------------------
// 交易所：撮合引擎 + 代理 + 偏移过程；同时是撮合引擎的监听器
// Venue: matching engine + agents + offset process; also the matching engine's listener
// ---------------------------------------------------------------------------

class Venue {
public:
    using Engine = mt::MatchingEngine<Venue>;
    using Book = Engine::Book;

    static constexpr AccountId kDepthAccount = 100;
    static constexpr AccountId kMakerAccountBase = 1;
    static constexpr AccountId kTakerAccount = 200;
    static constexpr std::size_t kReserveOrders = 8192;   // 撮合引擎预留的订单容量（活动订单约 3000 张）
                                                          // order capacity reserved by the matching engine
                                                          // (about 3000 live orders)

    Venue(const VenueParams& params, std::uint64_t seed)
        : name_(params.name), rng_(seed), offset_(params.offset), engine_(*this) {
        symbol_ = engine_.addSymbol("BTCUSDT", 0, kReserveOrders);
        std::uint32_t index = 0;
        // 顺序即每步的执行顺序：先撤掉越界的远端挂单，再让做市商重新报价，最后是吃单者
        // The order here is the execution order of each step: first the out-of-range distant resting orders are
        // cancelled, then the market makers requote, and the takers go last
        agents_.push_back(std::make_unique<DepthProvider>(index++, kDepthAccount, params.depth));
        for (std::size_t i = 0; i < params.makers.size(); ++i) {
            agents_.push_back(std::make_unique<MarketMaker>(index++, kMakerAccountBase + static_cast<AccountId>(i),
                                                            params.makers[i]));
        }
        agents_.push_back(std::make_unique<Taker>(index++, kTakerAccount, params.taker));
    }

    Venue(const Venue&) = delete;
    Venue& operator=(const Venue&) = delete;

    // 推进一步：偏移过程 → 本地公允价 → 各代理依次行动
    // Advances one step: offset process → local fair price → each agent acts in turn
    void step(double dt, double market_fair) {
        offset_.step(dt, rng_);
        fair_ticks_ = market_fair * (1.0 + offset_.bps() * 1e-4) * static_cast<double>(kTicksPerUnit);
        for (auto& agent : agents_) agent->step(*this, dt);
    }

    const std::string& name() const noexcept { return name_; }
    double fairTicks() const noexcept { return fair_ticks_; }
    double fairPrice() const noexcept { return fair_ticks_ / static_cast<double>(kTicksPerUnit); }
    std::uint64_t largeDislocations() const noexcept { return offset_.largeJumps(); }

    const Book& book() const { return engine_.book(symbol_); }
    std::vector<mt::LevelInfo> depth(Side side, std::size_t max_levels) const {
        return book().depth(side, max_levels);
    }
    std::uint64_t bookEvents() const noexcept { return stats_.book_events; }
    const VenueStats& stats() const noexcept { return stats_; }

    // 一致性检查（测试用）：引擎不变量；每个槽位的活动订单都在引擎中且价格/剩余量一致；数量相等；无拒单
    // Consistency check (for tests): engine invariants; every slot's live order is in the engine with the same
    // price/remaining quantity; equal counts; no rejects
    bool checkConsistency(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why != nullptr) *why = msg;
            return false;
        };
        std::string inner;
        if (!book().checkInvariants(&inner)) return fail(name_ + ": engine invariant violated: " + inner);
        std::size_t live = 0;
        for (const auto& a : agents_) {
            for (const Slot& s : a->slots()) {
                if (!s.live) continue;
                ++live;
                const std::optional<mt::OrderInfo> info = book().findOrder(s.id);
                if (!info) return fail(name_ + ": slot order " + std::to_string(s.id) + " is not in the book");
                if (info->price != s.price || info->remaining != s.qty || info->side != s.side ||
                    info->account != a->account()) {
                    return fail(name_ + ": slot order " + std::to_string(s.id) + " differs from the book");
                }
            }
        }
        if (live != book().orderCount()) {
            return fail(name_ + ": agents track " + std::to_string(live) + " orders, the book holds " +
                        std::to_string(book().orderCount()));
        }
        if (stats_.rejects != 0) return fail(name_ + ": " + std::to_string(stats_.rejects) + " engine rejects");
        return true;
    }

    // ---- 代理使用 ----
    // ---- Used by the agents ----
    Engine& engine() noexcept { return engine_; }
    mt::SymbolId symbol() const noexcept { return symbol_; }
    Random& rng() noexcept { return rng_; }

    // ---- 撮合引擎监听器：只更新槽位与统计（不得在回调里调用 submit/cancel/modify）----
    // ---- Matching engine listener: only updates slots and stats (never call submit/cancel/modify in a callback) ----
    void onAccepted(mt::SymbolId, const mt::OrderRequest&) noexcept { ++stats_.book_events; }
    void onRejected(mt::SymbolId, OrderId id, mt::RequestType request, mt::RejectReason) noexcept {
        ++stats_.rejects;
        if (request != mt::RequestType::Modify) remaining(id, 0);
    }
    void onTrade(mt::SymbolId, const mt::Trade& t) noexcept {
        ++stats_.book_events;
        ++stats_.trades;
        stats_.volume += t.qty;
        remaining(t.maker_id, t.maker_remaining);
        remaining(t.taker_id, t.taker_remaining);
    }
    void onCancelled(mt::SymbolId, OrderId id, Qty, mt::CancelReason) noexcept {
        ++stats_.book_events;
        remaining(id, 0);
    }
    void onModified(mt::SymbolId, OrderId id, Price new_price, Qty new_qty) noexcept {
        ++stats_.book_events;
        if (Agent* a = agentFor(id)) a->onModified(slotOf(id), id, new_price, new_qty);
    }

private:
    Agent* agentFor(OrderId id) noexcept {
        const std::uint32_t a = agentOf(id);
        return a < agents_.size() ? agents_[a].get() : nullptr;
    }
    void remaining(OrderId id, Qty qty) noexcept {
        if (Agent* a = agentFor(id)) a->onRemaining(slotOf(id), id, qty);
    }

    std::string name_;
    Random rng_;
    OffsetProcess offset_;
    Engine engine_;
    mt::SymbolId symbol_ = 0;
    double fair_ticks_ = 0.0;
    std::vector<std::unique_ptr<Agent>> agents_;
    VenueStats stats_;
};

// ---------------------------------------------------------------------------
// 市场：共享公允价 + 多家交易所
// Market: shared fair price + several venues
// ---------------------------------------------------------------------------

class Market {
public:
    Market(const MarketParams& params, std::uint64_t seed)
        : step_s_(params.step_s), rng_(splitmix64(seed)), fair_(params.fair) {
        for (std::size_t i = 0; i < params.venues.size(); ++i) {
            venues_.emplace_back(params.venues[i], splitmix64(seed ^ (0x5EED0000ULL + i)));
        }
    }

    void step() {
        fair_.step(step_s_, rng_);
        for (Venue& v : venues_) v.step(step_s_, fair_.price());
        ++steps_;
    }

    // 推进 seconds 秒（按步长取整）
    // Advances by seconds seconds (rounded to a whole number of steps)
    void advance(double seconds) {
        if (!(seconds > 0.0)) return;  // 负数与 NaN：不推进（llround 的结果转无符号会变成巨大的步数）
                                       // negative or NaN: no advance (llround's result
                                       // cast to unsigned would be a huge step count)
        const auto n = static_cast<std::uint64_t>(std::llround(seconds / step_s_));
        for (std::uint64_t i = 0; i < n; ++i) step();
    }

    std::size_t venueCount() const noexcept { return venues_.size(); }
    const Venue& venue(std::size_t i) const { return venues_.at(i); }
    double fair() const noexcept { return fair_.price(); }
    double stepSeconds() const noexcept { return step_s_; }
    double simSeconds() const noexcept { return static_cast<double>(steps_) * step_s_; }

private:
    double step_s_;
    Random rng_;
    FairValue fair_;
    std::deque<Venue> venues_;   // 元素地址稳定（引擎持有 Venue 监听器的引用）
                                 // stable element addresses (the engines hold references to the Venue listeners)
    std::uint64_t steps_ = 0;
};

// ===========================================================================
// 实现：代理的公共操作
// Implementation: common agent operations
// ===========================================================================

inline void Agent::place(Venue& v, std::uint32_t slot, Side side, Price price, Qty qty) {
    Slot& s = slots_[slot];
    s = Slot{makeOrderId(index_, slot, ++serial_), price, qty, side, true};  // 先登记：撮合过程中的回报据此更新槽位
                                                                             // register first: reports during matching
                                                                             // rely on it to update the slot
    // GTC 限价单；STP CancelTaker：不与自己的挂单成交，撞上时撤掉新单
    // GTC limit order; STP CancelTaker: never trades against its own resting orders; on a collision the new
    // order is cancelled
    const mt::OrderRequest req{s.id, account_, side, mt::OrderType::Limit, mt::TimeInForce::GTC, price, qty, false,
                               mt::StpMode::CancelTaker};
    v.engine().submit(v.symbol(), req);
}

inline void Agent::move(Venue& v, std::uint32_t slot, Price price, Qty qty) {
    const Slot& s = slots_[slot];
    if (price != s.price || qty != s.qty) v.engine().modify(v.symbol(), s.id, price, qty);
}

inline void Agent::cancel(Venue& v, std::uint32_t slot) { v.engine().cancel(v.symbol(), slots_[slot].id); }

// ===========================================================================
// 实现：做市商
// Implementation: market maker
// ===========================================================================

inline Price MarketMaker::target(Side side, int level, double fair) const noexcept {
    const Price distance = p_.half_spread + static_cast<Price>(level) * p_.level_gap +
                           jitter_[slotIndex(side, level)];
    return side == Side::Buy ? static_cast<Price>(std::floor(fair)) - distance
                             : static_cast<Price>(std::ceil(fair)) + distance;
}

inline Qty MarketMaker::size(int level, Random& rng) const {
    const double base = static_cast<double>(p_.base_size) * (1.0 + p_.size_growth * level);
    const double q = base * std::exp(0.45 * rng.normal());
    return std::clamp<Qty>(static_cast<Qty>(std::llround(q)), 1, p_.base_size * 100);
}

inline void MarketMaker::quoteSide(Venue& v, Side side, double fair) {
    Random& rng = v.rng();
    for (int k = 0; k < p_.levels; ++k) {
        const std::uint32_t idx = slotIndex(side, k);
        const Slot& s = slots_[idx];
        const Price tolerance = k == 0 ? 0 : static_cast<Price>(k) * p_.level_gap / 2;
        if (!s.live) {
            if (!rng.chance(p_.refill_prob)) continue;
            jitter_[idx] = rng.uniformInt(0, p_.jitter);
            place(v, idx, side, target(side, k, fair), size(k, rng));
        } else if (std::llabs(s.price - target(side, k, fair)) > tolerance) {
            jitter_[idx] = rng.uniformInt(0, p_.jitter);
            move(v, idx, target(side, k, fair), size(k, rng));
        } else if (rng.chance(p_.resize_prob)) {
            move(v, idx, s.price, size(k, rng));  // 改量：变小时原地修改（保留时间优先级）
                                                  // resize: a decrease is modified in place (keeps time priority)
        }
    }
}

inline void MarketMaker::step(Venue& v, double dt) {
    Random& rng = v.rng();
    if (!rng.chance(p_.requote_rate_per_s * dt)) return;
    const double fair = v.fairTicks();
    const bool rising = fair >= last_fair_;
    last_fair_ = fair;
    // 先挪"让开"的一侧：上行时先挪卖单，下行时先挪买单
    // Move the side that "gets out of the way" first: the asks first when rising, the bids first when falling
    const Side first = rising ? Side::Sell : Side::Buy;
    quoteSide(v, first, fair);
    quoteSide(v, first == Side::Buy ? Side::Sell : Side::Buy, fair);
}

// ===========================================================================
// 实现：深度提供者
// Implementation: depth provider
// ===========================================================================

inline Price DepthProvider::placementPrice(Side side, Price floor_fair, Price ceil_fair, Random& rng) const {
    const double u = rng.uniform();
    const Price distance =
        p_.inner + static_cast<Price>(std::llround(static_cast<double>(p_.outer - p_.inner) * std::pow(u, p_.inner_bias)));
    return side == Side::Buy ? floor_fair - distance : ceil_fair + distance;
}

inline void DepthProvider::step(Venue& v, double dt) {
    Random& rng = v.rng();
    const double fair = v.fairTicks();
    const Price fb = static_cast<Price>(std::floor(fair));
    const Price fa = static_cast<Price>(std::ceil(fair));
    // 保留区间带滞回：内侧放宽到 inner / 2，外侧放宽到 outer × 1.25（补单仍在 [inner, outer] 内）
    // The keep range has hysteresis: relaxed to inner / 2 on the inside and to outer × 1.25 on the outside
    // (top-up orders are still placed within [inner, outer])
    const Price keep_inner = p_.inner / 2;
    const Price keep_outer = p_.outer + p_.outer / 4;
    int live_bids = 0;
    int live_asks = 0;
    free_.clear();
    for (std::uint32_t idx = 0; idx < slots_.size(); ++idx) {
        const Slot& s = slots_[idx];
        if (s.live) {
            const bool keep = s.side == Side::Buy ? (s.price <= fb - keep_inner && s.price >= fb - keep_outer)
                                                  : (s.price >= fa + keep_inner && s.price <= fa + keep_outer);
            if (!keep) cancel(v, idx);
        }
        if (s.live) {
            (s.side == Side::Buy ? live_bids : live_asks) += 1;
        } else {
            free_.push_back(idx);
        }
    }
    for (Side side : {Side::Buy, Side::Sell}) {
        const int have = side == Side::Buy ? live_bids : live_asks;
        int adds = std::min(p_.max_adds_per_step, p_.orders_per_side - have);
        while (adds-- > 0 && !free_.empty()) {
            const std::uint32_t idx = free_.back();
            free_.pop_back();
            const Price price = placementPrice(side, fb, fa, rng);
            const double depth_factor = 1.0 + 2.0 * static_cast<double>(std::llabs(price - (side == Side::Buy ? fb : fa))) /
                                                  static_cast<double>(p_.outer);
            const double q = static_cast<double>(p_.base_size) * depth_factor * std::exp(0.7 * rng.normal());
            place(v, idx, side, price, std::clamp<Qty>(static_cast<Qty>(std::llround(q)), 1, p_.base_size * 200));
        }
    }
    // 随机扰动：改量或撤单（撤掉的会在之后的步骤里换个价格补回）
    // Random churn: change the quantity or cancel
    // (cancelled orders are refilled at another price in later steps)
    const double churn = p_.churn_per_s * dt;
    const int events = static_cast<int>(churn) + (rng.chance(churn - std::floor(churn)) ? 1 : 0);
    for (int e = 0; e < events; ++e) {
        const auto idx = static_cast<std::uint32_t>(rng.uniformInt(0, static_cast<std::int64_t>(slots_.size()) - 1));
        const Slot& s = slots_[idx];
        if (!s.live) continue;
        if (rng.chance(0.5)) {
            const double q = static_cast<double>(s.qty) * std::exp(0.5 * rng.normal());
            move(v, idx, s.price, std::clamp<Qty>(static_cast<Qty>(std::llround(q)), 1, p_.base_size * 200));
        } else {
            cancel(v, idx);
        }
    }
}

// ===========================================================================
// 实现：吃单者
// Implementation: taker
// ===========================================================================

inline void Taker::step(Venue& v, double dt) {
    Random& rng = v.rng();
    if (!rng.chance(p_.rate_per_s * dt)) return;
    const std::optional<Price> bid = v.book().bestBid();
    const std::optional<Price> ask = v.book().bestAsk();
    if (!bid && !ask) return;
    const double mid = bid && ask ? 0.5 * static_cast<double>(*bid + *ask) : static_cast<double>(bid ? *bid : *ask);
    const double dev = (v.fairTicks() - mid) / p_.pressure_scale;
    const double logistic = 1.0 / (1.0 + std::exp(-std::clamp(dev, -40.0, 40.0)));
    const double p_buy = 0.5 + p_.pressure * (logistic - 0.5);
    const Side side = rng.chance(p_buy) ? Side::Buy : Side::Sell;
    const std::optional<Price> opposite = side == Side::Buy ? ask : bid;
    if (!opposite) return;  // 对手方为空：这次不下单
                            // the opposite side is empty: no order this time
    Qty qty = 0;
    if (rng.chance(p_.whale_prob)) {
        qty = rng.uniformInt(p_.whale_min, p_.whale_max);
    } else {
        qty = static_cast<Qty>(std::llround(static_cast<double>(p_.median_size) * std::exp(p_.size_sigma * rng.normal())));
    }
    // IOC：市价单，或越过对手最优价至多 max_slippage tick 的限价单
    // IOC: a market order, or a limit order at most max_slippage ticks past the opposite best price
    mt::OrderRequest req{nextTakerId(), account_, side, mt::OrderType::Market, mt::TimeInForce::IOC, 0,
                         std::max<Qty>(1, qty)};
    if (!rng.chance(p_.market_prob)) {
        req.type = mt::OrderType::Limit;
        const Price slip = rng.uniformInt(0, p_.max_slippage);
        req.price = side == Side::Buy ? *opposite + slip : *opposite - slip;
    }
    v.engine().submit(v.symbol(), req);
}

}  // namespace mock_exchange::sim
