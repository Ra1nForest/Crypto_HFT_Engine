//
// mock_publishers.h
// 模拟交易所的行情发布状态机（纯协议层，无网络、无线程）：把撮合引擎的深度窗口按 Binance / OKX / Bybit 的协议
// 差分成推送报文并维护各自的序列号，按"最近一次发布的深度"生成新订阅的快照与 Binance REST 响应。
//

#pragma once

#include "mock_market.h"
#include "mock_wire.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// ============================================================================
// 一致性约定（三家相同）
//
// 每个发布者保存"最近一次发布的深度窗口" W（每侧前 N 档，最优在前）及其序列号。每个推送周期取引擎当前的
// 前 N 档与 W 逐档差分（wire::diffSide），只推送变化的档位：数量变化或新出现的档位给出新数量，消失的档位
// （撤光、吃光，或被更优的新档位挤出窗口）给出数量 0；然后 W ← 当前窗口。窗口没有变化的周期不推送
// （序列号不前进），与真实交易所"只在有变化时推送"一致。
//
// 推论：客户端从任何一个由 W 生成的快照（新订阅时的快照、Binance REST 响应）开始，按序应用之后的每条推送，
// 本地簿恒等于发布者的 W，也就是引擎在最近一次发布时刻的前 N 档（价格、数量逐档相等）。
// 测试（tests/test_mock_exchange.cpp）正是用真实的协议适配器验证这一点。
//
// 序列号：
// - Binance：U/u 使用交易所的"簿事件计数"（受理、成交、撤单、改单回调的累计数，单调递增）。
//   每条增量覆盖 [上一条的 u + 1, 当前计数]；REST 快照的 lastUpdateId = 最近一条增量的 u。
// - OKX books：seqId 同样取簿事件计数；prevSeqId = 上一条推送的 seqId；快照的 prevSeqId = -1。
//   checksum 按 OKX 规则用已发布窗口的前 25 档、以推送中完全相同的字符串计算（同一价格/数量的格式化是纯函数）。
// - Bybit：u 每条 delta 严格 +1（新订阅的快照给出当前 u）；seq（cross sequence）取簿事件计数。
// 计数偶尔没有前进（理论上不会：窗口变化必然伴随引擎事件）时取"上一条 + 1"，保证序列号严格递增。
// ============================================================================
namespace mock_exchange::pub {

using sim::Price;
using wire::Level;
using wire::Levels;

namespace detail {

// 在最优在前的有序档位中查找价格；不存在返回 nullptr
inline const Level* findLevel(const Levels& levels, Price price, bool bids) noexcept {
    const auto it = std::lower_bound(levels.begin(), levels.end(), price,
                                     [bids](const Level& l, Price p) { return bids ? l.price > p : l.price < p; });
    return it != levels.end() && it->price == price ? &*it : nullptr;
}

// 前 n 档的拷贝
inline Levels prefix(const Levels& src, std::size_t n) {
    return Levels(src.begin(), src.begin() + static_cast<std::ptrdiff_t>(std::min(n, src.size())));
}

// 两个窗口逐档相等（价格、数量；compare_orders 时还比较订单数）
inline bool sameLevels(const Levels& a, const Levels& b, bool compare_orders) noexcept {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].price != b[i].price || a[i].qty != b[i].qty) return false;
        if (compare_orders && a[i].orders != b[i].orders) return false;
    }
    return true;
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Binance：增量深度（@100ms 与默认的 1000 ms）、部分深度 depth5/10/20、REST GET /api/v3/depth
// ---------------------------------------------------------------------------
class BinanceDepthPublisher {
public:
    static constexpr std::size_t kDefaultWindow = 1000;     // 增量流覆盖的档位数（每侧）
    static constexpr std::size_t kDefaultRestLimit = 100;   // REST 不带 limit 时（与真实接口一致）
    static constexpr std::size_t kMaxRestLimit = 5000;      // REST limit 上限（超过按 5000）

    explicit BinanceDepthPublisher(std::string symbol = "BTCUSDT", std::size_t window = kDefaultWindow)
        : symbol_(std::move(symbol)), window_(std::max<std::size_t>(1, window)) {}

    // 初始状态（不产生推送）：例如预热结束时的深度与簿事件计数
    void reset(const Levels& bids, const Levels& asks, std::uint64_t update_id) {
        wire::topLevels(bids, window_, bids_);
        wire::topLevels(asks, window_, asks_);
        last_u_ = update_id;
        slow_last_u_ = update_id;
        touched_bids_.clear();
        touched_asks_.clear();
    }

    // 快速流（<symbol>@depth@100ms）的一个推送周期：与已发布窗口差分。窗口无变化返回 false（不推送，u 不前进）。
    bool publish(const Levels& bids, const Levels& asks, std::uint64_t update_id, std::int64_t event_ms,
                 std::string& out) {
        wire::topLevels(bids, window_, cur_bids_);
        wire::topLevels(asks, window_, cur_asks_);
        diff_bids_.clear();
        diff_asks_.clear();
        wire::diffSide(bids_, cur_bids_, true, false, diff_bids_);
        wire::diffSide(asks_, cur_asks_, false, false, diff_asks_);
        if (diff_bids_.empty() && diff_asks_.empty()) return false;
        const std::uint64_t first = last_u_ + 1;
        const std::uint64_t final_id = std::max(update_id, first);
        out = wire::binanceDepthUpdate(symbol_, event_ms, first, final_id, diff_bids_, diff_asks_);
        for (const Level& l : diff_bids_) touched_bids_.push_back(l.price);
        for (const Level& l : diff_asks_) touched_asks_.push_back(l.price);
        bids_.swap(cur_bids_);
        asks_.swap(cur_asks_);
        last_u_ = final_id;
        return true;
    }

    // 慢速流（<symbol>@depth，1000 ms）：汇总自上一条慢速事件以来快速流触及过的全部价位，给出它们的当前数量
    // （已不在窗口内 → 0）。U = 上一条慢速事件的 u + 1，u = 快速流当前的 u。
    // 为什么要汇总"触及过的价位"而不是对 1 s 前的窗口再做一次差分：REST 快照取自快速流的状态（某个 100 ms 边界 L），
    // 可能落在慢速事件的 [U, u] 区间内。某档在 L 之前变过、到 u 时又变回原值，净差分里没有它，客户端就会留着
    // 快照里的中间值；包含区间内触及过的每个价位（最终数量）则与任何 L 都一致——这也是真实 Binance 的语义。
    // 调用顺序：同一步先 publish 再 publishSlow。没有任何变化时返回 false。
    bool publishSlow(std::int64_t event_ms, std::string& out) {
        if (touched_bids_.empty() && touched_asks_.empty()) return false;
        sortUnique(touched_bids_, true);
        sortUnique(touched_asks_, false);
        slow_bids_.clear();
        slow_asks_.clear();
        for (const Price p : touched_bids_) slow_bids_.push_back(currentLevel(bids_, p, true));
        for (const Price p : touched_asks_) slow_asks_.push_back(currentLevel(asks_, p, false));
        out = wire::binanceDepthUpdate(symbol_, event_ms, slow_last_u_ + 1, last_u_, slow_bids_, slow_asks_);
        slow_last_u_ = last_u_;
        touched_bids_.clear();
        touched_asks_.clear();
        return true;
    }

    // GET /api/v3/depth 的响应体：已发布窗口的前 limit 档（窗口只有 window() 档，limit 更大时按窗口返回）
    std::string restSnapshot(std::size_t limit) const {
        return wire::binanceDepth(last_u_, detail::prefix(bids_, limit), detail::prefix(asks_, limit));
    }

    // 部分深度推送 <symbol>@depth<levels>[@100ms]（形状与 REST 响应相同）
    std::string partialDepth(std::size_t levels) const { return restSnapshot(levels); }

    std::uint64_t lastUpdateId() const noexcept { return last_u_; }
    const Levels& bids() const noexcept { return bids_; }
    const Levels& asks() const noexcept { return asks_; }
    std::size_t window() const noexcept { return window_; }
    const std::string& symbol() const noexcept { return symbol_; }

private:
    static void sortUnique(std::vector<Price>& v, bool descending) {
        if (descending) {
            std::sort(v.begin(), v.end(), [](Price a, Price b) { return a > b; });
        } else {
            std::sort(v.begin(), v.end());
        }
        v.erase(std::unique(v.begin(), v.end()), v.end());
    }

    static Level currentLevel(const Levels& levels, Price price, bool bids) noexcept {
        const Level* l = detail::findLevel(levels, price, bids);
        return l != nullptr ? *l : Level{price, 0, 0};
    }

    std::string symbol_;
    std::size_t window_;
    Levels bids_, asks_;                 // 已发布窗口
    std::uint64_t last_u_ = 0;           // 快速流最近一条事件的 u（REST lastUpdateId）
    std::uint64_t slow_last_u_ = 0;      // 慢速流最近一条事件的 u
    std::vector<Price> touched_bids_;    // 自上一条慢速事件以来快速流触及的价位
    std::vector<Price> touched_asks_;
    // 复用的暂存区（稳态不分配）
    Levels cur_bids_, cur_asks_, diff_bids_, diff_asks_, slow_bids_, slow_asks_;
};

// ---------------------------------------------------------------------------
// OKX books（400 档增量）：订阅时快照，之后每 100 ms 增量；prevSeqId/seqId 衔接，带 checksum
// ---------------------------------------------------------------------------
class OkxBooksPublisher {
public:
    static constexpr std::size_t kDefaultWindow = 400;
    // 窗口长时间不变时的心跳间隔（bids/asks 为空、prevSeqId == seqId）。真实 OKX 约 60 s；
    // 仿真中盘口每 100 ms 都在变，实际几乎不会触发，测试里用短间隔验证。<= 0 关闭。
    static constexpr std::int64_t kDefaultHeartbeatMs = 30000;

    explicit OkxBooksPublisher(std::string inst_id = "BTC-USDT", std::size_t window = kDefaultWindow,
                               std::int64_t heartbeat_ms = kDefaultHeartbeatMs)
        : inst_id_(std::move(inst_id)),
          window_(std::max<std::size_t>(wire::kOkxChecksumLevels, window)),
          heartbeat_ms_(heartbeat_ms) {}

    void reset(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t now_ms) {
        wire::topLevels(bids, window_, bids_);
        wire::topLevels(asks, window_, asks_);
        last_seq_ = std::max<std::int64_t>(0, seq);
        last_push_ms_ = now_ms;
        checksum_ = wire::okxChecksum(bids_, asks_);
    }

    // 一个推送周期：有变化 → action "update"；无变化且距上次推送超过心跳间隔 → 心跳；否则返回 false
    bool publish(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t ts_ms, std::string& out) {
        wire::topLevels(bids, window_, cur_bids_);
        wire::topLevels(asks, window_, cur_asks_);
        diff_bids_.clear();
        diff_asks_.clear();
        wire::diffSide(bids_, cur_bids_, true, true, diff_bids_);
        wire::diffSide(asks_, cur_asks_, false, true, diff_asks_);
        if (diff_bids_.empty() && diff_asks_.empty()) {
            if (heartbeat_ms_ <= 0 || ts_ms - last_push_ms_ < heartbeat_ms_) return false;
            // 心跳：两侧都是空数组（此时 diff_bids_ / diff_asks_ 恰好都为空），checksum 仍是当前窗口的
            out = wire::okxBooks("books", inst_id_, "update", diff_bids_, diff_asks_, ts_ms, checksum_, last_seq_,
                                 last_seq_);
            last_push_ms_ = ts_ms;
            return true;
        }
        bids_.swap(cur_bids_);
        asks_.swap(cur_asks_);
        checksum_ = wire::okxChecksum(bids_, asks_);
        const std::int64_t prev = last_seq_;
        last_seq_ = std::max(seq, prev + 1);
        out = wire::okxBooks("books", inst_id_, "update", diff_bids_, diff_asks_, ts_ms, checksum_, prev, last_seq_);
        last_push_ms_ = ts_ms;
        return true;
    }

    // 新订阅的快照：已发布窗口（prevSeqId = -1，seqId = 最近一条推送的 seqId）
    std::string snapshot(std::int64_t ts_ms) const {
        return wire::okxBooks("books", inst_id_, "snapshot", bids_, asks_, ts_ms, checksum_, -1, last_seq_);
    }

    std::int64_t lastSeqId() const noexcept { return last_seq_; }
    std::int32_t checksum() const noexcept { return checksum_; }
    const Levels& bids() const noexcept { return bids_; }
    const Levels& asks() const noexcept { return asks_; }
    std::size_t window() const noexcept { return window_; }
    const std::string& instId() const noexcept { return inst_id_; }

private:
    std::string inst_id_;
    std::size_t window_;
    std::int64_t heartbeat_ms_;
    Levels bids_, asks_;
    std::int64_t last_seq_ = 0;
    std::int64_t last_push_ms_ = 0;
    std::int32_t checksum_ = 0;
    Levels cur_bids_, cur_asks_, diff_bids_, diff_asks_;
};

// ---------------------------------------------------------------------------
// OKX books5 / bbo-tbt：每次推送完整的前 N 档（无 action），前 N 档变化时才推送；订阅时先推一次当前状态
// ---------------------------------------------------------------------------
class OkxTopPublisher {
public:
    OkxTopPublisher(std::string channel, std::size_t levels, std::string inst_id = "BTC-USDT")
        : channel_(std::move(channel)), inst_id_(std::move(inst_id)), levels_(std::max<std::size_t>(1, levels)) {}

    void reset(const Levels& bids, const Levels& asks, std::int64_t seq) {
        wire::topLevels(bids, levels_, bids_);
        wire::topLevels(asks, levels_, asks_);
        seq_ = std::max<std::int64_t>(0, seq);
    }

    bool publish(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t ts_ms, std::string& out) {
        wire::topLevels(bids, levels_, cur_bids_);
        wire::topLevels(asks, levels_, cur_asks_);
        if (detail::sameLevels(bids_, cur_bids_, true) && detail::sameLevels(asks_, cur_asks_, true)) return false;
        bids_.swap(cur_bids_);
        asks_.swap(cur_asks_);
        seq_ = std::max(seq, seq_ + 1);
        out = current(ts_ms);
        return true;
    }

    std::string current(std::int64_t ts_ms) const {
        return wire::okxTopBook(channel_, inst_id_, bids_, asks_, ts_ms, seq_);
    }

    const std::string& channel() const noexcept { return channel_; }
    std::size_t levels() const noexcept { return levels_; }
    const Levels& bids() const noexcept { return bids_; }
    const Levels& asks() const noexcept { return asks_; }
    std::int64_t seqId() const noexcept { return seq_; }

private:
    std::string channel_;
    std::string inst_id_;
    std::size_t levels_;
    Levels bids_, asks_;
    std::int64_t seq_ = 0;
    Levels cur_bids_, cur_asks_;
};

// ---------------------------------------------------------------------------
// Bybit orderbook.{depth}.{symbol}：订阅时 snapshot，之后 delta（u 严格 +1）
// ---------------------------------------------------------------------------
class BybitBookPublisher {
public:
    // 起始更新号：足够大，第一条 delta 不可能是 u == 1（Bybit 用 u == 1 表示"服务重启后的快照"）
    static constexpr std::uint64_t kInitialUpdateId = 1000000;

    explicit BybitBookPublisher(std::size_t depth = 50, std::string symbol = "BTCUSDT",
                                std::uint64_t initial_update_id = kInitialUpdateId)
        : depth_(std::max<std::size_t>(1, depth)),
          symbol_(std::move(symbol)),
          topic_("orderbook." + std::to_string(depth_) + "." + symbol_),
          u_(std::max<std::uint64_t>(2, initial_update_id)) {}

    void reset(const Levels& bids, const Levels& asks, std::uint64_t seq) {
        wire::topLevels(bids, depth_, bids_);
        wire::topLevels(asks, depth_, asks_);
        seq_ = seq;
    }

    bool publish(const Levels& bids, const Levels& asks, std::uint64_t seq, std::int64_t ts_ms, std::int64_t cts_ms,
                 std::string& out) {
        wire::topLevels(bids, depth_, cur_bids_);
        wire::topLevels(asks, depth_, cur_asks_);
        diff_bids_.clear();
        diff_asks_.clear();
        wire::diffSide(bids_, cur_bids_, true, false, diff_bids_);
        wire::diffSide(asks_, cur_asks_, false, false, diff_asks_);
        if (diff_bids_.empty() && diff_asks_.empty()) return false;
        bids_.swap(cur_bids_);
        asks_.swap(cur_asks_);
        ++u_;
        seq_ = std::max(seq, seq_ + 1);
        out = wire::bybitBook(topic_, "delta", symbol_, diff_bids_, diff_asks_, u_, seq_, ts_ms, cts_ms);
        return true;
    }

    std::string snapshot(std::int64_t ts_ms, std::int64_t cts_ms) const {
        return wire::bybitBook(topic_, "snapshot", symbol_, bids_, asks_, u_, seq_, ts_ms, cts_ms);
    }

    const std::string& topic() const noexcept { return topic_; }
    std::size_t depth() const noexcept { return depth_; }
    std::uint64_t lastUpdateId() const noexcept { return u_; }
    std::uint64_t seq() const noexcept { return seq_; }
    const Levels& bids() const noexcept { return bids_; }
    const Levels& asks() const noexcept { return asks_; }

private:
    std::size_t depth_;
    std::string symbol_;
    std::string topic_;
    Levels bids_, asks_;
    std::uint64_t u_;
    std::uint64_t seq_ = 0;
    Levels cur_bids_, cur_asks_, diff_bids_, diff_asks_;
};

}  // namespace mock_exchange::pub
