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

// 已发布的深度窗口（每侧前 n 档，最优在前）及差分用的暂存区（稳态不分配）
class DepthWindow {
public:
    explicit DepthWindow(std::size_t n) : n_(n) {}

    void reset(const Levels& bids, const Levels& asks) {
        top(bids, bids_);
        top(asks, asks_);
    }

    // 与新深度的前 n 档差分：有变化时窗口更新为新深度、差分留在 diffBids() / diffAsks()，返回 true；
    // 没有变化返回 false（差分为空，窗口不变）。compare_orders 见 wire::diffSide。
    bool advance(const Levels& bids, const Levels& asks, bool compare_orders) {
        top(bids, cur_bids_);
        top(asks, cur_asks_);
        diff_bids_.clear();
        diff_asks_.clear();
        wire::diffSide(bids_, cur_bids_, true, compare_orders, diff_bids_);
        wire::diffSide(asks_, cur_asks_, false, compare_orders, diff_asks_);
        if (diff_bids_.empty() && diff_asks_.empty()) return false;
        bids_.swap(cur_bids_);
        asks_.swap(cur_asks_);
        return true;
    }

    std::size_t size() const noexcept { return n_; }
    const Levels& bids() const noexcept { return bids_; }
    const Levels& asks() const noexcept { return asks_; }
    const Levels& diffBids() const noexcept { return diff_bids_; }
    const Levels& diffAsks() const noexcept { return diff_asks_; }

private:
    void top(const Levels& src, Levels& out) const {
        out.assign(src.begin(), src.begin() + static_cast<std::ptrdiff_t>(std::min(n_, src.size())));
    }

    std::size_t n_;
    Levels bids_, asks_, cur_bids_, cur_asks_, diff_bids_, diff_asks_;
};

}  // namespace detail

// ---------------------------------------------------------------------------
// Binance：增量深度（@100ms 与默认的 1000 ms）、部分深度 depth5/10/20、REST GET /api/v3/depth
// ---------------------------------------------------------------------------
class BinanceDepthPublisher {
public:
    static constexpr std::size_t kWindow = 1000;            // 增量流覆盖的档位数（每侧）
    static constexpr std::size_t kDefaultRestLimit = 100;   // REST 不带 limit 时（与真实接口一致）
    static constexpr std::size_t kMaxRestLimit = 5000;      // REST limit 上限（超过按 5000）

    // 初始状态（不产生推送）：例如预热结束时的深度与簿事件计数
    void reset(const Levels& bids, const Levels& asks, std::uint64_t update_id) {
        w_.reset(bids, asks);
        last_u_ = update_id;
        slow_last_u_ = update_id;
        touched_bids_.clear();
        touched_asks_.clear();
    }

    // 快速流（btcusdt@depth@100ms）的一个推送周期：与已发布窗口差分。窗口无变化返回 false（不推送，u 不前进）。
    bool publish(const Levels& bids, const Levels& asks, std::uint64_t update_id, std::int64_t event_ms,
                 std::string& out) {
        if (!w_.advance(bids, asks, false)) return false;
        const std::uint64_t first = last_u_ + 1;
        last_u_ = std::max(update_id, first);
        out = wire::binanceDepthUpdate(wire::kBinanceSymbol, event_ms, first, last_u_, w_.diffBids(), w_.diffAsks());
        for (const Level& l : w_.diffBids()) touched_bids_.push_back(l.price);
        for (const Level& l : w_.diffAsks()) touched_asks_.push_back(l.price);
        return true;
    }

    // 慢速流（btcusdt@depth，1000 ms）：汇总自上一条慢速事件以来快速流触及过的全部价位，给出它们的当前数量
    // （已不在窗口内 → 0）。U = 上一条慢速事件的 u + 1，u = 快速流当前的 u。
    // 为什么要汇总"触及过的价位"而不是对 1 s 前的窗口再做一次差分：REST 快照取自快速流的状态（某个 100 ms 边界 L），
    // 可能落在慢速事件的 [U, u] 区间内。某档在 L 之前变过、到 u 时又变回原值，净差分里没有它，客户端就会留着
    // 快照里的中间值；包含区间内触及过的每个价位（最终数量）则与任何 L 都一致——这也是真实 Binance 的语义。
    // 调用顺序：同一步先 publish 再 publishSlow。没有任何变化时返回 false。
    bool publishSlow(std::int64_t event_ms, std::string& out) {
        if (touched_bids_.empty() && touched_asks_.empty()) return false;
        collectTouched(touched_bids_, w_.bids(), true, slow_bids_);
        collectTouched(touched_asks_, w_.asks(), false, slow_asks_);
        out = wire::binanceDepthUpdate(wire::kBinanceSymbol, event_ms, slow_last_u_ + 1, last_u_, slow_bids_,
                                       slow_asks_);
        slow_last_u_ = last_u_;
        return true;
    }

    // GET /api/v3/depth 的响应体：已发布窗口的前 limit 档（窗口只有 kWindow 档，limit 更大时按窗口返回）
    std::string restSnapshot(std::size_t limit) const { return wire::binanceDepth(last_u_, w_.bids(), w_.asks(), limit); }

    // 部分深度推送 btcusdt@depth<levels>[@100ms]（形状与 REST 响应相同）
    std::string partialDepth(std::size_t levels) const { return restSnapshot(levels); }

    std::uint64_t lastUpdateId() const noexcept { return last_u_; }
    std::size_t window() const noexcept { return w_.size(); }

private:
    // 触及过的价位（排序去重，最优在前）在已发布窗口中的当前档位，放进 out；清空 touched
    static void collectTouched(std::vector<Price>& touched, const Levels& window, bool bids, Levels& out) {
        auto better = [bids](Price a, Price b) { return wire::better(bids, a, b); };
        std::sort(touched.begin(), touched.end(), better);
        touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
        out.clear();
        for (const Price p : touched) {
            const auto it = std::lower_bound(window.begin(), window.end(), p,
                                             [&better](const Level& l, Price x) { return better(l.price, x); });
            out.push_back(it != window.end() && it->price == p ? *it : Level{p, 0, 0});
        }
        touched.clear();
    }

    detail::DepthWindow w_{kWindow};     // 已发布窗口
    std::uint64_t last_u_ = 0;           // 快速流最近一条事件的 u（REST lastUpdateId）
    std::uint64_t slow_last_u_ = 0;      // 慢速流最近一条事件的 u
    std::vector<Price> touched_bids_;    // 自上一条慢速事件以来快速流触及的价位
    std::vector<Price> touched_asks_;
    Levels slow_bids_, slow_asks_;       // 慢速事件的暂存区
};

// ---------------------------------------------------------------------------
// OKX books（400 档增量）：订阅时快照，之后每 100 ms 增量；prevSeqId/seqId 衔接，带 checksum
// ---------------------------------------------------------------------------
class OkxBooksPublisher {
public:
    static constexpr std::size_t kWindow = 400;
    // 窗口长时间不变时的心跳间隔（bids/asks 为空、prevSeqId == seqId）。真实 OKX 约 60 s；
    // 仿真中盘口每 100 ms 都在变，实际几乎不会触发，测试里用短间隔验证。
    static constexpr std::int64_t kDefaultHeartbeatMs = 30000;

    explicit OkxBooksPublisher(std::int64_t heartbeat_ms = kDefaultHeartbeatMs) : heartbeat_ms_(heartbeat_ms) {}

    void reset(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t now_ms) {
        w_.reset(bids, asks);
        last_seq_ = seq;
        last_push_ms_ = now_ms;
        checksum_ = wire::okxChecksum(w_.bids(), w_.asks());
    }

    // 一个推送周期：有变化 → action "update"；无变化且距上次推送超过心跳间隔 → 心跳；否则返回 false
    bool publish(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t ts_ms, std::string& out) {
        const std::int64_t prev = last_seq_;
        if (w_.advance(bids, asks, true)) {
            checksum_ = wire::okxChecksum(w_.bids(), w_.asks());
            last_seq_ = std::max(seq, prev + 1);
        } else if (ts_ms - last_push_ms_ < heartbeat_ms_) {
            return false;
        }
        // 心跳时差分两侧都是空数组、prevSeqId == seqId，checksum 仍是当前窗口的
        out = wire::okxBooks("books", wire::kOkxInstId, "update", w_.diffBids(), w_.diffAsks(), ts_ms, checksum_, prev,
                             last_seq_);
        last_push_ms_ = ts_ms;
        return true;
    }

    // 新订阅的快照：已发布窗口（prevSeqId = -1，seqId = 最近一条推送的 seqId）
    std::string snapshot(std::int64_t ts_ms) const {
        return wire::okxBooks("books", wire::kOkxInstId, "snapshot", w_.bids(), w_.asks(), ts_ms, checksum_, -1,
                              last_seq_);
    }

    std::int64_t lastSeqId() const noexcept { return last_seq_; }
    std::size_t window() const noexcept { return w_.size(); }

private:
    std::int64_t heartbeat_ms_;
    detail::DepthWindow w_{kWindow};
    std::int64_t last_seq_ = 0;
    std::int64_t last_push_ms_ = 0;
    std::int32_t checksum_ = 0;
};

// ---------------------------------------------------------------------------
// OKX books5 / bbo-tbt：每次推送完整的前 N 档（无 action），前 N 档变化时才推送；订阅时先推一次当前状态
// ---------------------------------------------------------------------------
class OkxTopPublisher {
public:
    OkxTopPublisher(std::string channel, std::size_t levels) : channel_(std::move(channel)), w_(levels) {}

    void reset(const Levels& bids, const Levels& asks, std::int64_t seq) {
        w_.reset(bids, asks);
        seq_ = seq;
    }

    bool publish(const Levels& bids, const Levels& asks, std::int64_t seq, std::int64_t ts_ms, std::string& out) {
        if (!w_.advance(bids, asks, true)) return false;
        seq_ = std::max(seq, seq_ + 1);
        out = current(ts_ms);
        return true;
    }

    std::string current(std::int64_t ts_ms) const {
        return wire::okxTopBook(channel_, wire::kOkxInstId, w_.bids(), w_.asks(), ts_ms, seq_);
    }

private:
    std::string channel_;
    detail::DepthWindow w_;
    std::int64_t seq_ = 0;
};

// ---------------------------------------------------------------------------
// Bybit orderbook.{depth}.BTCUSDT：订阅时 snapshot，之后 delta（u 严格 +1）
// ---------------------------------------------------------------------------
class BybitBookPublisher {
public:
    // 起始更新号：足够大，第一条 delta 不可能是 u == 1（Bybit 用 u == 1 表示"服务重启后的快照"）
    static constexpr std::uint64_t kInitialUpdateId = 1000000;

    explicit BybitBookPublisher(std::size_t depth = 50)
        : topic_("orderbook." + std::to_string(depth) + "." + std::string(wire::kBybitSymbol)), w_(depth) {}

    void reset(const Levels& bids, const Levels& asks, std::uint64_t seq) {
        w_.reset(bids, asks);
        seq_ = seq;
    }

    bool publish(const Levels& bids, const Levels& asks, std::uint64_t seq, std::int64_t ts_ms, std::int64_t cts_ms,
                 std::string& out) {
        if (!w_.advance(bids, asks, false)) return false;
        ++u_;
        seq_ = std::max(seq, seq_ + 1);
        out = wire::bybitBook(topic_, "delta", wire::kBybitSymbol, w_.diffBids(), w_.diffAsks(), u_, seq_, ts_ms,
                              cts_ms);
        return true;
    }

    std::string snapshot(std::int64_t ts_ms, std::int64_t cts_ms) const {
        return wire::bybitBook(topic_, "snapshot", wire::kBybitSymbol, w_.bids(), w_.asks(), u_, seq_, ts_ms, cts_ms);
    }

    const std::string& topic() const noexcept { return topic_; }
    std::uint64_t lastUpdateId() const noexcept { return u_; }
    std::uint64_t seq() const noexcept { return seq_; }

private:
    std::string topic_;
    detail::DepthWindow w_;
    std::uint64_t u_ = kInitialUpdateId;
    std::uint64_t seq_ = 0;
};

}  // namespace mock_exchange::pub
