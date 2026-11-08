//
// arbitrage_detector.h
// 多交易所套利检测：对每个有序交易所对（在 i 买、在 j 卖）按盘口深度扫单，扣除双边手续费后判断净利润，并按交易所对去重。
// Multi-exchange arbitrage detection: for every ordered exchange pair (buy on i, sell on j), sweeps the book
// depth, judges the net profit after deducting the fees on both sides, and deduplicates per exchange pair.
//

#pragma once

#include "orderbook.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// 套利信号：一次可执行套利的关键字段
// Arbitrage signal: the key fields of one executable arbitrage
struct ArbitrageSignal {
    std::string buy_exchange;     // 买入交易所的簿标签（OrderBook::getSymbol()，如 "Binance:BTCUSDT"）
                                  // book label of the buy exchange (OrderBook::getSymbol(), e.g. "Binance:BTCUSDT")
    std::string sell_exchange;    // 卖出交易所的簿标签
                                  // book label of the sell exchange
    std::size_t buy_venue = 0;    // 交易所编号（与 detect* 的下标一致）
                                  // exchange index (same as the indices of detect*)
    std::size_t sell_venue = 0;
    double buy_price = 0.0;       // 买方交易所的最优卖价（卖一）
                                  // best ask price of the buy exchange (ask level 1)
    double sell_price = 0.0;      // 卖方交易所的最优买价（买一）
                                  // best bid price of the sell exchange (bid level 1)
    double quantity = 0.0;        // 扫单总数量
                                  // total quantity swept
    double vwap_buy = 0.0;        // 买入成交均价
                                  // average price of the buy fills
    double vwap_sell = 0.0;       // 卖出成交均价
                                  // average price of the sell fills
    double gross_profit = 0.0;    // Σ (bid - ask) × q
    double fees = 0.0;            // Σ (ask × f_buy + bid × f_sell) × q
    double net_profit = 0.0;      // gross_profit - fees
    int levels_used = 0;          // 两侧中吃掉档位较多的一侧的档数（1 = 只用到最优档）
                                  // level count of the side that consumed more levels (1 = best level only)
    long long timestamp_ms = 0;   // 墙上时钟（Unix 毫秒）
                                  // wall clock (Unix milliseconds)
};

// 一次扫单的结果。
// 定义为 net = revenue - cost - fees；数学上与这里的 gross - fees 相同。gross 按每一步的 (bid - ask) × q 累加：
// 每侧一档时它就是 v1 的 (sell - buy) × qty，逐位相等（revenue - cost 两个大数相减会多一次舍入，做不到）。
// Result of one sweep.
// By definition net = revenue - cost - fees; mathematically that equals the gross - fees used here. gross
// accumulates (bid - ask) × q at every step:
// with one level per side it is v1's (sell - buy) × qty, bit for bit (revenue - cost subtracts two large
// numbers and adds one more rounding, so it cannot achieve that).
struct SweepResult {
    double qty = 0.0;       // 总数量
                            // total quantity
    double cost = 0.0;      // Σ ask × q（买入花费，不含手续费）
                            // Σ ask × q (cost of buying, excluding fees)
    double revenue = 0.0;   // Σ bid × q（卖出所得，不含手续费）
                            // Σ bid × q (proceeds of selling, excluding fees)
    double gross = 0.0;     // Σ (bid - ask) × q（逐步累加，比 revenue - cost 精确）
                            // Σ (bid - ask) × q (accumulated step by step, more precise than revenue - cost)
    double fees = 0.0;      // 双边手续费
                            // fees on both sides
    double net = 0.0;       // gross - fees（= revenue - cost - fees，差别只在舍入）
                            // gross - fees (= revenue - cost - fees, differing only in rounding)
    int buy_levels = 0;     // 买方（卖盘）用到的档数
                            // levels used by the buy side (asks)
    int sell_levels = 0;    // 卖方（买盘）用到的档数
                            // levels used by the sell side (bids)

    double vwapBuy() const noexcept { return qty > 0.0 ? cost / qty : 0.0; }
    double vwapSell() const noexcept { return qty > 0.0 ? revenue / qty : 0.0; }
    int levelsUsed() const noexcept { return std::max(buy_levels, sell_levels); }
};

// 套利检测器：
// - N 个交易所（编号 0..N-1），检测所有有序交易所对：在 buy 的卖盘买入、在 sell 的买盘卖出。
// - 按深度扫单（sweep）：沿买方卖盘与卖方买盘逐档推进，只要边际单位扣费后仍盈利
//   （bid × (1 - f_sell) - ask × (1 + f_buy) > 0）就继续，受 max_qty（0 = 不限）与每侧 max_levels 限制。
// - 信号条件：净利润 > min_profit，且通过去重：同一有序交易所对上，买方卖一或卖方买一相对该对上一次信号
//   至少变动了 0.01（kDedupStep；1 分的变动也算，见 moved()）。
// - 事件驱动：某个交易所的簿变化后只需检测涉及它的交易所对（detectVenue）。
//   任一侧为空（行情源断线时会发布空簿）视为"无数据"：不出信号，pairState().has_data = false。
// - 信号保存在有界 deque 中（最近 kMaxSignals 条），另有累计统计；检测器从不向 stdout 打印。
// - 每侧一档、两侧手续费相等时与 v1 的公式逐位相同（v1 只有两个交易所、只看最优档：max_levels = 1）。
// - 非线程安全：只在主线程使用。
// Arbitrage detector:
// - N exchanges (numbered 0..N-1); checks every ordered exchange pair: buy from buy's asks, sell into
//   sell's bids.
// - Sweep by depth (sweep): advance level by level along the buy venue's asks and the sell venue's bids,
//   and continue as long as the marginal unit is still profitable after fees
//   (bid × (1 - f_sell) - ask × (1 + f_buy) > 0), limited by max_qty (0 = unlimited) and max_levels per
//   side.
// - Signal condition: net profit > min_profit, and passing deduplication: on the same ordered exchange
//   pair, the buy side's best ask or the sell side's best bid has moved by at least 0.01 since that pair's
//   last signal (kDedupStep; a 1-cent move counts too, see moved()).
// - Event-driven: after an exchange's book changes, only the exchange pairs involving it need checking
//   (detectVenue).
//   Either side being empty (a feed publishes an empty book on disconnect) counts as "no data": no signal,
//   pairState().has_data = false.
// - Signals are kept in a bounded deque (the latest kMaxSignals), plus cumulative statistics; the detector
//   never prints to stdout.
// - With one level per side and equal fees on both sides, the formula is bit-identical to v1
//   (v1 had only two exchanges and looked only at the best level: max_levels = 1).
// - Not thread-safe: used only on the main thread.
class ArbitrageDetector {
public:
    static constexpr std::size_t kMaxSignals = 1000;  // 保留的最近信号条数
                                                      // number of recent signals kept
    static constexpr std::size_t kMaxVenues = 16;     // 一次检测最多产生 N(N-1) <= 240 条信号，小于 kMaxSignals
                                                      // one detection yields at most N(N-1) <= 240 signals,
                                                      // fewer than kMaxSignals
    static constexpr double kDedupStep = 0.01;        // 去重的最小价格变动
                                                      // minimum price move for deduplication

    // 每个有序交易所对的状态
    // State of each ordered exchange pair
    struct PairState {
        bool has_data = false;          // 最近一次评估时两侧都有数据
                                        // both sides had data at the latest evaluation
        SweepResult sweep;              // 最近一次评估的扫单结果（即当前盘口下的机会）
                                        // latest evaluation's sweep result (i.e. the opportunity
                                        // in the current order books)
        std::uint64_t evaluations = 0;  // 评估次数
                                        // number of evaluations
        bool has_signal = false;        // 是否出过信号（去重基准有效）
                                        // whether a signal was ever emitted (dedup baseline valid)
        double signal_ask = 0.0;        // 上一次信号时买方的卖一 / 卖方的买一（去重基准）
                                        // buy-side best ask / sell-side best bid at the last signal (dedup baseline)
        double signal_bid = 0.0;
        std::uint64_t signals = 0;      // 该交易所对的信号数
                                        // number of signals for this exchange pair
        double net_total = 0.0;         // 该交易所对的累计净利润
                                        // cumulative net profit of this exchange pair
        double fees_total = 0.0;
        double qty_total = 0.0;
        double best_net = 0.0;          // 单次信号的最大净利润
                                        // largest net profit of a single signal
    };

    // fee_rates[i]：第 i 个交易所的单边手续费；min_profit：最小净利润；max_qty：单次扫单数量上限（<= 0 不限）；
    // max_levels：每侧最多扫的档数（<= 0 不限）
    // fee_rates[i]: one-sided fee of exchange i; min_profit: minimum net profit; max_qty: quantity cap of a
    // single sweep (<= 0 unlimited); max_levels: maximum levels swept per side (<= 0 unlimited)
    ArbitrageDetector(std::vector<double> fee_rates, double min_profit, double max_qty = 0.0, int max_levels = 20)
        : fees_(std::move(fee_rates)),
          min_profit_(min_profit),
          max_qty_(max_qty),
          max_levels_(max_levels),
          pairs_(fees_.size() * fees_.size()) {
        if (fees_.size() > kMaxVenues) throw std::invalid_argument("ArbitrageDetector: too many venues");
    }

    // ---------------- 纯函数：扫单 ----------------
    // ---------------- Pure function: sweep ----------------

    // asks：买方交易所的卖盘（价格递增），bids：卖方交易所的买盘（价格递减），最优档在下标 0。
    // 数量 <= 0 或 NaN 的档位被跳过；价格 <= 0 或非有限值时停止。两侧手续费相等时手续费按
    // (ask + bid) × q × f 计算，因此每侧一档时与 v1 公式逐位相同：
    //   gross = (bid - ask) × min(q_ask, q_bid)，fee = (ask + bid) × qty × fee_rate，net = gross - fee。
    // asks: the buy exchange's asks (ascending prices), bids: the sell exchange's bids (descending prices),
    // best level at index 0.
    // Levels whose quantity is <= 0 or NaN are skipped; a price <= 0 or non-finite stops the sweep.
    // With equal fees on both sides the fee is computed as (ask + bid) × q × f, so with one level per side it
    // is bit-identical to the v1 formula:
    //   gross = (bid - ask) × min(q_ask, q_bid), fee = (ask + bid) × qty × fee_rate, net = gross - fee.
    static SweepResult sweep(const PriceLevel* asks, std::size_t ask_count, const PriceLevel* bids,
                             std::size_t bid_count, double fee_buy, double fee_sell, double max_qty = 0.0,
                             int max_levels = 0) noexcept {
        SweepResult r;
        if (max_levels > 0) {
            ask_count = std::min(ask_count, static_cast<std::size_t>(max_levels));
            bid_count = std::min(bid_count, static_cast<std::size_t>(max_levels));
        }
        if (asks == nullptr || bids == nullptr || ask_count == 0 || bid_count == 0) return r;
        const bool equal_fees = fee_buy == fee_sell;
        double remaining = max_qty > 0.0 ? max_qty : std::numeric_limits<double>::infinity();
        std::size_t i = 0, j = 0;
        double ask_left = asks[0].quantity;
        double bid_left = bids[0].quantity;
        for (;;) {
            // 跳过已吃完（或数量非法）的档位
            // Skip levels that are used up (or have an invalid quantity)
            while (i < ask_count && !(ask_left > 0.0)) {
                if (++i < ask_count) ask_left = asks[i].quantity;
            }
            while (j < bid_count && !(bid_left > 0.0)) {
                if (++j < bid_count) bid_left = bids[j].quantity;
            }
            if (i >= ask_count || j >= bid_count || !(remaining > 0.0)) break;
            const double ask = asks[i].price;
            const double bid = bids[j].price;
            if (!(ask > 0.0) || !(bid > 0.0) || !std::isfinite(ask) || !std::isfinite(bid)) break;
            // 边际单位的净利润：bid × (1 - f_sell) - ask × (1 + f_buy)
            // Net profit of the marginal unit: bid × (1 - f_sell) - ask × (1 + f_buy)
            const double unit_fee = equal_fees ? (ask + bid) * fee_buy : ask * fee_buy + bid * fee_sell;
            if (!(bid - ask - unit_fee > 0.0)) break;
            const double q = std::min(std::min(ask_left, bid_left), remaining);
            if (!std::isfinite(q)) break;
            r.qty += q;
            r.cost += ask * q;
            r.revenue += bid * q;
            r.gross += (bid - ask) * q;
            r.fees += equal_fees ? (ask + bid) * q * fee_buy : (ask * fee_buy + bid * fee_sell) * q;
            r.buy_levels = static_cast<int>(i + 1);
            r.sell_levels = static_cast<int>(j + 1);
            // q 是三者中的最小值，被减到恰好为 0 的一方下一轮被跳过：每轮至少推进一档或结束，必然终止
            // q is the minimum of the three, and whichever drops to exactly 0 is skipped next round: every round
            // advances at least one level or ends, so the loop always terminates
            ask_left -= q;
            bid_left -= q;
            remaining -= q;
        }
        r.net = r.gross - r.fees;
        return r;
    }

    // 去重用的"价格变动"：|a - b| >= 0.01，带一个随价格量级缩放的容差。
    // 二进制浮点里 67000.01 - 67000.00 = 0.0099999999947...，不加容差时 1 分的变动会被当成"没动"（v1 的问题）。
    // The "price move" used by deduplication: |a - b| >= 0.01, with a tolerance that scales with the price
    // magnitude.
    // In binary floating point 67000.01 - 67000.00 = 0.0099999999947..., so without the tolerance a 1-cent
    // move would count as "no move" (the v1 problem).
    static bool moved(double a, double b) noexcept {
        const double scale = std::max(1.0, std::max(std::fabs(a), std::fabs(b)));
        return std::fabs(a - b) >= kDedupStep - 1e-12 * scale;
    }

    // ---------------- 检测 ----------------
    // ---------------- Detection ----------------

    // 评估一个有序交易所对（在 buy 买、在 sell 卖）。产生新信号时返回指向它的指针
    // （有效到下一次 detect* 调用），否则返回 nullptr。下标越界或 buy == sell 时什么也不做。
    // Evaluates one ordered exchange pair (buy on buy, sell on sell). When it produces a new signal it returns
    // a pointer to it (valid until the next detect* call), otherwise nullptr. Does nothing when an index is
    // out of range or buy == sell.
    const ArbitrageSignal* detectPair(std::size_t buy, std::size_t sell, const OrderBook& buy_book,
                                      const OrderBook& sell_book) {
        const std::size_t n = fees_.size();
        if (buy >= n || sell >= n || buy == sell) return nullptr;
        PairState& st = pairs_[buy * n + sell];
        ++st.evaluations;
        const std::vector<PriceLevel>& asks = buy_book.getAsks();
        const std::vector<PriceLevel>& bids = sell_book.getBids();
        if (asks.empty() || bids.empty()) {  // 无数据
                                             // no data
            st.has_data = false;
            st.sweep = SweepResult{};
            return nullptr;
        }
        st.has_data = true;
        st.sweep = sweep(asks.data(), asks.size(), bids.data(), bids.size(), fees_[buy], fees_[sell], max_qty_,
                         max_levels_);
        const SweepResult& r = st.sweep;
        const double ask = asks[0].price, bid = bids[0].price;
        if (!(r.qty > 0.0) || !(r.net > min_profit_)) return nullptr;
        if (st.has_signal && !moved(ask, st.signal_ask) && !moved(bid, st.signal_bid)) return nullptr;

        st.has_signal = true;
        st.signal_ask = ask;
        st.signal_bid = bid;

        ArbitrageSignal s;
        s.buy_exchange = label(buy_book, buy);
        s.sell_exchange = label(sell_book, sell);
        s.buy_venue = buy;
        s.sell_venue = sell;
        s.buy_price = ask;
        s.sell_price = bid;
        s.quantity = r.qty;
        s.vwap_buy = r.vwapBuy();
        s.vwap_sell = r.vwapSell();
        s.gross_profit = r.gross;
        s.fees = r.fees;
        s.net_profit = r.net;
        s.levels_used = r.levelsUsed();
        s.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

        ++st.signals;
        st.net_total += r.net;
        st.fees_total += r.fees;
        st.qty_total += r.qty;
        st.best_net = std::max(st.best_net, r.net);
        ++total_signals_;
        total_net_ += r.net;
        total_fees_ += r.fees;
        total_qty_ += r.qty;

        signals_.push_back(std::move(s));
        if (signals_.size() > kMaxSignals) signals_.pop_front();
        return &signals_.back();
    }

    // 检测所有涉及 venue 的有序交易所对：对每个 other 依次评估 venue → other 与 other → venue。
    // book_of(i) 返回交易所 i 当前参与检测的簿（i < venueCount()；调用方可以用空簿代替不参与的交易所）。
    // 返回新信号数 k：它们是 getSignals() 的最后 k 条。venue 越界时什么也不做。
    // Checks every ordered exchange pair that involves venue: for each other, evaluates venue → other and
    // then other → venue.
    // book_of(i) returns the book that exchange i currently contributes to detection (i < venueCount();
    // the caller may substitute an empty book for an exchange that does not take part).
    // Returns the number of new signals k: they are the last k entries of getSignals(). Does nothing when
    // venue is out of range.
    template <class BookOf>
    std::size_t detectVenue(std::size_t venue, const BookOf& book_of) {
        const std::size_t n = fees_.size();
        if (venue >= n) return 0;
        const OrderBook& mine = book_of(venue);
        std::size_t found = 0;
        for (std::size_t other = 0; other < n; ++other) {
            if (other == venue) continue;
            const OrderBook& theirs = book_of(other);
            if (detectPair(venue, other, mine, theirs) != nullptr) ++found;
            if (detectPair(other, venue, theirs, mine) != nullptr) ++found;
        }
        return found;
    }

    // ---------------- 访问器 ----------------
    // ---------------- Accessors ----------------

    std::size_t venueCount() const noexcept { return fees_.size(); }
    double feeRate(std::size_t venue) const { return fees_.at(venue); }
    double minProfit() const noexcept { return min_profit_; }
    double maxQty() const noexcept { return max_qty_; }
    int maxLevels() const noexcept { return max_levels_; }

    std::uint64_t getTotalSignals() const noexcept { return total_signals_; }
    double getTotalNetProfit() const noexcept { return total_net_; }
    double getTotalFees() const noexcept { return total_fees_; }
    double getTotalQty() const noexcept { return total_qty_; }

    // 最近的信号（最旧在前，最多 kMaxSignals 条）
    // Recent signals (oldest first, at most kMaxSignals)
    const std::deque<ArbitrageSignal>& getSignals() const noexcept { return signals_; }

    const PairState& pairState(std::size_t buy, std::size_t sell) const {
        const std::size_t n = fees_.size();
        if (buy >= n || sell >= n) throw std::out_of_range("ArbitrageDetector::pairState: venue out of range");
        return pairs_[buy * n + sell];
    }

private:
    static std::string label(const OrderBook& book, std::size_t venue) {
        if (!book.getSymbol().empty()) return book.getSymbol();
        return "venue" + std::to_string(venue);
    }

    std::vector<double> fees_;
    double min_profit_;
    double max_qty_;
    int max_levels_;
    std::vector<PairState> pairs_;   // 下标 buy * N + sell
                                     // index buy * N + sell
    std::deque<ArbitrageSignal> signals_;
    std::uint64_t total_signals_ = 0;
    double total_net_ = 0.0;
    double total_fees_ = 0.0;
    double total_qty_ = 0.0;
};
