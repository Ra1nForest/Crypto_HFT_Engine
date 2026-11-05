//
// naive_order_book.h
// 第二个独立预言机：全部挂单放在一个无序 vector 中，每次线性扫描求价格-时间优先级。
// 数据结构与参考模型完全不同，用来交叉验证参考模型本身（参考模型是差分测试的标准答案）。
//

#pragma once

#include "matching/types.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace hft::matching::test {

// 与参考模型刻意不同的实现方式：
// - 挂单只有一个 std::vector<Resting>，没有价位结构和索引；时间优先级用全局递增的到达序号 seq
// - 最优 maker、深度、档位等全部靠线性扫描 + 排序现算
// - FOK 预检不单独实现"遍历求和"，而是在挂单副本上空跑一遍真正的撮合循环（丢弃事件），
//   看能否完全成交 —— 这同时验证了规范中"预检按撮合顺序遍历"的描述与撮合循环本身一致
// - 档位总量上限（kMaxLevelQty）不用"上限减去现有总量"，而是把目标价位上其他挂单逐单加到请求数量上、
//   检测加法溢出；改单时按 id 排除订单自己，而不是减去它原来的数量
// 接口与 ReferenceOrderBook / LimitOrderBook 相同（docs/MATCHING_ENGINE.md）。仅用于测试，性能无关紧要。
template <class Listener>
class NaiveOrderBook {
public:
    explicit NaiveOrderBook(Listener& listener, std::size_t max_orders = 0,
                            std::size_t reserve_orders = 1024)
        : listener_(listener), max_orders_(max_orders) {
        orders_.reserve(reserve_orders);
    }

    NaiveOrderBook(const NaiveOrderBook&) = delete;
    NaiveOrderBook& operator=(const NaiveOrderBook&) = delete;

    void submit(const OrderRequest& req) {
        if (const std::optional<RejectReason> reason = newOrderRejection(req)) {
            listener_.onRejected(req.id, RequestType::New, *reason);
            return;
        }
        listener_.onAccepted(req);

        const TakerSpec taker{req.id, req.account, req.side, req.type == OrderType::Market,
                              req.price, req.stp};
        if (req.tif == TimeInForce::FOK) {
            // 在副本上空跑撮合：只有完全成交且未被 STP 终止才算可成交
            std::vector<Resting> scratch = orders_;
            DiscardEvents sink;
            TradeId scratch_trade_id = last_trade_id_;
            Qty left = req.qty;
            const bool alive = sweep(scratch, sink, scratch_trade_id, taker, left);
            if (!alive || left > 0) {
                listener_.onCancelled(req.id, req.qty, CancelReason::FokUnfillable);
                return;
            }
        }

        Qty left = req.qty;
        if (!sweep(orders_, listener_, last_trade_id_, taker, left)) return;
        if (left == 0) return;
        if (taker.market) {
            listener_.onCancelled(req.id, left, CancelReason::MarketRemainder);
        } else if (req.tif == TimeInForce::IOC) {
            listener_.onCancelled(req.id, left, CancelReason::IocRemainder);
        } else {
            // 限价 GTC（FOK 能走到这里说明预检通过，剩余必为 0）
            insert(OrderInfo{req.id, req.account, req.side, req.price, left, req.post_only, req.stp});
        }
    }

    void cancel(OrderId id) {
        const std::size_t i = indexOf(id);
        if (i == kNone) {
            listener_.onRejected(id, RequestType::Cancel, RejectReason::UnknownOrder);
            return;
        }
        const Qty remaining = orders_[i].info.remaining;
        orders_.erase(orders_.begin() + static_cast<std::ptrdiff_t>(i));
        listener_.onCancelled(id, remaining, CancelReason::UserRequested);
    }

    void modify(OrderId id, Price new_price, Qty new_qty) {
        const std::size_t i = indexOf(id);
        if (i == kNone) {
            listener_.onRejected(id, RequestType::Modify, RejectReason::UnknownOrder);
            return;
        }
        if (new_qty <= 0) {
            listener_.onRejected(id, RequestType::Modify, RejectReason::InvalidQuantity);
            return;
        }
        if (new_price <= 0) {
            listener_.onRejected(id, RequestType::Modify, RejectReason::InvalidPrice);
            return;
        }
        OrderInfo info = orders_[i].info;
        // 改价或加量会把订单重新挂到 new_price：挂上去之后该档总量不得越界（原地减量不受限）
        const bool in_place = new_price == info.price && new_qty <= info.remaining;
        if (!in_place && exceedsLevelBound(info.side, new_price, id, new_qty)) {
            listener_.onRejected(id, RequestType::Modify, RejectReason::InvalidQuantity);
            return;
        }
        if (info.post_only && new_price != info.price && wouldCross(info.side, new_price)) {
            listener_.onRejected(id, RequestType::Modify, RejectReason::PostOnlyWouldCross);
            return;
        }
        if (in_place) {
            orders_[i].info.remaining = new_qty;  // 原地修改：seq 不变，保留时间优先级
            listener_.onModified(id, new_price, new_qty);
            return;
        }
        orders_.erase(orders_.begin() + static_cast<std::ptrdiff_t>(i));
        listener_.onModified(id, new_price, new_qty);
        const TakerSpec taker{id, info.account, info.side, false, new_price, info.stp};
        Qty left = new_qty;
        if (!sweep(orders_, listener_, last_trade_id_, taker, left)) return;
        if (left == 0) return;
        info.price = new_price;
        info.remaining = left;
        insert(info);  // 新的 seq：排到队尾
    }

    // ---- 查询 ----
    std::optional<Price> bestBid() const { return bestPrice(Side::Buy); }
    std::optional<Price> bestAsk() const { return bestPrice(Side::Sell); }

    std::size_t orderCount() const { return orders_.size(); }

    std::size_t levelCount(Side side) const {
        std::set<Price> prices;
        for (const Resting& r : orders_) {
            if (r.info.side == side) prices.insert(r.info.price);
        }
        return prices.size();
    }

    std::vector<LevelInfo> depth(Side side, std::size_t max_levels = SIZE_MAX) const {
        std::vector<LevelInfo> out;
        for (const Resting* r : inPriority(orders_, side)) {
            if (!out.empty() && out.back().price == r->info.price) {
                out.back().qty = addToLevel(out.back().qty, r->info.remaining);
                ++out.back().orders;
                continue;
            }
            if (out.size() == max_levels) break;
            out.push_back(LevelInfo{r->info.price, r->info.remaining, 1});
        }
        return out;
    }

    std::vector<OrderInfo> ordersAt(Side side, Price price) const {
        std::vector<const Resting*> at;
        for (const Resting& r : orders_) {
            if (r.info.side == side && r.info.price == price) at.push_back(&r);
        }
        std::sort(at.begin(), at.end(),
                  [](const Resting* a, const Resting* b) { return a->seq < b->seq; });
        std::vector<OrderInfo> out;
        for (const Resting* r : at) out.push_back(r->info);
        return out;
    }

    std::optional<OrderInfo> findOrder(OrderId id) const {
        const std::size_t i = indexOf(id);
        if (i == kNone) return std::nullopt;
        return orders_[i].info;
    }

    Qty volumeAt(Side side, Price price) const {
        Qty total = 0;
        for (const Resting& r : orders_) {
            if (r.info.side == side && r.info.price == price) total = addToLevel(total, r.info.remaining);
        }
        return total;
    }

    TradeId lastTradeId() const { return last_trade_id_; }

    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why) *why = msg;
            return false;
        };
        std::set<OrderId> ids;
        std::set<std::uint64_t> seqs;
        std::map<std::pair<Side, Price>, Qty> level_totals;
        for (const Resting& r : orders_) {
            if (r.info.remaining <= 0) return fail("order " + std::to_string(r.info.id) + " has remaining <= 0");
            if (r.info.price <= 0) return fail("order " + std::to_string(r.info.id) + " has price <= 0");
            if (!ids.insert(r.info.id).second) return fail("duplicate live id " + std::to_string(r.info.id));
            if (!seqs.insert(r.seq).second) return fail("duplicate arrival sequence");
            Qty& total = level_totals[{r.info.side, r.info.price}];
            if (__builtin_add_overflow(total, r.info.remaining, &total) || total > kMaxLevelQty)
                return fail("level at price " + std::to_string(r.info.price) + ": total quantity exceeds kMaxLevelQty");
        }
        const std::optional<Price> bid = bestBid();
        const std::optional<Price> ask = bestAsk();
        if (bid && ask && *bid >= *ask) return fail("book crossed");
        return true;
    }

private:
    struct Resting {
        OrderInfo info;
        std::uint64_t seq;  // 到达序号：越小越早
    };

    struct TakerSpec {
        OrderId id;
        AccountId account;
        Side side;
        bool market;
        Price limit;  // 市价单不使用
        StpMode stp;
    };

    struct DiscardEvents {
        void onTrade(const Trade&) {}
        void onCancelled(OrderId, Qty, CancelReason) {}
    };

    static constexpr std::size_t kNone = SIZE_MAX;

    static Side other(Side side) { return side == Side::Buy ? Side::Sell : Side::Buy; }

    // 同侧两单 a 是否排在 b 前面：价格优先，其次到达顺序
    static bool ahead(const Resting& a, const Resting& b) {
        if (a.info.price != b.info.price)
            return a.info.side == Side::Buy ? a.info.price > b.info.price : a.info.price < b.info.price;
        return a.seq < b.seq;
    }

    static std::vector<const Resting*> inPriority(const std::vector<Resting>& book, Side side) {
        std::vector<const Resting*> v;
        for (const Resting& r : book) {
            if (r.info.side == side) v.push_back(&r);
        }
        std::sort(v.begin(), v.end(), [](const Resting* a, const Resting* b) { return ahead(*a, *b); });
        return v;
    }

    // 某侧排第一的订单下标；该侧为空返回 kNone
    static std::size_t topOf(const std::vector<Resting>& book, Side side) {
        std::size_t best = kNone;
        for (std::size_t i = 0; i < book.size(); ++i) {
            if (book[i].info.side != side) continue;
            if (best == kNone || ahead(book[i], book[best])) best = i;
        }
        return best;
    }

    // 撮合：taker 吃 book 中对手方的挂单。返回 false 表示被 STP 终止（CancelTaker/CancelBoth）。
    template <class Sink>
    static bool sweep(std::vector<Resting>& book, Sink& sink, TradeId& last_trade_id,
                      const TakerSpec& taker, Qty& left) {
        while (left > 0) {
            const std::size_t top = topOf(book, other(taker.side));
            if (top == kNone) break;
            const OrderInfo maker = book[top].info;
            const bool price_ok = taker.market || (taker.side == Side::Buy ? taker.limit >= maker.price
                                                                           : taker.limit <= maker.price);
            if (!price_ok) break;
            if (taker.stp != StpMode::None && maker.account == taker.account) {
                if (taker.stp == StpMode::CancelTaker) {
                    sink.onCancelled(taker.id, left, CancelReason::SelfTradePrevention);
                    return false;
                }
                book.erase(book.begin() + static_cast<std::ptrdiff_t>(top));
                sink.onCancelled(maker.id, maker.remaining, CancelReason::SelfTradePrevention);
                if (taker.stp == StpMode::CancelBoth) {
                    sink.onCancelled(taker.id, left, CancelReason::SelfTradePrevention);
                    return false;
                }
                continue;  // CancelMaker
            }
            const Qty q = std::min(left, maker.remaining);
            left -= q;
            const Qty maker_left = maker.remaining - q;
            if (maker_left == 0) {
                book.erase(book.begin() + static_cast<std::ptrdiff_t>(top));
            } else {
                book[top].info.remaining = maker_left;
            }
            sink.onTrade(Trade{++last_trade_id, maker.id, taker.id, maker.account, taker.account,
                               taker.side, maker.price, q, maker_left, left});
        }
        return true;
    }

    std::optional<Price> bestPrice(Side side) const {
        const std::size_t top = topOf(orders_, side);
        if (top == kNone) return std::nullopt;
        return orders_[top].info.price;
    }

    bool wouldCross(Side side, Price p) const {
        const std::optional<Price> best = bestPrice(other(side));
        if (!best) return false;
        return side == Side::Buy ? p >= *best : p <= *best;
    }

    std::size_t indexOf(OrderId id) const {
        for (std::size_t i = 0; i < orders_.size(); ++i) {
            if (orders_[i].info.id == id) return i;
        }
        return kNone;
    }

    std::optional<RejectReason> newOrderRejection(const OrderRequest& r) const {
        if (r.qty <= 0) return RejectReason::InvalidQuantity;
        if (r.type == OrderType::Limit && r.price <= 0) return RejectReason::InvalidPrice;
        const bool limit_gtc = r.type == OrderType::Limit && r.tif == TimeInForce::GTC;
        if (r.post_only && !limit_gtc) return RejectReason::InvalidFlags;
        if (indexOf(r.id) != kNone) return RejectReason::DuplicateOrderId;
        if (max_orders_ != 0 && limit_gtc && orders_.size() >= max_orders_)
            return RejectReason::CapacityExceeded;
        // 只有限价 GTC 会挂单；新单的 id 不在簿上，没有需要排除的订单
        if (limit_gtc && exceedsLevelBound(r.side, r.price, std::nullopt, r.qty)) return RejectReason::InvalidQuantity;
        if (r.post_only && wouldCross(r.side, r.price)) return RejectReason::PostOnlyWouldCross;
        return std::nullopt;
    }

    // 档位总量上限：把 (side, price) 上除 exclude 之外的挂单逐单加到 qty 上，结果是否超过 kMaxLevelQty
    // （任何一步加法溢出都算超过）
    bool exceedsLevelBound(Side side, Price price, std::optional<OrderId> exclude, Qty qty) const {
        Qty total = qty;
        for (const Resting& r : orders_) {
            if (r.info.side != side || r.info.price != price || (exclude && r.info.id == *exclude)) continue;
            if (__builtin_add_overflow(total, r.info.remaining, &total)) return true;
        }
        return total > kMaxLevelQty;
    }

    // 查询时累加档位总量：上限规则保证结果不超过 kMaxLevelQty；加法溢出说明本模型自身有错，直接中止
    static Qty addToLevel(Qty total, Qty qty) {
        Qty sum = 0;
        if (__builtin_add_overflow(total, qty, &sum) || sum > kMaxLevelQty) {
            std::fprintf(stderr, "NaiveOrderBook internal error: level total exceeds kMaxLevelQty\n");
            std::abort();
        }
        return sum;
    }

    void insert(const OrderInfo& info) { orders_.push_back(Resting{info, next_seq_++}); }

    Listener& listener_;
    std::size_t max_orders_;
    std::vector<Resting> orders_;
    std::uint64_t next_seq_ = 0;
    TradeId last_trade_id_ = 0;
};

}  // namespace hft::matching::test
