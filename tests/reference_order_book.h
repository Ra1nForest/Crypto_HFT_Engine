//
// reference_order_book.h
// 撮合引擎参考模型（测试预言机）：用最朴素的 std 容器逐条实现 docs/MATCHING_ENGINE.md「Semantics」中的规则，
// 作为差分测试的标准答案。正确性一目了然优先，完全不考虑性能。
//

#pragma once

#include "matching/types.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <list>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace hft::matching {

// 命名空间：ReferenceOrderBook 与优化版 LimitOrderBook 并列放在 hft::matching，
// 类名即可区分两种实现；它只存在于 tests/ 目录，不进入生产代码。
// 接口与 LimitOrderBook 完全一致（docs/MATCHING_ENGINE.md），因此同一套规范测试可以原样运行在两者之上。
//
// 数据结构（刻意保持朴素）：
// - 每侧一个 std::map<Price, std::list<OrderInfo>>，比较器保证 begin() 恒为最优档
//   （买盘价高者在前、卖盘价低者在前）；同一档内 list 头部即时间优先级最高的订单
// - 订单索引 std::unordered_map<OrderId, list 迭代器>：list 迭代器在其他元素增删时保持有效
// - 档位总量与订单数在查询时现算，不做任何缓存（档位总量上限检查另用只增不减的 max_rested_qty_
//   做 O(1) 的保守预判，见 fitsInLevel()；它不影响任何查询结果）
//
// 回调时机：每个事件发出时，簿内状态已包含该事件的效果（例如成交回报发出时，完全成交的
// maker 已被移除）。规范只约束事件流本身，不约束回调期间可见的簿状态；监听器不得在回调中
// 重入调用本簿的修改接口。
//
// 档位总量上限（契约的一部分，见 matching/types.h 的 kMaxLevelQty）：会让某档总量超过 INT64_MAX 的
// 限价 GTC 新单与改价/加量改单在 onAccepted / onModified 之前以 InvalidQuantity 拒绝，因此现算的档位
// 总量不会溢出（累加前仍逐项检查，越界即为本模型自身的错误，直接中止）；FOK 预检用"仍需数量"做减法，
// 跨档累计也不会溢出。
template <class Listener>
class ReferenceOrderBook {
public:
    explicit ReferenceOrderBook(Listener& listener, std::size_t max_orders = 0,
                                std::size_t reserve_orders = 1024)
        : listener_(listener),
          max_orders_(max_orders),
          bids_(PricePriority{Side::Buy}),
          asks_(PricePriority{Side::Sell}) {
        index_.reserve(reserve_orders);
    }

    // 索引保存 list 迭代器，拷贝后会指向原对象的节点：禁止拷贝
    ReferenceOrderBook(const ReferenceOrderBook&) = delete;
    ReferenceOrderBook& operator=(const ReferenceOrderBook&) = delete;

    // ---- 新订单（docs/MATCHING_ENGINE.md「submit」）----
    void submit(const OrderRequest& req) {
        // 校验：按规范顺序检查，第一条失败的规则生效，且只发出这一个拒单事件
        if (req.qty <= 0) {
            reject(req.id, RequestType::New, RejectReason::InvalidQuantity);
            return;
        }
        if (req.type == OrderType::Limit && req.price <= 0) {
            reject(req.id, RequestType::New, RejectReason::InvalidPrice);
            return;
        }
        if (req.post_only && (req.type != OrderType::Limit || req.tif != TimeInForce::GTC)) {
            reject(req.id, RequestType::New, RejectReason::InvalidFlags);
            return;
        }
        if (isLive(req.id)) {
            reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
            return;
        }
        if (max_orders_ != 0 && orderCount() >= max_orders_ &&
            req.type == OrderType::Limit && req.tif == TimeInForce::GTC) {
            reject(req.id, RequestType::New, RejectReason::CapacityExceeded);
            return;
        }
        // 档位总量上限：只有限价 GTC 会挂单。撮合循环从不触及 taker 自己这一侧；本侧该价位已有档位时
        // 订单也不可能穿价（盘口不交叉），会整单挂上去——所以这条检查恰好拒绝会让该档总量越界的请求
        // （该价位没有档位时检查恒通过）
        if (req.type == OrderType::Limit && req.tif == TimeInForce::GTC &&
            !fitsInLevel(req.side, req.price, req.qty, 0)) {
            reject(req.id, RequestType::New, RejectReason::InvalidQuantity);
            return;
        }
        if (req.post_only && crosses(req.side, req.price)) {
            reject(req.id, RequestType::New, RejectReason::PostOnlyWouldCross);
            return;
        }

        listener_.onAccepted(req);

        // FOK：先按撮合顺序预检能否全部成交；不能则整单撤销，簿不受任何影响
        if (req.tif == TimeInForce::FOK && !fokFillable(req)) {
            listener_.onCancelled(req.id, req.qty, CancelReason::FokUnfillable);
            return;
        }

        Taker taker{req.id, req.account, req.side, req.type, req.price, req.qty, req.stp};
        if (!matchTaker(taker)) return;      // 被自成交保护终止：不挂单
        if (taker.remaining == 0) return;    // 全部成交

        if (req.type == OrderType::Market) {
            listener_.onCancelled(req.id, taker.remaining, CancelReason::MarketRemainder);
            return;
        }
        if (req.tif == TimeInForce::IOC) {
            listener_.onCancelled(req.id, taker.remaining, CancelReason::IocRemainder);
            return;
        }
        if (req.tif == TimeInForce::FOK) {
            // 预检保证 FOK 不会走到这里；走到即说明参考模型自身有错
            fatal("FOK order has a remainder after a successful pre-check");
        }
        // 限价 GTC：剩余量挂到该价位 FIFO 队尾，不发事件
        rest(OrderInfo{req.id, req.account, req.side, req.price, taker.remaining,
                       req.post_only, req.stp});
    }

    // ---- 撤单 ----
    void cancel(OrderId id) {
        auto found = index_.find(id);
        if (found == index_.end()) {
            reject(id, RequestType::Cancel, RejectReason::UnknownOrder);
            return;
        }
        const Qty remaining = found->second->remaining;
        removeOrder(id);
        listener_.onCancelled(id, remaining, CancelReason::UserRequested);
    }

    // ---- 改单：new_qty 是新的剩余（未成交）数量 ----
    void modify(OrderId id, Price new_price, Qty new_qty) {
        auto found = index_.find(id);
        if (found == index_.end()) {
            reject(id, RequestType::Modify, RejectReason::UnknownOrder);
            return;
        }
        OrderInfo& order = *found->second;
        if (new_qty <= 0) {
            reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
            return;
        }
        if (new_price <= 0) {
            reject(id, RequestType::Modify, RejectReason::InvalidPrice);
            return;
        }
        // 档位总量上限：改价或加量会把订单重新挂到 new_price（原地减量不会增加档位总量，不受限）。
        // 目标档位中其他订单的总量 = 该档现有总量；同价加量时扣掉订单自己原来的数量
        if (new_price != order.price || new_qty > order.remaining) {
            const Qty own = new_price == order.price ? order.remaining : 0;
            if (!fitsInLevel(order.side, new_price, new_qty, own)) {
                reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
                return;
            }
        }
        if (order.post_only && new_price != order.price && crosses(order.side, new_price)) {
            reject(id, RequestType::Modify, RejectReason::PostOnlyWouldCross);
            return;
        }

        // 价格不变且数量不增加：原地修改，保留时间优先级（档位总量随之变化）
        if (new_price == order.price && new_qty <= order.remaining) {
            order.remaining = new_qty;
            listener_.onModified(id, order.price, new_qty);
            return;
        }

        // 改价或加量：先移出（失去时间优先级），再作为 taker（限价 GTC）撮合，剩余挂到新价位队尾
        OrderInfo moved = order;
        removeOrder(id);  // 此后 order 引用失效，只使用副本 moved
        moved.price = new_price;
        moved.remaining = new_qty;
        listener_.onModified(id, new_price, new_qty);

        Taker taker{id, moved.account, moved.side, OrderType::Limit, new_price, new_qty, moved.stp};
        if (!matchTaker(taker)) return;      // 被自成交保护撤销
        if (taker.remaining == 0) return;    // 全部成交
        moved.remaining = taker.remaining;
        rest(moved);
    }

    // ---- 查询（const，绝不调用监听器）----
    std::optional<Price> bestBid() const {
        if (bids_.empty()) return std::nullopt;
        return bids_.begin()->first;
    }

    std::optional<Price> bestAsk() const {
        if (asks_.empty()) return std::nullopt;
        return asks_.begin()->first;
    }

    // 活动（挂单中）订单数
    std::size_t orderCount() const { return index_.size(); }

    std::size_t levelCount(Side side) const { return levels(side).size(); }

    // 最优档在前；每档给出剩余总量与订单数
    std::vector<LevelInfo> depth(Side side, std::size_t max_levels = SIZE_MAX) const {
        std::vector<LevelInfo> out;
        for (const auto& [price, orders] : levels(side)) {
            if (out.size() >= max_levels) break;
            out.push_back(LevelInfo{price, levelTotal(orders), static_cast<std::uint32_t>(orders.size())});
        }
        return out;
    }

    // 该价位的活动订单，按 FIFO（时间优先级）顺序；无此价位返回空
    std::vector<OrderInfo> ordersAt(Side side, Price price) const {
        const LevelMap& book_side = levels(side);
        auto level = book_side.find(price);
        if (level == book_side.end()) return {};
        return std::vector<OrderInfo>(level->second.begin(), level->second.end());
    }

    std::optional<OrderInfo> findOrder(OrderId id) const {
        auto found = index_.find(id);
        if (found == index_.end()) return std::nullopt;
        return *found->second;
    }

    Qty volumeAt(Side side, Price price) const {
        const LevelMap& book_side = levels(side);
        auto level = book_side.find(price);
        if (level == book_side.end()) return 0;
        return levelTotal(level->second);
    }

    // 最近一笔成交的 id；尚无成交时为 0
    TradeId lastTradeId() const { return last_trade_id_; }

    // 结构不变量检查：失败时返回 false，并在 why 中写明第一处违例
    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why) *why = msg;
            return false;
        };
        std::size_t resting = 0;
        for (Side side : {Side::Buy, Side::Sell}) {
            const std::string name = toString(side);
            bool first = true;
            Price prev = 0;
            for (const auto& [price, orders] : levels(side)) {
                // 严格排序：前一档必须严格优于当前档
                if (!first && !better(side, prev, price))
                    return fail(name + " side not strictly sorted at price " + std::to_string(price));
                if (price <= 0)
                    return fail(name + " level with non-positive price " + std::to_string(price));
                if (orders.empty())
                    return fail(name + " empty level at price " + std::to_string(price));
                Qty total = 0;
                for (const OrderInfo& o : orders) {
                    if (o.remaining <= 0)
                        return fail("order " + std::to_string(o.id) + " has non-positive remaining " +
                                    std::to_string(o.remaining));
                    // 档位总量上限（remaining > 0 且 total 在 [0, kMaxLevelQty] 内，减法不会溢出）
                    if (o.remaining > kMaxLevelQty - total)
                        return fail(name + " level at price " + std::to_string(price) +
                                    ": total quantity exceeds kMaxLevelQty");
                    total += o.remaining;
                    if (o.remaining > max_rested_qty_)  // fitsInLevel() 的保守判断依赖这一点
                        return fail("order " + std::to_string(o.id) + " has more remaining than any order ever rested");
                    if (o.side != side || o.price != price)
                        return fail("order " + std::to_string(o.id) + " is stored at the wrong level");
                    auto found = index_.find(o.id);
                    if (found == index_.end())
                        return fail("resting order " + std::to_string(o.id) + " is missing from the index");
                    if (&*found->second != &o)
                        return fail("index entry of order " + std::to_string(o.id) +
                                    " points to another order (duplicate id?)");
                    ++resting;
                }
                prev = price;
                first = false;
            }
        }
        // 每个挂单都在索引中且索引指向它自身，再加上数量相等 => 索引与挂单一一对应
        if (resting != index_.size())
            return fail("index holds " + std::to_string(index_.size()) + " entries but " +
                        std::to_string(resting) + " orders rest in the book");
        if (!bids_.empty() && !asks_.empty() && bids_.begin()->first >= asks_.begin()->first)
            return fail("book crossed: best bid " + std::to_string(bids_.begin()->first) +
                        " >= best ask " + std::to_string(asks_.begin()->first));
        return true;
    }

private:
    // 价格优先比较器：买盘价高者在前，卖盘价低者在前（map 的 begin() 即最优档）
    struct PricePriority {
        Side side;
        bool operator()(Price a, Price b) const { return side == Side::Buy ? a > b : a < b; }
    };
    using OrderList = std::list<OrderInfo>;
    using LevelMap = std::map<Price, OrderList, PricePriority>;

    // 撮合中的主动方（新订单，或改价/加量后的订单）
    struct Taker {
        OrderId id;
        AccountId account;
        Side side;
        OrderType type;
        Price price;  // 限价（市价单不使用）
        Qty remaining;
        StpMode stp;
    };

    static Side opposite(Side side) { return side == Side::Buy ? Side::Sell : Side::Buy; }

    // 同一侧上价格 a 是否严格优于 b
    static bool better(Side side, Price a, Price b) { return side == Side::Buy ? a > b : a < b; }

    // side 方向、限价 limit 的订单能否与对手价 opposite_price 成交
    static bool priceAcceptable(Side side, Price limit, Price opposite_price) {
        return side == Side::Buy ? limit >= opposite_price : limit <= opposite_price;
    }

    LevelMap& levels(Side side) { return side == Side::Buy ? bids_ : asks_; }
    const LevelMap& levels(Side side) const { return side == Side::Buy ? bids_ : asks_; }

    bool isLive(OrderId id) const { return index_.count(id) != 0; }

    // 档位总量（现算）。剩余量恒为正、档位总量不超过 kMaxLevelQty 都是本模型维护的不变量；累加前仍逐项
    // 检查（这样 total 始终在 [0, kMaxLevelQty] 内，减法不会溢出），违反即为本模型自身的错误
    static Qty levelTotal(const OrderList& orders) {
        Qty total = 0;
        for (const OrderInfo& o : orders) {
            if (o.remaining <= 0 || o.remaining > kMaxLevelQty - total)
                fatal("level total: non-positive remaining or total above kMaxLevelQty");
            total += o.remaining;
        }
        return total;
    }

    // 档位总量上限：把 qty 挂到 (side, price) 之后，该档总量（先扣掉 own：同价加量的改单里订单自己原来的
    // 剩余量）是否仍不超过 kMaxLevelQty。先做 O(1) 的保守判断：剩余量只在 rest() 挂单时设定、之后只减不增，
    // 所以每个挂单的剩余量都不超过 max_rested_qty_，该档总量不超过"订单数 × max_rested_qty_"——这个上界
    // 放得下就一定放得下（正常流量都走这里，不必逐单求和）；否则按定义逐单求和后精确判断
    bool fitsInLevel(Side side, Price price, Qty qty, Qty own) const {
        const LevelMap& book_side = levels(side);
        const auto level = book_side.find(price);
        if (level == book_side.end()) return true;  // 空价位（此时 own 必为 0）：qty 本身不超过上限
        const auto orders = static_cast<Qty>(level->second.size());
        if (orders <= (kMaxLevelQty - qty) / max_rested_qty_) return true;  // 有挂单，max_rested_qty_ >= 1
        return qty <= kMaxLevelQty - (levelTotal(level->second) - own);
    }

    // 规范定义：买单 —— 卖盘非空且 p >= 卖一；卖单 —— 买盘非空且 p <= 买一
    bool crosses(Side side, Price p) const {
        if (side == Side::Buy) return !asks_.empty() && p >= asks_.begin()->first;
        return !bids_.empty() && p <= bids_.begin()->first;
    }

    // 自成交保护是否适用于 (taker, maker)：只看 taker 的 STP 模式与双方账户
    static bool stpApplies(StpMode taker_stp, AccountId taker_account, const OrderInfo& maker) {
        return taker_stp != StpMode::None && maker.account == taker_account;
    }

    void reject(OrderId id, RequestType request, RejectReason reason) {
        listener_.onRejected(id, request, reason);
    }

    // FOK 预检：按撮合顺序遍历可成交价位上的 maker，累计可成交量。
    // STP 适用的 maker：CancelMaker 跳过（贡献 0、继续），CancelTaker/CancelBoth 停止遍历。
    // 用"仍需数量"做减法代替求和，避免大数量相加溢出（与 "累计量 >= qty" 完全等价）。
    bool fokFillable(const OrderRequest& req) const {
        Qty still_needed = req.qty;
        for (const auto& [price, orders] : levels(opposite(req.side))) {
            if (req.type == OrderType::Limit && !priceAcceptable(req.side, req.price, price)) break;
            for (const OrderInfo& maker : orders) {
                if (stpApplies(req.stp, req.account, maker)) {
                    if (req.stp == StpMode::CancelMaker) continue;
                    return false;  // 停止遍历：到此为止的累计量 < req.qty
                }
                if (maker.remaining >= still_needed) return true;
                still_needed -= maker.remaining;
            }
        }
        return false;
    }

    // 撮合循环。返回 false 表示 taker 被自成交保护（CancelTaker/CancelBoth）终止。
    bool matchTaker(Taker& taker) {
        LevelMap& book_side = levels(opposite(taker.side));
        while (taker.remaining > 0 && !book_side.empty() &&
               (taker.type == OrderType::Market ||
                priceAcceptable(taker.side, taker.price, book_side.begin()->first))) {
            auto level = book_side.begin();
            OrderInfo& maker = level->second.front();

            if (stpApplies(taker.stp, taker.account, maker)) {
                const OrderId maker_id = maker.id;
                const Qty maker_remaining = maker.remaining;
                switch (taker.stp) {
                    case StpMode::CancelMaker:
                        removeOrder(maker_id);
                        listener_.onCancelled(maker_id, maker_remaining,
                                              CancelReason::SelfTradePrevention);
                        continue;  // 继续撮合
                    case StpMode::CancelTaker:
                        listener_.onCancelled(taker.id, taker.remaining,
                                              CancelReason::SelfTradePrevention);
                        return false;
                    case StpMode::CancelBoth:
                        removeOrder(maker_id);
                        listener_.onCancelled(maker_id, maker_remaining,
                                              CancelReason::SelfTradePrevention);
                        listener_.onCancelled(taker.id, taker.remaining,
                                              CancelReason::SelfTradePrevention);
                        return false;
                    case StpMode::None:
                        break;
                }
                fatal("stpApplies returned true for StpMode::None");
            }

            const Qty q = std::min(taker.remaining, maker.remaining);
            maker.remaining -= q;
            taker.remaining -= q;
            const Trade trade{++last_trade_id_, maker.id,      taker.id,
                              maker.account,    taker.account, taker.side,
                              level->first,     q,             maker.remaining,
                              taker.remaining};
            if (maker.remaining == 0) removeOrder(trade.maker_id);  // 完全成交：移除，不另发事件
            listener_.onTrade(trade);
        }
        return true;
    }

    // 挂单：放到该价位 FIFO 队尾（价位不存在则创建）
    void rest(const OrderInfo& order) {
        OrderList& queue = levels(order.side)[order.price];
        queue.push_back(order);
        index_[order.id] = std::prev(queue.end());
        max_rested_qty_ = std::max(max_rested_qty_, order.remaining);
    }

    // 移除一个活动订单；价位空了就删除该价位
    void removeOrder(OrderId id) {
        auto found = index_.find(id);
        if (found == index_.end()) fatal("removeOrder: order is not live");
        const typename OrderList::iterator it = found->second;
        LevelMap& book_side = levels(it->side);
        auto level = book_side.find(it->price);
        if (level == book_side.end()) fatal("removeOrder: level of a live order is missing");
        index_.erase(found);
        level->second.erase(it);
        if (level->second.empty()) book_side.erase(level);
    }

    [[noreturn]] static void fatal(const char* msg) {
        std::fprintf(stderr, "ReferenceOrderBook internal error: %s\n", msg);
        std::abort();
    }

    Listener& listener_;
    std::size_t max_orders_;  // 0 = 不限
    LevelMap bids_;
    LevelMap asks_;
    std::unordered_map<OrderId, typename OrderList::iterator> index_;
    TradeId last_trade_id_ = 0;
    Qty max_rested_qty_ = 0;  // 曾经挂单的最大剩余量（只增不减）：任何挂单的剩余量都不超过它
};

}  // namespace hft::matching
