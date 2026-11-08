//
// matching_test_util.h
// 撮合引擎测试公共工具：事件记录、命令回放、随机命令生成器、两个订单簿的状态比对与断言宏。
// 对任何满足 docs/MATCHING_ENGINE.md 所述接口的订单簿都适用（参考模型、优化引擎、朴素模型）。
//

#pragma once

#include "matching/types.h"
#include "test_framework.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace hft::matching::test {

// ============================================================================
// 字符串化（测试失败信息用）。统一命名为 str()，避免与 hft::matching::toString 重名遮蔽。
// ============================================================================

inline std::string str(const OrderRequest& r) {
    std::string s = "id=" + std::to_string(r.id) + " acct=" + std::to_string(r.account) + " " +
                    matching::toString(r.side) + " " + matching::toString(r.type) + " " +
                    matching::toString(r.tif) + " px=" + std::to_string(r.price) +
                    " qty=" + std::to_string(r.qty);
    if (r.post_only) s += " post_only";
    s += " stp=";
    s += matching::toString(r.stp);
    return s;
}

inline std::string str(const Trade& t) {
    return "tid=" + std::to_string(t.trade_id) + " maker=" + std::to_string(t.maker_id) +
           " taker=" + std::to_string(t.taker_id) + " maker_acct=" + std::to_string(t.maker_account) +
           " taker_acct=" + std::to_string(t.taker_account) + " taker_side=" +
           matching::toString(t.taker_side) + " px=" + std::to_string(t.price) +
           " qty=" + std::to_string(t.qty) + " maker_rem=" + std::to_string(t.maker_remaining) +
           " taker_rem=" + std::to_string(t.taker_remaining);
}

inline std::string str(const LevelInfo& l) {
    return "{px=" + std::to_string(l.price) + " qty=" + std::to_string(l.qty) +
           " orders=" + std::to_string(l.orders) + "}";
}

inline std::string str(const OrderInfo& o) {
    std::string s = "{id=" + std::to_string(o.id) + " acct=" + std::to_string(o.account) + " " +
                    matching::toString(o.side) + " px=" + std::to_string(o.price) +
                    " rem=" + std::to_string(o.remaining);
    if (o.post_only) s += " post_only";
    s += " stp=";
    s += matching::toString(o.stp);
    s += "}";
    return s;
}

inline std::string str(const std::optional<Price>& p) { return p ? std::to_string(*p) : "none"; }

inline std::string str(const std::optional<OrderInfo>& o) { return o ? str(*o) : "none"; }

// 请求逐字段相等（OrderRequest 本身不定义 operator==）
inline bool sameRequest(const OrderRequest& a, const OrderRequest& b) {
    return a.id == b.id && a.account == b.account && a.side == b.side && a.type == b.type &&
           a.tif == b.tif && a.price == b.price && a.qty == b.qty && a.post_only == b.post_only &&
           a.stp == b.stp;
}

// ============================================================================
// 事件：监听器每个回调对应一个 Event；比较时只比较该类事件的相关字段
// ============================================================================

struct Event {
    enum class Kind : std::uint8_t { Accepted, Rejected, Trade, Cancelled, Modified };

    Kind kind = Kind::Accepted;
    OrderId id = 0;                                              // 订单 id（Trade 事件为 taker_id）
    OrderRequest request{};                                      // Accepted：原样回显的请求
    RequestType request_type = RequestType::New;                 // Rejected
    RejectReason reject_reason = RejectReason::InvalidQuantity;  // Rejected
    Trade trade{};                                               // Trade
    Price price = 0;                                             // Modified：新价格
    Qty qty = 0;                                                 // Cancelled：撤销量；Modified：新数量
    CancelReason cancel_reason = CancelReason::UserRequested;    // Cancelled

    std::string toString() const {
        switch (kind) {
            case Kind::Accepted:
                return "Accepted{" + str(request) + "}";
            case Kind::Rejected:
                return "Rejected{id=" + std::to_string(id) + " request=" +
                       matching::toString(request_type) + " reason=" +
                       matching::toString(reject_reason) + "}";
            case Kind::Trade:
                return "Trade{" + str(trade) + "}";
            case Kind::Cancelled:
                return "Cancelled{id=" + std::to_string(id) + " qty=" + std::to_string(qty) +
                       " reason=" + matching::toString(cancel_reason) + "}";
            case Kind::Modified:
                return "Modified{id=" + std::to_string(id) + " px=" + std::to_string(price) +
                       " qty=" + std::to_string(qty) + "}";
        }
        return "Event{?}";
    }
};

inline bool operator==(const Event& a, const Event& b) {
    if (a.kind != b.kind) return false;
    switch (a.kind) {
        case Event::Kind::Accepted:
            return sameRequest(a.request, b.request);
        case Event::Kind::Rejected:
            return a.id == b.id && a.request_type == b.request_type &&
                   a.reject_reason == b.reject_reason;
        case Event::Kind::Trade:
            return a.trade == b.trade;
        case Event::Kind::Cancelled:
            return a.id == b.id && a.qty == b.qty && a.cancel_reason == b.cancel_reason;
        case Event::Kind::Modified:
            return a.id == b.id && a.price == b.price && a.qty == b.qty;
    }
    return false;
}
inline bool operator!=(const Event& a, const Event& b) { return !(a == b); }

inline std::string str(const Event& e) { return e.toString(); }

// ---- 事件构造函数（测试中写期望序列用）----

inline Event accepted(const OrderRequest& r) {
    Event e;
    e.kind = Event::Kind::Accepted;
    e.id = r.id;
    e.request = r;
    return e;
}

inline Event rejected(OrderId id, RequestType request, RejectReason reason) {
    Event e;
    e.kind = Event::Kind::Rejected;
    e.id = id;
    e.request_type = request;
    e.reject_reason = reason;
    return e;
}
inline Event rejectedNew(OrderId id, RejectReason reason) {
    return rejected(id, RequestType::New, reason);
}
inline Event rejectedCancel(OrderId id, RejectReason reason = RejectReason::UnknownOrder) {
    return rejected(id, RequestType::Cancel, reason);
}
inline Event rejectedModify(OrderId id, RejectReason reason) {
    return rejected(id, RequestType::Modify, reason);
}

inline Event trade(const Trade& t) {
    Event e;
    e.kind = Event::Kind::Trade;
    e.id = t.taker_id;
    e.trade = t;
    return e;
}
// 参数顺序与 Trade 字段顺序一致
inline Event trade(TradeId trade_id, OrderId maker_id, OrderId taker_id, AccountId maker_account,
                   AccountId taker_account, Side taker_side, Price price, Qty qty,
                   Qty maker_remaining, Qty taker_remaining) {
    return trade(Trade{trade_id, maker_id, taker_id, maker_account, taker_account, taker_side,
                       price, qty, maker_remaining, taker_remaining});
}

inline Event cancelled(OrderId id, Qty qty, CancelReason reason) {
    Event e;
    e.kind = Event::Kind::Cancelled;
    e.id = id;
    e.qty = qty;
    e.cancel_reason = reason;
    return e;
}

inline Event modified(OrderId id, Price price, Qty qty) {
    Event e;
    e.kind = Event::Kind::Modified;
    e.id = id;
    e.price = price;
    e.qty = qty;
    return e;
}

// ============================================================================
// 记录型监听器：把所有回调按顺序记成 Event
// ============================================================================

struct RecordingListener {
    std::vector<Event> events;

    void onAccepted(const OrderRequest& req) { events.push_back(accepted(req)); }
    void onRejected(OrderId id, RequestType request, RejectReason reason) {
        events.push_back(rejected(id, request, reason));
    }
    void onTrade(const Trade& t) { events.push_back(trade(t)); }
    void onCancelled(OrderId id, Qty cancelled_qty, CancelReason reason) {
        events.push_back(cancelled(id, cancelled_qty, reason));
    }
    void onModified(OrderId id, Price new_price, Qty new_qty) {
        events.push_back(modified(id, new_price, new_qty));
    }

    // 取走已记录的事件并清空
    std::vector<Event> take() { return std::exchange(events, {}); }
    void clear() { events.clear(); }
};

// 只计数的监听器：不分配、不哈希（零分配测试与基准测试用）
struct CountingListener {
    std::uint64_t events = 0;
    std::uint64_t trades = 0;
    std::uint64_t traded_qty = 0;

    void onAccepted(const OrderRequest&) { ++events; }
    void onRejected(OrderId, RequestType, RejectReason) { ++events; }
    void onTrade(const Trade& t) {
        ++events;
        ++trades;
        traded_qty += static_cast<std::uint64_t>(t.qty);
    }
    void onCancelled(OrderId, Qty, CancelReason) { ++events; }
    void onModified(OrderId, Price, Qty) { ++events; }
};

// ============================================================================
// 命令：New / Cancel / Modify，可回放到任意订单簿
// ============================================================================

struct Command {
    RequestType type = RequestType::New;
    OrderRequest order{};  // New：完整请求
    OrderId id = 0;        // 目标订单 id（New 时等于 order.id）
    Price price = 0;       // Modify：新价格
    Qty qty = 0;           // Modify：新数量

    static Command newOrder(const OrderRequest& r) {
        Command c;
        c.type = RequestType::New;
        c.order = r;
        c.id = r.id;
        return c;
    }
    static Command cancel(OrderId id) {
        Command c;
        c.type = RequestType::Cancel;
        c.id = id;
        return c;
    }
    static Command modify(OrderId id, Price price, Qty qty) {
        Command c;
        c.type = RequestType::Modify;
        c.id = id;
        c.price = price;
        c.qty = qty;
        return c;
    }

    std::string toString() const {
        switch (type) {
            case RequestType::New: return "New{" + str(order) + "}";
            case RequestType::Cancel: return "Cancel{id=" + std::to_string(id) + "}";
            case RequestType::Modify:
                return "Modify{id=" + std::to_string(id) + " px=" + std::to_string(price) +
                       " qty=" + std::to_string(qty) + "}";
        }
        return "Command{?}";
    }
};

inline std::string str(const Command& c) { return c.toString(); }

template <class Book>
void apply(Book& book, const Command& cmd) {
    switch (cmd.type) {
        case RequestType::New: book.submit(cmd.order); return;
        case RequestType::Cancel: book.cancel(cmd.id); return;
        case RequestType::Modify: book.modify(cmd.id, cmd.price, cmd.qty); return;
    }
}

// ============================================================================
// 订单请求链式构造器：默认限价 GTC、账户 1、非 post-only、无 STP
// 例：buy(7, 100, 5).account(2).ioc().stp(StpMode::CancelMaker)
// ============================================================================

class Req {
public:
    Req(OrderId id, Side side, Price price, Qty qty) {
        r_.id = id;
        r_.account = 1;
        r_.side = side;
        r_.price = price;
        r_.qty = qty;
    }
    Req& account(AccountId a) { r_.account = a; return *this; }
    Req& market() { r_.type = OrderType::Market; return *this; }
    Req& tif(TimeInForce t) { r_.tif = t; return *this; }
    Req& gtc() { return tif(TimeInForce::GTC); }
    Req& ioc() { return tif(TimeInForce::IOC); }
    Req& fok() { return tif(TimeInForce::FOK); }
    Req& postOnly(bool v = true) { r_.post_only = v; return *this; }
    Req& stp(StpMode m) { r_.stp = m; return *this; }
    Req& price(Price p) { r_.price = p; return *this; }

    operator OrderRequest() const { return r_; }  // NOLINT: 允许隐式转换，测试书写更简洁

private:
    OrderRequest r_;
};

inline Req buy(OrderId id, Price price, Qty qty) { return Req(id, Side::Buy, price, qty); }
inline Req sell(OrderId id, Price price, Qty qty) { return Req(id, Side::Sell, price, qty); }
// 市价单：price 字段为 0（规范规定市价单忽略价格）
inline Req marketBuy(OrderId id, Qty qty) { return Req(id, Side::Buy, 0, qty).market(); }
inline Req marketSell(OrderId id, Qty qty) { return Req(id, Side::Sell, 0, qty).market(); }

// 由请求推出它挂单后的查询行
inline OrderInfo infoOf(const OrderRequest& r, Qty remaining) {
    return OrderInfo{r.id, r.account, r.side, r.price, remaining, r.post_only, r.stp};
}
inline OrderInfo infoOf(const OrderRequest& r) { return infoOf(r, r.qty); }

// ============================================================================
// 列表字符串化与比较/断言辅助
// ============================================================================

template <class T>
std::string str(const std::vector<T>& v) {
    std::string s = "[";
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i) s += ", ";
        s += str(v[i]);
    }
    s += "]";
    return s;
}

// 两个事件序列逐条比较；不同则返回 false，并在 why 中给出带标记的逐行对照
inline bool sameEvents(const std::vector<Event>& actual, const std::vector<Event>& expected,
                       std::string* why = nullptr) {
    if (actual == expected) return true;
    if (why) {
        std::ostringstream ss;
        ss << "event sequence mismatch (actual " << actual.size() << " vs expected "
           << expected.size() << " events; '>>' marks differences)\n";
        const std::size_t n = std::max(actual.size(), expected.size());
        for (std::size_t i = 0; i < n; ++i) {
            const bool same = i < actual.size() && i < expected.size() && actual[i] == expected[i];
            ss << "      " << (same ? "  " : ">>") << " [" << i << "] expected: "
               << (i < expected.size() ? expected[i].toString() : std::string("<none>")) << "\n"
               << "              actual:   "
               << (i < actual.size() ? actual[i].toString() : std::string("<none>")) << "\n";
        }
        *why = ss.str();
    }
    return false;
}

inline bool checkEvents(const std::vector<Event>& actual, const std::vector<Event>& expected,
                        const char* file, int line) {
    std::string why;
    if (sameEvents(actual, expected, &why)) return true;
    ::hft_test::reportFailure(file, line, why);
    return false;
}

template <class T>
bool checkList(const std::vector<T>& actual, const std::vector<T>& expected, const char* what,
               const char* file, int line) {
    if (actual == expected) return true;
    ::hft_test::reportFailure(file, line, std::string(what) + "\n      expected: " + str(expected) +
                                              "\n      actual:   " + str(actual));
    return false;
}

template <class Book>
bool checkTop(const Book& book, const std::optional<Price>& bid, const std::optional<Price>& ask,
              const char* file, int line) {
    const std::optional<Price> actual_bid = book.bestBid();
    const std::optional<Price> actual_ask = book.bestAsk();
    if (actual_bid == bid && actual_ask == ask) return true;
    ::hft_test::reportFailure(file, line, "top of book: expected bid " + str(bid) + " / ask " +
                                              str(ask) + ", actual bid " + str(actual_bid) +
                                              " / ask " + str(actual_ask));
    return false;
}

template <class Book>
bool checkOrder(const Book& book, OrderId id, const std::optional<OrderInfo>& expected,
                const char* file, int line) {
    const std::optional<OrderInfo> actual = book.findOrder(id);
    if (actual == expected) return true;
    ::hft_test::reportFailure(file, line, "findOrder(" + std::to_string(id) + "): expected " +
                                              str(expected) + ", actual " + str(actual));
    return false;
}

// ============================================================================
// 状态快照：用于断言"某操作没有任何副作用"
// ============================================================================

struct StateSnapshot {
    std::optional<Price> best_bid;
    std::optional<Price> best_ask;
    std::size_t order_count = 0;
    std::vector<LevelInfo> bids;
    std::vector<LevelInfo> asks;
    std::vector<OrderInfo> bid_orders;  // 按档位（优先级）与 FIFO 顺序展开的全部挂单
    std::vector<OrderInfo> ask_orders;
    TradeId last_trade_id = 0;
};

inline bool operator==(const StateSnapshot& a, const StateSnapshot& b) {
    return a.best_bid == b.best_bid && a.best_ask == b.best_ask && a.order_count == b.order_count &&
           a.bids == b.bids && a.asks == b.asks && a.bid_orders == b.bid_orders &&
           a.ask_orders == b.ask_orders && a.last_trade_id == b.last_trade_id;
}

inline std::string str(const StateSnapshot& s) {
    return "bid=" + str(s.best_bid) + " ask=" + str(s.best_ask) +
           " orders=" + std::to_string(s.order_count) + " last_trade=" +
           std::to_string(s.last_trade_id) + "\n        bids: " + str(s.bids) +
           "\n        asks: " + str(s.asks) + "\n        bid orders: " + str(s.bid_orders) +
           "\n        ask orders: " + str(s.ask_orders);
}

template <class Book>
StateSnapshot snapshotOf(const Book& book) {
    StateSnapshot s;
    s.best_bid = book.bestBid();
    s.best_ask = book.bestAsk();
    s.order_count = book.orderCount();
    s.bids = book.depth(Side::Buy);
    s.asks = book.depth(Side::Sell);
    for (const LevelInfo& l : s.bids) {
        for (const OrderInfo& o : book.ordersAt(Side::Buy, l.price)) s.bid_orders.push_back(o);
    }
    for (const LevelInfo& l : s.asks) {
        for (const OrderInfo& o : book.ordersAt(Side::Sell, l.price)) s.ask_orders.push_back(o);
    }
    s.last_trade_id = book.lastTradeId();
    return s;
}

template <class Book>
bool checkUnchanged(const StateSnapshot& before, const Book& book, const char* file, int line) {
    const StateSnapshot after = snapshotOf(book);
    if (after == before) return true;
    ::hft_test::reportFailure(file, line, "book state changed\n      before: " + str(before) +
                                              "\n      after:  " + str(after));
    return false;
}

// ============================================================================
// 两个订单簿的完整状态比对（差分测试用）
// 比较：最优价、订单数、各侧档位数、两侧完整深度、每个档位的 FIFO 订单列表与 volumeAt、
// 每个挂单的 findOrder，以及 lastTradeId。
// ============================================================================

template <class BookA, class BookB>
bool sameState(const BookA& a, const BookB& b, std::string* why) {
    auto fail = [why](std::string msg) {
        if (why) *why = std::move(msg);
        return false;
    };
    if (a.bestBid() != b.bestBid())
        return fail("bestBid differs: " + str(a.bestBid()) + " vs " + str(b.bestBid()));
    if (a.bestAsk() != b.bestAsk())
        return fail("bestAsk differs: " + str(a.bestAsk()) + " vs " + str(b.bestAsk()));
    if (a.orderCount() != b.orderCount())
        return fail("orderCount differs: " + std::to_string(a.orderCount()) + " vs " +
                    std::to_string(b.orderCount()));
    for (Side side : {Side::Buy, Side::Sell}) {
        const std::string name = matching::toString(side);
        if (a.levelCount(side) != b.levelCount(side))
            return fail(name + " levelCount differs: " + std::to_string(a.levelCount(side)) +
                        " vs " + std::to_string(b.levelCount(side)));
        const std::vector<LevelInfo> depth_a = a.depth(side);
        const std::vector<LevelInfo> depth_b = b.depth(side);
        if (depth_a != depth_b)
            return fail(name + " depth differs:\n    A: " + str(depth_a) + "\n    B: " + str(depth_b));
        for (const LevelInfo& level : depth_a) {
            const std::vector<OrderInfo> orders_a = a.ordersAt(side, level.price);
            const std::vector<OrderInfo> orders_b = b.ordersAt(side, level.price);
            if (orders_a != orders_b)
                return fail(name + " ordersAt(" + std::to_string(level.price) + ") differs:\n    A: " +
                            str(orders_a) + "\n    B: " + str(orders_b));
            if (a.volumeAt(side, level.price) != b.volumeAt(side, level.price))
                return fail(name + " volumeAt(" + std::to_string(level.price) + ") differs: " +
                            std::to_string(a.volumeAt(side, level.price)) + " vs " +
                            std::to_string(b.volumeAt(side, level.price)));
            for (const OrderInfo& o : orders_a) {
                const std::optional<OrderInfo> found_a = a.findOrder(o.id);
                const std::optional<OrderInfo> found_b = b.findOrder(o.id);
                if (found_a != found_b)
                    return fail("findOrder(" + std::to_string(o.id) + ") differs: " + str(found_a) +
                                " vs " + str(found_b));
            }
        }
    }
    if (a.lastTradeId() != b.lastTradeId())
        return fail("lastTradeId differs: " + std::to_string(a.lastTradeId()) + " vs " +
                    std::to_string(b.lastTradeId()));
    return true;
}

// ============================================================================
// 单个订单簿各查询接口之间的一致性（与实现无关，可发现缓存的档位总量/计数错误）：
// depth 与 levelCount/bestBid/bestAsk/volumeAt/ordersAt/findOrder/orderCount 互相印证，
// depth(side, k) 是完整深度的前缀，两侧价格分离且不交叉。
// ============================================================================

template <class Book>
bool queriesConsistent(const Book& book, std::string* why) {
    auto fail = [why](std::string msg) {
        if (why) *why = std::move(msg);
        return false;
    };
    std::size_t total_orders = 0;
    for (Side side : {Side::Buy, Side::Sell}) {
        const std::string name = matching::toString(side);
        const Side other = side == Side::Buy ? Side::Sell : Side::Buy;
        const std::vector<LevelInfo> levels = book.depth(side);
        if (levels.size() != book.levelCount(side))
            return fail(name + ": depth has " + std::to_string(levels.size()) +
                        " levels but levelCount() = " + std::to_string(book.levelCount(side)));
        const std::optional<Price> best = side == Side::Buy ? book.bestBid() : book.bestAsk();
        if (levels.empty() == best.has_value())
            return fail(name + ": best price " + str(best) + " inconsistent with depth " + str(levels));
        if (best && levels.front().price != *best)
            return fail(name + ": best price " + str(best) + " != first depth level " +
                        std::to_string(levels.front().price));
        for (std::size_t k = 0; k <= 2; ++k) {
            const std::vector<LevelInfo> prefix(levels.begin(),
                                                levels.begin() + std::min(k, levels.size()));
            if (book.depth(side, k) != prefix)
                return fail(name + ": depth(" + std::to_string(k) + ") is not a prefix of depth()");
        }
        for (std::size_t i = 0; i < levels.size(); ++i) {
            const LevelInfo& level = levels[i];
            if (i > 0) {
                const Price prev = levels[i - 1].price;
                const bool strictly_better = side == Side::Buy ? prev > level.price : prev < level.price;
                if (!strictly_better)
                    return fail(name + ": depth not strictly sorted best-first: " + str(levels));
            }
            if (level.qty <= 0 || level.orders == 0)
                return fail(name + ": degenerate level " + str(level));
            if (book.volumeAt(side, level.price) != level.qty)
                return fail(name + ": volumeAt(" + std::to_string(level.price) + ") = " +
                            std::to_string(book.volumeAt(side, level.price)) + " but depth says " +
                            std::to_string(level.qty));
            if (!book.ordersAt(other, level.price).empty())
                return fail(name + ": price " + std::to_string(level.price) + " has orders on both sides");
            const std::vector<OrderInfo> orders = book.ordersAt(side, level.price);
            if (orders.size() != level.orders)
                return fail(name + ": ordersAt(" + std::to_string(level.price) + ") lists " +
                            std::to_string(orders.size()) + " orders but depth says " +
                            std::to_string(level.orders));
            Qty sum = 0;
            for (const OrderInfo& o : orders) {
                if (o.side != side || o.price != level.price || o.remaining <= 0)
                    return fail(name + ": bad order row " + str(o) + " at level " + str(level));
                const std::optional<OrderInfo> found = book.findOrder(o.id);
                if (!found || *found != o)
                    return fail("findOrder(" + std::to_string(o.id) + ") = " + str(found) +
                                " but ordersAt lists " + str(o));
                // 被检查的簿可能有错：累加前先判断是否越界（sum >= 0 且 remaining > 0，减法不会溢出）
                if (o.remaining > std::numeric_limits<Qty>::max() - sum)
                    return fail(name + ": orders at " + std::to_string(level.price) + " sum past INT64_MAX");
                sum += o.remaining;
                ++total_orders;
            }
            if (sum != level.qty)
                return fail(name + ": orders at " + std::to_string(level.price) + " sum to " +
                            std::to_string(sum) + " but depth says " + std::to_string(level.qty));
        }
        if (!levels.empty()) {
            // 最差档之外的价格必须没有档位。最差卖价为 INT64_MAX（或最差买价为 INT64_MIN）时其外没有
            // 可表示的价格，不再探测（否则 price + 1 有符号溢出）
            const Price worst = levels.back().price;
            const bool has_beyond = side == Side::Buy ? worst > std::numeric_limits<Price>::min()
                                                      : worst < std::numeric_limits<Price>::max();
            if (has_beyond) {
                const Price beyond = side == Side::Buy ? worst - 1 : worst + 1;
                if (book.volumeAt(side, beyond) != 0 || !book.ordersAt(side, beyond).empty())
                    return fail(name + ": phantom level at " + std::to_string(beyond));
            }
        }
    }
    if (total_orders != book.orderCount())
        return fail("orderCount() = " + std::to_string(book.orderCount()) + " but " +
                    std::to_string(total_orders) + " orders are listed");
    if (book.bestBid() && book.bestAsk() && *book.bestBid() >= *book.bestAsk())
        return fail("book crossed: " + str(book.bestBid()) + " >= " + str(book.bestAsk()));
    return true;
}

// ============================================================================
// 随机命令生成器（mt19937_64，给定种子完全可复现；只使用标准化的引擎原始输出，
// 不依赖各标准库实现不同的 std::uniform_int_distribution）
//
// 模拟的市场：中间价随机游走；被动单集中在中间价附近几十个 tick 内，少量主动单越过
// 中间价成交；数量以小单为主、偶有扫多档的大单；少数账户（STP 经常触发）；全部订单
// 类型/TIF/post-only/STP 模式；撤单、改单同时命中活动订单与已结束/未知订单；少量非法请求、
// 重复 id、id 复用以及 0 / UINT64_MAX 这样的边界 id。
// 生成器跟踪自己发出的限价 GTC 订单（"候选挂单"），数量超过 max_live 时强制撤最旧的一个，
// 因而活动挂单数有上界，簿规模保持在几百单以内。
// ============================================================================

struct GeneratorConfig {
    Price initial_mid = 10'000;         // 初始中间价（tick）
    Price passive_ticks = 25;           // 被动单离中间价最远的 tick 数
    Price aggressive_ticks = 4;         // 主动单最多越过中间价的 tick 数
    std::size_t max_live = 300;         // 候选挂单上限（超过则强制撤最旧的）
    std::uint64_t id_space = 4096;      // 新单 id 在 [1, id_space] 中循环分配（0 = 只增不循环）
    std::uint32_t accounts = 4;         // 账户取 [1, accounts]
    std::uint32_t weight_new = 60;      // 命令类型权重
    std::uint32_t weight_cancel = 22;
    std::uint32_t weight_modify = 18;
    double invalid_rate = 0.03;         // 非法新单比例（数量/价格/标志）
    double duplicate_rate = 0.03;       // 新单故意复用候选挂单 id 的比例（多半触发 DuplicateOrderId）
    double drift_rate = 0.05;           // 每条命令中间价 ±1 tick 的概率
};

class RandomCommandGenerator {
public:
    explicit RandomCommandGenerator(std::uint64_t seed, const GeneratorConfig& cfg = GeneratorConfig())
        : cfg_(cfg), rng_(seed), mid_(cfg.initial_mid) {
        clampMid();
    }

    Command next() {
        drift();
        if (tracked_.size() > cfg_.max_live) return cancelTracked(0);  // 强制撤最旧的候选挂单
        const std::uint64_t w_new = cfg_.weight_new;
        const std::uint64_t w_cancel = cfg_.weight_cancel;
        const std::uint64_t total = w_new + w_cancel + cfg_.weight_modify;
        const std::uint64_t roll = below(total);
        if (total == 0 || roll < w_new) return makeNew();
        if (roll < w_new + w_cancel) return makeCancel();
        return makeModify();
    }

    Price mid() const noexcept { return mid_; }
    std::size_t trackedCount() const noexcept { return tracked_.size(); }

private:
    struct Tracked {  // 生成器记下的候选挂单（不一定仍活动：可能已成交或被拒）
        OrderId id;
        Side side;
        Price price;
        Qty qty;
    };

    // ---- 随机原语 ----
    // [0, n)：直接取模，n 很小时偏差可忽略
    std::uint64_t below(std::uint64_t n) { return n == 0 ? 0 : rng_() % n; }
    // [lo, hi]，要求 lo <= hi
    std::int64_t range(std::int64_t lo, std::int64_t hi) {
        return lo + static_cast<std::int64_t>(below(static_cast<std::uint64_t>(hi - lo) + 1));
    }
    bool chance(double p) { return static_cast<double>(rng_() >> 11) * 0x1.0p-53 < p; }

    // ---- 市场状态 ----
    Price minMid() const {
        return std::max<Price>(1, cfg_.passive_ticks) + std::max<Price>(0, cfg_.aggressive_ticks) + 1;
    }
    void clampMid() { mid_ = std::max(mid_, minMid()); }
    void drift() {
        if (!chance(cfg_.drift_rate)) return;
        mid_ += chance(0.5) ? 1 : -1;
        clampMid();
    }

    Side randomSide() { return chance(0.5) ? Side::Buy : Side::Sell; }

    AccountId pickAccount() {
        if (cfg_.accounts == 0) return 0;
        return static_cast<AccountId>(range(1, cfg_.accounts));
    }

    StpMode pickStp() {
        switch (below(6)) {
            case 3: return StpMode::CancelTaker;
            case 4: return StpMode::CancelMaker;
            case 5: return StpMode::CancelBoth;
            default: return StpMode::None;  // 一半订单不启用 STP
        }
    }

    Qty randomQty() {
        const std::uint64_t roll = below(100);
        if (roll < 70) return range(1, 10);
        if (roll < 93) return range(11, 40);
        return range(41, 200);  // 偶尔扫多档
    }

    // 限价：45% 在盘口附近，35% 更深，20% 越过中间价（主动成交）
    Price limitPrice(Side side) {
        const Price passive = std::max<Price>(1, cfg_.passive_ticks);
        const Price aggressive = std::max<Price>(0, cfg_.aggressive_ticks);
        const std::uint64_t roll = below(100);
        Price offset;  // > 0：远离对手方；<= 0：越过中间价
        if (roll < 45) offset = range(1, std::min<Price>(5, passive));
        else if (roll < 80) offset = range(1, passive);
        else offset = -range(0, aggressive);
        return side == Side::Buy ? mid_ - offset : mid_ + offset;
    }

    // 市价单的 price 字段被忽略：随便给一个（常常是 0 或负数）
    Price marketPrice() { return chance(0.5) ? 0 : range(-5, 2 * mid_); }

    OrderId freshId() {
        // 极少数用边界 id，检查实现是否把 0 或 ~0 当作哨兵
        if (chance(0.002)) {
            static constexpr OrderId kEdge[] = {0, UINT64_MAX, UINT64_MAX - 1};
            return kEdge[below(3)];
        }
        ++id_counter_;
        if (cfg_.id_space == 0) return id_counter_;
        return (id_counter_ - 1) % cfg_.id_space + 1;
    }

    // 已结束或从未用过的 id（也可能恰好仍是活动订单）
    OrderId deadOrRandomId() {
        if (!history_.empty() && chance(0.6)) return history_[below(history_.size())];
        if (cfg_.id_space != 0) return static_cast<OrderId>(range(1, static_cast<std::int64_t>(cfg_.id_space)));
        return id_counter_ + 1 + below(1000);
    }

    void remember(OrderId id) {
        constexpr std::size_t kHistory = 256;
        if (history_.size() < kHistory) {
            history_.push_back(id);
        } else {
            history_[history_pos_] = id;
            history_pos_ = (history_pos_ + 1) % kHistory;
        }
    }

    // 把一个合法请求改成非法：数量、价格或标志
    void corrupt(OrderRequest& r) {
        switch (below(3)) {
            case 0:
                r.qty = range(-3, 0);
                break;
            case 1:
                if (r.type == OrderType::Limit) r.price = range(-3, 0);
                else r.qty = 0;
                break;
            default:
                r.post_only = true;
                if (r.type == OrderType::Limit && r.tif == TimeInForce::GTC) {
                    switch (below(3)) {
                        case 0: r.tif = TimeInForce::IOC; break;
                        case 1: r.tif = TimeInForce::FOK; break;
                        default: r.type = OrderType::Market; break;
                    }
                }
                break;
        }
    }

    Command makeNew() {
        OrderRequest r;
        if (!tracked_.empty() && chance(cfg_.duplicate_rate)) {
            r.id = tracked_[below(tracked_.size())].id;
        } else {
            r.id = freshId();
        }
        r.account = pickAccount();
        r.side = randomSide();
        const std::uint64_t kind = below(100);
        if (kind < 52) {
            // 普通限价 GTC
        } else if (kind < 64) {
            r.post_only = true;
        } else if (kind < 76) {
            r.tif = TimeInForce::IOC;
        } else if (kind < 86) {
            r.tif = TimeInForce::FOK;
        } else {
            r.type = OrderType::Market;
            static constexpr TimeInForce kTifs[] = {TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK};
            r.tif = kTifs[below(3)];
        }
        r.price = r.type == OrderType::Market ? marketPrice() : limitPrice(r.side);
        r.qty = randomQty();
        r.stp = pickStp();
        if (chance(cfg_.invalid_rate)) corrupt(r);
        if (r.type == OrderType::Limit && r.tif == TimeInForce::GTC) {
            tracked_.push_back(Tracked{r.id, r.side, r.price, r.qty});
        } else {
            remember(r.id);
        }
        return Command::newOrder(r);
    }

    Command cancelTracked(std::size_t i) {
        const OrderId id = tracked_[i].id;
        tracked_.erase(tracked_.begin() + static_cast<std::ptrdiff_t>(i));
        remember(id);
        return Command::cancel(id);
    }

    Command makeCancel() {
        const std::uint64_t roll = below(100);
        if (tracked_.empty() || roll < 15) return Command::cancel(deadOrRandomId());
        if (roll < 30) return cancelTracked(0);  // 最旧的
        return cancelTracked(below(tracked_.size()));
    }

    Command makeModify() {
        if (tracked_.empty() || chance(0.12)) {
            const Side side = randomSide();
            return Command::modify(deadOrRandomId(), limitPrice(side), randomQty());
        }
        Tracked& t = tracked_[below(tracked_.size())];
        Price p = t.price;
        Qty q = t.qty;
        const std::uint64_t roll = below(100);
        if (roll < 30) {
            q = range(1, std::max<Qty>(1, t.qty));          // 同价减量或不变：原地修改
        } else if (roll < 45) {
            q = std::max<Qty>(1, t.qty) + range(1, 10);     // 同价加量：失去时间优先级
        } else if (roll < 88) {
            p = limitPrice(t.side);                         // 改价：可能变成主动单而成交
            q = randomQty();
        } else {
            if (chance(0.5)) q = range(-3, 0);              // 非法改单（不更新跟踪信息）
            else p = range(-3, 0);
            return Command::modify(t.id, p, q);
        }
        t.price = p;
        t.qty = q;
        return Command::modify(t.id, p, q);
    }

    GeneratorConfig cfg_;
    std::mt19937_64 rng_;
    Price mid_;
    std::uint64_t id_counter_ = 0;
    std::vector<Tracked> tracked_;   // 候选挂单（最旧的在前）
    std::vector<OrderId> history_;   // 最近结束/撤销/非挂单类订单的 id（环形）
    std::size_t history_pos_ = 0;
};

}  // namespace hft::matching::test

// ============================================================================
// 断言宏（失败时报告调用处的文件与行号；期望列表用花括号逐项书写）
// ============================================================================

// CHECK_EVENTS(h.submit(r), accepted(r), trade(...), ...)：完整事件序列必须逐条相等
#define CHECK_EVENTS(actual, ...)                                                          \
    ::hft::matching::test::checkEvents((actual),                                           \
        std::vector<::hft::matching::test::Event>{__VA_ARGS__}, __FILE__, __LINE__)

#define CHECK_NO_EVENTS(actual)                                                            \
    ::hft::matching::test::checkEvents((actual), std::vector<::hft::matching::test::Event>{}, \
                                       __FILE__, __LINE__)

// CHECK_DEPTH(book, Side::Buy, {100, 5, 1}, {99, 3, 2})：完整深度（最优在前）
#define CHECK_DEPTH(book, side, ...)                                                       \
    ::hft::matching::test::checkList((book).depth(side),                                   \
        std::vector<::hft::matching::LevelInfo>{__VA_ARGS__}, #book ".depth(" #side ")",    \
        __FILE__, __LINE__)

#define CHECK_DEPTH_EMPTY(book, side)                                                      \
    ::hft::matching::test::checkList((book).depth(side), std::vector<::hft::matching::LevelInfo>{}, \
                                     #book ".depth(" #side ")", __FILE__, __LINE__)

// CHECK_ORDERS_AT(book, Side::Sell, 101, infoOf(r1, 2), infoOf(r2))：该价位 FIFO 顺序
#define CHECK_ORDERS_AT(book, side, price, ...)                                            \
    ::hft::matching::test::checkList((book).ordersAt(side, price),                         \
        std::vector<::hft::matching::OrderInfo>{__VA_ARGS__},                               \
        #book ".ordersAt(" #side ", " #price ")", __FILE__, __LINE__)

#define CHECK_NO_ORDERS_AT(book, side, price)                                              \
    ::hft::matching::test::checkList((book).ordersAt(side, price),                         \
        std::vector<::hft::matching::OrderInfo>{}, #book ".ordersAt(" #side ", " #price ")", \
        __FILE__, __LINE__)

// CHECK_TOP(book, 100, std::nullopt)：最优买价/卖价（std::nullopt 表示该侧为空）
#define CHECK_TOP(book, bid, ask)                                                          \
    ::hft::matching::test::checkTop((book), (bid), (ask), __FILE__, __LINE__)

#define CHECK_ORDER(book, id, info)                                                        \
    ::hft::matching::test::checkOrder((book), (id),                                        \
        std::optional<::hft::matching::OrderInfo>(info), __FILE__, __LINE__)

#define CHECK_NO_ORDER(book, id)                                                           \
    ::hft::matching::test::checkOrder((book), (id), std::optional<::hft::matching::OrderInfo>(), \
                                      __FILE__, __LINE__)

// CHECK_UNCHANGED(before, book)：与先前的 snapshotOf(book) 完全相同
#define CHECK_UNCHANGED(before, book)                                                      \
    ::hft::matching::test::checkUnchanged((before), (book), __FILE__, __LINE__)
