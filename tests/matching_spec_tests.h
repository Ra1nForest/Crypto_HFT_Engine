//
// matching_spec_tests.h
// 撮合引擎规范测试套件（docs/MATCHING_ENGINE.md「Semantics」的可执行版本）：对任意满足同一接口的订单簿模板注册
// 全部规范用例，逐条比对完整事件序列，并在每一步之后检查不变量与查询一致性。
// Matching engine spec test suite (the executable version of "Semantics" in docs/MATCHING_ENGINE.md):
// registers every spec case for any order book template that satisfies the same interface, compares the
// complete event sequence entry by entry, and checks invariants and query consistency after every step.
//
// 用法（test_matching.cpp 中，静态初始化期调用）：
// Usage (in test_matching.cpp, called during static initialization):
//   registerMatchingSpecTests<hft::matching::ReferenceOrderBook>("reference");
//   registerMatchingSpecTests<hft::matching::LimitOrderBook>("optimized");
// 用例名为 "<impl_name>/<area>: <description>"。
// Case names are "<impl_name>/<area>: <description>".
//

#pragma once

#include "matching/types.h"
#include "matching_test_util.h"
#include "test_framework.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// CHECK_LEVELS(expr, {px, qty, orders}, ...)：任意返回 std::vector<LevelInfo> 的表达式
// CHECK_LEVELS(expr, {px, qty, orders}, ...): any expression that returns a std::vector<LevelInfo>
#define CHECK_LEVELS(actual, ...)                                                          \
    ::hft::matching::test::checkList((actual),                                             \
        std::vector<::hft::matching::LevelInfo>{__VA_ARGS__}, #actual, __FILE__, __LINE__)

// 布置盘面：请求必须只产生 Accepted 并原样挂单（仅在本头文件内使用，末尾 #undef）
// Sets up the book: the request must produce only Accepted and rest as is (used only in this header,
// #undef at the end)
#define PLACE(h, req) (h).place((req), __FILE__, __LINE__)

namespace hft::matching::test {

// 规范测试夹具：持有记录型监听器与被测订单簿。每个修改操作返回该操作产生的全部事件，
// 并在操作之后检查 checkInvariants() 与查询一致性（失败即终止当前用例）。
// Spec test fixture: holds a recording listener and the order book under test. Every mutating operation
// returns all the events it produced, and checkInvariants() and query consistency are checked after the
// operation (a failure aborts the current case).
template <template <class> class BookT>
struct SpecHarness {
    using Book = BookT<RecordingListener>;

    RecordingListener listener;  // 必须先于 book 构造（book 持有它的引用）
                                 // must be constructed before book (book holds a reference to it)
    Book book;

    explicit SpecHarness(std::size_t max_orders = 0) : listener(), book(listener, max_orders) {
        verify("construction");
    }

    std::vector<Event> submit(const OrderRequest& r) {
        listener.clear();
        book.submit(r);
        verify("submit " + str(r));
        return listener.take();
    }

    std::vector<Event> cancel(OrderId id) {
        listener.clear();
        book.cancel(id);
        verify("cancel " + std::to_string(id));
        return listener.take();
    }

    std::vector<Event> modify(OrderId id, Price price, Qty qty) {
        listener.clear();
        book.modify(id, price, qty);
        verify("modify " + std::to_string(id) + " px=" + std::to_string(price) +
               " qty=" + std::to_string(qty));
        return listener.take();
    }

    void place(const OrderRequest& r, const char* file, int line) {
        const std::vector<Event> events = submit(r);
        const bool ok_events = checkEvents(events, {accepted(r)}, file, line);
        const bool ok_order = checkOrder(book, r.id, std::optional<OrderInfo>(infoOf(r)), file, line);
        if (!ok_events || !ok_order) throw ::hft_test::RequireFailure();
    }

    void verify(const std::string& what) const {
        std::string why;
        if (!book.checkInvariants(&why))
            abortCase("checkInvariants() failed after " + what + ": " + why);
        why.clear();
        if (!queriesConsistent(book, &why))
            abortCase("query results inconsistent after " + what + ": " + why);
    }

    [[noreturn]] static void abortCase(const std::string& msg) {
        ::hft_test::reportFailure(__FILE__, __LINE__, msg);
        throw ::hft_test::RequireFailure();
    }
};

// 订单查询行的简写构造
// Shorthand constructor for an order query row
inline OrderInfo orderInfo(OrderId id, AccountId account, Side side, Price price, Qty remaining,
                           bool post_only = false, StpMode stp = StpMode::None) {
    return OrderInfo{id, account, side, price, remaining, post_only, stp};
}

// 把用例注册为 "<impl>/<name>"
// Registers cases as "<impl>/<name>"
class SpecRegistrar {
public:
    explicit SpecRegistrar(std::string impl_name) : impl_name_(std::move(impl_name)) {}
    void operator()(const char* name, int line, void (*fn)()) const {
        ::hft_test::registerTest(impl_name_ + "/" + name, fn, __FILE__, line);
    }

private:
    std::string impl_name_;
};

// 各区域的用例注册函数放在 spec 子命名空间，简写别名与账户常量不外泄
// The per-area case registration functions live in the spec sub-namespace, so the shorthand aliases and
// account constants do not leak out
namespace spec {

using S = Side;
using RR = RejectReason;
using CR = CancelReason;
using SM = StpMode;

inline constexpr AccountId kA = 1;
inline constexpr AccountId kB = 2;
inline constexpr AccountId kC = 3;
inline constexpr AccountId kD = 4;

// ============================================================================
// submit 校验（规则 1-7 及其优先顺序）
// submit validation (rules 1-7 and their precedence)
// ============================================================================
template <template <class> class BookT>
void registerValidationSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("validation: non-positive quantity is rejected", __LINE__, [] {
        H h;
        CHECK_EVENTS(h.submit(buy(1, 100, 0)), rejectedNew(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(2, 100, -5)), rejectedNew(2, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(buy(3, 100, INT64_MIN)), rejectedNew(3, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(marketBuy(4, 0)), rejectedNew(4, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(5, 100, 0).ioc()), rejectedNew(5, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(6, 100, -1).fok()), rejectedNew(6, RR::InvalidQuantity));
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_TOP(h.book, std::nullopt, std::nullopt);
    });

    add("validation: limit price must be positive", __LINE__, [] {
        H h;
        CHECK_EVENTS(h.submit(buy(1, 0, 5)), rejectedNew(1, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(sell(2, -1, 5)), rejectedNew(2, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(buy(3, INT64_MIN, 5).ioc()), rejectedNew(3, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(sell(4, 0, 5).fok()), rejectedNew(4, RR::InvalidPrice));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("validation: market orders ignore the price field", __LINE__, [] {
        H h;
        const OrderRequest a1 = sell(1, 100, 5).account(kA);
        PLACE(h, a1);
        const OrderRequest m1 = marketBuy(2, 2).account(kB).price(-7);
        CHECK_EVENTS(h.submit(m1), accepted(m1), trade(1, 1, 2, kA, kB, S::Buy, 100, 2, 3, 0));
        const OrderRequest m2 = marketBuy(3, 1).account(kB).price(1);  // 远低于卖一，照样成交
                                                                       // far below the best ask, still trades
        CHECK_EVENTS(h.submit(m2), accepted(m2), trade(2, 1, 3, kA, kB, S::Buy, 100, 1, 2, 0));
        const OrderRequest m3 = marketSell(4, 1).account(kB).price(INT64_MAX);  // 买盘为空
                                                                                // bid side is empty
        CHECK_EVENTS(h.submit(m3), accepted(m3), cancelled(4, 1, CR::MarketRemainder));
        const OrderRequest m4 = marketBuy(5, 1).account(kB).price(0).fok();
        CHECK_EVENTS(h.submit(m4), accepted(m4), trade(3, 1, 5, kA, kB, S::Buy, 100, 1, 1, 0));
        CHECK_ORDER(h.book, 1, infoOf(a1, 1));
    });

    add("validation: post-only requires a limit GTC order", __LINE__, [] {
        H h;
        const OrderRequest a1 = sell(1, 101, 5).account(kA);
        PLACE(h, a1);
        CHECK_EVENTS(h.submit(buy(2, 100, 5).postOnly().ioc()), rejectedNew(2, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(buy(3, 100, 5).postOnly().fok()), rejectedNew(3, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(marketBuy(4, 5).postOnly()), rejectedNew(4, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(marketSell(5, 5).postOnly().ioc()), rejectedNew(5, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(marketBuy(6, 5).postOnly().fok()), rejectedNew(6, RR::InvalidFlags));
        CHECK_EQ(h.book.lastTradeId(), 0u);
        CHECK_ORDER(h.book, 1, infoOf(a1));
    });

    add("validation: duplicate id of a live order is rejected", __LINE__, [] {
        H h;
        const OrderRequest o1 = buy(1, 100, 5).account(kA);
        PLACE(h, o1);
        CHECK_EVENTS(h.submit(buy(1, 100, 5).account(kA)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(sell(1, 200, 3).account(kB)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_ORDER(h.book, 1, infoOf(o1));
        CHECK_EQ(h.book.orderCount(), 1u);
        CHECK_DEPTH(h.book, S::Buy, {100, 5, 1});
        CHECK_DEPTH_EMPTY(h.book, S::Sell);
    });

    add("validation: duplicate id check also applies to IOC, FOK and market orders", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 101, 5).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        // 这些请求若被接受都会成交；重复 id 必须在此之前拒绝
        // These requests would all trade if accepted; the duplicate id must be rejected before that
        CHECK_EVENTS(h.submit(buy(2, 101, 5).ioc().account(kC)), rejectedNew(2, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(buy(1, 101, 1).fok().account(kC)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(marketSell(1, 1).account(kC)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(marketBuy(2, 1).ioc().account(kC)), rejectedNew(2, RR::DuplicateOrderId));
        CHECK_UNCHANGED(before, h.book);
    });

    add("validation: quantity is checked before price and flags", __LINE__, [] {
        H h;
        CHECK_EVENTS(h.submit(buy(1, 0, 0)), rejectedNew(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(buy(2, -5, -5).postOnly().ioc()), rejectedNew(2, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(marketSell(3, 0).postOnly()), rejectedNew(3, RR::InvalidQuantity));
    });

    add("validation: price is checked before flags and market orders skip it", __LINE__, [] {
        H h;
        CHECK_EVENTS(h.submit(buy(1, 0, 5).postOnly().ioc()), rejectedNew(1, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(sell(2, -1, 5).postOnly().fok()), rejectedNew(2, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(marketBuy(3, 5).price(-1).postOnly()), rejectedNew(3, RR::InvalidFlags));
    });

    add("validation: quantity, price and flags are checked before the duplicate id", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5));
        CHECK_EVENTS(h.submit(buy(1, 100, 0)), rejectedNew(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(buy(1, 0, 5)), rejectedNew(1, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(buy(1, 100, 5).postOnly().ioc()), rejectedNew(1, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(marketBuy(1, 5).postOnly()), rejectedNew(1, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(buy(1, 100, 5)), rejectedNew(1, RR::DuplicateOrderId));
    });

    add("validation: duplicate id is checked before capacity", __LINE__, [] {
        H h(1);
        PLACE(h, buy(1, 100, 5));
        CHECK_EVENTS(h.submit(buy(1, 99, 5)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(buy(2, 99, 5)), rejectedNew(2, RR::CapacityExceeded));
    });

    add("validation: duplicate id is checked before post-only crossing", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 101, 5).account(kA));
        CHECK_EVENTS(h.submit(buy(2, 102, 1).postOnly()), rejectedNew(2, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(sell(1, 99, 1).postOnly()), rejectedNew(1, RR::DuplicateOrderId));
    });

    add("validation: quantity and price are checked before post-only crossing", __LINE__, [] {
        // 卖出 post-only 且价格 <= 0 时 crosses(Sell, p) 为真：只有规则顺序能决定拒单原因
        // For a post-only sell with a price <= 0, crosses(Sell, p) is true: only the rule order decides the
        // reject reason
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        CHECK_EVENTS(h.submit(sell(2, 0, 1).postOnly()), rejectedNew(2, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(sell(3, -100, 1).postOnly()), rejectedNew(3, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(sell(4, 100, 0).postOnly()), rejectedNew(4, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(5, 100, 1).postOnly()), rejectedNew(5, RR::PostOnlyWouldCross));
    });

    add("validation: capacity is checked before post-only crossing", __LINE__, [] {
        H h(2);
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 101, 5).account(kA));
        CHECK_EVENTS(h.submit(buy(3, 101, 1).postOnly()), rejectedNew(3, RR::CapacityExceeded));
        CHECK_EVENTS(h.submit(sell(4, 100, 1).postOnly()), rejectedNew(4, RR::CapacityExceeded));
        // 有空位时，同样的请求才轮到 post-only 规则
        // Only when there is room does the same request reach the post-only rule
        H g(3);
        PLACE(g, buy(1, 100, 5).account(kA));
        PLACE(g, sell(2, 101, 5).account(kA));
        CHECK_EVENTS(g.submit(buy(3, 101, 1).postOnly()), rejectedNew(3, RR::PostOnlyWouldCross));
    });

    add("validation: post-only order that would cross is rejected", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 102, 5).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.submit(buy(3, 103, 1).postOnly().account(kB)), rejectedNew(3, RR::PostOnlyWouldCross));
        CHECK_EVENTS(h.submit(buy(4, 1000, 1).postOnly().account(kB)), rejectedNew(4, RR::PostOnlyWouldCross));
        CHECK_EVENTS(h.submit(sell(5, 99, 1).postOnly().account(kB)), rejectedNew(5, RR::PostOnlyWouldCross));
        CHECK_EVENTS(h.submit(sell(6, 1, 1).postOnly().account(kB)), rejectedNew(6, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before, h.book);
    });

    add("validation: post-only order at the best opposite price (locking) is rejected", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 102, 5).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.submit(buy(3, 102, 1).postOnly().account(kB)), rejectedNew(3, RR::PostOnlyWouldCross));
        CHECK_EVENTS(h.submit(sell(4, 100, 1).postOnly().account(kB)), rejectedNew(4, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before, h.book);
    });

    add("validation: post-only order that does not cross rests", __LINE__, [] {
        H h;
        const OrderRequest a = sell(2, 102, 5).account(kA);
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, a);
        const OrderRequest p1 = buy(3, 101, 2).postOnly().account(kB);
        CHECK_EVENTS(h.submit(p1), accepted(p1));
        const OrderRequest p2 = sell(4, 102, 3).postOnly().account(kB).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(p2), accepted(p2));
        const OrderRequest p3 = buy(5, 50, 1).postOnly();
        CHECK_EVENTS(h.submit(p3), accepted(p3));
        CHECK_ORDER(h.book, 3, infoOf(p1));
        CHECK_ORDER(h.book, 4, infoOf(p2));
        CHECK_TOP(h.book, 101, 102);
        CHECK_ORDERS_AT(h.book, S::Sell, 102, infoOf(a), infoOf(p2));
        CHECK_EQ(h.book.lastTradeId(), 0u);
    });

    add("validation: post-only rests at any price when the opposite side is empty", __LINE__, [] {
        H h;
        const OrderRequest p = buy(1, 1'000'000, 1).postOnly();
        CHECK_EVENTS(h.submit(p), accepted(p));
        CHECK_TOP(h.book, 1'000'000, std::nullopt);
        H g;
        const OrderRequest q = sell(1, 1, 1).postOnly();
        CHECK_EVENTS(g.submit(q), accepted(q));
        CHECK_TOP(g.book, std::nullopt, 1);
    });

    add("validation: rejections have no side effects and do not reserve the id", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 101, 5).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.submit(buy(3, 101, 0)), rejectedNew(3, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(buy(4, 0, 5)), rejectedNew(4, RR::InvalidPrice));
        CHECK_EVENTS(h.submit(marketBuy(5, 5).postOnly()), rejectedNew(5, RR::InvalidFlags));
        CHECK_EVENTS(h.submit(buy(6, 101, 5).postOnly()), rejectedNew(6, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before, h.book);
        for (OrderId id = 3; id <= 6; ++id) {
            const OrderRequest ok = buy(id, 90, 1).account(kB);
            CHECK_EVENTS(h.submit(ok), accepted(ok));
        }
        CHECK_EQ(h.book.orderCount(), 6u);
    });

    add("validation: a GTC order that would push its level total past INT64_MAX is rejected", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        const OrderRequest a1 = sell(1, 100, kMax - 10).account(kA);
        PLACE(h, a1);
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.submit(sell(2, 100, 11).account(kB)), rejectedNew(2, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(3, 100, kMax).account(kB)), rejectedNew(3, RR::InvalidQuantity));
        CHECK_EVENTS(h.submit(sell(4, 100, 11).postOnly().account(kB)), rejectedNew(4, RR::InvalidQuantity));
        CHECK_UNCHANGED(before, h.book);
        // 恰好达到上限：接受；之后这一档再多一手也不行
        // Reaching the bound exactly: accepted; after that, not even one more lot fits at this level
        const OrderRequest a5 = sell(5, 100, 10).account(kB);
        CHECK_EVENTS(h.submit(a5), accepted(a5));
        CHECK_DEPTH(h.book, S::Sell, {100, kMax, 2});
        CHECK_ORDERS_AT(h.book, S::Sell, 100, infoOf(a1), infoOf(a5));
        CHECK_EVENTS(h.submit(sell(6, 100, 1).account(kC)), rejectedNew(6, RR::InvalidQuantity));
        // 买盘同理
        // Same for the bid side
        PLACE(h, buy(7, 99, kMax).account(kC));
        CHECK_EVENTS(h.submit(buy(8, 99, 1).account(kD)), rejectedNew(8, RR::InvalidQuantity));
        CHECK_DEPTH(h.book, S::Buy, {99, kMax, 1});
        // 上限按档位计算：同一侧的其他价位不受影响；被拒的 id 也没有被占用
        // The bound is per level: other prices on the same side are unaffected;
        // the rejected ids were not taken either
        const OrderRequest a2 = sell(2, 101, kMax).account(kB);
        CHECK_EVENTS(h.submit(a2), accepted(a2));
        const OrderRequest b8 = buy(8, 98, kMax).account(kD);
        CHECK_EVENTS(h.submit(b8), accepted(b8));
        CHECK_DEPTH(h.book, S::Sell, {100, kMax, 2}, {101, kMax, 1});
        CHECK_DEPTH(h.book, S::Buy, {99, kMax, 1}, {98, kMax, 1});
        CHECK_EQ(h.book.lastTradeId(), 0u);
    });

    add("validation: the level total bound is checked after duplicate id and capacity", __LINE__, [] {
        // 上限触发时本侧在该价位已有档位：价格必然为正、订单也不可能穿价，所以它与 InvalidPrice、
        // PostOnlyWouldCross 永远不会同时成立；能与它同时成立、决定先后的只有重复 id 与容量规则
        // When the bound triggers, this side already has a level at that price: the price must be positive and
        // the order cannot cross, so it never holds together with InvalidPrice or PostOnlyWouldCross; the only
        // rules that can hold at the same time, where precedence matters, are the duplicate id and capacity rules
        H h(2);
        constexpr Qty kMax = kMaxLevelQty;
        PLACE(h, sell(1, 100, kMax).account(kA));
        CHECK_EVENTS(h.submit(sell(1, 100, 1).account(kB)), rejectedNew(1, RR::DuplicateOrderId));
        CHECK_EVENTS(h.submit(sell(2, 100, 1).account(kB)), rejectedNew(2, RR::InvalidQuantity));
        PLACE(h, buy(3, 90, 1).account(kB));
        CHECK_EVENTS(h.submit(sell(2, 100, 1).account(kB)), rejectedNew(2, RR::CapacityExceeded));
        CHECK_EVENTS(h.submit(sell(4, 100, 1).postOnly().account(kB)), rejectedNew(4, RR::CapacityExceeded));
        CHECK_EQ(h.book.orderCount(), 2u);
    });

    add("validation: orders that never rest are not subject to the level total bound", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        PLACE(h, sell(1, 100, kMax).account(kA));
        PLACE(h, buy(2, 99, kMax).account(kA));
        // IOC / FOK / 市价单不会挂单：本侧该价位已满也照常接受
        // IOC / FOK / market orders never rest: accepted as usual even if this side's level at the price is full
        const OrderRequest i3 = sell(3, 100, kMax).ioc().account(kB);
        CHECK_EVENTS(h.submit(i3), accepted(i3), cancelled(3, kMax, CR::IocRemainder));
        const OrderRequest f4 = buy(4, 99, kMax).fok().account(kB);
        CHECK_EVENTS(h.submit(f4), accepted(f4), cancelled(4, kMax, CR::FokUnfillable));
        const OrderRequest m5 = marketSell(5, 2).account(kB);
        CHECK_EVENTS(h.submit(m5), accepted(m5), trade(1, 2, 5, kA, kB, S::Sell, 99, 2, kMax - 2, 0));
        // 会穿价的限价 GTC：本侧在它的价格上不可能已有档位（盘口不交叉），剩余量挂成新的一档
        // A limit GTC that crosses: this side cannot already have a level at its price (the book never crosses),
        // so the remainder rests as a new level
        const OrderRequest b6 = buy(6, 101, kMax).account(kB);
        CHECK_EVENTS(h.submit(b6), accepted(b6), trade(2, 1, 6, kA, kB, S::Buy, 100, kMax, 0, 0));
        PLACE(h, sell(7, 100, 5).account(kA));
        const OrderRequest b8 = buy(8, 100, kMax).account(kC);
        CHECK_EVENTS(h.submit(b8), accepted(b8), trade(3, 7, 8, kA, kC, S::Buy, 100, 5, 0, kMax - 5));
        CHECK_DEPTH(h.book, S::Buy, {100, kMax - 5, 1}, {99, kMax - 2, 1});
        CHECK_EVENTS(h.submit(buy(9, 100, 6).account(kD)), rejectedNew(9, RR::InvalidQuantity));
        const OrderRequest b10 = buy(10, 100, 5).account(kD);
        CHECK_EVENTS(h.submit(b10), accepted(b10));
        CHECK_DEPTH(h.book, S::Buy, {100, kMax, 2}, {99, kMax - 2, 1});
    });
}

// ============================================================================
// 挂单与深度
// Resting orders and depth
// ============================================================================
template <template <class> class BookT>
void registerRestingSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("resting: GTC limit order on an empty book rests silently", __LINE__, [] {
        H h;
        const OrderRequest b = buy(1, 100, 5).account(kA);
        CHECK_EVENTS(h.submit(b), accepted(b));
        CHECK_TOP(h.book, 100, std::nullopt);
        CHECK_EQ(h.book.orderCount(), 1u);
        CHECK_DEPTH(h.book, S::Buy, {100, 5, 1});
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b));
        const OrderRequest s = sell(2, 105, 3).account(kB);
        CHECK_EVENTS(h.submit(s), accepted(s));
        CHECK_TOP(h.book, 100, 105);
        CHECK_EQ(h.book.orderCount(), 2u);
        CHECK_EQ(h.book.lastTradeId(), 0u);
    });

    add("resting: depth is sorted best-first on both sides", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 1);
        const OrderRequest b4 = buy(4, 100, 4);
        PLACE(h, b1);
        PLACE(h, buy(2, 98, 2));
        PLACE(h, buy(3, 99, 3));
        PLACE(h, b4);
        PLACE(h, buy(5, 97, 5));
        const OrderRequest a7 = sell(7, 103, 2);
        const OrderRequest a9 = sell(9, 103, 4);
        PLACE(h, sell(6, 105, 1));
        PLACE(h, a7);
        PLACE(h, sell(8, 104, 3));
        PLACE(h, a9);
        CHECK_DEPTH(h.book, S::Buy, {100, 5, 2}, {99, 3, 1}, {98, 2, 1}, {97, 5, 1});
        CHECK_DEPTH(h.book, S::Sell, {103, 6, 2}, {104, 3, 1}, {105, 1, 1});
        CHECK_EQ(h.book.levelCount(S::Buy), 4u);
        CHECK_EQ(h.book.levelCount(S::Sell), 3u);
        CHECK_EQ(h.book.orderCount(), 9u);
        CHECK_TOP(h.book, 100, 103);
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b1), infoOf(b4));
        CHECK_ORDERS_AT(h.book, S::Sell, 103, infoOf(a7), infoOf(a9));
    });
}

// ============================================================================
// 撮合：价格-时间优先、成交价、部分成交、扫档
// Matching: price-time priority, trade price, partial fills, sweeping levels
// ============================================================================
template <template <class> class BookT>
void registerMatchingSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("matching: makers at one price are filled in FIFO order", __LINE__, [] {
        H h;
        const OrderRequest a3 = sell(3, 101, 2).account(kC);
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 101, 2).account(kB));
        PLACE(h, a3);
        const OrderRequest t = buy(4, 101, 5).account(kD);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     trade(1, 1, 4, kA, kD, S::Buy, 101, 2, 0, 3),
                     trade(2, 2, 4, kB, kD, S::Buy, 101, 2, 0, 1),
                     trade(3, 3, 4, kC, kD, S::Buy, 101, 1, 1, 0));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a3, 1));
        CHECK_NO_ORDER(h.book, 1);
        CHECK_NO_ORDER(h.book, 2);
        CHECK_NO_ORDER(h.book, 4);
        CHECK_TOP(h.book, std::nullopt, 101);
        CHECK_EQ(h.book.lastTradeId(), 3u);
    });

    add("matching: ask levels are consumed lowest price first", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 103, 1).account(kA));
        PLACE(h, sell(2, 101, 1).account(kA));
        PLACE(h, sell(3, 102, 1).account(kA));
        PLACE(h, sell(4, 101, 1).account(kA));
        const OrderRequest t = buy(5, 103, 4).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     trade(1, 2, 5, kA, kB, S::Buy, 101, 1, 0, 3),
                     trade(2, 4, 5, kA, kB, S::Buy, 101, 1, 0, 2),
                     trade(3, 3, 5, kA, kB, S::Buy, 102, 1, 0, 1),
                     trade(4, 1, 5, kA, kB, S::Buy, 103, 1, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_TOP(h.book, std::nullopt, std::nullopt);
    });

    add("matching: bid levels are consumed highest price first", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 1).account(kA));
        PLACE(h, buy(2, 100, 1).account(kA));
        PLACE(h, buy(3, 98, 1).account(kA));
        const OrderRequest t = sell(4, 98, 5).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     trade(1, 2, 4, kA, kB, S::Sell, 100, 1, 0, 4),
                     trade(2, 1, 4, kA, kB, S::Sell, 99, 1, 0, 3),
                     trade(3, 3, 4, kA, kB, S::Sell, 98, 1, 0, 2));
        CHECK_TOP(h.book, std::nullopt, 98);
        CHECK_ORDER(h.book, 4, infoOf(t, 2));
    });

    add("matching: trade price is the maker's price, not the taker's limit", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 100, 5).account(kA));
        const OrderRequest t1 = buy(2, 105, 5).account(kB);
        CHECK_EVENTS(h.submit(t1), accepted(t1), trade(1, 1, 2, kA, kB, S::Buy, 100, 5, 0, 0));
        const OrderRequest b3 = buy(3, 100, 5).account(kA);
        PLACE(h, b3);
        const OrderRequest t2 = sell(4, 95, 2).account(kB);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(2, 3, 4, kA, kB, S::Sell, 100, 2, 3, 0));
        CHECK_ORDER(h.book, 3, infoOf(b3, 3));
    });

    add("matching: partially filled maker keeps its place at the head of the queue", __LINE__, [] {
        H h;
        const OrderRequest a1 = sell(1, 101, 10).account(kA);
        const OrderRequest a2 = sell(2, 101, 5).account(kB);
        PLACE(h, a1);
        PLACE(h, a2);
        const OrderRequest t1 = buy(3, 101, 4).account(kC);
        CHECK_EVENTS(h.submit(t1), accepted(t1), trade(1, 1, 3, kA, kC, S::Buy, 101, 4, 6, 0));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a1, 6), infoOf(a2));
        CHECK_DEPTH(h.book, S::Sell, {101, 11, 2});
        const OrderRequest t2 = buy(4, 101, 7).account(kC);
        CHECK_EVENTS(h.submit(t2), accepted(t2),
                     trade(2, 1, 4, kA, kC, S::Buy, 101, 6, 0, 1),
                     trade(3, 2, 4, kB, kC, S::Buy, 101, 1, 4, 0));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a2, 4));
    });

    add("matching: partially filled GTC taker rests its remainder at its limit price", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        const OrderRequest t = buy(2, 102, 10).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 3, 0, 7));
        CHECK_ORDER(h.book, 2, infoOf(t, 7));
        CHECK_TOP(h.book, 102, std::nullopt);
        CHECK_DEPTH(h.book, S::Buy, {102, 7, 1});
        // 挂出的剩余量此后就是 maker
        // The rested remainder is a maker from then on
        const OrderRequest t2 = sell(3, 100, 2).account(kA);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(2, 2, 3, kB, kA, S::Sell, 102, 2, 5, 0));
    });

    add("matching: a sweep stops at the limit price and rests the remainder", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 102, 3).account(kA));
        PLACE(h, sell(3, 103, 4).account(kA));
        const OrderRequest t = buy(4, 102, 10).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     trade(1, 1, 4, kA, kB, S::Buy, 101, 2, 0, 8),
                     trade(2, 2, 4, kA, kB, S::Buy, 102, 3, 0, 5));
        CHECK_TOP(h.book, 102, 103);
        CHECK_ORDER(h.book, 4, infoOf(t, 5));
        CHECK_DEPTH(h.book, S::Sell, {103, 4, 1});
    });

    add("matching: consuming a whole level removes it", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 5).account(kA));
        PLACE(h, sell(2, 102, 1).account(kA));
        const OrderRequest t = buy(3, 101, 5).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 3, kA, kB, S::Buy, 101, 5, 0, 0));
        CHECK_EQ(h.book.levelCount(S::Sell), 1u);
        CHECK_TOP(h.book, std::nullopt, 102);
        CHECK_EQ(h.book.volumeAt(S::Sell, 101), 0);
        CHECK_NO_ORDERS_AT(h.book, S::Sell, 101);
        CHECK_NO_ORDER(h.book, 1);
        CHECK_NO_ORDER(h.book, 3);
    });

    add("matching: a limit order that does not cross rests without trading", __LINE__, [] {
        H h;
        const OrderRequest a1 = sell(1, 101, 5).account(kA);
        PLACE(h, a1);
        const OrderRequest b = buy(2, 100, 5).account(kB);
        CHECK_EVENTS(h.submit(b), accepted(b));
        CHECK_TOP(h.book, 100, 101);
        const OrderRequest s = sell(3, 101, 1).account(kB);  // 排在 1 号单之后
                                                             // queued behind order 1
        CHECK_EVENTS(h.submit(s), accepted(s));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a1), infoOf(s));
        CHECK_EQ(h.book.lastTradeId(), 0u);
    });

    add("matching: self-trade happens when the taker's STP mode is None", __LINE__, [] {
        H h;
        const OrderRequest m = sell(1, 101, 5).account(kA).stp(SM::CancelBoth);  // maker 的 STP 模式无关
                                                                                 // maker's STP mode is irrelevant
        PLACE(h, m);
        const OrderRequest t = buy(2, 101, 3).account(kA);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kA, S::Buy, 101, 3, 2, 0));
        CHECK_ORDER(h.book, 1, infoOf(m, 2));
    });

    add("matching: STP only applies between orders of the same account", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        const OrderRequest t1 = buy(2, 101, 2).account(kB).stp(SM::CancelBoth);
        CHECK_EVENTS(h.submit(t1), accepted(t1), trade(1, 1, 2, kA, kB, S::Buy, 101, 2, 0, 0));
        PLACE(h, sell(3, 101, 2).account(kA));
        const OrderRequest t2 = buy(4, 101, 1).account(kB).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(2, 3, 4, kA, kB, S::Buy, 101, 1, 1, 0));
        const OrderRequest t3 = buy(5, 101, 1).account(kB).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t3), accepted(t3), trade(3, 3, 5, kA, kB, S::Buy, 101, 1, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });
}

// ============================================================================
// 市价单
// Market orders
// ============================================================================
template <template <class> class BookT>
void registerMarketSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("market: empty opposite side cancels the whole quantity as MarketRemainder", __LINE__, [] {
        H h;
        const OrderRequest m1 = marketBuy(1, 5).account(kA);
        CHECK_EVENTS(h.submit(m1), accepted(m1), cancelled(1, 5, CR::MarketRemainder));
        const OrderRequest m2 = marketSell(2, 3).account(kA);
        CHECK_EVENTS(h.submit(m2), accepted(m2), cancelled(2, 3, CR::MarketRemainder));
        // 同侧有挂单不影响：只看对手方
        // Resting orders on the same side make no difference: only the opposite side counts
        PLACE(h, buy(3, 100, 1));
        const OrderRequest m3 = marketBuy(4, 2);
        CHECK_EVENTS(h.submit(m3), accepted(m3), cancelled(4, 2, CR::MarketRemainder));
        CHECK_EQ(h.book.orderCount(), 1u);
        CHECK_NO_ORDER(h.book, 4);
    });

    add("market: partial liquidity trades everything then cancels the rest", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 102, 3).account(kB));
        const OrderRequest m = marketBuy(3, 10).account(kC);
        CHECK_EVENTS(h.submit(m), accepted(m),
                     trade(1, 1, 3, kA, kC, S::Buy, 101, 2, 0, 8),
                     trade(2, 2, 3, kB, kC, S::Buy, 102, 3, 0, 5),
                     cancelled(3, 5, CR::MarketRemainder));
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_NO_ORDER(h.book, 3);
    });

    add("market: sweeps every level regardless of price until filled", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 2).account(kA));
        PLACE(h, buy(2, 50, 3).account(kA));
        PLACE(h, buy(3, 1, 4).account(kA));
        const OrderRequest m = marketSell(4, 6).account(kB);
        CHECK_EVENTS(h.submit(m), accepted(m),
                     trade(1, 1, 4, kA, kB, S::Sell, 100, 2, 0, 4),
                     trade(2, 2, 4, kA, kB, S::Sell, 50, 3, 0, 1),
                     trade(3, 3, 4, kA, kB, S::Sell, 1, 1, 3, 0));
        CHECK_DEPTH(h.book, S::Buy, {1, 3, 1});
        CHECK_NO_ORDER(h.book, 4);
    });

    add("market: GTC and IOC market orders both end with MarketRemainder", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 1).account(kA));
        const OrderRequest m1 = marketSell(2, 3).account(kB).ioc();
        CHECK_EVENTS(h.submit(m1), accepted(m1), trade(1, 1, 2, kA, kB, S::Sell, 99, 1, 0, 2),
                     cancelled(2, 2, CR::MarketRemainder));
        PLACE(h, buy(3, 98, 1).account(kA));
        const OrderRequest m2 = marketSell(4, 3).account(kB).gtc();
        CHECK_EVENTS(h.submit(m2), accepted(m2), trade(2, 3, 4, kA, kB, S::Sell, 98, 1, 0, 2),
                     cancelled(4, 2, CR::MarketRemainder));
        CHECK_EQ(h.book.orderCount(), 0u);  // 市价单从不挂单
                                            // market orders never rest
    });

    add("market: FOK market order is killed when total liquidity is short", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 105, 2).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest m1 = marketBuy(3, 5).account(kB).fok();
        CHECK_EVENTS(h.submit(m1), accepted(m1), cancelled(3, 5, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        const OrderRequest m2 = marketBuy(4, 4).account(kB).fok();
        CHECK_EVENTS(h.submit(m2), accepted(m2), trade(1, 1, 4, kA, kB, S::Buy, 101, 2, 0, 2),
                     trade(2, 2, 4, kA, kB, S::Buy, 105, 2, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
        const OrderRequest m3 = marketSell(5, 1).fok();
        CHECK_EVENTS(h.submit(m3), accepted(m3), cancelled(5, 1, CR::FokUnfillable));
    });
}

// ============================================================================
// IOC
// ============================================================================
template <template <class> class BookT>
void registerIocSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("ioc: an IOC that does not cross is cancelled in full", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 5).account(kA));
        const OrderRequest t1 = buy(2, 100, 5).ioc().account(kB);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(2, 5, CR::IocRemainder));
        const OrderRequest t2 = sell(3, 50, 2).ioc().account(kB);  // 买盘为空
                                                                   // bid side is empty
        CHECK_EVENTS(h.submit(t2), accepted(t2), cancelled(3, 2, CR::IocRemainder));
        CHECK_EQ(h.book.orderCount(), 1u);
        CHECK_NO_ORDER(h.book, 2);
        CHECK_NO_ORDER(h.book, 3);
    });

    add("ioc: partial fill cancels only the remainder and respects the limit", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 103, 5).account(kA));
        const OrderRequest t = buy(3, 102, 6).ioc().account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 3, kA, kB, S::Buy, 101, 2, 0, 4),
                     cancelled(3, 4, CR::IocRemainder));
        CHECK_DEPTH(h.book, S::Sell, {103, 5, 1});
        CHECK_DEPTH_EMPTY(h.book, S::Buy);
    });

    add("ioc: a fully filled IOC emits no cancel", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 5).account(kA));
        const OrderRequest i = buy(2, 101, 3).ioc().account(kB);
        CHECK_EVENTS(h.submit(i), accepted(i), trade(1, 1, 2, kA, kB, S::Buy, 101, 3, 2, 0));
        CHECK_NO_ORDER(h.book, 2);
        CHECK_EQ(h.book.orderCount(), 1u);
    });
}

// ============================================================================
// FOK（含与 STP 的组合）
// FOK (including combinations with STP)
// ============================================================================
template <template <class> class BookT>
void registerFokSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("fok: fillable order trades in full at one level", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 5).account(kA));
        const OrderRequest t = buy(2, 101, 5).fok().account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 5, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("fok: liquidity exactly equal to the quantity across levels is enough", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 102, 3).account(kA));
        PLACE(h, sell(3, 103, 9).account(kA));
        const OrderRequest t = buy(4, 102, 5).fok().account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 4, kA, kB, S::Buy, 101, 2, 0, 3),
                     trade(2, 2, 4, kA, kB, S::Buy, 102, 3, 0, 0));
        CHECK_DEPTH(h.book, S::Sell, {103, 9, 1});
    });

    add("fok: an unfillable order is killed with no side effects", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 102, 3).account(kA));
        PLACE(h, buy(3, 99, 1).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest t = buy(4, 102, 6).fok().account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(4, 6, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        CHECK_EQ(h.book.lastTradeId(), 0u);
        // 被撤销的 FOK 从未挂单，id 立即可用
        // The cancelled FOK never rested, so its id is available at once
        const OrderRequest again = buy(4, 98, 1).account(kB);
        CHECK_EVENTS(h.submit(again), accepted(again));
    });

    add("fok: only levels within the limit price count", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 103, 10).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest t1 = buy(3, 102, 5).fok().account(kB);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(3, 5, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        const OrderRequest t2 = buy(4, 103, 5).fok().account(kB);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(1, 1, 4, kA, kB, S::Buy, 101, 2, 0, 3),
                     trade(2, 2, 4, kA, kB, S::Buy, 103, 3, 7, 0));
    });

    add("fok: sell side mirrors the buy side", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, buy(2, 98, 5).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest t1 = sell(3, 99, 6).fok().account(kB);  // >= 99 的买单只有 5
                                                                   // bids at >= 99 add up to only 5
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(3, 6, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        const OrderRequest t2 = sell(4, 98, 10).fok().account(kB);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(1, 1, 4, kA, kB, S::Sell, 100, 5, 0, 5),
                     trade(2, 2, 4, kA, kB, S::Sell, 98, 5, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("fok: CancelMaker skips self makers in the pre-check and cancels them while filling", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        PLACE(h, sell(2, 101, 2).account(kB));
        PLACE(h, sell(3, 102, 3).account(kB));
        const OrderRequest t = buy(4, 102, 5).fok().account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     cancelled(1, 3, CR::SelfTradePrevention),
                     trade(1, 2, 4, kB, kA, S::Buy, 101, 2, 0, 3),
                     trade(2, 3, 4, kB, kA, S::Buy, 102, 3, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("fok: CancelMaker does not count self liquidity and a kill leaves self makers alone", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        PLACE(h, sell(2, 101, 2).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        // 总量 5 >= 4，但非本账户的只有 2
        // Total 5 >= 4, but only 2 of it belongs to other accounts
        const OrderRequest t = buy(3, 101, 4).fok().account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(3, 4, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
    });

    add("fok: CancelTaker stops the pre-check at the first self maker", __LINE__, [] {
        H h;
        const OrderRequest self = sell(2, 101, 3).account(kA);
        PLACE(h, sell(1, 101, 2).account(kB));
        PLACE(h, self);
        PLACE(h, sell(3, 102, 10).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        // 本账户 maker 之前只有 2；其后的 10 不计入
        // Only 2 ahead of the self maker; the 10 behind it do not count
        const OrderRequest t1 = buy(4, 102, 4).fok().account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(4, 4, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        // 本账户 maker 之前的量足够时正常成交，且不触碰它
        // When the quantity ahead of the self maker is enough, it trades normally and does not touch it
        const OrderRequest t2 = buy(5, 102, 2).fok().account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(1, 1, 5, kB, kA, S::Buy, 101, 2, 0, 0));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(self));
    });

    add("fok: CancelBoth stops the pre-check at the first self maker", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        PLACE(h, sell(2, 102, 10).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest t1 = buy(3, 102, 5).fok().account(kA).stp(SM::CancelBoth);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(3, 5, CR::FokUnfillable));  // maker 1 不被撤
                                                                                       // maker 1 is not cancelled
        CHECK_UNCHANGED(before, h.book);

        H g;
        const OrderRequest self = sell(2, 101, 1).account(kA);
        PLACE(g, sell(1, 101, 5).account(kB));
        PLACE(g, self);
        const OrderRequest t2 = buy(3, 101, 5).fok().account(kA).stp(SM::CancelBoth);
        CHECK_EVENTS(g.submit(t2), accepted(t2), trade(1, 1, 3, kB, kA, S::Buy, 101, 5, 0, 0));
        CHECK_ORDERS_AT(g.book, S::Sell, 101, infoOf(self));
    });

    add("fok: without STP, self liquidity counts and self-trades", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        PLACE(h, sell(2, 101, 2).account(kB));
        const OrderRequest t = buy(3, 101, 5).fok().account(kA);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 3, kA, kA, S::Buy, 101, 3, 0, 2),
                     trade(2, 2, 3, kB, kA, S::Buy, 101, 2, 0, 0));
    });

    add("fok: market FOK with CancelMaker and only self liquidity is killed", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 3).account(kA));
        PLACE(h, sell(2, 102, 3).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest t = marketBuy(3, 1).fok().account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(3, 1, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
    });

    add("fok: a level holding INT64_MAX in total fills FOK orders with and without STP", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        constexpr Qty kP62 = Qty{1} << 62;
        PLACE(h, sell(1, 100, kP62).account(kA));
        // 第二张 2^62 会让档位总量超过 INT64_MAX：拒单；补上 2^62 - 1 恰好到 INT64_MAX
        // A second 2^62 would push the level total past INT64_MAX: rejected;
        // topping up with 2^62 - 1 reaches INT64_MAX exactly
        CHECK_EVENTS(h.submit(sell(2, 100, kP62).account(kB)), rejectedNew(2, RR::InvalidQuantity));
        PLACE(h, sell(3, 100, kP62 - 1).account(kB));
        CHECK_DEPTH(h.book, S::Sell, {100, kMax, 2});
        const OrderRequest f4 = buy(4, 100, 10).fok().account(kC);
        CHECK_EVENTS(h.submit(f4), accepted(f4), trade(1, 1, 4, kA, kC, S::Buy, 100, 10, kP62 - 10, 0));
        const OrderRequest f5 = buy(5, 100, 10).fok().account(kC).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(f5), accepted(f5), trade(2, 1, 5, kA, kC, S::Buy, 100, 10, kP62 - 20, 0));
        // 整档还剩 INT64_MAX - 20：多要一手就整单撤销，正好这么多就全部成交
        // The level has INT64_MAX - 20 left in total: one lot more and the whole order is cancelled, exactly that
        // much and it fills completely
        const StateSnapshot before = snapshotOf(h.book);
        const OrderRequest f6 = buy(6, 100, kMax - 19).fok().account(kC);
        CHECK_EVENTS(h.submit(f6), accepted(f6), cancelled(6, kMax - 19, CR::FokUnfillable));
        const OrderRequest f7 = marketBuy(7, kMax - 19).fok().account(kC).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(f7), accepted(f7), cancelled(7, kMax - 19, CR::FokUnfillable));
        CHECK_UNCHANGED(before, h.book);
        const OrderRequest f8 = buy(8, 100, kMax - 20).fok().account(kC);
        CHECK_EVENTS(h.submit(f8), accepted(f8), trade(3, 1, 8, kA, kC, S::Buy, 100, kP62 - 20, 0, kP62 - 1),
                     trade(4, 3, 8, kB, kC, S::Buy, 100, kP62 - 1, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("fok: the pre-check counts down across several full levels", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        PLACE(h, sell(1, 100, kMax - 5).account(kA));
        PLACE(h, sell(2, 101, kMax).account(kB));
        PLACE(h, sell(3, 102, kMax).account(kC));
        // 整侧总量远超 INT64_MAX；预检从 INT64_MAX 倒数，跨档也不溢出
        // The total of the side is far above INT64_MAX; the pre-check counts down from INT64_MAX and does not
        // overflow across levels
        const OrderRequest f4 = buy(4, 102, kMax).fok().account(kD);
        CHECK_EVENTS(h.submit(f4), accepted(f4), trade(1, 1, 4, kA, kD, S::Buy, 100, kMax - 5, 0, 5),
                     trade(2, 2, 4, kB, kD, S::Buy, 101, 5, kMax - 5, 0));
        // CancelMaker 跳过本账户的一整档，下一档正好够
        // CancelMaker skips a whole level of self makers; the next level is exactly enough
        const OrderRequest f5 = buy(5, 102, kMax).fok().account(kB).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(f5), accepted(f5), cancelled(2, kMax - 5, CR::SelfTradePrevention),
                     trade(3, 3, 5, kC, kB, S::Buy, 102, kMax, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });
}

// ============================================================================
// 自成交保护（STP）
// Self-trade prevention (STP)
// ============================================================================
template <template <class> class BookT>
void registerStpSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("stp: CancelMaker cancels the self maker and keeps matching", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 101, 3).account(kB));
        const OrderRequest t = buy(3, 101, 4).account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(1, 2, CR::SelfTradePrevention),
                     trade(1, 2, 3, kB, kA, S::Buy, 101, 3, 0, 1));
        CHECK_ORDER(h.book, 3, infoOf(t, 1));  // GTC 剩余量挂单
                                               // the GTC remainder rests
        CHECK_TOP(h.book, 101, std::nullopt);
    });

    add("stp: CancelMaker removes every self maker it meets across levels", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 1).account(kA));
        PLACE(h, sell(2, 101, 1).account(kB));
        PLACE(h, sell(3, 102, 1).account(kA));
        PLACE(h, sell(4, 103, 1).account(kA));
        PLACE(h, sell(5, 103, 2).account(kC));
        PLACE(h, sell(6, 104, 1).account(kA));
        const OrderRequest t = marketBuy(7, 3).account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t),
                     cancelled(1, 1, CR::SelfTradePrevention),
                     trade(1, 2, 7, kB, kA, S::Buy, 101, 1, 0, 2),
                     cancelled(3, 1, CR::SelfTradePrevention),
                     cancelled(4, 1, CR::SelfTradePrevention),
                     trade(2, 5, 7, kC, kA, S::Buy, 103, 2, 0, 0));
        CHECK_DEPTH(h.book, S::Sell, {104, 1, 1});  // 成交完即停止：6 号单不受影响
                                                    // stops once filled: order 6 is not affected
    });

    add("stp: CancelTaker cancels the taker and leaves the maker untouched", __LINE__, [] {
        H h;
        const OrderRequest m = sell(1, 101, 5).account(kA);
        PLACE(h, m);
        const OrderRequest t = buy(2, 101, 3).account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(2, 3, CR::SelfTradePrevention));
        CHECK_ORDER(h.book, 1, infoOf(m));
        CHECK_NO_ORDER(h.book, 2);
        CHECK_TOP(h.book, std::nullopt, 101);
    });

    add("stp: CancelTaker after partial fills cancels the remainder and never rests it", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kB));
        PLACE(h, sell(2, 102, 5).account(kA));
        PLACE(h, sell(3, 102, 5).account(kB));
        const OrderRequest t = buy(4, 105, 10).account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 4, kB, kA, S::Buy, 101, 2, 0, 8),
                     cancelled(4, 8, CR::SelfTradePrevention));
        CHECK_DEPTH(h.book, S::Sell, {102, 10, 2});
        CHECK_DEPTH_EMPTY(h.book, S::Buy);
        CHECK_NO_ORDER(h.book, 4);
    });

    add("stp: CancelBoth cancels the maker first, then the taker", __LINE__, [] {
        H h;
        const OrderRequest other = buy(2, 100, 4).account(kB);
        PLACE(h, buy(1, 100, 4).account(kA));
        PLACE(h, other);
        const OrderRequest t = sell(3, 100, 6).account(kA).stp(SM::CancelBoth);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(1, 4, CR::SelfTradePrevention),
                     cancelled(3, 6, CR::SelfTradePrevention));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(other));
        CHECK_TOP(h.book, 100, std::nullopt);
        CHECK_NO_ORDER(h.book, 3);
    });

    add("stp: CancelBoth after partial fills", __LINE__, [] {
        H h;
        const OrderRequest b3 = buy(3, 99, 1).account(kB);
        PLACE(h, buy(1, 100, 2).account(kB));
        PLACE(h, buy(2, 99, 3).account(kA));
        PLACE(h, b3);
        const OrderRequest t = sell(4, 98, 5).account(kA).stp(SM::CancelBoth);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 4, kB, kA, S::Sell, 100, 2, 0, 3),
                     cancelled(2, 3, CR::SelfTradePrevention),
                     cancelled(4, 3, CR::SelfTradePrevention));
        CHECK_DEPTH(h.book, S::Buy, {99, 1, 1});
        CHECK_ORDERS_AT(h.book, S::Buy, 99, infoOf(b3));
        CHECK_DEPTH_EMPTY(h.book, S::Sell);
    });

    add("stp: CancelMaker that empties the crossing liquidity ends with the normal remainder", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        const OrderRequest m = marketBuy(2, 5).account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(m), accepted(m), cancelled(1, 2, CR::SelfTradePrevention),
                     cancelled(2, 5, CR::MarketRemainder));
        PLACE(h, sell(3, 101, 2).account(kA));
        PLACE(h, sell(4, 103, 1).account(kB));
        const OrderRequest i = buy(5, 102, 5).ioc().account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(i), accepted(i), cancelled(3, 2, CR::SelfTradePrevention),
                     cancelled(5, 5, CR::IocRemainder));
        CHECK_DEPTH(h.book, S::Sell, {103, 1, 1});
    });

    add("stp: GTC taker rests after CancelMaker removes the crossing self makers", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, sell(2, 103, 1).account(kB));
        const OrderRequest t = buy(3, 102, 3).account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t), accepted(t), cancelled(1, 2, CR::SelfTradePrevention));
        CHECK_ORDER(h.book, 3, infoOf(t));
        CHECK_TOP(h.book, 102, 103);
    });

    add("stp: self makers beyond the limit price are never touched", __LINE__, [] {
        // STP 只作用于撮合循环真正会成交的 maker：不穿价的同账户挂单不受影响
        // STP only applies to makers that the matching loop would really trade with: same-account resting
        // orders that are not crossed are unaffected
        H h;
        const OrderRequest far = sell(2, 103, 2).account(kA);
        PLACE(h, sell(1, 101, 2).account(kA));
        PLACE(h, far);
        const OrderRequest t1 = buy(3, 102, 3).account(kA).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(1, 2, CR::SelfTradePrevention));
        CHECK_ORDER(h.book, 2, infoOf(far));
        CHECK_ORDER(h.book, 3, infoOf(t1));
        const OrderRequest t2 = buy(4, 102, 1).account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t2), accepted(t2));
        const OrderRequest t3 = buy(5, 101, 1).account(kA).stp(SM::CancelBoth).ioc();
        CHECK_EVENTS(h.submit(t3), accepted(t3), cancelled(5, 1, CR::IocRemainder));
        CHECK_ORDER(h.book, 2, infoOf(far));
        CHECK_ORDERS_AT(h.book, S::Buy, 102, infoOf(t1), infoOf(t2));
    });

    add("stp: CancelTaker/CancelBoth report SelfTradePrevention for market and IOC takers", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 1).account(kB));
        PLACE(h, sell(2, 102, 1).account(kA));
        const OrderRequest m = marketBuy(3, 5).account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(m), accepted(m), trade(1, 1, 3, kB, kA, S::Buy, 101, 1, 0, 4),
                     cancelled(3, 4, CR::SelfTradePrevention));
        CHECK_DEPTH(h.book, S::Sell, {102, 1, 1});
        const OrderRequest i = buy(4, 102, 2).ioc().account(kA).stp(SM::CancelBoth);
        CHECK_EVENTS(h.submit(i), accepted(i), cancelled(2, 1, CR::SelfTradePrevention),
                     cancelled(4, 2, CR::SelfTradePrevention));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("stp: account 0 is an ordinary account", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 1).account(0));
        const OrderRequest t1 = buy(2, 101, 1).account(0).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(t1), accepted(t1), cancelled(2, 1, CR::SelfTradePrevention));
        const OrderRequest t2 = buy(3, 101, 1).account(0);
        CHECK_EVENTS(h.submit(t2), accepted(t2), trade(1, 1, 3, 0, 0, S::Buy, 101, 1, 0, 0));
    });
}

// ============================================================================
// 撤单
// Cancel
// ============================================================================
template <template <class> class BookT>
void registerCancelSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("cancel: a live order is removed and reports its remaining quantity", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 5, CR::UserRequested));
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_TOP(h.book, std::nullopt, std::nullopt);
        CHECK_EQ(h.book.levelCount(S::Buy), 0u);
        CHECK_NO_ORDER(h.book, 1);
    });

    add("cancel: a partially filled order reports its open quantity", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 5).account(kA));
        const OrderRequest t = buy(2, 101, 2).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 2, 3, 0));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 3, CR::UserRequested));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("cancel: an unknown id is rejected", __LINE__, [] {
        H h;
        CHECK_EVENTS(h.cancel(42), rejectedCancel(42, RR::UnknownOrder));
        PLACE(h, buy(1, 100, 5));
        CHECK_EVENTS(h.cancel(2), rejectedCancel(2, RR::UnknownOrder));
        CHECK_EQ(h.book.orderCount(), 1u);
    });

    add("cancel: orders that are no longer live are unknown", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        const OrderRequest t = buy(2, 101, 2).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 2, 0, 0));
        CHECK_EVENTS(h.cancel(1), rejectedCancel(1));  // 完全成交的 maker
                                                       // fully filled maker
        CHECK_EVENTS(h.cancel(2), rejectedCancel(2));  // 完全成交的 taker
                                                       // fully filled taker
        const OrderRequest ioc = buy(3, 100, 1).ioc();
        CHECK_EVENTS(h.submit(ioc), accepted(ioc), cancelled(3, 1, CR::IocRemainder));
        CHECK_EVENTS(h.cancel(3), rejectedCancel(3));  // IOC 从不挂单
                                                       // an IOC never rests
        const OrderRequest mkt = marketSell(4, 1);
        CHECK_EVENTS(h.submit(mkt), accepted(mkt), cancelled(4, 1, CR::MarketRemainder));
        CHECK_EVENTS(h.cancel(4), rejectedCancel(4));
        CHECK_EVENTS(h.submit(buy(5, 0, 1)), rejectedNew(5, RR::InvalidPrice));
        CHECK_EVENTS(h.cancel(5), rejectedCancel(5));  // 被拒的订单
                                                       // rejected order
    });

    add("cancel: cancelling twice rejects the second request", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 5, CR::UserRequested));
        CHECK_EVENTS(h.cancel(1), rejectedCancel(1));
    });

    add("cancel: removing an order from the middle of a queue keeps the others' order", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 1).account(kA);
        const OrderRequest b3 = buy(3, 100, 3).account(kC);
        PLACE(h, b1);
        PLACE(h, buy(2, 100, 2).account(kB));
        PLACE(h, b3);
        CHECK_EVENTS(h.cancel(2), cancelled(2, 2, CR::UserRequested));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b1), infoOf(b3));
        CHECK_DEPTH(h.book, S::Buy, {100, 4, 2});
        const OrderRequest t = sell(4, 100, 4).account(kD);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 4, kA, kD, S::Sell, 100, 1, 0, 3),
                     trade(2, 3, 4, kC, kD, S::Sell, 100, 3, 0, 0));
    });

    add("cancel: cancelling the only order at the best level exposes the next level", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 1));
        PLACE(h, buy(2, 99, 1));
        PLACE(h, sell(3, 101, 1));
        PLACE(h, sell(4, 102, 1));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 1, CR::UserRequested));
        CHECK_TOP(h.book, 99, 101);
        CHECK_EVENTS(h.cancel(3), cancelled(3, 1, CR::UserRequested));
        CHECK_TOP(h.book, 99, 102);
        CHECK_EQ(h.book.levelCount(S::Buy), 1u);
        CHECK_EQ(h.book.levelCount(S::Sell), 1u);
    });
}

// ============================================================================
// 改单
// Modify
// ============================================================================
template <template <class> class BookT>
void registerModifySpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("modify: reducing quantity at the same price keeps time priority", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 5).account(kA);
        const OrderRequest b2 = buy(2, 100, 5).account(kB);
        PLACE(h, b1);
        PLACE(h, b2);
        CHECK_EVENTS(h.modify(1, 100, 3), modified(1, 100, 3));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b1, 3), infoOf(b2));
        CHECK_DEPTH(h.book, S::Buy, {100, 8, 2});
        const OrderRequest t = sell(3, 100, 4).account(kC);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 3, kA, kC, S::Sell, 100, 3, 0, 1),
                     trade(2, 2, 3, kB, kC, S::Sell, 100, 1, 4, 0));
    });

    add("modify: same price and same quantity keeps priority and still reports Modified", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 5).account(kA);
        const OrderRequest b2 = buy(2, 100, 5).account(kB);
        PLACE(h, b1);
        PLACE(h, b2);
        CHECK_EVENTS(h.modify(1, 100, 5), modified(1, 100, 5));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b1), infoOf(b2));
    });

    add("modify: increasing quantity loses time priority", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 5).account(kA);
        const OrderRequest b2 = buy(2, 100, 5).account(kB);
        PLACE(h, b1);
        PLACE(h, b2);
        CHECK_EVENTS(h.modify(1, 100, 6), modified(1, 100, 6));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b2), infoOf(b1, 6));
        CHECK_DEPTH(h.book, S::Buy, {100, 11, 2});
        const OrderRequest t = sell(3, 100, 6).account(kC);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 2, 3, kB, kC, S::Sell, 100, 5, 0, 1),
                     trade(2, 1, 3, kA, kC, S::Sell, 100, 1, 5, 0));
    });

    add("modify: a price change loses time priority even when reducing quantity", __LINE__, [] {
        H h;
        const OrderRequest b2 = buy(2, 100, 5).account(kB);
        const OrderRequest b3 = buy(3, 99, 1).account(kC);
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, b2);
        PLACE(h, b3);
        CHECK_EVENTS(h.modify(1, 99, 2), modified(1, 99, 2));
        CHECK_ORDERS_AT(h.book, S::Buy, 99, infoOf(b3), orderInfo(1, kA, S::Buy, 99, 2));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b2));
        CHECK_EVENTS(h.modify(1, 100, 2), modified(1, 100, 2));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b2), orderInfo(1, kA, S::Buy, 100, 2));
        CHECK_ORDERS_AT(h.book, S::Buy, 99, infoOf(b3));
    });

    add("modify: moving the only order of a level removes that level", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, buy(2, 97, 1).account(kB));
        CHECK_EVENTS(h.modify(1, 98, 5), modified(1, 98, 5));
        CHECK_DEPTH(h.book, S::Buy, {98, 5, 1}, {97, 1, 1});
        CHECK_EQ(h.book.volumeAt(S::Buy, 100), 0);
        CHECK_NO_ORDERS_AT(h.book, S::Buy, 100);
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Buy, 98, 5));
    });

    add("modify: increasing the only order of a level keeps a single-order level", __LINE__, [] {
        H h;
        const OrderRequest b = buy(1, 100, 5);
        PLACE(h, b);
        CHECK_EVENTS(h.modify(1, 100, 7), modified(1, 100, 7));
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b, 7));
        CHECK_DEPTH(h.book, S::Buy, {100, 7, 1});
    });

    add("modify: repricing through the spread trades at the maker price and rests the rest", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 5).account(kA));
        PLACE(h, sell(2, 101, 3).account(kB));
        PLACE(h, sell(3, 103, 1).account(kB));
        CHECK_EVENTS(h.modify(1, 102, 5), modified(1, 102, 5),
                     trade(1, 2, 1, kB, kA, S::Buy, 101, 3, 0, 2));
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Buy, 102, 2));
        CHECK_TOP(h.book, 102, 103);
        CHECK_EQ(h.book.lastTradeId(), 1u);
        CHECK_EVENTS(h.cancel(1), cancelled(1, 2, CR::UserRequested));  // 撤单报告改单成交后的剩余量
                                                                        // the cancel reports the remainder
                                                                        // left after the modify's fill
    });

    add("modify: an aggressive modify that fills completely leaves no order", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 2).account(kA));
        PLACE(h, sell(2, 101, 5).account(kB));
        CHECK_EVENTS(h.modify(1, 101, 2), modified(1, 101, 2),
                     trade(1, 2, 1, kB, kA, S::Buy, 101, 2, 3, 0));
        CHECK_NO_ORDER(h.book, 1);
        CHECK_EQ(h.book.orderCount(), 1u);
        CHECK_EVENTS(h.cancel(1), rejectedCancel(1));
        CHECK_EVENTS(h.modify(1, 100, 1), rejectedModify(1, RR::UnknownOrder));
    });

    add("modify: a sell-side modify can sweep several bid levels", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 105, 10).account(kA));
        PLACE(h, buy(2, 101, 2).account(kB));
        PLACE(h, buy(3, 100, 3).account(kC));
        PLACE(h, buy(4, 99, 4).account(kB));
        CHECK_EVENTS(h.modify(1, 100, 6), modified(1, 100, 6),
                     trade(1, 2, 1, kB, kA, S::Sell, 101, 2, 0, 4),
                     trade(2, 3, 1, kC, kA, S::Sell, 100, 3, 0, 1));
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Sell, 100, 1));
        CHECK_TOP(h.book, 99, 100);
    });

    add("modify: a post-only order cannot be repriced to cross or lock", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).postOnly().account(kA));
        PLACE(h, buy(3, 100, 2).account(kB));
        PLACE(h, sell(2, 102, 5).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.modify(1, 102, 5), rejectedModify(1, RR::PostOnlyWouldCross));  // 锁价
                                                                                       // locking
        CHECK_EVENTS(h.modify(1, 103, 1), rejectedModify(1, RR::PostOnlyWouldCross));  // 穿价
                                                                                       // crossing
        CHECK_EVENTS(h.modify(1, 500, 9), rejectedModify(1, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before, h.book);  // 包括队列位置
                                          // including queue position

        H g;
        PLACE(g, buy(1, 100, 5).account(kB));
        PLACE(g, sell(2, 103, 5).postOnly().account(kA));
        CHECK_EVENTS(g.modify(2, 100, 5), rejectedModify(2, RR::PostOnlyWouldCross));
        CHECK_EVENTS(g.modify(2, 99, 5), rejectedModify(2, RR::PostOnlyWouldCross));
        CHECK_EVENTS(g.modify(2, 101, 5), modified(2, 101, 5));
        CHECK_ORDER(g.book, 2, orderInfo(2, kA, S::Sell, 101, 5, true));
    });

    add("modify: a post-only order may change quantity at the same price", __LINE__, [] {
        H h;
        const OrderRequest p = buy(1, 100, 5).postOnly().account(kA);
        const OrderRequest q = buy(2, 100, 1).account(kB);
        PLACE(h, p);
        PLACE(h, q);
        PLACE(h, sell(3, 101, 5).account(kB));
        CHECK_EVENTS(h.modify(1, 100, 9), modified(1, 100, 9));  // 加量：排到队尾
                                                                 // increase: moves to the back of the queue
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(q), infoOf(p, 9));
        CHECK_EVENTS(h.modify(1, 100, 4), modified(1, 100, 4));  // 减量：原地
                                                                 // decrease: in place
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(q), infoOf(p, 4));
    });

    add("modify: a post-only order may be repriced without crossing", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).postOnly().account(kA).stp(SM::CancelBoth));
        PLACE(h, sell(2, 103, 5).account(kB));
        CHECK_EVENTS(h.modify(1, 102, 4), modified(1, 102, 4));
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Buy, 102, 4, true, SM::CancelBoth));
        CHECK_TOP(h.book, 102, 103);
        // 对手方为空时任意价格都不穿价
        // With the opposite side empty, no price crosses
        CHECK_EVENTS(h.cancel(2), cancelled(2, 5, CR::UserRequested));
        CHECK_EVENTS(h.modify(1, 5000, 4), modified(1, 5000, 4));
        CHECK_TOP(h.book, 5000, std::nullopt);
    });

    add("modify: validation order is unknown id, quantity, price, post-only", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 5).postOnly().account(kA));
        PLACE(h, sell(2, 101, 5).account(kB));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.modify(99, 0, 0), rejectedModify(99, RR::UnknownOrder));
        CHECK_EVENTS(h.modify(99, 101, 5), rejectedModify(99, RR::UnknownOrder));
        CHECK_EVENTS(h.modify(1, 0, 0), rejectedModify(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(1, 101, -1), rejectedModify(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(1, -5, 5), rejectedModify(1, RR::InvalidPrice));
        CHECK_EVENTS(h.modify(1, 0, 5), rejectedModify(1, RR::InvalidPrice));
        CHECK_EVENTS(h.modify(1, 101, 5), rejectedModify(1, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before, h.book);

        // 卖出 post-only 改到 <= 0 的价格时 crosses() 为真：价格规则必须先于 post-only 规则
        // When a post-only sell is modified to a price <= 0, crosses() is true: the price rule must come before
        // the post-only rule
        H g;
        PLACE(g, buy(1, 100, 5).account(kA));
        PLACE(g, sell(2, 102, 5).postOnly().account(kB));
        const StateSnapshot before_g = snapshotOf(g.book);
        CHECK_EVENTS(g.modify(2, -5, 5), rejectedModify(2, RR::InvalidPrice));
        CHECK_EVENTS(g.modify(2, 0, 5), rejectedModify(2, RR::InvalidPrice));
        CHECK_EVENTS(g.modify(2, 0, 0), rejectedModify(2, RR::InvalidQuantity));
        CHECK_EVENTS(g.modify(2, 100, 5), rejectedModify(2, RR::PostOnlyWouldCross));
        CHECK_UNCHANGED(before_g, g.book);
    });

    add("modify: orders that are no longer live are unknown", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        const OrderRequest t = buy(2, 101, 2).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 2, 0, 0));
        CHECK_EVENTS(h.modify(1, 101, 1), rejectedModify(1, RR::UnknownOrder));
        CHECK_EVENTS(h.modify(2, 101, 1), rejectedModify(2, RR::UnknownOrder));
        PLACE(h, buy(3, 100, 1));
        CHECK_EVENTS(h.cancel(3), cancelled(3, 1, CR::UserRequested));
        CHECK_EVENTS(h.modify(3, 100, 1), rejectedModify(3, RR::UnknownOrder));
        CHECK_EVENTS(h.modify(77, 100, 1), rejectedModify(77, RR::UnknownOrder));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("modify: the new quantity is compared with the remaining open quantity", __LINE__, [] {
        H h;
        const OrderRequest a1 = sell(1, 101, 10).account(kA);
        const OrderRequest a2 = sell(2, 101, 5).account(kB);
        PLACE(h, a1);
        PLACE(h, a2);
        const OrderRequest t = buy(3, 101, 4).account(kC);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 3, kA, kC, S::Buy, 101, 4, 6, 0));
        CHECK_EVENTS(h.modify(1, 101, 6), modified(1, 101, 6));  // 等于剩余量：原地
                                                                 // equal to the remaining quantity: in place
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a1, 6), infoOf(a2));
        CHECK_EVENTS(h.modify(1, 101, 7), modified(1, 101, 7));  // 大于剩余量（仍小于原始 10）：失去优先级
                                                                 // above the remaining quantity
                                                                 // (still below the original 10): loses priority
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a2), infoOf(a1, 7));
    });

    add("modify: STP CancelTaker during an aggressive modify cancels the modified order", __LINE__, [] {
        H h;
        const OrderRequest m = sell(2, 101, 3).account(kA);
        PLACE(h, buy(1, 99, 5).account(kA).stp(SM::CancelTaker));
        PLACE(h, m);
        CHECK_EVENTS(h.modify(1, 101, 5), modified(1, 101, 5),
                     cancelled(1, 5, CR::SelfTradePrevention));
        CHECK_NO_ORDER(h.book, 1);
        CHECK_ORDER(h.book, 2, infoOf(m));
        CHECK_DEPTH_EMPTY(h.book, S::Buy);
    });

    add("modify: STP CancelMaker during an aggressive modify", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 5).account(kA).stp(SM::CancelMaker));
        PLACE(h, sell(2, 101, 2).account(kA));
        PLACE(h, sell(3, 101, 2).account(kB));
        CHECK_EVENTS(h.modify(1, 101, 5), modified(1, 101, 5),
                     cancelled(2, 2, CR::SelfTradePrevention),
                     trade(1, 3, 1, kB, kA, S::Buy, 101, 2, 0, 3));
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Buy, 101, 3, false, SM::CancelMaker));
        CHECK_TOP(h.book, 101, std::nullopt);
    });

    add("modify: STP CancelBoth during an aggressive modify", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 105, 4).account(kA).stp(SM::CancelBoth));
        PLACE(h, buy(2, 101, 1).account(kB));
        PLACE(h, buy(3, 100, 2).account(kA));
        PLACE(h, buy(4, 100, 2).account(kB));
        CHECK_EVENTS(h.modify(1, 99, 4), modified(1, 99, 4),
                     trade(1, 2, 1, kB, kA, S::Sell, 101, 1, 0, 3),
                     cancelled(3, 2, CR::SelfTradePrevention),
                     cancelled(1, 3, CR::SelfTradePrevention));
        CHECK_NO_ORDER(h.book, 1);
        CHECK_DEPTH(h.book, S::Buy, {100, 2, 1});
        CHECK_DEPTH_EMPTY(h.book, S::Sell);
    });

    add("modify: self-trade during a modify happens when the order's STP mode is None", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 99, 3).account(kA));
        PLACE(h, sell(2, 101, 3).account(kA));
        CHECK_EVENTS(h.modify(1, 101, 3), modified(1, 101, 3),
                     trade(1, 2, 1, kA, kA, S::Buy, 101, 3, 0, 0));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("modify: a repriced order keeps its account, post-only flag and STP mode", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 110, 5).account(kC).postOnly().stp(SM::CancelTaker));
        PLACE(h, sell(2, 108, 1).account(kD).stp(SM::CancelMaker));
        CHECK_EVENTS(h.modify(1, 109, 8), modified(1, 109, 8));
        CHECK_ORDER(h.book, 1, orderInfo(1, kC, S::Sell, 109, 8, true, SM::CancelTaker));
        CHECK_EVENTS(h.modify(2, 108, 3), modified(2, 108, 3));  // 加量
                                                                 // increase
        CHECK_ORDER(h.book, 2, orderInfo(2, kD, S::Sell, 108, 3, false, SM::CancelMaker));
        // 改价后的订单作为 maker：成交回报中的账户来自原订单
        // The repriced orders act as makers: the account in each fill report comes from the original order
        const OrderRequest t = buy(3, 109, 4).account(kA);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 2, 3, kD, kA, S::Buy, 108, 3, 0, 1),
                     trade(2, 1, 3, kC, kA, S::Buy, 109, 1, 7, 0));
    });

    add("modify: modify is not subject to the capacity limit", __LINE__, [] {
        H h(2);
        PLACE(h, buy(1, 100, 5).account(kA));
        PLACE(h, sell(2, 105, 5).account(kB));
        CHECK_EVENTS(h.submit(buy(3, 99, 1)), rejectedNew(3, RR::CapacityExceeded));
        CHECK_EVENTS(h.modify(1, 101, 50), modified(1, 101, 50));
        CHECK_EVENTS(h.modify(2, 104, 1), modified(2, 104, 1));
        CHECK_EVENTS(h.modify(1, 104, 50), modified(1, 104, 50),
                     trade(1, 2, 1, kB, kA, S::Buy, 104, 1, 0, 49));
        CHECK_ORDER(h.book, 1, orderInfo(1, kA, S::Buy, 104, 49));
        CHECK_EQ(h.book.orderCount(), 1u);
    });

    add("modify: a quantity increase that would push the level total past INT64_MAX is rejected", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        const OrderRequest a1 = sell(1, 100, kMax - 10).account(kA);
        const OrderRequest a2 = sell(2, 100, 5).account(kB);
        const OrderRequest a3 = sell(3, 100, 1).account(kC);
        PLACE(h, a1);
        PLACE(h, a2);
        PLACE(h, a3);
        const StateSnapshot before = snapshotOf(h.book);
        // 其他订单共 INT64_MAX - 9：2 号单最多加到 9（它自己原来的 5 不计入）
        // The other orders total INT64_MAX - 9: order 2 can grow to at most 9 (its own original 5 does not count)
        CHECK_EVENTS(h.modify(2, 100, 10), rejectedModify(2, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(2, 100, kMax), rejectedModify(2, RR::InvalidQuantity));
        CHECK_UNCHANGED(before, h.book);  // 被拒的改单保留原队列位置
                                          // a rejected modify keeps the original queue position
        CHECK_EVENTS(h.modify(2, 100, 9), modified(2, 100, 9));
        CHECK_DEPTH(h.book, S::Sell, {100, kMax, 3});
        CHECK_ORDERS_AT(h.book, S::Sell, 100, infoOf(a1), infoOf(a3), infoOf(a2, 9));
        // 原地减量从不受限；减量空出来的数量可以再加回去
        // An in-place decrease is never limited; the quantity freed by a decrease can be added back
        CHECK_EVENTS(h.modify(1, 100, kMax - 12), modified(1, 100, kMax - 12));
        CHECK_EVENTS(h.modify(3, 100, 4), rejectedModify(3, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(3, 100, 3), modified(3, 100, 3));
        CHECK_ORDERS_AT(h.book, S::Sell, 100, infoOf(a1, kMax - 12), infoOf(a2, 9), infoOf(a3, 3));
        CHECK_DEPTH(h.book, S::Sell, {100, kMax, 3});
        // 独占一档的订单可以加到 INT64_MAX
        // An order alone at its level can grow to INT64_MAX
        const OrderRequest b4 = buy(4, 90, 1).account(kD);
        PLACE(h, b4);
        CHECK_EVENTS(h.modify(4, 90, kMax), modified(4, 90, kMax));
        CHECK_ORDER(h.book, 4, infoOf(b4, kMax));
    });

    add("modify: a price change onto a level whose total would pass INT64_MAX is rejected", __LINE__, [] {
        H h;
        constexpr Qty kMax = kMaxLevelQty;
        PLACE(h, buy(1, 100, kMax - 3).account(kA));
        PLACE(h, buy(2, 98, 4).account(kB));
        PLACE(h, sell(3, 105, 1).account(kC));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.modify(2, 100, 4), rejectedModify(2, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(2, 100, kMax), rejectedModify(2, RR::InvalidQuantity));
        CHECK_UNCHANGED(before, h.book);
        // 数量减到 3 才放得下：恰好到 INT64_MAX
        // It only fits once the quantity is reduced to 3: exactly INT64_MAX
        CHECK_EVENTS(h.modify(2, 100, 3), modified(2, 100, 3));
        CHECK_DEPTH(h.book, S::Buy, {100, kMax, 2});
        CHECK_ORDER(h.book, 2, orderInfo(2, kB, S::Buy, 100, 3));
        // 改到没有档位的价格只受订单自身数量的限制；改回满档价位时只看目标档位的现有总量
        // Moving to a price with no level is limited only by the order's own quantity; moving back to the price of
        // the full level only considers the target level's existing total
        CHECK_EVENTS(h.modify(1, 99, kMax), modified(1, 99, kMax));
        CHECK_DEPTH(h.book, S::Buy, {100, 3, 1}, {99, kMax, 1});
        CHECK_EVENTS(h.modify(1, 100, kMax - 2), rejectedModify(1, RR::InvalidQuantity));
        // 穿价的改价：本侧在新价格上不可能已有档位，成交后剩余量挂成新的一档
        // A reprice that crosses: this side cannot already have a level at the new price, and after the fills the
        // remainder rests as a new level
        CHECK_EVENTS(h.modify(2, 105, kMax), modified(2, 105, kMax),
                     trade(1, 3, 2, kC, kB, S::Buy, 105, 1, 0, kMax - 1));
        CHECK_DEPTH(h.book, S::Buy, {105, kMax - 1, 1}, {99, kMax, 1});
        CHECK_EVENTS(h.modify(1, 105, 2), rejectedModify(1, RR::InvalidQuantity));
        CHECK_EVENTS(h.modify(1, 105, 1), modified(1, 105, 1));
        CHECK_DEPTH(h.book, S::Buy, {105, kMax, 2});
    });
}

// ============================================================================
// 容量上限（max_orders）
// Capacity limit (max_orders)
// ============================================================================
template <template <class> class BookT>
void registerCapacitySpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("capacity: a GTC limit order is rejected when full even if it would fully match", __LINE__, [] {
        H h(2);
        PLACE(h, sell(1, 101, 5).account(kA));
        PLACE(h, buy(2, 99, 5).account(kA));
        const StateSnapshot before = snapshotOf(h.book);
        CHECK_EVENTS(h.submit(buy(3, 101, 5).account(kB)), rejectedNew(3, RR::CapacityExceeded));
        CHECK_EVENTS(h.submit(sell(4, 99, 1).account(kB)), rejectedNew(4, RR::CapacityExceeded));
        CHECK_UNCHANGED(before, h.book);
    });

    add("capacity: IOC, FOK and market orders are not subject to the limit", __LINE__, [] {
        H h(2);
        PLACE(h, sell(1, 101, 5).account(kA));
        PLACE(h, buy(2, 99, 5).account(kA));
        const OrderRequest i1 = buy(3, 101, 2).ioc().account(kB);
        CHECK_EVENTS(h.submit(i1), accepted(i1), trade(1, 1, 3, kA, kB, S::Buy, 101, 2, 3, 0));
        const OrderRequest f1 = buy(4, 101, 1).fok().account(kB);
        CHECK_EVENTS(h.submit(f1), accepted(f1), trade(2, 1, 4, kA, kB, S::Buy, 101, 1, 2, 0));
        const OrderRequest m = marketSell(5, 1).account(kB);
        CHECK_EVENTS(h.submit(m), accepted(m), trade(3, 2, 5, kA, kB, S::Sell, 99, 1, 4, 0));
        const OrderRequest i2 = buy(6, 100, 1).ioc().account(kB);
        CHECK_EVENTS(h.submit(i2), accepted(i2), cancelled(6, 1, CR::IocRemainder));
        const OrderRequest f2 = sell(7, 200, 1).fok().account(kB);
        CHECK_EVENTS(h.submit(f2), accepted(f2), cancelled(7, 1, CR::FokUnfillable));
        CHECK_EQ(h.book.orderCount(), 2u);
    });

    add("capacity: post-only orders count as GTC", __LINE__, [] {
        H h(1);
        PLACE(h, buy(1, 100, 1));
        CHECK_EVENTS(h.submit(sell(2, 105, 1).postOnly()), rejectedNew(2, RR::CapacityExceeded));
        CHECK_EQ(h.book.orderCount(), 1u);
    });

    add("capacity: a slot freed by a cancel or a fill can be reused", __LINE__, [] {
        H h(1);
        PLACE(h, buy(1, 100, 5).account(kA));
        CHECK_EVENTS(h.submit(buy(2, 99, 1).account(kA)), rejectedNew(2, RR::CapacityExceeded));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 5, CR::UserRequested));
        const OrderRequest b2 = buy(2, 99, 1).account(kA);
        CHECK_EVENTS(h.submit(b2), accepted(b2));
        const OrderRequest s = sell(3, 99, 1).ioc().account(kB);
        CHECK_EVENTS(h.submit(s), accepted(s), trade(1, 2, 3, kA, kB, S::Sell, 99, 1, 0, 0));
        const OrderRequest b4 = buy(4, 98, 1).account(kA);
        CHECK_EVENTS(h.submit(b4), accepted(b4));
        CHECK_EQ(h.book.orderCount(), 1u);
    });

    add("capacity: max_orders = 0 means unlimited", __LINE__, [] {
        H h;
        // 批量灌单时绕过逐步检查以节省时间，最后统一检查
        // The bulk load bypasses the per-step checks to save time and checks once at the end
        for (OrderId id = 1; id <= 2000; ++id) {
            const Side side = id % 2 ? S::Buy : S::Sell;
            const Price price = side == S::Buy ? 1000 - static_cast<Price>(id % 40)
                                               : 1001 + static_cast<Price>(id % 40);
            h.book.submit(Req(id, side, price, 1));
        }
        h.verify("bulk load");
        CHECK_EQ(h.book.orderCount(), 2000u);
        CHECK_EQ(h.listener.events.size(), 2000u);
        bool all_accepted = true;
        for (const Event& e : h.listener.events) all_accepted = all_accepted && e.kind == Event::Kind::Accepted;
        CHECK(all_accepted);
    });
}

// ============================================================================
// 订单 id 与成交 id
// Order ids and trade ids
// ============================================================================
template <template <class> class BookT>
void registerIdSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("ids: an id can be reused once its order is gone", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 2).account(kA));
        const OrderRequest t = buy(2, 101, 2).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 2, 0, 0));
        const OrderRequest r1 = buy(1, 100, 3).account(kC);  // 已成交 maker 的 id，且换了方向
                                                             // id of a filled maker, with the side switched
        CHECK_EVENTS(h.submit(r1), accepted(r1));
        const OrderRequest r2 = sell(2, 105, 1).account(kC);  // 已成交 taker 的 id
                                                              // id of a filled taker
        CHECK_EVENTS(h.submit(r2), accepted(r2));
        CHECK_EVENTS(h.cancel(1), cancelled(1, 3, CR::UserRequested));
        const OrderRequest r3 = buy(1, 99, 1);  // 已撤订单的 id
                                                // id of a cancelled order
        CHECK_EVENTS(h.submit(r3), accepted(r3));
        const OrderRequest ioc = sell(3, 200, 1).ioc();
        CHECK_EVENTS(h.submit(ioc), accepted(ioc), cancelled(3, 1, CR::IocRemainder));
        const OrderRequest r4 = sell(3, 200, 1);  // IOC 结束后立即复用
                                                  // reused right after the IOC finished
        CHECK_EVENTS(h.submit(r4), accepted(r4));
        CHECK_ORDER(h.book, 1, infoOf(r3));
        CHECK_ORDER(h.book, 2, infoOf(r2));
        CHECK_ORDER(h.book, 3, infoOf(r4));
    });

    add("ids: zero and the maximum id are ordinary ids", __LINE__, [] {
        H h;
        constexpr OrderId kMax = UINT64_MAX;
        const OrderRequest z = buy(0, 100, 1).account(kA);
        CHECK_EVENTS(h.submit(z), accepted(z));
        CHECK_ORDER(h.book, 0, infoOf(z));
        CHECK_EVENTS(h.submit(buy(0, 99, 1)), rejectedNew(0, RR::DuplicateOrderId));
        const OrderRequest mx = sell(kMax, 101, 2).account(kB);
        CHECK_EVENTS(h.submit(mx), accepted(mx));
        const OrderRequest t = buy(kMax - 1, 101, 1).account(kC);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, kMax, kMax - 1, kB, kC, S::Buy, 101, 1, 1, 0));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(mx, 1));
        CHECK_EVENTS(h.modify(kMax, 101, 5), modified(kMax, 101, 5));
        CHECK_EVENTS(h.cancel(0), cancelled(0, 1, CR::UserRequested));
        CHECK_EVENTS(h.cancel(kMax), cancelled(kMax, 5, CR::UserRequested));
        CHECK_EQ(h.book.orderCount(), 0u);
    });

    add("ids: trade ids start at 1 and increase by one per trade", __LINE__, [] {
        H h;
        CHECK_EQ(h.book.lastTradeId(), 0u);
        PLACE(h, sell(1, 101, 1).account(kA));
        PLACE(h, sell(2, 102, 1).account(kA));
        PLACE(h, sell(3, 103, 5).account(kA));
        // 不成交的操作不消耗成交 id
        // Operations without trades do not consume trade ids
        CHECK_EVENTS(h.submit(buy(9, 0, 1)), rejectedNew(9, RR::InvalidPrice));
        CHECK_EVENTS(h.cancel(42), rejectedCancel(42));
        const OrderRequest k = buy(4, 103, 100).fok().account(kB);
        CHECK_EVENTS(h.submit(k), accepted(k), cancelled(4, 100, CR::FokUnfillable));
        const OrderRequest s = buy(5, 101, 1).account(kA).stp(SM::CancelTaker);
        CHECK_EVENTS(h.submit(s), accepted(s), cancelled(5, 1, CR::SelfTradePrevention));
        CHECK_EQ(h.book.lastTradeId(), 0u);
        const OrderRequest t1 = buy(6, 102, 2).account(kB);
        CHECK_EVENTS(h.submit(t1), accepted(t1), trade(1, 1, 6, kA, kB, S::Buy, 101, 1, 0, 1),
                     trade(2, 2, 6, kA, kB, S::Buy, 102, 1, 0, 0));
        CHECK_EQ(h.book.lastTradeId(), 2u);
        PLACE(h, buy(7, 100, 1).account(kB));
        CHECK_EVENTS(h.modify(7, 103, 2), modified(7, 103, 2),
                     trade(3, 3, 7, kA, kB, S::Buy, 103, 2, 3, 0));
        CHECK_EQ(h.book.lastTradeId(), 3u);
        const OrderRequest m = marketBuy(8, 1).account(kB);
        CHECK_EVENTS(h.submit(m), accepted(m), trade(4, 3, 8, kA, kB, S::Buy, 103, 1, 2, 0));
        CHECK_EQ(h.book.lastTradeId(), 4u);
    });

    add("ids: trade ids are counted per book", __LINE__, [] {
        H h;
        H g;
        PLACE(h, sell(1, 101, 1).account(kA));
        PLACE(g, sell(1, 101, 1).account(kA));
        const OrderRequest t = buy(2, 101, 1).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 1, 0, 0));
        CHECK_EQ(h.book.lastTradeId(), 1u);
        CHECK_EQ(g.book.lastTradeId(), 0u);
        CHECK_EVENTS(g.submit(t), accepted(t), trade(1, 1, 2, kA, kB, S::Buy, 101, 1, 0, 0));
    });
}

// ============================================================================
// 查询接口
// Query interface
// ============================================================================
template <template <class> class BookT>
void registerQuerySpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("queries: an empty book", __LINE__, [] {
        H h;
        CHECK_TOP(h.book, std::nullopt, std::nullopt);
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_EQ(h.book.levelCount(S::Buy), 0u);
        CHECK_EQ(h.book.levelCount(S::Sell), 0u);
        CHECK_DEPTH_EMPTY(h.book, S::Buy);
        CHECK_DEPTH_EMPTY(h.book, S::Sell);
        CHECK(h.book.depth(S::Buy, 5).empty());
        CHECK_NO_ORDERS_AT(h.book, S::Buy, 100);
        CHECK_NO_ORDER(h.book, 1);
        CHECK_NO_ORDER(h.book, 0);
        CHECK_EQ(h.book.volumeAt(S::Sell, 100), 0);
        CHECK_EQ(h.book.lastTradeId(), 0u);
        std::string why;
        CHECK(h.book.checkInvariants(&why));
        CHECK(h.book.checkInvariants());
    });

    add("queries: depth honours max_levels", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 1));
        PLACE(h, buy(2, 99, 2));
        PLACE(h, buy(3, 98, 3));
        PLACE(h, buy(4, 97, 4));
        PLACE(h, sell(5, 101, 5));
        CHECK(h.book.depth(S::Buy, 0).empty());
        CHECK_LEVELS(h.book.depth(S::Buy, 1), {100, 1, 1});
        CHECK_LEVELS(h.book.depth(S::Buy, 2), {100, 1, 1}, {99, 2, 1});
        CHECK_LEVELS(h.book.depth(S::Buy, 4), {100, 1, 1}, {99, 2, 1}, {98, 3, 1}, {97, 4, 1});
        CHECK_LEVELS(h.book.depth(S::Buy, 100), {100, 1, 1}, {99, 2, 1}, {98, 3, 1}, {97, 4, 1});
        CHECK_LEVELS(h.book.depth(S::Buy), {100, 1, 1}, {99, 2, 1}, {98, 3, 1}, {97, 4, 1});
        CHECK_LEVELS(h.book.depth(S::Sell, 3), {101, 5, 1});
        CHECK(h.book.depth(S::Sell, 0).empty());
    });

    add("queries: depth aggregates remaining quantity and order count per level", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 101, 4).account(kA));
        PLACE(h, sell(2, 101, 6).account(kB));
        PLACE(h, sell(3, 102, 1).account(kC));
        CHECK_DEPTH(h.book, S::Sell, {101, 10, 2}, {102, 1, 1});
        const OrderRequest t = buy(4, 101, 5).account(kD);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 1, 4, kA, kD, S::Buy, 101, 4, 0, 1),
                     trade(2, 2, 4, kB, kD, S::Buy, 101, 1, 5, 0));
        CHECK_DEPTH(h.book, S::Sell, {101, 5, 1}, {102, 1, 1});
        CHECK_EVENTS(h.modify(2, 101, 2), modified(2, 101, 2));
        CHECK_DEPTH(h.book, S::Sell, {101, 2, 1}, {102, 1, 1});
    });

    add("queries: ordersAt is specific to the exact price and side", __LINE__, [] {
        H h;
        const OrderRequest b1 = buy(1, 100, 1).account(kA);
        const OrderRequest b2 = buy(2, 100, 2).account(kB);
        const OrderRequest a3 = sell(3, 101, 3).account(kC);
        PLACE(h, b1);
        PLACE(h, b2);
        PLACE(h, a3);
        CHECK_ORDERS_AT(h.book, S::Buy, 100, infoOf(b1), infoOf(b2));
        CHECK_ORDERS_AT(h.book, S::Sell, 101, infoOf(a3));
        CHECK_NO_ORDERS_AT(h.book, S::Sell, 100);
        CHECK_NO_ORDERS_AT(h.book, S::Buy, 101);
        CHECK_NO_ORDERS_AT(h.book, S::Buy, 99);
        CHECK_NO_ORDERS_AT(h.book, S::Sell, 102);
    });

    add("queries: findOrder returns every field of a live order", __LINE__, [] {
        H h;
        PLACE(h, sell(7, 120, 9).account(kC).postOnly().stp(SM::CancelBoth));
        CHECK_ORDER(h.book, 7, orderInfo(7, kC, S::Sell, 120, 9, true, SM::CancelBoth));
        const OrderRequest t = buy(8, 120, 4).account(kA);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 7, 8, kC, kA, S::Buy, 120, 4, 5, 0));
        CHECK_ORDER(h.book, 7, orderInfo(7, kC, S::Sell, 120, 5, true, SM::CancelBoth));
        CHECK_NO_ORDER(h.book, 8);
    });

    add("queries: volumeAt sums a level and is 0 elsewhere", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 2));
        PLACE(h, buy(2, 100, 3));
        PLACE(h, sell(3, 102, 4));
        CHECK_EQ(h.book.volumeAt(S::Buy, 100), 5);
        CHECK_EQ(h.book.volumeAt(S::Sell, 102), 4);
        CHECK_EQ(h.book.volumeAt(S::Sell, 100), 0);
        CHECK_EQ(h.book.volumeAt(S::Buy, 102), 0);
        CHECK_EQ(h.book.volumeAt(S::Buy, 101), 0);
    });

    add("queries: orders at the minimum and the maximum price", __LINE__, [] {
        // 每一步之后 verify() 都要对挂在最高价 INT64_MAX 的卖单、价格 1 的买单做完整的查询一致性检查
        // After every step verify() has to run the full query consistency check on a sell order resting at the
        // maximum price INT64_MAX and a buy order at price 1
        H h;
        constexpr Price kTop = INT64_MAX;
        const OrderRequest a1 = sell(1, kTop, 3).account(kA);
        const OrderRequest b2 = buy(2, 1, 4).account(kB);
        PLACE(h, a1);
        PLACE(h, b2);
        PLACE(h, sell(3, kTop - 1, 1).account(kA));
        CHECK_TOP(h.book, 1, kTop - 1);
        CHECK_DEPTH(h.book, S::Sell, {kTop - 1, 1, 1}, {kTop, 3, 1});
        CHECK_DEPTH(h.book, S::Buy, {1, 4, 1});
        CHECK_EQ(h.book.volumeAt(S::Sell, kTop), 3);
        CHECK_ORDERS_AT(h.book, S::Sell, kTop, infoOf(a1));
        // 限价 INT64_MAX 的买单扫过两档卖单，剩余量挂在最高价
        // A buy with limit INT64_MAX sweeps both ask levels, and its remainder rests at the maximum price
        const OrderRequest t = buy(4, kTop, 5).account(kC);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 3, 4, kA, kC, S::Buy, kTop - 1, 1, 0, 4),
                     trade(2, 1, 4, kA, kC, S::Buy, kTop, 3, 0, 1));
        CHECK_TOP(h.book, kTop, std::nullopt);
        CHECK_DEPTH(h.book, S::Buy, {kTop, 1, 1}, {1, 4, 1});
        // 限价 1 的卖单从最高价一路扫到价格 1，剩余量挂在价格 1
        // A sell with limit 1 sweeps from the maximum price all the way down to price 1, and the remainder
        // rests at price 1
        const OrderRequest s = sell(5, 1, 6).account(kD);
        CHECK_EVENTS(h.submit(s), accepted(s), trade(3, 4, 5, kC, kD, S::Sell, kTop, 1, 0, 5),
                     trade(4, 2, 5, kB, kD, S::Sell, 1, 4, 0, 1));
        CHECK_TOP(h.book, std::nullopt, 1);
        CHECK_ORDER(h.book, 5, infoOf(s, 1));
    });

    add("queries: best prices follow the book as levels appear and disappear", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 1).account(kA));
        CHECK_TOP(h.book, 100, std::nullopt);
        PLACE(h, buy(2, 101, 1).account(kA));
        CHECK_TOP(h.book, 101, std::nullopt);
        PLACE(h, sell(3, 103, 1).account(kA));
        CHECK_TOP(h.book, 101, 103);
        PLACE(h, sell(4, 102, 1).account(kA));
        CHECK_TOP(h.book, 101, 102);
        const OrderRequest t = sell(5, 101, 1).account(kB);
        CHECK_EVENTS(h.submit(t), accepted(t), trade(1, 2, 5, kA, kB, S::Sell, 101, 1, 0, 0));
        CHECK_TOP(h.book, 100, 102);
        CHECK_EVENTS(h.cancel(1), cancelled(1, 1, CR::UserRequested));
        CHECK_TOP(h.book, std::nullopt, 102);
        CHECK_EVENTS(h.cancel(4), cancelled(4, 1, CR::UserRequested));
        CHECK_TOP(h.book, std::nullopt, 103);
        CHECK_EVENTS(h.cancel(3), cancelled(3, 1, CR::UserRequested));
        CHECK_TOP(h.book, std::nullopt, std::nullopt);
    });

    add("queries: queries never call the listener", __LINE__, [] {
        H h;
        PLACE(h, buy(1, 100, 2).account(kA));
        PLACE(h, sell(2, 102, 4).account(kB));
        h.listener.clear();
        const auto& book = h.book;
        (void)book.bestBid();
        (void)book.bestAsk();
        (void)book.orderCount();
        (void)book.levelCount(S::Buy);
        (void)book.depth(S::Sell);
        (void)book.ordersAt(S::Buy, 100);
        (void)book.findOrder(1);
        (void)book.findOrder(3);
        (void)book.volumeAt(S::Sell, 102);
        (void)book.lastTradeId();
        (void)book.checkInvariants();
        CHECK(h.listener.events.empty());
    });
}

// ============================================================================
// 综合脚本：多种规则交织，每一步完整比对事件
// Combined script: several rules interleaved, with the full events compared at every step
// ============================================================================
template <template <class> class BookT>
void registerScenarioSpecs(const SpecRegistrar& add) {
    using H = SpecHarness<BookT>;

    add("scenario: interleaved rules keep exact events and invariants", __LINE__, [] {
        H h;
        PLACE(h, sell(1, 105, 5).account(kA));
        PLACE(h, sell(2, 104, 3).account(kB).postOnly());
        PLACE(h, buy(3, 100, 4).account(kC));
        PLACE(h, buy(4, 101, 2).account(kA).stp(SM::CancelBoth));
        const OrderRequest t5 = buy(5, 104, 4).account(kC).stp(SM::CancelMaker);
        CHECK_EVENTS(h.submit(t5), accepted(t5), trade(1, 2, 5, kB, kC, S::Buy, 104, 3, 0, 1));
        CHECK_DEPTH(h.book, S::Buy, {104, 1, 1}, {101, 2, 1}, {100, 4, 1});
        CHECK_DEPTH(h.book, S::Sell, {105, 5, 1});
        // 4 号单改价穿到 105，遇到同账户的 1 号单：CancelBoth
        // Order 4 is repriced through to 105 and meets order 1 of the same account: CancelBoth
        CHECK_EVENTS(h.modify(4, 105, 2), modified(4, 105, 2),
                     cancelled(1, 5, CR::SelfTradePrevention),
                     cancelled(4, 2, CR::SelfTradePrevention));
        CHECK_DEPTH(h.book, S::Buy, {104, 1, 1}, {100, 4, 1});
        CHECK_DEPTH_EMPTY(h.book, S::Sell);
        const OrderRequest f6 = sell(6, 99, 10).fok().account(kB);
        CHECK_EVENTS(h.submit(f6), accepted(f6), cancelled(6, 10, CR::FokUnfillable));
        const OrderRequest m7 = marketSell(7, 3).account(kB);
        CHECK_EVENTS(h.submit(m7), accepted(m7), trade(2, 5, 7, kC, kB, S::Sell, 104, 1, 0, 2),
                     trade(3, 3, 7, kC, kB, S::Sell, 100, 2, 2, 0));
        CHECK_EVENTS(h.modify(3, 100, 1), modified(3, 100, 1));
        CHECK_EVENTS(h.cancel(3), cancelled(3, 1, CR::UserRequested));
        CHECK_EQ(h.book.orderCount(), 0u);
        CHECK_EQ(h.book.lastTradeId(), 3u);
    });
}

}  // namespace spec

// ============================================================================
// 入口：注册完整规范套件，用例名为 "<impl_name>/<area>: <description>"
// Entry point: registers the full spec suite, with case names "<impl_name>/<area>: <description>"
// ============================================================================
template <template <class> class BookT>
void registerMatchingSpecTests(const std::string& impl_name) {
    const SpecRegistrar add(impl_name);
    spec::registerValidationSpecs<BookT>(add);
    spec::registerRestingSpecs<BookT>(add);
    spec::registerMatchingSpecs<BookT>(add);
    spec::registerMarketSpecs<BookT>(add);
    spec::registerIocSpecs<BookT>(add);
    spec::registerFokSpecs<BookT>(add);
    spec::registerStpSpecs<BookT>(add);
    spec::registerCancelSpecs<BookT>(add);
    spec::registerModifySpecs<BookT>(add);
    spec::registerCapacitySpecs<BookT>(add);
    spec::registerIdSpecs<BookT>(add);
    spec::registerQuerySpecs<BookT>(add);
    spec::registerScenarioSpecs<BookT>(add);
}

}  // namespace hft::matching::test

// 允许不带命名空间直接调用 registerMatchingSpecTests<...>("...")
// Allows calling registerMatchingSpecTests<...>("...") directly without the namespace
using hft::matching::test::registerMatchingSpecTests;

#undef PLACE
