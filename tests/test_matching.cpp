//
// test_matching.cpp
// 撮合引擎测试：规范套件（参考模型 + 朴素模型）、参考模型随机自检、差分测试、测试工具自身的校验。
//

#include "matching/types.h"
#include "matching_spec_tests.h"
#include "matching_test_util.h"
#include "naive_order_book.h"
#include "reference_order_book.h"
#include "test_framework.h"

#include <cstdint>
#include <cstdio>
#include <map>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

using namespace hft::matching;
using namespace hft::matching::test;

namespace {

// ---- 静态初始化期注册规范套件 ----
// "reference"：参考模型（差分测试的标准答案）
// "naive"：第二个独立实现，交叉验证规范用例本身与参考模型
// 优化引擎完成后在这里追加：registerMatchingSpecTests<hft::matching::LimitOrderBook>("optimized");
struct RegisterSpecSuites {
    RegisterSpecSuites() {
        registerMatchingSpecTests<ReferenceOrderBook>("reference");
        registerMatchingSpecTests<NaiveOrderBook>("naive");
    }
} const kRegisterSpecSuites;

using RefBook = ReferenceOrderBook<RecordingListener>;
using NaiveBook = NaiveOrderBook<RecordingListener>;

// 单个订单簿 + 记录型监听器：逐条执行命令并返回该命令产生的事件
template <class Book>
struct Stepper {
    RecordingListener listener;
    Book book;
    explicit Stepper(std::size_t max_orders = 0) : listener(), book(listener, max_orders) {}
    std::vector<Event> run(const Command& cmd) {
        listener.clear();
        hft::matching::test::apply(book, cmd);
        return listener.take();
    }
};

// ============================================================================
// 与实现无关的单命令事件流检查：只依赖命令前后的查询结果，
// 独立地重新推导校验结果、数量守恒、成交 id 连续、价格不穿越限价、STP 事件结构等。
// ============================================================================

struct PreState {
    std::optional<OrderInfo> target;  // 命令执行前 findOrder(cmd.id)
    std::size_t order_count = 0;
    std::optional<Price> best_bid;
    std::optional<Price> best_ask;
    TradeId last_trade_id = 0;
};

template <class Book>
PreState capturePre(const Book& book, const Command& cmd) {
    PreState p;
    p.target = book.findOrder(cmd.id);
    p.order_count = book.orderCount();
    p.best_bid = book.bestBid();
    p.best_ask = book.bestAsk();
    p.last_trade_id = book.lastTradeId();
    return p;
}

struct TakerView {
    OrderId id;
    AccountId account;
    Side side;
    bool market;
    Price limit;
    TimeInForce tif;
    StpMode stp;
    Qty qty;
    bool post_only;
};

// 检查 Accepted/Modified 之后的成交与撤单序列，以及命令结束后 taker 是否正确挂单
template <class Book>
bool checkTakerTail(const std::vector<Event>& ev, std::size_t start, const TakerView& t,
                    const Book& book, std::string* why) {
    auto fail = [why](const std::string& msg) {
        if (why) *why = msg;
        return false;
    };
    Qty remaining = t.qty;
    bool taker_done = false;
    std::optional<CancelReason> final_reason;
    std::optional<Price> last_price;
    for (std::size_t i = start; i < ev.size(); ++i) {
        const Event& e = ev[i];
        if (taker_done) return fail("event after the taker's final cancel: " + e.toString());
        if (e.kind == Event::Kind::Trade) {
            const Trade& tr = e.trade;
            if (tr.taker_id != t.id || tr.taker_account != t.account || tr.taker_side != t.side)
                return fail("trade does not describe the taker: " + e.toString());
            if (tr.maker_id == t.id) return fail("order traded with itself: " + e.toString());
            if (tr.qty <= 0 || tr.qty > remaining) return fail("bad trade quantity: " + e.toString());
            if (tr.taker_remaining != remaining - tr.qty) return fail("bad taker_remaining: " + e.toString());
            if (tr.maker_remaining < 0) return fail("negative maker_remaining: " + e.toString());
            if (!t.market && (t.side == Side::Buy ? tr.price > t.limit : tr.price < t.limit))
                return fail("trade through the limit price: " + e.toString());
            if (last_price && (t.side == Side::Buy ? tr.price < *last_price : tr.price > *last_price))
                return fail("trade prices violate price priority: " + e.toString());
            if (t.stp != StpMode::None && tr.maker_account == t.account)
                return fail("traded with a same-account maker although STP is on: " + e.toString());
            last_price = tr.price;
            remaining = tr.taker_remaining;
        } else if (e.kind == Event::Kind::Cancelled) {
            if (e.id == t.id) {
                taker_done = true;
                final_reason = e.cancel_reason;
                if (e.qty != remaining) return fail("taker cancel qty != remaining: " + e.toString());
                switch (e.cancel_reason) {
                    case CancelReason::SelfTradePrevention: {
                        if (t.stp != StpMode::CancelTaker && t.stp != StpMode::CancelBoth)
                            return fail("taker cancelled by STP with mode " +
                                        std::string(hft::matching::toString(t.stp)));
                        if (t.tif == TimeInForce::FOK)
                            return fail("FOK taker cancelled by STP after passing its pre-check");
                        const bool after_maker_cancel =
                            i > start && ev[i - 1].kind == Event::Kind::Cancelled && ev[i - 1].id != t.id;
                        if (t.stp == StpMode::CancelBoth && !after_maker_cancel)
                            return fail("CancelBoth taker cancel is not preceded by the maker cancel");
                        break;
                    }
                    case CancelReason::IocRemainder:
                        if (t.market || t.tif != TimeInForce::IOC) return fail("IocRemainder for a non-IOC limit taker");
                        break;
                    case CancelReason::MarketRemainder:
                        if (!t.market || t.tif == TimeInForce::FOK) return fail("MarketRemainder for a non-market taker");
                        break;
                    case CancelReason::FokUnfillable:
                        if (t.tif != TimeInForce::FOK || i != start || remaining != t.qty)
                            return fail("bad FokUnfillable cancel: " + e.toString());
                        break;
                    case CancelReason::UserRequested:
                        return fail("taker cancelled as UserRequested");
                }
            } else {
                if (e.cancel_reason != CancelReason::SelfTradePrevention)
                    return fail("maker cancelled for a reason other than STP: " + e.toString());
                if (t.stp != StpMode::CancelMaker && t.stp != StpMode::CancelBoth)
                    return fail("maker cancelled by STP while taker mode is " +
                                std::string(hft::matching::toString(t.stp)));
                if (t.stp == StpMode::CancelBoth &&
                    (i + 1 >= ev.size() || ev[i + 1].kind != Event::Kind::Cancelled || ev[i + 1].id != t.id))
                    return fail("CancelBoth maker cancel not followed by the taker cancel");
                if (book.findOrder(e.id)) return fail("STP-cancelled maker is still live: " + e.toString());
            }
        } else {
            return fail("unexpected event after acceptance: " + e.toString());
        }
    }
    const std::optional<OrderInfo> resting = book.findOrder(t.id);
    const std::optional<Price> opposite = t.side == Side::Buy ? book.bestAsk() : book.bestBid();
    const bool still_crosses =
        opposite && (t.market || (t.side == Side::Buy ? t.limit >= *opposite : t.limit <= *opposite));
    // 正常剩余撤单意味着撮合循环是因"无可成交对手"而结束的
    if (final_reason == CancelReason::MarketRemainder && opposite)
        return fail("MarketRemainder although the opposite side is not empty");
    if (final_reason == CancelReason::IocRemainder && still_crosses)
        return fail("IocRemainder although the opposite side still crosses the limit");
    if (taker_done || remaining == 0) {
        if (resting) return fail("taker rests although it is done: " + str(resting));
        return true;
    }
    if (t.market || t.tif != TimeInForce::GTC)
        return fail("non-GTC taker ended with an open remainder and no cancel");
    const OrderInfo expect{t.id, t.account, t.side, t.limit, remaining, t.post_only, t.stp};
    if (resting != std::optional<OrderInfo>(expect))
        return fail("rested remainder " + str(resting) + " != expected " + str(expect));
    if (still_crosses) return fail("taker rested while it still crosses the opposite side");
    return true;
}

template <class Book>
bool checkCommandEvents(const Command& cmd, const PreState& pre, const std::vector<Event>& ev,
                        const Book& book, std::size_t max_orders, std::string* why) {
    auto fail = [why](const std::string& msg) {
        if (why) *why = msg;
        return false;
    };
    if (ev.empty()) return fail("command produced no events");
    TradeId expect_tid = pre.last_trade_id;
    for (const Event& e : ev) {
        if (e.kind == Event::Kind::Trade && e.trade.trade_id != ++expect_tid)
            return fail("trade id not consecutive: " + e.toString());
    }
    if (book.lastTradeId() != expect_tid)
        return fail("lastTradeId() = " + std::to_string(book.lastTradeId()) + ", expected " +
                    std::to_string(expect_tid));
    auto crossedBefore = [&pre](Side side, Price p) {
        if (side == Side::Buy) return pre.best_ask.has_value() && p >= *pre.best_ask;
        return pre.best_bid.has_value() && p <= *pre.best_bid;
    };

    switch (cmd.type) {
        case RequestType::New: {
            const OrderRequest& r = cmd.order;
            const bool limit_gtc = r.type == OrderType::Limit && r.tif == TimeInForce::GTC;
            std::optional<RejectReason> expect;
            if (r.qty <= 0) expect = RejectReason::InvalidQuantity;
            else if (r.type == OrderType::Limit && r.price <= 0) expect = RejectReason::InvalidPrice;
            else if (r.post_only && !limit_gtc) expect = RejectReason::InvalidFlags;
            else if (pre.target) expect = RejectReason::DuplicateOrderId;
            else if (max_orders != 0 && pre.order_count >= max_orders && limit_gtc) expect = RejectReason::CapacityExceeded;
            else if (r.post_only && crossedBefore(r.side, r.price)) expect = RejectReason::PostOnlyWouldCross;
            if (expect) return sameEvents(ev, {rejectedNew(r.id, *expect)}, why);
            if (ev[0] != accepted(r)) return fail("first event is not Accepted(request): " + ev[0].toString());
            const TakerView t{r.id, r.account, r.side, r.type == OrderType::Market, r.price, r.tif, r.stp,
                              r.qty, r.post_only};
            return checkTakerTail(ev, 1, t, book, why);
        }
        case RequestType::Cancel: {
            if (!pre.target) return sameEvents(ev, {rejectedCancel(cmd.id)}, why);
            if (!sameEvents(ev, {cancelled(cmd.id, pre.target->remaining, CancelReason::UserRequested)}, why))
                return false;
            if (book.findOrder(cmd.id)) return fail("cancelled order is still live");
            return true;
        }
        case RequestType::Modify: {
            if (!pre.target) return sameEvents(ev, {rejectedModify(cmd.id, RejectReason::UnknownOrder)}, why);
            const OrderInfo& o = *pre.target;
            std::optional<RejectReason> expect;
            if (cmd.qty <= 0) expect = RejectReason::InvalidQuantity;
            else if (cmd.price <= 0) expect = RejectReason::InvalidPrice;
            else if (o.post_only && cmd.price != o.price && crossedBefore(o.side, cmd.price))
                expect = RejectReason::PostOnlyWouldCross;
            if (expect) {
                if (!sameEvents(ev, {rejectedModify(cmd.id, *expect)}, why)) return false;
                if (book.findOrder(cmd.id) != pre.target) return fail("rejected modify changed the order");
                return true;
            }
            if (ev[0] != modified(cmd.id, cmd.price, cmd.qty))
                return fail("first event is not Modified: " + ev[0].toString());
            if (cmd.price == o.price && cmd.qty <= o.remaining) {
                if (ev.size() != 1) return fail("in-place modify emitted extra events");
                OrderInfo expect_info = o;
                expect_info.remaining = cmd.qty;
                if (book.findOrder(cmd.id) != std::optional<OrderInfo>(expect_info))
                    return fail("in-place modify left " + str(book.findOrder(cmd.id)));
                return true;
            }
            const TakerView t{cmd.id, o.account, o.side, false, cmd.price, TimeInForce::GTC, o.stp,
                              cmd.qty, o.post_only};
            return checkTakerTail(ev, 1, t, book, why);
        }
    }
    return fail("unknown command type");
}

// ============================================================================
// 覆盖率统计：确认随机流真的走到了每一条规则分支
// ============================================================================

struct Coverage {
    std::map<std::string, std::uint64_t> counts;
    std::unordered_set<OrderId> ever_accepted;

    void hit(const std::string& key) { ++counts[key]; }
    std::uint64_t get(const std::string& key) const {
        auto it = counts.find(key);
        return it == counts.end() ? 0 : it->second;
    }

    void classify(const Command& cmd, const PreState& pre, const std::vector<Event>& ev) {
        hit(std::string("command/") + hft::matching::toString(cmd.type));
        std::optional<Price> first_trade_price;
        bool multi_level = false;
        bool any_trade = false;
        for (std::size_t i = 0; i < ev.size(); ++i) {
            const Event& e = ev[i];
            switch (e.kind) {
                case Event::Kind::Accepted:
                    hit("accepted");
                    break;
                case Event::Kind::Rejected:
                    hit(std::string("reject/") + hft::matching::toString(e.request_type) + "/" +
                        hft::matching::toString(e.reject_reason));
                    break;
                case Event::Kind::Trade:
                    any_trade = true;
                    hit("trade");
                    hit(e.trade.maker_remaining == 0 ? "trade/maker_filled" : "trade/maker_partial");
                    if (e.trade.maker_account == e.trade.taker_account) hit("trade/self_trade");
                    if (!first_trade_price) first_trade_price = e.trade.price;
                    else if (*first_trade_price != e.trade.price) multi_level = true;
                    break;
                case Event::Kind::Cancelled:
                    hit(std::string("cancel/") + hft::matching::toString(e.cancel_reason));
                    if (e.cancel_reason == CancelReason::SelfTradePrevention) {
                        // maker 撤单后紧跟 taker 撤单 = CancelBoth；单独的 maker 撤单 = CancelMaker；
                        // 前面没有 maker 撤单的 taker 撤单 = CancelTaker
                        const bool is_taker = e.id == cmd.id;
                        const bool next_is_taker = i + 1 < ev.size() &&
                                                   ev[i + 1].kind == Event::Kind::Cancelled &&
                                                   ev[i + 1].id == cmd.id;
                        const bool prev_is_maker = i > 0 && ev[i - 1].kind == Event::Kind::Cancelled &&
                                                   ev[i - 1].id != cmd.id;
                        if (!is_taker) hit(next_is_taker ? "stp/CancelBoth" : "stp/CancelMaker");
                        else if (!prev_is_maker) hit("stp/CancelTaker");
                    }
                    break;
                case Event::Kind::Modified:
                    hit("modified");
                    if (pre.target && cmd.price == pre.target->price && cmd.qty <= pre.target->remaining)
                        hit("modify/in_place");
                    else
                        hit("modify/requeue");
                    break;
            }
        }
        if (multi_level) hit("sweep/multi_level");
        if (cmd.type == RequestType::Modify && any_trade) hit("modify/aggressive");
        if (cmd.type == RequestType::New && !ev.empty() && ev[0].kind == Event::Kind::Accepted) {
            const OrderRequest& r = cmd.order;
            if (!ever_accepted.insert(r.id).second) hit("id/reused_after_gone");
            if (r.id == 0 || r.id >= UINT64_MAX - 1) hit("id/edge");
            if (r.post_only) hit("new/post_only_accepted");
            if (r.type == OrderType::Market) hit(std::string("new/market_") + hft::matching::toString(r.tif));
            if (r.tif == TimeInForce::FOK) {
                const bool killed = ev.size() == 2 && ev[1].kind == Event::Kind::Cancelled &&
                                    ev[1].cancel_reason == CancelReason::FokUnfillable;
                hit(killed ? "fok/killed" : "fok/filled");
            }
        }
    }

    std::string summary() const {
        std::string s;
        for (const auto& [key, n] : counts) s += "    " + key + " = " + std::to_string(n) + "\n";
        return s;
    }
};

struct FuzzRun {
    const char* label;
    std::uint64_t seed;
    std::size_t commands;
    std::size_t max_orders;
    GeneratorConfig gen;
};

GeneratorConfig makeConfig(void (*tweak)(GeneratorConfig&)) {
    GeneratorConfig cfg;
    if (tweak) tweak(cfg);
    return cfg;
}

std::vector<FuzzRun> referenceRuns() {
    return {
        {"default mix", 1, 60'000, 0, GeneratorConfig()},
        {"two accounts, heavy STP", 2, 50'000, 0, makeConfig([](GeneratorConfig& c) { c.accounts = 2; })},
        {"capacity limited", 3, 40'000, 48, makeConfig([](GeneratorConfig& c) { c.max_live = 120; })},
        {"wide prices, small id space", 4, 40'000, 0, makeConfig([](GeneratorConfig& c) {
             c.passive_ticks = 60;
             c.aggressive_ticks = 12;
             c.id_space = 700;
         })},
        {"modify heavy, fast drift", 5, 40'000, 0, makeConfig([](GeneratorConfig& c) {
             c.weight_new = 45;
             c.weight_cancel = 15;
             c.weight_modify = 40;
             c.drift_rate = 0.2;
         })},
    };
}

std::string describe(const FuzzRun& run, std::size_t index, const Command& cmd,
                     const std::vector<Event>& ev) {
    return std::string("run '") + run.label + "' seed " + std::to_string(run.seed) + " command #" +
           std::to_string(index) + " " + cmd.toString() + "\n    events: " + str(ev) + "\n    ";
}

}  // namespace

// ============================================================================
// 参考模型随机自检：每条命令后 checkInvariants()，并做独立的事件流检查
// ============================================================================

TEST_CASE("reference/fuzz: random streams keep invariants and a self-consistent event stream") {
    Coverage cov;
    std::size_t total = 0;
    for (const FuzzRun& run : referenceRuns()) {
        RandomCommandGenerator gen(run.seed, run.gen);
        Stepper<RefBook> s(run.max_orders);
        std::size_t peak_orders = 0;
        for (std::size_t i = 0; i < run.commands; ++i) {
            const Command cmd = gen.next();
            const PreState pre = capturePre(s.book, cmd);
            const std::vector<Event> ev = s.run(cmd);
            std::string why;
            if (!s.book.checkInvariants(&why)) FAIL(describe(run, i, cmd, ev) + "checkInvariants: " + why);
            if (!checkCommandEvents(cmd, pre, ev, s.book, run.max_orders, &why))
                FAIL(describe(run, i, cmd, ev) + "event stream: " + why);
            if (i % 64 == 0 && !queriesConsistent(s.book, &why))
                FAIL(describe(run, i, cmd, ev) + "queries: " + why);
            cov.classify(cmd, pre, ev);
            peak_orders = std::max(peak_orders, s.book.orderCount());
        }
        std::string why;
        CHECK(queriesConsistent(s.book, &why));
        if (run.max_orders) CHECK_LE(peak_orders, run.max_orders);
        std::printf("    %-28s seed %llu: %zu commands, last trade id %llu, %zu live orders (peak %zu)\n",
                    run.label, static_cast<unsigned long long>(run.seed), run.commands,
                    static_cast<unsigned long long>(s.book.lastTradeId()), s.book.orderCount(), peak_orders);
        total += run.commands;
    }
    CHECK_GE(total, 200'000u);

    // 每个规则分支都必须被随机流触发过
    const char* required[] = {
        "accepted", "trade", "trade/maker_filled", "trade/maker_partial", "trade/self_trade",
        "sweep/multi_level", "reject/New/InvalidQuantity", "reject/New/InvalidPrice",
        "reject/New/InvalidFlags", "reject/New/DuplicateOrderId", "reject/New/CapacityExceeded",
        "reject/New/PostOnlyWouldCross", "reject/Cancel/UnknownOrder", "reject/Modify/UnknownOrder",
        "reject/Modify/InvalidQuantity", "reject/Modify/InvalidPrice", "reject/Modify/PostOnlyWouldCross",
        "cancel/UserRequested", "cancel/IocRemainder", "cancel/MarketRemainder", "cancel/FokUnfillable",
        "cancel/SelfTradePrevention", "stp/CancelMaker", "stp/CancelTaker", "stp/CancelBoth",
        "modified", "modify/in_place", "modify/requeue", "modify/aggressive", "fok/killed", "fok/filled",
        "new/post_only_accepted", "new/market_GTC", "new/market_IOC", "new/market_FOK",
        "id/reused_after_gone", "id/edge",
    };
    bool all = true;
    for (const char* key : required) {
        if (cov.get(key) == 0) {
            all = false;
            ::hft_test::reportFailure(__FILE__, __LINE__, std::string("branch never exercised: ") + key);
        }
    }
    if (!all) std::printf("%s", cov.summary().c_str());
    std::printf("    coverage: %llu trades, %llu STP cancels, %llu in-place modifies, %llu FOK kills\n",
                static_cast<unsigned long long>(cov.get("trade")),
                static_cast<unsigned long long>(cov.get("cancel/SelfTradePrevention")),
                static_cast<unsigned long long>(cov.get("modify/in_place")),
                static_cast<unsigned long long>(cov.get("fok/killed")));
}

// ============================================================================
// 差分测试：参考模型 vs 朴素模型（数据结构完全不同的两个实现）
// ============================================================================

TEST_CASE("differential/reference vs naive: identical event streams and book state") {
    const std::vector<FuzzRun> runs = {
        {"default mix", 101, 30'000, 0, GeneratorConfig()},
        {"two accounts", 102, 25'000, 0, makeConfig([](GeneratorConfig& c) { c.accounts = 2; })},
        {"capacity limited", 103, 25'000, 40, makeConfig([](GeneratorConfig& c) { c.max_live = 100; })},
        {"wide prices, modify heavy", 104, 25'000, 0, makeConfig([](GeneratorConfig& c) {
             c.passive_ticks = 50;
             c.aggressive_ticks = 10;
             c.weight_modify = 35;
         })},
    };
    for (const FuzzRun& run : runs) {
        RandomCommandGenerator gen(run.seed, run.gen);
        Stepper<RefBook> ref(run.max_orders);
        Stepper<NaiveBook> naive(run.max_orders);
        for (std::size_t i = 0; i < run.commands; ++i) {
            const Command cmd = gen.next();
            const std::vector<Event> expected = ref.run(cmd);
            const std::vector<Event> actual = naive.run(cmd);
            std::string why;
            if (!sameEvents(actual, expected, &why))
                FAIL(describe(run, i, cmd, expected) + "naive diverged from reference:\n" + why);
            if (!ref.book.checkInvariants(&why)) FAIL(describe(run, i, cmd, expected) + "reference: " + why);
            if (i % 16 == 15 || i + 1 == run.commands) {
                if (!naive.book.checkInvariants(&why)) FAIL(describe(run, i, cmd, expected) + "naive: " + why);
                if (!sameState(ref.book, naive.book, &why))
                    FAIL(describe(run, i, cmd, expected) + "state differs: " + why);
            }
        }
    }
}

TEST_CASE("differential/reference vs reference: replaying a stream is deterministic") {
    RandomCommandGenerator gen_a(77);
    RandomCommandGenerator gen_b(77);
    Stepper<RefBook> a;
    Stepper<RefBook> b;
    for (std::size_t i = 0; i < 20'000; ++i) {
        const Command ca = gen_a.next();
        const Command cb = gen_b.next();
        REQUIRE(ca.toString() == cb.toString());
        std::string why;
        if (!sameEvents(b.run(cb), a.run(ca), &why)) FAIL("replay diverged at #" + std::to_string(i) + ": " + why);
        if (i % 32 == 0 && !sameState(a.book, b.book, &why)) FAIL("replay state differs: " + why);
    }
    std::string why;
    CHECK(sameState(a.book, b.book, &why));
}

// ============================================================================
// 测试工具自身：生成器、sameState、queriesConsistent 必须真的能发现问题
// ============================================================================

namespace {

// 包装一个真实订单簿，并按开关篡改某个查询的结果
template <class Inner>
struct TamperedBook {
    enum class Fault {
        None, BestBid, BestAsk, OrderCount, LevelCount, DepthQty, DepthOrders, DepthPrefix,
        OrdersAtRemaining, OrdersAtOrder, FindOrder, VolumeAt, LastTradeId
    };
    const Inner& inner;
    Fault fault = Fault::None;

    std::optional<Price> bestBid() const {
        auto p = inner.bestBid();
        if (fault == Fault::BestBid && p) *p -= 1;
        return p;
    }
    std::optional<Price> bestAsk() const {
        auto p = inner.bestAsk();
        if (fault == Fault::BestAsk && p) *p += 1;
        return p;
    }
    std::size_t orderCount() const { return inner.orderCount() + (fault == Fault::OrderCount ? 1 : 0); }
    std::size_t levelCount(Side s) const { return inner.levelCount(s) + (fault == Fault::LevelCount ? 1 : 0); }
    std::vector<LevelInfo> depth(Side s, std::size_t n = SIZE_MAX) const {
        std::vector<LevelInfo> d = inner.depth(s, n);
        if (!d.empty() && s == Side::Sell) {
            if (fault == Fault::DepthQty) d.front().qty += 1;
            if (fault == Fault::DepthOrders) d.front().orders += 1;
            if (fault == Fault::DepthPrefix && n == 1) d.front().qty += 1;
        }
        return d;
    }
    std::vector<OrderInfo> ordersAt(Side s, Price p) const {
        std::vector<OrderInfo> v = inner.ordersAt(s, p);
        if (fault == Fault::OrdersAtRemaining && !v.empty()) v.back().remaining += 1;
        if (fault == Fault::OrdersAtOrder && v.size() >= 2) std::swap(v[0], v[1]);
        return v;
    }
    std::optional<OrderInfo> findOrder(OrderId id) const {
        auto o = inner.findOrder(id);
        if (fault == Fault::FindOrder && o) o->remaining += 1;
        return o;
    }
    Qty volumeAt(Side s, Price p) const {
        const Qty v = inner.volumeAt(s, p);
        return v + (fault == Fault::VolumeAt && v > 0 ? 1 : 0);
    }
    TradeId lastTradeId() const { return inner.lastTradeId() + (fault == Fault::LastTradeId ? 1 : 0); }
};

}  // namespace

TEST_CASE("tools: sameState and queriesConsistent detect every tampered query") {
    Stepper<RefBook> s;
    for (const OrderRequest& r : {OrderRequest(buy(1, 100, 2)), OrderRequest(buy(2, 100, 3)),
                                  OrderRequest(buy(3, 99, 1)), OrderRequest(sell(4, 102, 4).account(2)),
                                  OrderRequest(sell(5, 102, 1).account(3)), OrderRequest(sell(6, 104, 2))}) {
        s.run(Command::newOrder(r));
    }
    s.run(Command::newOrder(buy(7, 102, 1).account(4)));  // 产生一笔成交，使 lastTradeId 非零
    REQUIRE_EQ(s.book.lastTradeId(), 1u);

    using T = TamperedBook<RefBook>;
    T clean{s.book};
    std::string why;
    CHECK(queriesConsistent(clean, &why));
    CHECK(sameState(s.book, clean, &why));

    // queriesConsistent 能发现的（单个簿内部矛盾）
    for (T::Fault f : {T::Fault::BestBid, T::Fault::BestAsk, T::Fault::OrderCount, T::Fault::LevelCount,
                       T::Fault::DepthQty, T::Fault::DepthOrders, T::Fault::DepthPrefix,
                       T::Fault::OrdersAtRemaining, T::Fault::FindOrder, T::Fault::VolumeAt}) {
        T bad{s.book, f};
        why.clear();
        CHECK(!queriesConsistent(bad, &why));
        CHECK(!why.empty());
    }
    // sameState 能发现的（与真实簿对比）：所有篡改都会被发现
    for (T::Fault f : {T::Fault::BestBid, T::Fault::BestAsk, T::Fault::OrderCount, T::Fault::LevelCount,
                       T::Fault::DepthQty, T::Fault::DepthOrders, T::Fault::OrdersAtRemaining,
                       T::Fault::OrdersAtOrder, T::Fault::FindOrder, T::Fault::VolumeAt,
                       T::Fault::LastTradeId}) {
        T bad{s.book, f};
        why.clear();
        CHECK(!sameState(s.book, bad, &why));
        CHECK(!why.empty());
    }
}

TEST_CASE("tools: sameState detects a real divergence between two books") {
    Stepper<RefBook> a;
    Stepper<RefBook> b;
    RandomCommandGenerator gen(5);
    for (std::size_t i = 0; i < 2000; ++i) {
        const Command cmd = gen.next();
        a.run(cmd);
        b.run(cmd);
    }
    std::string why;
    REQUIRE(sameState(a.book, b.book, &why));
    REQUIRE(a.book.orderCount() > 0);
    // 撤掉 a 中一个挂单：两簿必须被判为不同
    const std::vector<LevelInfo> bids = a.book.depth(Side::Buy);
    const std::vector<LevelInfo> asks = a.book.depth(Side::Sell);
    const LevelInfo& level = bids.empty() ? asks.front() : bids.front();
    const Side side = bids.empty() ? Side::Sell : Side::Buy;
    a.run(Command::cancel(a.book.ordersAt(side, level.price).front().id));
    CHECK(!sameState(a.book, b.book, &why));
    CHECK(!why.empty());
}

TEST_CASE("tools: event comparison reports the first difference") {
    const std::vector<Event> x = {accepted(buy(1, 100, 5)), trade(1, 2, 1, 1, 2, Side::Buy, 100, 5, 0, 0)};
    std::vector<Event> y = x;
    std::string why;
    CHECK(sameEvents(x, y, &why));
    y[1].trade.maker_remaining = 1;
    CHECK(!sameEvents(x, y, &why));
    CHECK(why.find(">> [1]") != std::string::npos);
    y.pop_back();
    CHECK(!sameEvents(x, y, &why));
    // 不同种类的事件即使字段巧合也不相等
    CHECK(cancelled(1, 5, CancelReason::UserRequested) != modified(1, 0, 5));
    CHECK(rejectedNew(1, RejectReason::UnknownOrder) != rejectedCancel(1));
    // Accepted 比较完整请求
    CHECK(accepted(buy(1, 100, 5)) != accepted(buy(1, 100, 5).stp(StpMode::CancelMaker)));
    CHECK(accepted(marketBuy(1, 5).price(3)) != accepted(marketBuy(1, 5)));
}

TEST_CASE("generator: same seed gives the same stream and different seeds differ") {
    RandomCommandGenerator g1(42);
    RandomCommandGenerator g2(42);
    RandomCommandGenerator g3(43);
    bool identical = true;
    bool differs = false;
    for (int i = 0; i < 5000; ++i) {
        const std::string c1 = g1.next().toString();
        identical = identical && c1 == g2.next().toString();
        differs = differs || c1 != g3.next().toString();
    }
    CHECK(identical);
    CHECK(differs);
}

TEST_CASE("generator: produces every request flavour around a drifting mid") {
    GeneratorConfig cfg;
    RandomCommandGenerator gen(9, cfg);
    std::map<std::string, int> seen;
    std::size_t max_tracked = 0;
    Price min_mid = gen.mid();
    Price max_mid = gen.mid();
    for (int i = 0; i < 60'000; ++i) {
        const Command c = gen.next();
        max_tracked = std::max(max_tracked, gen.trackedCount());
        min_mid = std::min(min_mid, gen.mid());
        max_mid = std::max(max_mid, gen.mid());
        ++seen[std::string("type/") + hft::matching::toString(c.type)];
        if (c.type == RequestType::New) {
            const OrderRequest& r = c.order;
            ++seen[std::string("order/") + hft::matching::toString(r.type) + "/" + hft::matching::toString(r.tif)];
            ++seen[std::string("stp/") + hft::matching::toString(r.stp)];
            if (r.post_only) ++seen["post_only"];
            if (r.qty <= 0) ++seen["invalid_qty"];
            if (r.type == OrderType::Limit && r.price <= 0) ++seen["invalid_price"];
            if (r.post_only && !(r.type == OrderType::Limit && r.tif == TimeInForce::GTC)) ++seen["invalid_flags"];
            if (r.id == 0 || r.id >= UINT64_MAX - 1) ++seen["edge_id"];
            if (r.account < 1 || r.account > cfg.accounts) ++seen["bad_account"];
            if (r.qty > 40) ++seen["large_qty"];
            if (r.type == OrderType::Limit && r.price > 0 &&
                (r.price < gen.mid() - cfg.passive_ticks - 1 || r.price > gen.mid() + cfg.passive_ticks + 1))
                ++seen["far_price"];
        } else if (c.type == RequestType::Modify) {
            if (c.qty <= 0) ++seen["modify_invalid_qty"];
            if (c.price <= 0) ++seen["modify_invalid_price"];
        }
    }
    for (const char* key : {"type/New", "type/Cancel", "type/Modify", "order/Limit/GTC", "order/Limit/IOC",
                            "order/Limit/FOK", "order/Market/GTC", "order/Market/IOC", "order/Market/FOK",
                            "stp/None", "stp/CancelTaker", "stp/CancelMaker", "stp/CancelBoth", "post_only",
                            "invalid_qty", "invalid_price", "invalid_flags", "edge_id", "large_qty",
                            "modify_invalid_qty", "modify_invalid_price"}) {
        if (seen[key] == 0) ::hft_test::reportFailure(__FILE__, __LINE__, std::string("generator never produced ") + key);
    }
    CHECK_EQ(seen["bad_account"], 0);
    CHECK_EQ(seen["far_price"], 0);  // 限价都在中间价附近几十个 tick 内
    CHECK_LE(max_tracked, cfg.max_live + 1);
    // 命令类型比例大致符合权重 60/22/18
    CHECK_GT(seen["type/New"], 30'000);
    CHECK_GT(seen["type/Cancel"], 9'000);
    CHECK_GT(seen["type/Modify"], 7'000);
    CHECK_LT(max_mid - min_mid, 1000);
}

// ============================================================================
// 参考模型特有的行为与规模测试
// ============================================================================

namespace {

// 在每个回调里检查簿的不变量：参考模型承诺回调发生时簿状态已一致
struct ProbeListener {
    const ReferenceOrderBook<ProbeListener>* book = nullptr;
    int callbacks = 0;
    int violations = 0;
    void probe() {
        ++callbacks;
        if (book && !book->checkInvariants()) ++violations;
    }
    void onAccepted(const OrderRequest&) { probe(); }
    void onRejected(OrderId, RequestType, RejectReason) { probe(); }
    void onTrade(const Trade&) { probe(); }
    void onCancelled(OrderId, Qty, CancelReason) { probe(); }
    void onModified(OrderId, Price, Qty) { probe(); }
};

}  // namespace

TEST_CASE("reference: the book is consistent whenever a callback runs") {
    ProbeListener probe;
    ReferenceOrderBook<ProbeListener> book(probe);
    probe.book = &book;
    RandomCommandGenerator gen(1234);
    for (int i = 0; i < 20'000; ++i) hft::matching::test::apply(book, gen.next());
    CHECK_GT(probe.callbacks, 20'000);
    CHECK_EQ(probe.violations, 0);
}

TEST_CASE("reference: long FIFO queues and deep books") {
    // 同一价位 3000 个订单（账户轮换），市价单吃掉前 2000 个再加 1 手
    Stepper<RefBook> s;
    for (OrderId id = 1; id <= 3000; ++id) {
        s.run(Command::newOrder(sell(id, 500, 2).account(static_cast<AccountId>(1 + id % 3))));
    }
    REQUIRE_EQ(s.book.orderCount(), 3000u);
    CHECK_LEVELS(s.book.depth(Side::Sell), {500, 6000, 3000});
    const std::vector<Event> ev = s.run(Command::newOrder(marketBuy(9000, 4001).account(9)));
    // Accepted + 2000 笔完整成交 + 1 笔部分成交；市价单全部成交，没有撤单
    REQUIRE_EQ(ev.size(), 2002u);
    bool fifo = true;
    for (std::size_t i = 1; i < ev.size(); ++i) {
        const Trade& t = ev[i].trade;
        fifo = fifo && ev[i].kind == Event::Kind::Trade && t.maker_id == i && t.trade_id == i && t.price == 500;
    }
    CHECK(fifo);
    CHECK_EQ(ev.back().trade.maker_remaining, 1);
    CHECK_EQ(ev.back().trade.taker_remaining, 0);
    CHECK_EQ(s.book.orderCount(), 1000u);
    CHECK_LEVELS(s.book.depth(Side::Sell), {500, 1999, 1000});
    // 先把临时 vector 存下来：CHECK_EQ 内部用 const auto& 绑定，取临时对象成员的引用会悬空
    const std::vector<OrderInfo> queue = s.book.ordersAt(Side::Sell, 500);
    REQUIRE_EQ(queue.size(), 1000u);
    CHECK_EQ(queue.front().id, 2001u);
    CHECK_EQ(queue.back().id, 3000u);
    std::string why;
    CHECK(s.book.checkInvariants(&why));

    // 2000 个价位的深簿：一张卖单从 2000 扫到 1，剩余量挂在 1
    Stepper<RefBook> d;
    for (OrderId id = 1; id <= 2000; ++id) d.run(Command::newOrder(buy(id, static_cast<Price>(id), 1)));
    CHECK_EQ(d.book.levelCount(Side::Buy), 2000u);
    CHECK_TOP(d.book, 2000, std::nullopt);
    const std::vector<Event> sweep = d.run(Command::newOrder(sell(5000, 1, 2500).account(2)));
    REQUIRE_EQ(sweep.size(), 2001u);
    bool descending = true;
    for (std::size_t i = 1; i < sweep.size(); ++i) {
        descending = descending && sweep[i].kind == Event::Kind::Trade &&
                     sweep[i].trade.price == static_cast<Price>(2001 - i);
    }
    CHECK(descending);
    CHECK_TOP(d.book, std::nullopt, 1);
    CHECK_ORDER(d.book, 5000, orderInfo(5000, 2, Side::Sell, 1, 500));
    CHECK(d.book.checkInvariants(&why));
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
