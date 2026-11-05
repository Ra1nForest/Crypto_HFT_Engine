//
// test_matching.cpp
// 撮合引擎测试：规范套件（参考模型、朴素模型、优化引擎）、参考模型随机自检、差分测试
// （优化引擎 vs 参考模型的多场景模糊测试）、多交易对路由、零分配验证、测试工具自身的校验。
//

// 启用 hft 头文件中的调试断言：必须在包含任何 hft 头文件之前定义
#define HFT_ENABLE_ASSERTS 1

#include "matching/matching_engine.h"
#include "matching/order_book.h"
#include "matching/types.h"
#include "matching_spec_tests.h"
#include "matching_test_util.h"
#include "naive_order_book.h"
#include "reference_order_book.h"
#include "test_framework.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace hft::matching;
using namespace hft::matching::test;

namespace {

// ---- 静态初始化期注册规范套件 ----
// "reference"：参考模型（差分测试的标准答案）
// "naive"：第二个独立实现，交叉验证规范用例本身与参考模型
// "optimized"：被测的优化引擎（matching/order_book.h）
struct RegisterSpecSuites {
    RegisterSpecSuites() {
        registerMatchingSpecTests<ReferenceOrderBook>("reference");
        registerMatchingSpecTests<NaiveOrderBook>("naive");
        registerMatchingSpecTests<hft::matching::LimitOrderBook>("optimized");
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

// 随机流必须触发过的全部规则分支（Coverage::classify 的键）
const std::vector<const char*>& requiredBranches() {
    static const std::vector<const char*> kRequired = {
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
    return kRequired;
}

// 逐个报告从未触发过的分支；有遗漏时打印完整覆盖统计
void checkAllBranchesExercised(const Coverage& cov, const char* file, int line) {
    bool all = true;
    for (const char* key : requiredBranches()) {
        if (cov.get(key) == 0) {
            all = false;
            ::hft_test::reportFailure(file, line, std::string("branch never exercised: ") + key);
        }
    }
    if (!all) std::printf("%s", cov.summary().c_str());
}

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
    checkAllBranchesExercised(cov, __FILE__, __LINE__);
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

// 包装一个真实订单簿，记下按价格查询（ordersAt / volumeAt）时传入的每个价格
template <class Inner>
struct ProbeRecordingBook {
    const Inner& inner;
    mutable std::vector<Price> probed;

    std::optional<Price> bestBid() const { return inner.bestBid(); }
    std::optional<Price> bestAsk() const { return inner.bestAsk(); }
    std::size_t orderCount() const { return inner.orderCount(); }
    std::size_t levelCount(Side s) const { return inner.levelCount(s); }
    std::vector<LevelInfo> depth(Side s, std::size_t n = SIZE_MAX) const { return inner.depth(s, n); }
    std::vector<OrderInfo> ordersAt(Side s, Price p) const {
        probed.push_back(p);
        return inner.ordersAt(s, p);
    }
    std::optional<OrderInfo> findOrder(OrderId id) const { return inner.findOrder(id); }
    Qty volumeAt(Side s, Price p) const {
        probed.push_back(p);
        return inner.volumeAt(s, p);
    }
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

TEST_CASE("tools: queriesConsistent never computes a price beyond the representable range") {
    // 卖盘最差价为 INT64_MAX、买盘最差价为 1：检查"最差档之外没有幻影档位"时不能计算 INT64_MAX + 1
    Stepper<RefBook> s;
    s.run(Command::newOrder(sell(1, INT64_MAX, 2)));
    s.run(Command::newOrder(sell(2, INT64_MAX - 1, 1)));
    s.run(Command::newOrder(buy(3, 1, 5).account(2)));
    REQUIRE_EQ(s.book.orderCount(), 3u);
    ProbeRecordingBook<RefBook> probe{s.book, {}};
    std::string why;
    CHECK(queriesConsistent(probe, &why));
    REQUIRE(!probe.probed.empty());
    // 合法的簿上所有价格都 > 0；买盘最差档之外的探测价格是 0，因此任何负数都是算越界后回绕的结果
    const Price lowest = *std::min_element(probe.probed.begin(), probe.probed.end());
    if (lowest < 0)
        ::hft_test::reportFailure(__FILE__, __LINE__,
                                  "queriesConsistent probed price " + std::to_string(lowest) + " (overflowed)");
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

// ============================================================================
// 优化引擎（matching/order_book.h、matching/matching_engine.h）专项测试：
// 与参考模型的多场景差分模糊测试、回调时刻的簿状态、零分配、白盒不变量、极端数值、多交易对路由
// ============================================================================

// ---- 计数版全局分配函数：验证热路径零分配 ----
// 本测试程序是单个翻译单元；这里替换全局 operator new/delete 只为计数，底层仍是 malloc/free
// （ASan/UBSan 照常拦截 malloc）。计数器用 relaxed 原子量，测试本身是单线程的。
namespace {

std::atomic<std::uint64_t> g_heap_allocations{0};

void* countedAlloc(std::size_t n) {
    g_heap_allocations.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n == 0 ? 1 : n)) return p;
    throw std::bad_alloc();
}

void* countedAlignedAlloc(std::size_t n, std::align_val_t align) {
    g_heap_allocations.fetch_add(1, std::memory_order_relaxed);
    const std::size_t a = static_cast<std::size_t>(align);
    const std::size_t rounded = n == 0 ? a : (n + a - 1) / a * a;  // aligned_alloc 要求大小是对齐的整数倍
    if (void* p = std::aligned_alloc(a, rounded)) return p;
    throw std::bad_alloc();
}

std::uint64_t heapAllocations() { return g_heap_allocations.load(std::memory_order_relaxed); }

}  // namespace

void* operator new(std::size_t n) { return countedAlloc(n); }
void* operator new[](std::size_t n) { return countedAlloc(n); }
void* operator new(std::size_t n, std::align_val_t a) { return countedAlignedAlloc(n, a); }
void* operator new[](std::size_t n, std::align_val_t a) { return countedAlignedAlloc(n, a); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

// ---- 白盒访问：故意破坏优化引擎的内部结构，验证 checkInvariants() 能发现每一种问题 ----
namespace hft::matching::detail {
template <class Book>
struct OrderBookTestPeer {
    static auto& sides(Book& b) { return b.sides_; }
    static auto& index(Book& b) { return b.index_; }
    static auto& orderPool(Book& b) { return b.order_pool_; }
    static auto& levelPool(Book& b) { return b.level_pool_; }
};
}  // namespace hft::matching::detail

namespace {

using OptBook = LimitOrderBook<RecordingListener>;

// 可指定预留容量的执行器（Stepper 只接受 max_orders；扩容路径需要很小的预留量）
template <class Book>
struct SizedStepper {
    RecordingListener listener;
    Book book;
    SizedStepper(std::size_t max_orders, std::size_t reserve_orders)
        : listener(), book(listener, max_orders, reserve_orders) {}
    std::vector<Event> run(const Command& cmd) {
        listener.clear();
        hft::matching::test::apply(book, cmd);
        return listener.take();
    }
};

// 场景改写器的随机源：与生成器一样只用 mt19937_64 的原始输出，任何标准库实现上都可复现
class Mutator {
public:
    explicit Mutator(std::uint64_t seed) : rng_(seed ^ 0x9e3779b97f4a7c15ULL) {}
    std::uint64_t below(std::uint64_t n) { return n == 0 ? 0 : rng_() % n; }
    std::int64_t range(std::int64_t lo, std::int64_t hi) {
        return lo + static_cast<std::int64_t>(below(static_cast<std::uint64_t>(hi - lo) + 1));
    }
    bool chance(double p) { return static_cast<double>(rng_() >> 11) * 0x1.0p-53 < p; }

private:
    std::mt19937_64 rng_;
};

using MutateFn = void (*)(Command&, Mutator&);

// 主动扫档：三成新单数量放大 3~12 倍（配合更大的穿价幅度，一单常扫多档）
void mutateAggressive(Command& c, Mutator& m) {
    if (c.type != RequestType::New) return;
    OrderRequest& r = c.order;
    if (r.qty > 0 && m.chance(0.3)) r.qty *= m.range(3, 12);
}

// STP 密集：只有账户 0 和 1（0 是普通账户），四分之三的新单启用某种 STP 模式
void mutateStpHeavy(Command& c, Mutator& m) {
    if (c.type != RequestType::New) return;
    OrderRequest& r = c.order;
    if (r.account > 0) r.account -= 1;
    if (m.chance(0.75)) r.stp = static_cast<StpMode>(1 + m.below(3));
}

// FOK/IOC 密集：大量限价单改成 FOK/IOC 或市价单，FOK 数量偏大（整单撤销更常见），并混入各种 STP
void mutateFokIoc(Command& c, Mutator& m) {
    if (c.type != RequestType::New) return;
    OrderRequest& r = c.order;
    if (r.post_only) return;
    const std::uint64_t roll = m.below(100);
    if (roll < 30) {
        r.tif = TimeInForce::FOK;
    } else if (roll < 55) {
        r.tif = TimeInForce::IOC;
    } else if (roll < 65) {
        static constexpr TimeInForce kTifs[] = {TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK};
        r.type = OrderType::Market;
        r.tif = kTifs[m.below(3)];
    }
    if (r.tif == TimeInForce::FOK && r.qty > 0 && m.chance(0.5)) r.qty *= m.range(1, 6);
    if (m.chance(0.3)) r.stp = static_cast<StpMode>(m.below(4));
}

// 边界 id 与账户：把生成器的小 id 双射到 {0, 1, 2, ...} ∪ {..., UINT64_MAX - 1, UINT64_MAX}，
// 使 0 与 UINT64_MAX（哈希表的表外专用槽位）频繁出现；账户 1 → 0、账户 4 → UINT32_MAX
OrderId edgeId(OrderId x) {
    if (x == 0) return OrderId{1} << 62;
    return x % 2 == 1 ? UINT64_MAX - x / 2 : x / 2 - 1;
}

void mutateEdgeIds(Command& c, Mutator&) {
    c.id = edgeId(c.id);
    if (c.type != RequestType::New) return;
    c.order.id = c.id;
    if (c.order.account == 1) c.order.account = 0;
    else if (c.order.account == 4) c.order.account = UINT32_MAX;
}

// 大数量：新单与改单数量乘以 2^40（档位总量仍远小于 INT64_MAX），检查不存在 32 位截断
void mutateLargeQty(Command& c, Mutator&) {
    constexpr Qty kScale = Qty{1} << 40;
    if (c.type == RequestType::New && c.order.qty > 0) c.order.qty *= kScale;
    if (c.type == RequestType::Modify && c.qty > 0) c.qty *= kScale;
}

// 差分测试场景：生成器参数 + 订单簿参数 + 可选的命令改写
struct DiffProfile {
    const char* name;
    std::uint64_t seed_base;     // 第 k 个种子 = seed_base + k
    std::size_t seeds;           // 常规运行的种子数
    std::size_t commands;        // 每个种子的命令数
    std::size_t max_orders;      // 0 = 不限
    std::size_t reserve_orders;  // 优化引擎的预留容量（1 = 从极小容量开始反复扩容）
    GeneratorConfig gen;
    MutateFn mutate;
};

std::vector<DiffProfile> diffProfiles() {
    return {
        {"balanced", 1000, 8, 200'000, 0, 1024, GeneratorConfig(), nullptr},
        {"aggressive sweeps", 2000, 8, 200'000, 0, 1024, makeConfig([](GeneratorConfig& c) {
             c.passive_ticks = 12;
             c.aggressive_ticks = 25;
             c.max_live = 400;
         }),
         mutateAggressive},
        {"stp heavy", 3000, 8, 200'000, 0, 1024, makeConfig([](GeneratorConfig& c) { c.accounts = 2; }),
         mutateStpHeavy},
        {"fok/ioc heavy", 4000, 8, 200'000, 0, 1024,
         makeConfig([](GeneratorConfig& c) { c.aggressive_ticks = 8; }), mutateFokIoc},
        {"modify heavy", 5000, 8, 200'000, 0, 1024, makeConfig([](GeneratorConfig& c) {
             c.weight_new = 35;
             c.weight_cancel = 15;
             c.weight_modify = 50;
             c.drift_rate = 0.15;
         }),
         nullptr},
        {"deep sparse book", 6000, 4, 50'000, 0, 4096, makeConfig([](GeneratorConfig& c) {
             c.passive_ticks = 1500;
             c.aggressive_ticks = 15;
             c.max_live = 3000;
             c.id_space = 0;
             c.weight_cancel = 10;
         }),
         nullptr},
        {"capacity limited (8)", 7000, 4, 200'000, 8, 1024,
         makeConfig([](GeneratorConfig& c) { c.max_live = 40; }), nullptr},
        {"capacity limited (40)", 7500, 4, 200'000, 40, 64,
         makeConfig([](GeneratorConfig& c) { c.max_live = 120; }), nullptr},
        {"tiny reservation (growth)", 8000, 6, 150'000, 0, 1, makeConfig([](GeneratorConfig& c) {
             c.max_live = 600;
             c.passive_ticks = 80;
         }),
         nullptr},
        {"edge ids and accounts", 9000, 6, 200'000, 0, 1024, GeneratorConfig(), mutateEdgeIds},
        {"large quantities", 9500, 6, 200'000, 0, 1024,
         makeConfig([](GeneratorConfig& c) { c.aggressive_ticks = 10; }), mutateLargeQty},
    };
}

struct DiffTotals {
    std::size_t runs = 0;
    std::size_t commands = 0;
    std::size_t state_checks = 0;
    std::uint64_t trades = 0;
    std::size_t peak_orders = 0;
};

std::string diffFailure(const DiffProfile& p, std::uint64_t seed, std::size_t index, const Command& cmd,
                        const std::vector<Event>& ref_events, const std::vector<Event>& opt_events,
                        const std::string& what) {
    return std::string("profile '") + p.name + "' seed " + std::to_string(seed) + " command #" +
           std::to_string(index) + ": " + cmd.toString() + "\n    reproduce: HFT_DIFF_PROFILE='" + p.name +
           "' HFT_DIFF_SEED=" + std::to_string(seed) + " ./build/test_matching optimized/differential" +
           "\n    reference events: " + str(ref_events) + "\n    optimized events: " + str(opt_events) +
           "\n    " + what;
}

// 一个种子的差分运行：同一命令流同时喂给参考模型与优化引擎。
// 每条命令之后：事件序列逐条完全相同 + 与实现无关的事件流检查；每 64 条命令及结束时：
// 两边 checkInvariants()、sameState()、优化引擎的 queriesConsistent()。返回空串表示通过。
std::string runDifferential(const DiffProfile& p, std::uint64_t seed, std::size_t commands, Coverage* cov,
                            DiffTotals& totals) {
    RandomCommandGenerator gen(seed, p.gen);
    Mutator mutator(seed);
    Stepper<RefBook> ref(p.max_orders);
    SizedStepper<OptBook> opt(p.max_orders, p.reserve_orders);
    std::string why;
    for (std::size_t i = 0; i < commands; ++i) {
        Command cmd = gen.next();
        if (p.mutate != nullptr) p.mutate(cmd, mutator);
        const PreState pre = capturePre(opt.book, cmd);
        ref.listener.clear();
        opt.listener.clear();
        hft::matching::test::apply(ref.book, cmd);
        hft::matching::test::apply(opt.book, cmd);
        const std::vector<Event>& expected = ref.listener.events;
        const std::vector<Event>& actual = opt.listener.events;
        if (!sameEvents(actual, expected, &why))
            return diffFailure(p, seed, i, cmd, expected, actual,
                               "event streams differ (expected = reference, actual = optimized):\n" + why);
        if (!checkCommandEvents(cmd, pre, actual, opt.book, p.max_orders, &why))
            return diffFailure(p, seed, i, cmd, expected, actual, "event stream check: " + why);
        if (cov != nullptr) cov->classify(cmd, pre, actual);
        totals.peak_orders = std::max(totals.peak_orders, opt.book.orderCount());
        if (i % 64 == 63 || i + 1 == commands) {
            if (!opt.book.checkInvariants(&why))
                return diffFailure(p, seed, i, cmd, expected, actual, "optimized checkInvariants: " + why);
            if (!ref.book.checkInvariants(&why))
                return diffFailure(p, seed, i, cmd, expected, actual, "reference checkInvariants: " + why);
            if (!sameState(ref.book, opt.book, &why))
                return diffFailure(p, seed, i, cmd, expected, actual,
                                   "book state differs (A = reference, B = optimized): " + why);
            if (!queriesConsistent(opt.book, &why))
                return diffFailure(p, seed, i, cmd, expected, actual, "optimized queries inconsistent: " + why);
            ++totals.state_checks;
        }
    }
    ++totals.runs;
    totals.commands += commands;
    totals.trades += opt.book.lastTradeId();
    return {};
}

bool envFlag(const char* name) {
    const char* v = std::getenv(name);
    return v != nullptr && std::strcmp(v, "1") == 0;
}

// 运行一组场景。seed_offset / seed_multiplier 用于长时运行（不同的种子区间、更多种子）。
// 环境变量 HFT_DIFF_PROFILE / HFT_DIFF_SEED 可只重放某个场景/种子（失败信息里给出了完整命令）。
void runDiffSuite(std::uint64_t seed_offset, std::size_t seed_multiplier, bool enforce_totals) {
    const char* only_profile = std::getenv("HFT_DIFF_PROFILE");
    const char* only_seed = std::getenv("HFT_DIFF_SEED");
    const bool filtered = only_profile != nullptr || only_seed != nullptr;
    Coverage cov;
    DiffTotals all;
    for (const DiffProfile& p : diffProfiles()) {
        if (only_profile != nullptr && std::string(only_profile) != p.name) continue;
        DiffTotals totals;
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t k = 0; k < p.seeds * seed_multiplier; ++k) {
            const std::uint64_t seed = p.seed_base + seed_offset + k;
            if (only_seed != nullptr && std::to_string(seed) != only_seed) continue;
            // 覆盖率统计较慢（每个事件拼一次字符串），只对每个场景的第一个种子做
            const std::string failure = runDifferential(p, seed, p.commands, k == 0 ? &cov : nullptr, totals);
            if (!failure.empty()) FAIL(failure);
        }
        const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        if (totals.runs > 0) {
            std::printf("    %-26s %3zu seeds x %7zu commands: %9llu trades, peak %4zu orders, %6zu state checks, %.1f s\n",
                        p.name, totals.runs, p.commands, static_cast<unsigned long long>(totals.trades),
                        totals.peak_orders, totals.state_checks, secs);
        }
        all.runs += totals.runs;
        all.commands += totals.commands;
    }
    std::printf("    total: %zu runs, %zu commands\n", all.runs, all.commands);
    if (filtered) return;
    if (enforce_totals) {
        CHECK_GE(all.runs, 40u);
        CHECK_GE(all.commands, 5'000'000u);
    }
    checkAllBranchesExercised(cov, __FILE__, __LINE__);
}

}  // namespace

TEST_CASE("optimized/differential: identical events and state vs the reference across generator profiles") {
    runDiffSuite(0, 1, true);
}

TEST_CASE("optimized/differential (long, set HFT_LONG_TESTS=1): ten times more seeds per profile") {
    if (!envFlag("HFT_LONG_TESTS")) {
        std::printf("    skipped (set HFT_LONG_TESTS=1 to run)\n");
        return;
    }
    runDiffSuite(100'000, 10, false);
}

TEST_CASE("optimized/differential: identical events and state vs the naive model") {
    // 朴素模型的数据结构与另外两者都不同（无序 vector + 线性扫描），用它再交叉验证一次
    const std::vector<FuzzRun> runs = {
        {"default mix", 201, 20'000, 0, GeneratorConfig()},
        {"two accounts", 202, 15'000, 0, makeConfig([](GeneratorConfig& c) { c.accounts = 2; })},
        {"capacity limited", 203, 15'000, 30, makeConfig([](GeneratorConfig& c) { c.max_live = 90; })},
        {"wide prices, modify heavy", 204, 15'000, 0, makeConfig([](GeneratorConfig& c) {
             c.passive_ticks = 50;
             c.aggressive_ticks = 10;
             c.weight_modify = 35;
         })},
    };
    for (const FuzzRun& run : runs) {
        RandomCommandGenerator gen(run.seed, run.gen);
        Stepper<NaiveBook> naive(run.max_orders);
        SizedStepper<OptBook> opt(run.max_orders, 64);
        for (std::size_t i = 0; i < run.commands; ++i) {
            const Command cmd = gen.next();
            naive.listener.clear();
            opt.listener.clear();
            hft::matching::test::apply(naive.book, cmd);
            hft::matching::test::apply(opt.book, cmd);
            std::string why;
            if (!sameEvents(opt.listener.events, naive.listener.events, &why))
                FAIL(describe(run, i, cmd, naive.listener.events) + "optimized diverged from naive:\n" + why);
            if (i % 16 == 15 || i + 1 == run.commands) {
                if (!opt.book.checkInvariants(&why)) FAIL(describe(run, i, cmd, opt.listener.events) + why);
                if (!sameState(naive.book, opt.book, &why))
                    FAIL(describe(run, i, cmd, opt.listener.events) + "state differs: " + why);
            }
        }
    }
}

// ============================================================================
// 回调时刻的簿状态：优化引擎与参考模型在每个回调中暴露的查询结果必须完全相同
// （规范未规定回调期间的可见状态；这里与参考模型对齐："事件发出时簿已包含该事件的效果"）
// ============================================================================
namespace {

struct CallbackView {
    Event event;
    std::size_t order_count = 0;
    std::optional<Price> best_bid;
    std::optional<Price> best_ask;
    std::size_t bid_levels = 0;
    std::size_t ask_levels = 0;
    TradeId last_trade_id = 0;
    std::optional<OrderInfo> subject;      // findOrder(事件的订单 id；成交事件为 maker)
    std::optional<OrderInfo> counterpart;  // 成交事件：findOrder(taker)
    std::vector<LevelInfo> bids;           // depth(Buy, 4)
    std::vector<LevelInfo> asks;           // depth(Sell, 4)
};

bool operator==(const CallbackView& a, const CallbackView& b) {
    return a.event == b.event && a.order_count == b.order_count && a.best_bid == b.best_bid &&
           a.best_ask == b.best_ask && a.bid_levels == b.bid_levels && a.ask_levels == b.ask_levels &&
           a.last_trade_id == b.last_trade_id && a.subject == b.subject && a.counterpart == b.counterpart &&
           a.bids == b.bids && a.asks == b.asks;
}

using hft::matching::test::str;  // 本命名空间新增的 str 重载不能遮蔽工具库中的那些

std::string str(const CallbackView& v) {
    return v.event.toString() + " | orders=" + std::to_string(v.order_count) + " bid=" + str(v.best_bid) +
           " ask=" + str(v.best_ask) + " levels=" + std::to_string(v.bid_levels) + "/" +
           std::to_string(v.ask_levels) + " last_trade=" + std::to_string(v.last_trade_id) +
           " subject=" + str(v.subject) + " counterpart=" + str(v.counterpart) + " bids=" + str(v.bids) +
           " asks=" + str(v.asks);
}

// 在每个回调里拍下簿的查询结果，并检查簿此刻的不变量与查询一致性
template <template <class> class BookT>
struct ViewRecorder {
    const BookT<ViewRecorder>* book = nullptr;
    std::vector<CallbackView> views;
    std::string problem;  // 第一处回调时刻的不一致（空 = 无）
    std::size_t callbacks = 0;

    void record(const Event& e, OrderId subject, std::optional<OrderId> counterpart) {
        ++callbacks;
        CallbackView v;
        v.event = e;
        v.order_count = book->orderCount();
        v.best_bid = book->bestBid();
        v.best_ask = book->bestAsk();
        v.bid_levels = book->levelCount(Side::Buy);
        v.ask_levels = book->levelCount(Side::Sell);
        v.last_trade_id = book->lastTradeId();
        v.subject = book->findOrder(subject);
        if (counterpart) v.counterpart = book->findOrder(*counterpart);
        v.bids = book->depth(Side::Buy, 4);
        v.asks = book->depth(Side::Sell, 4);
        views.push_back(std::move(v));
        std::string why;
        if (problem.empty() && !book->checkInvariants(&why)) problem = "checkInvariants during " + e.toString() + ": " + why;
        if (problem.empty() && !queriesConsistent(*book, &why)) problem = "queries during " + e.toString() + ": " + why;
    }

    void onAccepted(const OrderRequest& r) { record(accepted(r), r.id, std::nullopt); }
    void onRejected(OrderId id, RequestType t, RejectReason r) { record(rejected(id, t, r), id, std::nullopt); }
    void onTrade(const Trade& t) { record(trade(t), t.maker_id, t.taker_id); }
    void onCancelled(OrderId id, Qty q, CancelReason r) { record(cancelled(id, q, r), id, std::nullopt); }
    void onModified(OrderId id, Price p, Qty q) { record(modified(id, p, q), id, std::nullopt); }
};

}  // namespace

TEST_CASE("optimized: listeners observe exactly the reference book state during every callback") {
    struct Run {
        std::uint64_t seed;
        GeneratorConfig gen;
        MutateFn mutate;
    };
    const std::vector<Run> runs = {
        {31, makeConfig([](GeneratorConfig& c) { c.max_live = 80; }), nullptr},
        {32, makeConfig([](GeneratorConfig& c) { c.max_live = 80; c.accounts = 2; }), mutateStpHeavy},
        {33, makeConfig([](GeneratorConfig& c) { c.max_live = 80; c.weight_modify = 45; }), nullptr},
        {34, makeConfig([](GeneratorConfig& c) { c.max_live = 80; c.aggressive_ticks = 10; }), mutateFokIoc},
        {35, makeConfig([](GeneratorConfig& c) { c.max_live = 80; c.aggressive_ticks = 20; }), mutateAggressive},
    };
    std::size_t callbacks = 0;
    for (const Run& run : runs) {
        RandomCommandGenerator gen(run.seed, run.gen);
        Mutator mutator(run.seed);
        ViewRecorder<ReferenceOrderBook> ref_rec;
        ReferenceOrderBook<ViewRecorder<ReferenceOrderBook>> ref(ref_rec);
        ref_rec.book = &ref;
        ViewRecorder<LimitOrderBook> opt_rec;
        LimitOrderBook<ViewRecorder<LimitOrderBook>> opt(opt_rec, 0, 32);
        opt_rec.book = &opt;
        for (std::size_t i = 0; i < 12'000; ++i) {
            Command cmd = gen.next();
            if (run.mutate != nullptr) run.mutate(cmd, mutator);
            ref_rec.views.clear();
            opt_rec.views.clear();
            hft::matching::test::apply(ref, cmd);
            hft::matching::test::apply(opt, cmd);
            if (!opt_rec.problem.empty())
                FAIL("seed " + std::to_string(run.seed) + " command #" + std::to_string(i) + " " + cmd.toString() +
                     ": optimized " + opt_rec.problem);
            if (!ref_rec.problem.empty())
                FAIL("seed " + std::to_string(run.seed) + " command #" + std::to_string(i) + ": reference " +
                     ref_rec.problem);
            if (!(opt_rec.views == ref_rec.views))
                FAIL("seed " + std::to_string(run.seed) + " command #" + std::to_string(i) + " " + cmd.toString() +
                     "\n    reference callbacks: " + str(ref_rec.views) + "\n    optimized callbacks: " +
                     str(opt_rec.views));
        }
        callbacks += opt_rec.callbacks;
    }
    CHECK_GT(callbacks, 60'000u);
}

// ============================================================================
// 零分配：预留容量之后 submit/cancel/modify 不调用全局 operator new
// ============================================================================
namespace {

// 不分配内存的监听器
struct CountingListener {
    std::uint64_t events = 0;
    std::uint64_t trades = 0;
    void onAccepted(const OrderRequest&) { ++events; }
    void onRejected(OrderId, RequestType, RejectReason) { ++events; }
    void onTrade(const Trade&) { ++events; ++trades; }
    void onCancelled(OrderId, Qty, CancelReason) { ++events; }
    void onModified(OrderId, Price, Qty) { ++events; }
};

struct CountingEngineListener {
    std::uint64_t events = 0;
    std::uint64_t trades = 0;
    std::uint64_t unknown_symbol = 0;
    void onAccepted(SymbolId, const OrderRequest&) { ++events; }
    void onRejected(SymbolId, OrderId, RequestType, RejectReason r) {
        ++events;
        if (r == RejectReason::UnknownSymbol) ++unknown_symbol;
    }
    void onTrade(SymbolId, const Trade&) { ++events; ++trades; }
    void onCancelled(SymbolId, OrderId, Qty, CancelReason) { ++events; }
    void onModified(SymbolId, OrderId, Price, Qty) { ++events; }
};

std::vector<Command> profileCommands(const DiffProfile& p, std::uint64_t seed, std::size_t n) {
    RandomCommandGenerator gen(seed, p.gen);
    Mutator mutator(seed);
    std::vector<Command> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        Command c = gen.next();
        if (p.mutate != nullptr) p.mutate(c, mutator);
        out.push_back(c);
    }
    return out;
}

}  // namespace

TEST_CASE("optimized: no heap allocation once capacity is reserved") {
    // 计数器自检：参考模型（std::map/list/unordered_map）在同样的命令流上必然分配
    const std::vector<DiffProfile> profiles = diffProfiles();
    {
        const std::vector<Command> cmds = profileCommands(profiles[0], 1, 2'000);
        CountingListener l;
        ReferenceOrderBook<CountingListener> ref(l);
        const std::uint64_t before = heapAllocations();
        for (const Command& c : cmds) hft::matching::test::apply(ref, c);
        CHECK_GT(heapAllocations() - before, 100u);
    }
    // 每个场景（容量充足的那些）：从构造之后的第一条命令起就不分配
    for (const DiffProfile& p : profiles) {
        if (p.reserve_orders < 1024) continue;  // 小预留量场景本来就要扩容
        const std::vector<Command> cmds = profileCommands(p, p.seed_base + 77, 60'000);
        CountingListener l;
        LimitOrderBook<CountingListener> book(l, p.max_orders, 4096);
        const std::uint64_t before = heapAllocations();
        for (const Command& c : cmds) hft::matching::test::apply(book, c);
        const std::uint64_t allocations = heapAllocations() - before;
        if (allocations != 0)
            ::hft_test::reportFailure(__FILE__, __LINE__, std::string("profile '") + p.name + "' allocated " +
                                                              std::to_string(allocations) + " times");
        CHECK_GT(l.trades, 1'000u);
        std::string why;
        CHECK(book.checkInvariants(&why));
    }
    // 多交易对引擎（含未知交易对的拒单）同样不分配
    {
        CountingEngineListener l;
        MatchingEngine<CountingEngineListener> engine(l);
        engine.addSymbol("A", 0, 4096);
        engine.addSymbol("B", 0, 4096);
        const std::vector<Command> a = profileCommands(profiles[0], 91, 30'000);
        const std::vector<Command> b = profileCommands(profiles[2], 92, 30'000);
        const std::uint64_t before = heapAllocations();
        for (std::size_t i = 0; i < a.size(); ++i) {
            for (SymbolId s : {SymbolId{0}, SymbolId{1}, SymbolId{7}}) {
                const Command& c = s == 0 ? a[i] : b[i];
                switch (c.type) {
                    case RequestType::New: engine.submit(s, c.order); break;
                    case RequestType::Cancel: engine.cancel(s, c.id); break;
                    case RequestType::Modify: engine.modify(s, c.id, c.price, c.qty); break;
                }
            }
        }
        CHECK_EQ(heapAllocations() - before, 0u);
        CHECK_EQ(l.unknown_symbol, 30'000u);
        CHECK_GT(l.trades, 1'000u);
    }
    // 超出预留量时会扩容（计数器确实能看到优化引擎的分配），而且扩容后结果依然正确
    {
        CountingListener l;
        LimitOrderBook<CountingListener> small(l, 0, 4);
        const std::uint64_t before = heapAllocations();
        for (OrderId id = 1; id <= 300; ++id) small.submit(buy(id, 1000 + static_cast<Price>(id % 97), 1));
        CHECK_GT(heapAllocations() - before, 0u);
        CHECK_EQ(small.orderCount(), 300u);
        CHECK_EQ(small.levelCount(Side::Buy), 97u);
        std::string why;
        CHECK(small.checkInvariants(&why));
    }
}

// ============================================================================
// 白盒：每一种内部结构损坏都会被 checkInvariants() 发现
// ============================================================================
TEST_CASE("optimized: checkInvariants detects every kind of internal corruption") {
    using Peer = hft::matching::detail::OrderBookTestPeer<OptBook>;
    struct Corruption {
        const char* name;
        const char* expect;  // why 中必须出现的子串
        void (*apply)(OptBook&);
    };
    // 布局：买盘 100 = [1, 2]、99 = [3]；卖盘 101 = [4]、103 = [5, 6]
    // sides[0] = 买盘（升序，末尾最优），sides[1] = 卖盘（价格降序，末尾最优）
    const Corruption cases[] = {
        {"level total", "caches total", [](OptBook& b) { Peer::sides(b)[0].back().level->total += 1; }},
        {"level count", "counts", [](OptBook& b) { Peer::sides(b)[1].front().level->count += 1; }},
        {"order count too small", "FIFO longer", [](OptBook& b) { Peer::sides(b)[1].front().level->count -= 1; }},
        {"non-positive remaining", "non-positive remaining", [](OptBook& b) {
             auto* level = Peer::sides(b)[0].back().level;
             level->total -= level->head->remaining;
             level->head->remaining = 0;
         }},
        {"unsorted side", "not strictly sorted",
         [](OptBook& b) { std::swap(Peer::sides(b)[0][0], Peer::sides(b)[0][1]); }},
        {"stale sort key", "stale sort key", [](OptBook& b) { Peer::sides(b)[1].front().key += 1; }},
        {"broken prev link", "broken prev link",
         [](OptBook& b) { Peer::sides(b)[0].back().level->tail->prev = nullptr; }},
        {"head with predecessor", "FIFO head but has a predecessor", [](OptBook& b) {
             auto* level = Peer::sides(b)[0].back().level;
             level->head->prev = level->tail;
         }},
        {"cyclic FIFO", "FIFO longer", [](OptBook& b) {
             auto* level = Peer::sides(b)[1].front().level;
             level->tail->next = level->head;
         }},
        {"stale tail", "tail is not the last", [](OptBook& b) {
             auto* level = Peer::sides(b)[1].front().level;
             level->tail = level->head;
         }},
        {"order points to another level", "points to another level",
         [](OptBook& b) { Peer::sides(b)[0].front().level->head->level = Peer::sides(b)[0].back().level; }},
        {"order on the wrong side", "wrong side",
         [](OptBook& b) { Peer::sides(b)[1].back().level->head->side = Side::Buy; }},
        {"missing index entry", "missing from the index", [](OptBook& b) { Peer::index(b).erase(4); }},
        {"index points elsewhere", "points to another order",
         [](OptBook& b) { *Peer::index(b).find(1) = *Peer::index(b).find(2); }},
        {"crossed book", "book crossed", [](OptBook& b) {
             auto& best_bid = Peer::sides(b)[0].back();
             best_bid.level->price = 102;
             best_bid.key = 102;
         }},
        {"leaked order", "leak",
         [](OptBook& b) { (void)Peer::orderPool(b).create(OrderId{99}, Qty{1}, AccountId{1}, Side::Buy, false, StpMode::None); }},
        {"leaked level", "leak", [](OptBook& b) { (void)Peer::levelPool(b).create(Price{50}); }},
        {"empty level", "is empty", [](OptBook& b) {
             auto& asks = Peer::sides(b)[1];
             using Ref = std::decay_t<decltype(asks)>::value_type;
             auto* level = Peer::levelPool(b).create(Price{200});
             asks.insert(asks.begin(), Ref{-200, level});
         }},
        {"non-positive level price", "non-positive price", [](OptBook& b) {
             auto& bids = Peer::sides(b)[0];
             bids.front().level->price = 0;
             bids.front().key = 0;
         }},
        // 不属于对象池的指针（内容是真实对象的拷贝，但 checkInvariants 不能信任它）
        {"foreign order", "does not come from the order pool", [](OptBook& b) {
             auto* level = Peer::sides(b)[1].back().level;
             static auto fake = *level->head;
             level->head = &fake;
             level->tail = &fake;
         }},
        {"foreign level", "does not come from the level pool", [](OptBook& b) {
             static auto fake = *Peer::sides(b)[0].back().level;
             Peer::sides(b)[0].back().level = &fake;
         }},
    };
    for (const Corruption& c : cases) {
        RecordingListener l;
        OptBook book(l, 0, 16);
        for (const OrderRequest& r : {OrderRequest(buy(1, 100, 2)), OrderRequest(buy(2, 100, 3)),
                                      OrderRequest(buy(3, 99, 1)), OrderRequest(sell(4, 101, 4)),
                                      OrderRequest(sell(5, 103, 1)), OrderRequest(sell(6, 103, 2))}) {
            book.submit(r);
        }
        std::string why;
        REQUIRE(book.checkInvariants(&why));
        c.apply(book);
        why.clear();
        const bool ok = book.checkInvariants(&why);
        if (ok || why.find(c.expect) == std::string::npos)
            ::hft_test::reportFailure(__FILE__, __LINE__, std::string("corruption '") + c.name +
                                                              "' not reported as expected; why = '" + why + "'");
        // 被破坏的簿只析构（Order/Level 平凡析构，对象池直接释放内存），不再调用任何修改接口
    }
}

// ============================================================================
// 与参考模型逐条比对的定向场景
// ============================================================================
namespace {

// 同一命令同时执行在参考模型与优化引擎上：事件必须完全相同
struct DualBook {
    Stepper<RefBook> ref;
    SizedStepper<OptBook> opt;
    std::size_t executed = 0;

    explicit DualBook(std::size_t max_orders = 0, std::size_t reserve_orders = 1024)
        : ref(max_orders), opt(max_orders, reserve_orders) {}

    std::vector<Event> run(const Command& cmd, const char* file, int line) {
        ref.listener.clear();
        opt.listener.clear();
        hft::matching::test::apply(ref.book, cmd);
        hft::matching::test::apply(opt.book, cmd);
        ++executed;
        std::string why;
        if (!sameEvents(opt.listener.events, ref.listener.events, &why)) {
            ::hft_test::reportFailure(file, line, "command #" + std::to_string(executed - 1) + " " +
                                                      cmd.toString() + ": optimized diverged from reference:\n" + why);
            throw ::hft_test::RequireFailure();
        }
        return ref.listener.events;
    }

    void verify(const char* file, int line) const {
        std::string why;
        bool ok = opt.book.checkInvariants(&why);
        if (ok) ok = sameState(ref.book, opt.book, &why);
        if (ok) ok = queriesConsistent(opt.book, &why);
        if (!ok) {
            ::hft_test::reportFailure(file, line, "after " + std::to_string(executed) + " commands: " + why);
            throw ::hft_test::RequireFailure();
        }
    }
};

#define DUAL_RUN(d, cmd) (d).run((cmd), __FILE__, __LINE__)
#define DUAL_VERIFY(d) (d).verify(__FILE__, __LINE__)

// 确定性的 Fisher-Yates 洗牌（只用 mt19937_64 原始输出）
template <class T>
void shuffleInPlace(std::vector<T>& v, std::mt19937_64& rng) {
    for (std::size_t i = v.size(); i > 1; --i) std::swap(v[i - 1], v[rng() % i]);
}

}  // namespace

TEST_CASE("optimized: long FIFO queues and deep books match the reference") {
    DualBook d(0, 16);  // 小预留量：同时覆盖扩容路径
    for (OrderId id = 1; id <= 3000; ++id)
        DUAL_RUN(d, Command::newOrder(sell(id, 500, 2).account(static_cast<AccountId>(1 + id % 3))));
    DUAL_VERIFY(d);
    const std::vector<Event> ev = DUAL_RUN(d, Command::newOrder(marketBuy(9000, 4001).account(9)));
    REQUIRE_EQ(ev.size(), 2002u);
    CHECK_EQ(d.opt.book.orderCount(), 1000u);
    CHECK_LEVELS(d.opt.book.depth(Side::Sell), {500, 1999, 1000});
    DUAL_VERIFY(d);
    // 2000 个价位的深簿（新簿），一张卖单扫到底，剩余量挂单
    DualBook e(0, 16);
    for (OrderId id = 1; id <= 2000; ++id) DUAL_RUN(e, Command::newOrder(buy(id, static_cast<Price>(id), 1)));
    DUAL_VERIFY(e);
    CHECK_EQ(e.opt.book.levelCount(Side::Buy), 2000u);
    const std::vector<Event> sweep = DUAL_RUN(e, Command::newOrder(sell(5000, 1, 2500).account(2)));
    CHECK_EQ(sweep.size(), 2001u);
    CHECK_TOP(e.opt.book, std::nullopt, 1);
    CHECK_ORDER(e.opt.book, 5000, orderInfo(5000, 2, Side::Sell, 1, 500));
    DUAL_VERIFY(e);
}

TEST_CASE("optimized: deep books built in random price order match the reference") {
    // 价位远超线性探测范围、乱序插入：覆盖二分查找与数组中间的插入/删除
    DualBook d(0, 64);
    std::mt19937_64 rng(4242);
    std::vector<Command> adds;
    OrderId next_id = 1;
    for (Price p = 1; p <= 1500; ++p) {
        const std::size_t copies = 1 + rng() % 3;
        for (std::size_t c = 0; c < copies; ++c) {
            adds.push_back(Command::newOrder(buy(next_id++, p * 7, 1 + static_cast<Qty>(rng() % 9))
                                                 .account(static_cast<AccountId>(1 + rng() % 3))));
            adds.push_back(Command::newOrder(sell(next_id++, 20'000 + p * 5, 1 + static_cast<Qty>(rng() % 9))
                                                 .account(static_cast<AccountId>(1 + rng() % 3))));
        }
    }
    shuffleInPlace(adds, rng);
    for (std::size_t i = 0; i < adds.size(); ++i) {
        DUAL_RUN(d, adds[i]);
        if (i % 512 == 0) DUAL_VERIFY(d);
    }
    DUAL_VERIFY(d);
    CHECK_EQ(d.opt.book.levelCount(Side::Buy), 1500u);
    CHECK_EQ(d.opt.book.levelCount(Side::Sell), 1500u);
    // 乱序撤掉一半、乱序改价一部分（远距离改价：删除深处价位 + 在别处新建价位）
    std::vector<OrderId> ids;
    for (OrderId id = 1; id < next_id; ++id) ids.push_back(id);
    shuffleInPlace(ids, rng);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        const OrderId id = ids[i];
        if (i % 2 == 0) {
            DUAL_RUN(d, Command::cancel(id));
        } else if (i % 3 == 0) {
            const bool is_bid = id % 2 == 1;
            const Price p = is_bid ? 1 + static_cast<Price>(rng() % 10'000) : 20'001 + static_cast<Price>(rng() % 10'000);
            DUAL_RUN(d, Command::modify(id, p, 1 + static_cast<Qty>(rng() % 5)));
        }
        if (i % 512 == 0) DUAL_VERIFY(d);
    }
    DUAL_VERIFY(d);
    // 两次深扫：市价卖单吃掉大半买盘，限价买单穿过一段卖盘后挂单
    DUAL_RUN(d, Command::newOrder(marketSell(900'001, 2'500).account(7)));
    DUAL_RUN(d, Command::newOrder(buy(900'002, 24'000, 3'000).account(7)));
    DUAL_VERIFY(d);
    CHECK_GT(d.opt.book.lastTradeId(), 500u);
}

TEST_CASE("optimized: extreme quantities and prices match the reference without overflow") {
    constexpr Qty kMax = INT64_MAX;
    constexpr Qty kHalf = INT64_MAX / 2;
    constexpr Price kTop = INT64_MAX;
    DualBook d(0, 8);
    DUAL_RUN(d, Command::newOrder(sell(1, 100, kHalf).account(1)));
    DUAL_RUN(d, Command::newOrder(sell(2, 100, kHalf).account(2)));  // 档位总量 = INT64_MAX - 1
    CHECK_LEVELS(d.opt.book.depth(Side::Sell), {100, kMax - 1, 2});
    // FOK 需要 INT64_MAX，只有 INT64_MAX - 1：预检做减法，不溢出，整单撤销
    const OrderRequest f1 = buy(3, 100, kMax).fok().account(3);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(f1)), accepted(f1), cancelled(3, kMax, CancelReason::FokUnfillable));
    const OrderRequest f2 = buy(4, kTop, kMax).fok().account(3).stp(StpMode::CancelMaker);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(f2)), accepted(f2), cancelled(4, kMax, CancelReason::FokUnfillable));
    // CancelMaker 跳过本账户的一半，另一半正好够
    const OrderRequest f3 = buy(5, 100, kHalf).fok().account(1).stp(StpMode::CancelMaker);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(f3)), accepted(f3),
                 cancelled(1, kHalf, CancelReason::SelfTradePrevention),
                 trade(1, 2, 5, 2, 1, Side::Buy, 100, kHalf, 0, 0));
    DUAL_VERIFY(d);
    // 价格为 INT64_MAX 的卖单与限价 INT64_MAX 的买单
    DUAL_RUN(d, Command::newOrder(sell(6, kTop, 7).account(1)));
    DUAL_VERIFY(d);  // 卖盘最差价为 INT64_MAX 时的完整查询一致性检查
    DUAL_RUN(d, Command::newOrder(sell(7, kTop - 1, 5).account(1)));
    DUAL_VERIFY(d);
    const OrderRequest i1 = buy(8, kTop, kMax).ioc().account(2);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(i1)), accepted(i1),
                 trade(2, 7, 8, 1, 2, Side::Buy, kTop - 1, 5, 0, kMax - 5),
                 trade(3, 6, 8, 1, 2, Side::Buy, kTop, 7, 0, kMax - 12),
                 cancelled(8, kMax - 12, CancelReason::IocRemainder));
    // 巨量买单挂在价格 1；巨量市价卖单几乎吃光它
    DUAL_RUN(d, Command::newOrder(buy(9, 1, kMax).account(1)));
    const OrderRequest m1 = marketSell(10, kMax - 1).account(2);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(m1)), accepted(m1),
                 trade(4, 9, 10, 1, 2, Side::Sell, 1, kMax - 1, 1, 0));
    // 改到最高价、最大数量（失去优先级、重新挂单），再被一张价格为 1 的卖单成交
    CHECK_EVENTS(DUAL_RUN(d, Command::modify(9, kTop - 1, kMax)), modified(9, kTop - 1, kMax));
    CHECK_TOP(d.opt.book, kTop - 1, std::nullopt);
    const OrderRequest s1 = sell(11, 1, 5).fok().account(2);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(s1)), accepted(s1),
                 trade(5, 9, 11, 1, 2, Side::Sell, kTop - 1, 5, kMax - 5, 0));
    const OrderRequest m2 = marketBuy(12, kMax).account(2);
    CHECK_EVENTS(DUAL_RUN(d, Command::newOrder(m2)), accepted(m2), cancelled(12, kMax, CancelReason::MarketRemainder));
    CHECK_EVENTS(DUAL_RUN(d, Command::cancel(9)), cancelled(9, kMax - 5, CancelReason::UserRequested));
    DUAL_VERIFY(d);
    CHECK_EQ(d.opt.book.orderCount(), 0u);
    // 查询任意（包括极端负数）价格不会溢出
    CHECK_EQ(d.opt.book.volumeAt(Side::Sell, INT64_MIN), 0);
    CHECK(d.opt.book.ordersAt(Side::Sell, INT64_MIN).empty());
    CHECK_EQ(d.opt.book.volumeAt(Side::Buy, 0), 0);
}

TEST_CASE("optimized: out-of-range enum values are rejected as InvalidFlags") {
    // 规范之外的输入（只能经强制类型转换构造）：优化引擎按 InvalidFlags 拒单，不越界、不崩溃
    SizedStepper<OptBook> s(0, 16);
    s.book.submit(sell(1, 101, 5));
    s.book.submit(buy(2, 99, 5));
    s.listener.clear();
    const StateSnapshot before = snapshotOf(s.book);
    auto with = [](OrderRequest r, void (*f)(OrderRequest&)) {
        f(r);
        return r;
    };
    const OrderRequest base = buy(10, 101, 1);
    const OrderRequest bad_side = with(base, [](OrderRequest& r) { r.side = static_cast<Side>(2); });
    const OrderRequest bad_type = with(base, [](OrderRequest& r) { r.type = static_cast<OrderType>(7); });
    const OrderRequest bad_type_zero_px = with(base, [](OrderRequest& r) { r.type = static_cast<OrderType>(7); r.price = 0; });
    const OrderRequest bad_tif = with(base, [](OrderRequest& r) { r.tif = static_cast<TimeInForce>(3); });
    const OrderRequest bad_stp = with(base, [](OrderRequest& r) { r.stp = static_cast<StpMode>(4); });
    const OrderRequest bad_side_zero_qty = with(bad_side, [](OrderRequest& r) { r.qty = 0; });
    const OrderRequest bad_stp_zero_px = with(bad_stp, [](OrderRequest& r) { r.price = 0; });
    CHECK_EVENTS(s.run(Command::newOrder(bad_side)), rejectedNew(10, RejectReason::InvalidFlags));
    CHECK_EVENTS(s.run(Command::newOrder(bad_type)), rejectedNew(10, RejectReason::InvalidFlags));
    CHECK_EVENTS(s.run(Command::newOrder(bad_type_zero_px)), rejectedNew(10, RejectReason::InvalidFlags));
    CHECK_EVENTS(s.run(Command::newOrder(bad_tif)), rejectedNew(10, RejectReason::InvalidFlags));
    CHECK_EVENTS(s.run(Command::newOrder(bad_stp)), rejectedNew(10, RejectReason::InvalidFlags));
    // 规范中排在前面的规则仍然优先
    CHECK_EVENTS(s.run(Command::newOrder(bad_side_zero_qty)), rejectedNew(10, RejectReason::InvalidQuantity));
    CHECK_EVENTS(s.run(Command::newOrder(bad_stp_zero_px)), rejectedNew(10, RejectReason::InvalidPrice));
    CHECK_UNCHANGED(before, s.book);
    std::string why;
    CHECK(s.book.checkInvariants(&why));
}

TEST_CASE("optimized: zero reservation and growth from empty are fine") {
    DualBook d(0, 0);
    RandomCommandGenerator gen(555);
    for (std::size_t i = 0; i < 20'000; ++i) {
        DUAL_RUN(d, gen.next());
        if (i % 256 == 255) DUAL_VERIFY(d);
    }
    DUAL_VERIFY(d);
}

// ============================================================================
// 多交易对撮合引擎（matching/matching_engine.h）
// ============================================================================
namespace {

struct SymbolEvent {
    SymbolId symbol;
    Event event;
};

bool operator==(const SymbolEvent& a, const SymbolEvent& b) { return a.symbol == b.symbol && a.event == b.event; }

using hft::matching::test::str;

std::string str(const SymbolEvent& e) { return "[sym " + std::to_string(e.symbol) + "] " + e.event.toString(); }

struct EngineRecorder {
    std::vector<SymbolEvent> events;
    void onAccepted(SymbolId s, const OrderRequest& r) { events.push_back({s, accepted(r)}); }
    void onRejected(SymbolId s, OrderId id, RequestType t, RejectReason r) { events.push_back({s, rejected(id, t, r)}); }
    void onTrade(SymbolId s, const Trade& t) { events.push_back({s, trade(t)}); }
    void onCancelled(SymbolId s, OrderId id, Qty q, CancelReason r) { events.push_back({s, cancelled(id, q, r)}); }
    void onModified(SymbolId s, OrderId id, Price p, Qty q) { events.push_back({s, modified(id, p, q)}); }
    std::vector<SymbolEvent> take() { return std::exchange(events, {}); }
};

using Engine = MatchingEngine<EngineRecorder>;

void applyTo(Engine& engine, SymbolId symbol, const Command& c) {
    switch (c.type) {
        case RequestType::New: engine.submit(symbol, c.order); return;
        case RequestType::Cancel: engine.cancel(symbol, c.id); return;
        case RequestType::Modify: engine.modify(symbol, c.id, c.price, c.qty); return;
    }
}

std::vector<SymbolEvent> tagged(SymbolId symbol, const std::vector<Event>& events) {
    std::vector<SymbolEvent> out;
    for (const Event& e : events) out.push_back({symbol, e});
    return out;
}

bool checkSymbolEvents(const std::vector<SymbolEvent>& actual, const std::vector<SymbolEvent>& expected,
                       const char* file, int line) {
    if (actual == expected) return true;
    ::hft_test::reportFailure(file, line, "engine events\n      expected: " + str(expected) +
                                              "\n      actual:   " + str(actual));
    return false;
}

#define CHECK_SYMBOL_EVENTS(actual, ...) \
    checkSymbolEvents((actual), std::vector<SymbolEvent>{__VA_ARGS__}, __FILE__, __LINE__)

}  // namespace

TEST_CASE("engine: addSymbol assigns sequential ids and exposes names and books") {
    EngineRecorder rec;
    Engine engine(rec);
    CHECK_EQ(engine.symbolCount(), 0u);
    CHECK(!engine.hasSymbol(0));
    CHECK(engine.findBook(0) == nullptr);
    CHECK_THROWS(engine.book(0));
    CHECK_THROWS(engine.symbolName(0));
    const SymbolId btc = engine.addSymbol("BTC-USDT");
    const SymbolId eth = engine.addSymbol("ETH-USDT", 10);
    const SymbolId sol = engine.addSymbol("SOL-USDT", 0, 1);
    CHECK_EQ(btc, 0u);
    CHECK_EQ(eth, 1u);
    CHECK_EQ(sol, 2u);
    CHECK_EQ(engine.symbolCount(), 3u);
    CHECK_EQ(engine.symbolName(eth), std::string("ETH-USDT"));
    CHECK(engine.findSymbol("SOL-USDT") == std::optional<SymbolId>(sol));
    CHECK(!engine.findSymbol("DOGE-USDT").has_value());
    CHECK(engine.hasSymbol(sol));
    CHECK(!engine.hasSymbol(3));
    CHECK(engine.findBook(eth) == &engine.book(eth));
    CHECK_EQ(engine.book(eth).maxOrders(), 10u);
    CHECK_EQ(engine.book(btc).maxOrders(), 0u);
    // 名称重复：抛异常且引擎不变
    CHECK_THROWS(engine.addSymbol("ETH-USDT"));
    CHECK_EQ(engine.symbolCount(), 3u);
    CHECK(engine.findSymbol("ETH-USDT") == std::optional<SymbolId>(eth));
    const Engine& view = engine;
    CHECK(view.findBook(btc) == &view.book(btc));
    CHECK(view.findBook(99) == nullptr);
    CHECK(rec.events.empty());
}

TEST_CASE("engine: commands are routed to each symbol's own independent book") {
    EngineRecorder rec;
    Engine engine(rec);
    const SymbolId a = engine.addSymbol("A");
    const SymbolId b = engine.addSymbol("B", 1);
    // 同一个订单 id 可以同时活在两个交易对里
    const OrderRequest a1 = sell(1, 101, 5).account(1);
    const OrderRequest b1 = sell(1, 201, 3).account(1);
    engine.submit(a, a1);
    engine.submit(b, b1);
    CHECK_SYMBOL_EVENTS(rec.take(), {a, accepted(a1)}, {b, accepted(b1)});
    CHECK_ORDER(engine.book(a), 1, infoOf(a1));
    CHECK_ORDER(engine.book(b), 1, infoOf(b1));
    // 成交 id 按交易对各自从 1 开始（B 已满 1 单：GTC 会被容量规则拒绝，所以用 IOC 吃单）
    const OrderRequest bt = buy(2, 201, 1).ioc().account(2);
    engine.submit(b, bt);
    CHECK_SYMBOL_EVENTS(rec.take(), {b, accepted(bt)}, {b, trade(1, 1, 2, 1, 2, Side::Buy, 201, 1, 2, 0)});
    const OrderRequest at = buy(3, 101, 2).account(2);
    engine.submit(a, at);
    CHECK_SYMBOL_EVENTS(rec.take(), {a, accepted(at)}, {a, trade(1, 1, 3, 1, 2, Side::Buy, 101, 2, 3, 0)});
    CHECK_EQ(engine.book(a).lastTradeId(), 1u);
    CHECK_EQ(engine.book(b).lastTradeId(), 1u);
    // 每个交易对自己的容量上限
    const OrderRequest b4 = buy(4, 100, 1).account(3);  // 不穿价（A 的卖一为 101）
    engine.submit(b, b4);
    CHECK_SYMBOL_EVENTS(rec.take(), {b, rejectedNew(4, RejectReason::CapacityExceeded)});
    engine.submit(a, b4);
    CHECK_SYMBOL_EVENTS(rec.take(), {a, accepted(b4)});
    // 撤单与改单只作用于指定交易对
    engine.modify(a, 1, 102, 4);
    CHECK_SYMBOL_EVENTS(rec.take(), {a, modified(1, 102, 4)});
    engine.cancel(b, 1);
    CHECK_SYMBOL_EVENTS(rec.take(), {b, cancelled(1, 2, CancelReason::UserRequested)});
    CHECK_ORDER(engine.book(a), 1, orderInfo(1, 1, Side::Sell, 102, 4));
    CHECK_NO_ORDER(engine.book(b), 1);
    engine.cancel(b, 1);
    CHECK_SYMBOL_EVENTS(rec.take(), {b, rejectedCancel(1)});
    std::string why;
    CHECK(engine.book(a).checkInvariants(&why));
    CHECK(engine.book(b).checkInvariants(&why));
}

TEST_CASE("engine: unknown symbols are rejected with UnknownSymbol and nothing else") {
    EngineRecorder rec;
    Engine engine(rec);
    const SymbolId a = engine.addSymbol("A");
    engine.submit(a, sell(1, 101, 5));
    rec.take();
    const StateSnapshot before = snapshotOf(engine.book(a));
    for (const SymbolId bad : {SymbolId{1}, SymbolId{42}, std::numeric_limits<SymbolId>::max()}) {
        engine.submit(bad, buy(7, 101, 5));
        CHECK_SYMBOL_EVENTS(rec.take(), {bad, rejectedNew(7, RejectReason::UnknownSymbol)});
        engine.submit(bad, marketBuy(8, 5));
        CHECK_SYMBOL_EVENTS(rec.take(), {bad, rejectedNew(8, RejectReason::UnknownSymbol)});
        engine.cancel(bad, 1);
        CHECK_SYMBOL_EVENTS(rec.take(), {bad, rejectedCancel(1, RejectReason::UnknownSymbol)});
        engine.modify(bad, 1, 100, 1);
        CHECK_SYMBOL_EVENTS(rec.take(), {bad, rejectedModify(1, RejectReason::UnknownSymbol)});
        // 未知交易对的检查先于一切订单校验
        engine.submit(bad, buy(9, 0, 0));
        CHECK_SYMBOL_EVENTS(rec.take(), {bad, rejectedNew(9, RejectReason::UnknownSymbol)});
    }
    CHECK_UNCHANGED(before, engine.book(a));
}

TEST_CASE("engine/differential: a multi-symbol engine matches independent reference books") {
    EngineRecorder rec;
    Engine engine(rec);
    const std::vector<DiffProfile> profiles = diffProfiles();
    struct Lane {
        const DiffProfile* profile;
        RandomCommandGenerator gen;
        Mutator mutator;
        Stepper<RefBook> ref;
        Lane(const DiffProfile* p, std::uint64_t seed)
            : profile(p), gen(seed, p->gen), mutator(seed), ref(p->max_orders) {}
    };
    std::vector<std::unique_ptr<Lane>> lanes;
    const std::size_t picks[] = {0, 2, 6, 8};  // balanced、STP 密集、容量受限、极小预留量
    for (std::size_t k = 0; k < 4; ++k) {
        const DiffProfile& p = profiles[picks[k]];
        const SymbolId id = engine.addSymbol(p.name, p.max_orders, p.reserve_orders);
        REQUIRE_EQ(id, static_cast<SymbolId>(k));
        lanes.push_back(std::make_unique<Lane>(&p, 600 + k));
    }
    RandomCommandGenerator stray(699);  // 发往未知交易对的命令
    Mutator pick(700);
    std::size_t unknown = 0;
    for (std::size_t i = 0; i < 160'000; ++i) {
        std::vector<SymbolEvent> expected;
        SymbolId symbol;
        Command cmd;
        if (pick.below(100) < 3) {
            symbol = static_cast<SymbolId>(4 + pick.below(3));
            cmd = stray.next();
            const OrderId id = cmd.type == RequestType::New ? cmd.order.id : cmd.id;
            expected.push_back({symbol, rejected(id, cmd.type, RejectReason::UnknownSymbol)});
            ++unknown;
        } else {
            symbol = static_cast<SymbolId>(pick.below(lanes.size()));
            Lane& lane = *lanes[symbol];
            cmd = lane.gen.next();
            if (lane.profile->mutate != nullptr) lane.profile->mutate(cmd, lane.mutator);
            expected = tagged(symbol, lane.ref.run(cmd));
        }
        rec.events.clear();
        applyTo(engine, symbol, cmd);
        if (!(rec.events == expected))
            FAIL("command #" + std::to_string(i) + " to symbol " + std::to_string(symbol) + " " + cmd.toString() +
                 "\n    expected: " + str(expected) + "\n    actual:   " + str(rec.events));
        if (i % 1024 == 1023) {
            for (SymbolId s = 0; s < lanes.size(); ++s) {
                std::string why;
                if (!engine.book(s).checkInvariants(&why)) FAIL("symbol " + std::to_string(s) + ": " + why);
                if (!sameState(lanes[s]->ref.book, engine.book(s), &why))
                    FAIL("symbol " + std::to_string(s) + " state differs: " + why);
            }
        }
    }
    CHECK_GT(unknown, 3'000u);
    for (SymbolId s = 0; s < lanes.size(); ++s) {
        std::string why;
        CHECK(sameState(lanes[s]->ref.book, engine.book(s), &why));
        CHECK_GT(engine.book(s).lastTradeId(), 1'000u);
    }
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
