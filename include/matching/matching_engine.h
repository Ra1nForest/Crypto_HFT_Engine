//
// matching_engine.h
// 多交易对撮合引擎：按 SymbolId 把命令路由到各自独立的 LimitOrderBook，回调额外带上交易对 id。
// Multi-symbol matching engine: routes commands by SymbolId to independent LimitOrderBooks, and the
// callbacks also carry the symbol id.
//

#pragma once

#include "hft/common.h"
#include "matching/order_book.h"
#include "matching/types.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hft::matching {

namespace detail {

// 把单个订单簿的回调转发给引擎监听器，并把交易对 id 补作第一个参数
// （onAccepted / onModified 是热路径，强制内联，见 EngineRunner::Listener）
// Forwards a single order book's callbacks to the engine listener, adding the symbol id as the first
// argument (onAccepted / onModified are on the hot path and forced inline, see EngineRunner::Listener)
template <class EngineListener>
struct SymbolRelay {
    EngineListener* target;
    SymbolId symbol;

    HFT_ALWAYS_INLINE void onAccepted(const OrderRequest& req) { target->onAccepted(symbol, req); }
    void onRejected(OrderId id, RequestType request, RejectReason reason) {
        target->onRejected(symbol, id, request, reason);
    }
    void onTrade(const Trade& trade) { target->onTrade(symbol, trade); }
    void onCancelled(OrderId id, Qty cancelled_qty, CancelReason reason) {
        target->onCancelled(symbol, id, cancelled_qty, reason);
    }
    HFT_ALWAYS_INLINE void onModified(OrderId id, Price new_price, Qty new_qty) {
        target->onModified(symbol, id, new_price, new_qty);
    }
};

}  // namespace detail

// 引擎监听器概念（静态分发）：与订单簿监听器相同，但每个回调多一个 SymbolId 首参数：
// Engine listener concept (static dispatch): same as the order book listener, but every callback takes
// an extra leading SymbolId parameter:
//   void onAccepted(SymbolId, const OrderRequest&);
//   void onRejected(SymbolId, OrderId, RequestType, RejectReason);
//   void onTrade(SymbolId, const Trade&);
//   void onCancelled(SymbolId, OrderId, Qty, CancelReason);
//   void onModified(SymbolId, OrderId, Price, Qty);
//
// 设计要点：
// - SymbolId 即 addSymbol() 的返回值：按添加顺序从 0 开始连续编号（id < symbolCount() 即为已知交易对），
//   路由就是一次数组下标访问。
// - 各交易对的订单簿完全独立：订单 id 只需在本交易对内唯一，成交 id 按交易对各自从 1 开始。
// - 未知交易对的 submit/cancel/modify 发出 onRejected(symbol, id, request, UnknownSymbol)，不产生其他事件。
// - 每个交易对的订单簿与转发器放在独立的堆对象中（地址稳定），addSymbol() 之后 book() 返回的引用一直有效。
// - addSymbol() 会分配内存（初始化期操作）；submit/cancel/modify 的分配行为与 LimitOrderBook 相同
//   （活动订单数不超过该交易对的预留量时零分配）。非线程安全：同一引擎只能由一个线程使用。
// Design notes:
// - SymbolId is the return value of addSymbol(): numbered consecutively from 0 in the order symbols are
//   added (id < symbolCount() means a known symbol), so routing is a single array index.
// - The order books of different symbols are fully independent: order ids only need to be unique
//   within their symbol, and trade ids start from 1 separately for each symbol.
// - submit/cancel/modify for an unknown symbol emit onRejected(symbol, id, request, UnknownSymbol) and
//   no other event.
// - Each symbol's order book and relay live in a separate heap object (stable address), so the
//   reference that book() returns stays valid after addSymbol().
// - addSymbol() allocates memory (an initialization-time operation); submit/cancel/modify allocate
//   exactly like LimitOrderBook (zero allocations while the active order count does not exceed the
//   symbol's reserve). Not thread-safe: an engine may be used by only one thread.
template <class Listener>
class MatchingEngine {
public:
    using Relay = detail::SymbolRelay<Listener>;
    using Book = LimitOrderBook<Relay>;
    static constexpr std::size_t kDefaultReserveOrders = Book::kDefaultReserveOrders;

    explicit MatchingEngine(Listener& listener) : listener_(listener) {}

    MatchingEngine(const MatchingEngine&) = delete;
    MatchingEngine& operator=(const MatchingEngine&) = delete;
    MatchingEngine(MatchingEngine&&) = delete;
    MatchingEngine& operator=(MatchingEngine&&) = delete;

    // 添加一个交易对，返回其 id（0, 1, 2, ...）。max_orders：该交易对的活动挂单上限（0 = 不限）；
    // reserve_orders：预留容量。名称重复时抛 std::invalid_argument（不修改引擎）。
    // Adds a symbol and returns its id (0, 1, 2, ...). max_orders: the symbol's limit on active resting
    // orders (0 = unlimited); reserve_orders: the capacity to reserve. A duplicate name throws
    // std::invalid_argument (the engine is not modified).
    SymbolId addSymbol(std::string name, std::size_t max_orders = 0,
                       std::size_t reserve_orders = kDefaultReserveOrders) {
        if (names_.count(name) != 0) throw std::invalid_argument("MatchingEngine: duplicate symbol '" + name + "'");
        if (slots_.size() >= static_cast<std::size_t>(std::numeric_limits<SymbolId>::max()))
            throw std::length_error("MatchingEngine: too many symbols");
        const SymbolId id = static_cast<SymbolId>(slots_.size());
        slots_.reserve(slots_.size() + 1);  // 先保证 push_back 不抛异常
                                            // first make sure push_back cannot throw
        auto slot = std::make_unique<Slot>(listener_, id, name, max_orders, reserve_orders);
        names_.emplace(std::move(name), id);  // 抛异常时 slot 自动释放，引擎不变
                                              // if this throws, slot is freed automatically
                                              // and the engine stays unchanged
        slots_.push_back(std::move(slot));
        return id;
    }

    // ---- 路由（热路径）----
    // ---- Routing (hot path) ----
    void submit(SymbolId symbol, const OrderRequest& req) {
        if (HFT_LIKELY(symbol < slots_.size())) {
            slots_[symbol]->book.submit(req);
        } else {
            listener_.onRejected(symbol, req.id, RequestType::New, RejectReason::UnknownSymbol);
        }
    }

    void cancel(SymbolId symbol, OrderId id) {
        if (HFT_LIKELY(symbol < slots_.size())) {
            slots_[symbol]->book.cancel(id);
        } else {
            listener_.onRejected(symbol, id, RequestType::Cancel, RejectReason::UnknownSymbol);
        }
    }

    void modify(SymbolId symbol, OrderId id, Price new_price, Qty new_qty) {
        if (HFT_LIKELY(symbol < slots_.size())) {
            slots_[symbol]->book.modify(id, new_price, new_qty);
        } else {
            listener_.onRejected(symbol, id, RequestType::Modify, RejectReason::UnknownSymbol);
        }
    }

    // ---- 访问 ----
    // 交易对的订单簿；未知 id 抛 std::out_of_range
    // ---- Access ----
    // The symbol's order book; an unknown id throws std::out_of_range
    Book& book(SymbolId symbol) { return slotAt(symbol).book; }
    const Book& book(SymbolId symbol) const { return slotAt(symbol).book; }

    std::size_t symbolCount() const noexcept { return slots_.size(); }

    // 交易对名称；未知 id 抛 std::out_of_range
    // Symbol name; an unknown id throws std::out_of_range
    const std::string& symbolName(SymbolId symbol) const { return slotAt(symbol).name; }

    // 按名称查找交易对 id
    // Looks up a symbol id by name
    std::optional<SymbolId> findSymbol(const std::string& name) const {
        auto it = names_.find(name);
        if (it == names_.end()) return std::nullopt;
        return it->second;
    }

private:
    // 转发器必须先于订单簿构造（订单簿持有它的引用）
    // The relay must be constructed before the order book (the order book holds a reference to it)
    struct Slot {
        Relay relay;
        Book book;
        std::string name;

        Slot(Listener& listener, SymbolId id, const std::string& symbol_name, std::size_t max_orders,
             std::size_t reserve_orders)
            : relay{&listener, id}, book(relay, max_orders, reserve_orders), name(symbol_name) {}
    };

    Slot& slotAt(SymbolId symbol) const {
        if (symbol >= slots_.size())
            throw std::out_of_range("MatchingEngine: unknown symbol id " + std::to_string(symbol));
        return *slots_[symbol];
    }

    Listener& listener_;
    std::vector<std::unique_ptr<Slot>> slots_;
    std::unordered_map<std::string, SymbolId> names_;
};

}  // namespace hft::matching
