//
// order_book.h
// 优化版限价订单簿（撮合核心）：池化订单 + 侵入式 FIFO 链表、池化价位、每侧最优档在末尾的有序扁平数组、
// 整数 id 开放寻址索引；预留容量之后热路径零堆分配。事件流与参考模型逐条一致（接口与语义见 docs/MATCHING_ENGINE.md）。
// Optimized limit order book (the matching core): pooled orders + intrusive FIFO lists, pooled price levels,
// per side a sorted flat array with the best level at the end, and an open-addressing index on integer ids;
// zero heap allocations on the hot path once capacity is reserved. The event stream matches the reference
// model event for event (interface and semantics in docs/MATCHING_ENGINE.md).
//

#pragma once

#include "hft/common.h"
#include "hft/flat_hash_map.h"
#include "hft/object_pool.h"
#include "matching/types.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace hft::matching {

namespace detail {
// 测试专用的友元入口（tests/ 中定义），用于故意破坏内部结构，验证 checkInvariants() 能发现问题
// Test-only friend entry point (defined in tests/) that deliberately corrupts internal structures to verify
// that checkInvariants() detects the problem
template <class Book>
struct OrderBookTestPeer;
}  // namespace detail

// ============================================================================
// 数据结构（推荐方案见 docs/MATCHING_ENGINE.md「Data structures」；在其基础上做了两处改进，理由见各条）：
// Data structures (see docs/MATCHING_ENGINE.md "Data structures" for the recommended design; this adds two
// improvements to it, with the reasons in the respective items):
//
// - 订单 Order：来自 hft::ObjectPool<Order>（地址稳定、LIFO 复用），48 字节；同一价位的订单用
//   prev/next 侵入式双向链表串成 FIFO（头部时间优先级最高）。订单不存价格，价格在所属 Level 中。
//   订单对象只在挂单时创建，因此对象池的存活对象数就是挂单数（orderCount()）。
// - 价位 Level：来自 hft::ObjectPool<Level>（地址稳定），缓存该档剩余总量与订单数，查询 O(1)。
// - 每侧一个按价格有序的扁平数组，最优档在末尾（买盘价格升序、卖盘价格降序）：吃掉盘口 = pop_back，
//   在盘口附近新建价位 = 靠近末尾的短 memmove。
//   改进 1：数组元素是 {key, Level*} 而不是裸 Level*。二分/线性查找只读连续内存，不必逐个解引用
//   Level 指针（深簿时每次探测都可能是一次缓存缺失）。
//   改进 2：key 把两侧统一成"升序且越大越优"：买盘 key = price，卖盘 key = -price（价格恒 > 0，
//   取负不会溢出）。于是两侧共用同一套查找代码，"taker 限价能否与对手最优价成交"也统一成一次比较：
//   对手最优 key >= taker 限价在对手 key 空间中的值（市价单取 INT64_MIN，总是成立）。
//   查找从末尾（盘口）先线性探测 kLinearProbe 个元素，再对剩余前缀二分：绝大多数价位操作发生在盘口附近。
// - 订单索引：hft::FlatHashMap<OrderId, Order*>（线性探测 + 反向移位删除）。0 和 UINT64_MAX 都是普通 id
//   （后者由哈希表存放在表外专用槽位）。每次增删只探测一次：撤单用 extract；不穿价的新单用 insert 查重，
//   同时插入一个值为空指针的占位条目占好槽位，onAccepted 之后经槽位指针写入订单；改价/加量的改单复用
//   find 得到的槽位（不穿价时原地占位、挂单后写回，穿价时 eraseAt）。占位条目对全部查询不可见。
//   槽位指针在任何插入/删除之后都可能失效，因此只在两次索引修改之间持有它。
// - 只用整数运算；不抛异常的正常控制流；回调里不做任何分配。
// - Order: from hft::ObjectPool<Order> (stable addresses, LIFO reuse), 48 bytes; the orders at one price
//   are chained into a FIFO by a prev/next intrusive doubly-linked list (the head has the highest time
//   priority). An order does not store its price; the price lives in its Level.
//   Order objects are created only when an order rests, so the pool's live object count is the number of
//   resting orders (orderCount()).
// - Level: from hft::ObjectPool<Level> (stable addresses); caches the level's total remaining quantity and
//   order count, so queries are O(1).
// - Per side, one flat array sorted by price with the best level at the end (bids by ascending price, asks
//   by descending price): taking out the top of book = pop_back, creating a level near the touch = a short
//   memmove near the end.
//   Improvement 1: the array elements are {key, Level*} instead of a bare Level*. Binary/linear search reads
//   only contiguous memory and does not dereference Level pointers one by one (in a deep book every probe
//   could be a cache miss).
//   Improvement 2: the key unifies both sides as "ascending, larger is better": bid key = price,
//   ask key = -price (prices are always > 0, so negation cannot overflow). Both sides thus share the same
//   lookup code, and "can the taker's limit trade against the opposite best price" also becomes a single
//   comparison: opposite best key >= the taker's limit in the opposite key space (INT64_MIN for market
//   orders, always true).
//   A lookup first probes kLinearProbe elements linearly from the end (top of book), then binary-searches
//   the remaining prefix: the vast majority of level operations happen near the touch.
// - Order index: hft::FlatHashMap<OrderId, Order*> (linear probing + backward-shift deletion). 0 and
//   UINT64_MAX are both ordinary ids (the hash map keeps the latter in a dedicated slot outside the table).
//   Each insert or erase probes only once: a cancel uses extract; a new order that does not cross checks for
//   a duplicate with insert, which at the same time claims the slot with a placeholder entry whose value is
//   a null pointer, and after onAccepted the order is written through the slot pointer; a modify that
//   reprices or increases the quantity reuses the slot found by find (held in place as a placeholder and
//   written back after resting when it does not cross, eraseAt when it crosses). Placeholder entries are
//   invisible to all queries.
//   A slot pointer may be invalidated by any insert/erase, so it is only held between two index changes.
// - Integer arithmetic only; no exceptions in normal control flow; no allocation of any kind in callbacks.
//
// 容量与分配：构造时为 reserve_orders 个订单预留全部结构（订单池、价位池、两侧价位数组、索引）。
// 活动订单数不超过预留量时，submit/cancel/modify 不做任何堆分配（基准测试用计数版 operator new 验证）。
// 超过预留量时按倍增扩容：会挂单的新单在产生第一个事件或修改任何状态之前就先完成扩容
// （ensureCapacityForOneMore），因此内存分配失败（std::bad_alloc）时簿与事件流都保持不变（强异常保证）。
// 改单不增加订单数，现有容量总够用，从不扩容。Order/Level 都是平凡析构类型，析构时对象池直接释放内存。
// Capacity and allocation: the constructor reserves every structure (order pool, level pool, both sides'
// level arrays, index) for reserve_orders orders.
// While the live order count does not exceed the reservation, submit/cancel/modify do no heap allocation at
// all (the benchmark verifies this with a counting operator new).
// Beyond the reservation the structures grow by doubling: a new order that will rest finishes growing before
// it produces its first event or modifies any state (ensureCapacityForOneMore), so a failed allocation
// (std::bad_alloc) leaves both the book and the event stream unchanged (strong exception guarantee).
// A modify does not increase the order count, so the existing capacity always suffices and it never grows.
// Order and Level are both trivially destructible, so on destruction the object pools just free the memory.
//
// 回调时机（与参考模型一致，监听器可以在回调中调用 const 查询）：每个事件发出时，簿内状态已经包含
// 该事件的全部效果——成交回报发出时完全成交的 maker 已被移除、空价位已删除；STP 撤 maker 时 maker
// 已移除；改价/加量的 onModified 发出时该订单已离开订单簿（之后作为 taker 撮合，剩余量最后静默挂单）；
// 原地改单的 onModified 发出时剩余量与档位总量已更新；新订单在 onAccepted 时尚未进入订单簿。
// 监听器不得在回调中重入调用本簿的 submit/cancel/modify；回调抛出的异常会原样传播，簿保持结构一致
// （checkInvariants() 成立，占位条目也会撤销），但正在处理的那条命令就此中止（例如主动单不再继续撮合、也不挂单）。
// Callback timing (same as the reference model; a listener may call const queries from a callback): when an
// event is emitted, the book state already includes all of that event's effects — when a trade is reported,
// a fully filled maker has already been removed and an empty level deleted; when STP cancels a maker, the
// maker has already been removed; when the onModified of a reprice/quantity increase is emitted, the order
// has already left the order book (it then matches as a taker, and its remainder finally rests silently);
// when the onModified of an in-place modify is emitted, the remaining quantity and the level total are
// already updated; a new order has not yet entered the order book at onAccepted.
// A listener must not re-enter this book's submit/cancel/modify from a callback; an exception thrown by a
// callback propagates unchanged and the book stays structurally consistent (checkInvariants() holds, and the
// placeholder entry is withdrawn too), but the command being processed is aborted there (e.g. an aggressive
// order neither continues matching nor rests).
//
// 档位总量上限（契约的一部分，三个实现完全一致，见 matching/types.h 的 kMaxLevelQty）：会让某档总量超过
// INT64_MAX 的限价 GTC 新单与改价/加量改单，在产生任何其他事件之前以 InvalidQuantity 拒绝。缓存的档位
// 总量因此始终精确、不会溢出：depth()/volumeAt() 直接返回它；不启用 STP 的 FOK 预检也直接拿它与"仍需
// 数量"比较——参考模型逐单累计，结论不依赖任何缓存，这里的 FOK 判定则依赖这条上限。
// Level total bound (part of the contract, identical in all three implementations; see kMaxLevelQty in
// matching/types.h): a new limit GTC order or a reprice/quantity-increase modify that would take a level's
// total past INT64_MAX is rejected with InvalidQuantity before any other event is produced. The cached level
// totals are therefore always exact and never overflow: depth()/volumeAt() return them directly; the FOK
// pre-check without STP also compares them directly with the "still needed" quantity — the reference model
// sums order by order, so its conclusion depends on no cache, whereas the FOK decision here relies on this
// bound.
//
// 规范未定义的输入：side/type/tif/stp 超出枚举取值范围（只能通过强制类型转换构造）时按 InvalidFlags 拒单
// （作为第 3 条规则"标志非法"的一部分），保证引擎对任意字节输入都有确定行为、不会越界访问。
// 合法输入的事件流与规范完全一致。
// Input the spec does not define: a side/type/tif/stp outside its enum's range (constructible only through
// a cast) is rejected with InvalidFlags (as part of rule 3, "invalid flags"), so the engine has defined
// behavior for arbitrary byte input and never accesses out of bounds.
// For valid input the event stream matches the spec exactly.
//
// 非线程安全：同一订单簿只能由一个线程使用（多线程场景由 engine_runner 的单引擎线程串行化）。
// Not thread-safe: an order book may only be used by one thread (multi-threaded use is serialized by the
// single engine thread of engine_runner).
// ============================================================================
template <class Listener>
class LimitOrderBook {
    template <class Book>
    friend struct detail::OrderBookTestPeer;

    struct Level;

    // 挂单（48 字节）。prev/next 为同一价位 FIFO 中的前驱/后继，level 为所属价位。
    // Resting order (48 bytes). prev/next are the predecessor/successor in the same price level's FIFO, and
    // level is the level it belongs to.
    struct Order {
        Order* prev;
        Order* next;
        Level* level;
        OrderId id;
        Qty remaining;
        AccountId account;
        Side side;
        bool post_only;
        StpMode stp;

        Order(OrderId id_, Qty remaining_, AccountId account_, Side side_, bool post_only_,
              StpMode stp_) noexcept
            : prev(nullptr), next(nullptr), level(nullptr), id(id_), remaining(remaining_),
              account(account_), side(side_), post_only(post_only_), stp(stp_) {}
    };

    // 价位：缓存剩余总量（不超过 kMaxLevelQty）与订单数（count 用 64 位，不可能回绕；对外的
    // LevelInfo::orders 为 32 位）
    // Price level: caches the total remaining quantity (at most kMaxLevelQty) and the order count (count is
    // 64-bit, so it cannot wrap; the public LevelInfo::orders is 32-bit)
    struct Level {
        Price price;
        Qty total;
        Order* head;
        Order* tail;
        std::uint64_t count;

        explicit Level(Price price_) noexcept
            : price(price_), total(0), head(nullptr), tail(nullptr), count(0) {}
    };

    static_assert(sizeof(void*) != 8 || sizeof(Order) == 48, "Order is expected to stay 48 bytes on 64-bit targets");

    // 有序价位数组的元素：key 升序，越大越优（见文件头"改进 2"）
    // Element of the sorted level array: key ascending, larger is better (see "Improvement 2" in the file header)
    struct LevelRef {
        Price key;
        Level* level;
    };

    using SideLevels = std::vector<LevelRef>;

    // 撮合中的主动方（新订单，或改价/加量后重新撮合的订单）
    // Aggressive side in matching (a new order, or an order matching again after a reprice/quantity increase)
    struct Taker {
        OrderId id;
        Qty remaining;
        Price limit_key;  // 限价在对手方 key 空间中的值；市价单为 kMarketKey
                          // the limit price in the opposite side's key space; kMarketKey for market orders
        AccountId account;
        Side side;
        StpMode stp;
    };

    static constexpr Price kMarketKey = std::numeric_limits<Price>::min();
    static constexpr std::size_t kLinearProbe = 8;   // 查找价位时先从盘口线性探测的元素数
                                                     // level lookup: elements probed linearly from the touch first
    static constexpr std::size_t kMinGrowth = 64;    // 超出预留容量后的最小扩容量
                                                     // minimum growth once past the reserved capacity

public:
    static constexpr std::size_t kDefaultReserveOrders = 1024;

    // max_orders：活动挂单上限（0 = 不限）；reserve_orders：预留容量（活动订单不超过它时热路径零分配）
    // max_orders: cap on live resting orders (0 = unlimited); reserve_orders: reserved capacity (zero
    // allocations on the hot path while the live orders do not exceed it)
    explicit LimitOrderBook(Listener& listener, std::size_t max_orders = 0,
                            std::size_t reserve_orders = kDefaultReserveOrders)
        : listener_(listener), max_orders_(max_orders) {
        reserve(reserve_orders);
    }

    // 持有监听器引用与指向池内对象的指针：禁止拷贝与移动
    // Holds a reference to the listener and pointers to pooled objects: copy and move are disabled
    LimitOrderBook(const LimitOrderBook&) = delete;
    LimitOrderBook& operator=(const LimitOrderBook&) = delete;
    LimitOrderBook(LimitOrderBook&&) = delete;
    LimitOrderBook& operator=(LimitOrderBook&&) = delete;

    // ---------------------------------------------------------------- 新订单（docs/MATCHING_ENGINE.md「submit」）
    // ---------------------------------------------------------------- New order (docs/MATCHING_ENGINE.md "submit")
    void submit(const OrderRequest& req) {
        // 校验：按规范顺序，第一条失败的规则生效，只发出这一个拒单事件
        // Validation: in spec order, the first failing rule wins and only that one reject event is emitted
        if (req.qty <= 0) return reject(req.id, RequestType::New, RejectReason::InvalidQuantity);
        if (req.type == OrderType::Limit && req.price <= 0)
            return reject(req.id, RequestType::New, RejectReason::InvalidPrice);
        const bool rests = req.type == OrderType::Limit && req.tif == TimeInForce::GTC;
        if (HFT_UNLIKELY(!validEnums(req)) || (req.post_only && !rests))
            return reject(req.id, RequestType::New, RejectReason::InvalidFlags);
        // 回调之前先把需要的字段取成局部变量：req 是调用方的引用，不依赖它在回调期间保持不变
        // Copy the needed fields into locals before any callback: req is a reference owned by the caller, so do not
        // rely on it staying unchanged during callbacks
        const bool market = req.type == OrderType::Market;
        Taker taker{req.id, req.qty, market ? kMarketKey : keyOf(opposite(req.side), req.price),
                    req.account, req.side, req.stp};
        if (rests) return submitGtc(req, taker);

        // IOC / FOK / 市价单：从不挂单，剩余量撤销
        // IOC / FOK / market orders: never rest; the remainder is cancelled
        if (index_.contains(req.id)) return reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
        const bool fok = req.tif == TimeInForce::FOK;
        listener_.onAccepted(req);
        // FOK：按撮合顺序预检能否全部成交；不能则整单撤销，簿不受任何影响
        // FOK: pre-check in matching order whether it can be filled in full; if not, cancel the whole order,
        // leaving the book completely untouched
        if (fok && !fokFillable(taker)) {
            listener_.onCancelled(taker.id, taker.remaining, CancelReason::FokUnfillable);
            return;
        }
        if (crossesTop(taker) && !match(taker)) return;  // 被自成交保护终止
                                                         // terminated by self-trade prevention
        if (taker.remaining == 0) return;                // 全部成交
                                                         // fully filled
        HFT_ASSERT(!fok && "FOK order has a remainder after a successful pre-check");
        listener_.onCancelled(taker.id, taker.remaining,
                              market ? CancelReason::MarketRemainder : CancelReason::IocRemainder);
    }

    // ---------------------------------------------------------------- 撤单
    // ---------------------------------------------------------------- Cancel
    void cancel(OrderId id) {
        const std::optional<Order*> found = index_.extract(id);  // 查找并删除索引条目：一次探测
                                                                 // find and erase the index entry: one probe
        if (!found) return reject(id, RequestType::Cancel, RejectReason::UnknownOrder);
        const Qty remaining = (*found)->remaining;
        unlink(*found);
        listener_.onCancelled(id, remaining, CancelReason::UserRequested);
    }

    // ---------------------------------------------------------------- 改单（new_qty 是新的剩余数量）
    // ---------------------------------------------------------------- Modify (new_qty is the new remaining quantity)
    void modify(OrderId id, Price new_price, Qty new_qty) {
        Order** const slot = index_.find(id);
        if (slot == nullptr) return reject(id, RequestType::Modify, RejectReason::UnknownOrder);
        Order* const order = *slot;
        if (new_qty <= 0) return reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
        if (new_price <= 0) return reject(id, RequestType::Modify, RejectReason::InvalidPrice);
        Level* const level = order->level;
        const bool same_price = new_price == level->price;
        // 价格不变且数量不增加：原地修改，保留时间优先级（档位总量随之调整）；规则 4、5 都不适用
        // Same price and the quantity does not grow: modify in place, keeping time priority (the level total is
        // adjusted accordingly); neither rule 4 nor rule 5 applies
        if (same_price && new_qty <= order->remaining) {
            level->total -= order->remaining - new_qty;
            order->remaining = new_qty;
            listener_.onModified(id, new_price, new_qty);
            return;
        }
        // 档位总量上限：订单将重新挂到 new_price。目标档位中其他订单的总量：同价时是本档总量减去它自己，
        // 改价时是新价位的现有总量（没有该价位为 0）
        // Level total bound: the order will rest again at new_price. Total of the other orders at the target level:
        // at the same price, this level's total minus the order itself; on a reprice, the existing total at the new
        // price (0 if that level does not exist)
        const Qty others = same_price ? level->total - order->remaining : volumeAt(order->side, new_price);
        if (HFT_UNLIKELY(new_qty > kMaxLevelQty - others))
            return reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
        if (order->post_only && !same_price && crosses(order->side, new_price))
            return reject(id, RequestType::Modify, RejectReason::PostOnlyWouldCross);

        // 改价或加量：移出（失去时间优先级）→ onModified → 作为限价 GTC taker 撮合 → 剩余量挂到新价位队尾。
        // 订单对象直接归还对象池、挂单时再取（LIFO 复用同一个槽位），与参考模型一样不存在"游离订单"状态。
        // Reprice or quantity increase: take out (loses time priority) → onModified → match as a limit GTC taker →
        // rest the remainder at the back of the new level's queue.
        // The order object goes straight back to the object pool and is taken again on resting (LIFO reuses the
        // same slot), so, as in the reference model, there is no "detached order" state.
        Taker taker{id, new_qty, keyOf(opposite(order->side), new_price), order->account, order->side,
                    order->stp};
        const bool post_only = order->post_only;
        unlink(order);
        const SideLevels& own = sides_[sideIndex(taker.side)];  // 移出旧价位（可能删掉该档）之后再查挂单位置
                                                                // find where to rest only after leaving the old level
                                                                // (which may remove that level)
        if (!crossesTop(taker)) {
            // 不穿价：整单挂到新价位。索引条目原地占位（查询看不到），挂单后写回，省掉一次删除与一次插入
            // No cross: the whole order rests at the new price. The index entry is held in place as a placeholder
            // (invisible to queries) and written back after resting, saving one erase and one insert
            *slot = nullptr;
            notifyHolding(slot, [&] { listener_.onModified(id, new_price, new_qty); });
            *slot = rest(taker, new_price, post_only, lowerBoundFromBack(own, keyOf(taker.side, new_price)));
            return;
        }
        index_.eraseAt(slot);  // 撮合会删除 maker 的索引条目、移动其他槽位：先删掉本单的条目
                               // matching erases maker index entries and moves other slots:
                               // erase this order's entry first
        listener_.onModified(id, new_price, new_qty);
        if (!match(taker) || taker.remaining == 0) return;  // 被自成交保护撤销，或全部成交
                                                            // cancelled by self-trade prevention, or fully filled
        index_.insert(id, rest(taker, new_price, post_only, lowerBoundFromBack(own, keyOf(taker.side, new_price))));
    }

    // ---------------------------------------------------------------- 查询（const，绝不调用监听器）
    // ---------------------------------------------------------------- Queries (const, never call the listener)
    std::optional<Price> bestBid() const noexcept {
        const SideLevels& v = sides_[sideIndex(Side::Buy)];
        if (v.empty()) return std::nullopt;
        return v.back().level->price;
    }

    std::optional<Price> bestAsk() const noexcept {
        const SideLevels& v = sides_[sideIndex(Side::Sell)];
        if (v.empty()) return std::nullopt;
        return v.back().level->price;
    }

    // 活动（挂单中）订单数
    // Number of live (resting) orders
    std::size_t orderCount() const noexcept { return order_pool_.size(); }

    std::size_t levelCount(Side side) const noexcept { return sides_[sideIndex(side)].size(); }

    // 最优档在前；每档给出剩余总量与订单数（订单数超过 32 位时饱和）
    // Best level first; per level, the total remaining quantity and the order count (which saturates when it
    // exceeds 32 bits)
    std::vector<LevelInfo> depth(Side side, std::size_t max_levels = SIZE_MAX) const {
        const SideLevels& v = sides_[sideIndex(side)];
        const std::size_t n = std::min(max_levels, v.size());
        std::vector<LevelInfo> out;
        out.reserve(n);
        for (std::size_t k = 1; k <= n; ++k) {
            const Level* l = v[v.size() - k].level;
            out.push_back(LevelInfo{l->price, l->total, saturate32(l->count)});
        }
        return out;
    }

    // 该价位的活动订单，按 FIFO（时间优先级）顺序；无此价位返回空
    // Live orders at that price in FIFO (time priority) order; empty if there is no such level
    std::vector<OrderInfo> ordersAt(Side side, Price price) const {
        std::vector<OrderInfo> out;
        const Level* l = findLevel(side, price);
        if (l == nullptr) return out;
        out.reserve(static_cast<std::size_t>(l->count));
        for (const Order* o = l->head; o != nullptr; o = o->next) out.push_back(infoOf(*o));
        return out;
    }

    std::optional<OrderInfo> findOrder(OrderId id) const noexcept {
        Order* const* slot = index_.find(id);
        if (slot == nullptr || *slot == nullptr) return std::nullopt;  // 空指针：挂单前的占位条目
                                                                       // null: placeholder entry before resting
        return infoOf(**slot);
    }

    // 某价位的剩余总量；没有该价位返回 0
    // Total remaining quantity at a price; 0 if there is no such level
    Qty volumeAt(Side side, Price price) const noexcept {
        const Level* l = findLevel(side, price);
        return l == nullptr ? 0 : l->total;
    }

    // 最近一笔成交的 id；尚无成交时为 0
    // Id of the most recent trade; 0 if there has been no trade yet
    TradeId lastTradeId() const noexcept { return last_trade_id_; }

    std::size_t maxOrders() const noexcept { return max_orders_; }

    // 结构不变量检查（O(n)，测试与调试用）：失败时返回 false，并在 why 中写明第一处违例。
    // 检查：两侧严格有序且 key 与价格一致、没有空价位、FIFO 双向链接完整、档位总量与订单数与链表一致、
    // 每个订单剩余量为正且归属正确、索引与挂单一一对应、对象池无泄漏、哈希表自身不变量、盘口不交叉。
    // Structural invariant check (O(n), for tests and debugging): returns false on failure and describes the
    // first violation in why.
    // Checks: both sides strictly sorted with keys consistent with prices, no empty levels, intact FIFO double
    // links, level totals and order counts consistent with the lists, every order's remaining quantity positive
    // and its ownership correct, the index and the resting orders in one-to-one correspondence, no object pool
    // leaks, the hash map's own invariants, and an uncrossed top of book.
    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why != nullptr) *why = msg;
            return false;
        };
        // 诊断字符串只在失败时才拼接：通过检查的路径上不做任何分配
        // Diagnostic strings are built only on failure: the passing path does no allocation at all
        auto levelName = [](Side side, const Level* l) {
            return std::string(toString(side)) + " level at price " + std::to_string(l->price);
        };
        auto orderName = [](const Order* o) { return "order " + std::to_string(o->id); };
        std::size_t resting = 0;
        for (Side side : {Side::Buy, Side::Sell}) {
            const SideLevels& v = sides_[sideIndex(side)];
            for (std::size_t i = 0; i < v.size(); ++i) {
                const LevelRef& ref = v[i];
                const Level* l = ref.level;
                if (l == nullptr || !level_pool_.owns(l))
                    return fail(std::string(toString(side)) + " level #" + std::to_string(i) +
                                (l == nullptr ? " is null" : " does not come from the level pool"));
                if (l->price <= 0) return fail(levelName(side, l) + " has a non-positive price");
                if (ref.key != keyOf(side, l->price)) return fail(levelName(side, l) + " has a stale sort key");
                if (i > 0 && !(v[i - 1].key < ref.key))
                    return fail(std::string(toString(side)) + " side not strictly sorted at " + levelName(side, l));
                if (l->count == 0 || l->head == nullptr || l->tail == nullptr)
                    return fail(levelName(side, l) + " is empty");
                std::uint64_t n = 0;
                Qty sum = 0;
                const Order* prev = nullptr;
                for (const Order* o = l->head; o != nullptr; o = o->next) {
                    if (++n > l->count)
                        return fail(levelName(side, l) + ": FIFO longer than its order count (or cyclic)");
                    if (!order_pool_.owns(o))
                        return fail(levelName(side, l) + ": FIFO holds an order that does not come from the order pool");
                    if (o->prev != prev)
                        return fail(orderName(o) + (prev == nullptr ? " is the FIFO head but has a predecessor"
                                                                    : " has a broken prev link"));
                    if (o->level != l) return fail(orderName(o) + " points to another level");
                    if (o->side != side) return fail(orderName(o) + " rests on the wrong side");
                    if (o->remaining <= 0)
                        return fail(orderName(o) + " has non-positive remaining " + std::to_string(o->remaining));
                    if (o->remaining > kMaxLevelQty - sum)  // 档位总量上限（sum >= 0、remaining > 0，不会溢出）
                                                            // level total bound (sum >= 0, remaining > 0: no overflow)
                        return fail(levelName(side, l) + ": total quantity exceeds kMaxLevelQty");
                    sum += o->remaining;
                    Order* const* found = index_.find(o->id);
                    if (found == nullptr) return fail("resting " + orderName(o) + " is missing from the index");
                    if (*found != o)
                        return fail("index entry of " + orderName(o) + " points to another order (duplicate id?)");
                    prev = o;
                }
                if (prev != l->tail) return fail(levelName(side, l) + ": tail is not the last order of the FIFO");
                if (n != l->count)
                    return fail(levelName(side, l) + " counts " + std::to_string(l->count) + " orders but holds " +
                                std::to_string(n));
                if (sum != l->total)
                    return fail(levelName(side, l) + " caches total " + std::to_string(l->total) +
                                " but its orders sum to " + std::to_string(sum));
                resting += static_cast<std::size_t>(n);
            }
        }
        // 每个挂单都在索引中且索引指向它自身，再加上数量相等 => 索引与挂单一一对应。唯一的例外是挂单前的
        // 占位条目（空指针）：它只在不穿价的新单 / 改单的那一次回调期间存在，因此至多一个
        // Every resting order is in the index and its entry points to that order, plus equal sizes => the index and
        // the resting orders correspond one to one. The only exception is the placeholder entry (null pointer)
        // before resting: it exists only during the one callback of a non-crossing new order / modify, so there is
        // at most one
        if (index_.size() != resting) {
            std::size_t placeholders = 0;
            index_.forEach([&placeholders](OrderId, Order* const& o) { placeholders += o == nullptr ? 1 : 0; });
            if (placeholders != 1 || resting + 1 != index_.size())
                return fail("index holds " + std::to_string(index_.size()) + " entries (" +
                            std::to_string(placeholders) + " placeholders) but " + std::to_string(resting) +
                            " orders rest in the book");
        }
        if (order_pool_.size() != resting)
            return fail("order pool holds " + std::to_string(order_pool_.size()) + " live orders but " +
                        std::to_string(resting) + " rest in the book (leak)");
        const std::size_t levels = sides_[0].size() + sides_[1].size();
        if (level_pool_.size() != levels)
            return fail("level pool holds " + std::to_string(level_pool_.size()) + " live levels but the book has " +
                        std::to_string(levels) + " (leak)");
        if (safe_orders_ > capacityBound())
            return fail("capacity bound safe_orders_ exceeds an actual capacity");
        std::string map_why;
        if (!index_.checkInvariants(&map_why)) return fail("order index: " + map_why);
        const std::optional<Price> bid = bestBid();
        const std::optional<Price> ask = bestAsk();
        if (bid && ask && *bid >= *ask)
            return fail("book crossed: best bid " + std::to_string(*bid) + " >= best ask " + std::to_string(*ask));
        return true;
    }

private:
    // ---------------------------------------------------------------- 小工具
    // ---------------------------------------------------------------- Helpers
    static constexpr std::size_t sideIndex(Side side) noexcept { return side == Side::Buy ? 0 : 1; }
    static constexpr Side opposite(Side side) noexcept { return side == Side::Buy ? Side::Sell : Side::Buy; }

    // 排序 key：买盘 = price，卖盘 = -price（两侧都是"越大越优"）。price 必须 > 0（取负不溢出）。
    // Sort key: bids = price, asks = -price (both sides are "larger is better"). price must be > 0 (so negation
    // does not overflow).
    static constexpr Price keyOf(Side side, Price price) noexcept {
        return side == Side::Buy ? price : -price;
    }

    static constexpr std::uint32_t saturate32(std::uint64_t v) noexcept {
        return v > std::numeric_limits<std::uint32_t>::max() ? std::numeric_limits<std::uint32_t>::max()
                                                             : static_cast<std::uint32_t>(v);
    }

    // 枚举字段是否都在定义的取值范围内（各枚举从 0 连续编号，见下方 static_assert）
    // Whether every enum field is within its defined range (each enum is numbered contiguously from 0; see the
    // static_assert below)
    template <class E>
    static constexpr std::uint8_t raw(E v) noexcept {
        return static_cast<std::uint8_t>(v);
    }
    static_assert(raw(Side::Buy) == 0 && raw(Side::Sell) == 1);
    static_assert(raw(OrderType::Limit) == 0 && raw(OrderType::Market) == 1);
    static_assert(raw(TimeInForce::GTC) == 0 && raw(TimeInForce::IOC) == 1 && raw(TimeInForce::FOK) == 2);
    static_assert(raw(StpMode::None) == 0 && raw(StpMode::CancelTaker) == 1 && raw(StpMode::CancelMaker) == 2 &&
                  raw(StpMode::CancelBoth) == 3);
    static constexpr bool validEnums(const OrderRequest& r) noexcept {
        return raw(r.side) <= raw(Side::Sell) && raw(r.type) <= raw(OrderType::Market) &&
               raw(r.tif) <= raw(TimeInForce::FOK) && raw(r.stp) <= raw(StpMode::CancelBoth);
    }

    static OrderInfo infoOf(const Order& o) noexcept {
        return OrderInfo{o.id, o.account, o.side, o.level->price, o.remaining, o.post_only, o.stp};
    }

    void reject(OrderId id, RequestType request, RejectReason reason) {
        listener_.onRejected(id, request, reason);
    }

    // 在 key 升序的数组中找第一个 key >= k 的下标（等价于 std::lower_bound）。
    // 先从末尾（盘口）线性探测至多 kLinearProbe 个元素，未命中再对剩余前缀二分。
    // 每张挂单都会调用：强制内联（submit 很大，编译器的内联预算用完后会把它留成函数调用）
    // Find the index of the first key >= k in an array with ascending keys (equivalent to std::lower_bound).
    // First probe at most kLinearProbe elements linearly from the end (top of book); on a miss, binary-search
    // the remaining prefix.
    // Called for every resting order: force-inlined (submit is large, and once the compiler's inlining budget
    // is used up it would leave this as a function call)
    HFT_ALWAYS_INLINE static std::size_t lowerBoundFromBack(const SideLevels& v, Price key) noexcept {
        const LevelRef* d = v.data();
        std::size_t hi = v.size();
        // 不变量：下标 >= hi 的元素 key 都 >= key
        // Invariant: every element at index >= hi has key >= key
        for (std::size_t step = 0; step < kLinearProbe; ++step) {
            if (hi == 0 || d[hi - 1].key < key) return hi;
            --hi;
        }
        std::size_t lo = 0;
        while (lo < hi) {
            const std::size_t mid = lo + (hi - lo) / 2;
            if (d[mid].key < key) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    // 查找价位；price <= 0 时不可能存在（也避免对任意查询价格取负溢出）
    // Find a level; none can exist for price <= 0 (this also avoids overflow when negating an arbitrary query
    // price)
    const Level* findLevel(Side side, Price price) const noexcept {
        if (price <= 0) return nullptr;
        const SideLevels& v = sides_[sideIndex(side)];
        const Price key = keyOf(side, price);
        const std::size_t i = lowerBoundFromBack(v, key);
        return (i < v.size() && v[i].key == key) ? v[i].level : nullptr;
    }

    // 规范定义：买单 —— 卖盘非空且 p >= 卖一；卖单 —— 买盘非空且 p <= 买一。要求 p > 0。
    // Spec definition: a buy order — the ask side is non-empty and p >= best ask; a sell order — the bid side is
    // non-empty and p <= best bid. Requires p > 0.
    bool crosses(Side side, Price p) const noexcept {
        const Side opp = opposite(side);
        const SideLevels& v = sides_[sideIndex(opp)];
        return !v.empty() && v.back().key >= keyOf(opp, p);
    }

    // taker 能否与对手方最优档成交（撮合循环的入口条件；被动单借此跳过整个撮合调用）
    // Whether the taker can trade against the opposite side's best level (the matching loop's entry condition;
    // passive orders use it to skip the whole match call)
    bool crossesTop(const Taker& t) const noexcept {
        const SideLevels& v = sides_[sideIndex(opposite(t.side))];
        return !v.empty() && v.back().key >= t.limit_key;
    }

    // 限价 GTC 新单：撮合后的剩余量挂到该价位 FIFO 队尾（不发事件）。规则 4~7 按规范顺序检查，但其中两条
    // 只对一类订单可能成立：穿价的订单在本侧该价位不可能已有档位（盘口不交叉），规则 6 恒通过；
    // 不穿价的订单规则 7 恒通过，而且会整单挂上去
    // New limit GTC order: the remainder after matching rests at the back of that price's FIFO (no event). Rules
    // 4~7 are checked in spec order, but two of them can only hold for one kind of order: an order that crosses
    // cannot already have a level at that price on its own side (the book is not crossed), so rule 6 always
    // passes; for an order that does not cross, rule 7 always passes, and the whole order rests
    HFT_ALWAYS_INLINE void submitGtc(const OrderRequest& req, Taker& taker) {
        const SideLevels& own = sides_[sideIndex(req.side)];
        const Price key = keyOf(req.side, req.price);
        // 挂单位置：从这里到挂单，撮合只改动对手方、回调不得重入、扩容不改变下标，本侧数组不变
        // Resting position: from here until the order rests, matching only touches the opposite side, callbacks
        // must not re-enter and growing does not change indices, so this side's array stays unchanged
        const std::size_t pos = lowerBoundFromBack(own, key);
        const bool full = max_orders_ != 0 && orderCount() >= max_orders_;
        const Price price = req.price;
        const bool post_only = req.post_only;
        if (crossesTop(taker)) {
            if (index_.contains(req.id)) return reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
            if (full) return reject(req.id, RequestType::New, RejectReason::CapacityExceeded);
            if (post_only) return reject(req.id, RequestType::New, RejectReason::PostOnlyWouldCross);
            ensureCapacityForOneMore();  // 可能扩容（抛 bad_alloc 时尚无任何副作用）
                                         // may grow (no side effects yet if it throws bad_alloc)
            listener_.onAccepted(req);
            if (!match(taker) || taker.remaining == 0) return;  // 被自成交保护终止，或全部成交
                                                                // terminated by self-trade prevention, or fully filled
            [[maybe_unused]] const bool fresh = index_.insert(taker.id, rest(taker, price, post_only, pos)).second;
            HFT_ASSERT(fresh && "accepted order id is already in the index");
            return;
        }
        // 规则 6（档位总量上限）只在本侧该价位已有档位时可能触发
        // Rule 6 (level total bound) can only fire when this side already has a level at that price
        const Qty resting = pos < own.size() && own[pos].key == key ? own[pos].level->total : 0;
        if (HFT_UNLIKELY(full || req.qty > kMaxLevelQty - resting)) {
            if (index_.contains(req.id)) return reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
            return reject(req.id, RequestType::New,
                          full ? RejectReason::CapacityExceeded : RejectReason::InvalidQuantity);
        }
        ensureCapacityForOneMore();  // 先扩容（抛 bad_alloc 时尚无任何副作用），下面的占位插入就不会再扩容
                                     // grow first (no side effects yet if it throws bad_alloc),
                                     // so the placeholder insert below does not grow again
        const auto [slot, fresh] = index_.insert(req.id, nullptr);  // 规则 4 查重，同时为挂单占好索引槽位
                                                                    // rule 4 duplicate check, which also claims
                                                                    // the index slot for the resting order
        if (!fresh) return reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
        notifyHolding(slot, [&] { listener_.onAccepted(req); });
        *slot = rest(taker, price, post_only, pos);
    }

    // 索引中有占位条目 slot 时调用监听器：回调抛异常则撤销占位（簿保持结构一致），异常原样传播
    // Invoke the listener while the index holds the placeholder entry slot: if the callback throws, remove the
    // placeholder (the book stays structurally consistent) and let the exception propagate as is
    template <class Callback>
    HFT_ALWAYS_INLINE void notifyHolding(Order** slot, Callback&& callback) {
        try {
            callback();
        } catch (...) {
            index_.eraseAt(slot);  // 回调不能修改索引，slot 仍然有效
                                   // callbacks cannot modify the index, so slot is still valid
            throw;
        }
    }

    // ---------------------------------------------------------------- 容量
    // 保证接下来挂一个新订单（可能新建一个价位）不需要任何分配。热路径上只有一次比较：
    // safe_orders_ = min(索引、订单池、价位池、两侧价位数组的容量)。每个价位至少有一个订单，所以价位总数
    // 与任一侧的价位数都不超过订单数；订单数 < safe_orders_ 时再加一个订单和一个价位不会超出任何一项容量。
    // 否则统一倍增扩容（可能抛 std::bad_alloc，此时尚未修改任何状态）。订单数因此始终不超过 safe_orders_，
    // 不增加订单数的改单也就总有足够的容量。
    // ---------------------------------------------------------------- Capacity
    // Guarantees that resting the next new order (possibly creating a level) needs no allocation. The hot path
    // has only one comparison:
    // safe_orders_ = min(capacities of the index, order pool, level pool and both sides' level arrays). Every
    // level has at least one order, so neither the total level count nor either side's level count exceeds the
    // order count; while the order count < safe_orders_, one more order and one more level exceed no capacity.
    // Otherwise everything grows by doubling (may throw std::bad_alloc, when no state has been modified yet).
    // The order count therefore never exceeds safe_orders_, and a modify, which does not increase the order
    // count, always has enough capacity.
    void ensureCapacityForOneMore() {
        if (HFT_LIKELY(orderCount() < safe_orders_)) return;
        grow();
    }

    __attribute__((noinline, cold)) void grow() {
        const std::size_t need = std::max(safe_orders_, orderCount() + 1);
        reserve(need + std::max(need, kMinGrowth));
    }

    // 预留容量：保证活动订单数不超过 n 时 submit/cancel/modify 不分配内存（只增不减）
    // Reserve capacity: guarantees that submit/cancel/modify do not allocate memory while there are at most n
    // live orders (grows only, never shrinks)
    void reserve(std::size_t n) {
        index_.reserve(n);
        order_pool_.reserve(n);
        level_pool_.reserve(n);
        sides_[0].reserve(n);
        sides_[1].reserve(n);
        safe_orders_ = capacityBound();
    }

    std::size_t capacityBound() const noexcept {
        return std::min({index_.capacity(), order_pool_.capacity(), level_pool_.capacity(), sides_[0].capacity(),
                         sides_[1].capacity()});
    }

    // ---------------------------------------------------------------- 挂单与移除
    // 剩余量挂到 price 价位 FIFO 队尾（价位不存在则在 pos 处创建），返回新订单，由调用方写入索引。
    // pos 是 price 在本侧价位数组中的 lowerBound 位置，由调用方查好、其后本侧数组未变。容量已经足够
    // （见 ensureCapacityForOneMore），这里的对象池与价位数组操作都不会分配内存；调用方也已检查过档位总量上限。
    // ---------------------------------------------------------------- Resting and removal
    // Rests the remainder at the back of the price level's FIFO (creating the level at pos if it does not
    // exist) and returns the new order, which the caller writes into the index.
    // pos is the lowerBound position of price in this side's level array, looked up by the caller, and this
    // side's array has not changed since. Capacity is already sufficient (see ensureCapacityForOneMore), so the
    // object pool and level array operations here allocate no memory; the caller has also already checked the
    // level total bound.
    HFT_ALWAYS_INLINE Order* rest(const Taker& t, Price price, bool post_only, std::size_t pos) {
        SideLevels& v = sides_[sideIndex(t.side)];
        const Price key = keyOf(t.side, price);
        HFT_ASSERT(pos == lowerBoundFromBack(v, key));
        Level* level;
        if (pos < v.size() && v[pos].key == key) {
            level = v[pos].level;
        } else {
            HFT_ASSERT(level_pool_.available() > 0 && v.size() < v.capacity());
            level = level_pool_.create(price);
            v.insert(v.begin() + static_cast<std::ptrdiff_t>(pos), LevelRef{key, level});
        }
        HFT_ASSERT(t.remaining <= kMaxLevelQty - level->total);
        HFT_ASSERT(order_pool_.available() > 0 && orderCount() < index_.capacity());
        Order* const order = order_pool_.create(t.id, t.remaining, t.account, t.side, post_only, t.stp);
        order->level = level;
        order->prev = level->tail;
        if (level->tail != nullptr) level->tail->next = order;
        else level->head = order;
        level->tail = order;
        level->total += order->remaining;
        ++level->count;
        return order;
    }

    // 把订单从所属价位的 FIFO 中摘下并归还对象池，价位空了就删除该价位（索引条目由调用方处理）
    // Unlinks the order from its level's FIFO and returns it to the object pool, deleting the level if it
    // becomes empty (the caller handles the index entry)
    void unlink(Order* order) noexcept {
        Level* const level = order->level;
        if (order->prev != nullptr) order->prev->next = order->next;
        else level->head = order->next;
        if (order->next != nullptr) order->next->prev = order->prev;
        else level->tail = order->prev;
        level->total -= order->remaining;
        --level->count;
        const Side side = order->side;
        order_pool_.destroy(order);
        if (level->count == 0) removeLevel(side, level);
    }

    // 删除一个空价位：通常是最优档（pop_back），否则查找后 erase（靠近盘口时 memmove 很短）
    // Deletes an empty level: usually the best level (pop_back), otherwise a lookup and then erase (the memmove
    // is short near the touch)
    void removeLevel(Side side, Level* level) noexcept {
        SideLevels& v = sides_[sideIndex(side)];
        if (HFT_LIKELY(v.back().level == level)) {
            v.pop_back();
        } else {
            const std::size_t i = lowerBoundFromBack(v, keyOf(side, level->price));
            HFT_ASSERT(i < v.size() && v[i].level == level);
            v.erase(v.begin() + static_cast<std::ptrdiff_t>(i));
        }
        level_pool_.destroy(level);
    }

    // ---------------------------------------------------------------- FOK 预检与撮合
    // FOK 预检：按撮合顺序遍历可成交价位上的 maker，用"仍需数量"做减法（只在不够时才减，结果恒为正）。
    // STP 适用的 maker：CancelMaker 跳过（贡献 0、继续），CancelTaker/CancelBoth 停止遍历（不可成交）。
    // 不启用 STP 时整档可成交，直接用缓存的档位总量，不必逐单遍历——档位总量上限保证它是精确值。
    // ---------------------------------------------------------------- FOK pre-check and matching
    // FOK pre-check: walks the makers on the tradable levels in matching order, subtracting from the "still
    // needed" quantity (only subtracting when a maker falls short, so the result is always positive).
    // Makers subject to STP: CancelMaker skips them (contributes 0, continues), CancelTaker/CancelBoth stop the
    // walk (not fillable).
    // Without STP a whole level is fillable, so the cached level total is used directly instead of walking order
    // by order — the level total bound guarantees that it is exact.
    bool fokFillable(const Taker& t) const noexcept {
        const SideLevels& book = sides_[sideIndex(opposite(t.side))];
        Qty needed = t.remaining;
        for (std::size_t i = book.size(); i-- > 0;) {
            const LevelRef& ref = book[i];
            if (ref.key < t.limit_key) break;  // 超出限价的档位不计入
                                               // levels beyond the limit price do not count
            const Level* level = ref.level;
            if (t.stp == StpMode::None) {
                if (level->total >= needed) return true;
                needed -= level->total;
                continue;
            }
            for (const Order* o = level->head; o != nullptr; o = o->next) {
                if (o->account == t.account) {
                    if (t.stp == StpMode::CancelMaker) continue;
                    return false;
                }
                if (o->remaining >= needed) return true;
                needed -= o->remaining;
            }
        }
        return false;
    }

    // 撮合循环：taker 按价格-时间优先级吃对手方，直到成交完、对手方空或不再穿价。
    // 返回 false 表示 taker 被自成交保护（CancelTaker/CancelBoth）终止。
    // Matching loop: the taker consumes the opposite side in price-time priority until it is fully filled, the
    // opposite side is empty, or it no longer crosses.
    // Returns false if self-trade prevention (CancelTaker/CancelBoth) terminated the taker.
    bool match(Taker& t) {
        SideLevels& book = sides_[sideIndex(opposite(t.side))];
        while (t.remaining > 0 && !book.empty()) {
            const LevelRef top = book.back();
            if (top.key < t.limit_key) break;  // 对手最优价不再可成交
                                               // the opposite best price is no longer tradable
            Level* const level = top.level;
            Order* const maker = level->head;

            if (HFT_UNLIKELY(t.stp != StpMode::None && maker->account == t.account)) {
                if (!preventSelfTrade(t, maker)) return false;
                continue;  // CancelMaker：继续撮合
                           // CancelMaker: keep matching
            }

            const Qty q = t.remaining < maker->remaining ? t.remaining : maker->remaining;
            maker->remaining -= q;
            level->total -= q;
            t.remaining -= q;
            const Trade trade{++last_trade_id_, maker->id,   t.id, maker->account, t.account, t.side,
                              level->price,     q,           maker->remaining, t.remaining};
            if (maker->remaining == 0) {
                // 完全成交：maker 必是队首，直接出队；价位空了就弹出最优档（不另发事件）
                // Fully filled: the maker must be the queue head, so dequeue it directly; if the level becomes empty,
                // pop the best level (no separate event)
                level->head = maker->next;
                if (level->head != nullptr) level->head->prev = nullptr;
                else level->tail = nullptr;
                --level->count;
                index_.erase(maker->id);
                order_pool_.destroy(maker);
                if (level->count == 0) {
                    book.pop_back();
                    level_pool_.destroy(level);
                }
            }
            listener_.onTrade(trade);
        }
        return true;
    }

    // 自成交保护：taker 撞上同账户的 maker（撮合循环的冷路径，不内联以免撑大热循环）。
    // CancelTaker 撤 taker；CancelMaker / CancelBoth 先移除 maker（回调时簿已更新）再发事件，CancelBoth 随后撤 taker。
    // 返回 false 表示 taker 被撤销、撮合终止
    // Self-trade prevention: the taker runs into a maker of the same account (a cold path of the matching loop,
    // not inlined so it does not bloat the hot loop).
    // CancelTaker cancels the taker; CancelMaker / CancelBoth first remove the maker (the book is already
    // updated at the callback) and then emit the event, after which CancelBoth cancels the taker.
    // Returns false if the taker was cancelled and matching ends
    __attribute__((noinline, cold)) bool preventSelfTrade(const Taker& t, Order* maker) {
        if (t.stp != StpMode::CancelTaker) {
            const OrderId maker_id = maker->id;
            const Qty maker_remaining = maker->remaining;
            index_.erase(maker_id);
            unlink(maker);
            listener_.onCancelled(maker_id, maker_remaining, CancelReason::SelfTradePrevention);
            if (t.stp == StpMode::CancelMaker) return true;
        }
        listener_.onCancelled(t.id, t.remaining, CancelReason::SelfTradePrevention);
        return false;
    }

    // ---------------------------------------------------------------- 成员
    // 对象池与索引在构造函数中由 reserve() 一次预留
    // ---------------------------------------------------------------- Members
    // The object pools and the index are reserved once by reserve() in the constructor
    Listener& listener_;
    std::size_t max_orders_;  // 0 = 不限
                              // 0 = unlimited
    hft::ObjectPool<Order> order_pool_{0};
    hft::ObjectPool<Level> level_pool_{0};
    SideLevels sides_[2];  // [0] 买盘（价格升序），[1] 卖盘（价格降序）；最优档都在末尾
                           // [0] bids (price ascending), [1] asks (price descending); best level at the end on both
    hft::FlatHashMap<OrderId, Order*> index_{0};  // 值为空指针的条目是挂单前的占位（对查询不可见）
                                                  // null-valued entries are placeholders before resting
                                                  // (invisible to queries)
    std::size_t safe_orders_ = 0;  // 订单数低于它时挂单保证不分配（见 ensureCapacityForOneMore）
                                   // resting never allocates while the order count is below it
                                   // (see ensureCapacityForOneMore)
    TradeId last_trade_id_ = 0;
};

}  // namespace hft::matching
