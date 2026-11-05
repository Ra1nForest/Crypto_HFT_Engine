//
// order_book.h
// 优化版限价订单簿（撮合核心）：池化订单 + 侵入式 FIFO 链表、池化价位、每侧最优档在末尾的有序扁平数组、
// 整数 id 开放寻址索引；预留容量之后热路径零堆分配。事件流与参考模型逐条一致（接口与语义见 docs/MATCHING_ENGINE.md）。
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
template <class Book>
struct OrderBookTestPeer;
}  // namespace detail

// ============================================================================
// 数据结构（推荐方案见 docs/MATCHING_ENGINE.md「Data structures」；在其基础上做了两处改进，理由见各条）：
//
// - 订单 Order：来自 hft::ObjectPool<Order>（地址稳定、LIFO 复用），48 字节；同一价位的订单用
//   prev/next 侵入式双向链表串成 FIFO（头部时间优先级最高）。订单不存价格，价格在所属 Level 中。
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
//   （后者由哈希表存放在表外专用槽位）。索引返回的槽位指针在任何插入/删除之后都可能失效，
//   因此代码只立即取出其中的 Order*，从不跨越修改操作持有槽位指针。
// - 只用整数运算；不抛异常的正常控制流；回调里不做任何分配。
//
// 容量与分配：构造时为 reserve_orders 个订单预留全部结构（订单池、价位池、两侧价位数组、索引）。
// 活动订单数不超过预留量时，submit/cancel/modify 不做任何堆分配（基准测试用计数版 operator new 验证）。
// 超过预留量时按倍增扩容：会挂单的操作在产生第一个事件或修改任何状态之前就先完成扩容
// （ensureCapacityForOneMore），因此内存分配失败（std::bad_alloc）时簿与事件流都保持不变（强异常保证）。
//
// 回调时机（与参考模型一致，监听器可以在回调中调用 const 查询）：每个事件发出时，簿内状态已经包含
// 该事件的全部效果——成交回报发出时完全成交的 maker 已被移除、空价位已删除；STP 撤 maker 时 maker
// 已移除；改价/加量的 onModified 发出时该订单已离开订单簿（之后作为 taker 撮合，剩余量最后静默挂单）；
// 原地改单的 onModified 发出时剩余量与档位总量已更新；新订单在 onAccepted 时尚未进入订单簿。
// 监听器不得在回调中重入调用本簿的 submit/cancel/modify；回调抛出的异常会原样传播，簿保持结构一致
// （checkInvariants() 成立），但正在处理的那条命令就此中止（例如主动单不再继续撮合、也不挂单）。
//
// 档位总量上限（契约的一部分，三个实现完全一致，见 matching/types.h 的 kMaxLevelQty）：会让某档总量超过
// INT64_MAX 的限价 GTC 新单与改价/加量改单，在产生任何其他事件之前以 InvalidQuantity 拒绝。缓存的档位
// 总量因此始终精确、不会溢出：depth()/volumeAt() 直接返回它；不启用 STP 的 FOK 预检也直接拿它与"仍需
// 数量"比较——参考模型逐单累计，结论不依赖任何缓存，这里的 FOK 判定则依赖这条上限。
//
// 规范未定义的输入：side/type/tif/stp 超出枚举取值范围（只能通过强制类型转换构造）时按 InvalidFlags 拒单
// （作为第 3 条规则"标志非法"的一部分），保证引擎对任意字节输入都有确定行为、不会越界访问。
// 合法输入的事件流与规范完全一致。
//
// 非线程安全：同一订单簿只能由一个线程使用（多线程场景由 engine_runner 的单引擎线程串行化）。
// ============================================================================
template <class Listener>
class LimitOrderBook {
    template <class Book>
    friend struct detail::OrderBookTestPeer;

    struct Level;

    // 挂单（48 字节）。prev/next 为同一价位 FIFO 中的前驱/后继，level 为所属价位。
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
    struct LevelRef {
        Price key;
        Level* level;
    };

    using SideLevels = std::vector<LevelRef>;

    // 撮合中的主动方（新订单，或改价/加量后重新撮合的订单）
    struct Taker {
        OrderId id;
        Qty remaining;
        Price limit_key;  // 限价在对手方 key 空间中的值；市价单为 kMarketKey
        AccountId account;
        Side side;
        StpMode stp;
    };

    static constexpr Price kMarketKey = std::numeric_limits<Price>::min();
    static constexpr std::size_t kLinearProbe = 8;   // 查找价位时先从盘口线性探测的元素数
    static constexpr std::size_t kMinGrowth = 64;    // 超出预留容量后的最小扩容量

public:
    using listener_type = Listener;
    static constexpr std::size_t kDefaultReserveOrders = 1024;

    // max_orders：活动挂单上限（0 = 不限）；reserve_orders：预留容量（活动订单不超过它时热路径零分配）
    explicit LimitOrderBook(Listener& listener, std::size_t max_orders = 0,
                            std::size_t reserve_orders = kDefaultReserveOrders)
        : listener_(listener),
          max_orders_(max_orders),
          order_pool_(reserve_orders, true),
          level_pool_(reserve_orders, true),
          index_(reserve_orders) {
        reserve(reserve_orders);
    }

    // 持有监听器引用与指向池内对象的指针：禁止拷贝与移动
    LimitOrderBook(const LimitOrderBook&) = delete;
    LimitOrderBook& operator=(const LimitOrderBook&) = delete;
    LimitOrderBook(LimitOrderBook&&) = delete;
    LimitOrderBook& operator=(LimitOrderBook&&) = delete;
    ~LimitOrderBook() = default;  // Order/Level 都是平凡析构类型，对象池直接释放内存

    // ---------------------------------------------------------------- 新订单（docs/MATCHING_ENGINE.md「submit」）
    void submit(const OrderRequest& req) {
        // 校验：按规范顺序，第一条失败的规则生效，只发出这一个拒单事件
        if (req.qty <= 0) return reject(req.id, RequestType::New, RejectReason::InvalidQuantity);
        if (req.type == OrderType::Limit && req.price <= 0)
            return reject(req.id, RequestType::New, RejectReason::InvalidPrice);
        if (HFT_UNLIKELY(!validEnums(req)) ||
            (req.post_only && (req.type != OrderType::Limit || req.tif != TimeInForce::GTC)))
            return reject(req.id, RequestType::New, RejectReason::InvalidFlags);
        if (index_.contains(req.id)) return reject(req.id, RequestType::New, RejectReason::DuplicateOrderId);
        const bool rests = req.type == OrderType::Limit && req.tif == TimeInForce::GTC;
        if (max_orders_ != 0 && index_.size() >= max_orders_ && rests)
            return reject(req.id, RequestType::New, RejectReason::CapacityExceeded);
        // 档位总量上限：只有限价 GTC 会挂单。撮合不触及本侧；本侧该价位已有档位时订单也不可能穿价
        // （盘口不交叉），会整单挂上去——所以这条检查恰好拒绝会让该档总量越界的请求。顺便记下挂单位置
        // 交给 rest()：从这里到挂单，撮合只改动对手方、回调不得重入、扩容不改变下标，本侧数组不变
        std::size_t rest_pos = 0;
        if (rests) {
            const SideLevels& own = sides_[sideIndex(req.side)];
            const Price key = keyOf(req.side, req.price);
            rest_pos = lowerBoundFromBack(own, key);
            const Qty resting = rest_pos < own.size() && own[rest_pos].key == key ? own[rest_pos].level->total : 0;
            if (HFT_UNLIKELY(req.qty > kMaxLevelQty - resting))
                return reject(req.id, RequestType::New, RejectReason::InvalidQuantity);
        }
        if (req.post_only && crosses(req.side, req.price))
            return reject(req.id, RequestType::New, RejectReason::PostOnlyWouldCross);

        // 回调之前先把需要的字段取成局部变量：req 是调用方的引用，不依赖它在回调期间保持不变
        const bool market = req.type == OrderType::Market;
        const TimeInForce tif = req.tif;
        const Price price = req.price;
        const bool post_only = req.post_only;
        Taker taker{req.id, req.qty, market ? kMarketKey : keyOf(opposite(req.side), req.price),
                    req.account, req.side, req.stp};
        if (rests) ensureCapacityForOneMore();  // 可能扩容（抛 bad_alloc 时尚无任何副作用）

        listener_.onAccepted(req);

        // FOK：按撮合顺序预检能否全部成交；不能则整单撤销，簿不受任何影响
        if (tif == TimeInForce::FOK && !fokFillable(taker)) {
            listener_.onCancelled(taker.id, taker.remaining, CancelReason::FokUnfillable);
            return;
        }
        if (crossesTop(taker) && !match(taker)) return;  // 被自成交保护终止：不挂单
        if (taker.remaining == 0) return;                // 全部成交
        if (market) {
            listener_.onCancelled(taker.id, taker.remaining, CancelReason::MarketRemainder);
            return;
        }
        if (tif == TimeInForce::IOC) {
            listener_.onCancelled(taker.id, taker.remaining, CancelReason::IocRemainder);
            return;
        }
        HFT_ASSERT(tif == TimeInForce::GTC && "FOK order has a remainder after a successful pre-check");
        rest(taker, price, post_only, rest_pos);  // 限价 GTC：剩余量挂到该价位 FIFO 队尾，不发事件
    }

    // ---------------------------------------------------------------- 撤单
    void cancel(OrderId id) {
        Order* const* slot = index_.find(id);
        if (slot == nullptr) return reject(id, RequestType::Cancel, RejectReason::UnknownOrder);
        Order* const order = *slot;  // 立即取出：removeOrder 会修改索引，使槽位指针失效
        const Qty remaining = order->remaining;
        removeOrder(order);
        listener_.onCancelled(id, remaining, CancelReason::UserRequested);
    }

    // ---------------------------------------------------------------- 改单（new_qty 是新的剩余数量）
    void modify(OrderId id, Price new_price, Qty new_qty) {
        Order* const* slot = index_.find(id);
        if (slot == nullptr) return reject(id, RequestType::Modify, RejectReason::UnknownOrder);
        Order* const order = *slot;
        if (new_qty <= 0) return reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
        if (new_price <= 0) return reject(id, RequestType::Modify, RejectReason::InvalidPrice);
        Level* const level = order->level;
        const bool same_price = new_price == level->price;
        const bool in_place = same_price && new_qty <= order->remaining;
        // 档位总量上限：改价或加量会把订单重新挂到 new_price（原地减量不会增加档位总量）。目标档位中其他
        // 订单的总量：同价时是本档总量减去它自己，改价时是新价位的现有总量（没有该价位为 0）
        if (!in_place) {
            const Qty others = same_price ? level->total - order->remaining : volumeAt(order->side, new_price);
            if (HFT_UNLIKELY(new_qty > kMaxLevelQty - others))
                return reject(id, RequestType::Modify, RejectReason::InvalidQuantity);
        }
        if (order->post_only && !same_price && crosses(order->side, new_price))
            return reject(id, RequestType::Modify, RejectReason::PostOnlyWouldCross);

        // 价格不变且数量不增加：原地修改，保留时间优先级（档位总量随之调整）
        if (in_place) {
            level->total -= order->remaining - new_qty;
            order->remaining = new_qty;
            listener_.onModified(id, new_price, new_qty);
            return;
        }

        // 改价或加量：移出（失去时间优先级）→ onModified → 作为限价 GTC taker 撮合 → 剩余量挂到新价位队尾。
        // 订单对象直接归还对象池、挂单时再取（LIFO 复用同一个槽位），与参考模型一样不存在"游离订单"状态。
        ensureCapacityForOneMore();  // 新价位可能需要新档位：先扩容，失败时尚无副作用
        Taker taker{id, new_qty, keyOf(opposite(order->side), new_price), order->account, order->side,
                    order->stp};
        const bool post_only = order->post_only;
        removeOrder(order);
        listener_.onModified(id, new_price, new_qty);
        if (crossesTop(taker) && !match(taker)) return;  // 被自成交保护撤销
        if (taker.remaining == 0) return;                // 全部成交
        // 移出旧价位（可能删掉该档）之后本侧数组已变：在这里查找挂单位置
        rest(taker, new_price, post_only,
             lowerBoundFromBack(sides_[sideIndex(taker.side)], keyOf(taker.side, new_price)));
    }

    // ---------------------------------------------------------------- 查询（const，绝不调用监听器）
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
    std::size_t orderCount() const noexcept { return index_.size(); }

    std::size_t levelCount(Side side) const noexcept { return sides_[sideIndex(side)].size(); }

    // 最优档在前；每档给出剩余总量与订单数（订单数超过 32 位时饱和）
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
        if (slot == nullptr) return std::nullopt;
        return infoOf(**slot);
    }

    // 某价位的剩余总量；没有该价位返回 0
    Qty volumeAt(Side side, Price price) const noexcept {
        const Level* l = findLevel(side, price);
        return l == nullptr ? 0 : l->total;
    }

    // 最近一笔成交的 id；尚无成交时为 0
    TradeId lastTradeId() const noexcept { return last_trade_id_; }

    std::size_t maxOrders() const noexcept { return max_orders_; }

    // 预留容量：保证活动订单数不超过 n 时 submit/cancel/modify 不分配内存（只增不减）
    void reserve(std::size_t n) {
        index_.reserve(n);
        order_pool_.reserve(n);
        level_pool_.reserve(n);
        sides_[0].reserve(n);
        sides_[1].reserve(n);
        safe_orders_ = std::min({index_.capacity(), order_pool_.capacity(), level_pool_.capacity(),
                                 sides_[0].capacity(), sides_[1].capacity()});
    }

    // 结构不变量检查（O(n)，测试与调试用）：失败时返回 false，并在 why 中写明第一处违例。
    // 检查：两侧严格有序且 key 与价格一致、没有空价位、FIFO 双向链接完整、档位总量与订单数与链表一致、
    // 每个订单剩余量为正且归属正确、索引与挂单一一对应、对象池无泄漏、哈希表自身不变量、盘口不交叉。
    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why != nullptr) *why = msg;
            return false;
        };
        // 诊断字符串只在失败时才拼接：通过检查的路径上不做任何分配
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
        // 每个挂单都在索引中且索引指向它自身，再加上数量相等 => 索引与挂单一一对应
        if (resting != index_.size())
            return fail("index holds " + std::to_string(index_.size()) + " entries but " +
                        std::to_string(resting) + " orders rest in the book");
        if (order_pool_.size() != resting)
            return fail("order pool holds " + std::to_string(order_pool_.size()) + " live orders but " +
                        std::to_string(resting) + " rest in the book (leak)");
        const std::size_t levels = sides_[0].size() + sides_[1].size();
        if (level_pool_.size() != levels)
            return fail("level pool holds " + std::to_string(level_pool_.size()) + " live levels but the book has " +
                        std::to_string(levels) + " (leak)");
        if (safe_orders_ > std::min({index_.capacity(), order_pool_.capacity(), level_pool_.capacity(),
                                     sides_[0].capacity(), sides_[1].capacity()}))
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
    static constexpr std::size_t sideIndex(Side side) noexcept { return side == Side::Buy ? 0 : 1; }
    static constexpr Side opposite(Side side) noexcept { return side == Side::Buy ? Side::Sell : Side::Buy; }

    // 排序 key：买盘 = price，卖盘 = -price（两侧都是"越大越优"）。price 必须 > 0（取负不溢出）。
    static constexpr Price keyOf(Side side, Price price) noexcept {
        return side == Side::Buy ? price : -price;
    }

    static constexpr std::uint32_t saturate32(std::uint64_t v) noexcept {
        return v > std::numeric_limits<std::uint32_t>::max() ? std::numeric_limits<std::uint32_t>::max()
                                                             : static_cast<std::uint32_t>(v);
    }

    // 枚举字段是否都在定义的取值范围内（各枚举从 0 连续编号，见下方 static_assert）
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
    static std::size_t lowerBoundFromBack(const SideLevels& v, Price key) noexcept {
        const LevelRef* d = v.data();
        std::size_t hi = v.size();
        // 不变量：下标 >= hi 的元素 key 都 >= key
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
    const Level* findLevel(Side side, Price price) const noexcept {
        if (price <= 0) return nullptr;
        const SideLevels& v = sides_[sideIndex(side)];
        const Price key = keyOf(side, price);
        const std::size_t i = lowerBoundFromBack(v, key);
        return (i < v.size() && v[i].key == key) ? v[i].level : nullptr;
    }

    // 规范定义：买单 —— 卖盘非空且 p >= 卖一；卖单 —— 买盘非空且 p <= 买一。要求 p > 0。
    bool crosses(Side side, Price p) const noexcept {
        const Side opp = opposite(side);
        const SideLevels& v = sides_[sideIndex(opp)];
        return !v.empty() && v.back().key >= keyOf(opp, p);
    }

    // taker 能否与对手方最优档成交（撮合循环的入口条件；被动单借此跳过整个撮合调用）
    bool crossesTop(const Taker& t) const noexcept {
        const SideLevels& v = sides_[sideIndex(opposite(t.side))];
        return !v.empty() && v.back().key >= t.limit_key;
    }

    // ---------------------------------------------------------------- 容量
    // 保证接下来挂一个新订单（可能新建一个价位）不需要任何分配。热路径上只有一次比较：
    // safe_orders_ = min(索引、订单池、价位池、两侧价位数组的容量)。每个价位至少有一个订单，所以价位总数
    // 与任一侧的价位数都不超过订单数；订单数 < safe_orders_ 时再加一个订单和一个价位不会超出任何一项容量。
    // 否则统一倍增扩容（可能抛 std::bad_alloc，此时尚未修改任何状态）。
    void ensureCapacityForOneMore() {
        if (HFT_LIKELY(index_.size() < safe_orders_)) return;
        grow();
    }

    __attribute__((noinline, cold)) void grow() {
        const std::size_t need = std::max(safe_orders_, index_.size() + 1);
        reserve(need + std::max(need, kMinGrowth));
    }

    // ---------------------------------------------------------------- 挂单与移除
    // 剩余量挂到 price 价位 FIFO 队尾（价位不存在则在 pos 处创建）。pos 是 price 在本侧价位数组中的
    // lowerBound 位置，由调用方查好、其后本侧数组未变。调用前必须已 ensureCapacityForOneMore()，
    // 因此这里的对象池、价位数组、索引操作都不会分配内存；调用方也已检查过档位总量上限。
    HFT_ALWAYS_INLINE void rest(const Taker& t, Price price, bool post_only, std::size_t pos) {
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
        HFT_ASSERT(order_pool_.available() > 0 && index_.size() < index_.capacity());
        Order* const order = order_pool_.create(t.id, t.remaining, t.account, t.side, post_only, t.stp);
        order->level = level;
        order->prev = level->tail;
        if (level->tail != nullptr) level->tail->next = order;
        else level->head = order;
        level->tail = order;
        level->total += order->remaining;
        ++level->count;
        index_.insert(t.id, order);
    }

    // 从订单簿移除一个活动订单（撤单、STP 撤 maker、改价/加量前移出）；价位空了就删除该价位
    void removeOrder(Order* order) noexcept {
        Level* const level = order->level;
        if (order->prev != nullptr) order->prev->next = order->next;
        else level->head = order->next;
        if (order->next != nullptr) order->next->prev = order->prev;
        else level->tail = order->prev;
        level->total -= order->remaining;
        --level->count;
        const Side side = order->side;
        index_.erase(order->id);
        order_pool_.destroy(order);
        if (level->count == 0) removeLevel(side, level);
    }

    // 删除一个空价位：通常是最优档（pop_back），否则查找后 erase（靠近盘口时 memmove 很短）
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
    bool fokFillable(const Taker& t) const noexcept {
        const SideLevels& book = sides_[sideIndex(opposite(t.side))];
        Qty needed = t.remaining;
        for (std::size_t i = book.size(); i-- > 0;) {
            const LevelRef& ref = book[i];
            if (ref.key < t.limit_key) break;  // 超出限价的档位不计入
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
    bool match(Taker& t) {
        SideLevels& book = sides_[sideIndex(opposite(t.side))];
        while (t.remaining > 0 && !book.empty()) {
            const LevelRef top = book.back();
            if (top.key < t.limit_key) break;  // 对手最优价不再可成交
            Level* const level = top.level;
            Order* const maker = level->head;

            if (HFT_UNLIKELY(t.stp != StpMode::None && maker->account == t.account)) {
                if (t.stp == StpMode::CancelTaker) {
                    listener_.onCancelled(t.id, t.remaining, CancelReason::SelfTradePrevention);
                    return false;
                }
                // CancelMaker / CancelBoth：先移除 maker（回调时簿已更新），再发事件
                const OrderId maker_id = maker->id;
                const Qty maker_remaining = maker->remaining;
                removeOrder(maker);
                listener_.onCancelled(maker_id, maker_remaining, CancelReason::SelfTradePrevention);
                if (t.stp == StpMode::CancelBoth) {
                    listener_.onCancelled(t.id, t.remaining, CancelReason::SelfTradePrevention);
                    return false;
                }
                continue;  // CancelMaker：继续撮合
            }

            const Qty q = t.remaining < maker->remaining ? t.remaining : maker->remaining;
            maker->remaining -= q;
            level->total -= q;
            t.remaining -= q;
            const Trade trade{++last_trade_id_, maker->id,   t.id, maker->account, t.account, t.side,
                              level->price,     q,           maker->remaining, t.remaining};
            if (maker->remaining == 0) {
                // 完全成交：maker 必是队首，直接出队；价位空了就弹出最优档（不另发事件）
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

    // ---------------------------------------------------------------- 成员
    Listener& listener_;
    std::size_t max_orders_;  // 0 = 不限
    hft::ObjectPool<Order> order_pool_;
    hft::ObjectPool<Level> level_pool_;
    SideLevels sides_[2];  // [0] 买盘（价格升序），[1] 卖盘（价格降序）；最优档都在末尾
    hft::FlatHashMap<OrderId, Order*> index_;
    std::size_t safe_orders_ = 0;  // 订单数低于它时挂单保证不分配（见 ensureCapacityForOneMore）
    TradeId last_trade_id_ = 0;
};

}  // namespace hft::matching
