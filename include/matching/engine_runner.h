//
// engine_runner.h
// 撮合引擎线程运行器：单引擎线程轮询各网关的入站 SPSC 命令队列并串行撮合，把事件按订单归属发布到
// 各网关的出站 SPSC 事件队列（成交同时回报 maker 与 taker 所在网关）；出站队列满时自旋等待，绝不丢弃。
// Matching engine thread runner: a single engine thread polls each gateway's inbound SPSC command queue
// and matches serially, publishing events by order ownership to each gateway's outbound SPSC event queue
// (a fill is reported to both the maker's and the taker's gateway); when an outbound queue is full it
// spins and never drops.
//

#pragma once

#include "hft/common.h"
#include "hft/flat_hash_map.h"
#include "hft/spsc_queue.h"
#include "hft/timing.h"
#include "matching/matching_engine.h"
#include "matching/types.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace hft::matching {

// ============================================================================
// 线程模型与数据流
// Threading model and data flow
//
//   网关线程 g ── EngineCommand ──▶ inbound(g) ──┐
//                                                 ├──▶ 引擎线程：轮询 + MatchingEngine（单线程撮合）
//   网关线程 g ◀── EngineEvent ─── outbound(g) ◀─┘
//   gateway thread g ── EngineCommand ──▶ inbound(g) ──┐
//                                                      ├──▶ engine thread: polling + MatchingEngine
//                                                      │    (single-threaded matching)
//   gateway thread g ◀── EngineEvent ─── outbound(g) ◀─┘
//
// - 每个网关一对 SPSC 队列：入站命令队列（网关线程是唯一生产者，引擎线程是唯一消费者）和出站事件队列
//   （引擎线程是唯一生产者，网关线程是唯一消费者）。没有锁，引擎线程之外没有任何线程触碰撮合状态。
// - 引擎线程轮询（round-robin）各入站队列：每个队列每轮最多连续处理 batch 条命令（批量消费，摊薄
//   读对方缓存行的开销，同时保证公平），然后换下一个队列；一整轮都没有命令时 cpuRelax() 忙等
//   （可配置连续空转若干次后 std::this_thread::yield()，用于 CPU 不够分的机器）。可选绑核。
// - 引擎序号 engine_seq：每处理一条命令加 1（从 1 开始，stop()/start() 重启后继续递增），即全局处理顺序。
//   同一条命令产生的全部事件都带这条命令的 engine_seq、client_ts 与发送网关 id。
// - 事件路由（"订单属于提交它的网关"）：
//   * Accepted / Rejected / Modified、命令自身订单的 Cancelled、成交的 taker 一侧 Fill → 发送该命令的网关；
//   * 成交的 maker 一侧 Fill → maker 订单所属的网关（双方属于同一网关时，该网关先后收到 maker、taker
//     两条 Fill）；自成交保护（STP）撤掉的 maker → maker 所属的网关。
//   归属表：每个交易对一个 hft::FlatHashMap<OrderId, GatewayId>，恰好包含该交易对的全部挂单
//   （checkInvariants() 验证与订单簿一一对应）。它完全由事件推导维护，热路径上不额外查询订单簿：
//   新的限价 GTC 订单撮合后仍有剩余（会挂单）时插入；maker 完全成交、被 STP 撤销、被撤单，
//   或改单后作为 taker 完全成交 / 被 STP 撤销时删除。
// - 会话隔离：撤单 / 改单的目标订单正挂在簿上但属于其他网关时，运行器直接回 Rejected(UnknownOrder)，
//   不交给撮合引擎——与真实交易所一样，一个会话既看不到也动不了别的会话的订单（对该网关而言，
//   这与订单不存在无法区分）。新订单 id 与其他网关的活动订单重复时照常由引擎拒绝（DuplicateOrderId）。
//   由此可推出：每条命令至少产生一个发给其发送网关的事件，且网关按发送顺序收到各命令的第一个事件
//   （网关据此确认在途命令、测量往返延迟）。
// - 背压：出站队列满时引擎线程原地自旋（cpuRelax，可配置 yield），直到消费者腾出空间；事件从不丢弃。
//   代价是一个不消费的网关会让整个引擎停下来，因此网关线程必须持续消费自己的事件队列，包括在等待
//   入站队列空位时（否则"入站满 + 出站满"会互相等待而死锁），以及 stop() 进行期间。
// - 停止：stop() 置位原子标志（release）；引擎线程看到标志（acquire）后继续处理，直到完整一轮发现
//   所有入站队列都为空才退出，stop() 随后 join 该线程。在 stop() 之前已入队（happens-before）的命令
//   全部会被处理；调用者应先让网关停止发送，与 stop() 并发入队的命令不保证被处理。
// - 撮合引擎监听器的回调只把事件写入出站队列，从不重入调用 submit/cancel/modify。
// - 引擎线程内的异常（例如活动订单超出预留容量后扩容时的 std::bad_alloc）无法恢复：线程函数为
//   noexcept，异常会调用 std::terminate。预留足够的容量（addSymbol 的 reserve_orders）即可让热路径
//   完全不分配内存。
// - One pair of SPSC queues per gateway: an inbound command queue (the gateway thread is the only
//   producer, the engine thread the only consumer) and an outbound event queue (the engine thread is the
//   only producer, the gateway thread the only consumer). There are no locks, and no thread other than
//   the engine thread touches the matching state.
// - The engine thread polls (round-robin) the inbound queues: per round it processes at most batch
//   consecutive commands from each queue (batched consumption amortizes the cost of reading the other
//   side's cache line while still guaranteeing fairness), then moves on to the next queue; when a whole
//   round finds no commands it busy-waits with cpuRelax() (optionally std::this_thread::yield() after a
//   configurable number of consecutive idle spins, for machines without enough CPUs to go around).
//   Optionally pinned to a CPU.
// - Engine sequence number engine_seq: incremented by 1 for every command processed (starting at 1, and
//   it keeps increasing after a stop()/start() restart), i.e. the global processing order. All events
//   produced by one command carry that command's engine_seq, client_ts and sending gateway id.
// - Event routing ("an order belongs to the gateway that submitted it"):
//   * Accepted / Rejected / Modified, Cancelled for the command's own order, the taker-side Fill of a
//     trade → the gateway that sent the command;
//   * the maker-side Fill of a trade → the gateway that owns the maker order (when both sides belong to
//     the same gateway, that gateway receives two Fills, maker first, then taker); a maker cancelled by
//     self-trade prevention (STP) → the gateway that owns the maker.
//   Owner map: one hft::FlatHashMap<OrderId, GatewayId> per symbol, holding exactly that symbol's resting
//   orders (checkInvariants() verifies the one-to-one correspondence with the order book). It is
//   maintained entirely by inference from the events, with no extra order book lookups on the hot path:
//   an entry is inserted when a new limit GTC order still has a remainder after matching (so it rests),
//   and removed when a maker is fully filled, cancelled by STP or cancelled by a cancel command, or
//   when a modified order is fully filled as a taker / cancelled by STP.
// - Session isolation: when the target order of a cancel / modify rests in the book but belongs to
//   another gateway, the runner replies Rejected(UnknownOrder) directly without handing it to the
//   matching engine — as on a real exchange, a session can neither see nor touch another session's
//   orders (for that gateway this is indistinguishable from the order not existing). A new order id that
//   duplicates a live order of another gateway is rejected by the engine as usual (DuplicateOrderId).
//   It follows that every command produces at least one event for its sending gateway, and that a
//   gateway receives the first event of each command in the order it sent the commands (gateways use
//   this to acknowledge in-flight commands and to measure round-trip latency).
// - Back-pressure: when an outbound queue is full the engine thread spins in place (cpuRelax,
//   configurable yield) until the consumer frees up space; events are never dropped. The cost is that
//   one gateway that does not consume stalls the whole engine, so gateway threads must keep consuming
//   their own event queue, including while waiting for space in the inbound queue (otherwise
//   "inbound full + outbound full" wait on each other and deadlock) and while stop() is in progress.
// - Stop: stop() sets an atomic flag (release); after seeing the flag (acquire) the engine thread keeps
//   processing and exits only once a complete round finds all inbound queues empty, after which stop()
//   joins the thread. All commands enqueued before stop() (happens-before) are processed; callers should
//   first make the gateways stop sending, since commands enqueued concurrently with stop() are not
//   guaranteed to be processed.
// - The matching engine listener's callbacks only write events to the outbound queues and never
//   re-enter submit/cancel/modify.
// - An exception on the engine thread (e.g. std::bad_alloc when growing after the live orders exceed
//   the reserved capacity) is unrecoverable: the thread function is noexcept, so the exception calls
//   std::terminate. Reserving enough capacity (addSymbol's reserve_orders) keeps the hot path completely
//   allocation-free.
// ============================================================================

using GatewayId = std::uint16_t;

// 网关数上限：GatewayId 能表示的全部取值
// Maximum number of gateways: every value a GatewayId can represent
inline constexpr std::size_t kMaxGateways = std::size_t{std::numeric_limits<GatewayId>::max()} + 1;

enum class EventType : std::uint8_t { Accepted, Rejected, Fill, Cancelled, Modified };

// 成交中本订单的角色：Maker = 挂单方（提供流动性），Taker = 主动方（消耗流动性）
// Role of this order in a fill: Maker = the resting side (provides liquidity),
// Taker = the aggressive side (consumes liquidity)
enum class Liquidity : std::uint8_t { None, Maker, Taker };

constexpr const char* toString(EventType v) noexcept {
    switch (v) {
        case EventType::Accepted: return "Accepted";
        case EventType::Rejected: return "Rejected";
        case EventType::Fill: return "Fill";
        case EventType::Cancelled: return "Cancelled";
        case EventType::Modified: return "Modified";
    }
    return "?";
}

constexpr const char* toString(Liquidity v) noexcept {
    switch (v) {
        case Liquidity::None: return "None";
        case Liquidity::Maker: return "Maker";
        case Liquidity::Taker: return "Taker";
    }
    return "?";
}

// ============================================================================
// 网关 → 引擎的命令：64 字节（一个缓存行，独占一个队列槽位，生产者与消费者不会在相邻槽位上伪共享）。
// 扁平 POD：New 使用全部字段，Cancel 只用 order_id，Modify 用 order_id / price / qty。
// Gateway → engine command: 64 bytes (one cache line, so it occupies a queue slot of its own and the
// producer and consumer never false-share adjacent slots).
// Flat POD: New uses all fields, Cancel only order_id, Modify order_id / price / qty.
// ============================================================================
struct alignas(kCacheLineSize) EngineCommand {
    std::uint64_t client_ts;   // 网关发送时的 TSC 读数（hft::rdtsc()），原样回显在该命令产生的每个事件里
                               // TSC reading when the gateway sent it (hft::rdtsc()),
                               // echoed unchanged in every event this command produces
    std::uint64_t seq;         // 每网关命令序号 1, 2, 3, ...（引擎统计不连续的次数，但不拒绝命令）
                               // per-gateway command sequence number 1, 2, 3, ...
                               // (the engine counts discontinuities but does not reject the command)
    OrderId order_id;          // New：新订单 id；Cancel / Modify：目标订单 id
                               // New: new order id; Cancel / Modify: target order id
    Price price;               // New：限价（市价单忽略）；Modify：新价格
                               // New: limit price (ignored for market orders); Modify: new price
    Qty qty;                   // New：数量；Modify：新的剩余（未成交）数量
                               // New: quantity; Modify: new remaining (unfilled) quantity
    SymbolId symbol;           // 交易对（MatchingEngine::addSymbol 的返回值）
                               // symbol (the return value of MatchingEngine::addSymbol)
    AccountId account;         // New：账户（用于自成交保护）
                               // New: account (for self-trade prevention)
    GatewayId gateway;         // 发送方网关；仅作记录，引擎以命令所在入站队列的下标为准（网关无法冒充别人）
                               // sending gateway; for the record only: the engine goes by the index of the
                               // inbound queue the command is in (a gateway cannot impersonate another)
    RequestType type;          // New / Cancel / Modify（其他取值按 InvalidFlags 拒绝）
                               // New / Cancel / Modify (other values are rejected as InvalidFlags)
    Side side;                 // New
    OrderType order_type;      // New
    TimeInForce tif;           // New
    bool post_only;            // New
    StpMode stp;               // New

    // 命令工厂（未用到的字段为 0）
    // Command factories (unused fields are 0)
    static EngineCommand newOrder(GatewayId gateway, std::uint64_t seq, SymbolId symbol, const OrderRequest& req,
                                  std::uint64_t client_ts = 0) noexcept {
        EngineCommand c = make(RequestType::New, gateway, seq, symbol, req.id, client_ts);
        c.price = req.price;
        c.qty = req.qty;
        c.account = req.account;
        c.side = req.side;
        c.order_type = req.type;
        c.tif = req.tif;
        c.post_only = req.post_only;
        c.stp = req.stp;
        return c;
    }

    static EngineCommand cancel(GatewayId gateway, std::uint64_t seq, SymbolId symbol, OrderId id,
                                std::uint64_t client_ts = 0) noexcept {
        return make(RequestType::Cancel, gateway, seq, symbol, id, client_ts);
    }

    static EngineCommand modify(GatewayId gateway, std::uint64_t seq, SymbolId symbol, OrderId id, Price new_price,
                                Qty new_qty, std::uint64_t client_ts = 0) noexcept {
        EngineCommand c = make(RequestType::Modify, gateway, seq, symbol, id, client_ts);
        c.price = new_price;
        c.qty = new_qty;
        return c;
    }

    // New 命令对应的订单请求
    // The order request of a New command
    OrderRequest request() const noexcept {
        return OrderRequest{order_id, account, side, order_type, tif, price, qty, post_only, stp};
    }

private:
    static EngineCommand make(RequestType type, GatewayId gateway, std::uint64_t seq, SymbolId symbol, OrderId id,
                              std::uint64_t client_ts) noexcept {
        EngineCommand c{};
        c.client_ts = client_ts;
        c.seq = seq;
        c.order_id = id;
        c.symbol = symbol;
        c.gateway = gateway;
        c.type = type;
        return c;
    }
};

// ============================================================================
// 引擎 → 网关的事件：64 字节 = 32 字节公共头 + 32 字节按类型解释的载荷（union）。
// 成交按"每个订单一条执行回报"发送（与 FIX ExecutionReport 一致）：maker、taker 各收到一条 Fill，
// 内容是本方订单视角（本方订单 id、成交价量、本方剩余量、Maker/Taker）。不披露对手方的订单 id 与账户
// （真实交易所也不披露）；本方订单的方向、账户网关自己知道（Accepted 回显了完整请求）。
// 这样每条事件恰好一个缓存行；把完整的 Trade（72 字节）塞进事件则需要两个缓存行。
// Engine → gateway event: 64 bytes = 32-byte common header + 32-byte payload interpreted by type (union).
// Fills are sent as "one execution report per order" (as in FIX ExecutionReport): the maker and the taker
// each receive one Fill, from the point of view of their own order (own order id, fill price and
// quantity, own remaining quantity, Maker/Taker). The counterparty's order id and account are not
// disclosed (real exchanges do not disclose them either); the gateway itself knows its own order's side
// and account (Accepted echoed the full request).
// This way every event is exactly one cache line; putting the full Trade (72 bytes) into the event would
// take two cache lines.
// ============================================================================
struct AcceptedInfo {      // 新订单被接受：原样回显请求（订单 id 在事件头）
                           // new order accepted: the request echoed unchanged
                           // (the order id is in the event header)
    Price price;
    Qty qty;
    AccountId account;
    Side side;
    OrderType order_type;
    TimeInForce tif;
    bool post_only;
    StpMode stp;
};

struct RejectedInfo {      // 请求被拒（订单 id 在事件头）
                           // request rejected (the order id is in the event header)
    RequestType request;
    RejectReason reason;
};

struct FillInfo {          // 本方订单成交：成交 id（每个交易对从 1 递增）、成交价（恒为 maker 价）、成交量、本方剩余量
                           // own order filled: trade id (increasing from 1 per symbol), fill price
                           // (always the maker price), fill quantity, own remaining quantity
    TradeId trade_id;
    Price price;
    Qty qty;
    Qty remaining;         // 0 表示本方订单已完全成交（不再另发事件）
                           // 0 = own order fully filled (no further event is sent)
};

struct CancelledInfo {     // 订单（剩余部分）被撤销
                           // order (its remainder) cancelled
    Qty qty;               // 被撤销的数量
                           // cancelled quantity
    CancelReason reason;
};

struct ModifiedInfo {      // 改单生效（之后可能紧跟作为 taker 的 Fill / Cancelled）
                           // modify took effect (may be followed directly by Fill / Cancelled as a taker)
    Price price;
    Qty qty;
};

struct alignas(kCacheLineSize) EngineEvent {
    std::uint64_t engine_seq;  // 产生本事件的命令的引擎序号（全局处理顺序）
                               // engine sequence number of the command that produced this event
                               // (global processing order)
    std::uint64_t client_ts;   // 该命令的 client_ts（原样回显）
                               // that command's client_ts (echoed unchanged)
    OrderId order_id;          // 本事件涉及的订单（Fill：接收方自己的订单）
                               // the order this event concerns (Fill: the receiver's own order)
    SymbolId symbol;           // 交易对（UnknownSymbol 拒单时回显命令中的值）
                               // symbol (an UnknownSymbol reject echoes the value from the command)
    GatewayId gateway;         // 发出该命令的网关（maker 收到的 Fill 中是 taker 的网关）
                               // gateway that sent the command (in the Fill a maker receives,
                               // this is the taker's gateway)
    EventType type;
    Liquidity liquidity;       // 仅 Fill：Maker / Taker；其他事件为 None
                               // Fill only: Maker / Taker; None for other events
    union {                    // 按 type 选择有效成员
                               // type selects the active member
        AcceptedInfo accepted;
        RejectedInfo rejected;
        FillInfo fill;
        CancelledInfo cancelled;
        ModifiedInfo modified;
    };
};

static_assert(sizeof(EngineCommand) == kCacheLineSize, "EngineCommand must fill exactly one cache line");
static_assert(sizeof(EngineEvent) == kCacheLineSize, "EngineEvent must fill exactly one cache line");
static_assert(std::is_trivially_copyable_v<EngineCommand> && std::is_trivial_v<EngineCommand> &&
                  std::is_standard_layout_v<EngineCommand>,
              "EngineCommand must be a POD (it is copied bitwise through SPSC queues)");
static_assert(std::is_trivially_copyable_v<EngineEvent> && std::is_trivial_v<EngineEvent> &&
                  std::is_standard_layout_v<EngineEvent>,
              "EngineEvent must be a POD (it is copied bitwise through SPSC queues)");

// 逐字段相等：公共头全部字段 + 当前类型的有效载荷（未知类型只比较公共头）
// Field-by-field equality: all common header fields + the payload that is valid for the current type
// (an unknown type compares only the common header)
inline bool operator==(const EngineEvent& a, const EngineEvent& b) noexcept {
    if (a.engine_seq != b.engine_seq || a.client_ts != b.client_ts || a.order_id != b.order_id ||
        a.symbol != b.symbol || a.gateway != b.gateway || a.type != b.type || a.liquidity != b.liquidity)
        return false;
    switch (a.type) {
        case EventType::Accepted:
            return a.accepted.price == b.accepted.price && a.accepted.qty == b.accepted.qty &&
                   a.accepted.account == b.accepted.account && a.accepted.side == b.accepted.side &&
                   a.accepted.order_type == b.accepted.order_type && a.accepted.tif == b.accepted.tif &&
                   a.accepted.post_only == b.accepted.post_only && a.accepted.stp == b.accepted.stp;
        case EventType::Rejected:
            return a.rejected.request == b.rejected.request && a.rejected.reason == b.rejected.reason;
        case EventType::Fill:
            return a.fill.trade_id == b.fill.trade_id && a.fill.price == b.fill.price && a.fill.qty == b.fill.qty &&
                   a.fill.remaining == b.fill.remaining;
        case EventType::Cancelled:
            return a.cancelled.qty == b.cancelled.qty && a.cancelled.reason == b.cancelled.reason;
        case EventType::Modified:
            return a.modified.price == b.modified.price && a.modified.qty == b.modified.qty;
    }
    return true;
}

inline bool operator!=(const EngineEvent& a, const EngineEvent& b) noexcept { return !(a == b); }

// ============================================================================
// 命令日志（journal）概念：引擎线程在处理每条命令之前、按处理顺序调用
// Command journal concept: the engine thread calls, before each command and in processing order,
//   void onCommand(std::uint64_t engine_seq, GatewayId gateway, const EngineCommand& cmd);
// gateway 是命令所在入站队列的下标（权威发送方）；cmd 引用队列槽位，只在调用期间有效。
// 用于顺序日志 / 确定性回放（同一顺序在单线程引擎上重放得到完全相同的事件）。必须非常快且不抛异常。
// gateway is the index of the inbound queue the command is in (the authoritative sender); cmd refers to
// the queue slot and is valid only during the call.
// Used for sequential journaling / deterministic replay (replaying in the same order on a single-threaded
// engine yields exactly the same events). Must be very fast and must not throw.
// ============================================================================
struct NullJournal {
    void onCommand(std::uint64_t, GatewayId, const EngineCommand&) noexcept {}
};

struct EngineRunnerConfig {
    std::size_t gateways = 1;                   // 网关数，[1, kMaxGateways]
                                                // number of gateways, [1, kMaxGateways]
    std::size_t inbound_capacity = 1024;        // 每个网关入站命令队列容量（向上取整到 2 的幂，最小 2）
                                                // per-gateway inbound command queue capacity
                                                // (rounded up to a power of two, minimum 2)
    std::size_t outbound_capacity = 4096;       // 每个网关出站事件队列容量（同上）
                                                // per-gateway outbound event queue capacity (same as above)
    std::size_t batch = 32;                     // 每轮每个入站队列最多连续处理的命令数（>= 1）
                                                // max consecutive commands per inbound queue per round (>= 1)
    int engine_cpu = -1;                        // 引擎线程绑定的 CPU 编号（< 0 = 不绑核）
                                                // CPU the engine thread is pinned to (< 0 = do not pin)
    std::uint32_t idle_spins_before_yield = 0;  // 连续空转（无命令 / 出站队列满）多少次后 yield 一次（0 = 纯忙等）
                                                // yield once after this many consecutive idle spins
                                                // (no command / outbound queue full) (0 = pure busy-wait)
    std::string thread_name = "hft-engine";     // 引擎线程名（Linux 上限 15 个字符；空串 = 不设置）
                                                // engine thread name (Linux limit 15 characters; empty = not set)
};

// 统计快照（引擎线程单写的原子计数器，任意线程可读；运行中读到的是近似值。commands 用 release/acquire，
// 读到 n 时前 n 条命令的事件均已可见，见 processedCommands()；其余为 relaxed）
// Statistics snapshot (atomic counters written only by the engine thread, readable from any thread; values
// read while running are approximate. commands uses release/acquire: once a reader sees n, the events of
// the first n commands are all visible, see processedCommands(); the rest are relaxed)
struct EngineRunnerStats {
    std::uint64_t commands = 0;         // 已处理的命令数（= 最近分配的引擎序号）
                                        // commands processed (= the most recently assigned engine sequence number)
    std::uint64_t events = 0;           // 已发布的事件数
                                        // events published
    std::uint64_t outbound_full = 0;    // 发布事件时出站队列已满、引擎因此自旋等待的次数（背压）
                                        // times an outbound queue was full when publishing an event,
                                        // so the engine spun waiting (back-pressure)
    std::uint64_t sequence_gaps = 0;    // 网关命令序号不是"上一条 + 1"的命令数
                                        // commands whose gateway sequence number was not "previous + 1"
    std::uint64_t foreign_rejects = 0;  // 撤 / 改其他网关的订单而被拒的命令数
                                        // commands rejected for cancelling / modifying another gateway's order
    std::uint64_t idle_polls = 0;       // 一整轮都没有任何命令的轮询次数
                                        // polling rounds that found no command at all
    std::uint64_t orphan_events = 0;    // 找不到所属网关的 maker 事件数（归属表与订单簿不一致；恒应为 0）
                                        // maker events whose owning gateway was not found (owner map out of
                                        // sync with the order book; should always be 0)
};

template <class Journal = NullJournal>
class EngineRunner {
    class Listener;

public:
    using CommandQueue = SpscQueue<EngineCommand>;
    using EventQueue = SpscQueue<EngineEvent>;
    using Engine = MatchingEngine<Listener>;
    using Book = typename Engine::Book;
    static constexpr std::size_t kDefaultReserveOrders = Engine::kDefaultReserveOrders;

    explicit EngineRunner(const EngineRunnerConfig& config = EngineRunnerConfig(), Journal journal = Journal())
        : config_(validated(config)), journal_(std::move(journal)), listener_(*this), engine_(listener_) {
        const std::size_t n = config_.gateways;
        inbound_.reserve(n);
        outbound_.reserve(n);
        for (std::size_t g = 0; g < n; ++g) {
            inbound_.push_back(std::make_unique<CommandQueue>(config_.inbound_capacity));
            outbound_.push_back(std::make_unique<EventQueue>(config_.outbound_capacity));
        }
        next_seq_.assign(n, 1);
    }

    // 析构时若引擎线程仍在运行则先 stop()（排空入站队列；网关此时仍须消费事件）
    // On destruction, stop() first if the engine thread is still running (drains the inbound queues; the
    // gateways must still consume events at that point)
    ~EngineRunner() { stop(); }

    EngineRunner(const EngineRunner&) = delete;
    EngineRunner& operator=(const EngineRunner&) = delete;
    EngineRunner(EngineRunner&&) = delete;
    EngineRunner& operator=(EngineRunner&&) = delete;

    // ---------------------------------------------------------------- 初始化（引擎线程未运行时）
    // 添加交易对，返回其 id（0, 1, 2, ...）。reserve_orders 同时用于订单簿与归属表的预留容量：
    // 活动订单数不超过它时热路径零分配。名称重复时抛 std::invalid_argument（运行器不变）。
    // ---------------------------------------------------------------- Setup (while the engine thread is not running)
    // Adds a symbol and returns its id (0, 1, 2, ...). reserve_orders is the reserved capacity of both the
    // order book and the owner map: while live orders do not exceed it, the hot path is allocation-free.
    // A duplicate name throws std::invalid_argument (the runner is left unchanged).
    SymbolId addSymbol(std::string name, std::size_t max_orders = 0,
                       std::size_t reserve_orders = kDefaultReserveOrders) {
        if (running()) throw std::logic_error("EngineRunner::addSymbol: engine thread is running");
        owners_.reserve(owners_.size() + 1);  // 之后的 push_back 不会抛异常
                                              // so the push_back below cannot throw
        OwnerMap owners(reserve_orders);      // 先分配归属表：失败时引擎尚未改变
                                              // allocate the owner map first: on failure the engine is unchanged
        const SymbolId id = engine_.addSymbol(std::move(name), max_orders, reserve_orders);
        owners_.push_back(std::move(owners));  // FlatHashMap 的移动构造是 noexcept
                                               // FlatHashMap's move constructor is noexcept
        return id;
    }

    // ---------------------------------------------------------------- 生命周期（控制线程）
    // 启动引擎线程；已在运行时抛 std::logic_error。stop() 之后可以再次 start()（状态与序号延续）。
    // ---------------------------------------------------------------- Lifecycle (control thread)
    // Starts the engine thread; throws std::logic_error if it is already running. start() may be called again
    // after stop() (state and sequence numbers carry over).
    void start() {
        if (thread_.joinable()) throw std::logic_error("EngineRunner::start: engine thread is already running");
        stop_requested_.store(false, std::memory_order_relaxed);
        thread_ = std::thread([this]() noexcept { run(); });  // 线程创建之前的全部写入对引擎线程可见
                                                              // all writes made before creating the thread
                                                              // are visible to the engine thread
    }

    // 请求停止并等待：引擎线程处理完所有入站队列中的命令后退出。未运行时为空操作。
    // Requests a stop and waits: the engine thread exits after processing the commands in all inbound queues.
    // A no-op when not running.
    void stop() {
        if (!thread_.joinable()) return;
        stop_requested_.store(true, std::memory_order_release);
        thread_.join();
    }

    // 引擎线程是否在运行（控制线程调用）
    // Whether the engine thread is running (called from the control thread)
    bool running() const noexcept { return thread_.joinable(); }

    // ---------------------------------------------------------------- 网关端
    // 网关 g 的入站命令队列：只能由网关 g 的线程 tryPush（单生产者）
    // ---------------------------------------------------------------- Gateway side
    // Inbound command queue of gateway g: only gateway g's thread may tryPush (single producer)
    CommandQueue& inbound(GatewayId g) { return *inbound_.at(g); }
    // 网关 g 的出站事件队列：只能由网关 g 的线程 front/pop/tryPop（单消费者）
    // Outbound event queue of gateway g: only gateway g's thread may front/pop/tryPop (single consumer)
    EventQueue& outbound(GatewayId g) { return *outbound_.at(g); }

    std::size_t gatewayCount() const noexcept { return inbound_.size(); }
    std::size_t symbolCount() const noexcept { return owners_.size(); }

    // ---------------------------------------------------------------- 统计（任意线程）
    // 已处理的命令数（acquire）：读到 n 时，前 n 条命令产生的全部事件都已写入出站队列、对读者可见。
    // 同步驱动（测试、单步工具）据此等待一条命令处理完毕再读取它的事件。
    // ---------------------------------------------------------------- Statistics (any thread)
    // Number of commands processed (acquire): once a reader sees n, all events produced by the first n
    // commands have been written to the outbound queues and are visible to the reader.
    // Synchronous drivers (tests, single-step tools) use this to wait until a command has been processed
    // before reading its events.
    std::uint64_t processedCommands() const noexcept { return processed_.load(std::memory_order_acquire); }

    EngineRunnerStats stats() const noexcept {
        EngineRunnerStats s;
        s.commands = processed_.load(std::memory_order_acquire);
        s.events = events_.load(std::memory_order_relaxed);
        s.outbound_full = outbound_full_.load(std::memory_order_relaxed);
        s.sequence_gaps = sequence_gaps_.load(std::memory_order_relaxed);
        s.foreign_rejects = foreign_rejects_.load(std::memory_order_relaxed);
        s.idle_polls = idle_polls_.load(std::memory_order_relaxed);
        s.orphan_events = orphan_events_.load(std::memory_order_relaxed);
        return s;
    }

    // 引擎线程是否成功绑核（start() 之后由引擎线程写入；在收到引擎的任何事件或 stop() 之后读取才可靠）
    // Whether the engine thread was pinned to a CPU successfully (written by the engine thread after start();
    // reliable only when read after receiving any event from the engine, or after stop())
    bool pinned() const noexcept { return pinned_.load(std::memory_order_relaxed); }

    // ---------------------------------------------------------------- 引擎状态（仅在引擎线程未运行时访问）
    // -------------------------------------------- Engine state (access only while the engine thread is not running)
    const Engine& engine() const noexcept { return engine_; }

    // 订单所属网关（不在簿上返回空）
    // Gateway that owns the order (empty if the order is not in the book)
    std::optional<GatewayId> ownerOf(SymbolId symbol, OrderId id) const noexcept {
        if (symbol >= owners_.size()) return std::nullopt;
        const GatewayId* g = owners_[symbol].find(id);
        if (g == nullptr) return std::nullopt;
        return *g;
    }

    // 不变量检查（O(订单数)，引擎线程未运行时调用）：每个订单簿的 checkInvariants()；归属表自身的不变量；
    // 归属表与订单簿一一对应（条目数 == 挂单数，每个条目都是正在挂单的订单，网关 id 合法）。
    // Invariant check (O(orders), call while the engine thread is not running): checkInvariants() of every
    // order book; the owner maps' own invariants; one-to-one correspondence between the owner maps and the
    // order books (entry count == resting order count, every entry is a resting order, valid gateway ids).
    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](std::string msg) {
            if (why != nullptr) *why = std::move(msg);
            return false;
        };
        if (running()) return fail("checkInvariants() called while the engine thread is running");
        if (owners_.size() != engine_.symbolCount())
            return fail("owner maps (" + std::to_string(owners_.size()) + ") do not match symbols (" +
                        std::to_string(engine_.symbolCount()) + ")");
        for (SymbolId s = 0; s < owners_.size(); ++s) {
            const Book& book = engine_.book(s);
            const std::string name = engine_.symbolName(s);
            std::string inner;
            if (!book.checkInvariants(&inner)) return fail(name + ": " + inner);
            const OwnerMap& owners = owners_[s];
            if (!owners.checkInvariants(&inner)) return fail(name + ": owner map: " + inner);
            if (owners.size() != book.orderCount())
                return fail(name + ": owner map holds " + std::to_string(owners.size()) + " orders but " +
                            std::to_string(book.orderCount()) + " rest in the book");
            std::string bad;
            owners.forEach([&](OrderId id, const GatewayId& g) {
                if (!bad.empty()) return;
                if (g >= gatewayCount())
                    bad = "order " + std::to_string(id) + " is owned by unknown gateway " + std::to_string(g);
                else if (!book.findOrder(id))
                    bad = "owner map lists order " + std::to_string(id) + " which is not resting";
            });
            if (!bad.empty()) return fail(name + ": " + bad);
        }
        return true;
    }

private:
    using OwnerMap = FlatHashMap<OrderId, GatewayId>;

    // 撮合引擎监听器：把回调转给运行器（运行器持有当前命令的上下文）。
    // onAccepted / onModified 几乎每条命令都会走到：整条转发链强制内联，否则 submit/modify 体积变大后
    // 编译器会把它留成函数调用（实测 exchange_sim 吞吐因此下降约 3%）
    // Matching engine listener: forwards the callbacks to the runner (the runner holds the current command's
    // context).
    // Almost every command reaches onAccepted / onModified: the whole forwarding chain is force-inlined,
    // otherwise once submit/modify grow larger the compiler leaves it as a function call (measured:
    // exchange_sim throughput drops by about 3% because of it)
    class Listener {
    public:
        explicit Listener(EngineRunner& runner) noexcept : runner_(&runner) {}
        HFT_ALWAYS_INLINE void onAccepted(SymbolId s, const OrderRequest& req) { runner_->handleAccepted(s, req); }
        void onRejected(SymbolId s, OrderId id, RequestType request, RejectReason reason) {
            runner_->publishRejected(s, id, request, reason);
        }
        void onTrade(SymbolId s, const Trade& trade) { runner_->handleTrade(s, trade); }
        void onCancelled(SymbolId s, OrderId id, Qty qty, CancelReason reason) {
            runner_->handleCancelled(s, id, qty, reason);
        }
        HFT_ALWAYS_INLINE void onModified(SymbolId s, OrderId id, Price price, Qty qty) {
            runner_->handleModified(s, id, price, qty);
        }

    private:
        EngineRunner* runner_;
    };

    // 正在处理的命令（只有引擎线程访问）
    // The command being processed (accessed only by the engine thread)
    struct CommandContext {
        std::uint64_t engine_seq = 0;
        std::uint64_t client_ts = 0;
        OrderId order_id = 0;      // 命令自身的订单
                                   // the command's own order
        Qty remaining = 0;         // 命令自身订单的剩余量（Accepted / Modified 设置，taker 成交后更新）
                                   // remaining quantity of the command's own order
                                   // (set by Accepted / Modified, updated after taker fills)
        GatewayId gateway = 0;     // 发送网关（入站队列下标）
                                   // sending gateway (inbound queue index)
        bool accepted = false;     // New 被接受 / Modify 已生效
                                   // New accepted / Modify took effect
        bool rests = false;        // New 是限价 GTC（撮合后若有剩余就会挂单）
                                   // New is a limit GTC order (rests if anything remains after matching)
        bool terminated = false;   // 命令自身订单收到了 Cancelled（撤单成功、IOC/市价剩余、FOK、STP）
                                   // the command's own order received Cancelled
                                   // (successful cancel, IOC/market remainder, FOK, STP)
    };

    static EngineRunnerConfig validated(const EngineRunnerConfig& c) {
        if (c.gateways == 0 || c.gateways > kMaxGateways)
            throw std::invalid_argument("EngineRunner: gateways must be in [1, " + std::to_string(kMaxGateways) +
                                        "], got " + std::to_string(c.gateways));
        if (c.batch == 0) throw std::invalid_argument("EngineRunner: batch must be >= 1");
        return c;
    }

    // 单写者计数器：relaxed 读 + 写（无 RMW 指令）
    // Single-writer counter: relaxed load + store (no RMW instruction)
    HFT_ALWAYS_INLINE static void bump(std::atomic<std::uint64_t>& counter) noexcept {
        counter.store(counter.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    }

    // 空转一次：cpuRelax；配置了 yield 阈值时每连续空转 yield_after 次让出一次 CPU
    // One idle spin: cpuRelax; when a yield threshold is configured, yields the CPU once every yield_after
    // consecutive idle spins
    static void idlePause(std::uint32_t& spins, std::uint32_t yield_after) noexcept {
        if (yield_after != 0 && ++spins >= yield_after) {
            spins = 0;
            std::this_thread::yield();
        } else {
            cpuRelax();
        }
    }

    // ---------------------------------------------------------------- 引擎线程
    // ---------------------------------------------------------------- Engine thread
    void run() noexcept {
        if (!config_.thread_name.empty()) setThreadName(config_.thread_name.c_str());
        if (config_.engine_cpu >= 0) pinned_.store(pinThreadToCpu(config_.engine_cpu), std::memory_order_relaxed);
        const std::unique_ptr<CommandQueue>* const queues = inbound_.data();
        const std::size_t n = inbound_.size();
        const std::size_t batch = config_.batch;
        const std::uint32_t yield_after = config_.idle_spins_before_yield;
        std::uint32_t spins = 0;
        for (;;) {
            // 先读停止标志再扫一整轮：标志置位之前入队的命令在这一轮里一定可见；整轮为空才退出
            // Read the stop flag first, then scan a full round: commands enqueued before the flag was set are
            // guaranteed to be visible in this round; exit only if the whole round was empty
            const bool stopping = stop_requested_.load(std::memory_order_acquire);
            bool any = false;
            for (std::size_t g = 0; g < n; ++g) {
                CommandQueue& q = *queues[g];
                for (std::size_t k = 0; k < batch; ++k) {
                    const EngineCommand* const cmd = q.front();
                    if (cmd == nullptr) break;
                    process(static_cast<GatewayId>(g), *cmd);  // 直接读槽位，pop 之前生产者不会覆盖它
                                                               // read the slot in place; the producer cannot
                                                               // overwrite it before pop
                    q.pop();
                    any = true;
                }
            }
            if (any) {
                spins = 0;
                continue;
            }
            if (stopping) break;
            bump(idle_polls_);
            idlePause(spins, yield_after);
        }
    }

    HFT_ALWAYS_INLINE void process(GatewayId g, const EngineCommand& cmd) {
        const std::uint64_t seq = ++engine_seq_;
        std::uint64_t& expected = next_seq_[g];
        if (HFT_UNLIKELY(cmd.seq != expected)) bump(sequence_gaps_);
        expected = cmd.seq + 1;
        journal_.onCommand(seq, g, cmd);

        ctx_ = CommandContext{seq, cmd.client_ts, cmd.order_id, 0, g, false, false, false};

        switch (cmd.type) {
            case RequestType::New: processNew(cmd); break;
            case RequestType::Cancel: processCancel(cmd); break;
            case RequestType::Modify: processModify(cmd); break;
            default:  // 只能由强制类型转换构造的非法命令类型：拒绝，不交给引擎
                      // invalid command type that only a forced cast can produce:
                      // reject it without passing it to the engine
                publishRejected(cmd.symbol, cmd.order_id, cmd.type, RejectReason::InvalidFlags);
                break;
        }
        processed_.store(seq, std::memory_order_release);  // 在本命令的全部事件之后发布（见 processedCommands）
                                                           // published after all of this command's events
                                                           // (see processedCommands)
    }

    void processNew(const EngineCommand& cmd) {
        const SymbolId s = cmd.symbol;
        if (HFT_LIKELY(s < owners_.size()) && cmd.order_type == OrderType::Limit && cmd.tif == TimeInForce::GTC) {
            // 可能挂单：先保证归属表插入不需要扩容（扩容抛 bad_alloc 时尚无任何副作用）
            // May rest: first make sure the owner map insert will not need to grow (if growing throws bad_alloc,
            // there are no side effects yet)
            OwnerMap& owners = owners_[s];
            owners.reserve(owners.size() + 1);
        }
        engine_.submit(s, cmd.request());
        if (ctx_.accepted && ctx_.rests && !ctx_.terminated && ctx_.remaining > 0) {
            // 接受时该 id 不在簿上，因此归属表中也没有它
            // When accepted, this id was not in the book, so it is not in the owner map either
            const bool inserted = owners_[s].insert(cmd.order_id, ctx_.gateway).second;
            HFT_ASSERT(inserted && "new resting order already has an owner");
            static_cast<void>(inserted);
        }
    }

    void processCancel(const EngineCommand& cmd) {
        const GatewayId* const owner = ownerSlot(cmd.symbol, cmd.order_id);
        if (HFT_UNLIKELY(ownedByOther(owner))) return rejectForeign(cmd);
        engine_.cancel(cmd.symbol, cmd.order_id);
        // 撤单成功：订单此前挂在簿上，归属条目就是 owner（撤单不改动归属表，指针仍然有效）
        // Cancel succeeded: the order was resting in the book, so its owner entry is owner (a cancel does not
        // change the owner map, so the pointer is still valid)
        if (ctx_.terminated) owners_[cmd.symbol].eraseAt(owner);
    }

    void processModify(const EngineCommand& cmd) {
        if (HFT_UNLIKELY(ownedByOther(ownerSlot(cmd.symbol, cmd.order_id)))) return rejectForeign(cmd);
        engine_.modify(cmd.symbol, cmd.order_id, cmd.price, cmd.qty);
        // 改价 / 加量后作为 taker 完全成交或被 STP 撤销：订单已离开订单簿（撮合会删除 maker 的归属条目，按 id 删除）
        // After a reprice / quantity increase, fully filled as a taker or cancelled by STP: the order has left the
        // order book (matching erases makers' owner entries, so erase by id)
        if (ctx_.accepted && (ctx_.terminated || ctx_.remaining == 0)) owners_[cmd.symbol].erase(cmd.order_id);
    }

    // 挂单的归属条目；未知交易对或订单不在簿上时为 nullptr（未知交易对交给引擎拒绝为 UnknownSymbol）
    // Owner entry of a resting order; nullptr for an unknown symbol or an order not in the book (an unknown
    // symbol is left to the engine to reject as UnknownSymbol)
    const GatewayId* ownerSlot(SymbolId s, OrderId id) const noexcept {
        return s < owners_.size() ? owners_[s].find(id) : nullptr;
    }

    bool ownedByOther(const GatewayId* owner) const noexcept { return owner != nullptr && *owner != ctx_.gateway; }

    void rejectForeign(const EngineCommand& cmd) {
        bump(foreign_rejects_);
        publishRejected(cmd.symbol, cmd.order_id, cmd.type, RejectReason::UnknownOrder);
    }

    // ---------------------------------------------------------------- 事件（引擎线程，在撮合回调内）
    // ---------------------------------------------------------------- Events (engine thread, in matching callbacks)
    EngineEvent makeEvent(EventType type, SymbolId s, OrderId id) const noexcept {
        EngineEvent ev{};  // 整个缓存行清零：填充字节也是确定的
                           // zero the whole cache line: padding bytes are deterministic too
        ev.engine_seq = ctx_.engine_seq;
        ev.client_ts = ctx_.client_ts;
        ev.order_id = id;
        ev.symbol = s;
        ev.gateway = ctx_.gateway;
        ev.type = type;
        ev.liquidity = Liquidity::None;
        return ev;
    }

    HFT_ALWAYS_INLINE void handleAccepted(SymbolId s, const OrderRequest& req) {
        ctx_.accepted = true;
        ctx_.rests = req.type == OrderType::Limit && req.tif == TimeInForce::GTC;
        ctx_.remaining = req.qty;
        EngineEvent ev = makeEvent(EventType::Accepted, s, req.id);
        ev.accepted = AcceptedInfo{req.price, req.qty, req.account, req.side, req.type, req.tif, req.post_only, req.stp};
        publish(ctx_.gateway, ev);
    }

    void publishRejected(SymbolId s, OrderId id, RequestType request, RejectReason reason) {
        EngineEvent ev = makeEvent(EventType::Rejected, s, id);
        ev.rejected = RejectedInfo{request, reason};
        publish(ctx_.gateway, ev);
    }

    void handleTrade(SymbolId s, const Trade& t) {
        // maker 一侧：发给 maker 所属网关；完全成交则移出归属表（先取出网关 id：删除会移动槽位）
        // Maker side: sent to the maker's owning gateway; removed from the owner map if fully filled (read the
        // gateway id first: erasing moves slots)
        OwnerMap& owners = owners_[s];
        const GatewayId* found = owners.find(t.maker_id);
        HFT_ASSERT(found != nullptr && "resting maker has no owning gateway");
        if (HFT_LIKELY(found != nullptr)) {
            const GatewayId maker_gateway = *found;
            if (t.maker_remaining == 0) owners.eraseAt(found);
            EngineEvent ev = makeEvent(EventType::Fill, s, t.maker_id);
            ev.liquidity = Liquidity::Maker;
            ev.fill = FillInfo{t.trade_id, t.price, t.qty, t.maker_remaining};
            publish(maker_gateway, ev);
        } else {
            bump(orphan_events_);
        }
        // taker 一侧：taker 总是当前命令的订单
        // Taker side: the taker is always the current command's order
        ctx_.remaining = t.taker_remaining;
        EngineEvent ev = makeEvent(EventType::Fill, s, t.taker_id);
        ev.liquidity = Liquidity::Taker;
        ev.fill = FillInfo{t.trade_id, t.price, t.qty, t.taker_remaining};
        publish(ctx_.gateway, ev);
    }

    void handleCancelled(SymbolId s, OrderId id, Qty qty, CancelReason reason) {
        GatewayId to = ctx_.gateway;
        if (id == ctx_.order_id) {
            // 命令自身的订单：撤单成功、IOC / 市价剩余、FOK 不能全部成交、作为 taker 被 STP 撤销。
            // （同一命令里被撤的 maker 不可能与它同 id：新订单 id 不在簿上，改单的订单撮合前已离开订单簿）
            // The command's own order: successful cancel, IOC / market remainder, FOK that cannot fill completely,
            // cancelled by STP as a taker.
            // (A maker cancelled in the same command cannot have the same id: a new order's id is not in the book,
            // and a modified order has left the order book before matching)
            ctx_.terminated = true;
        } else {
            // 被 STP 撤掉的 maker：发给其所属网关并移出归属表
            // A maker cancelled by STP: sent to its owning gateway and removed from the owner map
            OwnerMap& owners = owners_[s];
            const GatewayId* found = owners.find(id);
            HFT_ASSERT(found != nullptr && "cancelled maker has no owning gateway");
            if (HFT_UNLIKELY(found == nullptr)) {
                bump(orphan_events_);
                return;
            }
            to = *found;
            owners.eraseAt(found);
        }
        EngineEvent ev = makeEvent(EventType::Cancelled, s, id);
        ev.cancelled = CancelledInfo{qty, reason};
        publish(to, ev);
    }

    HFT_ALWAYS_INLINE void handleModified(SymbolId s, OrderId id, Price price, Qty qty) {
        ctx_.accepted = true;
        ctx_.remaining = qty;
        EngineEvent ev = makeEvent(EventType::Modified, s, id);
        ev.modified = ModifiedInfo{price, qty};
        publish(ctx_.gateway, ev);
    }

    // 写入网关 to 的出站队列；满了就自旋等待（背压），绝不丢弃
    // Writes to the outbound queue of gateway to; spins while it is full (back-pressure), never drops
    HFT_ALWAYS_INLINE void publish(GatewayId to, const EngineEvent& ev) {
        EventQueue& q = *outbound_[to];
        if (HFT_UNLIKELY(!q.tryPush(ev))) {
            bump(outbound_full_);
            std::uint32_t spins = 0;
            do {
                idlePause(spins, config_.idle_spins_before_yield);
            } while (!q.tryPush(ev));
        }
        bump(events_);
    }

    // ---------------------------------------------------------------- 成员
    // ---------------------------------------------------------------- Members
    EngineRunnerConfig config_;
    Journal journal_;
    Listener listener_;  // 必须先于 engine_ 构造（引擎持有它的引用）
                         // must be constructed before engine_ (the engine holds a reference to it)
    Engine engine_;
    std::vector<OwnerMap> owners_;  // [symbol]：挂单 id → 所属网关
                                    // [symbol]: resting order id → owning gateway
    std::vector<std::unique_ptr<CommandQueue>> inbound_;  // [gateway]
    std::vector<std::unique_ptr<EventQueue>> outbound_;   // [gateway]
    std::vector<std::uint64_t> next_seq_;      // [gateway]：期望的下一个网关序号（引擎线程独占）
                                               // [gateway]: expected next gateway sequence number
                                               // (engine thread only)
    CommandContext ctx_;                       // 引擎线程独占
                                               // engine thread only
    std::uint64_t engine_seq_ = 0;             // 引擎线程独占（对外发布的是 processed_）
                                               // engine thread only (what gets published is processed_)
    std::thread thread_;

    alignas(kCacheLineSize) std::atomic<bool> stop_requested_{false};
    std::atomic<bool> pinned_{false};

    // 统计：引擎线程单写，独占缓存行，避免与停止标志等控制字段伪共享
    // Statistics: written only by the engine thread, on their own cache line to avoid false sharing with
    // control fields such as the stop flag
    alignas(kCacheLineSize) std::atomic<std::uint64_t> processed_{0};
    std::atomic<std::uint64_t> events_{0};
    std::atomic<std::uint64_t> outbound_full_{0};
    std::atomic<std::uint64_t> sequence_gaps_{0};
    std::atomic<std::uint64_t> foreign_rejects_{0};
    std::atomic<std::uint64_t> idle_polls_{0};
    std::atomic<std::uint64_t> orphan_events_{0};
};

}  // namespace hft::matching
