//
// market_data_feed.h
// 行情连接管理器：每个 feed 一个线程（Boost.Beast：同步建立连接，流式阶段在本线程上驱动异步读写）
// 驱动交易所协议适配器，负责连接、心跳、退避重连、停顿踢断、REST 快照，并经 SPSC 队列向主线程发布 BookSnapshot。
// Market data connection manager: one thread per feed (Boost.Beast: connections are set up synchronously,
// and the streaming phase drives async reads and writes on that same thread). It drives the exchange
// protocol adapter, takes care of connecting, heartbeats, reconnecting with backoff, stall kicks and REST
// snapshots, and publishes BookSnapshot to the main thread over an SPSC queue.
//

#pragma once

#include "book_snapshot.h"
#include "exchanges/adapters.h"
#include "exchanges/json_util.h"
#include "hft/spsc_queue.h"
#include "hft/timing.h"
#include "latency_tracker.h"
#include "net/https_client.h"
#include "orderbook.h"

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 交易所枚举 Exchange 见 exchanges/exchange_adapter.h
// For the exchange enum Exchange, see exchanges/exchange_adapter.h

// 连接状态
// Connection status
enum class FeedStatus : int { Idle, Connecting, Syncing, Live, Reconnecting, Stopped };

inline const char* feedStatusName(FeedStatus status) noexcept {
    switch (status) {
        case FeedStatus::Idle: return "IDLE";
        case FeedStatus::Connecting: return "CONNECTING";
        case FeedStatus::Syncing: return "SYNCING";
        case FeedStatus::Live: return "LIVE";
        case FeedStatus::Reconnecting: return "RECONNECTING";
        case FeedStatus::Stopped: return "STOPPED";
    }
    return "UNKNOWN";
}

// 行情源配置
// Feed configuration
struct FeedConfig {
    Exchange exchange = Exchange::BINANCE;
    std::uint32_t venue_id = 0;                // 写入每个 BookSnapshot::venue
                                               // written into every BookSnapshot::venue
    std::string name, host, port, path, symbol, subscribe_msg;
    std::string rest_host, rest_port = "443", snapshot_path;   // Binance 增量深度模式的 REST 快照
                                                               // REST snapshot for Binance incremental depth mode
    bool tls_verify = true;          // 校验服务器证书链与主机名
                                     // verify the server certificate chain and hostname
    std::string ca_file;             // 额外信任的 CA（PEM，测试用）；"" = 只用系统默认 CA
                                     // additional trusted CA (PEM, for tests); "" = system default CAs only
    bool verify_checksum = true;     // OKX books checksum
    int stall_timeout_ms = 15000;    // 已连接但这么久没有收到任何帧（含心跳回应）→ 踢断连接（checkHealth；<= 0 关闭）
                                     // connected but no frame of any kind (heartbeat replies included) for this long
                                     // → kick the connection (checkHealth; <= 0 disables)
    int reconnect_initial_ms = 500;  // 指数退避（±20% 抖动），健康会话后重置
                                     // exponential backoff (±20% jitter), reset after a healthy session
    int reconnect_max_ms = 30000;
    int publish_levels = BookSnapshot::kMaxLevels;   // 每侧发布的档位数（钳制到 [0, kMaxLevels]）
                                                     // levels published per side (clamped to [0, kMaxLevels])
    std::size_t queue_capacity = 1024;               // SPSC 队列容量（向上取整到 2 的幂；最后一个槽位留给空簿）
                                                     // SPSC queue capacity (rounded up to a power of two; the
                                                     // last slot is kept for an empty book)

    // ---- 以下为扩展参数：默认值即生产行为，测试用来缩短时间 ----
    // ---- Extended parameters below: defaults are production behavior; tests use them to shorten times ----
    int connect_timeout_ms = 10000;    // 解析 + TCP + TLS + WebSocket 握手的总时限（checkHealth 踢断；<= 0 关闭）；
                                       // total time limit for resolve + TCP + TLS + WebSocket handshake
                                       // (checkHealth kicks; <= 0 disables);
                                       // TCP 连接时每个地址另有 max(1 s, 剩余时限 / 地址数) 的时限
                                       // during TCP connect, each address also gets its own limit of
                                       // max(1 s, remaining limit / number of addresses)
    int ping_interval_ms = -1;         // 心跳间隔：< 0 用适配器默认值（OKX/Bybit 20000，Binance 无），0 关闭心跳，
                                       // heartbeat interval: < 0 uses the adapter default (OKX/Bybit 20000,
                                       // Binance none), 0 disables heartbeats,
                                       // > 0 覆盖。空闲 min(间隔, stall_timeout_ms / 2) 即发心跳（见 feed_detail::Keepalive）
                                       // > 0 overrides. A heartbeat is sent after being idle for min(interval,
                                       // stall_timeout_ms / 2) (see feed_detail::Keepalive)
    int healthy_session_ms = 30000;    // 会话连续 LIVE 超过这么久即视为健康：退避计数清零
                                       // a session continuously LIVE for longer than this counts as healthy:
                                       // the backoff count is reset
    int rest_max_attempts = 5;         // 每个会话最多 REST 快照请求次数，用完即结束会话重连；簿连续同步
                                       // max REST snapshot requests per session; once used up, the session ends
                                       // and reconnects; the book staying synced
                                       // healthy_session_ms 之后额度恢复
                                       // for healthy_session_ms restores the budget
    int rest_min_interval_ms = 1000;   // 同一会话内两次 REST 快照请求（开始时刻）的最小间隔；连续失败时按 2 倍
                                       // minimum interval between two REST snapshot requests (start to start) in
                                       // one session; on consecutive failures it
                                       // 递增（±20% 抖动，上限 reconnect_max_ms；每次等待不超过 stall_timeout_ms / 2）
                                       // doubles (±20% jitter, capped at reconnect_max_ms; no single wait exceeds
                                       // stall_timeout_ms / 2)
    std::size_t max_buffered_events = 2000;   // Binance 同步前事件缓冲上限（AdapterConfig）
                                              // Binance: cap on events buffered before sync (AdapterConfig)
    bool log_to_stderr = true;         // 会话结束的错误是否写 stderr（测试可关闭）
                                       // whether session-ending errors go to stderr (tests can turn this off)
};

// 统计快照：各字段是 relaxed 原子量的拷贝，任意线程可读（字段之间不保证同一时刻）
// Statistics snapshot: each field is a copy of a relaxed atomic, readable from any thread (the fields are
// not guaranteed to come from the same instant)
struct FeedStats {
    std::uint64_t messages = 0, bytes = 0, book_updates = 0, published = 0, dropped_snapshots = 0, reconnects = 0,
                  connect_failures = 0, stalls = 0, gaps = 0, checksum_failures = 0, resyncs = 0, rest_snapshots = 0,
                  parse_errors = 0;
    std::int64_t last_message_ns = 0;     // 最后一条消息的 hft::nowNs()，无则为 0
                                          // hft::nowNs() of the last message, 0 if none
    std::int64_t connected_since_ns = 0;  // 当前会话建立（WebSocket 握手完成）的 hft::nowNs()，未连接为 0
                                          // hft::nowNs() when the current session was established (WebSocket
                                          // handshake completed), 0 when not connected
    std::uint64_t pings = 0;              // 发出的心跳（应用层 ping 消息或 WebSocket ping 控制帧）
                                          // heartbeats sent (application-level ping messages or WebSocket ping
                                          // control frames)
    std::uint64_t pongs = 0;              // 收到的 WebSocket pong 控制帧（应用层的 "pong" 是普通消息，计入 messages）
                                          // WebSocket pong control frames received (an application-level "pong" is
                                          // an ordinary message, counted in messages)
};

namespace feed_detail {

constexpr std::int64_t kNever = std::numeric_limits<std::int64_t>::max();

constexpr std::int64_t msToNs(int ms) noexcept { return static_cast<std::int64_t>(ms) * 1000000; }

// 两个时限中较小的正值；都 <= 0（关闭）时为 0
// The smaller positive value of the two limits; 0 when both are <= 0 (disabled)
constexpr int minPositive(int a, int b) noexcept {
    if (a <= 0) return std::max(b, 0);
    return b <= 0 ? a : std::min(a, b);
}

// 停顿阈值的一半（至少 1 ms；看门狗关闭时为 0）：心跳的回应与 REST 的等待都要在看门狗动手之前完成
// Half the stall threshold (at least 1 ms; 0 when the watchdog is off): heartbeat replies and REST waits
// must both finish before the watchdog acts
constexpr int halfStallMs(int stall_timeout_ms) noexcept {
    return stall_timeout_ms > 0 ? std::max(1, stall_timeout_ms / 2) : 0;
}

// 只有一个写者的计数器：relaxed 读 + 写，不用 RMW
// Counter with a single writer: relaxed load + store, no RMW
inline void bump(std::atomic<std::uint64_t>& counter, std::uint64_t n = 1) noexcept {
    counter.store(counter.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
}

// 错误信息里引用响应体的前 200 个字符（单行）
// Quote the first 200 characters of the response body in error messages (as a single line)
inline std::string bodySnippet(const std::string& body) {
    constexpr std::size_t kMax = 200;
    if (body.empty()) return std::string();
    std::string s = body.substr(0, kMax);
    for (char& c : s) {
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
    }
    return ": " + s + (body.size() > kMax ? "..." : "");
}

// 异步处理器内存：feed 线程私有、按块大小分级的空闲链表。asio 自带的 recycling allocator 把缓存放在 run_one()
// 栈上的 thread_info 里，每次 run_one() / run_one_until() 返回就释放：用它们驱动会话时，每个异步操作都会访问
// 全局堆。这里的缓存跨调用与会话保留：块大小是 128 字节的 2 的幂倍（最大 16 KiB，更大的请求直接走全局堆），
// 释放的块挂回所属级别的链表，稳态下 async_read / async_write 反复使用同几块内存。
// 不加锁：分配与释放都在 feed 线程上（feed 析构时才释放缓存，那时运行 run() 的线程已经 join）。
// Async handler memory: free lists private to the feed thread, segregated by block size. asio's own
// recycling allocator keeps its cache in the thread_info on the stack of run_one() and frees it every time
// run_one() / run_one_until() returns: when sessions are driven with those, every async operation touches
// the global heap. The cache here persists across calls and sessions: block sizes are 128 bytes times a
// power of two (at most 16 KiB; larger requests go straight to the global heap), freed blocks go back onto
// their class's list, and in steady state async_read / async_write keep reusing the same few blocks.
// No locking: allocation and deallocation both happen on the feed thread (the cache is freed only when the
// feed is destroyed, and by then the thread that ran run() has been joined).
class HandlerMemory {
public:
    static constexpr std::size_t kMinBlock = 128;
    static constexpr std::size_t kClasses = 8;  // 128 B .. 16 KiB

    HandlerMemory() = default;
    HandlerMemory(const HandlerMemory&) = delete;
    HandlerMemory& operator=(const HandlerMemory&) = delete;
    ~HandlerMemory() {
        for (Block* head : free_) {
            while (head != nullptr) {
                Block* next = head->next;
                ::operator delete(head);
                head = next;
            }
        }
    }

    void* allocate(std::size_t size) {
        const std::size_t c = sizeClass(size);
        if (c < kClasses && free_[c] != nullptr) {
            Block* b = free_[c];
            free_[c] = b->next;
            return b;
        }
        ++heap_allocations_;
        return ::operator new(c < kClasses ? kMinBlock << c : size);
    }

    // size 与 allocate 时相同（分配器的 deallocate 总会给出它）
    // size is the same as in allocate (the allocator's deallocate always passes it)
    void deallocate(void* p, std::size_t size) noexcept {
        const std::size_t c = sizeClass(size);
        if (c == kClasses) return ::operator delete(p);
        free_[c] = ::new (p) Block{free_[c]};
    }

    // 向全局堆申请过的块数（测试用）
    // Number of blocks requested from the global heap (for tests)
    std::uint64_t heapAllocations() const noexcept { return heap_allocations_; }

private:
    struct Block {
        Block* next;
    };

    // 能容纳 size 的最小级别；kClasses = 超过最大级别
    // Smallest class that can hold size; kClasses = beyond the largest class
    static std::size_t sizeClass(std::size_t size) noexcept {
        std::size_t c = 0;
        while (c < kClasses && (kMinBlock << c) < size) ++c;
        return c;
    }

    Block* free_[kClasses] = {};
    std::uint64_t heap_allocations_ = 0;
};

// 从 HandlerMemory 分配的分配器：用 asio::bind_allocator 关联到完成处理器上，asio 与 Beast 为该处理器的
// 异步操作分配的全部内存（reactor 操作、投递的完成、Beast 的稳定状态与暂停的操作）都经过它
// Allocator that allocates from HandlerMemory: bound to a completion handler with asio::bind_allocator, so
// all memory that asio and Beast allocate for that handler's async operations (reactor operations, posted
// completions, Beast's stable state and paused operations) goes through it
template <class T>
class HandlerAllocator {
public:
    using value_type = T;

    explicit HandlerAllocator(HandlerMemory& memory) noexcept : memory_(&memory) {}
    template <class U>
    HandlerAllocator(const HandlerAllocator<U>& other) noexcept : memory_(other.memory_) {}

    T* allocate(std::size_t n) {
        static_assert(alignof(T) <= alignof(std::max_align_t), "HandlerMemory blocks are max_align_t aligned");
        // 防止 n * sizeof(T) 溢出
        // prevent n * sizeof(T) overflow
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) throw std::bad_array_new_length();
        return static_cast<T*>(memory_->allocate(n * sizeof(T)));
    }
    void deallocate(T* p, std::size_t n) noexcept { memory_->deallocate(p, n * sizeof(T)); }

    template <class U>
    bool operator==(const HandlerAllocator<U>& other) const noexcept {
        return memory_ == other.memory_;
    }
    template <class U>
    bool operator!=(const HandlerAllocator<U>& other) const noexcept {
        return memory_ != other.memory_;
    }

private:
    template <class>
    friend class HandlerAllocator;
    HandlerMemory* memory_;
};

// 心跳策略（纯计算，只由 feed 线程使用）。有应用层心跳的交易所（OKX "ping"、Bybit {"op":"ping"}）在 idle 内没有
// 收到任何帧时发心跳消息，数据一直在来也至少每个心跳间隔发一次；没有应用层心跳的交易所（Binance）空闲同样久之后
// 发 WebSocket ping 控制帧。idle = min(心跳间隔, stall_timeout_ms / 2)：回应总能在看门狗动手之前到达。
// 每次最多一个心跳在途。
// Heartbeat policy (pure computation, used only by the feed thread). For exchanges with an application-level
// heartbeat (OKX "ping", Bybit {"op":"ping"}), a heartbeat message is sent when no frame at all has arrived
// within idle, and at least once per heartbeat interval even while data keeps coming; for exchanges without
// an application-level heartbeat (Binance), a WebSocket ping control frame is sent after the same idle time.
// idle = min(heartbeat interval, stall_timeout_ms / 2): the reply always has time to arrive before the
// watchdog acts.
// At most one heartbeat is in flight at a time.
class Keepalive {
public:
    // text：适配器的心跳消息（"" = 发 WebSocket ping）；ping_interval_ms 同 FeedConfig（< 0 用 adapter_interval_ms，
    // 0 关闭）；now：会话开始的时刻
    // text: the adapter's heartbeat message ("" = send a WebSocket ping); ping_interval_ms as in FeedConfig
    // (< 0 uses adapter_interval_ms, 0 disables); now: the time the session started
    void start(std::string text, int ping_interval_ms, int adapter_interval_ms, int stall_timeout_ms,
               std::int64_t now) {
        text_ = std::move(text);
        int interval_ms = 0;
        if (ping_interval_ms > 0) {
            interval_ms = ping_interval_ms;
        } else if (ping_interval_ms < 0 && !text_.empty()) {
            interval_ms = std::max(0, adapter_interval_ms);
        }
        idle_ns_ = ping_interval_ms == 0 ? 0 : msToNs(minPositive(interval_ms, halfStallMs(stall_timeout_ms)));
        interval_ns_ = text_.empty() ? 0 : msToNs(interval_ms);
        last_ns_ = now;
        outstanding_ = false;
    }

    const std::string& text() const noexcept { return text_; }

    // 下一个心跳到期的时刻（kNever = 关闭）。last_rx：最近收到任何帧的时刻
    // Time the next heartbeat is due (kNever = disabled). last_rx: time any frame was last received
    std::int64_t dueNs(std::int64_t last_rx) const noexcept {
        std::int64_t due = kNever;
        if (idle_ns_ > 0) due = std::max(last_rx, last_ns_) + idle_ns_;
        if (interval_ns_ > 0) due = std::min(due, last_ns_ + interval_ns_);
        return due;
    }

    // 现在是否该发心跳（已到期且没有心跳在途）；是则记为已发出
    // Whether to send a heartbeat now (due, with no heartbeat in flight); if so, it is marked as sent
    bool take(std::int64_t last_rx, std::int64_t now) noexcept {
        if (outstanding_ || now < dueNs(last_rx)) return false;
        last_ns_ = now;
        outstanding_ = true;
        return true;
    }

    // 心跳已写出（或写失败）
    // The heartbeat has been written (or the write failed)
    void written() noexcept { outstanding_ = false; }

private:
    std::string text_;              // 应用层心跳消息；"" = WebSocket ping 控制帧
                                    // application-level heartbeat message; "" = WebSocket ping control frame
    std::int64_t idle_ns_ = 0;      // 这么久没有收到任何帧就发心跳（0 = 关闭）
                                    // send a heartbeat after this long without any frame received (0 = disabled)
    std::int64_t interval_ns_ = 0;  // 应用层心跳：数据一直在来也至少每隔这么久发一次（0 = 无）
                                    // application-level heartbeat: sent at least this often even while data keeps
                                    // coming (0 = none)
    std::int64_t last_ns_ = 0;      // 上一次发心跳（或会话开始）的时刻
                                    // when the last heartbeat was sent (or the session started)
    bool outstanding_ = false;      // 有心跳已排队或正在发送
                                    // a heartbeat is queued or being sent
};

// 快照发布（只在 feed 线程上发布；published()/dropped() 任意线程可读）。
// 非空快照入队之后至少要留一个空槽位（tryPushReserving(…, 1)），空簿可以用掉最后一个：只要消费者最后收到的快照
// 非空，下一个空簿（断线、缺口）就一定能入队，消费者不会在断线期间继续使用过期价格——无论队列多满、feed 线程
// 接下来阻塞在哪一步，都不需要重试。所以非空快照最多占用 capacity - 1 个槽位。
// 入队失败 → dropped++；seq 对每次发布尝试递增，消费者看到 seq 跳号即表示丢了快照。消费者手里是空簿时没能入队的
// 非空快照（例如断线之后新会话的第一本簿）保持待发布，由 retryPending() 重试当时的最新状态，否则推送稀少的交易所
// 可能长时间只显示空簿；消费者手里是非空簿时照常丢弃，下一次推送会替换它。
// 每个快照真正入队之后（含空簿与重试）调用发布回调，异常被吞掉；被丢弃的快照不通知。
// Snapshot publishing (published only on the feed thread; published()/dropped() readable from any thread).
// After a non-empty snapshot is enqueued, at least one free slot must remain (tryPushReserving(…, 1)); an
// empty book may take the last one: as long as the last snapshot the consumer received is non-empty, the
// next empty book (disconnect, gap) is guaranteed to get into the queue, so the consumer never keeps using
// stale prices during a disconnect — however full the queue is and wherever the feed thread blocks next, no
// retry is needed. So non-empty snapshots occupy at most capacity - 1 slots.
// Enqueue failure → dropped++; seq increments on every publish attempt, so a skipped seq tells the consumer
// that snapshots were dropped. A non-empty snapshot that failed to enqueue while the consumer holds an
// empty book (e.g. the first book of a new session after a disconnect) stays pending, and retryPending()
// retries with the latest state at that time; otherwise an exchange that pushes rarely could show only an
// empty book for a long time. While the consumer holds a non-empty book, such a snapshot is dropped as
// usual, and the next push replaces it.
// The publish callback is invoked after each snapshot is actually enqueued (empty books and retries
// included), with exceptions swallowed; dropped snapshots are not notified.
class SnapshotPublisher {
public:
    SnapshotPublisher(std::size_t capacity, std::uint32_t venue) : queue_(capacity) { snapshot_.venue = venue; }

    hft::SpscQueue<BookSnapshot>& queue() noexcept { return queue_; }
    void setCallback(std::function<void()> cb) { callback_ = std::move(cb); }
    std::uint64_t published() const noexcept { return published_.load(std::memory_order_relaxed); }
    std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
    bool pending() const noexcept { return pending_; }

    // 下一个快照（已分配 seq）：调用方填好档位与时间戳之后 push()
    // Next snapshot (seq already assigned): the caller fills in the levels and timestamps, then calls push()
    BookSnapshot& next() noexcept {
        snapshot_.seq = ++seq_;
        return snapshot_;
    }

    // 入队 next() 填好的快照，绝不阻塞
    // Enqueue the snapshot filled in through next(); never blocks
    void push() noexcept {
        const bool nonempty = !empty();
        if (queue_.tryPushReserving(snapshot_, nonempty ? 1 : 0)) return delivered();
        HFT_ASSERT((nonempty || !delivered_nonempty_) && "the slot reserved for an empty book was taken");
        bump(dropped_);
        pending_ = nonempty && !delivered_nonempty_;
    }

    // 重试待发布的快照：seq 不变、不再计入 dropped；publish_ns 改为真正入队的时刻（消费者的队列延迟从这里算起）
    // Retry the pending snapshot: seq stays the same and it is not counted in dropped again; publish_ns becomes
    // the time it is actually enqueued (the consumer's queue latency counts from there)
    void retryPending() noexcept {
        if (!pending_) return;
        snapshot_.publish_ns = hft::nowNs();
        if (queue_.tryPushReserving(snapshot_, 1)) delivered();
    }

    // 会话结束：消费者最后收到的簿非空 → 发布空簿（一定能入队；不记录处理延迟：不对应任何消息）。
    // 待发布的快照随会话一起作废：那时消费者手里本来就是空簿，正是断线期间该有的状态。
    // Session end: the last book the consumer received is non-empty → publish an empty book (it always fits in
    // the queue; no processing latency is recorded, since it corresponds to no message).
    // A pending snapshot is discarded along with the session: the consumer already holds an empty book then,
    // which is exactly the state it should have during a disconnect.
    void endSession() noexcept {
        if (delivered_nonempty_) {
            BookSnapshot& s = next();
            s.bid_count = 0;
            s.ask_count = 0;
            s.update_id = 0;
            s.exchange_ts_ms = 0;
            s.recv_ns = hft::nowNs();
            s.publish_ns = s.recv_ns;
            push();
        }
        pending_ = false;
    }

private:
    bool empty() const noexcept { return snapshot_.bid_count + snapshot_.ask_count == 0; }

    // 快照已经入队：记账只跟踪真正送到消费者手里的快照，然后通知
    // The snapshot is in the queue: the bookkeeping tracks only snapshots that actually reach the consumer;
    // then notify
    void delivered() noexcept {
        bump(published_);
        delivered_nonempty_ = !empty();
        pending_ = false;
        if (!callback_) return;
        try {
            callback_();
        } catch (...) {
        }
    }

    hft::SpscQueue<BookSnapshot> queue_;
    BookSnapshot snapshot_;            // 最近一次发布（或待发布）的快照
                                       // the most recently published (or pending) snapshot
    std::uint64_t seq_ = 0;
    bool delivered_nonempty_ = false;  // 消费者最后收到（真正入队）的快照非空
                                       // the last snapshot the consumer received (actually enqueued) is non-empty
    bool pending_ = false;             // 消费者手里是空簿、snapshot_ 非空却没能入队：需要重试
                                       // the consumer holds an empty book and the non-empty snapshot_ failed to
                                       // enqueue: needs a retry
    std::function<void()> callback_;   // run() 之前设置；之后只由 feed 线程读取
                                       // set before run(); afterwards read only by the feed thread
    std::atomic<std::uint64_t> published_{0}, dropped_{0};
};

}  // namespace feed_detail

// 行情连接管理器。每个 feed 一个线程（简单、延迟低：读到数据立即在本线程处理）。
// Market data connection manager. One thread per feed (simple and low-latency: data is processed on that
// thread as soon as it is read).
//
// 会话：解析 → 打开并登记套接字 → TCP connect → TCP_NODELAY → SNI →（tls_verify：系统 CA + 可选 ca_file、
// verify_peer、host_name_verification）→ TLS 握手 → WebSocket 握手（User-Agent: crypto_hft_engine/2.0）→ 同步发送
// adapter.onConnected() 的帧 → 流式阶段。建立连接是同步的；流式阶段用一个只属于本会话的 io_context，在本线程上
// 驱动 ws.async_read 与写队列，并在两次处理器之间按时发心跳（run_one_until 在下一个到期时刻醒来：连接安静时也能
// 按时发心跳）。异步读开始之后的全部写（适配器要求发送的帧，含重订阅；心跳）都经同一个写队列依次异步发出：
// Beast 允许一个读与一个写同时挂起，异步读挂起时不能再同步写。
// Session: resolve → open and register the socket → TCP connect → TCP_NODELAY → SNI → (tls_verify: system
// CAs + optional ca_file, verify_peer, host_name_verification) → TLS handshake → WebSocket handshake
// (User-Agent: crypto_hft_engine/2.0) → synchronously send the frames from adapter.onConnected() →
// streaming phase. Connection setup is synchronous; the streaming phase uses an io_context that belongs to
// this session alone to drive ws.async_read and the write queue on this thread, sending heartbeats on time
// between handlers (run_one_until wakes up at the next due time, so heartbeats go out on time even while
// the connection is quiet). Every write after the async read has started (frames the adapter asks to send,
// resubscriptions included; heartbeats) goes out asynchronously, one after another, through the same write
// queue: Beast allows one read and one write to be pending at the same time, and no synchronous write is
// possible while an async read is pending.
//
// 踢断：阻塞调用没有超时，及时退出与停顿恢复靠"踢断"：所有活动套接字（含 REST 快照的）登记在
// hft::net::SocketRegistry 中，stop() 与 checkHealth() 对它们 ::shutdown(SHUT_RDWR)，阻塞的 handshake/read/write 与
// 挂起的异步读写立即失败；TCP 连接（hft::net::connectSocket：非阻塞、约 50 ms 一片地等待，每个地址有自己的时限）
// 与 DNS 解析（后台线程）也随时可被打断。连接超时结束的会话让下一次从下一个地址开始（WebSocket 与 REST 各有一个
// 轮转游标），连上之后下次仍从这个地址开始。
// Kick: blocking calls have no timeout; prompt exit and stall recovery rely on "kicking": every active
// socket (the REST snapshot's included) is registered in hft::net::SocketRegistry, and stop() and
// checkHealth() call ::shutdown(SHUT_RDWR) on them, so blocked handshake/read/write calls and pending async
// reads and writes fail immediately; TCP connect (hft::net::connectSocket: non-blocking, waiting in slices
// of about 50 ms, with a time limit per address) and DNS resolution (background thread) can be interrupted
// at any time as well. A session that ended in a connect timeout makes the next one start at the next
// address (WebSocket and REST each have a rotation cursor); after a successful connect, the next session
// starts at that same address again.
//
// 每条消息：recv_ns = 读处理器开始执行的时刻 → adapter.onMessage → 依次处理结果：状态 SYNCING/LIVE 按
// adapter.synced() 更新；error → lastError() 与日志回调；send → 按顺序进入写队列；交易所延迟记录；book_updated →
// 发布快照（空簿同样发布；处理延迟在入队前记录）。因此消费者取到一个快照时，status() 与两个延迟统计都已包含这条
// 消息（队列的 release/acquire），只有 published/dropped 计数在入队之后才更新。reconnect → 结束会话并立即重连
// （不退避）。need_rest_snapshot → 在读处理器里同步拉取 REST 快照（见 fetchRestSnapshot；其间 WebSocket 数据暂存在
// 内核缓冲区）。先发布、后拉 REST：缺口导致的空簿立即让消费者知道，不必等待 REST 往返。
// 心跳见 feed_detail::Keepalive；快照队列、待发布快照（流式阶段约每 5 ms 重试一次，会话内 REST 请求之间的等待中
// 同样重试）与断线时的空簿见 feed_detail::SnapshotPublisher：会话结束（任何原因）时若消费者最后收到的簿非空，
// 再发布一个空簿，断线期间消费者不会继续使用过期价格。收到的任何帧都算活动（含文本 pong、pong 控制帧与服务器的
// ping）：刷新停顿看门狗与健康会话判定，所以只有心跳往来的安静会话不会被踢断；对端不回应时看门狗照常踢断。
// Per message: recv_ns = time the read handler starts running → adapter.onMessage → the result is handled
// in order: status SYNCING/LIVE is updated from adapter.synced(); error → lastError() and the log callback;
// send → into the write queue in order; exchange latency recorded; book_updated → publish a snapshot (an
// empty book is published too; the processing latency is recorded before the enqueue). So when the
// consumer takes a snapshot, status() and both latency stats already include this message (the queue's
// release/acquire); only the published/dropped counts are updated after the enqueue. reconnect → end the
// session and reconnect immediately (no backoff). need_rest_snapshot → fetch the REST snapshot
// synchronously in the read handler (see fetchRestSnapshot; meanwhile WebSocket data waits in the kernel
// buffer). Publish first, fetch REST afterwards: the consumer learns of the empty book caused by a gap
// right away, without waiting for the REST round trip.
// Heartbeats: see feed_detail::Keepalive; the snapshot queue, pending snapshots (retried about every 5 ms
// in the streaming phase, and also during the waits between REST requests within a session) and the empty
// book on disconnect: see feed_detail::SnapshotPublisher: when a session ends (for any reason) and the
// last book the consumer received is non-empty, one more empty book is published, so the consumer does not
// keep using stale prices during the disconnect. Any frame received counts as activity (text pongs, pong
// control frames and server pings included): it refreshes the stall watchdog and the healthy-session
// check, so a quiet session with only heartbeat traffic is not kicked; when the peer does not answer, the
// watchdog kicks as usual.
//
// 状态：建立连接期间 Connecting；握手完成后 Syncing，adapter.synced() 后 Live；会话结束 → Reconnecting
// （错误写 stderr 并存入 lastError()），退避 min(max, initial * 2^attempt)（±20% 抖动，结果钳制到 [1, max]，
// 可被 stop() 打断）；会话连续 Live 超过 healthy_session_ms（默认 30 s）后 attempt 清零（每次收到帧时检查）。
// 适配器要求重连时立即重连；但连续 kMaxImmediateReconnects 次立即重连之后（期间没有健康会话）改用正常退避，
// 防止服务器刚连上就要求重连时我们无间隔地反复握手。run() 返回后为 Stopped；stop 之后不能再次 run()。
// Status: Connecting while the connection is being set up; Syncing after the handshake, Live after
// adapter.synced(); session end → Reconnecting (the error goes to stderr and into lastError()), backoff
// min(max, initial * 2^attempt) (±20% jitter, result clamped to [1, max], interruptible by stop()); once a
// session has been Live continuously for longer than healthy_session_ms (default 30 s), attempt resets to 0
// (checked on every frame received).
// When the adapter asks for a reconnect, reconnect immediately; but after kMaxImmediateReconnects immediate
// reconnects in a row (with no healthy session in between), fall back to normal backoff, so that we do not
// handshake again and again without pause when a server asks for a reconnect right after connecting.
// Stopped after run() returns; run() cannot be called again after stop.
//
// 计数：reconnects = 第一次之后的每次连接尝试；connect_failures = 在 WebSocket 握手完成之前失败的尝试
// （含证书校验失败、连接超时）；stalls = checkHealth 因无数据踢断的会话数。适配器统计每条消息后复制到原子量。
// 处理延迟（getLatencyTracker）= publish_ns - recv_ns，每次发布记录一次；交易所延迟（getExchangeLatency）
// = wallClockMs() - exchange_ts_ms（纳秒存储，时钟偏差导致的负值记为 0），每条带时间戳的消息记录一次。
// Counters: reconnects = every connection attempt after the first; connect_failures = attempts that failed
// before the WebSocket handshake completed (certificate verification failures and connect timeouts
// included); stalls = sessions that checkHealth kicked for lack of data. The adapter's stats are copied
// into atomics after every message.
// Processing latency (getLatencyTracker) = publish_ns - recv_ns, recorded once per publish; exchange
// latency (getExchangeLatency) = wallClockMs() - exchange_ts_ms (stored in nanoseconds, negative values
// caused by clock skew recorded as 0), recorded once per timestamped message.
//
// 线程模型：run() 在专用线程上调用一次；stop()、status()、stats()、lastError()、延迟统计可在任意线程调用；
// checkHealth() 与 queue() 的消费端只在主线程调用；setPublishCallback() 只能在 run() 之前调用。
// 销毁前必须 stop() 并 join 运行 run() 的线程。
// Threading model: run() is called once on a dedicated thread; stop(), status(), stats(), lastError() and
// the latency stats may be called from any thread; checkHealth() and the consumer side of queue() are used
// only on the main thread; setPublishCallback() may only be called before run().
// Before destruction, stop() must be called and the thread running run() joined.
class MarketDataFeed {
public:
    // 连续立即重连的上限（之后改用退避，直到出现健康会话）
    // Cap on consecutive immediate reconnects (after that, backoff is used until a healthy session occurs)
    static constexpr int kMaxImmediateReconnects = 3;

    explicit MarketDataFeed(const FeedConfig& cfg)
        : cfg_(sanitize(cfg)),
          adapter_(makeAdapter(cfg_.exchange, adapterConfig(cfg_))),
          publisher_(cfg_.queue_capacity, cfg_.venue_id),
          latency_(cfg_.name),
          exchange_latency_(cfg_.name + " exchange"),
          rest_request_(restRequest(cfg_, &rest_cursor_)),
          rng_(makeSeed()) {
        if (!adapter_) throw std::invalid_argument("MarketDataFeed: unsupported exchange");
    }

    MarketDataFeed(const MarketDataFeed&) = delete;
    MarketDataFeed& operator=(const MarketDataFeed&) = delete;

    // 线程入口：connect → stream → reconnect 循环，直到 stop()。不抛异常。
    // Thread entry point: connect → stream → reconnect loop until stop(). Never throws.
    void run() noexcept {
        try {
            hft::setThreadName(("feed-" + cfg_.name).c_str());
            runLoop();
        } catch (...) {
            // 只有错误上报路径上的内存分配失败会走到这里：结束线程
            // Only a memory allocation failure on the error reporting path ends up here: end the thread
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            phase_ = Phase::Idle;
        }
        connected_since_ns_.store(0, std::memory_order_relaxed);
        status_.store(FeedStatus::Stopped, std::memory_order_release);
    }

    // 任意线程：请求停止。踢断所有套接字（阻塞的读写/握手立即返回，进行中的 TCP 连接在一个轮询片
    // ——不超过 50 ms——之内返回）并唤醒退避等待。
    // Any thread: request a stop. Kicks all sockets (blocked reads/writes/handshakes return immediately; a TCP
    // connect in progress returns within one poll slice, at most 50 ms) and wakes up the backoff wait.
    void stop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
        registry_.close();
        { std::lock_guard<std::mutex> lock(mutex_); }  // 与 sleepUntil 的谓词检查配对，避免丢失唤醒
                                                       // pairs with sleepUntil's predicate check, so no wakeup
                                                       // is lost
        cv_.notify_all();
    }

    // 主线程每次刷新调用：已连接但 stall_timeout_ms 没有数据 → stalls++ 并踢断；
    // 连接建立阶段超过 connect_timeout_ms → 踢断（计入 connect_failures）。同一会话只踢一次。
    // Called by the main thread on every refresh: connected but no data for stall_timeout_ms → stalls++ and
    // kick; connection setup longer than connect_timeout_ms → kick (counted in connect_failures). A session
    // is kicked only once.
    void checkHealth(std::int64_t now_ns) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (phase_ == Phase::Streaming && cfg_.stall_timeout_ms > 0) {
            const std::int64_t idle = now_ns - activity_ns_.load(std::memory_order_relaxed);
            if (idle > feed_detail::msToNs(cfg_.stall_timeout_ms) && registry_.kick(hft::net::KickReason::Stall)) {
                stalls_.fetch_add(1, std::memory_order_relaxed);
            }
        } else if (phase_ == Phase::Connecting && cfg_.connect_timeout_ms > 0) {
            if (now_ns - phase_start_ns_ > feed_detail::msToNs(cfg_.connect_timeout_ms)) {
                registry_.kick(hft::net::KickReason::ConnectTimeout);
            }
        }
    }

    // 快照队列：生产者 = feed 线程，消费者 = 主线程
    // Snapshot queue: producer = feed thread, consumer = main thread
    hft::SpscQueue<BookSnapshot>& queue() noexcept { return publisher_.queue(); }

    FeedStatus status() const noexcept { return status_.load(std::memory_order_acquire); }

    FeedStats stats() const noexcept {
        constexpr auto r = std::memory_order_relaxed;
        FeedStats s;
        s.messages = messages_.load(r);
        s.bytes = bytes_.load(r);
        s.book_updates = book_updates_.load(r);
        s.published = publisher_.published();
        s.dropped_snapshots = publisher_.dropped();
        s.reconnects = reconnects_.load(r);
        s.connect_failures = connect_failures_.load(r);
        s.stalls = stalls_.load(r);
        s.gaps = gaps_.load(r);
        s.checksum_failures = checksum_failures_.load(r);
        s.resyncs = resyncs_.load(r);
        s.rest_snapshots = rest_snapshots_.load(r);
        s.parse_errors = parse_errors_.load(r);
        s.last_message_ns = last_message_ns_.load(r);
        s.connected_since_ns = connected_since_ns_.load(r);
        s.pings = pings_.load(r);
        s.pongs = pongs_.load(r);
        return s;
    }

    // 最近一次问题（连接失败、断线、适配器报告的缺口/校验错误等）；没有时为空
    // The most recent problem (connect failure, disconnect, gap/checksum error reported by the adapter, etc.);
    // empty if there is none
    std::string lastError() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return last_error_;
    }

    const LatencyTracker& getLatencyTracker() const noexcept { return latency_; }
    const LatencyTracker& getExchangeLatency() const noexcept { return exchange_latency_; }
    const std::string& getName() const noexcept { return cfg_.name; }
    std::uint32_t venueId() const noexcept { return cfg_.venue_id; }

    // 日志回调：在 feed 线程上针对连接/断开/重同步/错误事件调用，参数为 "[name] message"。
    // 可在任意时刻设置（内部加锁）；回调抛出的异常被吞掉。
    // Log callback: called on the feed thread for connect/disconnect/resync/error events, with the argument
    // "[name] message".
    // Can be set at any time (locks internally); exceptions thrown by the callback are swallowed.
    void setLogCallback(std::function<void(const std::string&)> cb) {
        std::lock_guard<std::mutex> lock(error_mutex_);
        log_cb_ = std::move(cb);
    }

    // 发布通知：每个快照（含空簿与待发布快照的重试）真正进入队列之后，立即在 feed 线程上调用 cb，
    // 例如唤醒等待新数据的主循环；被丢弃（队列满）的快照不通知。必须在 run() 之前设置（之后不能再改，
    // 不加锁）。cb 在行情热路径上执行：必须很快返回、不能阻塞；抛出的异常被吞掉。
    // Publish notification: right after each snapshot (empty books and retries of pending snapshots included)
    // actually enters the queue, cb is called on the feed thread, e.g. to wake a main loop waiting for new
    // data; dropped snapshots (queue full) are not notified. Must be set before run() (it cannot be changed
    // afterwards; no locking). cb runs on the market data hot path: it must return quickly and must not block;
    // exceptions it throws are swallowed.
    void setPublishCallback(std::function<void()> cb) { publisher_.setCallback(std::move(cb)); }

    // 退避时长：min(max, initial * 2^attempt) * (1 + 0.2 * jitter)，jitter ∈ [-1, 1]，结果钳制到 [1, max]
    // Backoff delay: min(max, initial * 2^attempt) * (1 + 0.2 * jitter), jitter ∈ [-1, 1], result clamped
    // to [1, max]
    static int backoffDelayMs(int attempt, int initial_ms, int max_ms, double jitter) noexcept {
        const double initial = static_cast<double>(std::max(1, initial_ms));
        const double cap = std::max(initial, static_cast<double>(max_ms));
        double base = initial;
        for (int i = 0; i < attempt && base < cap; ++i) base *= 2.0;
        base = std::min(base, cap);
        if (!(jitter >= -1.0)) jitter = -1.0;  // 同时处理 NaN
                                               // also handles NaN
        if (jitter > 1.0) jitter = 1.0;
        const double d = std::clamp(base * (1.0 + 0.2 * jitter), 1.0, cap);
        return static_cast<int>(std::lround(d));
    }

private:
    // 会话对象直接使用 io_context 的执行器类型（不经 any_io_executor 类型擦除）：投递完成处理器时也用处理器
    // 关联的分配器（handler_memory_），稳态零堆分配
    // Session objects use the io_context's executor type directly (no any_io_executor type erasure): posting a
    // completion handler also uses the handler's associated allocator (handler_memory_), so the steady state
    // has zero heap allocations
    using Executor = boost::asio::io_context::executor_type;
    using Socket = boost::asio::basic_stream_socket<boost::asio::ip::tcp, Executor>;
    using WsStream = boost::beast::websocket::stream<boost::beast::ssl_stream<Socket>>;
    using Endpoints = boost::asio::ip::tcp::resolver::results_type;
    using Clock = std::chrono::steady_clock;
    using error_code = boost::system::error_code;

    // 看门狗关心的阶段（mutex_ 保护）
    // Phase the watchdog cares about (guarded by mutex_)
    enum class Phase { Idle, Connecting, Streaming };

    // 会话进行到的步骤（错误信息里的名字见 kStageNames）。顺序有意义：>= TlsHandshake 表示 TCP 已连上，
    // >= Subscribe 表示 WebSocket 握手已完成（会话已建立）
    // Step the session has reached (for the names in error messages see kStageNames). The order matters:
    // >= TlsHandshake means TCP is connected, >= Subscribe means the WebSocket handshake has completed (the
    // session is established)
    enum class Stage { Setup, TlsSetup, Resolve, TcpConnect, TlsHandshake, WsHandshake, Subscribe, Read, Rest };
    static constexpr const char* kStageNames[] = {"setup",         "TLS setup",     "DNS resolve",
                                                  "TCP connect",   "TLS handshake", "WebSocket handshake",
                                                  "subscribe",     "read",          "REST snapshot"};
    static_assert(std::size(kStageNames) == static_cast<std::size_t>(Stage::Rest) + 1, "one name per stage");

    // 会话结束的原因
    // Why a session ended
    struct SessionEnd {
        std::string message;               // 错误描述（主动停止时为空、不上报）
                                           // error description (empty on a requested stop, and not reported)
        bool immediate_reconnect = false;  // 适配器要求重连：不退避
                                           // the adapter asked for a reconnect: no backoff
    };

    // 每个会话开始时重置的状态（只由 feed 线程访问）
    // State reset at the start of every session (accessed only by the feed thread)
    struct SessionState {
        Stage stage = Stage::Setup;
        bool synced = false;             // adapter.synced() 的最近值
                                         // latest value of adapter.synced()
        bool healthy = false;            // 已连续 Live 超过 healthy_session_ms（退避计数已清零）
                                         // continuously Live for longer than healthy_session_ms (backoff
                                         // count already reset)
        std::int64_t live_since_ns = 0;  // 最近一次（重新）同步的时刻
                                         // time of the latest (re)sync
        int rest_requests = 0;           // REST 快照请求数（额度恢复以来）
                                         // REST snapshot requests (since the budget was last restored)
        int rest_failures = 0;           // 连续失败的 REST 请求数（同步成功后清零）
                                         // consecutive failed REST requests (reset after a successful sync)
        std::int64_t last_rest_ns = 0;   // 上一次 REST 请求的开始时刻（0 = 还没有）
                                         // start time of the last REST request (0 = none yet)
    };

    // 写队列中的一项：Text（适配器要求发送的帧）与 AppPing 是文本帧，WsPing 是 WebSocket ping 控制帧
    // An entry in the write queue: Text (frames the adapter asks to send) and AppPing are text frames, WsPing
    // is a WebSocket ping control frame
    enum class WriteKind : std::uint8_t { Text, AppPing, WsPing };
    struct QueuedWrite {
        WriteKind kind = WriteKind::Text;
        std::string text;
    };

    // 一个 WebSocket 会话的网络对象与流式阶段的状态（只在 feed 线程上使用）。成员按此顺序声明：析构时先注销套接字
    // （registration）、再关闭它（ws），最后销毁 io_context（其中尚未执行的处理器随之销毁、不会被调用）。
    // Network objects and streaming-phase state of one WebSocket session (used only on the feed thread). The
    // members are declared in this order: on destruction the socket is first unregistered (registration), then
    // closed (ws), and finally the io_context is destroyed (handlers in it that have not run yet are destroyed
    // with it and never called).
    struct Connection {
        explicit Connection(boost::asio::ssl::context& ctx) : ioc(1), ws(ioc, ctx) {}

        boost::asio::io_context ioc;                 // 只属于本会话，只由 feed 线程驱动
                                                     // owned by this session alone, driven only by the feed thread
        WsStream ws;
        hft::net::SocketRegistration registration;   // 声明在 ws 之后：析构时先注销、再关闭套接字
                                                     // declared after ws: destruction first unregisters,
                                                     // then closes the socket

        std::optional<SessionEnd> end;               // 已决定结束会话：之后的处理器什么也不做
                                                     // set once the session is to end: later handlers do nothing
        feed_detail::Keepalive keepalive;
        std::int64_t wake_ns = feed_detail::kNever;  // 下一次检查心跳与待发布快照的时刻
                                                     // next time to check the heartbeat and pending snapshot
        std::int64_t retry_ns = 0;                   // 待发布快照的下一次重试时刻（0 = 没有安排）
                                                     // next retry time of the pending snapshot (0 = none scheduled)
        // 写队列：[write_next, write_end) 是待发送的项；发完复位下标，各项保留字符串容量（稳态不分配）
        // Write queue: [write_next, write_end) are the entries still to be sent; the indices reset once everything
        // is sent, and entries keep their string capacity (no allocation in steady state)
        std::vector<QueuedWrite> writes;
        std::size_t write_next = 0;
        std::size_t write_end = 0;
        QueuedWrite inflight;                        // 正在发送的项：缓冲区在写完成之前保持不变
                                                     // entry being sent: its buffer stays untouched until
                                                     // the write completes
        bool writing = false;
    };

    static constexpr std::int64_t kMinWaitNs = 1000000;  // 两次定时检查至少相隔 1 ms：不会空转
                                                         // timed checks at least 1 ms apart: no busy spinning
    static constexpr int kPendingRetryMs = 5;            // 待发布快照的重试间隔
                                                         // retry interval for the pending snapshot

    static FeedConfig sanitize(FeedConfig c) {
        c.reconnect_initial_ms = std::max(1, c.reconnect_initial_ms);
        c.reconnect_max_ms = std::max(c.reconnect_initial_ms, c.reconnect_max_ms);
        c.rest_max_attempts = std::max(1, c.rest_max_attempts);
        c.rest_min_interval_ms = std::max(1, c.rest_min_interval_ms);
        c.publish_levels = std::clamp(c.publish_levels, 0, BookSnapshot::kMaxLevels);
        if (c.path.empty()) c.path = "/";
        return c;
    }

    static AdapterConfig adapterConfig(const FeedConfig& c) {
        AdapterConfig a;
        a.name = c.name;
        a.symbol = c.symbol;
        a.subscribe_msg = c.subscribe_msg;
        a.verify_checksum = c.verify_checksum;
        a.max_buffered_events = c.max_buffered_events;
        return a;
    }

    // REST 快照请求。TCP 连接的总时限是 connect_timeout_ms 与 stall_timeout_ms / 2 中较小的正值（都关闭时不限）：
    // REST 请求期间停顿看门狗照常计时，连接必须远早于它完成
    // REST snapshot request. The total TCP connect limit is the smaller positive value of connect_timeout_ms
    // and stall_timeout_ms / 2 (unlimited when both are disabled): the stall watchdog keeps timing during a
    // REST request, so the connect must complete well before it fires
    static hft::net::HttpsGetRequest restRequest(const FeedConfig& c, std::size_t* cursor) {
        hft::net::HttpsGetRequest rq;
        rq.host = c.rest_host;
        rq.port = c.rest_port;
        rq.target = c.snapshot_path;
        rq.verify = c.tls_verify;
        rq.body_limit = adapter_detail::kMaxMessageBytes;
        rq.connect_timeout_ms =
            feed_detail::minPositive(c.connect_timeout_ms, feed_detail::halfStallMs(c.stall_timeout_ms));
        rq.endpoint_cursor = cursor;
        return rq;
    }

    std::uint64_t makeSeed() const noexcept {
        std::uint64_t seed = static_cast<std::uint64_t>(hft::nowNs()) ^ reinterpret_cast<std::uintptr_t>(this);
        try {
            std::random_device rd;
            seed ^= (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
        } catch (...) {
        }
        return seed;
    }

    bool stopRequested() const noexcept { return stop_requested_.load(std::memory_order_acquire); }
    const char* stageName() const noexcept { return kStageNames[static_cast<int>(session_.stage)]; }
    bool established() const noexcept { return session_.stage >= Stage::Subscribe; }
    double jitter() { return std::uniform_real_distribution<double>(-1.0, 1.0)(rng_); }

    // ---------------- 重连循环 ----------------
    // ---------------- Reconnect loop ----------------

    void runLoop() {
        for (bool first = true; !stopRequested(); first = false) {
            if (!first) reconnects_.fetch_add(1, std::memory_order_relaxed);
            const SessionEnd end = runSessionGuarded();
            finishSession(end);
            if (stopRequested()) break;
            int delay = 0;
            if (end.immediate_reconnect && immediate_budget_ > 0) {
                --immediate_budget_;
            } else {
                delay = backoffDelayMs(attempt_, cfg_.reconnect_initial_ms, cfg_.reconnect_max_ms, jitter());
                if (attempt_ < 30) ++attempt_;
            }
            noteEvent(delay == 0 ? std::string("reconnecting now")
                                 : "reconnecting in " + std::to_string(delay) + " ms");
            if (delay > 0 && !sleepUntil(Clock::now() + std::chrono::milliseconds(delay))) break;
        }
    }

    // 可被 stop() 打断的等待；返回 false 表示已请求停止
    // Wait that stop() can interrupt; returns false if a stop has been requested
    bool sleepUntil(Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex_);
        return !cv_.wait_until(lock, deadline, [this] { return stopRequested(); });
    }

    SessionEnd runSessionGuarded() {
        session_ = SessionState{};
        SessionEnd end;
        try {
            end = runSession();
        } catch (const std::exception& e) {
            end = fail(std::string("unexpected error during ") + stageName() + ": " + e.what());
        } catch (...) {
            end = fail(std::string("unexpected non-standard exception during ") + stageName());
        }
        // TCP 已连上、但 TLS / WebSocket 握手超时：下一次从下一个地址开始（TCP 连接超时由 connectSocket 自己轮转）
        // TCP connected, but the TLS / WebSocket handshake timed out: start from the next address next time
        // (connectSocket rotates by itself on TCP connect timeouts)
        if (session_.stage >= Stage::TlsHandshake && !established() &&
            registry_.reason() == hft::net::KickReason::ConnectTimeout) {
            ++ws_cursor_;
        }
        return end;
    }

    // 会话结束后的清理与上报（此时套接字已注销并关闭）
    // Cleanup and reporting after a session ends (the socket has already been unregistered and closed)
    void finishSession(const SessionEnd& end) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            phase_ = Phase::Idle;
        }
        connected_since_ns_.store(0, std::memory_order_relaxed);
        publisher_.endSession();
        copyAdapterStats();
        if (stopRequested()) return;
        status_.store(FeedStatus::Reconnecting, std::memory_order_release);
        if (!established()) connect_failures_.fetch_add(1, std::memory_order_relaxed);
        noteError(end.message);
        if (cfg_.log_to_stderr) {  // 整行一次写出：多个 feed 线程的输出不会交错在一行里
                                   // write the whole line at once: output of several feed threads never
                                   // interleaves within one line
            const std::string line = "[" + cfg_.name + "] " + end.message + "\n";
            std::cerr.write(line.data(), static_cast<std::streamsize>(line.size()));
            std::cerr.flush();
        }
    }

    // ---------------- 建立连接（同步）----------------
    // ---------------- Connection setup (synchronous) ----------------

    SessionEnd runSession() {
        const std::string where = cfg_.host + ":" + cfg_.port;
        noteEvent("connecting to wss://" + where + cfg_.path);  // 先记日志再开始计时：日志时刻 <= 连接计时起点
                                                                // log before starting the clock: log time <= start
                                                                // of the connect timing
        beginConnecting();

        session_.stage = Stage::TlsSetup;
        if (!ssl_ctx_) {
            try {
                ssl_ctx_ = hft::net::makeClientContext(hft::net::TlsOptions{cfg_.tls_verify, cfg_.ca_file});
            } catch (const std::exception& e) {
                return fail(e.what());
            }
        }

        session_.stage = Stage::Resolve;
        Endpoints endpoints;
        const error_code ec =
            ws_resolver_.resolve(cfg_.host, cfg_.port, endpoints, [this] { return registry_.interrupted(); });
        if (ec) return fail("resolve " + where + ": " + ec.message());

        Connection c(*ssl_ctx_);
        if (std::optional<SessionEnd> end = openWebSocket(c, endpoints, where)) return std::move(*end);
        if (!beginStreaming()) return fail("interrupted");  // 连接超时恰好在握手完成时触发
                                                            // connect timeout fired just as the handshake completed
        noteEvent("connected to wss://" + where + cfg_.path);

        for (const std::string& frame : adapter_->onConnected()) {
            error_code wec;
            c.ws.write(boost::asio::buffer(frame), wec);  // 同步写：异步读还没有开始
                                                          // synchronous write: the async read has not started yet
            if (wec) return fail("send subscribe frame: " + wec.message());
        }
        session_.stage = Stage::Read;
        read_buffer_.clear();
        return streamLoop(c);
    }

    // TCP 连接（每个地址有自己的时限，见 connectSocket）→ TLS 握手 → WebSocket 握手；失败时返回结束原因
    // TCP connect (each address has its own time limit, see connectSocket) → TLS handshake → WebSocket
    // handshake; on failure, returns the end reason
    std::optional<SessionEnd> openWebSocket(Connection& c, const Endpoints& endpoints, const std::string& where) {
        namespace websocket = boost::beast::websocket;
        session_.stage = Stage::TcpConnect;
        hft::net::ConnectOptions copt;
        copt.timeout_ms = remainingConnectMs();
        copt.cursor = &ws_cursor_;
        Socket& sock = boost::beast::get_lowest_layer(c.ws);
        error_code ec = hft::net::connectSocket(sock, endpoints, registry_, c.registration, copt);
        if (ec == boost::asio::error::timed_out && !registry_.interrupted()) return connectTimeoutEnd();
        if (ec) return fail("connect " + where + ": " + ec.message());
        sock.set_option(boost::asio::ip::tcp::no_delay(true), ec);

        session_.stage = Stage::TlsHandshake;
        try {
            hft::net::prepareTlsStream(c.ws.next_layer(), cfg_.host, cfg_.tls_verify);
        } catch (const std::exception& e) {
            return fail(e.what());
        }
        c.ws.next_layer().handshake(boost::asio::ssl::stream_base::client, ec);
        if (ec) {
            return fail(hft::net::describeHandshakeError(c.ws.next_layer().native_handle(), ec, cfg_.host,
                                                         cfg_.tls_verify));
        }

        session_.stage = Stage::WsHandshake;
        c.ws.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(boost::beast::http::field::user_agent, hft::net::kUserAgent);
        }));
        c.ws.read_message_max(adapter_detail::kMaxMessageBytes);
        websocket::response_type res;
        c.ws.handshake(res, hft::net::hostHeader(cfg_.host, cfg_.port), cfg_.path, ec);
        if (ec == websocket::error::upgrade_declined) {
            return fail("WebSocket upgrade to " + where + cfg_.path + " rejected: HTTP " +
                        std::to_string(res.result_int()) + " " + std::string(res.reason()) +
                        feed_detail::bodySnippet(res.body()));
        }
        if (ec) return fail("WebSocket handshake with " + where + " failed: " + ec.message());
        c.ws.text(true);
        return std::nullopt;
    }

    // 新会话开始：重新武装登记表并进入 Connecting（与 checkHealth 同一把锁，看门狗不会踢错会话）
    // New session: re-arm the registry and enter Connecting (under the same lock as checkHealth, so the
    // watchdog never kicks the wrong session)
    void beginConnecting() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            registry_.rearm();
            phase_ = Phase::Connecting;
            phase_start_ns_ = hft::nowNs();
        }
        status_.store(FeedStatus::Connecting, std::memory_order_release);
    }

    // 连接阶段剩余的时限（毫秒，向上取整，至少 1）；connect_timeout_ms <= 0 时为 0（不限）。
    // phase_start_ns_ 只由 feed 线程写，这里在 feed 线程上读，不需要加锁。
    // Time left in the connect phase (milliseconds, rounded up, at least 1); 0 (no limit) when
    // connect_timeout_ms <= 0.
    // phase_start_ns_ is written only by the feed thread and read here on the feed thread: no lock needed.
    int remainingConnectMs() const noexcept {
        if (cfg_.connect_timeout_ms <= 0) return 0;
        const std::int64_t left = feed_detail::msToNs(cfg_.connect_timeout_ms) - (hft::nowNs() - phase_start_ns_);
        if (left <= 0) return 1;
        return static_cast<int>(std::min<std::int64_t>((left + 999999) / 1000000, cfg_.connect_timeout_ms));
    }

    // 握手完成：进入 Streaming 与 Syncing。若此前已被踢断（连接超时恰好在握手完成时触发）则返回 false。
    // Handshake done: enter Streaming and Syncing. Returns false if already kicked (the connect timeout fired
    // just as the handshake completed).
    bool beginStreaming() {
        const std::int64_t now = hft::nowNs();
        activity_ns_.store(now, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (registry_.interrupted()) return false;
            phase_ = Phase::Streaming;
        }
        session_.stage = Stage::Subscribe;
        connected_since_ns_.store(now, std::memory_order_relaxed);
        status_.store(FeedStatus::Syncing, std::memory_order_release);
        return true;
    }

    // ---------------- 流式阶段（异步读 + 写队列，全部在 feed 线程上）----------------
    // ---------------- Streaming phase (async read + write queue, all on the feed thread) ----------------

    // 在本线程上驱动异步读写，直到某个处理器决定结束会话。心跳与待发布快照的重试不用定时器：run_one_until 在下一次
    // 检查的时刻（wake_ns）返回，到期的动作由 runTimers 执行。处理器只在这里与 closeConnection() 里执行，都在 c 的
    // 生命周期之内，所以可以按引用捕获 c。
    // Drive async reads and writes on this thread until some handler decides to end the session. Heartbeats
    // and pending snapshot retries use no timer: run_one_until returns at the next check time (wake_ns), and
    // runTimers performs the actions that are due. Handlers run only here and in closeConnection(), both
    // within the lifetime of c, so they may capture c by reference.
    SessionEnd streamLoop(Connection& c) {
        const std::int64_t start = hft::nowNs();
        c.keepalive.start(adapter_->pingMessage(), cfg_.ping_interval_ms, adapter_->pingIntervalMs(),
                          cfg_.stall_timeout_ms, start);
        c.ws.control_callback(
            [this](boost::beast::websocket::frame_type kind, boost::beast::string_view) { onControlFrame(kind); });
        startRead(c);
        c.wake_ns = runTimers(c, start);
        while (!c.end) {
            const std::size_t ran =
                c.wake_ns == feed_detail::kNever ? c.ioc.run_one() : c.ioc.run_one_until(steadyTime(c.wake_ns));
            if (ran > 0) continue;
            if (c.ioc.stopped()) {  // 不应发生：会话进行中总有一个挂起的读
                                    // should not happen: a session in progress always has a pending read
                endSession(c, SessionEnd{"internal error: no pending network operation"});
            } else {
                c.wake_ns = runTimers(c, hft::nowNs());
            }
        }
        closeConnection(c);
        return std::move(*c.end);
    }

    static Clock::time_point steadyTime(std::int64_t ns) noexcept {
        return Clock::time_point(std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(ns)));
    }

    // 执行到期的定时动作（待发布快照每 kPendingRetryMs 重试一次；心跳），返回下一次需要检查的时刻（至少 1 ms 之后；
    // kNever = 不需要）。消息到达时不重算：到期时再按最新的活动时刻计算，提前醒来只是什么也不做。
    // Run the timed actions that are due (the pending snapshot is retried every kPendingRetryMs; heartbeats)
    // and return the next time a check is needed (at least 1 ms later; kNever = none). Not recomputed when a
    // message arrives: it is computed from the latest activity time when it comes due, and waking up early
    // just does nothing.
    std::int64_t runTimers(Connection& c, std::int64_t now) {
        if (c.retry_ns != 0 && now >= c.retry_ns) {
            publisher_.retryPending();
            c.retry_ns = publisher_.pending() ? now + feed_detail::msToNs(kPendingRetryMs) : 0;
        }
        const std::int64_t last_rx = activity_ns_.load(std::memory_order_relaxed);
        if (c.keepalive.take(last_rx, now)) {
            feed_detail::bump(pings_);
            const std::string& text = c.keepalive.text();
            enqueueWrite(c, text.empty() ? WriteKind::WsPing : WriteKind::AppPing, text);
        }
        std::int64_t wake = c.keepalive.dueNs(last_rx);
        if (c.retry_ns != 0) wake = std::min(wake, c.retry_ns);
        return wake == feed_detail::kNever ? wake : std::max(wake, now + kMinWaitNs);
    }

    // 会话结束：先注销、再关闭套接字（挂起的读写以 operation_aborted 完成），然后执行完已就绪的处理器（它们看到
    // end 已设置，什么也不做）。仍未完成的操作随 io_context 析构而销毁、不会被调用。
    // Session end: unregister first, then close the socket (pending reads and writes complete with
    // operation_aborted), then run the handlers that are ready (they see end set and do nothing). Operations
    // still outstanding are destroyed with the io_context and never called.
    void closeConnection(Connection& c) noexcept {
        c.registration.release();
        error_code ignored;
        boost::beast::get_lowest_layer(c.ws).close(ignored);
        try {
            c.ioc.restart();
            c.ioc.poll();
        } catch (...) {
        }
    }

    // 第一个结束原因生效（例如写失败之后读也会以错误完成）
    // The first end reason wins (e.g. after a failed write, the read also completes with an error)
    static void endSession(Connection& c, SessionEnd end) {
        if (!c.end) c.end = std::move(end);
    }

    // 让异步操作的内存都来自 handler_memory_
    // Make all memory for async operations come from handler_memory_
    template <class Handler>
    auto bindHandler(Handler&& handler) {
        return boost::asio::bind_allocator(feed_detail::HandlerAllocator<void>(handler_memory_),
                                           std::forward<Handler>(handler));
    }

    void startRead(Connection& c) {
        c.ws.async_read(read_buffer_, bindHandler([this, &c](const error_code& ec, std::size_t) { onRead(c, ec); }));
    }

    void onRead(Connection& c, const error_code& ec) {
        const std::int64_t recv_ns = hft::nowNs();
        if (c.end) return;
        if (ec) return endSession(c, readFailed(ec, c.ws));
        const auto data = read_buffer_.cdata();
        const std::string_view payload(static_cast<const char*>(data.data()), data.size());
        countMessage(payload.size(), recv_ns);
        out_.reset();
        adapter_->onMessage(payload, out_);  // 适配器把消息复制进自己的缓冲区，之后不再引用 payload
                                             // the adapter copies the message into its own buffer and no longer
                                             // refers to payload afterwards
        read_buffer_.consume(read_buffer_.size());
        copyAdapterStats();
        std::optional<SessionEnd> end = processResult(c, out_, recv_ns);
        if (!end && out_.need_rest_snapshot) end = fetchRestSnapshot(c);
        if (end) return endSession(c, std::move(*end));
        if (publisher_.pending() && c.retry_ns == 0) {  // 待发布快照只在处理消息时产生
                                                        // a pending snapshot only arises while handling a message
            c.retry_ns = hft::nowNs() + feed_detail::msToNs(kPendingRetryMs);
            c.wake_ns = std::min(c.wake_ns, c.retry_ns);
        }
        startRead(c);
    }

    // WebSocket 控制帧（在异步读内部回调）：pong 与服务器的 ping 都证明连接活着，算作活动
    // WebSocket control frames (called back from inside the async read): pongs and server pings both prove
    // the connection is alive and count as activity
    void onControlFrame(boost::beast::websocket::frame_type kind) noexcept {
        if (kind == boost::beast::websocket::frame_type::close) return;
        const std::int64_t now = hft::nowNs();
        activity_ns_.store(now, std::memory_order_relaxed);
        if (kind == boost::beast::websocket::frame_type::pong) feed_detail::bump(pongs_);
        updateHealthy(now);
    }

    // 写队列：任何时刻最多一个写操作（async_write / async_ping）在进行
    // Write queue: at most one write operation (async_write / async_ping) in progress at any time
    void enqueueWrite(Connection& c, WriteKind kind, std::string_view text) {
        if (c.end) return;
        if (c.write_end == c.writes.size()) c.writes.emplace_back();
        QueuedWrite& w = c.writes[c.write_end++];
        w.kind = kind;
        w.text.assign(text.data(), text.size());
        if (!c.writing) startNextWrite(c);
    }

    void startNextWrite(Connection& c) {
        if (c.write_next == c.write_end) {  // 队列已空：复位下标
                                            // queue empty: reset the indices
            c.write_next = 0;
            c.write_end = 0;
            return;
        }
        QueuedWrite& w = c.writes[c.write_next++];
        c.inflight.kind = w.kind;
        c.inflight.text.swap(w.text);  // 交换而不是复制：两边的容量都保留下来
                                       // swap rather than copy: both sides keep their capacity
        c.writing = true;
        auto done = bindHandler([this, &c](const error_code& ec, auto&&...) { onWritten(c, ec); });
        if (c.inflight.kind == WriteKind::WsPing) {
            c.ws.async_ping(boost::beast::websocket::ping_data(), std::move(done));
        } else {
            c.ws.async_write(boost::asio::buffer(c.inflight.text), std::move(done));
        }
    }

    void onWritten(Connection& c, const error_code& ec) {
        c.writing = false;
        const bool ping = c.inflight.kind != WriteKind::Text;
        if (ping) c.keepalive.written();
        if (c.end) return;
        if (ec) return endSession(c, fail((ping ? "send ping: " : "send frame: ") + ec.message()));
        startNextWrite(c);
    }

    // 处理一次适配器结果；需要结束会话时返回结束原因。先更新状态（SYNCING/LIVE）、记录交易所延迟，再发布：
    // 消费者取到快照时看到的 status() 与延迟统计至少与该快照一样新。
    // Handle one adapter result; returns the end reason when the session must end. Update the status
    // (SYNCING/LIVE) and record the exchange latency first, then publish: the status() and latency stats the
    // consumer sees when it takes a snapshot are at least as new as that snapshot.
    std::optional<SessionEnd> processResult(Connection& c, const AdapterResult& out, std::int64_t recv_ns) {
        updateSyncState(recv_ns);
        if (!out.error.empty()) noteError(out.error);
        for (const std::string& frame : out.send) enqueueWrite(c, WriteKind::Text, frame);
        if (out.exchange_ts_ms > 0) recordExchangeLatency(out.exchange_ts_ms);
        if (out.book_updated) publish(recv_ns, out.exchange_ts_ms);
        if (!out.reconnect) return std::nullopt;
        return SessionEnd{out.error.empty() ? std::string("exchange adapter requested a reconnect") : out.error, true};
    }

    // 拉取 REST 快照直到适配器满意：HTTPS GET rest_host:rest_port/snapshot_path（与 WebSocket 同样的校验规则，响应体
    // 上限 16 MiB），200 的响应体交给 onRestSnapshot，非 200 / 传输失败视为失败，适配器继续要求时重试。每个会话最多
    // rest_max_attempts 次请求，用完即结束会话（之后按退避重连）；簿连续同步 healthy_session_ms 之后额度恢复（长时间
    // 运行的会话偶尔出现缺口不会因此重连，见 updateHealthy）。请求之间的间隔见 waitRestSpacing：持续出现缺口的上游
    // 不会让我们密集地请求 REST。
    // Fetch REST snapshots until the adapter is satisfied: HTTPS GET rest_host:rest_port/snapshot_path (same
    // verification rules as the WebSocket, response body capped at 16 MiB); a 200 body goes to
    // onRestSnapshot, a non-200 / transport error counts as a failure, and the request is retried while the
    // adapter keeps asking. At most rest_max_attempts requests per session; when they run out, the session
    // ends (and reconnects with backoff); the budget is restored once the book has stayed synced for
    // healthy_session_ms (so occasional gaps in a long-running session do not cause reconnects, see
    // updateHealthy). For the spacing between requests see waitRestSpacing: an upstream that keeps producing
    // gaps cannot make us hammer REST.
    std::optional<SessionEnd> fetchRestSnapshot(Connection& c) {
        if (cfg_.rest_host.empty() || cfg_.snapshot_path.empty()) {
            return SessionEnd{"REST snapshot required, but rest_host/snapshot_path are not configured"};
        }
        session_.stage = Stage::Rest;
        std::string problem = out_.error;  // 触发这一轮的原因（例如缺口）；out_ 之后用来接收 REST 快照的结果
                                           // what triggered this round (e.g. a gap); out_ is then used to receive
                                           // the REST snapshot result
        for (;;) {
            if (session_.rest_requests >= cfg_.rest_max_attempts) {
                return SessionEnd{"REST snapshot request limit reached (rest_max_attempts " +
                                  std::to_string(cfg_.rest_max_attempts) + "), reconnecting" +
                                  (problem.empty() ? std::string() : " (last error: " + problem + ")")};
            }
            if (std::optional<SessionEnd> end = waitRestSpacing()) return end;
            ++session_.rest_requests;
            session_.last_rest_ns = hft::nowNs();
            activity_ns_.store(session_.last_rest_ns, std::memory_order_relaxed);
            const hft::net::HttpResult http =
                hft::net::httpsGet(rest_ioc_, *ssl_ctx_, rest_resolver_, registry_, rest_request_);
            const std::int64_t recv_ns = hft::nowNs();
            activity_ns_.store(recv_ns, std::memory_order_relaxed);
            if (registry_.interrupted()) return interruptedEnd();
            if (http.ok && http.status == 200) {
                out_.reset();
                adapter_->onRestSnapshot(http.body, out_);
                copyAdapterStats();
                // 已记录 out_.error
                // out_.error recorded
                if (std::optional<SessionEnd> end = processResult(c, out_, recv_ns)) return end;
                if (!out_.need_rest_snapshot) {
                    session_.rest_failures = 0;
                    session_.stage = Stage::Read;
                    return std::nullopt;
                }
                problem = out_.error.empty() ? std::string("exchange adapter asked for another REST snapshot")
                                             : out_.error;
            } else {
                problem = http.ok ? "REST snapshot request returned HTTP " + std::to_string(http.status) + " " +
                                        http.reason + feed_detail::bodySnippet(http.body)
                                  : "REST snapshot request failed: " + http.error;
                noteError(problem);
            }
            ++session_.rest_failures;
        }
    }

    // 同一会话内两次 REST 请求的开始时刻至少相隔 rest_min_interval_ms；连续失败 k 次之后为它的 2^(k-1) 倍
    // （±20% 抖动，上限 reconnect_max_ms）。本会话的第一次请求不等待。等待期间不读 WebSocket：每次等待
    // 不超过停顿阈值的一半，避免看门狗把这段等待当成停顿。有待发布的快照时分片等待，每片之后重试入队。
    // Within one session, two REST requests start at least rest_min_interval_ms apart; after k consecutive
    // failures, 2^(k-1) times that (±20% jitter, capped at reconnect_max_ms). The session's first request does
    // not wait. The WebSocket is not read while waiting: each wait is at most half the stall threshold, so the
    // watchdog does not take the wait for a stall. With a pending snapshot the wait is split into slices, and
    // the enqueue is retried after each slice.
    std::optional<SessionEnd> waitRestSpacing() {
        if (session_.last_rest_ns == 0) return std::nullopt;
        const int spacing = backoffDelayMs(std::max(0, session_.rest_failures - 1), cfg_.rest_min_interval_ms,
                                           cfg_.reconnect_max_ms, jitter());
        const std::int64_t wait_ns = session_.last_rest_ns + feed_detail::msToNs(spacing) - hft::nowNs();
        if (wait_ns <= 0) return std::nullopt;
        const int wait_ms = feed_detail::minPositive(
            static_cast<int>(std::min<std::int64_t>((wait_ns + 999999) / 1000000, spacing)),
            feed_detail::halfStallMs(cfg_.stall_timeout_ms));
        const Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(wait_ms);
        for (Clock::time_point now = Clock::now(); now < deadline; now = Clock::now()) {
            const Clock::time_point retry = now + std::chrono::milliseconds(kPendingRetryMs);
            // 已请求停止
            // asked to stop
            if (!sleepUntil(publisher_.pending() ? std::min(deadline, retry) : deadline)) return SessionEnd{};
            publisher_.retryPending();
        }
        if (registry_.interrupted()) return interruptedEnd();
        return std::nullopt;
    }

    // ---------------- 会话结束的原因 ----------------
    // ---------------- Session end reasons ----------------

    SessionEnd readFailed(const error_code& ec, WsStream& ws) const {
        namespace websocket = boost::beast::websocket;
        if (registry_.interrupted()) return interruptedEnd();
        if (ec == websocket::error::closed) {
            const websocket::close_reason& r = ws.reason();
            std::string msg = "server closed the WebSocket (code " + std::to_string(r.code);
            if (!r.reason.empty()) msg += ": " + std::string(r.reason.data(), r.reason.size());
            return SessionEnd{msg + ")"};
        }
        if (ec == websocket::error::message_too_big) {
            return SessionEnd{"message larger than " + std::to_string(adapter_detail::kMaxMessageBytes) +
                              " bytes, connection dropped"};
        }
        return SessionEnd{"connection lost: " + ec.message()};
    }

    // 失败：若是被踢断导致的，改用踢断原因描述
    // Failure: if it was caused by a kick, describe the kick reason instead
    SessionEnd fail(std::string message) const {
        return registry_.interrupted() ? interruptedEnd() : SessionEnd{std::move(message)};
    }

    SessionEnd interruptedEnd() const {
        switch (registry_.reason()) {
            case hft::net::KickReason::Stop: return SessionEnd{};
            case hft::net::KickReason::Stall:
                return SessionEnd{"stalled: no data for more than " + std::to_string(cfg_.stall_timeout_ms) +
                                  " ms during " + stageName() + ", connection kicked"};
            case hft::net::KickReason::ConnectTimeout: return connectTimeoutEnd();
            case hft::net::KickReason::None: break;
        }
        return SessionEnd{std::string("interrupted during ") + stageName()};
    }

    // 连接阶段超时（看门狗踢断，或 connectSocket 自己用完了时限）
    // Connect phase timeout (kicked by the watchdog, or connectSocket used up its time limit by itself)
    SessionEnd connectTimeoutEnd() const {
        return SessionEnd{std::string("connect timeout: ") + stageName() + " did not complete within " +
                          std::to_string(cfg_.connect_timeout_ms) + " ms"};
    }

    // ---------------- 每条消息的记账与发布（只有 feed 线程写）----------------
    // ---------------- Per-message bookkeeping and publishing (only the feed thread writes) ----------------

    void countMessage(std::size_t bytes, std::int64_t recv_ns) noexcept {
        feed_detail::bump(messages_);
        feed_detail::bump(bytes_, bytes);
        last_message_ns_.store(recv_ns, std::memory_order_relaxed);
        activity_ns_.store(recv_ns, std::memory_order_relaxed);
    }

    void copyAdapterStats() noexcept {
        constexpr auto r = std::memory_order_relaxed;
        const AdapterStats& a = adapter_->stats();
        book_updates_.store(a.book_updates, r);
        gaps_.store(a.gaps, r);
        checksum_failures_.store(a.checksum_failures, r);
        resyncs_.store(a.resyncs, r);
        rest_snapshots_.store(a.rest_snapshots, r);
        parse_errors_.store(a.parse_errors, r);
    }

    void updateSyncState(std::int64_t now) {
        const bool synced = adapter_->synced();
        if (synced != session_.synced) {
            session_.synced = synced;
            session_.live_since_ns = synced ? now : 0;
            status_.store(synced ? FeedStatus::Live : FeedStatus::Syncing, std::memory_order_release);
            noteEvent(synced ? "book synced (LIVE)" : "book lost sync, resyncing");
        }
        updateHealthy(now);
    }

    // 会话连续 Live 超过 healthy_session_ms：退避计数清零（每个会话一次）。簿自上次（重新）同步以来已连续同步
    // 这么久，本会话的 REST 额度也恢复。每次收到帧（消息、控制帧）都检查，所以只有心跳往来的安静会话同样会被
    // 认定为健康。
    // Session continuously Live for longer than healthy_session_ms: reset the backoff count (once per session).
    // The book has then stayed synced that long since the last (re)sync, so this session's REST budget is
    // restored as well. Checked on every frame received (messages, control frames), so a quiet session with
    // only heartbeat traffic is considered healthy too.
    void updateHealthy(std::int64_t now) noexcept {
        if (!session_.synced || now - session_.live_since_ns < feed_detail::msToNs(cfg_.healthy_session_ms)) return;
        if (!session_.healthy) {
            session_.healthy = true;
            attempt_ = 0;
            immediate_budget_ = kMaxImmediateReconnects;
        }
        session_.rest_requests = 0;
        session_.rest_failures = 0;
    }

    // 发布当前的簿；处理延迟在入队之前记录（取到快照即可见）
    // Publish the current book; the processing latency is recorded before the enqueue (visible as soon as the
    // snapshot is taken)
    void publish(std::int64_t recv_ns, std::int64_t exchange_ts_ms) noexcept {
        BookSnapshot& s = publisher_.next();
        s.assignFrom(adapter_->book(), cfg_.publish_levels);
        s.exchange_ts_ms = exchange_ts_ms;
        s.recv_ns = recv_ns;
        s.publish_ns = hft::nowNs();
        latency_.recordNs(static_cast<std::uint64_t>(std::max<std::int64_t>(0, s.publish_ns - recv_ns)));
        publisher_.push();
    }

    void recordExchangeLatency(std::int64_t exchange_ts_ms) noexcept {
        constexpr std::int64_t kMaxMs = std::numeric_limits<std::int64_t>::max() / 1000000;
        const std::int64_t lag_ms = std::clamp<std::int64_t>(hft::wallClockMs() - exchange_ts_ms, 0, kMaxMs);
        exchange_latency_.recordNs(static_cast<std::uint64_t>(lag_ms) * 1000000);
    }

    // ---------------- 日志 ----------------
    // ---------------- Logging ----------------

    // 日志回调（参数 "[name] message"，异常被吞掉）；noteError 同时记为 lastError()
    // Log callback (argument "[name] message", exceptions swallowed); noteError also records it as lastError()
    void noteEvent(const std::string& message) { log(message, false); }
    void noteError(const std::string& message) { log(message, true); }

    void log(const std::string& message, bool is_error) {
        std::function<void(const std::string&)> cb;
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            if (is_error) last_error_ = message;
            cb = log_cb_;
        }
        if (!cb) return;
        try {
            cb("[" + cfg_.name + "] " + message);
        } catch (...) {
        }
    }

    // ---------------- 成员 ----------------
    // ---------------- Members ----------------

    const FeedConfig cfg_;
    std::unique_ptr<ExchangeAdapter> adapter_;   // feed 线程私有
                                                 // private to the feed thread
    feed_detail::SnapshotPublisher publisher_;   // 快照队列与发布计数
                                                 // snapshot queue and publish counters
    LatencyTracker latency_;                     // 单写者（feed 线程），任意线程读
                                                 // single writer (feed thread), read from any thread
    LatencyTracker exchange_latency_;

    // 跨线程状态
    // Cross-thread state
    std::atomic<FeedStatus> status_{FeedStatus::Idle};
    std::atomic<bool> stop_requested_{false};
    hft::net::SocketRegistry registry_;
    mutable std::mutex mutex_;                   // 保护 phase_ / phase_start_ns_，并用于退避等待
                                                 // guards phase_ / phase_start_ns_, also used for the backoff wait
    std::condition_variable cv_;
    Phase phase_ = Phase::Idle;
    std::int64_t phase_start_ns_ = 0;            // 进入 Connecting 的时刻
                                                 // time Connecting was entered
    std::atomic<std::int64_t> activity_ns_{0};   // 看门狗的"最近活动"时刻（收到的任何帧、REST 请求）
                                                 // the watchdog's "last activity" time (any frame
                                                 // received, REST requests)
    mutable std::mutex error_mutex_;             // 保护 last_error_ / log_cb_
                                                 // guards last_error_ / log_cb_
    std::string last_error_;
    std::function<void(const std::string&)> log_cb_;

    std::atomic<std::uint64_t> messages_{0}, bytes_{0}, book_updates_{0}, reconnects_{0}, connect_failures_{0},
        stalls_{0}, gaps_{0}, checksum_failures_{0}, resyncs_{0}, rest_snapshots_{0}, parse_errors_{0}, pings_{0},
        pongs_{0};
    std::atomic<std::int64_t> last_message_ns_{0}, connected_since_ns_{0};

    // 以下只由 feed 线程访问
    // The following are accessed only by the feed thread
    feed_detail::HandlerMemory handler_memory_;   // 异步操作的内存（会话的 io_context 是局部对象，先于它销毁）
                                                  // memory for async operations (the session's io_context is a local
                                                  // object, destroyed before this one)
    boost::asio::io_context rest_ioc_;            // REST 请求用：只做同步操作，从不 run()
                                                  // for REST requests: synchronous operations only, never run()
    std::unique_ptr<boost::asio::ssl::context> ssl_ctx_;   // 首次连接时创建，之后各会话（含 REST）复用
                                                           // created on the first connect, then reused by all sessions
                                                           // (REST included)
    hft::net::Resolver ws_resolver_;
    hft::net::Resolver rest_resolver_;
    std::size_t ws_cursor_ = 0;                   // WebSocket 地址轮转游标（见 hft::net::ConnectOptions::cursor）
                                                  // WebSocket address rotation cursor (see
                                                  // hft::net::ConnectOptions::cursor)
    std::size_t rest_cursor_ = 0;                 // REST 地址轮转游标
                                                  // REST address rotation cursor
    const hft::net::HttpsGetRequest rest_request_;
    boost::beast::flat_buffer read_buffer_;       // 跨消息、跨会话复用（只增不减）
                                                  // reused across messages and sessions (only grows)
    AdapterResult out_;                           // 复用：每条消息与每个 REST 响应之前 reset()
                                                  // reused: reset() before every message and every REST response
    std::mt19937_64 rng_;
    int attempt_ = 0;                             // 退避指数
                                                  // backoff exponent
    int immediate_budget_ = kMaxImmediateReconnects;  // 剩余的立即重连次数
                                                      // immediate reconnects left
    SessionState session_;
};
