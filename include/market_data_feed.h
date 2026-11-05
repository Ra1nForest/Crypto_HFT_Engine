//
// market_data_feed.h
// 行情连接管理器：每个 feed 一个线程（Boost.Beast：同步建立连接，流式阶段在本线程上驱动异步读与心跳定时器）
// 驱动交易所协议适配器，负责连接、心跳、退避重连、停顿踢断、REST 快照，并经 SPSC 队列向主线程发布 BookSnapshot。
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

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/wait_traits.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 交易所枚举 Exchange 见 exchanges/exchange_adapter.h

// 连接状态
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
struct FeedConfig {
    Exchange exchange = Exchange::BINANCE;
    std::uint32_t venue_id = 0;                // 写入每个 BookSnapshot::venue
    std::string name, host, port, path, symbol, subscribe_msg;
    std::string rest_host, rest_port = "443", snapshot_path;   // Binance 增量深度模式的 REST 快照
    bool tls_verify = true;          // 校验服务器证书链与主机名
    std::string ca_file;             // 额外信任的 CA（PEM，测试用）；"" = 只用系统默认 CA
    bool verify_checksum = true;     // OKX books checksum
    int stall_timeout_ms = 15000;    // 已连接但这么久没有收到任何帧（含心跳回应）→ 踢断连接（checkHealth；<= 0 关闭）
    int reconnect_initial_ms = 500;  // 指数退避（±20% 抖动），健康会话后重置
    int reconnect_max_ms = 30000;
    int publish_levels = BookSnapshot::kMaxLevels;   // 每侧发布的档位数（钳制到 [0, kMaxLevels]）
    std::size_t queue_capacity = 1024;               // SPSC 队列容量（向上取整到 2 的幂；最后一个槽位留给空簿）

    // ---- 以下为扩展参数：默认值即生产行为，测试用来缩短时间 ----
    int connect_timeout_ms = 10000;    // 解析 + TCP + TLS + WebSocket 握手的总时限（checkHealth 踢断；<= 0 关闭）；
                                       // TCP 连接时每个地址另有 max(1 s, 剩余时限 / 地址数) 的时限
    int ping_interval_ms = -1;         // 心跳间隔：< 0 用适配器默认值（OKX/Bybit 20000，Binance 无），0 关闭心跳，
                                       // > 0 覆盖。空闲 min(间隔, stall_timeout_ms / 2) 即发心跳（见类注释）
    int healthy_session_ms = 30000;    // 会话连续 LIVE 超过这么久即视为健康：退避计数清零
    int rest_max_attempts = 5;         // 每个会话最多 REST 快照请求次数，用完即结束会话重连；簿连续同步
                                       // healthy_session_ms 之后额度恢复
    int rest_min_interval_ms = 1000;   // 同一会话内两次 REST 快照请求（开始时刻）的最小间隔；连续失败时按 2 倍
                                       // 递增（±20% 抖动，上限 reconnect_max_ms；每次等待不超过 stall_timeout_ms / 2）
    std::size_t max_buffered_events = 2000;   // Binance 同步前事件缓冲上限（AdapterConfig）
    bool log_to_stderr = true;         // 会话结束的错误是否写 stderr（测试可关闭）
};

// 统计快照：各字段是 relaxed 原子量的拷贝，任意线程可读（字段之间不保证同一时刻）
struct FeedStats {
    std::uint64_t messages = 0, bytes = 0, book_updates = 0, published = 0, dropped_snapshots = 0, reconnects = 0,
                  connect_failures = 0, stalls = 0, gaps = 0, checksum_failures = 0, resyncs = 0, rest_snapshots = 0,
                  parse_errors = 0;
    std::int64_t last_message_ns = 0;     // 最后一条消息的 hft::nowNs()，无则为 0
    std::int64_t connected_since_ns = 0;  // 当前会话建立（WebSocket 握手完成）的 hft::nowNs()，未连接为 0
    std::uint64_t pings = 0;              // 发出的心跳（应用层 ping 消息或 WebSocket ping 控制帧）
    std::uint64_t pongs = 0;              // 收到的 WebSocket pong 控制帧（应用层的 "pong" 是普通消息，计入 messages）
};

namespace feed_detail {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;

inline std::int64_t msToNs(int ms) noexcept { return static_cast<std::int64_t>(ms) * 1000000; }

// 错误信息里引用响应体的前 200 个字符（单行）
inline std::string bodySnippet(const std::string& body) {
    constexpr std::size_t kMax = 200;
    if (body.empty()) return std::string();
    std::string s = body.substr(0, kMax);
    for (char& c : s) {
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
    }
    return ": " + s + (body.size() > kMax ? "..." : "");
}

// 异步处理器内存：feed 线程私有的小块缓存。asio 自带的 recycling allocator 把缓存放在 run_one() 栈上的
// thread_info 里，每次 run_one() 返回就释放：用 run_one() 驱动会话时，每个异步操作都会访问全局堆。
// 这里的缓存跨 run_one() 与会话保留，稳态下 async_read / 定时器 / 写操作反复使用同几块内存。
// 取最小的够用块；块大小按 128 字节取整、只增不减；缓存里没有够用的块时退回 ::operator new（释放时放回缓存）。
// 不加锁：分配与释放都在 feed 线程上（feed 析构时才释放缓存，那时运行 run() 的线程已经 join）。
class HandlerMemory {
public:
    static constexpr std::size_t kSlots = 8;

    HandlerMemory() = default;
    HandlerMemory(const HandlerMemory&) = delete;
    HandlerMemory& operator=(const HandlerMemory&) = delete;
    ~HandlerMemory() {
        for (void* p : free_) {
            if (p != nullptr) ::operator delete(header(p));
        }
    }

    void* allocate(std::size_t size) {
        std::size_t best = kSlots;
        for (std::size_t i = 0; i < kSlots; ++i) {
            if (free_[i] != nullptr && capacity(free_[i]) >= size &&
                (best == kSlots || capacity(free_[i]) < capacity(free_[best]))) {
                best = i;
            }
        }
        if (best != kSlots) {
            void* p = free_[best];
            free_[best] = nullptr;
            return p;
        }
        if (size > std::numeric_limits<std::size_t>::max() - kRound - kHeader) throw std::bad_alloc();
        const std::size_t cap = (size + kRound - 1) / kRound * kRound;
        void* raw = ::operator new(cap + kHeader);
        *static_cast<std::size_t*>(raw) = cap;
        ++heap_allocations_;
        return static_cast<unsigned char*>(raw) + kHeader;
    }

    void deallocate(void* p) noexcept {
        if (p == nullptr) return;
        std::size_t smallest = kSlots;
        for (std::size_t i = 0; i < kSlots; ++i) {
            if (free_[i] == nullptr) {
                free_[i] = p;
                return;
            }
            if (smallest == kSlots || capacity(free_[i]) < capacity(free_[smallest])) smallest = i;
        }
        if (capacity(free_[smallest]) < capacity(p)) std::swap(free_[smallest], p);  // 缓存已满：留下较大的块
        ::operator delete(header(p));
    }

    // 向全局堆申请过的块数（测试用）
    std::uint64_t heapAllocations() const noexcept { return heap_allocations_; }

private:
    static constexpr std::size_t kHeader = alignof(std::max_align_t);  // 块头存容量；之后的地址保持最大对齐
    static constexpr std::size_t kRound = 128;
    static void* header(void* p) noexcept { return static_cast<unsigned char*>(p) - kHeader; }
    static std::size_t capacity(void* p) noexcept { return *static_cast<std::size_t*>(header(p)); }

    void* free_[kSlots] = {};
    std::uint64_t heap_allocations_ = 0;
};

// 从 HandlerMemory 分配的分配器：用 asio::bind_allocator 关联到完成处理器上，asio 与 Beast 为该处理器的
// 异步操作分配的全部内存（reactor 操作、定时器等待、投递的完成、Beast 的稳定状态与暂停的操作）都经过它
template <class T>
class HandlerAllocator {
public:
    using value_type = T;

    explicit HandlerAllocator(HandlerMemory& memory) noexcept : memory_(&memory) {}
    template <class U>
    HandlerAllocator(const HandlerAllocator<U>& other) noexcept : memory_(other.memory_) {}

    T* allocate(std::size_t n) {
        static_assert(alignof(T) <= alignof(std::max_align_t), "HandlerMemory blocks are max_align_t aligned");
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) throw std::bad_alloc();
        return static_cast<T*>(memory_->allocate(n * sizeof(T)));
    }
    void deallocate(T* p, std::size_t) noexcept { memory_->deallocate(p); }

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
}  // namespace feed_detail

// 行情连接管理器。
// 设计决定：每个 feed 一个线程（简单、延迟低：读到数据立即在本线程处理）。建立连接（解析、TCP、TLS 与
// WebSocket 握手、首批订阅帧）是同步的；流式阶段用一个只属于本会话的 io_context，在本线程上以 run_one()
// 驱动 ws.async_read 与心跳定时器：连接安静时也能按时发心跳。
// 阻塞调用没有超时，及时退出与停顿恢复靠"踢断"：所有活动套接字登记在 hft::net::SocketRegistry 中，
// stop() 与 checkHealth() 对它们 ::shutdown(SHUT_RDWR)，阻塞的 handshake/read/write 与挂起的异步读写立即失败。
// 每个套接字都是先 open、先登记，再发起非阻塞连接并分片等待（hft::net::connectSocket，每片之间检查踢断状态），
// 踢断落在连接过程的任何时刻都在约 50 ms 内生效；REST 快照的套接字同样登记。注销总在关闭之前
// （见 SocketRegistry 关于 fd 复用的说明）。DNS 解析在后台线程中进行，可被踢断状态中断。
// 多地址主机：每个地址有自己的连接时限 max(1 s, 总时限 / 地址数)，超时换下一个地址；连接超时结束的会话
// 让下一次从下一个地址开始（WebSocket 与 REST 各有一个轮转游标），连上之后下次仍从这个地址开始。
// WebSocket 的总时限是 connect_timeout_ms 的剩余部分；REST 的总时限是 connect_timeout_ms 与
// stall_timeout_ms / 2 中较小的（REST 请求期间停顿看门狗照常计时，连接必须远早于它完成）。
//
// 会话流程：解析 → 打开并登记套接字 → connect → TCP_NODELAY → SNI → （tls_verify：系统 CA + 可选 ca_file、
// verify_peer、host_name_verification）→ TLS 握手 → WebSocket 握手（User-Agent: crypto_hft_engine/2.0）
// → 同步发送 adapter.onConnected() 的帧 → 读循环（异步）。
// 异步读开始之后的全部写（适配器要求发送的帧，含重订阅；心跳）都经同一个写队列依次异步发出：Beast 允许
// 一个读与一个写同时挂起，异步读挂起时不能再同步写。
// 每条消息：recv_ns = 读处理器开始执行的时刻 → adapter.onMessage → 依次处理结果：
//   状态 SYNCING/LIVE 按 adapter.synced() 更新；error → lastError() 与日志回调；send → 按顺序进入写队列；
//   交易所延迟记录；book_updated → 发布快照（空簿同样发布；处理延迟在入队前记录）；
//   因此消费者取到一个快照时，status() 与两个延迟统计都已包含这条消息（队列的 release/acquire），
//   只有 published/dropped 计数在入队之后才更新；
//   reconnect → 结束会话并立即重连（不退避）；need_rest_snapshot → 在读处理器里同步执行 HTTPS GET
//   rest_host:rest_port/snapshot_path（同样的校验规则，响应体上限 16 MiB；其间 io_context 暂停，
//   WebSocket 数据暂存在内核缓冲区），200 的响应体交给 onRestSnapshot，非 200 / 传输失败视为失败；
//   适配器继续要求时重试。每个会话最多 rest_max_attempts 次请求（簿连续同步 healthy_session_ms 之后额度恢复），
//   用完即结束会话重连；同一会话内两次请求至少相隔 rest_min_interval_ms，连续失败时间隔按 2 倍递增。
//   （先发布、后拉 REST：缺口导致的空簿立即让消费者知道，不必等待 REST 往返。）
// 心跳：有应用层心跳的交易所（OKX "ping"、Bybit {"op":"ping"}）在 idle_ping = min(心跳间隔, stall_timeout_ms / 2)
// 内没有收到任何帧时发送 pingMessage()，数据一直在来也至少每个心跳间隔发一次；没有应用层心跳的交易所
// （Binance）空闲同样久之后发 WebSocket ping 控制帧。收到的任何帧都算活动（含文本 pong、pong 控制帧与服务器的
// ping）：刷新停顿看门狗与健康会话判定，所以只有心跳往来的安静会话不会被踢断；对端不回应时看门狗照常踢断。
// 每次最多一个心跳在途。ping_interval_ms = 0 关闭心跳。
// 会话结束（任何原因）时若消费者最后收到的簿非空，再发布一个空簿：断线期间消费者不会继续使用过期价格。
// 这个空簿一定能入队，不需要重试：非空快照入队之后总要留一个空槽位，空簿可以用掉最后一个（见 pushSnapshot；
// 缺口导致的空簿同理）。所以非空快照最多占用 queue_capacity - 1 个槽位。
// 消费者手里是空簿时没能入队的非空快照保持待发布：流式阶段由单独的定时器约每 5 ms 重试一次（会话内 REST 请求
// 之间的等待中同样重试），送出的总是当时的最新状态；会话结束时它随之作废（过期的簿不会在空簿之后送出）。
//
// 状态：建立连接期间 Connecting；握手完成后 Syncing，adapter.synced() 后 Live；会话结束 → Reconnecting
// （错误写 stderr 并存入 lastError()），退避 min(max, initial * 2^attempt)（±20% 抖动，结果钳制到
// [1, max]，可被 stop() 打断）；会话连续 Live 超过 healthy_session_ms（默认 30 s）后 attempt 清零
// （每次活动时检查：消息、控制帧）。
// 适配器要求重连时立即重连；但连续 3 次立即重连之后（期间没有健康会话）改用正常退避，防止服务器
// 刚连上就要求重连时我们无间隔地反复握手。run() 返回后为 Stopped；stop 之后不能再次 run()。
//
// 计数：reconnects = 第一次之后的每次连接尝试；connect_failures = 在 WebSocket 握手完成之前失败的尝试
// （含证书校验失败、连接超时）；stalls = checkHealth 因无数据踢断的会话数。适配器统计每条消息后复制到原子量。
// seq 对每次发布尝试递增（含因队列满被丢弃的），消费者看到 seq 跳号即表示丢了快照；待发布快照的重试沿用原来的 seq。
// 处理延迟（getLatencyTracker）= publish_ns - recv_ns，每次发布记录一次；交易所延迟（getExchangeLatency）
// = wallClockMs() - exchange_ts_ms（纳秒存储，时钟偏差导致的负值记为 0），每条带时间戳的消息记录一次。
//
// 线程模型：run() 在专用线程上调用一次；stop()、status()、stats()、lastError()、延迟统计可在任意线程调用；
// checkHealth() 与 queue() 的消费端只在主线程调用；setPublishCallback() 只能在 run() 之前调用。
// 销毁前必须 stop() 并 join 运行 run() 的线程。
class MarketDataFeed {
public:
    // 连续立即重连的上限（之后改用退避，直到出现健康会话）
    static constexpr int kMaxImmediateReconnects = 3;

    explicit MarketDataFeed(const FeedConfig& cfg)
        : cfg_(sanitize(cfg)),
          adapter_(makeAdapter(cfg_.exchange, adapterConfig(cfg_))),
          queue_(cfg_.queue_capacity),
          latency_(cfg_.name),
          exchange_latency_(cfg_.name + " exchange"),
          rng_(makeSeed()) {
        if (!adapter_) throw std::invalid_argument("MarketDataFeed: unsupported exchange");
    }

    MarketDataFeed(const MarketDataFeed&) = delete;
    MarketDataFeed& operator=(const MarketDataFeed&) = delete;

    // 线程入口：connect → stream → reconnect 循环，直到 stop()。不抛异常。
    void run() noexcept {
        try {
            const std::string thread_name = "feed-" + cfg_.name;
            hft::setThreadName(thread_name.c_str());
            runLoop();
        } catch (...) {
            // 只有错误上报路径上的内存分配失败会走到这里：结束线程
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
    void stop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
        registry_.close();
        { std::lock_guard<std::mutex> lock(mutex_); }  // 与 sleepFor 的谓词检查配对，避免丢失唤醒
        cv_.notify_all();
    }

    // 主线程每次刷新调用：已连接但 stall_timeout_ms 没有数据 → stalls++ 并踢断；
    // 连接建立阶段超过 connect_timeout_ms → 踢断（计入 connect_failures）。同一会话只踢一次。
    void checkHealth(std::int64_t now_ns) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (phase_ == Phase::Streaming) {
            if (cfg_.stall_timeout_ms <= 0) return;
            const std::int64_t idle = now_ns - activity_ns_.load(std::memory_order_relaxed);
            if (idle > feed_detail::msToNs(cfg_.stall_timeout_ms) && registry_.kick(hft::net::KickReason::Stall)) {
                stalls_.fetch_add(1, std::memory_order_relaxed);
            }
        } else if (phase_ == Phase::Connecting) {
            if (cfg_.connect_timeout_ms <= 0) return;
            if (now_ns - phase_start_ns_ > feed_detail::msToNs(cfg_.connect_timeout_ms)) {
                registry_.kick(hft::net::KickReason::ConnectTimeout);
            }
        }
    }

    // 快照队列：生产者 = feed 线程，消费者 = 主线程
    hft::SpscQueue<BookSnapshot>& queue() noexcept { return queue_; }

    FeedStatus status() const noexcept { return status_.load(std::memory_order_acquire); }

    FeedStats stats() const noexcept {
        constexpr auto r = std::memory_order_relaxed;
        FeedStats s;
        s.messages = messages_.load(r);
        s.bytes = bytes_.load(r);
        s.book_updates = book_updates_.load(r);
        s.published = published_.load(r);
        s.dropped_snapshots = dropped_.load(r);
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
    std::string lastError() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return last_error_;
    }

    const LatencyTracker& getLatencyTracker() const noexcept { return latency_; }
    const LatencyTracker& getExchangeLatency() const noexcept { return exchange_latency_; }
    const std::string& getName() const noexcept { return cfg_.name; }
    Exchange exchange() const noexcept { return cfg_.exchange; }
    std::uint32_t venueId() const noexcept { return cfg_.venue_id; }
    const FeedConfig& config() const noexcept { return cfg_; }

    // 状态属于 {Connecting, Syncing, Live, Reconnecting}
    bool isRunning() const noexcept {
        const FeedStatus s = status();
        return s == FeedStatus::Connecting || s == FeedStatus::Syncing || s == FeedStatus::Live ||
               s == FeedStatus::Reconnecting;
    }

    // 兼容旧接口：收到的 WebSocket 消息数（超过 INT_MAX 时饱和）
    int getMessageCount() const noexcept {
        const std::uint64_t n = messages_.load(std::memory_order_relaxed);
        return n > static_cast<std::uint64_t>(INT_MAX) ? INT_MAX : static_cast<int>(n);
    }

    // 日志回调：在 feed 线程上针对连接/断开/重同步/错误事件调用，参数为 "[name] message"。
    // 可在任意时刻设置（内部加锁）；回调抛出的异常被吞掉。
    void setLogCallback(std::function<void(const std::string&)> cb) {
        std::lock_guard<std::mutex> lock(error_mutex_);
        log_cb_ = std::move(cb);
    }

    // 发布通知：每个快照（含空簿与待发布快照的重试）真正进入队列之后，立即在 feed 线程上调用 cb，
    // 例如唤醒等待新数据的主循环；被丢弃（队列满）的快照不通知。必须在 run() 之前设置（之后不能再改，
    // 不加锁）。cb 在行情热路径上执行：必须很快返回、不能阻塞；抛出的异常被吞掉。
    void setPublishCallback(std::function<void()> cb) { publish_cb_ = std::move(cb); }

    // 退避时长：min(max, initial * 2^attempt) * (1 + 0.2 * jitter)，jitter ∈ [-1, 1]，结果钳制到 [1, max]
    static int backoffDelayMs(int attempt, int initial_ms, int max_ms, double jitter) noexcept {
        const double initial = static_cast<double>(std::max(1, initial_ms));
        const double cap = std::max(initial, static_cast<double>(max_ms));
        double base = initial;
        for (int i = 0; i < attempt && base < cap; ++i) base *= 2.0;
        base = std::min(base, cap);
        if (!(jitter >= -1.0)) jitter = -1.0;  // 同时处理 NaN
        if (jitter > 1.0) jitter = 1.0;
        const double d = std::clamp(base * (1.0 + 0.2 * jitter), 1.0, cap);
        return static_cast<int>(std::lround(d));
    }

private:
    // 会话对象直接使用 io_context 的执行器类型（不经 any_io_executor 类型擦除）：投递完成处理器时也用处理器
    // 关联的分配器（handler_memory_），稳态零堆分配
    using Executor = boost::asio::io_context::executor_type;
    using Socket = boost::asio::basic_stream_socket<boost::asio::ip::tcp, Executor>;
    using Timer = boost::asio::basic_waitable_timer<std::chrono::steady_clock,
                                                     boost::asio::wait_traits<std::chrono::steady_clock>, Executor>;
    using WsStream = boost::beast::websocket::stream<boost::beast::ssl_stream<Socket>>;

    // 看门狗关心的阶段（mutex_ 保护）
    enum class Phase { Idle, Connecting, Streaming };
    enum class EndKind { Stopped, Error, AdapterReconnect };
    struct SessionEnd {
        EndKind kind = EndKind::Error;
        std::string message;
    };

    // 写队列中的一项
    enum class WriteKind : std::uint8_t { Text, AppPing, WsPing };
    struct QueuedWrite {
        WriteKind kind = WriteKind::Text;
        std::string text;  // 只用于 Text
    };

    // 一个 WebSocket 会话的网络对象与异步状态（只在 feed 线程上使用）。成员按此顺序声明：析构时先注销套接字
    // （registration）、再关闭它（ws），最后销毁 io_context（其中尚未执行的处理器随之销毁、不会被调用）。
    struct Connection {
        explicit Connection(boost::asio::ssl::context& ctx) : ioc(1), ws(ioc, ctx), timer(ioc), retry_timer(ioc) {}

        boost::asio::io_context ioc;                 // 只属于本会话，只由 feed 线程 run_one() / poll()
        WsStream ws;
        Timer timer;                                 // 心跳
        Timer retry_timer;                           // 待发布快照的重试（只在有待发布快照时武装）
        hft::net::SocketRegistration registration;   // 声明在 ws 之后：析构时先注销、再关闭套接字

        std::optional<SessionEnd> end;               // 已决定结束会话：之后的处理器什么也不做
        bool timer_armed = false;
        bool retry_armed = false;
        // 写队列：[queue_next, queue_used) 是待发送的项；发完复位下标，各项保留字符串容量（稳态不分配）
        std::vector<QueuedWrite> queue;
        std::size_t queue_next = 0;
        std::size_t queue_used = 0;
        QueuedWrite inflight;                        // 正在发送的项：缓冲区在写完成之前保持不变
        bool writing = false;
        bool ping_outstanding = false;               // 有心跳已排队或正在发送（最多一个）
        // 心跳策略（会话开始时确定，见 setupKeepalive）
        std::string ping_text;                       // 应用层心跳消息；"" = 发 WebSocket ping 控制帧
        std::int64_t idle_ping_ns = 0;               // 这么久没有收到任何帧就发心跳（0 = 关闭）
        std::int64_t ping_interval_ns = 0;           // 应用层心跳：数据一直在来也至少每隔这么久发一次（0 = 无）
        std::int64_t last_ping_ns = 0;
    };

    static constexpr std::int64_t kMinTimerDelayNs = 1000000;  // 定时器至少 1 ms 之后才到期：不会空转
    static constexpr int kPendingRetryMs = 5;                  // 待发布快照的重试间隔

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

    // ---------------- 重连循环 ----------------

    void runLoop() {
        bool first = true;
        while (!stopRequested()) {
            if (!first) reconnects_.fetch_add(1, std::memory_order_relaxed);
            first = false;
            const SessionEnd end = runSessionGuarded();
            finishSession(end);
            if (stopRequested()) break;
            int delay = 0;
            if (end.kind == EndKind::AdapterReconnect && immediate_budget_ > 0) {
                --immediate_budget_;
            } else {
                delay = nextBackoffMs();
            }
            noteEvent(delay == 0 ? std::string("reconnecting now") : "reconnecting in " + std::to_string(delay) + " ms");
            if (delay > 0 && !sleepFor(delay)) break;
        }
    }

    int nextBackoffMs() {
        std::uniform_real_distribution<double> jitter(-1.0, 1.0);
        const int delay = backoffDelayMs(attempt_, cfg_.reconnect_initial_ms, cfg_.reconnect_max_ms, jitter(rng_));
        if (attempt_ < 30) ++attempt_;
        return delay;
    }

    // 可被 stop() 打断的等待；返回 false 表示已请求停止。有待发布的快照时分片等待，每片之后（锁外）重试入队
    // （待发布快照不会跨过会话边界：只有会话内 REST 请求之间的等待可能遇到，退避等待时没有）。
    bool sleepFor(int ms) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            if (stopRequested()) return false;
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return true;
            const auto until =
                publish_pending_ ? std::min(deadline, now + std::chrono::milliseconds(kPendingRetryMs)) : deadline;
            cv_.wait_until(lock, until, [this] { return stopRequested(); });
            if (publish_pending_) {
                lock.unlock();
                retryPendingPublish();
                lock.lock();
            }
        }
    }

    SessionEnd runSessionGuarded() {
        session_established_ = false;
        tcp_connected_ = false;
        stage_ = "setup";
        SessionEnd end;
        try {
            end = runSession();
        } catch (const std::exception& e) {
            end = fail(std::string("unexpected error during ") + stage_ + ": " + e.what());
        } catch (...) {
            end = fail(std::string("unexpected non-standard exception during ") + stage_);
        }
        // TCP 已连上、但 TLS / WebSocket 握手超时：下一次从下一个地址开始（TCP 连接超时由 connectSocket 自己轮转）
        if (tcp_connected_ && !session_established_ && registry_.reason() == hft::net::KickReason::ConnectTimeout) {
            ++ws_cursor_;
        }
        return end;
    }

    // 会话结束后的清理与上报（此时套接字已注销并关闭）
    void finishSession(const SessionEnd& end) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            phase_ = Phase::Idle;
            phase_start_ns_ = 0;
        }
        connected_since_ns_.store(0, std::memory_order_relaxed);
        // 消费者最后收到的簿非空 → 发布空簿（一定能入队，见 pushSnapshot）。本会话没能入队的待发布快照随会话一起
        // 过期、不再送出：那时消费者手里本来就是空簿，正是断线期间该有的状态。
        if (delivered_nonempty_) publishEmpty();
        publish_pending_ = false;
        copyAdapterStats();
        if (stopRequested() || end.kind == EndKind::Stopped) return;
        status_.store(FeedStatus::Reconnecting, std::memory_order_release);
        if (!session_established_) connect_failures_.fetch_add(1, std::memory_order_relaxed);
        reportError(end.message);
    }

    // ---------------- 单个会话 ----------------

    SessionEnd runSession() {
        using namespace feed_detail;
        const std::string where = cfg_.host + ":" + cfg_.port;
        noteEvent("connecting to wss://" + where + cfg_.path);  // 先记日志再开始计时：日志时刻 <= 连接计时起点
        beginConnecting();
        status_.store(FeedStatus::Connecting, std::memory_order_release);

        stage_ = "TLS setup";
        if (!ssl_ctx_) {
            try {
                ssl_ctx_ = hft::net::makeClientContext(hft::net::TlsOptions{cfg_.tls_verify, cfg_.ca_file});
            } catch (const std::exception& e) {
                return fail(e.what());
            }
        }

        stage_ = "DNS resolve";
        tcp::resolver::results_type endpoints;
        error_code ec = ws_resolver_.resolve(cfg_.host, cfg_.port, endpoints, [this] { return registry_.interrupted(); });
        if (ec) return fail("resolve " + where + ": " + hft::net::errorText(ec));

        Connection conn(*ssl_ctx_);
        WsStream& ws = conn.ws;
        stage_ = "TCP connect";
        hft::net::ConnectOptions copt;
        copt.timeout_ms = remainingConnectMs();
        copt.cursor = &ws_cursor_;
        ec = hft::net::connectSocket(beast::get_lowest_layer(ws), endpoints, registry_, conn.registration, nullptr,
                                     copt);
        if (ec == asio::error::timed_out && !registry_.interrupted()) return connectTimeoutEnd();
        if (ec) return fail("connect " + where + ": " + hft::net::errorText(ec));
        tcp_connected_ = true;
        beast::get_lowest_layer(ws).set_option(tcp::no_delay(true), ec);

        stage_ = "TLS handshake";
        try {
            hft::net::prepareTlsStream(ws.next_layer(), cfg_.host, cfg_.tls_verify);
        } catch (const std::exception& e) {
            return fail(e.what());
        }
        ws.next_layer().handshake(ssl::stream_base::client, ec);
        if (ec) {
            return fail(hft::net::describeHandshakeError(ws.next_layer().native_handle(), ec, cfg_.host,
                                                         cfg_.tls_verify));
        }

        stage_ = "WebSocket handshake";
        ws.set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) { req.set(http::field::user_agent, hft::net::kUserAgent); }));
        ws.read_message_max(adapter_detail::kMaxMessageBytes);
        websocket::response_type res;
        ws.handshake(res, hft::net::hostHeader(cfg_.host, cfg_.port), cfg_.path, ec);
        if (ec) {
            if (ec == websocket::error::upgrade_declined) {
                return fail("WebSocket upgrade to " + where + cfg_.path + " rejected: HTTP " +
                            std::to_string(res.result_int()) + " " + std::string(res.reason()) +
                            bodySnippet(res.body()));
            }
            return fail("WebSocket handshake with " + where + " failed: " + hft::net::errorText(ec));
        }
        ws.text(true);
        if (!beginStreaming()) return fail("interrupted");
        noteEvent("connected to wss://" + where + cfg_.path);

        stage_ = "subscribe";
        for (const std::string& frame : adapter_->onConnected()) {
            ws.write(asio::buffer(frame), ec);  // 同步写：异步读还没有开始
            if (ec) return fail("send subscribe frame: " + hft::net::errorText(ec));
        }

        stage_ = "read";
        read_buffer_.clear();
        return streamLoop(conn);
    }

    // ---------------- 流式阶段（异步读 + 心跳定时器 + 写队列，全部在 feed 线程上）----------------

    // 在本线程上用 run_one() 驱动异步读与心跳定时器，直到某个处理器决定结束会话。处理器只在这里的
    // run_one() 与 closeConnection() 的 poll() 里执行，都在 c 的生命周期之内，所以可以按引用捕获 c。
    SessionEnd streamLoop(Connection& c) {
        setupKeepalive(c);
        c.ws.control_callback(
            [this](boost::beast::websocket::frame_type kind, boost::beast::string_view) { onControlFrame(kind); });
        startRead(c);
        armTimer(c, hft::nowNs());
        while (!c.end) {
            if (c.ioc.run_one() == 0) {  // 不应发生：会话进行中总有一个挂起的读
                c.end = SessionEnd{EndKind::Error, "internal error: no pending network operation"};
            }
        }
        closeConnection(c);
        return std::move(*c.end);
    }

    // 会话结束：取消定时器，先注销、再关闭套接字（挂起的读写以 operation_aborted 完成），然后执行完已就绪的
    // 处理器（它们看到 end 已设置，什么也不做）。仍未完成的操作随 io_context 析构而销毁、不会被调用。
    void closeConnection(Connection& c) noexcept {
        try {
            c.timer.cancel();
            c.retry_timer.cancel();
        } catch (...) {
        }
        c.registration.release();
        feed_detail::error_code ignored;
        boost::beast::get_lowest_layer(c.ws).close(ignored);
        try {
            c.ioc.restart();
            c.ioc.poll();
        } catch (...) {
        }
    }

    // 第一个结束原因生效（例如写失败之后读也会以错误完成）
    static void endSession(Connection& c, SessionEnd end) {
        if (!c.end) c.end = std::move(end);
    }

    // 让异步操作的内存都来自 handler_memory_
    template <class Handler>
    auto bindHandler(Handler&& handler) {
        return boost::asio::bind_allocator(feed_detail::HandlerAllocator<void>(handler_memory_),
                                           std::forward<Handler>(handler));
    }

    void startRead(Connection& c) {
        c.ws.async_read(read_buffer_, bindHandler([this, &c](const feed_detail::error_code& ec, std::size_t) {
                            onRead(c, ec);
                        }));
    }

    void onRead(Connection& c, const feed_detail::error_code& ec) {
        const std::int64_t recv_ns = hft::nowNs();
        if (c.end) return;
        if (ec) return endSession(c, readFailed(ec, c.ws));
        const auto data = read_buffer_.cdata();
        const std::string_view payload(static_cast<const char*>(data.data()), data.size());
        countMessage(payload.size(), recv_ns);
        out_.reset();
        adapter_->onMessage(payload, out_);  // 适配器把消息复制进自己的缓冲区，之后不再引用 payload
        read_buffer_.consume(read_buffer_.size());
        copyAdapterStats();
        if (auto end = processResult(c, out_, recv_ns)) return endSession(c, std::move(*end));
        if (out_.need_rest_snapshot) {
            if (auto end = fetchRestSnapshot(c)) return endSession(c, std::move(*end));
        }
        if (publish_pending_) armRetry(c);
        startRead(c);
    }

    // WebSocket 控制帧（在异步读内部回调）：pong 与服务器的 ping 都证明连接活着，算作活动
    void onControlFrame(boost::beast::websocket::frame_type kind) noexcept {
        if (kind == boost::beast::websocket::frame_type::close) return;
        constexpr auto r = std::memory_order_relaxed;
        const std::int64_t now = hft::nowNs();
        activity_ns_.store(now, r);
        if (kind == boost::beast::websocket::frame_type::pong) pongs_.store(pongs_.load(r) + 1, r);
        updateHealthy(now);
    }

    // 心跳策略：有应用层心跳的交易所空闲 idle_ping 就发、并且至少每个心跳间隔发一次；没有的发 WebSocket ping，
    // 只在空闲时发。idle_ping = min(心跳间隔, stall_timeout_ms / 2)：回应总能在看门狗动手之前到达。
    void setupKeepalive(Connection& c) {
        c.ping_text = adapter_->pingMessage();
        const bool app_ping = !c.ping_text.empty();
        int interval_ms = 0;
        if (cfg_.ping_interval_ms > 0) {
            interval_ms = cfg_.ping_interval_ms;
        } else if (cfg_.ping_interval_ms < 0 && app_ping) {
            interval_ms = std::max(0, adapter_->pingIntervalMs());
        }
        int idle_ms = 0;
        if (cfg_.ping_interval_ms != 0) {
            idle_ms = interval_ms;
            if (cfg_.stall_timeout_ms > 0) {
                const int half = std::max(1, cfg_.stall_timeout_ms / 2);
                idle_ms = idle_ms > 0 ? std::min(idle_ms, half) : half;
            }
        }
        c.idle_ping_ns = feed_detail::msToNs(idle_ms);
        c.ping_interval_ns = app_ping ? feed_detail::msToNs(interval_ms) : 0;
        c.last_ping_ns = hft::nowNs();
    }

    // 定时器下一次需要到期的时刻（0 = 不需要）。消息到达时不动定时器：到期时再按最新的活动时刻重新计算。
    std::int64_t nextWakeNs(const Connection& c, std::int64_t now) const noexcept {
        std::int64_t due = std::numeric_limits<std::int64_t>::max();
        if (c.idle_ping_ns > 0) {
            due = std::min(due, std::max(activity_ns_.load(std::memory_order_relaxed), c.last_ping_ns) + c.idle_ping_ns);
        }
        if (c.ping_interval_ns > 0) due = std::min(due, c.last_ping_ns + c.ping_interval_ns);
        if (due == std::numeric_limits<std::int64_t>::max()) return 0;
        return std::max(due, now + kMinTimerDelayNs);
    }

    void armTimer(Connection& c, std::int64_t now) {
        if (c.timer_armed || c.end) return;
        const std::int64_t due = nextWakeNs(c, now);
        if (due == 0) return;
        c.timer.expires_at(Timer::time_point(
            std::chrono::duration_cast<Timer::duration>(std::chrono::nanoseconds(due))));
        c.timer_armed = true;
        c.timer.async_wait(bindHandler([this, &c](const feed_detail::error_code& ec) { onTimer(c, ec); }));
    }

    void onTimer(Connection& c, const feed_detail::error_code& ec) {
        c.timer_armed = false;
        if (c.end || ec) return;
        const std::int64_t now = hft::nowNs();
        maybeSendPing(c, now);
        armTimer(c, now);
    }

    // 有待发布的快照时每 kPendingRetryMs 重试一次入队，直到入队（或会话结束）。用单独的定时器：心跳定时器可能
    // 几秒之后才到期，关闭心跳时根本不武装。待发布快照只在处理消息时产生，所以读处理器末尾武装即可。
    void armRetry(Connection& c) {
        if (c.retry_armed || c.end || !publish_pending_) return;
        c.retry_timer.expires_after(std::chrono::milliseconds(kPendingRetryMs));
        c.retry_armed = true;
        c.retry_timer.async_wait(bindHandler([this, &c](const feed_detail::error_code& ec) { onRetry(c, ec); }));
    }

    void onRetry(Connection& c, const feed_detail::error_code& ec) {
        c.retry_armed = false;
        if (c.end || ec) return;
        retryPendingPublish();
        armRetry(c);
    }

    void maybeSendPing(Connection& c, std::int64_t now) {
        if (c.ping_outstanding) return;
        const std::int64_t last_rx = activity_ns_.load(std::memory_order_relaxed);
        const bool idle = c.idle_ping_ns > 0 && now - last_rx >= c.idle_ping_ns && now - c.last_ping_ns >= c.idle_ping_ns;
        const bool periodic = c.ping_interval_ns > 0 && now - c.last_ping_ns >= c.ping_interval_ns;
        if (!idle && !periodic) return;
        constexpr auto r = std::memory_order_relaxed;
        c.last_ping_ns = now;
        c.ping_outstanding = true;
        pings_.store(pings_.load(r) + 1, r);
        enqueueWrite(c, c.ping_text.empty() ? WriteKind::WsPing : WriteKind::AppPing);
    }

    // 写队列：任何时刻最多一个写操作（async_write / async_ping）在进行
    void enqueueWrite(Connection& c, WriteKind kind, std::string_view text = std::string_view()) {
        if (c.end) return;
        if (c.queue_used == c.queue.size()) c.queue.emplace_back();
        QueuedWrite& w = c.queue[c.queue_used++];
        w.kind = kind;
        w.text.assign(text.data(), text.size());
        if (!c.writing) startNextWrite(c);
    }

    void startNextWrite(Connection& c) {
        if (c.queue_next == c.queue_used) {  // 队列已空：复位下标
            c.queue_next = 0;
            c.queue_used = 0;
            return;
        }
        QueuedWrite& w = c.queue[c.queue_next++];
        c.inflight.kind = w.kind;
        c.inflight.text.swap(w.text);  // 交换而不是复制：两边的容量都保留下来
        c.writing = true;
        auto done = [this, &c](const feed_detail::error_code& ec, std::size_t) { onWritten(c, ec); };
        switch (c.inflight.kind) {
            case WriteKind::Text: c.ws.async_write(boost::asio::buffer(c.inflight.text), bindHandler(done)); break;
            case WriteKind::AppPing: c.ws.async_write(boost::asio::buffer(c.ping_text), bindHandler(done)); break;
            case WriteKind::WsPing:
                c.ws.async_ping(boost::beast::websocket::ping_data(),
                                bindHandler([this, &c](const feed_detail::error_code& ec) { onWritten(c, ec); }));
                break;
        }
    }

    void onWritten(Connection& c, const feed_detail::error_code& ec) {
        c.writing = false;
        if (c.inflight.kind != WriteKind::Text) c.ping_outstanding = false;
        if (c.end) return;
        if (ec) {
            const char* what = c.inflight.kind == WriteKind::Text ? "send frame: " : "send ping: ";
            return endSession(c, fail(what + hft::net::errorText(ec)));
        }
        startNextWrite(c);
    }

    // 新会话开始：重新武装登记表并进入 Connecting（与 checkHealth 同一把锁，看门狗不会踢错会话）
    void beginConnecting() {
        std::lock_guard<std::mutex> lock(mutex_);
        registry_.rearm();
        phase_ = Phase::Connecting;
        phase_start_ns_ = hft::nowNs();
    }

    // 连接阶段剩余的时限（毫秒，向上取整，至少 1）；connect_timeout_ms <= 0 时为 0（不限）。
    // phase_start_ns_ 只由 feed 线程写，这里在 feed 线程上读，不需要加锁。
    int remainingConnectMs() const noexcept {
        if (cfg_.connect_timeout_ms <= 0) return 0;
        const std::int64_t left =
            feed_detail::msToNs(cfg_.connect_timeout_ms) - (hft::nowNs() - phase_start_ns_);
        if (left <= 0) return 1;
        return static_cast<int>(std::min<std::int64_t>((left + 999999) / 1000000, cfg_.connect_timeout_ms));
    }

    // REST 请求的 TCP 连接总时限：connect_timeout_ms 与 stall_timeout_ms / 2 中较小的正值（都关闭时不限）
    int restConnectBudgetMs() const noexcept {
        int budget = cfg_.connect_timeout_ms > 0 ? cfg_.connect_timeout_ms : 0;
        if (cfg_.stall_timeout_ms > 0) {
            const int half = std::max(1, cfg_.stall_timeout_ms / 2);
            budget = budget > 0 ? std::min(budget, half) : half;
        }
        return budget;
    }

    // 握手完成：进入 Streaming。若此前已被踢断（连接超时恰好在握手完成时触发）则返回 false。
    bool beginStreaming() {
        const std::int64_t now = hft::nowNs();
        activity_ns_.store(now, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (registry_.interrupted()) return false;
            phase_ = Phase::Streaming;
            phase_start_ns_ = now;
        }
        session_established_ = true;
        connected_since_ns_.store(now, std::memory_order_relaxed);
        synced_ = false;
        healthy_ = false;
        live_since_ns_ = 0;
        rest_requests_ = 0;
        rest_failures_ = 0;
        last_rest_ns_ = 0;
        status_.store(FeedStatus::Syncing, std::memory_order_release);
        return true;
    }

    // 处理一次适配器结果；需要结束会话时返回结束原因。
    // 先更新状态（SYNCING/LIVE）再发布：消费者取到快照时看到的 status() 至少与该快照一样新。
    std::optional<SessionEnd> processResult(Connection& c, const AdapterResult& out, std::int64_t recv_ns) {
        updateSyncState(hft::nowNs());
        if (!out.error.empty()) noteError(out.error);
        for (const std::string& frame : out.send) enqueueWrite(c, WriteKind::Text, frame);
        // 先记交易所延迟再发布：消费者取到快照时，这条消息的延迟样本必然已经可见（队列的 release/acquire）
        if (out.exchange_ts_ms > 0) recordExchangeLatency(out.exchange_ts_ms);
        if (out.book_updated) publish(recv_ns, out.exchange_ts_ms);
        if (out.reconnect) {
            return SessionEnd{EndKind::AdapterReconnect,
                              out.error.empty() ? std::string("exchange adapter requested a reconnect") : out.error};
        }
        return std::nullopt;
    }

    // 拉取 REST 快照直到适配器满意。每个会话最多 rest_max_attempts 次请求，用完即结束会话（之后按退避重连）；
    // 簿连续同步 healthy_session_ms 之后额度恢复（长时间运行的会话偶尔出现缺口不会因此重连）。
    // 请求之间的间隔见 waitRestSpacing：持续出现缺口的上游不会让我们密集地请求 REST。
    std::optional<SessionEnd> fetchRestSnapshot(Connection& c) {
        if (cfg_.rest_host.empty() || cfg_.snapshot_path.empty()) {
            return SessionEnd{EndKind::Error,
                              "REST snapshot required, but rest_host/snapshot_path are not configured"};
        }
        const char* const saved_stage = stage_;
        stage_ = "REST snapshot";
        hft::net::HttpsGetRequest rq;
        rq.host = cfg_.rest_host;
        rq.port = cfg_.rest_port;
        rq.target = cfg_.snapshot_path;
        rq.verify = cfg_.tls_verify;
        rq.body_limit = adapter_detail::kMaxMessageBytes;
        rq.connect_timeout_ms = restConnectBudgetMs();
        rq.endpoint_cursor = &rest_cursor_;
        std::string problem = out_.error;  // 触发这一轮的原因（例如缺口）
        for (;;) {
            if (rest_requests_ >= cfg_.rest_max_attempts) {
                return SessionEnd{EndKind::Error, "REST snapshot request limit reached (rest_max_attempts " +
                                                      std::to_string(cfg_.rest_max_attempts) + "), reconnecting" +
                                                      (problem.empty() ? std::string()
                                                                       : " (last error: " + problem + ")")};
            }
            if (auto end = waitRestSpacing()) return end;
            ++rest_requests_;
            last_rest_ns_ = hft::nowNs();
            activity_ns_.store(last_rest_ns_, std::memory_order_relaxed);
            const hft::net::HttpResult http = hft::net::httpsGet(ioc_, *ssl_ctx_, rest_resolver_, registry_, rq);
            const std::int64_t recv_ns = hft::nowNs();
            activity_ns_.store(recv_ns, std::memory_order_relaxed);
            if (registry_.interrupted()) return interruptedEnd();
            if (http.ok && http.status == 200) {
                rest_out_.reset();
                adapter_->onRestSnapshot(http.body, rest_out_);
                copyAdapterStats();
                if (auto end = processResult(c, rest_out_, recv_ns)) return end;  // 已记录 rest_out_.error
                if (!rest_out_.need_rest_snapshot) {
                    rest_failures_ = 0;
                    stage_ = saved_stage;
                    return std::nullopt;
                }
                problem = rest_out_.error.empty() ? std::string("exchange adapter asked for another REST snapshot")
                                                  : rest_out_.error;
            } else {
                problem = http.ok ? "REST snapshot request returned HTTP " + std::to_string(http.status) + " " +
                                        http.reason + feed_detail::bodySnippet(http.body)
                                  : "REST snapshot request failed: " + http.error;
                noteError(problem);
            }
            ++rest_failures_;
        }
    }

    // 同一会话内两次 REST 请求的开始时刻至少相隔 rest_min_interval_ms；连续失败 k 次之后为它的 2^(k-1) 倍
    // （±20% 抖动，上限 reconnect_max_ms）。本会话的第一次请求不等待。等待期间不读 WebSocket：每次等待
    // 不超过停顿阈值的一半，避免看门狗把这段等待当成停顿。
    std::optional<SessionEnd> waitRestSpacing() {
        if (last_rest_ns_ == 0) return std::nullopt;
        std::uniform_real_distribution<double> jitter(-1.0, 1.0);
        const int spacing = backoffDelayMs(std::max(0, rest_failures_ - 1), cfg_.rest_min_interval_ms,
                                           cfg_.reconnect_max_ms, jitter(rng_));
        const std::int64_t wait_ns = last_rest_ns_ + feed_detail::msToNs(spacing) - hft::nowNs();
        if (wait_ns <= 0) return std::nullopt;
        int delay = static_cast<int>(std::min<std::int64_t>((wait_ns + 999999) / 1000000, spacing));
        if (cfg_.stall_timeout_ms > 0) delay = std::min(delay, std::max(1, cfg_.stall_timeout_ms / 2));
        if (!sleepFor(delay)) return SessionEnd{EndKind::Stopped, std::string()};
        if (registry_.interrupted()) return interruptedEnd();
        return std::nullopt;
    }

    SessionEnd readFailed(const feed_detail::error_code& ec, WsStream& ws) const {
        namespace websocket = boost::beast::websocket;
        if (registry_.interrupted()) return interruptedEnd();
        if (ec == websocket::error::closed) {
            const websocket::close_reason& r = ws.reason();
            std::string msg = "server closed the WebSocket (code " + std::to_string(r.code);
            if (!r.reason.empty()) msg += ": " + std::string(r.reason.data(), r.reason.size());
            return SessionEnd{EndKind::Error, msg + ")"};
        }
        if (ec == websocket::error::message_too_big) {
            return SessionEnd{EndKind::Error, "message larger than " +
                                                  std::to_string(adapter_detail::kMaxMessageBytes) +
                                                  " bytes, connection dropped"};
        }
        return SessionEnd{EndKind::Error, "connection lost: " + hft::net::errorText(ec)};
    }

    // 失败：若是被踢断导致的，改用踢断原因描述
    SessionEnd fail(std::string message) const {
        if (registry_.interrupted()) return interruptedEnd();
        return SessionEnd{EndKind::Error, std::move(message)};
    }

    SessionEnd interruptedEnd() const {
        switch (registry_.reason()) {
            case hft::net::KickReason::Stop: return SessionEnd{EndKind::Stopped, std::string()};
            case hft::net::KickReason::Stall:
                return SessionEnd{EndKind::Error, "stalled: no data for more than " +
                                                      std::to_string(cfg_.stall_timeout_ms) + " ms during " + stage_ +
                                                      ", connection kicked"};
            case hft::net::KickReason::ConnectTimeout: return connectTimeoutEnd();
            case hft::net::KickReason::None: break;
        }
        return SessionEnd{EndKind::Error, std::string("interrupted during ") + stage_};
    }

    // 连接阶段超时（看门狗踢断，或 connectSocket 自己用完了时限）
    SessionEnd connectTimeoutEnd() const {
        return SessionEnd{EndKind::Error, std::string("connect timeout: ") + stage_ + " did not complete within " +
                                              std::to_string(cfg_.connect_timeout_ms) + " ms"};
    }

    // ---------------- 每条消息的记账（只有 feed 线程写，relaxed 存储，无 RMW）----------------

    void countMessage(std::size_t bytes, std::int64_t recv_ns) noexcept {
        constexpr auto r = std::memory_order_relaxed;
        messages_.store(messages_.load(r) + 1, r);
        bytes_.store(bytes_.load(r) + bytes, r);
        last_message_ns_.store(recv_ns, r);
        activity_ns_.store(recv_ns, r);
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
        if (synced != synced_) {
            synced_ = synced;
            live_since_ns_ = synced ? now : 0;
            status_.store(synced ? FeedStatus::Live : FeedStatus::Syncing, std::memory_order_release);
            noteEvent(synced ? "book synced (LIVE)" : "book lost sync, resyncing");
        }
        updateHealthy(now);
    }

    // 会话连续 Live 超过 healthy_session_ms：退避计数清零。每次活动（消息、控制帧）都检查一次，
    // 所以只有心跳往来的安静会话同样会被认定为健康。
    void updateHealthy(std::int64_t now) noexcept {
        if (!synced_ || now - live_since_ns_ < feed_detail::msToNs(cfg_.healthy_session_ms)) return;
        if (!healthy_) {
            healthy_ = true;
            attempt_ = 0;
            immediate_budget_ = kMaxImmediateReconnects;
        }
        // 簿自上次（重新）同步以来已连续同步这么久：本会话的 REST 额度恢复
        rest_requests_ = 0;
        rest_failures_ = 0;
    }

    // ---------------- 发布 ----------------

    void publish(std::int64_t recv_ns, std::int64_t exchange_ts_ms) noexcept {
        snapshot_.assignFrom(adapter_->book(), cfg_.publish_levels);
        snapshot_.venue = cfg_.venue_id;
        snapshot_.seq = ++seq_;
        snapshot_.exchange_ts_ms = exchange_ts_ms;
        snapshot_.recv_ns = recv_ns;
        snapshot_.publish_ns = hft::nowNs();
        // 处理延迟在入队之前记录（同上：取到快照即可见）；published/dropped 只能在入队之后计数，
        // 所以消费者刚取到快照时，这两个计数可能还差这一次
        const std::int64_t latency = snapshot_.publish_ns - recv_ns;
        latency_.recordNs(latency > 0 ? static_cast<std::uint64_t>(latency) : 0);
        pushSnapshot();
    }

    // 会话结束时发布空簿（不记录处理延迟：不对应任何消息）。只在消费者最后收到的快照非空时调用，
    // 这时它一定能入队（见 pushSnapshot）。
    void publishEmpty() noexcept {
        snapshot_.bid_count = 0;
        snapshot_.ask_count = 0;
        snapshot_.update_id = 0;
        snapshot_.venue = cfg_.venue_id;
        snapshot_.seq = ++seq_;
        snapshot_.exchange_ts_ms = 0;
        snapshot_.recv_ns = hft::nowNs();
        snapshot_.publish_ns = snapshot_.recv_ns;
        pushSnapshot();
    }

    // 入队 snapshot_，绝不阻塞。非空快照入队之后至少要留一个空槽位（tryPushReserving(…, 1)），空簿可以用掉
    // 最后一个：只要消费者最后收到的快照非空，下一个空簿（断线、缺口）就一定能入队，消费者不会在断线期间继续
    // 使用过期价格——无论队列多满、feed 线程接下来阻塞在哪一步，都不需要重试。
    // 入队失败 → dropped++。消费者手里是空簿而 snapshot_ 非空时（例如断线之后新会话的第一本簿没能入队）保持待发布，
    // 之后重试最新状态（见 retryPendingPublish），否则推送稀少的交易所可能长时间只显示空簿；消费者手里是非空簿时
    // 照常丢弃（seq 跳号），下一次推送会替换它。
    bool pushSnapshot() noexcept {
        constexpr auto r = std::memory_order_relaxed;
        const bool empty = snapshot_.bid_count + snapshot_.ask_count == 0;
        if (!queue_.tryPushReserving(snapshot_, empty ? 0 : 1)) {
            HFT_ASSERT(!(empty && delivered_nonempty_) && "the slot reserved for an empty book was taken");
            dropped_.store(dropped_.load(r) + 1, r);
            publish_pending_ = !empty && !delivered_nonempty_;
            return false;
        }
        delivered();
        return true;
    }

    // 快照已经入队：记账只跟踪真正送到消费者手里的快照，然后通知（发布回调）
    void delivered() noexcept {
        constexpr auto r = std::memory_order_relaxed;
        published_.store(published_.load(r) + 1, r);
        delivered_nonempty_ = snapshot_.bid_count + snapshot_.ask_count > 0;
        publish_pending_ = false;
        if (publish_cb_) {
            try {
                publish_cb_();
            } catch (...) {
            }
        }
    }

    // 重试待发布的快照（只在消费者手里是空簿、snapshot_ 非空时才有，见 pushSnapshot）。snapshot_ 总是最新的
    // 状态：期间又有推送没能入队时，送出的是最新的那一本。重试不再计入 dropped，seq 不变；
    // publish_ns 改为真正入队的时刻（消费者的队列延迟从这里算起）。
    void retryPendingPublish() noexcept {
        if (!publish_pending_) return;
        snapshot_.publish_ns = hft::nowNs();
        if (queue_.tryPushReserving(snapshot_, 1)) delivered();
    }

    void recordExchangeLatency(std::int64_t exchange_ts_ms) noexcept {
        constexpr std::int64_t kMaxMs = std::numeric_limits<std::int64_t>::max() / 1000000;
        const std::int64_t lag_ms = hft::wallClockMs() - exchange_ts_ms;
        std::uint64_t ns = 0;
        if (lag_ms >= kMaxMs) {
            ns = static_cast<std::uint64_t>(kMaxMs) * 1000000;
        } else if (lag_ms > 0) {
            ns = static_cast<std::uint64_t>(lag_ms) * 1000000;
        }
        exchange_latency_.recordNs(ns);
    }

    // ---------------- 错误与日志 ----------------

    // 记录到 lastError() 并调用日志回调
    void noteError(const std::string& message) {
        std::function<void(const std::string&)> cb;
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            last_error_ = message;
            cb = log_cb_;
        }
        if (cb) callLog(cb, message);
    }

    // 会话结束的错误：额外写 stderr（整行一次写出，多个 feed 线程的输出不会交错在一行里）
    void reportError(const std::string& message) {
        noteError(message);
        if (cfg_.log_to_stderr) {
            const std::string line = "[" + cfg_.name + "] " + message + "\n";
            std::cerr.write(line.data(), static_cast<std::streamsize>(line.size()));
            std::cerr.flush();
        }
    }

    // 普通事件：只调用日志回调
    void noteEvent(const std::string& message) {
        std::function<void(const std::string&)> cb;
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            cb = log_cb_;
        }
        if (cb) callLog(cb, message);
    }

    void callLog(const std::function<void(const std::string&)>& cb, const std::string& message) const noexcept {
        try {
            cb("[" + cfg_.name + "] " + message);
        } catch (...) {
        }
    }

    // ---------------- 成员 ----------------

    const FeedConfig cfg_;
    std::unique_ptr<ExchangeAdapter> adapter_;   // feed 线程私有
    hft::SpscQueue<BookSnapshot> queue_;
    LatencyTracker latency_;                     // 单写者（feed 线程），任意线程读
    LatencyTracker exchange_latency_;

    // 跨线程状态
    std::atomic<FeedStatus> status_{FeedStatus::Idle};
    std::atomic<bool> stop_requested_{false};
    hft::net::SocketRegistry registry_;
    mutable std::mutex mutex_;                   // 保护 phase_ / phase_start_ns_，并用于退避等待
    std::condition_variable cv_;
    Phase phase_ = Phase::Idle;
    std::int64_t phase_start_ns_ = 0;
    std::atomic<std::int64_t> activity_ns_{0};   // 看门狗的"最近活动"时刻（消息、REST 请求）
    mutable std::mutex error_mutex_;             // 保护 last_error_ / log_cb_
    std::string last_error_;
    std::function<void(const std::string&)> log_cb_;
    std::function<void()> publish_cb_;           // run() 之前设置；之后只由 feed 线程读取

    std::atomic<std::uint64_t> messages_{0}, bytes_{0}, book_updates_{0}, published_{0}, dropped_{0},
        reconnects_{0}, connect_failures_{0}, stalls_{0}, gaps_{0}, checksum_failures_{0}, resyncs_{0},
        rest_snapshots_{0}, parse_errors_{0}, pings_{0}, pongs_{0};
    std::atomic<std::int64_t> last_message_ns_{0}, connected_since_ns_{0};

    // 以下只由 feed 线程访问
    feed_detail::HandlerMemory handler_memory_;       // 异步操作的内存（会话的 io_context 是局部对象，先于它销毁）
    boost::asio::io_context ioc_;                     // REST 请求用：只做同步操作，从不 run()
    std::unique_ptr<boost::asio::ssl::context> ssl_ctx_;   // 首次连接时创建，之后各会话（含 REST）复用
    hft::net::Resolver ws_resolver_;
    hft::net::Resolver rest_resolver_;
    boost::beast::flat_buffer read_buffer_;           // 跨消息、跨会话复用（只增不减）
    AdapterResult out_;                               // 复用：每条消息前 reset()
    AdapterResult rest_out_;
    BookSnapshot snapshot_;
    std::mt19937_64 rng_;
    std::uint64_t seq_ = 0;
    int attempt_ = 0;                                 // 退避指数
    int immediate_budget_ = kMaxImmediateReconnects;  // 剩余的立即重连次数
    bool session_established_ = false;
    bool tcp_connected_ = false;                      // 本会话的 TCP 连接已建立
    std::size_t ws_cursor_ = 0;                       // WebSocket 地址轮转游标（见 hft::net::ConnectOptions::cursor）
    std::size_t rest_cursor_ = 0;                     // REST 地址轮转游标
    bool synced_ = false;
    bool healthy_ = false;
    std::int64_t live_since_ns_ = 0;
    int rest_requests_ = 0;                           // 本会话（额度恢复以来）的 REST 快照请求数
    int rest_failures_ = 0;                           // 连续失败的 REST 请求数（同步成功后清零）
    std::int64_t last_rest_ns_ = 0;                   // 本会话上一次 REST 请求的开始时刻（0 = 还没有）
    bool delivered_nonempty_ = false;                 // 消费者最后收到（真正入队）的快照非空
    bool publish_pending_ = false;                    // 消费者手里是空簿、snapshot_ 非空却没能入队：需要重试
    const char* stage_ = "setup";                     // 当前步骤（用于错误信息）
};
