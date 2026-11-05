//
// market_data_feed.h
// 行情连接管理器：每个 feed 一个阻塞线程（同步 Boost.Beast）驱动交易所协议适配器，负责连接、退避重连、
// 停顿踢断、REST 快照，并经 SPSC 队列向主线程发布 BookSnapshot。
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
    int stall_timeout_ms = 15000;    // 已连接但这么久没有数据 → 踢断连接（checkHealth；<= 0 关闭）
    int reconnect_initial_ms = 500;  // 指数退避（±20% 抖动），健康会话后重置
    int reconnect_max_ms = 30000;
    int publish_levels = BookSnapshot::kMaxLevels;   // 每侧发布的档位数（钳制到 [0, kMaxLevels]）
    std::size_t queue_capacity = 1024;               // SPSC 队列容量（向上取整到 2 的幂）

    // ---- 以下为规格之外的扩展：默认值即生产行为，测试用来缩短时间 ----
    int connect_timeout_ms = 10000;    // 解析 + TCP + TLS + WebSocket 握手的总时限（checkHealth 踢断；<= 0 关闭）；
                                       // TCP 连接时每个地址另有 max(1 s, 剩余时限 / 地址数) 的时限
    int ping_interval_ms = -1;         // 应用层心跳间隔：< 0 用适配器默认值（OKX/Bybit 20000），0 关闭，> 0 覆盖
    int healthy_session_ms = 30000;    // 会话连续 LIVE 超过这么久即视为健康：退避计数清零
    int rest_max_attempts = 5;         // 每个会话 REST 快照最多尝试次数，超过则重连
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
}  // namespace feed_detail

// 行情连接管理器。
// 设计决定：保持同步 Boost.Beast，每个 feed 一个阻塞线程（简单、延迟低：读到数据立即在本线程处理）。
// 阻塞调用没有超时，及时退出与停顿恢复靠"踢断"：所有活动套接字登记在 hft::net::SocketRegistry 中，
// stop() 与 checkHealth() 对它们 ::shutdown(SHUT_RDWR)，阻塞的 handshake/read/write 立即失败。
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
// → 发送 adapter.onConnected() 的帧 → 读循环。
// 每条消息：recv_ns = 读取返回时刻 → adapter.onMessage → 依次处理结果：
//   状态 SYNCING/LIVE 按 adapter.synced() 更新；error → lastError() 与日志回调；send → 按顺序发送；
//   交易所延迟记录；book_updated → 发布快照（空簿同样发布；处理延迟在入队前记录）；
//   因此消费者取到一个快照时，status() 与两个延迟统计都已包含这条消息（队列的 release/acquire），
//   只有 published/dropped 计数在 tryPush 之后才更新；
//   reconnect → 结束会话并立即重连（不退避）；need_rest_snapshot → 阻塞式 HTTPS GET
//   rest_host:rest_port/snapshot_path（同样的校验规则，响应体上限 16 MiB，WebSocket 数据暂存在内核缓冲区），
//   200 的响应体交给 onRestSnapshot，非 200 / 传输失败视为失败；适配器继续要求时按退避重试，
//   每会话最多 rest_max_attempts 次，之后重连。
//   （先发布、后拉 REST：缺口导致的空簿立即让消费者知道，不必等待 REST 往返。）
// 应用层心跳：每条消息之后，若心跳间隔已到则发送 pingMessage()。
// 会话结束（任何原因）时若最后发布的簿非空，再发布一个空簿：断线期间消费者不会继续使用过期价格。
//
// 状态：建立连接期间 Connecting；握手完成后 Syncing，adapter.synced() 后 Live；会话结束 → Reconnecting
// （错误写 stderr 并存入 lastError()），退避 min(max, initial * 2^attempt)（±20% 抖动，结果钳制到
// [1, max]，可被 stop() 打断）；会话连续 Live 超过 healthy_session_ms（默认 30 s）后 attempt 清零。
// 适配器要求重连时立即重连；但连续 3 次立即重连之后（期间没有健康会话）改用正常退避，防止服务器
// 刚连上就要求重连时我们无间隔地反复握手。run() 返回后为 Stopped；stop 之后不能再次 run()。
//
// 计数：reconnects = 第一次之后的每次连接尝试；connect_failures = 在 WebSocket 握手完成之前失败的尝试
// （含证书校验失败、连接超时）；stalls = checkHealth 因无数据踢断的会话数。适配器统计每条消息后复制到原子量。
// seq 对每次发布尝试递增（含因队列满被丢弃的），消费者看到 seq 跳号即表示丢了快照。
// 处理延迟（getLatencyTracker）= publish_ns - recv_ns，每次发布记录一次；交易所延迟（getExchangeLatency）
// = wallClockMs() - exchange_ts_ms（纳秒存储，时钟偏差导致的负值记为 0），每条带时间戳的消息记录一次。
//
// 线程模型：run() 在专用线程上调用一次；stop()、status()、stats()、lastError()、延迟统计可在任意线程调用；
// checkHealth() 与 queue() 的消费端只在主线程调用。销毁前必须 stop() 并 join 运行 run() 的线程。
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
    using WsStream = boost::beast::websocket::stream<boost::beast::ssl_stream<boost::asio::ip::tcp::socket>>;

    // 看门狗关心的阶段（mutex_ 保护）
    enum class Phase { Idle, Connecting, Streaming };
    enum class EndKind { Stopped, Error, AdapterReconnect };
    struct SessionEnd {
        EndKind kind = EndKind::Error;
        std::string message;
    };

    static FeedConfig sanitize(FeedConfig c) {
        c.reconnect_initial_ms = std::max(1, c.reconnect_initial_ms);
        c.reconnect_max_ms = std::max(c.reconnect_initial_ms, c.reconnect_max_ms);
        c.rest_max_attempts = std::max(1, c.rest_max_attempts);
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

    // 可被 stop() 打断的等待；返回 false 表示已请求停止
    bool sleepFor(int ms) {
        std::unique_lock<std::mutex> lock(mutex_);
        return !cv_.wait_for(lock, std::chrono::milliseconds(ms), [this] { return stopRequested(); });
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
        if (last_published_nonempty_) publishEmpty();
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

        WsStream ws(ioc_, *ssl_ctx_);
        hft::net::SocketRegistration registration;  // 声明在 ws 之后：析构时先注销、再关闭套接字
        stage_ = "TCP connect";
        hft::net::ConnectOptions copt;
        copt.timeout_ms = remainingConnectMs();
        copt.cursor = &ws_cursor_;
        ec = hft::net::connectSocket(beast::get_lowest_layer(ws), endpoints, registry_, registration, nullptr, copt);
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
            ws.write(asio::buffer(frame), ec);
            if (ec) return fail("send subscribe frame: " + hft::net::errorText(ec));
        }

        const std::string ping = adapter_->pingMessage();
        const int ping_ms = cfg_.ping_interval_ms < 0 ? adapter_->pingIntervalMs() : cfg_.ping_interval_ms;
        const std::int64_t ping_ns = ping.empty() || ping_ms <= 0 ? 0 : msToNs(ping_ms);
        std::int64_t last_ping_ns = hft::nowNs();

        stage_ = "read";
        read_buffer_.clear();
        for (;;) {
            ws.read(read_buffer_, ec);
            const std::int64_t recv_ns = hft::nowNs();
            if (ec) return readFailed(ec, ws);
            const auto data = read_buffer_.cdata();
            const std::string_view payload(static_cast<const char*>(data.data()), data.size());
            countMessage(payload.size(), recv_ns);
            out_.reset();
            adapter_->onMessage(payload, out_);   // 适配器把消息复制进自己的缓冲区，之后不再引用 payload
            read_buffer_.consume(read_buffer_.size());
            copyAdapterStats();
            if (auto end = processResult(ws, out_, recv_ns)) return std::move(*end);
            if (out_.need_rest_snapshot) {
                if (auto end = fetchRestSnapshot(ws)) return std::move(*end);
            }
            if (ping_ns > 0 && recv_ns - last_ping_ns >= ping_ns) {
                last_ping_ns = recv_ns;
                ws.write(asio::buffer(ping), ec);
                if (ec) return fail("send ping: " + hft::net::errorText(ec));
            }
        }
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
        status_.store(FeedStatus::Syncing, std::memory_order_release);
        return true;
    }

    // 处理一次适配器结果；需要结束会话时返回结束原因。
    // 先更新状态（SYNCING/LIVE）再发布：消费者取到快照时看到的 status() 至少与该快照一样新。
    std::optional<SessionEnd> processResult(WsStream& ws, const AdapterResult& out, std::int64_t recv_ns) {
        updateSyncState(hft::nowNs());
        if (!out.error.empty()) noteError(out.error);
        for (const std::string& frame : out.send) {
            feed_detail::error_code ec;
            ws.write(boost::asio::buffer(frame), ec);
            if (ec) return fail("send frame: " + hft::net::errorText(ec));
        }
        // 先记交易所延迟再发布：消费者取到快照时，这条消息的延迟样本必然已经可见（队列的 release/acquire）
        if (out.exchange_ts_ms > 0) recordExchangeLatency(out.exchange_ts_ms);
        if (out.book_updated) publish(recv_ns, out.exchange_ts_ms);
        if (out.reconnect) {
            return SessionEnd{EndKind::AdapterReconnect,
                              out.error.empty() ? std::string("exchange adapter requested a reconnect") : out.error};
        }
        return std::nullopt;
    }

    // 拉取 REST 快照直到适配器满意；失败按退避重试，超过 rest_max_attempts 次结束会话
    std::optional<SessionEnd> fetchRestSnapshot(WsStream& ws) {
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
        for (int attempt = 1;; ++attempt) {
            activity_ns_.store(hft::nowNs(), std::memory_order_relaxed);
            const hft::net::HttpResult http = hft::net::httpsGet(ioc_, *ssl_ctx_, rest_resolver_, registry_, rq);
            const std::int64_t recv_ns = hft::nowNs();
            activity_ns_.store(recv_ns, std::memory_order_relaxed);
            if (registry_.interrupted()) return interruptedEnd();
            std::string problem;
            if (http.ok && http.status == 200) {
                rest_out_.reset();
                adapter_->onRestSnapshot(http.body, rest_out_);
                copyAdapterStats();
                if (auto end = processResult(ws, rest_out_, recv_ns)) return end;  // 已记录 rest_out_.error
                if (!rest_out_.need_rest_snapshot) {
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
            if (attempt >= cfg_.rest_max_attempts) {
                return SessionEnd{EndKind::Error, "REST snapshot failed " + std::to_string(attempt) +
                                                      " times, reconnecting (last error: " + problem + ")"};
            }
            std::uniform_real_distribution<double> jitter(-1.0, 1.0);
            int delay = backoffDelayMs(attempt - 1, cfg_.reconnect_initial_ms, cfg_.reconnect_max_ms, jitter(rng_));
            // 退避期间不读 WebSocket：等待不超过停顿阈值的一半，避免看门狗把这段等待当成停顿
            if (cfg_.stall_timeout_ms > 0) delay = std::min(delay, std::max(1, cfg_.stall_timeout_ms / 2));
            if (!sleepFor(delay)) return SessionEnd{EndKind::Stopped, std::string()};
            if (registry_.interrupted()) return interruptedEnd();
        }
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
        if (synced_ && !healthy_ && now - live_since_ns_ >= feed_detail::msToNs(cfg_.healthy_session_ms)) {
            healthy_ = true;
            attempt_ = 0;
            immediate_budget_ = kMaxImmediateReconnects;
        }
    }

    // ---------------- 发布 ----------------

    void publish(std::int64_t recv_ns, std::int64_t exchange_ts_ms) noexcept {
        snapshot_.assignFrom(adapter_->book(), cfg_.publish_levels);
        snapshot_.venue = cfg_.venue_id;
        snapshot_.seq = ++seq_;
        snapshot_.exchange_ts_ms = exchange_ts_ms;
        snapshot_.recv_ns = recv_ns;
        snapshot_.publish_ns = hft::nowNs();
        // 处理延迟在入队之前记录（同上：取到快照即可见）；published/dropped 只能在 tryPush 之后计数，
        // 所以消费者刚取到快照时，这两个计数可能还差这一次
        const std::int64_t latency = snapshot_.publish_ns - recv_ns;
        latency_.recordNs(latency > 0 ? static_cast<std::uint64_t>(latency) : 0);
        pushSnapshot();
    }

    // 会话结束时发布空簿（不记录处理延迟：不对应任何消息）
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

    void pushSnapshot() noexcept {
        constexpr auto r = std::memory_order_relaxed;
        if (queue_.tryPush(snapshot_)) {
            published_.store(published_.load(r) + 1, r);
        } else {
            dropped_.store(dropped_.load(r) + 1, r);  // 队列满：丢弃，绝不阻塞
        }
        last_published_nonempty_ = snapshot_.bid_count + snapshot_.ask_count > 0;
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

    std::atomic<std::uint64_t> messages_{0}, bytes_{0}, book_updates_{0}, published_{0}, dropped_{0},
        reconnects_{0}, connect_failures_{0}, stalls_{0}, gaps_{0}, checksum_failures_{0}, resyncs_{0},
        rest_snapshots_{0}, parse_errors_{0};
    std::atomic<std::int64_t> last_message_ns_{0}, connected_since_ns_{0};

    // 以下只由 feed 线程访问
    boost::asio::io_context ioc_;                     // 只做同步操作，从不 run()
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
    bool last_published_nonempty_ = false;
    const char* stage_ = "setup";                     // 当前步骤（用于错误信息）
};
