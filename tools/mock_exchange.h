//
// mock_exchange.h
// 离线模拟交易所（网络与线程层）：三个 TLS WebSocket + HTTPS 服务器共用一张自签名证书，仿真线程按实时节奏推进
// 三家撮合引擎并按各自的推送周期发布行情；每个连接按 Binance / OKX / Bybit 的真实协议处理订阅、心跳与 REST 快照。
// Offline mock exchange (network and thread layer): three TLS WebSocket + HTTPS servers share one
// self-signed certificate, a simulation thread advances three matching engines at real-time pace and
// publishes market data at each one's push period; every connection handles subscriptions, heartbeats
// and REST snapshots with the real Binance / OKX / Bybit protocols.
//

#pragma once

#include "mock_exchange_server.h"
#include "hft/timing.h"
#include "mock_market.h"
#include "mock_publishers.h"
#include "mock_wire.h"
#include "simdjson.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// 结构
// Structure
//
//   仿真线程 ──(每 5 ms 一步)──▶ sim::Market（三家 MatchingEngine + 模拟交易者）
//        │  到推送周期时取深度窗口，交给各交易所的 Hub：
//   Simulation thread ──(one step every 5 ms)──▶ sim::Market (three MatchingEngines + simulated traders)
//        │  when a push period comes due, takes the depth window and hands it to each exchange's Hub:
//        ▼
//   BinanceHub / OkxHub / BybitHub：发布者状态机（mock_publishers.h）+ 每个推送流一个 StreamLog（最近的报文，
//        │                          共享只读的 shared_ptr<const std::string>）+ 会话唤醒表；一把互斥锁保护全部状态
//   BinanceHub / OkxHub / BybitHub: publisher state machines (mock_publishers.h) + one StreamLog per
//        │                          push stream (recent messages, as shared read-only
//        │                          shared_ptr<const std::string>) + session waker table; one mutex
//        │                          guards all the state
//        ▼
//   MockExchangeServer（每家一个端口）：每个连接一个线程运行 Hub::serve(WsSession&)：
//        处理客户端帧（订阅/退订/心跳）→ 从订阅流的 StreamLog 取游标之后的新报文并发送 → 等待新帧或新推送
//   MockExchangeServer (one port per exchange): one thread per connection runs Hub::serve(WsSession&):
//        handle client frames (subscribe/unsubscribe/heartbeat)
//        → take the new messages after the cursor from the subscribed streams' StreamLog and send them
//        → wait for a new frame or a new push
//
// - 订阅的原子性：会话在 Hub 锁内同时取得"由已发布窗口生成的快照"与该流日志的当前序号作为游标，
//   之后只转发游标之后的报文——快照与后续增量严格衔接（OKX prevSeqId、Bybit u、Binance REST lastUpdateId）。
// - 所有连接看到同一条报文序列（与真实交易所一样，序列号是每个交易对全局的），慢的连接只影响它自己的线程；
//   落后超过日志保留范围（kRetentionMs）的连接被断开（真实交易所也会断开慢消费者），重连后重新同步。
// - 推送后发布者通过 SessionWaker 立即唤醒各会话线程（不必等待 WsSession 的 10 ms 轮询片），
//   所以报文的 E / ts 时间戳与客户端收到的时刻只差网络与调度延迟。
// - 仿真按仿真时间调度推送（每 N 步一次），整条报文序列只由种子决定；墙钟只决定"何时"发生。
//   跟不上实时（积压超过 kMaxCatchUpSteps 步）时丢弃积压、从当前时刻继续，而不是突发补发。
// - 线程与锁：仿真线程持有 sim_mutex_ 推进与发布（期间短暂获取各 Hub 锁）；会话与 HTTP 线程只获取 Hub 锁；
//   测试线程经 withMarket() 获取 sim_mutex_。没有反向的加锁顺序。
// - Atomic subscription: inside the Hub lock a session takes both "a snapshot generated from the
//   published window" and the stream log's current sequence number as its cursor, and afterwards
//   forwards only the messages after the cursor, so the snapshot and the later incremental updates
//   chain exactly (OKX prevSeqId, Bybit u, Binance REST lastUpdateId).
// - All connections see the same message sequence (as on the real exchanges, sequence numbers are
//   global per trading pair), and a slow connection affects only its own thread; a connection that
//   falls behind by more than the log retention (kRetentionMs) is disconnected (real exchanges also
//   disconnect slow consumers) and resyncs after reconnecting.
// - After a push the publisher immediately wakes every session thread through SessionWaker (no need
//   to wait for WsSession's 10 ms poll slice), so a message's E / ts timestamp differs from the moment
//   the client receives it only by network and scheduling latency.
// - The simulation schedules pushes in simulated time (once every N steps), so the whole message
//   sequence depends only on the seed; the wall clock only decides "when" things happen. When it
//   cannot keep up with real time (a backlog of more than kMaxCatchUpSteps steps), it drops the
//   backlog and continues from the current time instead of catching up in a burst.
// - Threads and locks: the simulation thread holds sim_mutex_ to advance and publish (briefly taking
//   each Hub lock meanwhile); session and HTTP threads take only Hub locks; test threads take
//   sim_mutex_ through withMarket(). There is no reverse lock order.
// ============================================================================
namespace mock_exchange {

using MessagePtr = std::shared_ptr<const std::string>;
using LogFn = std::function<void(const std::string&)>;

inline MessagePtr makeMessage(std::string s) { return std::make_shared<const std::string>(std::move(s)); }

// 推送周期（毫秒，均为仿真步长 5 ms 的整数倍）
// Push periods (milliseconds, all integer multiples of the 5 ms simulation step)
inline constexpr int kBinanceFastMs = 100;    // btcusdt@depth@100ms、depth<N>@100ms
                                              // btcusdt@depth@100ms, depth<N>@100ms
inline constexpr int kBinanceSlowMs = 1000;   // btcusdt@depth、depth<N>
                                              // btcusdt@depth, depth<N>
inline constexpr int kOkxBooksMs = 100;       // books（400 档增量）、books5
                                              // books (400-level incremental), books5
inline constexpr int kOkxBboMs = 10;          // bbo-tbt
inline constexpr std::size_t kBybitDepthCount = 3;
inline constexpr std::array<std::size_t, kBybitDepthCount> kBybitDepths{1, 50, 200};
inline constexpr std::array<int, kBybitDepthCount> kBybitPeriodsMs{10, 20, 200};   // 现货：1 档 10 ms、50 档 20 ms、200 档 200 ms
                                                                                   // spot: 1 level 10 ms,
                                                                                   // 50 levels 20 ms,
                                                                                   // 200 levels 200 ms
// 每个推送流保留的报文时长（至少 64 条）：落后超过它的慢连接被断开
// How long each push stream keeps its messages (at least 64 of them): a slow connection that falls
// further behind than this is disconnected
inline constexpr int kRetentionMs = 30000;

// ---------------------------------------------------------------------------
// 单个推送流的报文日志：按序号（1, 2, ...）保存最近 capacity 条
// Message log of one push stream: keeps the latest capacity messages by sequence number (1, 2, ...)
// ---------------------------------------------------------------------------
class StreamLog {
public:
    explicit StreamLog(std::size_t capacity) : capacity_(capacity) {}

    void append(std::string message) {
        entries_.push_back(makeMessage(std::move(message)));
        ++last_seq_;
        if (entries_.size() > capacity_) entries_.pop_front();
    }

    std::uint64_t lastSeq() const noexcept { return last_seq_; }
    std::size_t size() const noexcept { return entries_.size(); }

    // 把序号 > cursor 的报文追加到 out，cursor 前进到最新序号。cursor 之后的报文已有被淘汰的（客户端落后太多）
    // 时返回 false，out 与 cursor 都不变。
    // Appends the messages with sequence number > cursor to out and advances cursor to the latest
    // sequence number. When some of the messages after cursor have already been evicted (the client fell
    // too far behind), returns false and leaves both out and cursor unchanged.
    bool collect(std::uint64_t& cursor, std::vector<MessagePtr>& out) const {
        if (cursor >= last_seq_) return true;
        const std::uint64_t first = last_seq_ - entries_.size() + 1;  // entries_.front() 的序号
                                                                      // sequence number of entries_.front()
        if (cursor + 1 < first) return false;
        for (std::uint64_t s = cursor + 1; s <= last_seq_; ++s) {
            out.push_back(entries_[static_cast<std::size_t>(s - first)]);
        }
        cursor = last_seq_;
        return true;
    }

private:
    std::deque<MessagePtr> entries_;
    std::size_t capacity_;
    std::uint64_t last_seq_ = 0;
};

namespace detail {

inline std::mt19937_64& threadRng() {
    thread_local std::mt19937_64 rng([] {
        std::uint64_t seed = static_cast<std::uint64_t>(hft::nowNs()) ^ 0x6D6F636B65786368ULL;
        try {
            std::random_device rd;
            seed ^= (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
        } catch (...) {
        }
        return seed;
    }());
    return rng;
}

inline std::string randomHex(std::size_t digits) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string s(digits, '0');
    for (char& c : s) c = kHex[threadRng()() & 0xF];
    return s;
}

// UUID 形式的连接标识（Bybit conn_id）
// Connection id in UUID form (Bybit conn_id)
inline std::string makeUuid() {
    return randomHex(8) + "-" + randomHex(4) + "-" + randomHex(4) + "-" + randomHex(4) + "-" + randomHex(12);
}

// 步数：period_ms / 步长，至少 1
// Step count: period_ms / step length, at least 1
inline std::uint64_t periodSteps(int period_ms, double step_s) {
    const double steps = static_cast<double>(period_ms) / (step_s * 1000.0);
    return static_cast<std::uint64_t>(std::max<long long>(1, std::llround(steps)));
}

// 按 '/' 切分（忽略空段）
// Splits on '/' (ignoring empty segments)
inline std::vector<std::string> splitStreams(std::string_view s) {
    std::vector<std::string> out;
    while (!s.empty()) {
        const std::size_t slash = s.find('/');
        const std::string_view part = s.substr(0, slash);
        if (!part.empty()) out.emplace_back(part);
        if (slash == std::string_view::npos) break;
        s.remove_prefix(slash + 1);
    }
    return out;
}

// 写文件：先写同目录的临时文件再 rename，读者不会看到写了一半的文件。失败抛 std::runtime_error。
// Writes a file: writes a temporary file in the same directory first and then renames it, so readers
// never see a half-written file. Throws std::runtime_error on failure.
inline void writeFileAtomic(const std::string& path, const std::string& contents) {
    static std::atomic<unsigned> counter{0};
    const std::string tmp = path + ".tmp." + std::to_string(::getpid()) + "." + std::to_string(counter.fetch_add(1));
    std::FILE* f = std::fopen(tmp.c_str(), "wb");
    if (f == nullptr) throw std::runtime_error("cannot create " + tmp + ": " + std::strerror(errno));
    const bool written = std::fwrite(contents.data(), 1, contents.size(), f) == contents.size();
    const bool closed = std::fclose(f) == 0;
    if (!written || !closed || std::rename(tmp.c_str(), path.c_str()) != 0) {
        const int err = errno;
        std::remove(tmp.c_str());
        throw std::runtime_error("cannot write " + path + ": " + std::strerror(err));
    }
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Hub 基类：互斥锁、推送流日志、会话唤醒表、计数与通用的会话主循环
// Hub base class: mutex, push stream logs, session waker table, counters and the generic session main
// loop
// ---------------------------------------------------------------------------
class SessionHub {
public:
    struct Counters {
        std::uint64_t sessions_total = 0;    // 累计 WebSocket 会话
                                             // cumulative WebSocket sessions
        std::uint64_t sessions_open = 0;     // 当前打开的会话
                                             // currently open sessions
        std::uint64_t messages_sent = 0;     // 发出的文本帧（推送 + 应答）
                                             // text frames sent (pushes + replies)
        std::uint64_t bytes_sent = 0;
        std::uint64_t requests = 0;          // 收到的客户端文本帧
                                             // client text frames received
        std::uint64_t bad_requests = 0;      // 其中被拒绝 / 报错的
                                             // of those, rejected / answered with an error
        std::uint64_t rest_requests = 0;     // HTTP 请求（Binance REST）
                                             // HTTP requests (Binance REST)
        std::uint64_t dropped = 0;           // 故障注入丢弃的推送报文
                                             // push messages dropped by fault injection
    };

    // 每个推送流一个日志：periods_ms[i] 是第 i 个流的推送周期，日志保留 kRetentionMs 内的报文（至少 64 条）
    // One log per push stream: periods_ms[i] is the push period of stream i, and each log keeps the
    // messages of the last kRetentionMs (at least 64 messages)
    SessionHub(std::string name, std::initializer_list<int> periods_ms) : name_(std::move(name)) {
        for (const int period : periods_ms) {
            logs_.emplace_back(static_cast<std::size_t>(std::max(64, kRetentionMs / period)));
        }
    }
    SessionHub(const SessionHub&) = delete;
    SessionHub& operator=(const SessionHub&) = delete;

    const std::string& name() const noexcept { return name_; }

    Counters counters() const noexcept {
        constexpr auto r = std::memory_order_relaxed;
        Counters c;
        c.sessions_total = sessions_total_.load(r);
        c.sessions_open = sessions_open_.load(r);
        c.messages_sent = messages_sent_.load(r);
        c.bytes_sent = bytes_sent_.load(r);
        c.requests = requests_.load(r);
        c.bad_requests = bad_requests_.load(r);
        c.rest_requests = rest_requests_.load(r);
        c.dropped = dropped_.load(r);
        return c;
    }

    // 日志回调：须在服务器开始接受连接之前设置（之后只读，会话线程无需加锁）
    // Log callback: must be set before the server starts accepting connections (read-only afterwards, so
    // session threads need no lock)
    void setLog(LogFn fn) { log_ = std::move(fn); }

    // 故障注入（测试 / 演示）：每条推送报文以概率 p 不发给某个会话（各会话独立抽签；订阅应答与快照照常发送），
    // 客户端于是看到序列号缺口并按各自的协议重同步。任意线程可随时修改。
    // Fault injection (tests / demo): each push message is withheld from a given session with
    // probability p (each session draws independently; subscription replies and snapshots are sent as
    // usual), so the client sees a sequence number gap and resyncs according to its own protocol. Any
    // thread may change it at any time.
    void setDropRate(double p) noexcept { drop_rate_.store(p, std::memory_order_relaxed); }

protected:
    // 会话登记（RAII）：唤醒句柄 + 打开会话计数
    // Session registration (RAII): waker handle + open session count
    class Scope {
    public:
        Scope(SessionHub& hub, WsSession& ws) : hub_(hub) {
            std::lock_guard<std::mutex> lock(hub_.mutex_);
            id_ = hub_.next_waker_id_++;
            hub_.wakers_.emplace_back(id_, ws.waker());
            hub_.sessions_total_.fetch_add(1, std::memory_order_relaxed);
            hub_.sessions_open_.fetch_add(1, std::memory_order_relaxed);
        }
        ~Scope() {
            std::lock_guard<std::mutex> lock(hub_.mutex_);
            auto& w = hub_.wakers_;
            w.erase(std::remove_if(w.begin(), w.end(), [this](const auto& e) { return e.first == id_; }), w.end());
            hub_.sessions_open_.fetch_sub(1, std::memory_order_relaxed);
        }
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

    private:
        SessionHub& hub_;
        std::uint64_t id_ = 0;
    };

    // 通用会话主循环：
    //   on_frame(frame, out) -> bool：处理一个客户端文本帧，应答放进 out（返回 false = 错误请求，只用于计数）；
    //   collect(out) -> bool：取各订阅流的新报文放进 out（返回 false = 落后超过保留范围）。
    // 客户端断开时 ws 的调用抛出 ConnectionClosed，服务器停止时同样（由 MockExchangeServer 处理）。
    // Generic session main loop:
    //   on_frame(frame, out) -> bool: handles one client text frame and puts the replies into out
    //     (returns false = bad request, used only for counting);
    //   collect(out) -> bool: puts the new messages of the subscribed streams into out (returns false =
    //     fell behind by more than the retention range).
    // When the client disconnects, the ws calls throw ConnectionClosed, and likewise when the server
    // stops (handled by MockExchangeServer).
    template <class OnFrame, class Collect>
    void runSession(WsSession& ws, OnFrame&& on_frame, Collect&& collect) {
        Scope scope(*this, ws);
        std::vector<MessagePtr> out;
        std::size_t cursor = 0;
        std::mt19937_64 rng(detail::threadRng()());  // 故障注入的抽签
                                                     // draws for fault injection
        for (;;) {
            // 先读 tick 再取报文：两者之间到达的推送会让下面的等待立即返回，不会漏掉
            // Read the tick before fetching messages: a push that arrives in between makes the wait below return
            // at once, so nothing is missed
            const std::uint64_t seen = ticks_.load(std::memory_order_acquire);
            while (cursor < ws.received().size()) {
                // 拷贝：发送期间 io_context 运行，received() 可能追加新帧而重新分配
                // Copy: the io_context runs while sending, and received() may append new frames and reallocate
                const std::string frame = ws.received()[cursor++];
                requests_.fetch_add(1, std::memory_order_relaxed);
                if (!on_frame(frame, out)) bad_requests_.fetch_add(1, std::memory_order_relaxed);
                sendAll(ws, out);
            }
            if (!collect(out)) {
                log("session " + std::to_string(ws.sessionIndex()) +
                    ": client fell behind the message log, disconnecting (slow consumer)");
                ws.close(1008, "slow consumer");
                return;
            }
            dropSome(out, rng);
            sendAll(ws, out);
            if (!ws.clientConnected()) return;
            ws.waitUntil(
                [&] {
                    return ticks_.load(std::memory_order_acquire) != seen || ws.received().size() != cursor ||
                           !ws.clientConnected();
                },
                1000);
            if (!ws.clientConnected()) return;
        }
    }

    // 按流编号订阅（OKX 频道、Bybit 深度）的会话取新报文：按编号顺序收集；任一流落后超过保留范围返回 false
    // Fetches new messages for sessions that subscribe by stream number (OKX channels, Bybit depths):
    // collects in stream number order; returns false if any stream fell behind by more than the
    // retention range
    template <std::size_t N>
    bool collectSubscribed(const bool (&subscribed)[N], std::uint64_t (&cursor)[N], std::vector<MessagePtr>& out) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (std::size_t i = 0; i < N; ++i) {
            if (subscribed[i] && !logs_[i].collect(cursor[i], out)) return false;
        }
        return true;
    }

    // 发布了新报文之后调用（mutex_ 已持有）：推进 tick 并唤醒全部会话
    // Called after publishing new messages (mutex_ already held): advances the tick and wakes all sessions
    void notifyLocked() noexcept {
        ticks_.fetch_add(1, std::memory_order_acq_rel);
        for (const auto& w : wakers_) w.second.wake();
    }

    void log(const std::string& line) const noexcept {
        if (!log_) return;
        try {
            log_("[" + name_ + "] " + line);
        } catch (...) {
        }
    }

    mutable std::mutex mutex_;     // 保护 logs_、派生类的发布者状态，以及 wakers_
                                   // guards logs_, the derived class's publisher state, and wakers_
    std::vector<StreamLog> logs_;  // 每个推送流一个（编号由派生类定义）
                                   // one per push stream (numbering defined by the derived class)
    std::atomic<std::uint64_t> rest_requests_{0};
    std::atomic<std::uint64_t> bad_requests_{0};

private:
    // 按 drop_rate_ 随机去掉 out 中的推送报文
    // Randomly removes push messages from out according to drop_rate_
    void dropSome(std::vector<MessagePtr>& out, std::mt19937_64& rng) {
        const double p = drop_rate_.load(std::memory_order_relaxed);
        if (!(p > 0.0) || out.empty()) return;
        std::uniform_real_distribution<double> draw(0.0, 1.0);
        const std::size_t before = out.size();
        out.erase(std::remove_if(out.begin(), out.end(), [&](const MessagePtr&) { return draw(rng) < p; }), out.end());
        dropped_.fetch_add(before - out.size(), std::memory_order_relaxed);
    }

    void sendAll(WsSession& ws, std::vector<MessagePtr>& out) {
        for (const MessagePtr& m : out) {
            ws.send(*m);
            messages_sent_.fetch_add(1, std::memory_order_relaxed);
            bytes_sent_.fetch_add(m->size(), std::memory_order_relaxed);
        }
        out.clear();
    }

    std::string name_;
    LogFn log_;
    std::atomic<std::uint64_t> ticks_{0};
    std::vector<std::pair<std::uint64_t, SessionWaker>> wakers_;
    std::uint64_t next_waker_id_ = 1;
    std::atomic<std::uint64_t> sessions_total_{0}, sessions_open_{0}, messages_sent_{0}, bytes_sent_{0}, requests_{0},
        dropped_{0};
    std::atomic<double> drop_rate_{0.0};
};

// ---------------------------------------------------------------------------
// Binance：/ws/<stream>、/ws、/stream?streams=a/b、/stream；SUBSCRIBE / UNSUBSCRIBE / LIST_SUBSCRIPTIONS /
// SET_PROPERTY / GET_PROPERTY；REST GET /api/v3/depth、/api/v3/ping、/api/v3/time
// Binance: /ws/<stream>, /ws, /stream?streams=a/b, /stream; SUBSCRIBE / UNSUBSCRIBE /
// LIST_SUBSCRIPTIONS / SET_PROPERTY / GET_PROPERTY; REST GET /api/v3/depth, /api/v3/ping,
// /api/v3/time
// ---------------------------------------------------------------------------
class BinanceHub final : public SessionHub {
public:
    BinanceHub()
        : SessionHub("Binance", {kBinanceFastMs, kBinanceFastMs, kBinanceFastMs, kBinanceFastMs, kBinanceSlowMs,
                                 kBinanceSlowMs, kBinanceSlowMs, kBinanceSlowMs}) {}

    // ---- 仿真线程 ----
    // ---- Simulation thread ----
    void reset(const pub::Levels& bids, const pub::Levels& asks, std::uint64_t update_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        pub_.reset(bids, asks, update_id);
    }

    // 100 ms：增量快速流 + 三个 100 ms 部分深度流
    // 100 ms: fast incremental stream + three 100 ms partial depth streams
    void tickFast(const pub::Levels& bids, const pub::Levels& asks, std::uint64_t update_id, std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pub_.publish(bids, asks, update_id, now_ms, scratch_)) logs_[kFast].append(std::move(scratch_));
        publishPartialsLocked(kFast);
    }

    // 1000 ms：增量慢速流 + 三个 1000 ms 部分深度流（同一步先 tickFast）
    // 1000 ms: slow incremental stream + three 1000 ms partial depth streams (in the same step tickFast
    // runs first)
    void tickSlow(std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pub_.publishSlow(now_ms, scratch_)) logs_[kSlow].append(std::move(scratch_));
        publishPartialsLocked(kSlow);
    }

    // ---- 观测（测试用）----
    // ---- Observation (for tests) ----
    std::uint64_t lastUpdateId() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pub_.lastUpdateId();
    }

    // ---- 服务器回调（连接线程）----
    // ---- Server callbacks (connection threads) ----
    bool acceptUpgrade(const std::string& target) const { return parseEndpoint(target).ok; }

    void serve(WsSession& ws) {
        State st;
        const Endpoint ep = parseEndpoint(ws.target());
        st.combined = ep.combined;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const std::string& name : ep.streams) subscribeLocked(st, name);
        }
        log("session " + std::to_string(ws.sessionIndex()) + " opened: " + ws.target());
        runSession(
            ws, [&](const std::string& frame, std::vector<MessagePtr>& out) { return handleRequest(st, frame, out); },
            [&](std::vector<MessagePtr>& out) { return collect(st, out); });
    }

    HttpReply handleHttp(const HttpRequestInfo& req) {
        rest_requests_.fetch_add(1, std::memory_order_relaxed);
        HttpReply reply = restReply(req);
        if (reply.status != 200) bad_requests_.fetch_add(1, std::memory_order_relaxed);
        log("REST " + req.method + " " + req.target + " -> " + std::to_string(reply.status) + " (" +
            std::to_string(reply.body.size()) + " bytes)");
        return reply;
    }

private:
    // 流编号：快速流（100 ms）kFast + k、慢速流（1000 ms）kSlow + k；k = 0 为增量深度，1..3 为 depth5/10/20
    // Stream numbers: fast streams (100 ms) kFast + k, slow streams (1000 ms) kSlow + k; k = 0 is the
    // incremental depth, 1..3 are depth5/10/20
    static constexpr std::size_t kFast = 0;
    static constexpr std::size_t kSlow = 4;
    static constexpr std::size_t kPartialLevels[] = {5, 10, 20};

    struct Sub {
        std::string name;      // 客户端使用的流名（combined 包装里原样返回）
                               // stream name the client used (echoed verbatim in the combined wrapper)
        std::size_t stream;
        std::uint64_t cursor;
    };
    struct State {
        std::vector<Sub> subs;
        bool combined = false;
        simdjson::dom::parser parser;
        std::vector<std::size_t> owners;   // collect 暂存：每条报文属于哪个订阅
                                           // collect scratch: which subscription each message belongs to
    };
    struct Endpoint {
        bool ok = false;
        bool combined = false;
        std::vector<std::string> streams;
    };

    // 三个部分深度流各追加一条（mutex_ 已持有），然后唤醒会话
    // Appends one message to each of the three partial depth streams (mutex_ already held), then wakes
    // the sessions
    void publishPartialsLocked(std::size_t group) {
        for (std::size_t k = 0; k < 3; ++k) logs_[group + 1 + k].append(pub_.partialDepth(kPartialLevels[k]));
        notifyLocked();
    }

    // 流名 → 流编号；不是 btcusdt 的深度流返回 false
    // Stream name → stream number; returns false for anything that is not a btcusdt depth stream
    static bool streamFor(std::string_view name, std::size_t& index) {
        wire::BinanceStream s;
        if (!wire::parseBinanceStream(name, s)) return false;
        index = (s.period_ms == kBinanceFastMs ? kFast : kSlow) +
                (s.levels == 0 ? 0 : s.levels == 5 ? 1 : s.levels == 10 ? 2 : 3);
        return true;
    }

    static Endpoint parseEndpoint(const std::string& target) {
        const wire::HttpTarget t = wire::parseHttpTarget(target);
        Endpoint e;
        if (t.path == "/ws" || t.path == "/ws/") {
            e.ok = true;
        } else if (t.path.rfind("/ws/", 0) == 0) {
            e.ok = true;
            e.streams = detail::splitStreams(std::string_view(t.path).substr(4));
        } else if (t.path == "/stream" || t.path == "/stream/") {
            e.ok = true;
            e.combined = true;
            if (const std::string* s = t.param("streams")) e.streams = detail::splitStreams(*s);
        }
        std::size_t index = 0;
        for (const std::string& name : e.streams) {
            if (!streamFor(name, index)) e.ok = false;
        }
        return e;
    }

    void subscribeLocked(State& st, const std::string& name) {
        std::size_t index = 0;
        if (!streamFor(name, index)) return;
        for (const Sub& s : st.subs) {
            if (s.name == name) return;  // 已订阅：保持原游标
                                         // already subscribed: keep the existing cursor
        }
        st.subs.push_back(Sub{name, index, logs_[index].lastSeq()});
    }

    bool collect(State& st, std::vector<MessagePtr>& out) {
        const std::size_t base = out.size();
        st.owners.clear();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (std::size_t k = 0; k < st.subs.size(); ++k) {
                const std::size_t before = out.size();
                if (!logs_[st.subs[k].stream].collect(st.subs[k].cursor, out)) return false;
                st.owners.insert(st.owners.end(), out.size() - before, k);
            }
        }
        if (st.combined) {  // 包装在锁外进行
                            // wrapping happens outside the lock
            for (std::size_t i = base; i < out.size(); ++i) {
                out[i] = makeMessage(wire::binanceCombined(st.subs[st.owners[i - base]].name, *out[i]));
            }
        }
        return true;
    }

    // 请求 id：64 位整数、不超过 36 个字母数字的字符串或 null（与真实接口相同）；缺省按 null
    // Request id: a 64-bit integer, a string of at most 36 alphanumeric characters, or null (same as the
    // real API); a missing id counts as null
    static bool requestId(const simdjson::dom::object& obj, std::string& id_json) {
        simdjson::dom::element el;
        if (obj["id"].get(el) != simdjson::SUCCESS) {
            id_json = "null";
            return true;
        }
        switch (el.type()) {
            case simdjson::dom::element_type::INT64: {
                std::int64_t v = 0;
                if (el.get_int64().get(v) != simdjson::SUCCESS) return false;
                id_json = std::to_string(v);
                return true;
            }
            case simdjson::dom::element_type::NULL_VALUE: id_json = "null"; return true;
            case simdjson::dom::element_type::STRING: {
                std::string_view s;
                if (el.get_string().get(s) != simdjson::SUCCESS || s.size() > 36) return false;
                for (const char c : s) {
                    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) return false;
                }
                id_json.clear();
                wire::appendJsonString(id_json, s);
                return true;
            }
            default: return false;
        }
    }

    bool handleRequest(State& st, const std::string& frame, std::vector<MessagePtr>& out) {
        namespace dom = simdjson::dom;
        constexpr auto kOk = simdjson::SUCCESS;
        std::string id;  // 原样的 JSON 记号；解析出 id 之前为空（错误应答不带 id）
                         // the JSON token as written; empty until the id is parsed
                         // (error replies before that carry no id)
        auto reply = [&](std::string message) {
            out.push_back(makeMessage(std::move(message)));
            return true;
        };
        auto error = [&](int code, const std::string& msg) {
            out.push_back(makeMessage(wire::binanceError(code, msg, id)));
            return false;
        };
        dom::element doc;
        dom::object obj;
        if (st.parser.parse(frame).get(doc) != kOk) return error(3, "Invalid JSON: expected a JSON request object");
        if (doc.get_object().get(obj) != kOk) return error(2, "Invalid request: expected a JSON object");
        if (!requestId(obj, id)) return error(2, "Invalid request: request ID must be an unsigned integer");
        std::string_view method;
        if (obj["method"].get_string().get(method) != kOk) return error(2, "Invalid request: missing field `method`");
        dom::array params;
        const bool has_params = obj["params"].get_array().get(params) == kOk;
        if (method == "SUBSCRIBE" || method == "UNSUBSCRIBE") {
            if (!has_params) return error(2, "Invalid request: missing field `params`");
            std::vector<std::string> names;
            for (dom::element p : params) {
                std::string_view name;
                std::size_t index = 0;
                if (p.get_string().get(name) != kOk) return error(2, "Invalid request: stream names must be strings");
                if (!streamFor(name, index)) return error(2, "Invalid request: unknown stream '" + std::string(name) + "'");
                names.emplace_back(name);
            }
            if (method == "SUBSCRIBE") {
                std::lock_guard<std::mutex> lock(mutex_);
                for (const std::string& name : names) subscribeLocked(st, name);
            } else {
                for (const std::string& name : names) {
                    st.subs.erase(std::remove_if(st.subs.begin(), st.subs.end(),
                                                 [&name](const Sub& s) { return s.name == name; }),
                                  st.subs.end());
                }
            }
            std::string joined;
            for (const std::string& n : names) joined += (joined.empty() ? "" : ",") + n;
            log(std::string(method) + " " + joined);
            return reply(wire::binanceResult("null", id));
        }
        if (method == "LIST_SUBSCRIPTIONS") {
            std::string list = "[";
            for (const Sub& s : st.subs) {
                if (list.size() > 1) list.push_back(',');
                wire::appendJsonString(list, s.name);
            }
            list.push_back(']');
            return reply(wire::binanceResult(list, id));
        }
        if (method == "SET_PROPERTY" || method == "GET_PROPERTY") {
            std::string_view prop;
            if (!has_params || params.at(0).get_string().get(prop) != kOk || prop != "combined") {
                return error(0, "Unknown property");
            }
            if (method == "GET_PROPERTY") return reply(wire::binanceResult(st.combined ? "true" : "false", id));
            bool value = false;
            if (params.size() != 2 || params.at(1).get_bool().get(value) != kOk) {
                return error(1, "Invalid value type: expected Boolean");
            }
            st.combined = value;
            return reply(wire::binanceResult("null", id));
        }
        return error(2, "Invalid request: unknown variant `" + std::string(method) +
                            "`, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, "
                            "`GET_PROPERTY`");
    }

    HttpReply restReply(const HttpRequestInfo& req) const {
        auto reply = [](int status, std::string body) {
            return HttpReply{status, std::move(body), "application/json", 0};
        };
        if (req.method != "GET") {
            return reply(405, wire::binanceError(-1000, "Only GET is supported by the mock exchange."));
        }
        const wire::HttpTarget t = wire::parseHttpTarget(req.target);
        if (t.path == "/api/v3/ping") return reply(200, "{}");
        if (t.path == "/api/v3/time") return reply(200, "{\"serverTime\":" + std::to_string(hft::wallClockMs()) + "}");
        if (t.path != "/api/v3/depth") return reply(404, wire::binanceError(-1, "Unknown endpoint " + t.path));
        const std::string* symbol = t.param("symbol");
        if (symbol == nullptr || symbol->empty()) {
            return reply(400, wire::binanceError(-1102, "Mandatory parameter 'symbol' was not sent, was empty/null, or "
                                                        "malformed."));
        }
        const bool legal = symbol->size() <= 20 && std::all_of(symbol->begin(), symbol->end(), [](char c) {
                               return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                                      c == '.';
                           });
        if (!legal) {
            return reply(400, wire::binanceError(-1100, "Illegal characters found in parameter 'symbol'; legal range "
                                                        "is '^[A-Z0-9-_.]{1,20}$'."));
        }
        if (*symbol != wire::kBinanceSymbol) return reply(400, wire::binanceError(-1121, "Invalid symbol."));
        std::size_t limit = pub::BinanceDepthPublisher::kDefaultRestLimit;
        if (const std::string* l = t.param("limit")) {
            std::uint64_t v = 0;
            const bool digits = !l->empty() && l->size() <= 20 &&
                                std::all_of(l->begin(), l->end(), [](char c) { return c >= '0' && c <= '9'; });
            const auto res = std::from_chars(l->data(), l->data() + l->size(), v);
            if (!digits || res.ec != std::errc() || v == 0) {
                return reply(400, wire::binanceError(-1100, "Illegal characters found in parameter 'limit'; legal "
                                                            "range is '^[0-9]{1,20}$'."));
            }
            limit = static_cast<std::size_t>(std::min<std::uint64_t>(v, pub::BinanceDepthPublisher::kMaxRestLimit));
        }
        std::lock_guard<std::mutex> lock(mutex_);
        return reply(200, pub_.restSnapshot(limit));
    }

    pub::BinanceDepthPublisher pub_;
    std::string scratch_;
};

// ---------------------------------------------------------------------------
// OKX：/ws/v5/public；op subscribe / unsubscribe（args [{channel, instId}]，可带 id），"ping" → "pong"；
// 频道 books（400 档增量 + checksum）、books5、bbo-tbt
// OKX: /ws/v5/public; op subscribe / unsubscribe (args [{channel, instId}], optionally with id),
// "ping" → "pong"; channels books (400-level incremental + checksum), books5, bbo-tbt
// ---------------------------------------------------------------------------
class OkxHub final : public SessionHub {
public:
    enum Channel : std::size_t { kBooks, kBooks5, kBbo, kChannelCount };

    OkxHub() : SessionHub("OKX", {kOkxBooksMs, kOkxBooksMs, kOkxBboMs}) {}

    void reset(const pub::Levels& bids, const pub::Levels& asks, std::int64_t seq, std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        books_.reset(bids, asks, seq, now_ms);
        books5_.reset(bids, asks, seq);
        bbo_.reset(bids, asks, seq);
    }

    // 100 ms：books + books5
    // 100 ms: books + books5
    void tickBooks(const pub::Levels& bids, const pub::Levels& asks, std::int64_t seq, std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        const bool books = books_.publish(bids, asks, seq, now_ms, scratch_);
        if (books) logs_[kBooks].append(std::move(scratch_));
        const bool books5 = books5_.publish(bids, asks, seq, now_ms, scratch_);
        if (books5) logs_[kBooks5].append(std::move(scratch_));
        if (books || books5) notifyLocked();
    }

    // 10 ms：bbo-tbt
    // 10 ms: bbo-tbt
    void tickBbo(const pub::Levels& bids, const pub::Levels& asks, std::int64_t seq, std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (bbo_.publish(bids, asks, seq, now_ms, scratch_)) {
            logs_[kBbo].append(std::move(scratch_));
            notifyLocked();
        }
    }

    std::int64_t lastSeqId() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return books_.lastSeqId();
    }

    bool acceptUpgrade(const std::string& target) const {
        return wire::parseHttpTarget(target).path == "/ws/v5/public";
    }

    void serve(WsSession& ws) {
        State st;
        st.conn_id = detail::randomHex(8);
        log("session " + std::to_string(ws.sessionIndex()) + " opened (connId " + st.conn_id + ")");
        runSession(
            ws, [&](const std::string& frame, std::vector<MessagePtr>& out) { return handleRequest(st, frame, out); },
            [&](std::vector<MessagePtr>& out) { return collectSubscribed(st.subscribed, st.cursor, out); });
    }

private:
    struct State {
        std::string conn_id;
        bool subscribed[kChannelCount] = {};
        std::uint64_t cursor[kChannelCount] = {};
        simdjson::dom::parser parser;
    };

    static int channelIndex(std::string_view channel) noexcept {
        if (channel == "books") return kBooks;
        if (channel == "books5") return kBooks5;
        if (channel == "bbo-tbt") return kBbo;
        return -1;
    }

    // 新订阅的第一条推送（mutex_ 已持有）
    // First push of a new subscription (mutex_ already held)
    std::string initialLocked(std::size_t channel, std::int64_t now_ms) const {
        switch (channel) {
            case kBooks: return books_.snapshot(now_ms);
            case kBooks5: return books5_.current(now_ms);
            default: return bbo_.current(now_ms);
        }
    }

    bool handleRequest(State& st, const std::string& frame, std::vector<MessagePtr>& out) {
        namespace dom = simdjson::dom;
        if (frame == "ping") {
            out.push_back(makeMessage("pong"));
            return true;
        }
        auto invalid = [&](std::string_view id) {
            out.push_back(makeMessage(wire::okxError("60012", "Invalid request: " + frame, st.conn_id, id)));
            return false;
        };
        dom::element doc;
        dom::object obj;
        if (st.parser.parse(frame).get(doc) != simdjson::SUCCESS || doc.get_object().get(obj) != simdjson::SUCCESS) {
            return invalid(std::string_view());
        }
        std::string_view id;
        dom::element id_el;
        if (obj["id"].get(id_el) == simdjson::SUCCESS && id_el.get_string().get(id) != simdjson::SUCCESS) {
            return invalid(std::string_view());
        }
        std::string_view op;
        dom::array args;
        if (obj["op"].get_string().get(op) != simdjson::SUCCESS || (op != "subscribe" && op != "unsubscribe") ||
            obj["args"].get_array().get(args) != simdjson::SUCCESS || args.size() == 0) {
            return invalid(id);
        }
        bool ok = true;
        for (dom::element a : args) {
            dom::object arg;
            std::string_view channel, inst;
            if (a.get_object().get(arg) != simdjson::SUCCESS ||
                arg["channel"].get_string().get(channel) != simdjson::SUCCESS) {
                ok = invalid(id);
                continue;
            }
            if (arg["instId"].get_string().get(inst) != simdjson::SUCCESS) inst = std::string_view();
            const int c = channelIndex(channel);
            if (c < 0 || inst != wire::kOkxInstId) {
                out.push_back(makeMessage(wire::okxError("60018",
                                                         "Wrong URL or channel:" + std::string(channel) + ",instId:" +
                                                             std::string(inst) + " doesn't exist",
                                                         st.conn_id, id)));
                ok = false;
                continue;
            }
            const auto ch = static_cast<std::size_t>(c);
            out.push_back(makeMessage(wire::okxEvent(op, channel, inst, st.conn_id, id)));
            if (op == "subscribe") {
                // 快照与游标在同一把锁内取得：之后的增量严格衔接快照的 seqId
                // Snapshot and cursor are taken under the same lock: the incremental updates that follow chain
                // exactly onto the snapshot's seqId
                std::lock_guard<std::mutex> lock(mutex_);
                out.push_back(makeMessage(initialLocked(ch, hft::wallClockMs())));
                st.subscribed[ch] = true;
                st.cursor[ch] = logs_[ch].lastSeq();
            } else {
                st.subscribed[ch] = false;
            }
            log(std::string(op) + " " + std::string(channel) + " " + std::string(inst) + " (connId " + st.conn_id +
                ")");
        }
        return ok;
    }

    pub::OkxBooksPublisher books_;
    pub::OkxTopPublisher books5_{"books5", 5};
    pub::OkxTopPublisher bbo_{"bbo-tbt", 1};
    std::string scratch_;
};

// ---------------------------------------------------------------------------
// Bybit：/v5/public/spot；op subscribe / unsubscribe（args ["orderbook.{1,50,200}.BTCUSDT"]，可带 req_id）、
// op ping → pong 应答
// Bybit: /v5/public/spot; op subscribe / unsubscribe (args ["orderbook.{1,50,200}.BTCUSDT"],
// optionally with req_id), op ping → pong reply
// ---------------------------------------------------------------------------
class BybitHub final : public SessionHub {
public:
    BybitHub() : SessionHub("Bybit", {kBybitPeriodsMs[0], kBybitPeriodsMs[1], kBybitPeriodsMs[2]}) {}

    void reset(const pub::Levels& bids, const pub::Levels& asks, std::uint64_t seq) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& b : books_) b.reset(bids, asks, seq);
    }

    void tick(std::size_t depth_index, const pub::Levels& bids, const pub::Levels& asks, std::uint64_t seq,
              std::int64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (books_[depth_index].publish(bids, asks, seq, now_ms, now_ms, scratch_)) {
            logs_[depth_index].append(std::move(scratch_));
            notifyLocked();
        }
    }

    std::uint64_t lastUpdateId(std::size_t depth_index) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return books_.at(depth_index).lastUpdateId();
    }

    bool acceptUpgrade(const std::string& target) const {
        return wire::parseHttpTarget(target).path == "/v5/public/spot";
    }

    void serve(WsSession& ws) {
        State st;
        st.conn_id = detail::makeUuid();
        log("session " + std::to_string(ws.sessionIndex()) + " opened (conn_id " + st.conn_id + ")");
        runSession(
            ws, [&](const std::string& frame, std::vector<MessagePtr>& out) { return handleRequest(st, frame, out); },
            [&](std::vector<MessagePtr>& out) { return collectSubscribed(st.subscribed, st.cursor, out); });
    }

private:
    struct State {
        std::string conn_id;
        bool subscribed[kBybitDepthCount] = {};
        std::uint64_t cursor[kBybitDepthCount] = {};
        simdjson::dom::parser parser;
    };

    static int depthIndex(int depth) noexcept {
        for (std::size_t i = 0; i < kBybitDepthCount; ++i) {
            if (static_cast<int>(kBybitDepths[i]) == depth) return static_cast<int>(i);
        }
        return -1;
    }

    bool handleRequest(State& st, const std::string& frame, std::vector<MessagePtr>& out) {
        namespace dom = simdjson::dom;
        auto respond = [&](bool success, const std::string& ret_msg, std::string_view req_id, std::string_view op) {
            out.push_back(makeMessage(wire::bybitResponse(success, ret_msg, st.conn_id, req_id, op)));
            return success;
        };
        dom::element doc;
        dom::object obj;
        if (st.parser.parse(frame).get(doc) != simdjson::SUCCESS || doc.get_object().get(obj) != simdjson::SUCCESS) {
            return respond(false, "error:request is not a JSON object", std::string_view(), std::string_view());
        }
        std::string_view req_id;
        if (obj["req_id"].get_string().get(req_id) != simdjson::SUCCESS) req_id = std::string_view();
        std::string_view op;
        if (obj["op"].get_string().get(op) != simdjson::SUCCESS) {
            return respond(false, "error:missing op", req_id, std::string_view());
        }
        if (op == "ping") return respond(true, "pong", req_id, "ping");
        if (op != "subscribe" && op != "unsubscribe") {
            return respond(false, "error:unsupported op " + std::string(op), req_id, op);
        }
        dom::array args;
        if (obj["args"].get_array().get(args) != simdjson::SUCCESS) {
            return respond(false, "error:args must be an array of topics", req_id, op);
        }
        std::string failures;
        std::vector<std::size_t> accepted;
        auto fail = [&failures](const char* what, std::string_view topic) {
            failures += (failures.empty() ? "" : ";") + std::string(what) + std::string(topic);
        };
        for (dom::element a : args) {
            std::string_view topic;
            int depth = 0;
            int index = -1;
            if (a.get_string().get(topic) == simdjson::SUCCESS && wire::parseBybitTopic(topic, depth)) {
                index = depthIndex(depth);
            }
            if (index < 0) {
                fail("error:handler not found,topic:", topic);
                continue;
            }
            const auto i = static_cast<std::size_t>(index);
            const bool duplicate = st.subscribed[i] || std::find(accepted.begin(), accepted.end(), i) != accepted.end();
            if (op == "subscribe" && duplicate) {
                fail("error:already subscribed,topic:", topic);
                continue;
            }
            accepted.push_back(i);
        }
        const bool success = respond(failures.empty(), failures.empty() ? std::string(op) : failures, req_id, op);
        for (const std::size_t i : accepted) {
            if (op == "subscribe") {
                // 快照与游标在同一把锁内取得：之后的 delta 从快照的 u + 1 开始
                // Snapshot and cursor are taken under the same lock: the deltas that follow start at the
                // snapshot's u + 1
                std::lock_guard<std::mutex> lock(mutex_);
                const std::int64_t now = hft::wallClockMs();
                out.push_back(makeMessage(books_[i].snapshot(now, now)));
                st.subscribed[i] = true;
                st.cursor[i] = logs_[i].lastSeq();
            } else {
                st.subscribed[i] = false;
            }
            log(std::string(op) + " " + books_[i].topic() + " (conn_id " + st.conn_id + ")");
        }
        return success;
    }

    std::array<pub::BybitBookPublisher, kBybitDepthCount> books_{pub::BybitBookPublisher(kBybitDepths[0]),
                                                                 pub::BybitBookPublisher(kBybitDepths[1]),
                                                                 pub::BybitBookPublisher(kBybitDepths[2])};
    std::string scratch_;
};

// ---------------------------------------------------------------------------
// 整个模拟交易所
// The whole mock exchange
// ---------------------------------------------------------------------------
struct MockExchangeOptions {
    std::string bind_address = "127.0.0.1";
    unsigned short binance_port = 19443;   // 0 = 临时端口（测试用）
                                           // 0 = ephemeral port (for tests)
    unsigned short okx_port = 18443;
    unsigned short bybit_port = 18444;
    std::uint64_t seed = 1;
    double warmup_s = 5.0;                 // 开始监听之前先仿真这么久（簿已建好、价格已有走势）
                                           // simulate this long before listening starts
                                           // (books built up, prices already moving)
    double drop_rate = 0.0;                // 故障注入：推送报文不发给某个会话的概率（见 SessionHub::setDropRate）
                                           // fault injection: probability that a push message is withheld
                                           // from a given session (see SessionHub::setDropRate)
    LogFn log;                             // 连接 / 订阅 / REST 事件（在连接线程上调用，须线程安全）；空 = 不记录
                                           // connection / subscription / REST events (called on connection
                                           // threads, must be thread-safe); empty = no logging
};

class MockExchange {
public:
    static constexpr int kMaxCatchUpSteps = 50;   // 一次最多补跑的步数（250 ms），再多就丢弃积压
                                                  // max steps run at once to catch up (250 ms),
                                                  // any larger backlog is dropped
    static constexpr double kArbFeeRate = 0.001;  // 统计"扣除手续费后仍有利润"的交叉时使用的费率
                                                  // fee rate for counting crosses that are
                                                  // "still profitable after fees"

    struct VenueStatus {
        std::string name;
        double best_bid = 0.0, best_ask = 0.0;   // 空簿为 0
                                                 // 0 for an empty book
        std::size_t orders = 0;
        std::uint64_t trades = 0;
        std::uint64_t large_dislocations = 0;
        SessionHub::Counters net;
    };

    struct Status {
        double sim_seconds = 0.0;
        double market_fair = 0.0;
        std::vector<VenueStatus> venues;
        // 每 100 ms 采样一次三家盘口：存在跨所交叉（某家买一 > 另一家卖一）/ 扣除 0.1% 双边手续费后仍交叉
        // The three venues' top of book is sampled every 100 ms: a cross-venue cross exists (one venue's
        // best bid > another venue's best ask) / still crossed after deducting the 0.1% fee on both sides
        std::uint64_t arb_samples = 0, arb_cross = 0, arb_cross_after_fees = 0;
        double last_gap_bps = 0.0;         // 最近一次采样的最大交叉（基点，负数 = 没有交叉）
                                           // largest cross in the latest sample (bps, negative = no cross)
        std::string last_gap_pair;         // 例如 "buy OKX / sell Bybit"
                                           // e.g. "buy OKX / sell Bybit"
        std::uint64_t lag_resets = 0;      // 跟不上实时而丢弃积压的次数
                                           // times the backlog was dropped for falling behind real time
    };

    // 三家交易所，顺序 Binance、OKX、Bybit（sim::defaultMarketParams()）
    // Three exchanges, in the order Binance, OKX, Bybit (sim::defaultMarketParams())
    explicit MockExchange(MockExchangeOptions opt = MockExchangeOptions())
        : opt_(std::move(opt)), market_(sim::defaultMarketParams(), opt_.seed) {
        const double step_s = market_.stepSeconds();
        step_ = std::chrono::nanoseconds(std::llround(step_s * 1e9));
        binance_fast_ = detail::periodSteps(kBinanceFastMs, step_s);
        binance_slow_ = binance_fast_ * static_cast<std::uint64_t>(kBinanceSlowMs / kBinanceFastMs);
        okx_books_ = detail::periodSteps(kOkxBooksMs, step_s);
        okx_bbo_ = detail::periodSteps(kOkxBboMs, step_s);
        for (std::size_t i = 0; i < kBybitDepthCount; ++i) {
            bybit_period_[i] = detail::periodSteps(kBybitPeriodsMs[i], step_s);
        }
        for (SessionHub* h : hubs_) {
            h->setLog(opt_.log);
            h->setDropRate(opt_.drop_rate);
        }

        market_.advance(opt_.warmup_s);
        resetHubs();

        identity_ = generateSelfSigned();  // CN/SAN = localhost、127.0.0.1；三个端口共用
                                           // CN/SAN = localhost, 127.0.0.1; shared by all three ports
        servers_[0] = makeServer(opt_.binance_port, binance_);
        servers_[1] = makeServer(opt_.okx_port, okx_);
        servers_[2] = makeServer(opt_.bybit_port, bybit_);
        sim_thread_ = std::thread([this] { simLoop(); });
    }

    ~MockExchange() { stop(); }
    MockExchange(const MockExchange&) = delete;
    MockExchange& operator=(const MockExchange&) = delete;

    // 停止仿真与全部服务器（连接在约 10 ms 内结束，线程全部 join）。幂等。
    // Stops the simulation and all servers (connections end within about 10 ms, all threads are joined).
    // Idempotent.
    void stop() noexcept {
        if (stopped_.exchange(true)) return;
        {
            std::lock_guard<std::mutex> lock(sim_mutex_);
            stop_ = true;
        }
        sim_cv_.notify_all();
        if (sim_thread_.joinable()) sim_thread_.join();
        for (auto& s : servers_) {
            if (s) s->stop();
        }
    }

    unsigned short binancePort() const noexcept { return servers_[0]->port(); }
    unsigned short okxPort() const noexcept { return servers_[1]->port(); }
    unsigned short bybitPort() const noexcept { return servers_[2]->port(); }
    const std::string& caPem() const noexcept { return identity_.cert_pem; }

    // 把 CA 证书（即服务器的自签名证书）写到 path（原子替换）；失败抛 std::runtime_error
    // Writes the CA certificate (i.e. the server's self-signed certificate) to path (atomic replace);
    // throws std::runtime_error on failure
    void writeCaFile(const std::string& path) const { detail::writeFileAtomic(path, identity_.cert_pem); }

    const BinanceHub& binance() const noexcept { return binance_; }
    const OkxHub& okx() const noexcept { return okx_; }
    const BybitHub& bybit() const noexcept { return bybit_; }

    // 故障注入：三家交易所的推送报文丢弃概率（0 = 关闭）
    // Fault injection: push message drop probability for all three exchanges (0 = off)
    void setDropRate(double p) noexcept {
        for (SessionHub* h : hubs_) h->setDropRate(p);
    }
    // 单独设置一家（0 = Binance，1 = OKX，2 = Bybit；其他编号忽略）
    // Sets a single exchange (0 = Binance, 1 = OKX, 2 = Bybit; other numbers are ignored)
    void setDropRate(std::size_t venue, double p) noexcept {
        if (venue < hubs_.size()) hubs_[venue]->setDropRate(p);
    }

    // ---- 仿真控制（测试用）----
    // 暂停仿真，并立即对全部推送流做一次发布（计划外），使各发布者的窗口等于引擎此刻的深度
    // ---- Simulation control (for tests) ----
    // Pauses the simulation and immediately publishes all push streams once (unscheduled), so that every
    // publisher's window equals the engine's current depth
    void pause() {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        paused_ = true;
        publishLocked(steps_, true);
    }
    void resume() {
        {
            std::lock_guard<std::mutex> lock(sim_mutex_);
            paused_ = false;
        }
        sim_cv_.notify_all();
    }

    // 在仿真锁内访问市场（仿真线程此时不会推进）
    // Accesses the market under the simulation lock (the simulation thread does not advance meanwhile)
    template <class F>
    auto withMarket(F&& f) const -> decltype(f(std::declval<const sim::Market&>())) {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        return f(market_);
    }

    // 三个服务器记录的脚本错误（正常运行应为空）
    // Script errors recorded by the three servers (should be empty in a normal run)
    std::vector<std::string> serverErrors() const {
        std::vector<std::string> all;
        for (const auto& s : servers_) {
            for (std::string& e : s->errors()) all.push_back(std::move(e));
        }
        return all;
    }

    Status status() const {
        Status st;
        std::lock_guard<std::mutex> lock(sim_mutex_);
        st.sim_seconds = market_.simSeconds();
        st.market_fair = market_.fair();
        for (std::size_t i = 0; i < market_.venueCount(); ++i) {
            const sim::Venue& v = market_.venue(i);
            VenueStatus vs;
            vs.name = v.name();
            if (const auto b = v.book().bestBid()) vs.best_bid = toPrice(*b);
            if (const auto a = v.book().bestAsk()) vs.best_ask = toPrice(*a);
            vs.orders = v.book().orderCount();
            vs.trades = v.stats().trades;
            vs.large_dislocations = v.largeDislocations();
            vs.net = hubs_[i]->counters();
            st.venues.push_back(std::move(vs));
        }
        st.arb_samples = arb_samples_;
        st.arb_cross = arb_cross_;
        st.arb_cross_after_fees = arb_cross_fees_;
        st.last_gap_bps = last_gap_bps_;
        st.last_gap_pair = last_gap_pair_;
        st.lag_resets = lag_resets_;
        return st;
    }

private:
    using Clock = std::chrono::steady_clock;

    static double toPrice(sim::Price ticks) noexcept {
        return static_cast<double>(ticks) / static_cast<double>(sim::kTicksPerUnit);
    }

    template <class Hub>
    std::unique_ptr<MockExchangeServer> makeServer(unsigned short port, Hub& hub) {
        ServerOptions so;
        so.bind_address = opt_.bind_address;
        so.port = port;
        so.identity = identity_;
        so.upgrade_filter = [&hub](const std::string& target) { return hub.acceptUpgrade(target); };
        so.script = [&hub](WsSession& ws) { hub.serve(ws); };
        if constexpr (std::is_same_v<Hub, BinanceHub>) {
            so.http_handler = [&hub](const HttpRequestInfo& req) { return hub.handleHttp(req); };
        } else {
            so.http_handler = [](const HttpRequestInfo&) {
                return HttpReply{404, R"({"code":"404","msg":"no REST endpoints on this port"})", "application/json",
                                 0};
            };
        }
        try {
            return std::make_unique<MockExchangeServer>(std::move(so));
        } catch (const boost::system::system_error& e) {  // 典型：端口已被占用
                                                          // typically: the port is already in use
            throw std::runtime_error("cannot listen on " + opt_.bind_address + ":" + std::to_string(port) + " (" +
                                     hub.name() + "): " + e.code().message());
        }
    }

    // 预热结束：以当前深度初始化全部发布者（不产生推送）
    // End of warm-up: initializes all publishers with the current depth (produces no pushes)
    void resetHubs() {
        const std::int64_t now_ms = hft::wallClockMs();
        capture(0, pub::BinanceDepthPublisher::kWindow);
        binance_.reset(bids_, asks_, market_.venue(0).bookEvents());
        capture(1, pub::OkxBooksPublisher::kWindow);
        okx_.reset(bids_, asks_, static_cast<std::int64_t>(market_.venue(1).bookEvents()), now_ms);
        capture(2, kBybitDepths[kBybitDepthCount - 1]);
        bybit_.reset(bids_, asks_, market_.venue(2).bookEvents());
    }

    void capture(std::size_t venue, std::size_t levels) {
        const sim::Venue& v = market_.venue(venue);
        bids_ = v.depth(sim::Side::Buy, levels);
        asks_ = v.depth(sim::Side::Sell, levels);
    }

    void simLoop() {
        hft::setThreadName("mock-sim");
        std::unique_lock<std::mutex> lock(sim_mutex_);
        Clock::time_point next = Clock::now() + step_;
        while (!stop_) {
            if (paused_) {
                sim_cv_.wait(lock, [this] { return stop_ || !paused_; });
                next = Clock::now() + step_;
                continue;
            }
            if (Clock::now() < next) {
                sim_cv_.wait_until(lock, next, [this] { return stop_ || paused_; });
                continue;
            }
            int budget = kMaxCatchUpSteps;
            while (!stop_ && !paused_ && budget > 0 && Clock::now() >= next) {
                market_.step();
                ++steps_;
                publishLocked(steps_, false);
                next += step_;
                --budget;
            }
            if (budget == 0 && Clock::now() >= next) {  // 跟不上实时：丢弃积压，从现在继续
                                                        // cannot keep up with real time: drop the backlog
                                                        // and continue from now
                ++lag_resets_;
                next = Clock::now() + step_;
            }
        }
    }

    // 到期的推送流发布一次（force：全部发布）。sim_mutex_ 已持有。
    // Publishes each push stream that is due, once (force: publish all of them). sim_mutex_ is already
    // held.
    void publishLocked(std::uint64_t k, bool force) {
        const std::int64_t now_ms = hft::wallClockMs();
        auto due = [k, force](std::uint64_t period) { return force || k % period == 0; };
        if (due(binance_fast_)) {
            capture(0, pub::BinanceDepthPublisher::kWindow);
            binance_.tickFast(bids_, asks_, market_.venue(0).bookEvents(), now_ms);
            sampleArbitrageLocked();
        }
        if (due(binance_slow_)) binance_.tickSlow(now_ms);
        const bool okx_books = due(okx_books_);
        const bool okx_bbo = due(okx_bbo_);
        if (okx_books || okx_bbo) {
            capture(1, pub::OkxBooksPublisher::kWindow);
            const auto seq = static_cast<std::int64_t>(market_.venue(1).bookEvents());
            if (okx_books) okx_.tickBooks(bids_, asks_, seq, now_ms);
            if (okx_bbo) okx_.tickBbo(bids_, asks_, seq, now_ms);
        }
        bool captured = false;
        for (std::size_t i = 0; i < kBybitDepthCount; ++i) {
            if (!due(bybit_period_[i])) continue;
            if (!captured) {
                capture(2, kBybitDepths[kBybitDepthCount - 1]);
                captured = true;
            }
            bybit_.tick(i, bids_, asks_, market_.venue(2).bookEvents(), now_ms);
        }
    }

    // 跨所交叉统计：任意有序对 (i 买, j 卖) 的 bid_j - ask_i
    // Cross-venue cross statistics: bid_j - ask_i over every ordered pair (i buys, j sells)
    void sampleArbitrageLocked() {
        double best = -std::numeric_limits<double>::infinity();
        bool fee_ok = false;
        std::string pair;
        for (std::size_t i = 0; i < market_.venueCount(); ++i) {
            const auto ask = market_.venue(i).book().bestAsk();
            if (!ask) continue;
            for (std::size_t j = 0; j < market_.venueCount(); ++j) {
                if (i == j) continue;
                const auto bid = market_.venue(j).book().bestBid();
                if (!bid) continue;
                const double a = toPrice(*ask);
                const double b = toPrice(*bid);
                const double gap_bps = (b - a) / a * 1e4;
                if (b * (1.0 - kArbFeeRate) - a * (1.0 + kArbFeeRate) > 0.0) fee_ok = true;
                if (gap_bps > best) {
                    best = gap_bps;
                    pair = "buy " + market_.venue(i).name() + " / sell " + market_.venue(j).name();
                }
            }
        }
        if (!std::isfinite(best)) return;
        ++arb_samples_;
        if (best > 0.0) ++arb_cross_;
        if (fee_ok) ++arb_cross_fees_;
        last_gap_bps_ = best;
        last_gap_pair_ = std::move(pair);
    }

    MockExchangeOptions opt_;
    sim::Market market_;   // 必须在 opt_ 之后声明（用 opt_.seed 构造）
                           // must be declared after opt_ (constructed with opt_.seed)
    Clock::duration step_{};
    std::uint64_t binance_fast_ = 1, binance_slow_ = 1, okx_books_ = 1, okx_bbo_ = 1;
    std::array<std::uint64_t, kBybitDepthCount> bybit_period_{};

    // Hub 必须先于服务器构造、晚于服务器销毁（服务器的回调持有 Hub 的引用）
    // The Hubs must be constructed before the servers and destroyed after them (the servers' callbacks
    // hold references to the Hubs)
    BinanceHub binance_;
    OkxHub okx_;
    BybitHub bybit_;
    const std::array<SessionHub*, 3> hubs_{&binance_, &okx_, &bybit_};   // 按交易所编号
                                                                         // indexed by exchange number
    TlsIdentity identity_;
    std::unique_ptr<MockExchangeServer> servers_[3];

    mutable std::mutex sim_mutex_;    // 保护 market_ 与以下仿真状态
                                      // guards market_ and the simulation state below
    std::condition_variable sim_cv_;
    bool stop_ = false;
    bool paused_ = false;
    std::uint64_t steps_ = 0;
    std::uint64_t lag_resets_ = 0;
    pub::Levels bids_, asks_;          // 取深度的复用缓冲
                                       // reused buffers for depth captures
    std::uint64_t arb_samples_ = 0, arb_cross_ = 0, arb_cross_fees_ = 0;
    double last_gap_bps_ = 0.0;
    std::string last_gap_pair_;
    std::thread sim_thread_;
    std::atomic<bool> stopped_{false};
};

}  // namespace mock_exchange
