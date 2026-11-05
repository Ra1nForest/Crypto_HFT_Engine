//
// test_feed_integration.cpp
// 行情连接管理器集成测试：进程内 TLS 模拟交易所（tests/mock_exchange_server.h）驱动真实的 MarketDataFeed，
// 覆盖 Binance 增量同步、OKX 校验和与缺口重订阅、Bybit 心跳、断线退避重连、停顿踢断、stop 的及时性与证书校验。
//

// 启用库内调试断言（必须在包含任何 hft 头文件之前定义）
#define HFT_ENABLE_ASSERTS

#include "test_framework.h"

#include "hft/crc32.h"
#include "hft/timing.h"
#include "market_data_feed.h"
#include "mock_exchange_server.h"
#include "net/https_client.h"

#include <arpa/inet.h>
#include <dlfcn.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// ============================================================================
// 全局 operator new 计数（只统计被跟踪的线程）：验证 feed 线程稳态下每条消息零堆分配
// ============================================================================

namespace {
std::atomic<std::uint64_t> g_alloc_thread{0};  // 被跟踪线程的 pthread_self()，0 = 不统计
std::atomic<std::uint64_t> g_alloc_count{0};

inline void countAllocation() noexcept {
    const std::uint64_t tracked = g_alloc_thread.load(std::memory_order_relaxed);
    if (tracked != 0 && tracked == static_cast<std::uint64_t>(pthread_self())) {
        g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    }
}
}  // namespace

// 替换函数标记 noinline：否则 GCC 把 free 内联进调用点后会误报 -Wmismatched-new-delete
// （与 tests/test_market_data.cpp 相同；nothrow 版本也替换，保证同一分配器配对）
__attribute__((noinline)) void* operator new(std::size_t n) {
    countAllocation();
    void* p = std::malloc(n == 0 ? 1 : n);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}
__attribute__((noinline)) void* operator new[](std::size_t n) { return ::operator new(n); }
__attribute__((noinline)) void operator delete(void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::size_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
__attribute__((noinline)) void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    countAllocation();
    return std::malloc(n == 0 ? 1 : n);
}
__attribute__((noinline)) void* operator new[](std::size_t n, const std::nothrow_t& t) noexcept {
    return ::operator new(n, t);
}
__attribute__((noinline)) void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }

// ============================================================================
// getaddrinfo 替换："dual.test" 解析为两个 127.0.0.1 端点（各带端口，忽略 service），模拟第一个地址不通的
// 多地址主机（本机没有可用的 IPv6，无法用真实的双栈地址）；其他名字交给真正的 getaddrinfo。
// connect 替换：测试可以让下一次 connect() 停在系统调用之前——正是"套接字已登记、连接尚未发起"的窗口；
// 也可以记录发往某个端口的每次 connect() 的时刻（见 ConnectTrace）。
// ============================================================================

namespace {
std::atomic<int> g_dual_first_port{0};   // 0 = 不替换 "dual.test"
std::atomic<int> g_dual_second_port{0};
std::atomic<bool> g_connect_park{false};     // 下一次 connect() 停住
std::atomic<bool> g_connect_parked{false};   // 已经停在系统调用之前
std::atomic<bool> g_connect_release{false};  // 放行
std::atomic<int> g_connect_trace_port{0};    // 非 0：记录发往这个端口的每次 connect() 的 nowNs()
std::mutex g_connect_trace_mutex;            // 保护 g_connect_trace_ns
std::vector<std::int64_t> g_connect_trace_ns;

// 与 glibc 相同：addrinfo 与它的 sockaddr 在同一块内存里（freeaddrinfo 只释放 addrinfo 本身）
addrinfo* loopbackAddrinfo(int port, addrinfo* next) {
    void* block = std::calloc(1, sizeof(addrinfo) + sizeof(sockaddr_in));
    if (block == nullptr) return nullptr;
    auto* ai = static_cast<addrinfo*>(block);
    auto* sa = reinterpret_cast<sockaddr_in*>(static_cast<char*>(block) + sizeof(addrinfo));
    sa->sin_family = AF_INET;
    sa->sin_port = htons(static_cast<std::uint16_t>(port));
    sa->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ai->ai_family = AF_INET;
    ai->ai_socktype = SOCK_STREAM;
    ai->ai_protocol = IPPROTO_TCP;
    ai->ai_addrlen = sizeof(sockaddr_in);
    ai->ai_addr = reinterpret_cast<sockaddr*>(sa);
    ai->ai_next = next;
    return ai;
}
}  // namespace

extern "C" int getaddrinfo(const char* node, const char* service, const struct addrinfo* hints,
                           struct addrinfo** res) {
    using Fn = int (*)(const char*, const char*, const addrinfo*, addrinfo**);
    static const Fn real = reinterpret_cast<Fn>(::dlsym(RTLD_NEXT, "getaddrinfo"));
    if (node != nullptr && std::strcmp(node, "dual.test") == 0 && g_dual_first_port.load() != 0) {
        addrinfo* second = loopbackAddrinfo(g_dual_second_port.load(), nullptr);
        addrinfo* first = second != nullptr ? loopbackAddrinfo(g_dual_first_port.load(), second) : nullptr;
        if (first == nullptr) {
            std::free(second);
            return EAI_MEMORY;
        }
        *res = first;
        return 0;
    }
    return real(node, service, hints, res);
}

extern "C" int connect(int fd, const struct sockaddr* addr, socklen_t len) {
    using Fn = int (*)(int, const sockaddr*, socklen_t);
    static const Fn real = reinterpret_cast<Fn>(::dlsym(RTLD_NEXT, "connect"));
    const int trace_port = g_connect_trace_port.load();
    if (trace_port != 0 && addr != nullptr && addr->sa_family == AF_INET &&
        ntohs(reinterpret_cast<const sockaddr_in*>(addr)->sin_port) == trace_port) {
        const std::int64_t now = hft::nowNs();
        std::lock_guard<std::mutex> lock(g_connect_trace_mutex);
        g_connect_trace_ns.push_back(now);
    }
    if (g_connect_park.exchange(false)) {
        g_connect_parked.store(true);
        while (!g_connect_release.load()) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return real(fd, addr, len);
}

namespace {

using mock_exchange::AcceptAction;
using mock_exchange::HttpReply;
using mock_exchange::HttpRequestInfo;
using mock_exchange::MockExchangeServer;
using mock_exchange::WsSession;
using mock_exchange::waitFor;

using Levels = std::vector<std::pair<std::string, std::string>>;  // [(price, size)]

constexpr const char* kOkxBooks5Sub = R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})";
constexpr const char* kOkxBooksSub = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
constexpr const char* kOkxBooksUnsub = R"({"op":"unsubscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
constexpr const char* kBybitSub = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";
constexpr const char* kBybitPing = R"({"op":"ping"})";
constexpr const char* kBinanceSnapshotPath = "/api/v3/depth?symbol=BTCUSDT&limit=1000";

double ms(std::int64_t ns) { return static_cast<double>(ns) / 1e6; }

bool contains(const std::string& s, const std::string& part) { return s.find(part) != std::string::npos; }

// ---------------------------------------------------------------------------
// 报文构造
// ---------------------------------------------------------------------------

std::string levelsJson(const Levels& levels, bool okx_style) {
    std::string s = "[";
    for (std::size_t i = 0; i < levels.size(); ++i) {
        if (i > 0) s += ",";
        s += "[\"" + levels[i].first + "\",\"" + levels[i].second + "\"";
        if (okx_style) s += ",\"0\",\"1\"";
        s += "]";
    }
    return s + "]";
}

// 十分位整数价格 -> 字符串（650001 -> "65000.1"），保证字符串精确
std::string tenths(int v) { return std::to_string(v / 10) + "." + std::to_string(v % 10); }

std::string binanceDiff(std::uint64_t first, std::uint64_t last, const Levels& bids, const Levels& asks,
                        std::int64_t event_ms = 1700000000000) {
    return R"({"e":"depthUpdate","E":)" + std::to_string(event_ms) + R"(,"s":"BTCUSDT","U":)" +
           std::to_string(first) + R"(,"u":)" + std::to_string(last) + R"(,"b":)" + levelsJson(bids, false) +
           R"(,"a":)" + levelsJson(asks, false) + "}";
}

// REST 深度快照与 depth20 部分深度推送的形状相同
std::string binanceDepth(std::uint64_t last_update_id, const Levels& bids, const Levels& asks) {
    return R"({"lastUpdateId":)" + std::to_string(last_update_id) + R"(,"bids":)" + levelsJson(bids, false) +
           R"(,"asks":)" + levelsJson(asks, false) + "}";
}

std::string okxBooks5(const Levels& bids, const Levels& asks, std::int64_t ts_ms) {
    return R"({"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"asks":)" + levelsJson(asks, true) +
           R"(,"bids":)" + levelsJson(bids, true) + R"(,"instId":"BTC-USDT","ts":")" + std::to_string(ts_ms) +
           R"("}]})";
}

// 简单的单档 books5 推送（买一 65000 + i，卖一 65001 + i）
std::string simpleBook(int i, std::int64_t ts_ms = 1700000000000) {
    return okxBooks5({{std::to_string(65000 + i) + ".0", "1.5"}}, {{std::to_string(65001 + i) + ".0", "2.5"}}, ts_ms);
}

std::string okxBooks(const char* action, const Levels& bids, const Levels& asks, std::int64_t prev_seq,
                     std::int64_t seq, std::int32_t checksum, std::int64_t ts_ms = 1700000000000) {
    return R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":")" + std::string(action) +
           R"(","data":[{"asks":)" + levelsJson(asks, true) + R"(,"bids":)" + levelsJson(bids, true) +
           R"(,"ts":")" + std::to_string(ts_ms) + R"(","checksum":)" + std::to_string(checksum) +
           R"(,"prevSeqId":)" + std::to_string(prev_seq) + R"(,"seqId":)" + std::to_string(seq) + "}]}";
}

// 测试自己的 OKX 订单簿模型：保存原始字符串，按文档规则（前 25 档交错拼接）计算校验和
struct OkxModel {
    std::map<double, std::pair<std::string, std::string>, std::greater<double>> bids;
    std::map<double, std::pair<std::string, std::string>> asks;

    void apply(const Levels& b, const Levels& a) {
        for (const auto& [px, sz] : b) {
            if (std::stod(sz) == 0.0) {
                bids.erase(std::stod(px));
            } else {
                bids[std::stod(px)] = {px, sz};
            }
        }
        for (const auto& [px, sz] : a) {
            if (std::stod(sz) == 0.0) {
                asks.erase(std::stod(px));
            } else {
                asks[std::stod(px)] = {px, sz};
            }
        }
    }

    std::int32_t checksum() const {
        std::string s;
        auto add = [&s](const std::pair<std::string, std::string>& l) {
            if (!s.empty()) s += ":";
            s += l.first + ":" + l.second;
        };
        auto b = bids.begin();
        auto a = asks.begin();
        for (int i = 0; i < 25; ++i) {
            if (b != bids.end()) add((b++)->second);
            if (a != asks.end()) add((a++)->second);
        }
        return hft::crc32Signed(s);
    }
};

std::string bybitBook(const char* type, const Levels& bids, const Levels& asks, std::uint64_t u, std::int64_t ts_ms) {
    return R"({"topic":"orderbook.50.BTCUSDT","type":")" + std::string(type) + R"(","ts":)" + std::to_string(ts_ms) +
           R"(,"data":{"s":"BTCUSDT","b":)" + levelsJson(bids, false) + R"(,"a":)" + levelsJson(asks, false) +
           R"(,"u":)" + std::to_string(u) + R"(,"seq":)" + std::to_string(u * 10) + R"(},"cts":)" +
           std::to_string(ts_ms) + "}";
}

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// feed 的日志事件（带时间戳）。必须声明在 FeedRunner 之前（回调在 feed 线程上调用）。
class EventLog {
public:
    struct Event {
        std::int64_t time_ns;
        std::string line;
    };

    std::function<void(const std::string&)> callback() {
        return [this](const std::string& line) {
            std::lock_guard<std::mutex> lock(mutex_);
            events_.push_back(Event{hft::nowNs(), line});
        };
    }

    std::vector<Event> events() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }

    bool any(const std::string& part) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const Event& e : events_) {
            if (e.line.find(part) != std::string::npos) return true;
        }
        return false;
    }

    // 事件中是否按顺序（不必相邻）出现这些子串；失败时打印全部事件
    bool inOrder(std::initializer_list<const char*> parts) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = parts.begin();
        for (const Event& e : events_) {
            if (it != parts.end() && e.line.find(*it) != std::string::npos) ++it;
        }
        if (it == parts.end()) return true;
        std::cerr << "    missing event \"" << *it << "\"; events were:" << std::endl;
        for (const Event& e : events_) std::cerr << "      " << e.line << std::endl;
        return false;
    }

    // 依次宣布的重连等待："reconnecting in N ms" -> N，"reconnecting now" -> 0
    std::vector<Event> reconnects(std::vector<int>& delays_ms) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<Event> out;
        delays_ms.clear();
        for (const Event& e : events_) {
            const std::size_t p = e.line.find("reconnecting ");
            if (p == std::string::npos) continue;
            const std::string rest = e.line.substr(p + 13);
            if (rest.rfind("now", 0) == 0) {
                delays_ms.push_back(0);
            } else if (rest.rfind("in ", 0) == 0) {
                delays_ms.push_back(std::stoi(rest.substr(3)));
            } else {
                continue;
            }
            out.push_back(e);
        }
        return out;
    }

private:
    mutable std::mutex mutex_;
    std::vector<Event> events_;
};

// 在独立线程上运行 feed；析构时 stop + join（必须声明在 MockExchangeServer 之后）。
// 日志回调与发布回调在线程启动之前设置，不会漏掉最早的事件。
struct FeedRunner {
    MarketDataFeed feed;
    std::thread thread;

    explicit FeedRunner(const FeedConfig& cfg, std::function<void(const std::string&)> log = {},
                        std::function<void(MarketDataFeed&)> on_publish = {})
        : feed(cfg) {
        if (log) feed.setLogCallback(std::move(log));
        if (on_publish) feed.setPublishCallback([this, cb = std::move(on_publish)] { cb(feed); });
        thread = std::thread([this] { feed.run(); });
    }
    ~FeedRunner() { stop(); }
    FeedRunner(const FeedRunner&) = delete;
    FeedRunner& operator=(const FeedRunner&) = delete;

    // 返回 stop() 到 run() 返回（join）所用的毫秒数
    double stop() {
        const std::int64_t t0 = hft::nowNs();
        feed.stop();
        if (thread.joinable()) thread.join();
        return ms(hft::nowNs() - t0);
    }
};

// 指向模拟服务器的基础配置：tls_verify + ca_file = 服务器证书；短退避；关闭应用层心跳与 stderr 输出
FeedConfig baseConfig(Exchange exchange, MockExchangeServer& server) {
    FeedConfig c;
    c.exchange = exchange;
    c.venue_id = 3;
    c.name = exchangeName(exchange);
    c.host = "127.0.0.1";
    c.port = server.portString();
    c.path = "/ws";
    c.symbol = "BTCUSDT";
    c.tls_verify = true;
    c.ca_file = server.caFile();
    c.stall_timeout_ms = 10000;
    c.connect_timeout_ms = 10000;
    c.reconnect_initial_ms = 20;
    c.reconnect_max_ms = 200;
    c.rest_min_interval_ms = 20;
    c.ping_interval_ms = 0;
    c.log_to_stderr = false;
    if (exchange == Exchange::OKX) c.subscribe_msg = kOkxBooks5Sub;
    if (exchange == Exchange::BYBIT) c.subscribe_msg = kBybitSub;
    return c;
}

// 消费者：把队列中的快照全部取出并保存（只在测试线程调用）
struct SnapshotLog {
    std::vector<BookSnapshot> all;
    std::vector<std::int64_t> pop_ns;

    std::size_t pump(MarketDataFeed& feed) {
        BookSnapshot s;
        std::size_t n = 0;
        while (feed.queue().tryPop(s)) {
            all.push_back(s);
            pop_ns.push_back(hft::nowNs());
            ++n;
        }
        return n;
    }

    std::vector<std::uint64_t> updateIds() const {
        std::vector<std::uint64_t> ids;
        for (const BookSnapshot& s : all) ids.push_back(s.update_id);
        return ids;
    }
};

std::string idsToString(const std::vector<std::uint64_t>& ids) {
    std::string s = "[";
    for (std::size_t i = 0; i < ids.size(); ++i) s += (i ? "," : "") + std::to_string(ids[i]);
    return s + "]";
}

// 记录对象存活期间发往 server 端口的每次 connect()（客户端时刻，按发起顺序）。测试里只有 feed 一个客户端、
// 服务器一直在监听，所以第 i 次 connect 就是服务器接受的第 i 个连接（HttpRequestInfo::connection）。
// REST 请求的间隔要按客户端发起连接的时刻量：feed 保证的是请求开始时刻的间隔，而服务器解析到请求的时刻还隔着
// 各自的 TCP 接受与 TLS 握手（负载下相差几十毫秒），服务器接受连接的时刻也带着接受线程的调度延迟。
class ConnectTrace {
public:
    explicit ConnectTrace(const MockExchangeServer& server) {
        std::lock_guard<std::mutex> lock(g_connect_trace_mutex);
        g_connect_trace_ns.clear();
        g_connect_trace_ns.reserve(256);
        g_connect_trace_port.store(server.port());
    }
    ~ConnectTrace() { g_connect_trace_port.store(0); }
    ConnectTrace(const ConnectTrace&) = delete;
    ConnectTrace& operator=(const ConnectTrace&) = delete;

    // 第 k 个与第 k - 1 个 REST 请求的连接发起时刻之差（毫秒）
    double requestGapMs(const std::vector<HttpRequestInfo>& reqs, std::size_t k) const {
        std::lock_guard<std::mutex> lock(g_connect_trace_mutex);
        REQUIRE(k >= 1 && k < reqs.size());
        REQUIRE(reqs[k].connection < g_connect_trace_ns.size());
        return ms(g_connect_trace_ns[reqs[k].connection] - g_connect_trace_ns[reqs[k - 1].connection]);
    }
};

// 状态观察者：后台线程每 100 µs 采样一次 status()，记录每次变化
class StatusWatcher {
public:
    explicit StatusWatcher(const MarketDataFeed& feed) : feed_(feed), thread_([this] { loop(); }) {}
    ~StatusWatcher() { stopWatching(); }

    // 停止采样；join 之后再采一次，保证调用者此刻看到的状态也被记录
    void stopWatching() {
        done_.store(true);
        if (thread_.joinable()) thread_.join();
        sample();
    }

    std::vector<FeedStatus> transitions() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return seen_;
    }

    // 采样序列中是否按顺序（不必相邻）出现 expected
    bool sawInOrder(std::initializer_list<FeedStatus> expected) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = expected.begin();
        for (FeedStatus s : seen_) {
            if (it != expected.end() && s == *it) ++it;
        }
        return it == expected.end();
    }

    std::string describe() const {
        std::string s;
        for (FeedStatus st : transitions()) s += std::string(s.empty() ? "" : " -> ") + feedStatusName(st);
        return s;
    }

private:
    void sample() {
        const FeedStatus s = feed_.status();
        std::lock_guard<std::mutex> lock(mutex_);
        if (seen_.empty() || seen_.back() != s) seen_.push_back(s);
    }

    void loop() {
        while (!done_.load()) {
            sample();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    const MarketDataFeed& feed_;
    mutable std::mutex mutex_;
    std::vector<FeedStatus> seen_;
    std::atomic<bool> done_{false};
    std::thread thread_;
};

// 在 127.0.0.1 上监听但从不 accept、且全连接队列已被占满的端口：新的 connect() 会停在 SYN_SENT
class BlackholeListener {
public:
    BlackholeListener() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ok_ = listen_fd_ >= 0 && ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
              ::listen(listen_fd_, 0) == 0;
        socklen_t len = sizeof(addr);
        ok_ = ok_ && ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0;
        port_ = ntohs(addr.sin_port);
        for (int i = 0; ok_ && i < 4; ++i) {  // 占满队列（backlog 0 时内核允许 1 个已完成连接）
            const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
            if (fd < 0) break;
            ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            fillers_.push_back(fd);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ~BlackholeListener() {
        for (int fd : fillers_) ::close(fd);
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }
    bool ok() const { return ok_; }
    unsigned short port() const { return port_; }
    std::string portString() const { return std::to_string(port_); }

private:
    int listen_fd_ = -1;
    unsigned short port_ = 0;
    bool ok_ = false;
    std::vector<int> fillers_;
};

// 在 127.0.0.1 上监听、从不 accept 的端口（队列足够长）：内核替它完成三次握手，connect() 立即成功
class OpenListener {
public:
    OpenListener() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(addr);
        ok_ = fd_ >= 0 && ::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
              ::listen(fd_, 64) == 0 && ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0;
        port_ = ntohs(addr.sin_port);
    }
    ~OpenListener() {
        if (fd_ >= 0) ::close(fd_);
    }
    OpenListener(const OpenListener&) = delete;
    OpenListener& operator=(const OpenListener&) = delete;
    bool ok() const { return ok_; }
    unsigned short port() const { return port_; }

private:
    int fd_ = -1;
    unsigned short port_ = 0;
    bool ok_ = false;
};

// 作用域内 "dual.test" 解析为 [127.0.0.1:first, 127.0.0.1:second]（见文件开头的 getaddrinfo 替换）
struct DualHost {
    DualHost(unsigned short first, unsigned short second) {
        g_dual_second_port.store(second);
        g_dual_first_port.store(first);
    }
    ~DualHost() {
        g_dual_first_port.store(0);
        g_dual_second_port.store(0);
    }
    DualHost(const DualHost&) = delete;
    DualHost& operator=(const DualHost&) = delete;
};

// 证书 SAN 同时包含 "dual.test" 的模拟服务器选项（tls_verify 照常开启）
mock_exchange::ServerOptions dualHostServerOptions() {
    mock_exchange::ServerOptions opt;
    opt.certificate.dns_names = {"localhost", "dual.test"};
    return opt;
}

// 脚本：之后只应答心跳——每收到一个 ping 文本帧回一个 pong（pong 为空时不回应），直到客户端断开
void answerPings(WsSession& ws, const std::string& ping, const std::string& pong) {
    std::size_t seen = 0;
    while (ws.clientConnected()) {
        ws.waitUntil([&] { return ws.received().size() > seen || !ws.clientConnected(); }, 50);
        while (seen < ws.received().size()) {
            if (ws.received()[seen] == ping && !pong.empty()) ws.send(pong);
            ++seen;
        }
    }
}

// frames 中等于 text 的帧的到达时刻
std::vector<std::int64_t> timesOf(const std::vector<std::string>& frames, const std::vector<std::int64_t>& times,
                                  const std::string& text) {
    std::vector<std::int64_t> out;
    for (std::size_t i = 0; i < frames.size() && i < times.size(); ++i) {
        if (frames[i] == text) out.push_back(times[i]);
    }
    return out;
}

// 主线程的刷新循环：duration_ms 内每 10 ms 调用一次 checkHealth（停顿看门狗照常工作）
void runHealthChecks(MarketDataFeed& feed, int duration_ms) {
    const std::int64_t t0 = hft::nowNs();
    while (ms(hft::nowNs() - t0) < duration_ms) {
        feed.checkHealth(hft::nowNs());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// 检查 feed 宣布的重连等待序列：第 i 次等待 ∈ [0.8, 1.2] x expected[i]（钳制到 max_ms；0 = 立即重连），
// 且第 i+1 个 TCP 连接确实在宣布之后至少等待了这么久才到达（每次尝试恰好产生一个连接）
void checkAnnouncedBackoff(const EventLog& events, const MockExchangeServer& server, std::vector<int> expected,
                           int max_ms) {
    std::vector<int> delays;
    const std::vector<EventLog::Event> announced = events.reconnects(delays);
    const std::vector<std::int64_t> accepts = server.acceptTimesNs();
    REQUIRE(delays.size() >= expected.size());
    REQUIRE(accepts.size() >= expected.size() + 1);
    for (std::size_t i = 0; i < expected.size(); ++i) {
        const int lo = static_cast<int>(expected[i] * 0.8);
        const int hi = std::min(max_ms, static_cast<int>(expected[i] * 1.2 + 0.999));
        CHECK_GE(delays[i], lo);
        CHECK_LE(delays[i], hi);
        const double waited = ms(accepts[i + 1] - announced[i].time_ns);
        CHECK_GE(waited, delays[i] - 1.0);
        CHECK_LT(waited, delays[i] + 500.0);
    }
}

}  // namespace

// ===========================================================================
// 构件单元测试
// ===========================================================================

TEST_CASE("feed: backoff delay is min(max, initial * 2^attempt) with +-20% jitter, clamped to [1, max]") {
    CHECK_EQ(MarketDataFeed::backoffDelayMs(0, 500, 30000, 0.0), 500);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(1, 500, 30000, 0.0), 1000);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(5, 500, 30000, 0.0), 16000);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(6, 500, 30000, 0.0), 30000);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(1000, 500, 30000, 0.0), 30000);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(0, 500, 30000, 1.0), 600);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(0, 500, 30000, -1.0), 400);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(2, 500, 30000, 0.5), 2200);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(10, 500, 30000, 1.0), 30000);   // 钳制到 max
    CHECK_EQ(MarketDataFeed::backoffDelayMs(10, 500, 30000, -1.0), 24000);
    CHECK_EQ(MarketDataFeed::backoffDelayMs(0, 0, 0, 0.0), 1);              // 非法参数也给出 >= 1 的值
    CHECK_EQ(MarketDataFeed::backoffDelayMs(3, 100, 50, 0.0), 100);         // max < initial 时按 initial
    CHECK_EQ(MarketDataFeed::backoffDelayMs(0, 500, 30000, 7.0), 600);      // 抖动参数钳制到 [-1, 1]
}

TEST_CASE("feed: feedStatusName covers every status") {
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Idle)), std::string("IDLE"));
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Connecting)), std::string("CONNECTING"));
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Syncing)), std::string("SYNCING"));
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Live)), std::string("LIVE"));
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Reconnecting)), std::string("RECONNECTING"));
    CHECK_EQ(std::string(feedStatusName(FeedStatus::Stopped)), std::string("STOPPED"));
}

TEST_CASE("registry: kick shuts down registered sockets, add after kick fails, rearm and close semantics") {
    hft::net::SocketRegistry reg;
    int sv[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    const hft::net::SocketRegistry::Token t = reg.add(sv[0]);
    REQUIRE(t != 0);
    CHECK_EQ(reg.size(), std::size_t{1});

    // 阻塞在 recv 中的线程被踢断后立即返回 0（EOF）
    std::atomic<long> got{-2};
    std::thread reader([&] {
        char c;
        got.store(::recv(sv[0], &c, 1, 0));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const std::int64_t t0 = hft::nowNs();
    CHECK(reg.kick(hft::net::KickReason::Stall));
    reader.join();
    CHECK_EQ(got.load(), 0L);
    CHECK_LT(ms(hft::nowNs() - t0), 500.0);
    CHECK(reg.interrupted());
    CHECK_EQ(static_cast<int>(reg.reason()), static_cast<int>(hft::net::KickReason::Stall));
    CHECK(!reg.kick(hft::net::KickReason::Stall));  // 同一会话不重复踢

    // 踢断状态下新登记失败，且该套接字被立即 shutdown
    int sv2[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv2) == 0);
    CHECK_EQ(reg.add(sv2[0]), hft::net::SocketRegistry::Token{0});
    char c = 0;
    CHECK_EQ(::recv(sv2[0], &c, 1, 0), static_cast<ssize_t>(0));

    // rearm 之后恢复正常
    reg.remove(t);
    CHECK_EQ(reg.size(), std::size_t{0});
    reg.rearm();
    CHECK(!reg.interrupted());
    int sv3[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv3) == 0);
    const hft::net::SocketRegistry::Token t3 = reg.add(sv3[0]);
    CHECK(t3 != 0);

    // close：永久停止，原因为 Stop，rearm 无效
    reg.close();
    CHECK_EQ(static_cast<int>(reg.reason()), static_cast<int>(hft::net::KickReason::Stop));
    CHECK_EQ(::recv(sv3[0], &c, 1, 0), static_cast<ssize_t>(0));
    reg.rearm();
    CHECK(reg.interrupted());
    int sv4[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv4) == 0);
    CHECK_EQ(reg.add(sv4[0]), hft::net::SocketRegistry::Token{0});
    reg.remove(t3);
    for (int fd : {sv[0], sv[1], sv2[0], sv2[1], sv3[0], sv3[1], sv4[0], sv4[1]}) ::close(fd);
}

TEST_CASE("registry: kicking after the original fd was closed and its number reused never touches the new socket") {
    // Beast 在收到 close 帧时会在 teardown 中自行关闭套接字；登记表持有 fd 副本，fd 号被复用也不会误踢
    hft::net::SocketRegistry reg;
    int a[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, a) == 0);
    const int old_fd = a[0];
    const hft::net::SocketRegistry::Token t = reg.add(old_fd);
    REQUIRE(t != 0);
    ::close(old_fd);  // 原 fd 被"别人"关闭
    int b[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, b) == 0);
    const int reused = b[0] == old_fd ? b[0] : (b[1] == old_fd ? b[1] : -1);
    CHECK(reused >= 0);  // 内核总是分配最小的空闲 fd 号
    CHECK(reg.kick(hft::net::KickReason::Stall));
    // 新套接字不受影响：仍可双向收发
    CHECK_EQ(::send(b[0], "x", 1, MSG_NOSIGNAL), static_cast<ssize_t>(1));
    char c = 0;
    CHECK_EQ(::recv(b[1], &c, 1, 0), static_cast<ssize_t>(1));
    CHECK_EQ(c, 'x');
    // 被踢的是旧套接字：对端读到 EOF
    CHECK_EQ(::recv(a[1], &c, 1, 0), static_cast<ssize_t>(0));
    reg.remove(t);
    ::close(a[1]);
    ::close(b[0]);
    ::close(b[1]);
}

TEST_CASE("registry: a blocking TCP connect stuck in SYN_SENT is aborted by kick") {
    BlackholeListener hole;
    REQUIRE(hole.ok());
    hft::net::SocketRegistry reg;
    boost::asio::io_context ioc;
    hft::net::tcp::socket sock(ioc);
    hft::net::SocketRegistration registration;
    hft::net::Resolver resolver;
    hft::net::tcp::resolver::results_type endpoints;
    REQUIRE(!resolver.resolve("127.0.0.1", hole.portString(), endpoints, [] { return false; }));
    boost::system::error_code ec;
    std::atomic<bool> done{false};
    std::thread t([&] {
        ec = hft::net::connectSocket(sock, endpoints, reg, registration);
        done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    CHECK(!done.load());  // 仍在等待连接完成（没有时限）
    const std::int64_t t0 = hft::nowNs();
    reg.kick(hft::net::KickReason::ConnectTimeout);
    t.join();
    CHECK_LT(ms(hft::nowNs() - t0), 500.0);
    CHECK(ec == boost::asio::error::operation_aborted);
    CHECK(!sock.is_open());
    CHECK(!registration.attached());
    CHECK_EQ(reg.size(), std::size_t{0});
}

TEST_CASE("net: a kick landing between socket registration and connect() still aborts the connect (200 trials)") {
    // Linux 上对尚未发起连接的套接字 shutdown 是空操作：踢断若只靠 shutdown，落在这个窗口里就会丢失，
    // connect 随后在 SYN_SENT 里一直等到内核放弃（约 2 分钟）。每次试验在登记出现的瞬间踢断（close / kick 交替）。
    BlackholeListener hole;
    REQUIRE(hole.ok());
    hft::net::Resolver resolver;
    hft::net::tcp::resolver::results_type endpoints;
    REQUIRE(!resolver.resolve("127.0.0.1", hole.portString(), endpoints, [] { return false; }));
    int lost = 0;
    int aborted = 0;
    double worst_ms = 0.0;
    for (int trial = 0; trial < 200; ++trial) {
        hft::net::SocketRegistry reg;
        boost::asio::io_context ioc;
        hft::net::tcp::socket sock(ioc);
        hft::net::SocketRegistration registration;
        boost::system::error_code ec;
        std::atomic<bool> done{false};
        std::thread worker([&] {
            ec = hft::net::connectSocket(sock, endpoints, reg, registration);
            done.store(true);
        });
        while (reg.size() == 0) {
        }
        const std::int64_t t0 = hft::nowNs();
        if (trial % 2 == 0) {
            reg.close();
        } else {
            reg.kick(hft::net::KickReason::ConnectTimeout);
        }
        if (!waitFor([&] { return done.load(); }, 250)) {
            ++lost;
            reg.close();  // 救援：套接字此时已在 SYN_SENT，再 shutdown 一次就能让它返回
        }
        worker.join();
        worst_ms = std::max(worst_ms, ms(hft::nowNs() - t0));
        if (ec == boost::asio::error::operation_aborted) ++aborted;
        CHECK(!registration.attached());
    }
    CHECK_EQ(lost, 0);
    CHECK_EQ(aborted, 200);
    std::printf("    200 kicks at registration time: %d lost, worst return %.1f ms\n", lost, worst_ms);
}

TEST_CASE("net: connectSocket gives every address its own deadline, falls through a blackhole and rotates its cursor") {
    BlackholeListener hole;
    OpenListener good;
    REQUIRE(hole.ok());
    REQUIRE(good.ok());
    const DualHost dual(hole.port(), good.port());
    hft::net::Resolver resolver;
    hft::net::tcp::resolver::results_type endpoints;
    REQUIRE(!resolver.resolve("dual.test", "443", endpoints, [] { return false; }));
    REQUIRE_EQ(endpoints.size(), std::size_t{2});
    hft::net::SocketRegistry reg;
    boost::asio::io_context ioc;
    auto attempt = [&](int timeout_ms, std::size_t& cursor, boost::asio::ip::tcp::endpoint& connected, int& waits,
                       double& took_ms) {
        hft::net::tcp::socket sock(ioc);
        hft::net::SocketRegistration registration;
        hft::net::ConnectOptions opt;
        opt.timeout_ms = timeout_ms;
        opt.cursor = &cursor;
        waits = 0;
        opt.on_wait = [&waits] { ++waits; };
        const std::int64_t t0 = hft::nowNs();
        const boost::system::error_code ec = hft::net::connectSocket(sock, endpoints, reg, registration, &connected, opt);
        took_ms = ms(hft::nowNs() - t0);
        CHECK_EQ(registration.attached(), !ec);
        CHECK_EQ(sock.is_open(), !ec);
        hft::net::closeSocket(sock, registration);
        return ec;
    };
    boost::asio::ip::tcp::endpoint connected;
    int waits = 0;
    double took = 0.0;
    // [黑洞, 好]：第一个地址用满自己的时限 max(1000, 1500 / 2) = 1000 ms，然后连上第二个；游标指向连上的地址
    std::size_t cursor = 0;
    boost::system::error_code ec = attempt(1500, cursor, connected, waits, took);
    CHECK(!ec);
    CHECK_EQ(connected.port(), good.port());
    CHECK_EQ(cursor, std::size_t{1});
    CHECK_GE(took, 990.0);
    CHECK_LT(took, 1400.0);
    CHECK_GE(waits, 10);  // 等待期间每个空闲的轮询片（<= 50 ms）回调一次
    // 游标 = 1：直接从好地址开始，立即连上，游标不变
    ec = attempt(1500, cursor, connected, waits, took);
    CHECK(!ec);
    CHECK_EQ(cursor, std::size_t{1});
    CHECK_LT(took, 200.0);
    // 总时限比单个地址的下限还短：第一个地址用完全部时限，返回 timed_out，游标指向下一个（没来得及试的）地址
    cursor = 0;
    ec = attempt(300, cursor, connected, waits, took);
    CHECK(ec == boost::asio::error::timed_out);
    CHECK_EQ(cursor, std::size_t{1});
    CHECK_GE(took, 290.0);
    CHECK_LT(took, 700.0);
    // 没有时限：游标从 2 % 2 = 0 开始也一样，被踢断时及时返回 operation_aborted，游标指向下一个地址
    cursor = 2;
    std::thread kicker([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        reg.kick(hft::net::KickReason::ConnectTimeout);
    });
    ec = attempt(0, cursor, connected, waits, took);
    kicker.join();
    CHECK(ec == boost::asio::error::operation_aborted);
    CHECK_EQ(cursor, std::size_t{1});
    CHECK_LT(took, 400.0);
    CHECK_EQ(reg.size(), std::size_t{0});
}

TEST_CASE("mock: generated certificate verifies for 127.0.0.1 and localhost; the HTTPS helper reports status, body and limits") {
    MockExchangeServer server;
    server.setHttpHandler([](const HttpRequestInfo& info) {
        if (info.target == "/big") return HttpReply{200, std::string(4096, 'x'), "text/plain", 0};
        if (info.target == "/missing") return HttpReply{404, R"({"code":-1121,"msg":"Invalid symbol."})", "application/json", 0};
        return HttpReply{200, "hello " + info.target, "text/plain", 0};
    });
    auto ctx = hft::net::makeClientContext(hft::net::TlsOptions{true, server.caFile()});
    boost::asio::io_context ioc;
    hft::net::Resolver resolver;
    hft::net::SocketRegistry reg;
    hft::net::HttpsGetRequest rq;
    rq.port = server.portString();
    for (const char* host : {"127.0.0.1", "localhost"}) {
        rq.host = host;
        rq.target = "/a?b=1";
        const hft::net::HttpResult r = hft::net::httpsGet(ioc, *ctx, resolver, reg, rq);
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, std::string("hello /a?b=1"));
        CHECK_EQ(r.error, std::string());
    }
    rq.host = "127.0.0.1";
    rq.target = "/missing";
    hft::net::HttpResult r = hft::net::httpsGet(ioc, *ctx, resolver, reg, rq);
    CHECK(r.ok);
    CHECK_EQ(r.status, 404);
    CHECK(contains(r.body, "-1121"));
    rq.target = "/big";
    rq.body_limit = 1024;
    r = hft::net::httpsGet(ioc, *ctx, resolver, reg, rq);
    CHECK(!r.ok);
    CHECK(contains(r.error, "exceeds 1024 bytes"));
    CHECK_EQ(reg.size(), std::size_t{0});  // 每次请求结束都已注销

    const std::vector<HttpRequestInfo> reqs = server.httpRequests();
    REQUIRE_EQ(reqs.size(), std::size_t{4});
    CHECK_EQ(reqs[0].method, std::string("GET"));
    CHECK_EQ(reqs[0].user_agent, std::string("crypto_hft_engine/2.0"));
    CHECK_EQ(reqs[0].host, "127.0.0.1:" + server.portString());
    CHECK(server.errors().empty());

    // 不信任该证书的客户端：清晰的校验错误
    auto system_only = hft::net::makeClientContext(hft::net::TlsOptions{true, ""});
    rq.target = "/a";
    r = hft::net::httpsGet(ioc, *system_only, resolver, reg, rq);
    CHECK(!r.ok);
    CHECK(contains(r.error, "certificate verification failed"));
    // 不存在的 CA 文件：makeClientContext 抛出带文件名的错误
    bool threw = false;
    try {
        hft::net::makeClientContext(hft::net::TlsOptions{true, "/nonexistent/ca.pem"});
    } catch (const std::exception& e) {
        threw = contains(e.what(), "/nonexistent/ca.pem");
    }
    CHECK(threw);
}

// ===========================================================================
// 协议端到端
// ===========================================================================

TEST_CASE("feed: Binance diff depth syncs end to end (too-old REST snapshot, refetch, buffered events, gap resync)") {
    MockExchangeServer server;
    // 第 1 次 GET：快照太旧（L=90 < 第一条缓冲事件的 U=101）；第 2 次：L=105；缺口重同步后第 3 次：L=145
    server.setHttpHandler([](const HttpRequestInfo& info) {
        if (info.target != kBinanceSnapshotPath) return HttpReply{404, "{}", "application/json", 0};
        if (info.index == 0) return HttpReply{200, binanceDepth(90, {{"64000.00", "1.0"}}, {{"66000.00", "1.0"}})};
        if (info.index == 1) {
            return HttpReply{200, binanceDepth(105, {{"65000.00", "1.0"}, {"64999.00", "2.0"}},
                                               {{"65001.00", "1.5"}, {"65002.00", "3.0"}})};
        }
        return HttpReply{200, binanceDepth(145, {{"65100.00", "1.0"}}, {{"65101.00", "1.0"}})};
    });
    server.setWebSocketScript([](WsSession& ws) {
        ws.send(binanceDiff(101, 110, {{"65000.00", "1.5"}}, {{"65001.00", "0.00000000"}}));
        // 下面两条在客户端拉取 REST 快照期间留在内核缓冲区里
        ws.send(binanceDiff(111, 120, {{"64998.00", "4.0"}}, {}));
        ws.send(binanceDiff(121, 130, {{"64999.00", "0"}}, {{"65003.00", "1.0"}}));
        ws.waitUntil([&ws] { return ws.server().httpRequestCount() >= 2; }, 5000);
        ws.sleepMs(50);
        ws.send(binanceDiff(141, 150, {{"65100.00", "2.0"}}, {}));  // 缺口：本地 130，U = 141
        ws.waitUntil([&ws] { return ws.server().httpRequestCount() >= 3; }, 5000);
        ws.send(binanceDiff(151, 160, {}, {{"65102.00", "5.0"}}));
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth@100ms";
    cfg.rest_host = "127.0.0.1";
    cfg.rest_port = server.portString();
    cfg.snapshot_path = kBinanceSnapshotPath;
    const ConnectTrace trace(server);
    FeedRunner runner(cfg);
    SnapshotLog log;
    const bool done = waitFor(
        [&] {
            log.pump(runner.feed);
            return !log.all.empty() && log.all.back().update_id == 160;
        },
        8000);
    REQUIRE(done);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK_EQ(idsToString(log.updateIds()), std::string("[110,120,130,0,150,160]"));
    for (std::size_t i = 0; i < log.all.size(); ++i) {
        CHECK_EQ(log.all[i].seq, static_cast<std::uint64_t>(i + 1));
        CHECK_EQ(log.all[i].venue, 3u);
    }
    // REST 快照 L=105 + 回放 e1(101..110)：买 65000.00 -> 1.5，卖 65001.00 删除
    const BookSnapshot& first = log.all[0];
    REQUIRE_EQ(first.bid_count, 2u);
    REQUIRE_EQ(first.ask_count, 1u);
    CHECK_EQ(first.bids[0].price, 65000.0);
    CHECK_EQ(first.bids[0].quantity, 1.5);
    CHECK_EQ(first.asks[0].price, 65002.0);
    // e3 之后：买 [65000.00 x1.5, 64998.00 x4]，卖 [65002 x3, 65003 x1]
    const BookSnapshot& third = log.all[2];
    REQUIRE_EQ(third.bid_count, 2u);
    REQUIRE_EQ(third.ask_count, 2u);
    CHECK_EQ(third.bids[1].price, 64998.0);
    CHECK_EQ(third.bids[1].quantity, 4.0);
    CHECK_EQ(third.asks[1].price, 65003.0);
    CHECK_EQ(log.all[3].bid_count + log.all[3].ask_count, 0u);  // 缺口：先发布空簿
    const BookSnapshot& last = log.all.back();
    REQUIRE_EQ(last.bid_count, 1u);
    REQUIRE_EQ(last.ask_count, 2u);
    CHECK_EQ(last.bids[0].quantity, 2.0);
    CHECK_EQ(last.asks[1].price, 65102.0);
    CHECK_EQ(last.exchange_ts_ms, std::int64_t{1700000000000});

    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 不应再有多余的 GET
    const std::vector<HttpRequestInfo> reqs = server.httpRequests();
    REQUIRE_EQ(reqs.size(), std::size_t{3});
    for (const HttpRequestInfo& r : reqs) {
        CHECK_EQ(r.target, std::string(kBinanceSnapshotPath));
        CHECK_EQ(r.user_agent, std::string("crypto_hft_engine/2.0"));
    }
    // 两次快照请求（开始时刻）之间至少相隔 rest_min_interval_ms（20 ms，-20% 抖动下至少 16 ms）
    CHECK_GE(trace.requestGapMs(reqs, 1), 15.0);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.messages, 5u);
    CHECK_EQ(s.rest_snapshots, 3u);
    CHECK_EQ(s.gaps, 1u);
    CHECK_EQ(s.resyncs, 1u);
    CHECK_EQ(s.parse_errors, 0u);
    CHECK(waitFor([&] { return runner.feed.stats().published == 6; }, 2000));  // 计数在 tryPush 之后才更新
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(s.connect_failures, 0u);
    CHECK(contains(runner.feed.lastError(), "gap"));
    CHECK_EQ(server.wsTargets(), std::vector<std::string>{"/ws/btcusdt@depth@100ms"});
    CHECK(server.errors().empty());
}

TEST_CASE("feed: OKX books with checksums; seq gap and checksum mismatch each resubscribe (unsubscribe, subscribe)") {
    MockExchangeServer server;
    // 三本簿：每本 30 档，价格用十分位整数生成，字符串精确
    auto makeBook = [](int base_tenths) {
        Levels bids, asks;
        for (int i = 0; i < 30; ++i) {
            bids.push_back({tenths(base_tenths - i), std::to_string(1 + i % 7) + "." + std::to_string(i % 10)});
            asks.push_back({tenths(base_tenths + 1 + i), std::to_string(2 + i % 5) + ".25"});
        }
        return std::make_pair(bids, asks);
    };
    auto expected = std::make_shared<OkxModel>();  // 脚本线程写；测试线程在 server.stop()（join）之后读
    // 脚本只按值捕获：测试中途失败时局部变量先于服务器析构，脚本线程不能引用它们
    server.setWebSocketScript([makeBook, expected](WsSession& ws) {
        ws.expectText(kOkxBooksSub);
        OkxModel m;
        auto [b1, a1] = makeBook(650000);
        m.apply(b1, a1);
        ws.send(okxBooks("snapshot", b1, a1, -1, 1000, m.checksum()));
        Levels u1b{{tenths(650000), "9.9"}, {tenths(649995), "0"}}, u1a{{tenths(650003), "0"}, {"65000.25", "1.0"}};
        m.apply(u1b, u1a);
        ws.send(okxBooks("update", u1b, u1a, 1000, 1001, m.checksum()));
        Levels u2b{{"64999.95", "3.5"}}, u2a{};
        m.apply(u2b, u2a);
        ws.send(okxBooks("update", u2b, u2a, 1001, 1002, m.checksum()));
        ws.send(okxBooks("update", {{tenths(649990), "1.0"}}, {}, 1005, 1006, 0));  // 缺口
        ws.expectText(kOkxBooksUnsub);
        ws.expectText(kOkxBooksSub);
        OkxModel m2;
        auto [b2, a2] = makeBook(651000);
        m2.apply(b2, a2);
        ws.send(okxBooks("snapshot", b2, a2, -1, 2000, m2.checksum()));
        Levels bad_b{{tenths(651000), "7.7"}};
        m2.apply(bad_b, {});
        ws.send(okxBooks("update", bad_b, {}, 2000, 2001, m2.checksum() ^ 0x1234));  // 校验和错误
        ws.expectText(kOkxBooksUnsub);
        ws.expectText(kOkxBooksSub);
        OkxModel m3;
        auto [b3, a3] = makeBook(652000);
        m3.apply(b3, a3);
        ws.send(okxBooks("snapshot", b3, a3, -1, 3000, m3.checksum()));
        Levels u5b{{tenths(652000), "0"}}, u5a{{tenths(652001), "0.5"}};
        m3.apply(u5b, u5a);
        *expected = m3;  // 先写模型再发最后一条：测试看到 3001 时模型已就绪（之后还会 join）
        ws.send(okxBooks("update", u5b, u5a, 3000, 3001, m3.checksum()));
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.subscribe_msg = kOkxBooksSub;
    FeedRunner runner(cfg);
    SnapshotLog log;
    REQUIRE(waitFor(
        [&] {
            log.pump(runner.feed);
            return !log.all.empty() && log.all.back().update_id == 3001;
        },
        8000));
    CHECK_EQ(idsToString(log.updateIds()), std::string("[1000,1001,1002,0,2000,0,3000,3001]"));
    CHECK_EQ(server.framesReceived(0), (std::vector<std::string>{kOkxBooksSub, kOkxBooksUnsub, kOkxBooksSub,
                                                                  kOkxBooksUnsub, kOkxBooksSub}));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.gaps, 1u);
    CHECK_EQ(s.checksum_failures, 1u);
    CHECK_EQ(s.resyncs, 2u);
    CHECK_EQ(s.parse_errors, 0u);
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(s.messages, 8u);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK(contains(runner.feed.lastError(), "checksum mismatch"));
    runner.stop();
    server.stop();  // 脚本线程结束：expected 可以安全读取
    CHECK(server.errors().empty());
    // 最终发布的前 20 档与测试模型一致
    const BookSnapshot& last = log.all.back();
    REQUIRE_EQ(last.bid_count, 20u);
    REQUIRE_EQ(last.ask_count, 20u);
    auto bit = expected->bids.begin();
    auto ait = expected->asks.begin();
    for (int i = 0; i < 20; ++i, ++bit, ++ait) {
        CHECK_EQ(last.bids[i].price, bit->first);
        CHECK_EQ(last.bids[i].quantity, std::stod(bit->second.second));
        CHECK_EQ(last.asks[i].price, ait->first);
        CHECK_EQ(last.asks[i].quantity, std::stod(ait->second.second));
    }
}

TEST_CASE("feed: Bybit snapshot and deltas; periodic {\"op\":\"ping\"} frames at the configured interval") {
    MockExchangeServer server;
    auto deltas_sent = std::make_shared<std::atomic<int>>(0);
    auto pongs_sent = std::make_shared<std::atomic<int>>(0);
    const std::int64_t base_ts = hft::wallClockMs();
    server.setWebSocketScript([deltas_sent, pongs_sent, base_ts](WsSession& ws) {
        ws.expectText(kBybitSub);
        ws.send(R"({"success":true,"ret_msg":"subscribe","conn_id":"c1","op":"subscribe"})");
        ws.send(bybitBook("snapshot", {{"65000.0", "1"}, {"64999.5", "2"}}, {{"65000.5", "3"}}, 100, base_ts));
        int i = 0;
        std::size_t pongs = 0;
        // 每 10 ms 一条 delta，直到收到 3 个 ping（最多 3 s）；每个 ping 回一个 pong（适配器忽略 pong）
        while (ws.countReceived(kBybitPing) < 3 && i < 300) {
            ++i;
            ws.sleepMs(10);
            ws.send(bybitBook("delta", {{"65000.0", std::to_string(i + 1)}}, {{"65000.5", std::to_string(i + 2)}},
                              100 + static_cast<std::uint64_t>(i), base_ts + i));
            if (pongs < ws.countReceived(kBybitPing)) {
                ++pongs;
                ws.send(R"({"op":"pong","args":["1700000000000"],"conn_id":"c1"})");
            }
        }
        pongs_sent->store(static_cast<int>(pongs));
        deltas_sent->store(i);
    });
    FeedConfig cfg = baseConfig(Exchange::BYBIT, server);
    cfg.ping_interval_ms = 40;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    SnapshotLog log;
    REQUIRE(waitFor(
        [&] {
            log.pump(runner.feed);
            return deltas_sent->load() > 0 && !log.all.empty() &&
                   log.all.back().update_id == 100 + static_cast<std::uint64_t>(deltas_sent->load());
        },
        8000));
    const int n = deltas_sent->load();
    const std::vector<std::string> frames = server.framesReceived(0);
    const std::vector<std::int64_t> times = server.frameTimesNs(0);
    const std::int64_t window_end_ns = hft::nowNs();  // 上面这些 ping 都在此刻之前发出
    REQUIRE(!frames.empty());
    REQUIRE_EQ(times.size(), frames.size());
    CHECK_EQ(frames[0], std::string(kBybitSub));
    std::size_t pings = 0;
    for (std::size_t i = 1; i < frames.size(); ++i) {
        CHECK_EQ(frames[i], std::string(kBybitPing));
        pings += frames[i] == kBybitPing ? 1 : 0;
        CHECK_GE(times[i], times[i - 1]);
    }
    CHECK_GE(pings, std::size_t{3});
    CHECK_GE(n, 3);  // 数据一直在来（每 10 ms 一条）：心跳由周期规则（每 40 ms）发出，不依赖空闲
    // 心跳不会比间隔更频繁：会话开始（"connected" 日志）之后的窗口内最多 window / 40 ms 个
    std::int64_t connected_ns = 0;
    for (const EventLog::Event& e : events.events()) {
        if (connected_ns == 0 && contains(e.line, "[Bybit] connected to")) connected_ns = e.time_ns;
    }
    REQUIRE(connected_ns != 0);
    CHECK_LE(static_cast<double>(pings), ms(window_end_ns - connected_ns) / 40.0 + 1.0);
    const BookSnapshot& last = log.all.back();
    REQUIRE_EQ(last.bid_count, 2u);
    REQUIRE_EQ(last.ask_count, 1u);
    CHECK_EQ(last.bids[0].quantity, static_cast<double>(n + 1));
    CHECK_EQ(last.asks[0].quantity, static_cast<double>(n + 2));
    CHECK_EQ(last.exchange_ts_ms, base_ts + n);
    CHECK_EQ(log.all.front().update_id, 100u);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.gaps, 0u);
    CHECK_EQ(s.parse_errors, 0u);
    CHECK(waitFor([&] { return runner.feed.stats().published == static_cast<std::uint64_t>(n + 1); }, 2000));
    // 消息数 = 订阅回执 + 快照 + n 个 delta + pong（最后一个 pong 可能还在路上）
    const std::uint64_t expected_messages = static_cast<std::uint64_t>(n + 2 + pongs_sent->load());
    CHECK(waitFor([&] { return runner.feed.stats().messages == expected_messages; }, 2000));
    CHECK_EQ(runner.feed.getExchangeLatency().count(), static_cast<std::uint64_t>(n + 1));  // 带 ts 的消息
    CHECK(server.errors().empty());
}

// ---------------------------------------------------------------------------
// 心跳：安静的连接也按空闲时间发心跳；回应刷新停顿看门狗；没有回应时看门狗照常踢断
// ---------------------------------------------------------------------------

// 一个快照之后只有心跳往来的会话：idle_ping = min(适配器默认 20000, stall 600 / 2) = 300 ms，
// 运行 5 个空闲间隔：心跳按空闲时间发出、回应让看门狗不动手
void checkIdleAppKeepalive(Exchange exchange, const std::string& ping, const std::string& pong,
                           const std::function<void(WsSession&)>& start) {
    MockExchangeServer server;
    server.setWebSocketScript([start, ping, pong](WsSession& ws) {
        start(ws);
        answerPings(ws, ping, pong);
    });
    FeedConfig cfg = baseConfig(exchange, server);
    cfg.ping_interval_ms = -1;  // 适配器默认间隔（20 s）
    cfg.stall_timeout_ms = 600;
    FeedRunner runner(cfg);
    REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Live; }, 5000));
    runHealthChecks(runner.feed, 1500);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.stalls, 0u);
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK_GE(s.pings, 3u);
    CHECK_EQ(s.pongs, 0u);  // 应用层 pong 是普通消息
    const std::vector<std::string> frames = server.framesReceived(0);
    const std::vector<std::int64_t> pings = timesOf(frames, server.frameTimesNs(0), ping);
    REQUIRE(pings.size() >= 3);
    REQUIRE(!frames.empty());
    // 第一个心跳在快照之后空闲约 300 ms 才发出；之后每个间隔都 >= 300 ms（回应刷新了空闲计时）
    CHECK_GE(ms(pings[0] - server.frameTimesNs(0)[0]), 280.0);
    for (std::size_t i = 1; i < pings.size(); ++i) CHECK_GE(ms(pings[i] - pings[i - 1]), 280.0);
    CHECK_LE(pings.size(), std::size_t{6});
    CHECK(waitFor([&] { return runner.feed.stats().messages >= 1 + pings.size(); }, 1000));  // 快照 + 每个 pong
    CHECK(server.errors().empty());
}

TEST_CASE("feed: an idle OKX books5 session pings on idle time (\"ping\" -> \"pong\"): no stall, no reconnect") {
    checkIdleAppKeepalive(Exchange::OKX, "ping", "pong", [](WsSession& ws) {
        ws.expectText(kOkxBooks5Sub);
        ws.send(simpleBook(0));  // 之后市场安静：books5 只在前 5 档变化时推送
    });
}

TEST_CASE("feed: an idle Bybit session pings on idle time ({\"op\":\"ping\"} -> pong): no stall, no reconnect") {
    checkIdleAppKeepalive(Exchange::BYBIT, kBybitPing, R"({"op":"pong","args":["1700000000000"],"conn_id":"c1"})",
                          [](WsSession& ws) {
                              ws.expectText(kBybitSub);
                              ws.send(bybitBook("snapshot", {{"65000.0", "1"}}, {{"65000.5", "2"}}, 100, 1700000000000));
                          });
}

TEST_CASE("feed: a silent Binance stream gets WebSocket ping frames; the server's pongs keep it up") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) {
        ws.send(binanceDepth(1000, {{"65000.00", "1.0"}}, {{"65000.01", "0.5"}}));  // 之后静默（Beast 自动应答 ping）
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth20@100ms";
    cfg.ping_interval_ms = -1;  // Binance 没有应用层心跳：空闲 stall / 2 = 300 ms 发 WebSocket ping
    cfg.stall_timeout_ms = 600;
    FeedRunner runner(cfg);
    REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Live; }, 5000));
    runHealthChecks(runner.feed, 1500);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.stalls, 0u);
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK_GE(s.pings, 3u);
    CHECK_GE(s.pongs + 1, s.pings);  // 最后一个 pong 可能还在路上
    CHECK_EQ(s.messages, 1u);        // 控制帧不算消息
    CHECK(server.framesReceived(0).empty());  // 没有发任何文本帧
    const std::vector<std::int64_t> pings = server.pingTimesNs(0);
    CHECK_GE(pings.size(), std::size_t{3});
    for (std::size_t i = 1; i < pings.size(); ++i) CHECK_GE(ms(pings[i] - pings[i - 1]), 280.0);
    CHECK(server.errors().empty());
}

TEST_CASE("feed: a server that never answers pings is still kicked by the stall watchdog (app-level and WebSocket)") {
    {  // OKX：收到 "ping" 但从不回 "pong"
        MockExchangeServer server;
        server.setWebSocketScripts({
            [](WsSession& ws) {
                ws.expectText(kOkxBooks5Sub);
                ws.send(simpleBook(0));
                answerPings(ws, "ping", std::string());
            },
            [](WsSession& ws) { ws.send(simpleBook(1)); },
        });
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.ping_interval_ms = -1;
        cfg.stall_timeout_ms = 600;
        FeedRunner runner(cfg);
        REQUIRE(waitFor(
            [&] {
                runner.feed.checkHealth(hft::nowNs());
                return runner.feed.stats().stalls >= 1 && server.wsSessionCount() >= 2;
            },
            5000, 10));
        CHECK(contains(runner.feed.lastError(), "stalled: no data for more than 600 ms during read"));
        const std::vector<std::string> frames = server.framesReceived(0);
        CHECK_GE(std::count(frames.begin(), frames.end(), std::string("ping")), 1);  // 心跳确实发了
    }
    {  // Binance：服务器卡住（不运行 io_context，不应答 WebSocket ping）
        MockExchangeServer server;
        server.setWebSocketScripts({
            [](WsSession& ws) {
                ws.send(binanceDepth(1000, {{"65000.00", "1.0"}}, {{"65000.01", "0.5"}}));
                ws.freezeMs(3000);
            },
            [](WsSession& ws) { ws.send(binanceDepth(1001, {{"65000.00", "1.0"}}, {{"65000.01", "0.5"}})); },
        });
        FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
        cfg.path = "/ws/btcusdt@depth20@100ms";
        cfg.ping_interval_ms = -1;
        cfg.stall_timeout_ms = 600;
        FeedRunner runner(cfg);
        REQUIRE(waitFor(
            [&] {
                runner.feed.checkHealth(hft::nowNs());
                return runner.feed.stats().stalls >= 1 && server.wsSessionCount() >= 2;
            },
            5000, 10));
        CHECK(contains(runner.feed.lastError(), "stalled: no data for more than 600 ms during read"));
        const FeedStats s = runner.feed.stats();
        CHECK_GE(s.pings, 1u);
        CHECK_EQ(s.pongs, 0u);
    }
}

TEST_CASE("feed: a quiet session kept alive only by ping/pong still counts as healthy and resets the backoff") {
    MockExchangeServer server;
    // 连接 0..2 失败；3 = 安静的会话：一条 depth20 推送，之后约 600 ms 只有 WebSocket ping/pong，然后服务器关闭；
    // 4 及以后失败
    server.setAcceptHandler([](std::size_t i) { return i == 3 ? AcceptAction::Serve : AcceptAction::CloseImmediately; });
    server.setWebSocketScript([](WsSession& ws) {
        ws.send(binanceDepth(1000, {{"65000.00", "1.0"}}, {{"65000.01", "0.5"}}));
        ws.sleepMs(600);  // 期间 Beast 自动应答客户端的 ping
        ws.close();
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth20@100ms";
    cfg.ping_interval_ms = -1;  // WebSocket ping：空闲 100 ms 就发
    cfg.stall_timeout_ms = 200;
    cfg.healthy_session_ms = 300;
    cfg.reconnect_initial_ms = 30;
    cfg.reconnect_max_ms = 4000;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());
            return server.acceptCount() >= 5;
        },
        10000, 10));
    runner.stop();
    // 失败 0,1,2：30, 60, 120；安静但健康的会话 3 之后重置：30（不重置则为 240）
    checkAnnouncedBackoff(events, server, {30, 60, 120, 30}, 4000);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.stalls, 0u);
    CHECK_GE(s.pongs, 3u);
    CHECK(server.errors().empty());
}

TEST_CASE("feed: handler memory reuses the smallest fitting block and only touches the heap when the cache misses") {
    feed_detail::HandlerMemory mem;
    void* a = mem.allocate(100);   // 新块：128
    void* b = mem.allocate(300);   // 新块：384
    CHECK_EQ(mem.heapAllocations(), 2u);
    mem.deallocate(a);
    mem.deallocate(b);
    void* c = mem.allocate(90);    // 最小的够用块：a
    CHECK(c == a);
    void* d = mem.allocate(200);   // b
    CHECK(d == b);
    void* e = mem.allocate(50);    // 缓存已空：新块
    CHECK_EQ(mem.heapAllocations(), 3u);
    mem.deallocate(c);
    mem.deallocate(d);
    mem.deallocate(e);
    // 稳态：反复申请 / 释放同几种大小不再访问全局堆
    for (int i = 0; i < 1000; ++i) {
        void* x = mem.allocate(120);
        void* y = mem.allocate(380);
        void* z = mem.allocate(64);
        mem.deallocate(y);
        mem.deallocate(x);
        mem.deallocate(z);
    }
    CHECK_EQ(mem.heapAllocations(), 3u);
    // 缓存满（8 块）时再释放：留下较大的块
    std::vector<void*> blocks;
    for (std::size_t i = 0; i < feed_detail::HandlerMemory::kSlots + 1; ++i) blocks.push_back(mem.allocate(1000));
    for (void* p : blocks) mem.deallocate(p);
    void* big = mem.allocate(1000);  // 不需要新块
    CHECK_EQ(mem.heapAllocations(), 3u + feed_detail::HandlerMemory::kSlots + 1);
    mem.deallocate(big);
    feed_detail::HandlerAllocator<int> alloc(mem);
    int* p = alloc.allocate(4);
    p[3] = 7;
    CHECK_EQ(p[3], 7);
    alloc.deallocate(p, 4);
    CHECK(alloc == feed_detail::HandlerAllocator<double>(mem));
}

TEST_CASE("feed: Binance depth20 partial book stream goes LIVE on the first message (no REST)") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) {
        for (std::uint64_t id = 1000; id < 1005; ++id) {
            ws.send(binanceDepth(id, {{"65000.00", "1.0"}, {"64999.99", "2.0"}}, {{"65000.01", "0.5"}}));
            ws.sleepMs(5);
        }
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth20@100ms";
    FeedRunner runner(cfg);
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return log.all.size() >= 5; }, 5000));
    CHECK_EQ(idsToString(log.updateIds()), std::string("[1000,1001,1002,1003,1004]"));
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK_EQ(server.httpRequestCount(), std::size_t{0});
    CHECK_EQ(log.all[0].exchange_ts_ms, std::int64_t{0});  // depth20 推送没有时间戳
    CHECK_EQ(runner.feed.getExchangeLatency().count(), 0u);
}

// ===========================================================================
// 连接管理
// ===========================================================================

TEST_CASE("feed: server closes the WebSocket -> RECONNECTING, backoff, reconnect; empty book published on disconnect") {
    for (const bool graceful : {true, false}) {
        MockExchangeServer server;
        server.setWebSocketScripts({
            [graceful](WsSession& ws) {
                ws.send(simpleBook(0));
                ws.sleepMs(30);
                if (graceful) {
                    ws.close(1001, "going away");
                } else {
                    ws.abort();
                }
            },
            [](WsSession& ws) { ws.send(simpleBook(1)); },
        });
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.reconnect_initial_ms = 100;
        cfg.reconnect_max_ms = 1000;
        EventLog events;
        FeedRunner runner(cfg, events.callback());
        StatusWatcher watcher(runner.feed);
        SnapshotLog log;
        REQUIRE(waitFor(
            [&] {
                log.pump(runner.feed);
                return server.wsSessionCount() == 2 && log.all.size() >= 3 &&
                       runner.feed.status() == FeedStatus::Live;
            },
            5000));
        watcher.stopWatching();
        // 每次状态变化都有对应的日志事件：按顺序检查（采样式的 StatusWatcher 可能错过很短的状态）
        CHECK(events.inOrder({"[OKX] connecting to", "[OKX] connected to", "[OKX] book synced (LIVE)",
                              graceful ? "[OKX] server closed the WebSocket" : "[OKX] connection lost",
                              "[OKX] reconnecting in ", "[OKX] connecting to", "[OKX] connected to",
                              "[OKX] book synced (LIVE)"}));
        // 采样只做"至少看到"的弱检查：断线后的 RECONNECTING（持续约 100 ms 退避）
        CHECK(watcher.sawInOrder({FeedStatus::Reconnecting, FeedStatus::Live}));
        CHECK_EQ(runner.feed.status(), FeedStatus::Live);
        const FeedStats s = runner.feed.stats();
        CHECK_EQ(s.reconnects, 1u);
        CHECK_EQ(s.connect_failures, 0u);
        CHECK(s.connected_since_ns > 0);
        // 断线时发布空簿：书 -> 空 -> 书
        REQUIRE_EQ(log.all.size(), std::size_t{3});
        CHECK_EQ(log.all[0].bids[0].price, 65000.0);
        CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
        CHECK_EQ(log.all[2].bids[0].price, 65001.0);
        CHECK_EQ(log.all[2].seq, 3u);
        // 退避：宣布的等待时间在 100 ms ±20% 内，且下一次连接确实在宣布之后至少这么久才到达
        std::vector<int> delays;
        const std::vector<EventLog::Event> announced = events.reconnects(delays);
        REQUIRE_EQ(delays.size(), std::size_t{1});
        CHECK_GE(delays[0], 80);
        CHECK_LE(delays[0], 120);
        const std::vector<std::int64_t> accepts = server.acceptTimesNs();
        REQUIRE_EQ(accepts.size(), std::size_t{2});
        CHECK_GE(ms(accepts[1] - announced[0].time_ns), delays[0] - 1.0);
        CHECK_LT(ms(accepts[1] - announced[0].time_ns), delays[0] + 1000.0);
        const std::string err = runner.feed.lastError();
        if (graceful) {
            CHECK(contains(err, "server closed the WebSocket (code 1001: going away)"));
        } else {
            CHECK(contains(err, "connection lost"));
        }
        CHECK(events.any("[OKX] connected to wss://127.0.0.1:" + server.portString() + "/ws"));
        CHECK(events.any(graceful ? "[OKX] server closed the WebSocket" : "[OKX] connection lost"));
        CHECK(events.any("[OKX] book synced (LIVE)"));
        CHECK(server.errors().empty());
    }
}

TEST_CASE("feed: silent server -> checkHealth kicks after stall_timeout_ms -> reconnect") {
    MockExchangeServer server;
    server.setWebSocketScripts({
        [](WsSession& ws) { ws.send(simpleBook(0)); },  // 之后静默
        [](WsSession& ws) {
            for (int i = 0; i < 100; ++i) {  // 第二个会话持续有数据（5 s）
                ws.send(simpleBook(1 + i));
                ws.sleepMs(50);
            }
        },
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.stall_timeout_ms = 300;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    SnapshotLog log;
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());  // 模拟主线程每次刷新调用
            log.pump(runner.feed);
            return server.wsSessionCount() == 2 && runner.feed.status() == FeedStatus::Live && log.all.size() >= 3;
        },
        5000, 10));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.stalls, 1u);
    CHECK_EQ(s.reconnects, 1u);
    CHECK_EQ(s.connect_failures, 0u);
    CHECK(contains(runner.feed.lastError(), "stalled: no data for more than 300 ms during read"));
    CHECK_EQ(log.all[0].bids[0].price, 65000.0);  // 第一个会话的快照确实收到了（停顿计时从它开始）
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
    // 客户端时间线：连接日志 -> 快照（之后静默）-> 至少 300 ms 后踢断
    std::int64_t connected_ns = 0, stalled_ns = 0;
    for (const EventLog::Event& e : events.events()) {
        if (connected_ns == 0 && contains(e.line, "[OKX] connected to")) connected_ns = e.time_ns;
        if (stalled_ns == 0 && contains(e.line, "[OKX] stalled")) stalled_ns = e.time_ns;
    }
    REQUIRE(connected_ns != 0);
    REQUIRE(stalled_ns != 0);
    CHECK_GE(ms(stalled_ns - connected_ns), 300.0);
    CHECK_LT(ms(stalled_ns - connected_ns), 3000.0);
    // 健康的第二个会话不会被误踢
    for (int i = 0; i < 40; ++i) {
        runner.feed.checkHealth(hft::nowNs());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK_EQ(runner.feed.stats().stalls, 1u);
    CHECK(server.errors().empty());
}

TEST_CASE("feed: stop() while blocked in read returns and run() exits within 500 ms") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });  // 之后静默
    FeedRunner runner(baseConfig(Exchange::OKX, server));
    REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Live; }, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 确保阻塞在 ws.read 中
    CHECK(runner.feed.isRunning());
    const double elapsed = runner.stop();
    CHECK_LT(elapsed, 500.0);
    CHECK_EQ(runner.feed.status(), FeedStatus::Stopped);
    CHECK(!runner.feed.isRunning());
    CHECK(waitFor([&] { return server.openConnections() == 0; }, 2000));  // 服务器看到连接关闭
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connected_since_ns, std::int64_t{0});
    CHECK_EQ(s.connect_failures, 0u);
    CHECK_EQ(runner.feed.lastError(), std::string());  // 主动停止不是错误
}

TEST_CASE("feed: stop() is prompt in every phase (backoff, TCP connect, TLS handshake, REST request, before run)") {
    {  // 退避等待中
        MockExchangeServer server;
        server.setAcceptHandler([](std::size_t) { return AcceptAction::CloseImmediately; });
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.reconnect_initial_ms = 5000;
        cfg.reconnect_max_ms = 5000;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Reconnecting; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        CHECK_LT(runner.stop(), 500.0);
        CHECK_EQ(runner.feed.status(), FeedStatus::Stopped);
        CHECK_EQ(runner.feed.stats().connect_failures, 1u);
    }
    {  // 阻塞在 TCP connect（SYN_SENT）中
        BlackholeListener hole;
        REQUIRE(hole.ok());
        FeedConfig cfg;
        cfg.exchange = Exchange::OKX;
        cfg.name = "OKX";
        cfg.host = "127.0.0.1";
        cfg.port = hole.portString();
        cfg.log_to_stderr = false;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Connecting; }, 2000));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK_EQ(runner.feed.status(), FeedStatus::Connecting);
        CHECK_LT(runner.stop(), 500.0);
        CHECK_EQ(runner.feed.stats().connect_failures, 0u);  // 主动停止不计为连接失败
    }
    {  // TCP 已连接，服务器不做 TLS 握手
        MockExchangeServer server;
        server.setAcceptHandler([](std::size_t) { return AcceptAction::Silent; });
        FeedRunner runner(baseConfig(Exchange::OKX, server));
        REQUIRE(waitFor([&] { return server.acceptCount() == 1; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK_EQ(runner.feed.status(), FeedStatus::Connecting);
        CHECK_LT(runner.stop(), 500.0);
    }
    {  // 阻塞在 REST 快照请求中（服务器 10 s 后才应答）
        MockExchangeServer server;
        server.setHttpHandler([](const HttpRequestInfo&) { return HttpReply{200, "{}", "application/json", 10000}; });
        server.setWebSocketScript([](WsSession& ws) { ws.send(binanceDiff(1, 2, {{"1.00", "1"}}, {})); });
        FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
        cfg.rest_host = "127.0.0.1";
        cfg.rest_port = server.portString();
        cfg.snapshot_path = kBinanceSnapshotPath;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return server.httpRequestCount() == 1; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        CHECK_EQ(runner.feed.status(), FeedStatus::Syncing);
        CHECK_LT(runner.stop(), 500.0);
    }
    {  // run() 之前就 stop()：run() 立即返回
        FeedConfig cfg;
        cfg.exchange = Exchange::BYBIT;
        cfg.name = "Bybit";
        cfg.host = "127.0.0.1";
        cfg.port = "1";
        cfg.log_to_stderr = false;
        MarketDataFeed feed(cfg);
        CHECK_EQ(feed.status(), FeedStatus::Idle);
        CHECK(!feed.isRunning());
        feed.stop();
        const std::int64_t t0 = hft::nowNs();
        feed.run();
        CHECK_LT(ms(hft::nowNs() - t0), 100.0);
        CHECK_EQ(feed.status(), FeedStatus::Stopped);
        CHECK_EQ(feed.stats().reconnects, 0u);
    }
}

TEST_CASE("feed: stop() during a blackholed TCP connect returns within 200 ms, also when it lands before connect() starts") {
    BlackholeListener hole;
    REQUIRE(hole.ok());
    FeedConfig cfg;
    cfg.exchange = Exchange::OKX;
    cfg.name = "OKX";
    cfg.host = "127.0.0.1";
    cfg.port = hole.portString();
    cfg.tls_verify = false;
    cfg.log_to_stderr = false;
    {  // 已在 SYN_SENT 中
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Connecting; }, 2000));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK_EQ(runner.feed.status(), FeedStatus::Connecting);
        CHECK_LT(runner.stop(), 200.0);
        CHECK_EQ(runner.feed.status(), FeedStatus::Stopped);
    }
    {  // feed 线程停在 connect() 系统调用之前（套接字已登记）：此刻的 shutdown 对套接字是空操作
        g_connect_parked.store(false);
        g_connect_release.store(false);
        g_connect_park.store(true);
        FeedRunner runner(cfg);
        REQUIRE(waitFor([] { return g_connect_parked.load(); }, 2000));
        const std::int64_t t0 = hft::nowNs();
        runner.feed.stop();
        g_connect_release.store(true);
        const bool stopped = waitFor([&] { return runner.feed.status() == FeedStatus::Stopped; }, 1000);
        CHECK(stopped);
        CHECK_LT(ms(hft::nowNs() - t0), 200.0);
        CHECK_EQ(runner.feed.stats().connect_failures, 0u);
    }
    {  // 连接超时的踢断落在同一个窗口里：这次尝试仍在约一个轮询片内结束（而不是等内核放弃 SYN 重传）
        FeedConfig c = cfg;
        c.connect_timeout_ms = 300;
        c.reconnect_initial_ms = 5000;
        c.reconnect_max_ms = 5000;
        g_connect_parked.store(false);
        g_connect_release.store(false);
        g_connect_park.store(true);
        FeedRunner runner(c);
        REQUIRE(waitFor([] { return g_connect_parked.load(); }, 2000));
        const std::int64_t t0 = hft::nowNs();
        runner.feed.checkHealth(hft::nowNs() + std::int64_t{10} * 1000000000);  // 连接时限早已过去
        g_connect_release.store(true);
        REQUIRE(waitFor([&] { return runner.feed.stats().connect_failures == 1 && !runner.feed.lastError().empty(); },
                        1000));
        CHECK_LT(ms(hft::nowNs() - t0), 200.0);
        CHECK_EQ(runner.feed.status(), FeedStatus::Reconnecting);
        CHECK(contains(runner.feed.lastError(), "connect timeout: TCP connect did not complete within 300 ms"));
        CHECK_LT(runner.stop(), 200.0);  // 退避等待中
    }
    g_connect_release.store(true);
}

TEST_CASE("feed: a blackholed first address falls through to the next one within connect_timeout_ms (WebSocket host)") {
    MockExchangeServer server(dualHostServerOptions());
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    BlackholeListener hole;
    REQUIRE(hole.ok());
    const DualHost dual(hole.port(), server.port());
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.host = "dual.test";
    cfg.connect_timeout_ms = 2000;  // 每个地址 max(1000, 2000 / 2) = 1000 ms
    FeedRunner runner(cfg);
    const std::int64_t t0 = hft::nowNs();
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());  // 模拟主线程每次刷新调用
            return runner.feed.status() == FeedStatus::Live;
        },
        5000, 10));
    const double took = ms(hft::nowNs() - t0);
    CHECK_GE(took, 990.0);   // 黑洞地址用满了自己的时限
    CHECK_LT(took, 2000.0);  // 同一个会话、在总时限之内换到了第二个地址
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connect_failures, 0u);
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(server.acceptCount(), std::size_t{1});
    CHECK(server.errors().empty());
}

TEST_CASE("feed: a blackholed first REST address falls through to the next one before the stall watchdog fires") {
    MockExchangeServer server(dualHostServerOptions());
    server.setHttpHandler([](const HttpRequestInfo&) {
        return HttpReply{200, binanceDepth(105, {{"65000.00", "1.0"}}, {{"65001.00", "1.0"}})};
    });
    server.setWebSocketScript([](WsSession& ws) { ws.send(binanceDiff(101, 110, {{"65000.00", "1.5"}}, {})); });
    BlackholeListener hole;
    REQUIRE(hole.ok());
    const DualHost dual(hole.port(), server.port());
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth@100ms";
    cfg.rest_host = "dual.test";
    cfg.rest_port = "443";  // 替换后的解析忽略端口
    cfg.snapshot_path = kBinanceSnapshotPath;
    cfg.connect_timeout_ms = 2000;  // REST 连接总时限 min(2000, 10000 / 2) = 2000 ms → 每个地址 1000 ms
    FeedRunner runner(cfg);
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());
            return runner.feed.status() == FeedStatus::Live;
        },
        5000, 10));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.rest_snapshots, 1u);
    CHECK_EQ(s.stalls, 0u);
    CHECK_EQ(s.reconnects, 0u);
    CHECK_EQ(s.connect_failures, 0u);
    const std::vector<HttpRequestInfo> reqs = server.httpRequests();
    const std::vector<std::int64_t> sessions = server.wsSessionTimesNs();
    REQUIRE_EQ(reqs.size(), std::size_t{1});
    REQUIRE_EQ(sessions.size(), std::size_t{1});
    CHECK_GE(ms(reqs[0].time_ns - sessions[0]), 990.0);  // 先在黑洞地址上用满 1000 ms
    CHECK_LT(ms(reqs[0].time_ns - sessions[0]), 2000.0);
    CHECK(server.errors().empty());
}

TEST_CASE("feed: after a connect timeout on a blackholed address the next session starts at the next address") {
    MockExchangeServer server(dualHostServerOptions());
    server.setWebSocketScripts({
        [](WsSession& ws) {
            ws.send(simpleBook(0));
            ws.sleepMs(50);
            ws.close(1001, "going away");
        },
        [](WsSession& ws) { ws.send(simpleBook(1)); },
    });
    BlackholeListener hole;
    REQUIRE(hole.ok());
    const DualHost dual(hole.port(), server.port());
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.host = "dual.test";
    cfg.connect_timeout_ms = 400;  // 单个地址的下限 1000 ms 比总时限还长：第一个会话耗在黑洞地址上
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());
            return server.wsSessionCount() == 2 && runner.feed.status() == FeedStatus::Live;
        },
        5000, 10));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connect_failures, 1u);  // 只有第一个会话失败；服务器关闭之后的会话仍从连上过的地址开始
    CHECK_EQ(s.reconnects, 2u);
    CHECK_EQ(server.acceptCount(), std::size_t{2});
    CHECK(events.inOrder({"[OKX] connect timeout: TCP connect did not complete within 400 ms", "[OKX] connected to",
                          "[OKX] server closed the WebSocket", "[OKX] connected to"}));
    CHECK(server.errors().empty());
}

TEST_CASE("feed: wrong CA / untrusted certificate -> connect_failures and a clear lastError, never LIVE") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    mock_exchange::CertificateOptions other_opt;
    other_opt.common_name = "Some Other CA";
    const mock_exchange::TlsIdentity other = mock_exchange::generateSelfSigned(other_opt);
    const mock_exchange::TempFile other_ca(other.cert_pem, "wrong_ca");
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.ca_file = other_ca.path();  // 一个没有签发服务器证书的 CA
    cfg.reconnect_initial_ms = 30;
    FeedConfig system_only = baseConfig(Exchange::OKX, server);
    system_only.ca_file.clear();  // 只信任系统 CA
    system_only.reconnect_initial_ms = 30;
    {
        FeedRunner runner(cfg);
        StatusWatcher watcher(runner.feed);
        FeedRunner runner2(system_only);
        REQUIRE(waitFor(
            [&] {
                return runner.feed.stats().connect_failures >= 2 && runner2.feed.stats().connect_failures >= 2 &&
                       !runner.feed.lastError().empty() && !runner2.feed.lastError().empty();
            },
            5000));
        watcher.stopWatching();
        for (MarketDataFeed* f : {&runner.feed, &runner2.feed}) {
            const std::string err = f->lastError();
            CHECK(contains(err, "TLS certificate verification failed for 127.0.0.1: self-signed certificate"));
            if (!contains(err, "self-signed certificate")) std::cerr << "    lastError: " << err << std::endl;
            CHECK_EQ(f->stats().messages, 0u);
        }
        for (FeedStatus st : watcher.transitions()) CHECK(st != FeedStatus::Live && st != FeedStatus::Syncing);
    }
    CHECK_EQ(server.wsSessionCount(), std::size_t{0});
    CHECK(!server.tlsFailures().empty());  // 服务器端看到客户端拒绝证书的 alert
}

TEST_CASE("feed: certificate without a matching SAN -> IP/hostname mismatch; tls_verify=false still connects") {
    mock_exchange::ServerOptions opt;
    opt.certificate.common_name = "example.invalid";
    opt.certificate.dns_names = {"example.invalid"};
    opt.certificate.ip_addresses.clear();
    MockExchangeServer server(opt);
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    // 证书本身可信（ca_file = 服务器证书），但 SAN 里既没有 127.0.0.1 也没有 localhost
    for (const char* host : {"127.0.0.1", "localhost"}) {
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.host = host;
        cfg.reconnect_initial_ms = 30;
        FeedRunner runner(cfg);
        REQUIRE(waitFor(
            [&] { return runner.feed.stats().connect_failures >= 1 && !runner.feed.lastError().empty(); }, 5000));
        const std::string err = runner.feed.lastError();
        CHECK(contains(err, std::string("TLS certificate verification failed for ") + host));
        CHECK(contains(err, std::string(host) == "localhost" ? "hostname mismatch" : "IP address mismatch"));
        CHECK_EQ(runner.feed.stats().messages, 0u);
    }
    CHECK_EQ(server.wsSessionCount(), std::size_t{0});
    {
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.tls_verify = false;
        cfg.ca_file.clear();
        FeedRunner runner(cfg);
        SnapshotLog log;
        REQUIRE(waitFor([&] { log.pump(runner.feed); return !log.all.empty(); }, 5000));
        CHECK_EQ(runner.feed.status(), FeedStatus::Live);
        CHECK_EQ(runner.feed.stats().connect_failures, 0u);
    }
}

TEST_CASE("feed: tls_verify=false connects to the self-signed server without any CA") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.tls_verify = false;
    cfg.ca_file.clear();
    cfg.host = "localhost";  // 同时走后台线程的 DNS 解析路径
    FeedRunner runner(cfg);
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return !log.all.empty(); }, 5000));
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    CHECK_EQ(log.all[0].bids[0].price, 65000.0);
    // 脚本不等订阅帧就推送：服务器线程只在下一次运行 io_context 时才记录客户端的帧，先等它记录下来
    CHECK(waitFor([&] { return !server.framesReceived(0).empty(); }, 2000));
    CHECK_EQ(server.framesReceived(0), std::vector<std::string>{kOkxBooks5Sub});
}

TEST_CASE("feed: snapshots arrive through the SPSC queue with increasing seq, sane timestamps and the venue id") {
    MockExchangeServer server;
    const std::int64_t base_ts = hft::wallClockMs();
    constexpr int kCount = 50;
    server.setWebSocketScript([base_ts](WsSession& ws) {
        for (int i = 0; i < kCount; ++i) {
            ws.send(simpleBook(i, base_ts + i));
            ws.sleepMs(2);
        }
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.venue_id = 7;
    cfg.publish_levels = 1;
    FeedRunner runner(cfg);
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return log.all.size() >= static_cast<std::size_t>(kCount); }, 5000));
    const FeedStats s = runner.feed.stats();
    for (int i = 0; i < kCount; ++i) {
        const BookSnapshot& snap = log.all[static_cast<std::size_t>(i)];
        CHECK_EQ(snap.seq, static_cast<std::uint64_t>(i + 1));
        CHECK_EQ(snap.venue, 7u);
        CHECK_LE(snap.recv_ns, snap.publish_ns);
        CHECK_LE(snap.publish_ns, log.pop_ns[static_cast<std::size_t>(i)]);
        CHECK_GE(snap.recv_ns, s.connected_since_ns);
        CHECK_EQ(snap.exchange_ts_ms, base_ts + i);
        CHECK_EQ(snap.bid_count, 1u);
        CHECK_EQ(snap.bids[0].price, 65000.0 + i);
        CHECK_EQ(snap.asks[0].price, 65001.0 + i);
        if (i > 0) CHECK_GE(snap.recv_ns, log.all[static_cast<std::size_t>(i - 1)].recv_ns);
    }
    // published 在 tryPush 之后才计数：取到最后一个快照时可能还差一次，稍等即可
    CHECK(waitFor([&] { return runner.feed.stats().published == static_cast<std::uint64_t>(kCount); }, 2000));
    CHECK_EQ(s.dropped_snapshots, 0u);
    CHECK_EQ(s.messages, static_cast<std::uint64_t>(kCount));
    CHECK_EQ(s.book_updates, static_cast<std::uint64_t>(kCount));
    CHECK_GT(s.bytes, static_cast<std::uint64_t>(kCount * 100));
    CHECK_GT(s.last_message_ns, s.connected_since_ns);
    CHECK_EQ(runner.feed.getMessageCount(), kCount);
    CHECK_EQ(runner.feed.getLatencyTracker().count(), static_cast<std::uint64_t>(kCount));
    CHECK_EQ(runner.feed.getExchangeLatency().count(), static_cast<std::uint64_t>(kCount));
    CHECK_LT(runner.feed.getLatencyTracker().maxNs(), static_cast<std::uint64_t>(1000000000));
    CHECK_EQ(runner.feed.venueId(), 7u);
    CHECK_EQ(runner.feed.exchange(), Exchange::OKX);
    CHECK_EQ(runner.feed.getName(), std::string("OKX"));
}

TEST_CASE("feed: dropped_snapshots counts publishes that found the tiny queue full; the producer never blocks") {
    MockExchangeServer server;
    constexpr int kCount = 20;
    server.setWebSocketScript([](WsSession& ws) {
        for (int i = 0; i < kCount; ++i) ws.send(simpleBook(i));
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.queue_capacity = 2;
    FeedRunner runner(cfg);
    REQUIRE_EQ(runner.feed.queue().capacity(), std::size_t{2});
    // messages 在处理之前计数；等待每条消息的发布尝试都已计入 published 或 dropped
    REQUIRE(waitFor(
        [&] {
            const FeedStats st = runner.feed.stats();
            return st.published + st.dropped_snapshots == static_cast<std::uint64_t>(kCount);
        },
        5000));
    // 非空快照入队之后要留一个空槽位（给断线时的空簿）：容量 2 的队列只放得下一本簿
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.published, 1u);
    CHECK_EQ(s.dropped_snapshots, static_cast<std::uint64_t>(kCount - 1));
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    SnapshotLog log;
    log.pump(runner.feed);
    REQUIRE_EQ(log.all.size(), std::size_t{1});
    CHECK_EQ(log.all[0].seq, 1u);  // 留在队列里的是最早的那一本；之后的 seq 跳号表示丢弃
    CHECK_EQ(runner.feed.getLatencyTracker().count(), static_cast<std::uint64_t>(kCount));  // 丢弃的也计入处理延迟
    // 消费者手里是非空簿时丢弃的快照不重试：腾出位置之后也不会再送来
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_EQ(log.pump(runner.feed), std::size_t{0});
}

TEST_CASE("feed: the empty book of a disconnect always fits: a full queue keeps its last slot for it") {
    // 队列容量 2 代表"消费者卡住、1024 个槽位已满"；断线之后交易所长时间不可用（退避 3 s）
    MockExchangeServer server;
    server.setWebSocketScripts({
        [](WsSession& ws) {
            for (int i = 0; i < 3; ++i) ws.send(simpleBook(i));
            ws.sleepMs(50);
            ws.close(1001, "going away");
        },
        [](WsSession& ws) { ws.send(simpleBook(9)); },
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.queue_capacity = 2;
    cfg.reconnect_initial_ms = 3000;
    cfg.reconnect_max_ms = 3000;
    FeedRunner runner(cfg);
    // 消费者直到断线之后才开始取
    REQUIRE(waitFor([&] { return runner.feed.status() == FeedStatus::Reconnecting; }, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.published, 2u);          // 第 1 本簿与断线时的空簿
    CHECK_EQ(s.dropped_snapshots, 2u);  // 第 2、3 本簿：非空快照不能占用最后一个槽位
    // 队列里最后一个就是空簿：消费者一腾出位置就不再使用断线前的价格，不必等任何重试
    SnapshotLog log;
    log.pump(runner.feed);
    REQUIRE_EQ(log.all.size(), std::size_t{2});
    CHECK_EQ(log.all[0].bids[0].price, 65000.0);
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
    CHECK_EQ(log.all[1].seq, 4u);  // 2、3 号（第 2、3 本簿）确实丢了
    CHECK_LE(log.all[1].publish_ns, log.pop_ns[1]);
    CHECK_EQ(runner.feed.status(), FeedStatus::Reconnecting);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK_EQ(log.pump(runner.feed), std::size_t{0});  // 只送一次
    CHECK_EQ(runner.feed.stats().published, 2u);
}

namespace {

// 消费者在 feed 线程卡在下一个会话的某个同步步骤里时才腾出队列：它取到的最后一个快照必须是断线时的空簿
//（第 1 本簿、空簿；第 2、3 本簿被丢弃），而且立即就能取到
void checkDrainEndsWithEmptyBook(MarketDataFeed& feed, const char* phase) {
    SnapshotLog log;
    log.pump(feed);
    const FeedStats s = feed.stats();
    const char* last = "-";
    if (!log.all.empty()) last = log.all.back().bid_count + log.all.back().ask_count == 0 ? "empty" : "NOT empty";
    std::printf("    next session stuck in %s: consumer drained %zu snapshot(s), last one %s (published %llu, "
                "dropped %llu)\n",
                phase, log.all.size(), last, static_cast<unsigned long long>(s.published),
                static_cast<unsigned long long>(s.dropped_snapshots));
    REQUIRE_EQ(log.all.size(), std::size_t{2});
    CHECK_GT(log.all[0].bid_count + log.all[0].ask_count, 0u);
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
    CHECK_EQ(log.all[1].seq, 4u);
    CHECK_EQ(s.published, 2u);
    CHECK_EQ(s.dropped_snapshots, 2u);
}

// 第一个会话：三本簿，然后服务器关闭
void threeBooksThenClose(WsSession& ws) {
    for (int i = 0; i < 3; ++i) ws.send(simpleBook(i));
    ws.sleepMs(50);
    ws.close(1001, "going away");
}

}  // namespace

TEST_CASE("feed: a consumer that drains while the next session is stuck (TLS, WebSocket upgrade, REST) gets the empty book") {
    // 队列容量 2 代表"消费者卡住、队列已满"。之前断线时的空簿没能入队、保持待发布，而 TLS 握手、WebSocket 升级和
    // 第一个读处理器里的 REST 请求都是同步的、期间没有重试：消费者腾出位置之后仍拿着断线前的价格，直到
    // connect_timeout_ms（默认 10 s）或停顿看门狗结束那个会话
    {  // TCP 已连上，服务器不做 TLS 握手
        MockExchangeServer server;
        server.setAcceptHandler([](std::size_t i) { return i == 0 ? AcceptAction::Serve : AcceptAction::Silent; });
        server.setWebSocketScript(threeBooksThenClose);
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.queue_capacity = 2;
        cfg.connect_timeout_ms = 3000;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return server.acceptCount() >= 2; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK_EQ(runner.feed.status(), FeedStatus::Connecting);
        checkDrainEndsWithEmptyBook(runner.feed, "the TLS handshake");
    }
    {  // WebSocket 升级请求迟迟得不到应答
        std::atomic<int> upgrades{0};      // 声明在服务器之前：服务器线程里的过滤器可能用到它们，直到服务器析构
        std::atomic<bool> release{false};
        MockExchangeServer server;
        const MockExchangeServer* srv = &server;
        server.setUpgradeFilter([&upgrades, &release, srv](const std::string&) {
            if (upgrades.fetch_add(1) >= 1) {
                const std::int64_t t0 = hft::nowNs();
                while (!release.load() && !srv->stopping() && hft::nowNs() - t0 < 5'000'000'000LL) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            }
            return true;
        });
        server.setWebSocketScript(threeBooksThenClose);
        FeedConfig cfg = baseConfig(Exchange::OKX, server);
        cfg.queue_capacity = 2;
        cfg.connect_timeout_ms = 3000;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return upgrades.load() >= 2; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK_EQ(runner.feed.status(), FeedStatus::Connecting);
        checkDrainEndsWithEmptyBook(runner.feed, "the WebSocket upgrade");
        release.store(true);
    }
    {  // Binance 增量深度：下一个会话第一条消息触发的 REST 快照请求迟迟得不到应答
        MockExchangeServer server;
        server.setHttpHandler([](const HttpRequestInfo& info) {
            if (info.index == 0) return HttpReply{200, binanceDepth(105, {{"65000.00", "1.0"}}, {{"65001.00", "1.0"}})};
            return HttpReply{200, binanceDepth(205, {{"65000.00", "1.0"}}, {{"65001.00", "1.0"}}), "application/json",
                             3000};
        });
        server.setWebSocketScripts({
            [](WsSession& ws) {
                // 快照（L=105）之后回放 101..110 → 第 1 本簿；111..120、121..130 → 第 2、3 本簿
                ws.send(binanceDiff(101, 110, {{"65000.00", "1.5"}}, {}));
                ws.send(binanceDiff(111, 120, {{"65000.00", "2.5"}}, {}));
                ws.send(binanceDiff(121, 130, {{"65000.00", "3.5"}}, {}));
                ws.waitUntil([&ws] { return ws.server().httpRequestCount() >= 1; }, 5000);
                ws.sleepMs(100);
                ws.close(1001, "going away");
            },
            [](WsSession& ws) { ws.send(binanceDiff(201, 210, {{"65000.00", "1.5"}}, {})); },
        });
        FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
        cfg.path = "/ws/btcusdt@depth@100ms";
        cfg.rest_host = "127.0.0.1";
        cfg.rest_port = server.portString();
        cfg.snapshot_path = kBinanceSnapshotPath;
        cfg.queue_capacity = 2;
        FeedRunner runner(cfg);
        REQUIRE(waitFor([&] { return server.httpRequestCount() >= 2; }, 5000));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK_EQ(runner.feed.status(), FeedStatus::Syncing);
        CHECK_EQ(server.wsSessionCount(), std::size_t{2});
        checkDrainEndsWithEmptyBook(runner.feed, "a REST snapshot request");
    }
}

TEST_CASE("feed: the publish callback runs on the feed thread right after every successful push, empty books and retries included") {
    MockExchangeServer server;
    server.setWebSocketScripts({
        threeBooksThenClose,
        [](WsSession& ws) { ws.send(simpleBook(9)); },  // 之后安静
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    // 第 2、3 本簿入队失败（非空快照要给空簿留一个槽位），断线时的空簿用掉最后一个；新会话的第一本簿入队失败、
    // 保持待发布，消费者腾出位置之后由重试送达
    cfg.queue_capacity = 2;
    struct Calls {
        std::mutex mutex;
        std::vector<std::size_t> queue_sizes;    // 回调时队列里的快照数（消费者在新会话 LIVE 之前不取）
        std::vector<std::uint64_t> published;    // 回调时的 stats().published
        std::vector<std::thread::id> threads;
    };
    auto calls = std::make_shared<Calls>();
    FeedRunner runner(cfg, {}, [calls](MarketDataFeed& f) {
        const std::size_t size = f.queue().size();
        const std::uint64_t published = f.stats().published;
        std::lock_guard<std::mutex> lock(calls->mutex);
        calls->queue_sizes.push_back(size);
        calls->published.push_back(published);
        calls->threads.push_back(std::this_thread::get_id());
    });
    REQUIRE(waitFor(
        [&] {
            return server.wsSessionCount() == 2 && runner.feed.status() == FeedStatus::Live &&
                   runner.feed.stats().dropped_snapshots == 3;
        },
        5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    {
        std::lock_guard<std::mutex> lock(calls->mutex);
        CHECK_EQ(calls->queue_sizes, (std::vector<std::size_t>{1, 2}));   // 每次都在入队之后：第 1 本簿、空簿
        CHECK_EQ(calls->published, (std::vector<std::uint64_t>{1, 2}));    // 计数已包含这一次
    }
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return log.all.size() == 3; }, 500));  // 重试送达新会话的簿
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
    CHECK_EQ(log.all[2].bids[0].price, 65009.0);
    std::lock_guard<std::mutex> lock(calls->mutex);
    CHECK_EQ(calls->queue_sizes.size(), std::size_t{3});  // 丢弃的 3 次没有回调，重试成功的那次有
    CHECK_EQ(calls->published.back(), 3u);
    for (const std::thread::id& id : calls->threads) CHECK(id == runner.thread.get_id());
}

TEST_CASE("feed: an exception thrown by the publish callback is swallowed and the feed keeps publishing") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) {
        for (int i = 0; i < 5; ++i) ws.send(simpleBook(i));
    });
    auto calls = std::make_shared<std::atomic<int>>(0);
    FeedRunner runner(baseConfig(Exchange::OKX, server), {}, [calls](MarketDataFeed&) {
        if (calls->fetch_add(1) == 0) throw std::runtime_error("callback failure");
    });
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return log.all.size() == 5; }, 5000));
    CHECK_EQ(calls->load(), 5);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
}

TEST_CASE("feed: after a disconnect, the next session's book that found the queue full is retried once there is room") {
    MockExchangeServer server;
    server.setWebSocketScripts({
        threeBooksThenClose,
        [](WsSession& ws) { ws.send(simpleBook(9)); },  // 之后安静：没有新的推送来替换消费者手里的空簿
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.queue_capacity = 2;
    FeedRunner runner(cfg);
    // 消费者直到新会话 LIVE（其第一本簿因队列满没能入队）之后才开始取
    REQUIRE(waitFor(
        [&] {
            return server.wsSessionCount() == 2 && runner.feed.status() == FeedStatus::Live &&
                   runner.feed.stats().dropped_snapshots == 3;
        },
        5000));
    SnapshotLog log;
    REQUIRE(waitFor([&] { log.pump(runner.feed); return log.all.size() == 3; }, 500));
    CHECK_EQ(log.all[0].bids[0].price, 65000.0);
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);  // 断线时的空簿
    CHECK_EQ(log.all[1].seq, 4u);
    // 消费者手里是空簿：新会话没能入队的簿保持待发布，腾出位置之后送达（推送稀少的交易所不会一直显示空簿）
    REQUIRE_EQ(log.all[2].bid_count, 1u);
    CHECK_EQ(log.all[2].bids[0].price, 65009.0);
    CHECK_EQ(log.all[2].seq, 5u);
    CHECK_LT(ms(log.pop_ns[2] - log.all[2].publish_ns), 100.0);  // publish_ns = 真正入队的时刻
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK_EQ(log.pump(runner.feed), std::size_t{0});  // 只送一次
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.published, 3u);
    CHECK_EQ(s.dropped_snapshots, 3u);  // 重试不重复计数
}

TEST_CASE("feed: a book still pending when its session ends is dropped with it, never delivered after the empty book") {
    MockExchangeServer server;
    // 第三个连接起服务器不做 TLS 握手：之后不会再有新的簿
    server.setAcceptHandler([](std::size_t i) { return i < 2 ? AcceptAction::Serve : AcceptAction::Silent; });
    server.setWebSocketScripts({
        [](WsSession& ws) {
            ws.send(simpleBook(0));
            ws.sleepMs(50);
            ws.close(1001, "going away");
        },
        [](WsSession& ws) {
            ws.send(simpleBook(9));  // 队列已满（第 1 本簿 + 断线时的空簿）：没能入队，保持待发布
            ws.sleepMs(50);
            ws.close(1001, "going away");
        },
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.queue_capacity = 2;
    cfg.reconnect_initial_ms = 1000;
    cfg.reconnect_max_ms = 1000;
    FeedRunner runner(cfg);
    // 第二个会话也结束之后（退避约 1 s 期间），消费者才开始取
    REQUIRE(waitFor(
        [&] {
            return server.wsSessionCount() == 2 && runner.feed.stats().dropped_snapshots == 1 &&
                   runner.feed.status() == FeedStatus::Reconnecting;
        },
        5000));
    SnapshotLog log;
    log.pump(runner.feed);
    REQUIRE_EQ(log.all.size(), std::size_t{2});
    CHECK_EQ(log.all[0].bids[0].price, 65000.0);
    CHECK_EQ(log.all[1].bid_count + log.all[1].ask_count, 0u);
    // 第二个会话的簿随会话一起作废：腾出位置之后也不会在空簿之后送来过期的 65009
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    CHECK_EQ(log.pump(runner.feed), std::size_t{0});
    CHECK_EQ(runner.feed.stats().published, 2u);
    CHECK_NE(runner.feed.status(), FeedStatus::Live);
}

TEST_CASE("feed: failed attempts back off exponentially (initial, 2x, 4x ... capped at max)") {
    MockExchangeServer server;
    server.setAcceptHandler([](std::size_t) { return AcceptAction::CloseImmediately; });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.reconnect_initial_ms = 40;
    cfg.reconnect_max_ms = 320;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor([&] { return server.acceptCount() >= 6; }, 8000));
    runner.stop();
    checkAnnouncedBackoff(events, server, {40, 80, 160, 320, 320}, 320);
    const FeedStats s = runner.feed.stats();
    CHECK_GE(s.connect_failures, 5u);
    CHECK_GE(s.reconnects, 5u);
    CHECK_EQ(s.messages, 0u);
    CHECK(contains(runner.feed.lastError(), "TLS handshake with 127.0.0.1 failed"));
}

TEST_CASE("feed: the backoff attempt counter resets only after a session stayed LIVE for healthy_session_ms") {
    MockExchangeServer server;
    // 连接 0..2 失败；3 = 短会话（LIVE 约 40 ms）；4 失败；5 = 健康会话（LIVE 约 300 ms）；6 及以后失败
    server.setAcceptHandler([](std::size_t i) {
        return i == 3 || i == 5 ? AcceptAction::Serve : AcceptAction::CloseImmediately;
    });
    server.setWebSocketScripts({
        [](WsSession& ws) {
            ws.send(simpleBook(0));
            ws.sleepMs(40);
            ws.close();
        },
        [](WsSession& ws) {
            for (int i = 0; i < 15; ++i) {
                ws.send(simpleBook(i));
                ws.sleepMs(20);
            }
            ws.close();
        },
    });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.reconnect_initial_ms = 30;
    cfg.reconnect_max_ms = 4000;
    cfg.healthy_session_ms = 150;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor([&] { return server.acceptCount() >= 7; }, 10000));
    runner.stop();
    // 失败 0,1,2：30, 60, 120；短会话 3 不重置：240；失败 4：480；健康会话 5 之后重置：30（否则 960）
    checkAnnouncedBackoff(events, server, {30, 60, 120, 240, 480, 30}, 4000);
    CHECK(server.errors().empty());
}

TEST_CASE("feed: adapter-requested reconnect (Binance serverShutdown) is immediate, then falls back to backoff") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) {
        ws.send(binanceDepth(10, {{"65000.00", "1.0"}}, {{"65000.01", "1.0"}}));
        ws.send(R"({"e":"serverShutdown","E":1700000000000})");
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth20@100ms";
    cfg.reconnect_initial_ms = 400;
    cfg.reconnect_max_ms = 400;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor([&] { return server.acceptCount() >= 5; }, 8000));
    runner.stop();
    // 前 kMaxImmediateReconnects 次立即重连（"reconnecting now"），之后按退避（400 ms，-20% 抖动）
    static_assert(MarketDataFeed::kMaxImmediateReconnects == 3, "update the expected sequence");
    checkAnnouncedBackoff(events, server, {0, 0, 0, 400}, 400);
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connect_failures, 0u);
    CHECK_GE(s.reconnects, 4u);
    CHECK(contains(runner.feed.lastError(), "serverShutdown"));
}

TEST_CASE("feed: REST snapshot keeps failing -> retried with backoff, session ends after rest_max_attempts") {
    MockExchangeServer server;
    server.setHttpHandler([](const HttpRequestInfo&) {
        return HttpReply{500, R"({"code":-1001,"msg":"Internal error; unable to process your request."})",
                         "application/json", 0};
    });
    server.setWebSocketScript([](WsSession& ws) { ws.send(binanceDiff(100, 110, {{"65000.00", "1"}}, {})); });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.rest_host = "127.0.0.1";
    cfg.rest_port = server.portString();
    cfg.snapshot_path = kBinanceSnapshotPath;
    cfg.rest_max_attempts = 3;
    cfg.rest_min_interval_ms = 20;
    const ConnectTrace trace(server);
    FeedRunner runner(cfg);
    REQUIRE(waitFor([&] { return server.wsSessionCount() >= 2; }, 5000));
    const std::vector<HttpRequestInfo> reqs = server.httpRequests();
    REQUIRE(reqs.size() >= 3);
    CHECK_GE(trace.requestGapMs(reqs, 1), 20 * 0.8 - 2.0);  // 第 1 次重试：20 ms ±20%
    CHECK_GE(trace.requestGapMs(reqs, 2), 40 * 0.8 - 2.0);  // 连续失败 2 次：间隔加倍，40 ms ±20%
    // 第一个会话恰好请求了 rest_max_attempts 次就放弃并重连
    const std::vector<std::int64_t> sessions = server.wsSessionTimesNs();
    REQUIRE(sessions.size() >= 2);
    std::size_t first_session_requests = 0;
    for (const HttpRequestInfo& r : reqs) {
        if (r.time_ns > sessions[0] && r.time_ns < sessions[1]) ++first_session_requests;
    }
    CHECK_EQ(first_session_requests, std::size_t{3});
    runner.stop();
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connect_failures, 0u);
    CHECK_GE(s.reconnects, 1u);
    CHECK_EQ(s.rest_snapshots, 0u);
    CHECK_EQ(s.parse_errors, 0u);  // 非 200 不交给适配器解析
    const std::string err = runner.feed.lastError();
    CHECK(contains(err, "HTTP 500"));
    CHECK(contains(err, "REST snapshot request limit reached (rest_max_attempts 3)"));
}

namespace {

// Binance 增量深度：第 k 个 REST 请求返回 lastUpdateId = last_update_ids[k] 的快照（之后的请求用最后一个）
mock_exchange::HttpHandler snapshotsBridging(std::vector<std::uint64_t> last_update_ids) {
    auto ids = std::make_shared<const std::vector<std::uint64_t>>(std::move(last_update_ids));
    return [ids](const HttpRequestInfo& info) {
        const std::uint64_t id = (*ids)[std::min(info.index, ids->size() - 1)];
        return HttpReply{200, binanceDepth(id, {{"65000.00", "1.0"}}, {{"65001.00", "1.0"}})};
    };
}

// 第一个 WebSocket 会话期间到达的 REST 请求
std::vector<HttpRequestInfo> firstSessionRequests(const MockExchangeServer& server) {
    const std::vector<std::int64_t> sessions = server.wsSessionTimesNs();
    std::vector<HttpRequestInfo> out;
    for (const HttpRequestInfo& r : server.httpRequests()) {
        if (r.time_ns > sessions.at(0) && (sessions.size() < 2 || r.time_ns < sessions[1])) out.push_back(r);
    }
    return out;
}

}  // namespace

TEST_CASE("feed: rest_max_attempts bounds REST requests per session even when every resync succeeds; requests are spaced") {
    // 上游每个事件都有缺口：每次 REST 快照都能同步成功，但下一个事件又触发一轮重同步
    MockExchangeServer server;
    std::vector<std::uint64_t> ids;
    for (std::uint64_t k = 0; k < 12; ++k) ids.push_back(100 * (k + 1) + 5);
    server.setHttpHandler(snapshotsBridging(ids));
    server.setWebSocketScript([](WsSession& ws) {
        for (std::uint64_t k = 0; k < 12; ++k) {
            const std::uint64_t base = 100 * (k + 1);
            ws.send(binanceDiff(base + 1, base + 10, {{"65000.00", "1.5"}}, {}));
            ws.sleepMs(30);
        }
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth@100ms";
    cfg.rest_host = "127.0.0.1";
    cfg.rest_port = server.portString();
    cfg.snapshot_path = kBinanceSnapshotPath;
    cfg.rest_max_attempts = 2;
    cfg.rest_min_interval_ms = 150;
    cfg.reconnect_initial_ms = 100;
    cfg.reconnect_max_ms = 100;
    EventLog events;
    const ConnectTrace trace(server);
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor([&] { return server.wsSessionCount() >= 2; }, 5000));
    const std::vector<HttpRequestInfo> reqs = firstSessionRequests(server);
    REQUIRE_EQ(reqs.size(), std::size_t{2});  // 之前：一个会话里每个缺口一次，没有上限
    CHECK_GE(trace.requestGapMs(reqs, 1), 150 * 0.8 - 2.0);  // 事件每 30 ms 一个，请求却至少相隔 ~150 ms
    CHECK(contains(runner.feed.lastError(), "REST snapshot request limit reached (rest_max_attempts 2), reconnecting"));
    CHECK(contains(runner.feed.lastError(), "gap"));
    CHECK(events.inOrder({"[Binance] book synced (LIVE)", "[Binance] REST snapshot request limit reached",
                          "[Binance] reconnecting in "}));
    const FeedStats s = runner.feed.stats();
    CHECK_GE(s.rest_snapshots, 2u);
    CHECK_EQ(s.connect_failures, 0u);
}

TEST_CASE("feed: the per-session REST budget refills once the book has stayed in sync for healthy_session_ms") {
    // rest_max_attempts = 2：同步（请求 0）→ 连续 400 ms 没有缺口（> healthy 200 ms，额度恢复）→ 缺口（请求 1）
    // → 缺口（请求 2，额度恢复后的第 2 次）→ 缺口：额度用完，重连。不恢复的话第一个会话只有 2 次请求。
    MockExchangeServer server;
    server.setHttpHandler(snapshotsBridging({105, 1005, 2005}));
    server.setWebSocketScript([](WsSession& ws) {
        ws.send(binanceDiff(101, 110, {{"65000.00", "1.5"}}, {}));
        for (std::uint64_t j = 0; j < 20; ++j) {  // 不间断的事件：111..120, 121..130, ...
            ws.sleepMs(20);
            ws.send(binanceDiff(111 + 10 * j, 120 + 10 * j, {{"65000.00", std::to_string(2 + j)}}, {}));
        }
        for (std::uint64_t g = 1; g <= 3; ++g) {  // 三个缺口
            ws.sleepMs(100);
            ws.send(binanceDiff(1000 * g + 1, 1000 * g + 10, {{"65000.00", "1.0"}}, {}));
        }
    });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    cfg.path = "/ws/btcusdt@depth@100ms";
    cfg.rest_host = "127.0.0.1";
    cfg.rest_port = server.portString();
    cfg.snapshot_path = kBinanceSnapshotPath;
    cfg.rest_max_attempts = 2;
    cfg.rest_min_interval_ms = 50;
    cfg.healthy_session_ms = 200;
    FeedRunner runner(cfg);
    REQUIRE(waitFor([&] { return server.wsSessionCount() >= 2; }, 5000));
    const std::vector<HttpRequestInfo> reqs = firstSessionRequests(server);
    CHECK_EQ(reqs.size(), std::size_t{3});
    CHECK(contains(runner.feed.lastError(), "REST snapshot request limit reached (rest_max_attempts 2)"));
    CHECK_GE(runner.feed.stats().gaps, 3u);
}

TEST_CASE("feed: connect timeout kicks a server that accepts TCP but never completes the TLS handshake") {
    MockExchangeServer server;
    server.setAcceptHandler([](std::size_t i) { return i == 0 ? AcceptAction::Silent : AcceptAction::Serve; });
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.connect_timeout_ms = 300;
    EventLog events;
    FeedRunner runner(cfg, events.callback());
    REQUIRE(waitFor(
        [&] {
            runner.feed.checkHealth(hft::nowNs());  // 模拟主线程每次刷新调用
            return runner.feed.status() == FeedStatus::Live;
        },
        5000, 10));
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.connect_failures, 1u);
    CHECK_EQ(s.stalls, 0u);
    CHECK_EQ(s.reconnects, 1u);
    CHECK_EQ(server.acceptCount(), std::size_t{2});
    CHECK(contains(runner.feed.lastError(), "connect timeout: TLS handshake did not complete within 300 ms"));
    // 客户端时间线：第一次 "connecting" 日志（早于连接计时起点）到超时错误日志至少 300 ms
    std::int64_t connecting_ns = 0, timeout_ns = 0;
    for (const EventLog::Event& e : events.events()) {
        if (connecting_ns == 0 && contains(e.line, "[OKX] connecting to wss://127.0.0.1:")) connecting_ns = e.time_ns;
        if (timeout_ns == 0 && contains(e.line, "[OKX] connect timeout")) timeout_ns = e.time_ns;
    }
    REQUIRE(connecting_ns != 0);
    REQUIRE(timeout_ns != 0);
    CHECK_GE(ms(timeout_ns - connecting_ns), 300.0);
    CHECK_LT(ms(timeout_ns - connecting_ns), 3000.0);
}

TEST_CASE("feed: WebSocket upgrade rejected with HTTP 404 -> connect failure naming the status") {
    MockExchangeServer server;
    server.setUpgradeFilter([](const std::string& target) { return target == "/ws/v5/public"; });
    server.setWebSocketScript([](WsSession& ws) { ws.send(simpleBook(0)); });
    FeedConfig cfg = baseConfig(Exchange::OKX, server);
    cfg.path = "/ws/v5/privat";
    FeedRunner runner(cfg);
    REQUIRE(waitFor(
        [&] { return runner.feed.stats().connect_failures >= 1 && !runner.feed.lastError().empty(); }, 5000));
    const std::string err = runner.feed.lastError();
    CHECK(contains(err, "WebSocket upgrade to 127.0.0.1:" + server.portString() + "/ws/v5/privat rejected: HTTP 404"));
    CHECK(contains(err, "no such stream"));
    CHECK_EQ(server.wsSessionCount(), std::size_t{0});
}

TEST_CASE("feed: Binance diff stream without REST configuration ends the session with a clear error") {
    MockExchangeServer server;
    server.setWebSocketScript([](WsSession& ws) { ws.send(binanceDiff(100, 110, {{"65000.00", "1"}}, {})); });
    FeedConfig cfg = baseConfig(Exchange::BINANCE, server);
    FeedRunner runner(cfg);
    REQUIRE(waitFor(
        [&] {
            return runner.feed.stats().reconnects >= 1 && server.wsSessionCount() >= 2 &&
                   !runner.feed.lastError().empty();
        },
        5000));
    CHECK(contains(runner.feed.lastError(), "rest_host/snapshot_path are not configured"));
    CHECK_EQ(runner.feed.stats().connect_failures, 0u);
    CHECK_EQ(server.httpRequestCount(), std::size_t{0});
}

TEST_CASE("feed: steady state allocates nothing on the feed thread (TLS read, adapter, publish, ping); loopback rate") {
    MockExchangeServer server;
    constexpr int kWarmup = 300;
    constexpr int kMeasured = 3000;
    auto go = std::make_shared<std::atomic<bool>>(false);
    server.setWebSocketScript([go](WsSession& ws) {
        ws.expectText(kBybitSub);
        // 50+50 档快照；之后每条 delta 修改/删除/恢复其中一档（价格集合有界，簿永不交叉）
        Levels bids, asks;
        for (int i = 0; i < 50; ++i) {
            bids.push_back({tenths(650000 - i), "1.0"});
            asks.push_back({tenths(650001 + i), "1.0"});
        }
        std::uint64_t u = 1000;
        const std::int64_t ts = hft::wallClockMs();
        ws.send(bybitBook("snapshot", bids, asks, u, ts));
        auto delta = [&u, ts](int i) {
            const int k = i % 50;
            const std::string size = (i / 50) % 3 == 2 ? std::string("0") : std::to_string(1 + i % 7) + ".5";
            ++u;
            return bybitBook("delta", {{tenths(650000 - k), size}}, {{tenths(650001 + k), size}}, u, ts + i);
        };
        for (int i = 0; i < kWarmup; ++i) {
            ws.send(delta(i));
            if (i % 10 == 0) ws.sleepMs(1);
        }
        ws.waitUntil([go] { return go->load(); }, 20000);
        for (int i = kWarmup; i < kWarmup + kMeasured; ++i) {
            ws.send(delta(i));
            if (i % 100 == 0) ws.sleepMs(1);  // 让心跳（5 ms）在测量区间内也会发出
        }
    });
    FeedConfig cfg = baseConfig(Exchange::BYBIT, server);
    cfg.ping_interval_ms = 5;
    FeedRunner runner(cfg);
    SnapshotLog log;
    REQUIRE(waitFor(
        [&] {
            log.pump(runner.feed);
            return runner.feed.stats().messages >= static_cast<std::uint64_t>(1 + kWarmup);
        },
        20000));
    const std::size_t pings_before = server.framesReceived(0).size();
    g_alloc_count.store(0);
    g_alloc_thread.store(static_cast<std::uint64_t>(runner.thread.native_handle()));
    const std::int64_t t0 = hft::nowNs();
    go->store(true);
    REQUIRE(waitFor(
        [&] {
            log.pump(runner.feed);
            return runner.feed.stats().messages >= static_cast<std::uint64_t>(1 + kWarmup + kMeasured);
        },
        30000));
    const std::int64_t t1 = hft::nowNs();
    g_alloc_thread.store(0);
    const std::uint64_t allocs = g_alloc_count.load();
    const std::size_t pings_during = server.framesReceived(0).size() - pings_before;
    CHECK_EQ(allocs, 0u);
    CHECK_GT(pings_during, std::size_t{0});  // 测量区间内确实发送过心跳
    const FeedStats s = runner.feed.stats();
    CHECK_EQ(s.gaps, 0u);
    CHECK_EQ(s.parse_errors, 0u);
    CHECK_EQ(runner.feed.status(), FeedStatus::Live);
    const LatencyTracker& lat = runner.feed.getLatencyTracker();
    std::printf("    loopback TLS, Bybit deltas: %d msgs in %.1f ms (%.0f msg/s, server-paced), "
                "read->publish p50 %.2f us p99 %.2f us max %.2f us, %zu pings, %llu feed-thread allocations\n",
                kMeasured, ms(t1 - t0), kMeasured / (ms(t1 - t0) / 1000.0), lat.getP50Us(), lat.getP99Us(),
                static_cast<double>(lat.maxNs()) / 1000.0, pings_during, static_cast<unsigned long long>(allocs));
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
