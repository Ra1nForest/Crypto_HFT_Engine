//
// https_client.h
// 阻塞式 TLS 客户端工具：可被其他线程"踢断"的套接字登记表、可中断的 DNS 解析、证书与主机名校验、HTTPS GET。
//

#pragma once

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace hft::net {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

// HTTP / WebSocket 握手使用的 User-Agent
inline constexpr const char* kUserAgent = "crypto_hft_engine/2.0";

// 踢断原因：None = 未踢断；Stop = 永久停止；Stall / ConnectTimeout = 看门狗踢断当前会话
enum class KickReason : int { None = 0, Stop = 1, Stall = 2, ConnectTimeout = 3 };

// ---------------------------------------------------------------------------
// SocketRegistry：活动套接字登记表（"踢断"机制）
// ---------------------------------------------------------------------------
// 同步 Boost.Beast 的阻塞调用（connect / TLS 握手 / read / write）没有超时也无法取消。其他线程调用 kick()
// 时，对登记的每个套接字执行 ::shutdown(SHUT_RDWR)：阻塞在其上的系统调用立即返回（connect 以
// ECONNRESET 失败，recv 返回 EOF），会话随之结束。
// fd 复用竞争：登记时保存的是 fd 的副本（fcntl(F_DUPFD_CLOEXEC)）。shutdown 作用于套接字本身，
// 对副本调用与对原 fd 调用效果完全相同；副本在注销前一直打开，它的编号不可能被别的套接字复用，
// 因此即使原 fd 已被关闭（Beast 在收到 close 帧或协议错误时会在 teardown 里自行关闭套接字），踢断也
// 不会误伤别的连接。使用方仍然遵守"先注销、再关闭"（SocketRegistration 声明在流对象之后）。
// 副本让底层套接字在注销前保持存活：注销（关闭副本）后才真正释放连接。
// 注意：副本会让 epoll 中原 fd 的登记在原 fd 关闭后残留到副本关闭为止，所以本工具只用于从不 run()
// 的、纯同步使用的 io_context（feed 正是如此）。
// 线程安全：全部方法可在任意线程调用（内部互斥锁）；reason()/interrupted() 无锁。
class SocketRegistry {
public:
    using Token = std::uint64_t;                    // 0 = 无效
    static constexpr std::size_t kMaxSockets = 8;   // feed 同时最多 2 个（WebSocket + REST）

    SocketRegistry() = default;
    SocketRegistry(const SocketRegistry&) = delete;
    SocketRegistry& operator=(const SocketRegistry&) = delete;

    ~SocketRegistry() {
        for (std::size_t i = 0; i < count_; ++i) ::close(entries_[i].kick_fd);
    }

    // 登记套接字 fd，返回注销用的令牌。已被踢断/已关闭时立即 shutdown 该套接字并返回 0
    // （调用方应放弃本次操作）；登记表已满或复制 fd 失败时也返回 0。
    Token add(int fd) noexcept {
        if (fd < 0) return 0;
        const int kick_fd = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (kick_fd < 0) return 0;
        std::lock_guard<std::mutex> lock(mutex_);
        if (reason_.load(std::memory_order_relaxed) != KickReason::None || count_ == kMaxSockets) {
            ::shutdown(kick_fd, SHUT_RDWR);
            ::close(kick_fd);
            return 0;
        }
        const Token token = next_token_++;
        entries_[count_++] = Entry{token, kick_fd};
        return token;
    }

    // 注销（关闭 fd 副本）。必须在关闭/销毁原套接字之前调用。
    void remove(Token token) noexcept {
        if (token == 0) return;
        std::lock_guard<std::mutex> lock(mutex_);
        for (std::size_t i = 0; i < count_; ++i) {
            if (entries_[i].token == token) {
                ::close(entries_[i].kick_fd);
                entries_[i] = entries_[--count_];
                return;
            }
        }
    }

    // 踢断当前会话：记录原因并 shutdown 全部已登记套接字；之后的 add() 一律失败，直到 rearm()。
    // 已处于踢断状态时什么也不做并返回 false（同一会话不会被重复计数）。
    bool kick(KickReason why) noexcept {
        if (why == KickReason::None) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        if (reason_.load(std::memory_order_relaxed) != KickReason::None) return false;
        reason_.store(why, std::memory_order_release);
        shutdownAllLocked();
        return true;
    }

    // 永久关闭（stop）：原因改为 Stop（覆盖看门狗的原因），shutdown 全部套接字，之后的 add() 永远失败
    void close() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        reason_.store(KickReason::Stop, std::memory_order_release);
        shutdownAllLocked();
    }

    // 新会话开始前重新武装（清除看门狗的踢断原因；已 close() 时无效）
    void rearm() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!closed_) reason_.store(KickReason::None, std::memory_order_release);
    }

    KickReason reason() const noexcept { return reason_.load(std::memory_order_acquire); }
    bool interrupted() const noexcept { return reason() != KickReason::None; }

    // 当前登记的套接字数（测试用）
    std::size_t size() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return count_;
    }

private:
    struct Entry {
        Token token;
        int kick_fd;  // 原 fd 的副本
    };

    void shutdownAllLocked() noexcept {
        for (std::size_t i = 0; i < count_; ++i) ::shutdown(entries_[i].kick_fd, SHUT_RDWR);
    }

    mutable std::mutex mutex_;
    Entry entries_[kMaxSockets] = {};
    std::size_t count_ = 0;
    Token next_token_ = 1;
    bool closed_ = false;
    std::atomic<KickReason> reason_{KickReason::None};
};

// RAII 登记：析构（或 release()）时注销。必须声明在所登记的流/套接字对象之后，保证先注销、后关闭。
class SocketRegistration {
public:
    SocketRegistration() = default;
    ~SocketRegistration() { release(); }
    SocketRegistration(const SocketRegistration&) = delete;
    SocketRegistration& operator=(const SocketRegistration&) = delete;

    // 登记 fd；失败（已踢断等）时返回 false
    bool attach(SocketRegistry& registry, int fd) noexcept {
        release();
        const SocketRegistry::Token token = registry.add(fd);
        if (token == 0) return false;
        registry_ = &registry;
        token_ = token;
        return true;
    }

    void release() noexcept {
        if (registry_ != nullptr) {
            registry_->remove(token_);
            registry_ = nullptr;
            token_ = 0;
        }
    }

    bool attached() const noexcept { return registry_ != nullptr; }

private:
    SocketRegistry* registry_ = nullptr;
    SocketRegistry::Token token_ = 0;
};

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

inline std::string errorText(const boost::system::error_code& ec) { return ec.message(); }

// 主机名是否为 IP 字面量（IP 字面量不发送 SNI，按 IP SAN 校验证书）
inline bool isIpLiteral(const std::string& host) noexcept {
    boost::system::error_code ec;
    asio::ip::make_address(host, ec);
    return !ec;
}

// HTTP Host 头：IPv6 字面量加方括号；端口等于 default_port 时省略
inline std::string hostHeader(const std::string& host, const std::string& port, const char* default_port = nullptr) {
    std::string h = host.find(':') != std::string::npos ? "[" + host + "]" : host;
    if (default_port == nullptr || port != default_port) h += ":" + port;
    return h;
}

// 先注销再关闭套接字
inline void closeSocket(tcp::socket& sock, SocketRegistration& reg) noexcept {
    reg.release();
    boost::system::error_code ignored;
    sock.close(ignored);
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

struct TlsOptions {
    bool verify = true;      // 校验服务器证书链与主机名
    std::string ca_file;     // 额外信任的 CA（PEM，测试用）；"" = 只用系统默认 CA
};

// 创建客户端 TLS 上下文（TLS 1.2+）。verify 时加载系统默认 CA 路径与可选的 ca_file。
// 失败（如 CA 文件不存在）抛出带清晰原因的 std::runtime_error。
inline std::unique_ptr<ssl::context> makeClientContext(const TlsOptions& opt) {
    auto ctx = std::make_unique<ssl::context>(ssl::context::tls_client);
    if (SSL_CTX_set_min_proto_version(ctx->native_handle(), TLS1_2_VERSION) != 1) {
        throw std::runtime_error("TLS setup: cannot require TLS 1.2 or newer");
    }
    if (!opt.verify) {
        ctx->set_verify_mode(ssl::verify_none);
        return ctx;
    }
    boost::system::error_code ec;
    ctx->set_default_verify_paths(ec);
    if (ec) throw std::runtime_error("TLS setup: cannot load the system CA store: " + errorText(ec));
    if (!opt.ca_file.empty()) {
        ctx->load_verify_file(opt.ca_file, ec);
        if (ec) throw std::runtime_error("TLS setup: cannot load CA file '" + opt.ca_file + "': " + errorText(ec));
    }
    ctx->set_verify_mode(ssl::verify_peer);
    return ctx;
}

// 证书校验回调：委托给 host_name_verification（叶子证书的 DNS/IP SAN 与 host 比对）。
// 主机名不符时 host_name_verification 只返回 false、不设置错误码，握手失败后 SSL_get_verify_result
// 会是 X509_V_OK；这里补上 X509_V_ERR_HOSTNAME_MISMATCH / X509_V_ERR_IP_ADDRESS_MISMATCH，
// 错误信息才能写明 "hostname mismatch" / "IP address mismatch"。
class VerifyHost {
public:
    explicit VerifyHost(const std::string& host)
        : check_(host),
          mismatch_error_(isIpLiteral(host) ? X509_V_ERR_IP_ADDRESS_MISMATCH : X509_V_ERR_HOSTNAME_MISMATCH) {}

    bool operator()(bool preverified, ssl::verify_context& ctx) const {
        const bool ok = check_(preverified, ctx);
        if (!ok && preverified) X509_STORE_CTX_set_error(ctx.native_handle(), mismatch_error_);
        return ok;
    }

private:
    ssl::host_name_verification check_;
    int mismatch_error_;
};

// 握手前设置：SNI（host 不是 IP 字面量时）与证书/主机名校验。失败抛出 std::runtime_error。
template <class SslStream>
void prepareTlsStream(SslStream& stream, const std::string& host, bool verify) {
    if (!host.empty() && !isIpLiteral(host)) {
        if (SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()) != 1) {
            throw std::runtime_error("TLS setup: cannot set the SNI host name '" + host + "'");
        }
    }
    if (verify) {
        stream.set_verify_mode(ssl::verify_peer);
        stream.set_verify_callback(VerifyHost(host));
    } else {
        stream.set_verify_mode(ssl::verify_none);
    }
}

// TLS 握手失败的可读描述：证书校验失败时给出 OpenSSL 的校验错误（如 "self-signed certificate"、
// "unable to get local issuer certificate"、"hostname mismatch"）
inline std::string describeHandshakeError(SSL* ssl, const boost::system::error_code& ec, const std::string& host,
                                          bool verify) {
    if (verify && ssl != nullptr) {
        const long result = SSL_get_verify_result(ssl);
        if (result != X509_V_OK) {
            return "TLS certificate verification failed for " + host + ": " +
                   X509_verify_cert_error_string(result) + " (" + errorText(ec) + ")";
        }
    }
    return "TLS handshake with " + host + " failed: " + errorText(ec);
}

// ---------------------------------------------------------------------------
// Resolver：可中断的 DNS 解析
// ---------------------------------------------------------------------------
// getaddrinfo 无法取消（glibc 默认超时可达 timeout x attempts 秒），因此在分离的后台线程中执行，
// 调用方每 poll_ms 检查一次 abort()；放弃的查询在后台自行结束，只访问自己的共享状态。
// 对同一 host/port 的下一次 resolve() 会接着等待仍在进行中的查询，所以每个 Resolver 最多只有一个后台线程。
// IP 字面量直接同步解析（AI_NUMERICHOST，不访问 DNS、不创建线程）。
// 非线程安全：一个 Resolver 只由一个线程使用（feed 线程）。
class Resolver {
public:
    using results_type = tcp::resolver::results_type;

    template <class AbortFn>
    boost::system::error_code resolve(const std::string& host, const std::string& port, results_type& out,
                                      AbortFn&& abort, int poll_ms = 10) {
        boost::system::error_code ec;
        if (isIpLiteral(host)) {
            asio::io_context ioc;
            tcp::resolver resolver(ioc);
            out = resolver.resolve(host, port, tcp::resolver::numeric_host, ec);
            return ec;
        }
        if (!pending_ || pending_->host != host || pending_->port != port || isDone(*pending_)) {
            start(host, port);
        }
        const std::shared_ptr<State> st = pending_;
        std::unique_lock<std::mutex> lock(st->mutex);
        while (!st->done) {
            if (abort()) return asio::error::operation_aborted;  // pending_ 保留，下次复用
            st->cv.wait_for(lock, std::chrono::milliseconds(poll_ms > 0 ? poll_ms : 1));
        }
        out = st->results;
        ec = st->ec;
        lock.unlock();
        pending_.reset();
        return ec;
    }

private:
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::string host, port;
        boost::system::error_code ec;
        results_type results;
    };

    static bool isDone(State& st) {
        std::lock_guard<std::mutex> lock(st.mutex);
        return st.done;
    }

    void start(const std::string& host, const std::string& port) {
        auto st = std::make_shared<State>();
        st->host = host;
        st->port = port;
        std::thread([st] {
            asio::io_context ioc;
            tcp::resolver resolver(ioc);
            boost::system::error_code ec;
            results_type results = resolver.resolve(st->host, st->port, ec);
            {
                std::lock_guard<std::mutex> lock(st->mutex);
                st->ec = ec;
                st->results = std::move(results);
                st->done = true;
            }
            st->cv.notify_all();
        }).detach();
        pending_ = std::move(st);
    }

    std::shared_ptr<State> pending_;
};

// ---------------------------------------------------------------------------
// TCP 连接
// ---------------------------------------------------------------------------

// 依次尝试每个端点：先自己 open 套接字并登记（保证阻塞的 connect() 能被踢断），再 connect。
// 成功时 sock 已连接、reg 已登记；被踢断时返回 operation_aborted；全部失败时返回最后一个错误。
inline boost::system::error_code connectSocket(tcp::socket& sock, const tcp::resolver::results_type& endpoints,
                                               SocketRegistry& registry, SocketRegistration& reg,
                                               tcp::endpoint* connected = nullptr) {
    boost::system::error_code last = asio::error::host_not_found;
    for (const auto& entry : endpoints) {
        const tcp::endpoint ep = entry.endpoint();
        closeSocket(sock, reg);
        boost::system::error_code ec;
        sock.open(ep.protocol(), ec);
        if (ec) {
            last = ec;
            continue;
        }
        if (!reg.attach(registry, sock.native_handle())) {
            closeSocket(sock, reg);
            return registry.interrupted() ? asio::error::operation_aborted : asio::error::no_descriptors;
        }
        sock.connect(ep, ec);
        if (!ec) {
            if (connected != nullptr) *connected = ep;
            return ec;
        }
        last = ec;
        closeSocket(sock, reg);
        if (registry.interrupted()) return asio::error::operation_aborted;
    }
    return last;
}

// ---------------------------------------------------------------------------
// HTTPS GET
// ---------------------------------------------------------------------------

struct HttpsGetRequest {
    std::string host;
    std::string port = "443";
    std::string target = "/";
    bool verify = true;                          // 与 makeClientContext 的 verify 保持一致
    std::size_t body_limit = std::size_t{16} << 20;  // 响应体上限（默认 16 MiB）
};

struct HttpResult {
    bool ok = false;     // 收到了完整的 HTTP 响应（无论状态码）
    int status = 0;      // HTTP 状态码
    std::string reason;  // 状态行原因短语
    std::string body;
    std::string error;   // ok == false 时的可读原因
};

// 阻塞式 HTTPS GET（每次请求一个连接，Connection: close）。套接字登记到 registry，可被踢断；
// 解析可被 registry 的踢断状态中断。不抛异常（std::bad_alloc 除外）。
// ioc 必须是只做同步操作、从不 run() 的 io_context（见 SocketRegistry 的说明）。
inline HttpResult httpsGet(asio::io_context& ioc, ssl::context& ctx, Resolver& resolver, SocketRegistry& registry,
                           const HttpsGetRequest& rq) {
    HttpResult r;
    const std::string where = rq.host + ":" + rq.port;
    tcp::resolver::results_type endpoints;
    boost::system::error_code ec =
        resolver.resolve(rq.host, rq.port, endpoints, [&registry] { return registry.interrupted(); });
    if (ec) {
        r.error = "resolve " + where + ": " + errorText(ec);
        return r;
    }
    beast::ssl_stream<tcp::socket> stream(ioc, ctx);
    SocketRegistration reg;  // 声明在 stream 之后：析构时先注销、再由 stream 关闭套接字
    tcp::socket& sock = beast::get_lowest_layer(stream);
    ec = connectSocket(sock, endpoints, registry, reg);
    if (ec) {
        r.error = "connect " + where + ": " + errorText(ec);
        return r;
    }
    sock.set_option(tcp::no_delay(true), ec);
    try {
        prepareTlsStream(stream, rq.host, rq.verify);
    } catch (const std::exception& e) {
        r.error = e.what();
        return r;
    }
    stream.handshake(ssl::stream_base::client, ec);
    if (ec) {
        r.error = describeHandshakeError(stream.native_handle(), ec, rq.host, rq.verify);
        return r;
    }
    http::request<http::empty_body> req{http::verb::get, rq.target, 11};
    req.set(http::field::host, hostHeader(rq.host, rq.port, "443"));
    req.set(http::field::user_agent, kUserAgent);
    req.set(http::field::accept, "application/json");
    req.set(http::field::connection, "close");
    http::write(stream, req, ec);
    if (ec) {
        r.error = "send request to " + where + ": " + errorText(ec);
        return r;
    }
    beast::flat_buffer buffer;
    http::response_parser<http::string_body> parser;
    parser.body_limit(static_cast<std::uint64_t>(rq.body_limit));
    http::read(stream, buffer, parser, ec);
    // 服务器在完整响应之后直接断开（没有 close_notify）不算错误；响应不完整时才是截断
    if (ec == ssl::error::stream_truncated && parser.is_done()) ec = {};
    if (ec) {
        r.error = ec == http::error::body_limit
                      ? "response from " + where + " exceeds " + std::to_string(rq.body_limit) + " bytes"
                      : "read response from " + where + ": " + errorText(ec);
        return r;
    }
    auto& res = parser.get();
    r.ok = true;
    r.status = static_cast<int>(res.result_int());
    r.reason = std::string(res.reason());
    r.body = std::move(res.body());
    return r;
}

}  // namespace hft::net
