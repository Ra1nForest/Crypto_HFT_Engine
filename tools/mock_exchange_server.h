//
// mock_exchange_server.h
// 进程内模拟交易所：127.0.0.1 临时端口上的 TLS WebSocket + HTTPS 服务器（Boost.Beast），启动时用 OpenSSL 3 API 生成
// 自签名证书；每个连接一个线程，按脚本发送/接收帧、关闭、静默、应答 HTTP GET。供集成测试与独立的模拟交易所工具复用。
// In-process mock exchange: a TLS WebSocket + HTTPS server (Boost.Beast) on an ephemeral 127.0.0.1
// port that generates a self-signed certificate with the OpenSSL 3 API at startup; one thread per
// connection sends/receives frames, closes, stays silent or answers HTTP GET as scripted. Shared by
// the integration tests and the standalone mock exchange tool.
//

#pragma once

#include "hft/timing.h"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <openssl/asn1.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <poll.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

// 用法概要：
//   mock_exchange::MockExchangeServer server;                    // 绑定 127.0.0.1:<临时端口>，生成证书
// Usage summary:
//   mock_exchange::MockExchangeServer server;                    // binds 127.0.0.1:<ephemeral port>,
//                                                                // generates the certificate
//   server.setWebSocketScript([](mock_exchange::WsSession& ws) {
//       ws.expectText(R"({"op":"subscribe",...})");                // 等待客户端的订阅帧
//                                                                  // waits for the client's subscribe frame
//       ws.send(snapshot); ws.sleepMs(20); ws.send(update);
//   });                                                          // 脚本返回后保持连接、静默，直到客户端断开
//                                                                // after the script returns, the connection stays
//                                                                // open and silent until the client disconnects
//   server.setHttpHandler(MockExchangeServer::replySequence({{200, body1}, {200, body2}}));
//   cfg.port = server.portString(); cfg.ca_file = server.caFile();
// 线程模型：一个接受线程（异步 accept）+ 每个连接一个线程，各自拥有独立的 io_context。脚本在连接线程上同步执行；
// 脚本的每个阻塞调用（send / sleepMs / expectText / waitUntil ...）都在等待期间运行该连接的 io_context，
// 因此客户端发来的帧随时被接收、记录（WebSocket ping 自动应答）。异步处理器只访问 WsSession 的成员，
// 脚本因异常提前结束也不会留下悬空引用。stop()（析构）关闭监听套接字，各连接在约 10 ms 内结束并被 join。
// 脚本必须通过 WsSession 的方法等待，不要直接 sleep 线程（否则停止时要等它返回）。
// 其他线程产生了需要推送的数据时，可通过 WsSession::waker() 取得的 SessionWaker 立即唤醒正在等待的脚本。
// 长期运行的服务器（tools/mock_exchange.h）把脚本、HTTP 处理与升级过滤直接放进 ServerOptions，开始监听之前即生效。
// 所有时间戳都是 hft::nowNs()（steady_clock 纳秒），测试可以直接与自己的时间戳比较。
// Thread model: one accept thread (async accept) + one thread per connection, each with its own
// io_context. Scripts run synchronously on the connection thread; every blocking script call (send /
// sleepMs / expectText / waitUntil ...) runs that connection's io_context while it waits, so frames
// from the client are received and recorded at any time (WebSocket pings are answered automatically).
// The async handlers only access WsSession's members, so even a script that ends early with an
// exception leaves no dangling references. stop() (the destructor) closes the listening socket; every
// connection ends within about 10 ms and is joined.
// Scripts must wait through WsSession's methods, not sleep the thread directly (otherwise stopping
// has to wait for it to return).
// When another thread produces data to push, it can wake the waiting script at once through the
// SessionWaker obtained from WsSession::waker().
// Long-running servers (tools/mock_exchange.h) put the script, the HTTP handler and the upgrade filter
// straight into ServerOptions, so they take effect before listening starts.
// All timestamps are hft::nowNs() (steady_clock nanoseconds), so tests can compare them directly with
// their own timestamps.
namespace mock_exchange {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using Clock = std::chrono::steady_clock;

// 轮询等待条件成立（测试线程用）；超时返回 pred() 的最终结果
// Polls until the condition holds (for test threads); on timeout returns the final result of pred()
template <class Pred>
bool waitFor(Pred&& pred, int timeout_ms, int poll_ms = 1) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!pred()) {
        if (Clock::now() >= deadline) return pred();
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
    return true;
}

// ---------------------------------------------------------------------------
// 证书
// Certificates
// ---------------------------------------------------------------------------

struct TlsIdentity {
    std::string cert_pem;  // 自签名证书（同时充当它自己的 CA，可直接作为客户端的 ca_file）
                           // self-signed certificate (also serves as its own CA,
                           // so it can be used directly as the client's ca_file)
    std::string key_pem;   // 私钥（PKCS#8 PEM）
                           // private key (PKCS#8 PEM)
};

struct CertificateOptions {
    std::string common_name = "localhost";
    std::vector<std::string> dns_names{"localhost"};
    std::vector<std::string> ip_addresses{"127.0.0.1"};
};

namespace detail {

[[noreturn]] inline void throwOpenSsl(const char* what) {
    const unsigned long code = ERR_get_error();
    char buf[256] = "unknown error";
    if (code != 0) ERR_error_string_n(code, buf, sizeof(buf));
    ERR_clear_error();
    throw std::runtime_error(std::string("mock_exchange: ") + what + " failed: " + buf);
}

struct FreeKey {
    void operator()(EVP_PKEY* p) const noexcept { EVP_PKEY_free(p); }
};
struct FreeCert {
    void operator()(X509* p) const noexcept { X509_free(p); }
};
struct FreeBio {
    void operator()(BIO* p) const noexcept { BIO_free(p); }
};
struct FreeExt {
    void operator()(X509_EXTENSION* p) const noexcept { X509_EXTENSION_free(p); }
};

inline void addExtension(X509* cert, int nid, const std::string& value) {
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, cert, cert, nullptr, nullptr, 0);
    std::unique_ptr<X509_EXTENSION, FreeExt> ext(X509V3_EXT_nconf_nid(nullptr, &ctx, nid, value.c_str()));
    if (!ext) throwOpenSsl("X509V3_EXT_nconf_nid");
    if (X509_add_ext(cert, ext.get(), -1) != 1) throwOpenSsl("X509_add_ext");
}

inline std::string bioContents(BIO* bio) {
    char* data = nullptr;
    const long n = BIO_get_mem_data(bio, &data);
    return n > 0 ? std::string(data, static_cast<std::size_t>(n)) : std::string();
}

inline void addNameEntry(X509_NAME* name, const char* field, const std::string& value) {
    if (X509_NAME_add_entry_by_txt(name, field, MBSTRING_ASC, reinterpret_cast<const unsigned char*>(value.c_str()),
                                   -1, -1, 0) != 1) {
        throwOpenSsl("X509_NAME_add_entry_by_txt");
    }
}

// 读掉并丢弃套接字上收到的数据，直到对端关闭（EOF）、出错、到达 deadline 或 stop() 成立（每 10 ms 检查一次）
// Reads and discards the data received on the socket until the peer closes (EOF), an error occurs, the
// deadline is reached or stop() holds (checked every 10 ms)
template <class Stop>
void drainUntilPeerCloses(int fd, Clock::time_point deadline, Stop&& stop) noexcept {
    char buf[1024];
    while (fd >= 0 && Clock::now() < deadline && !stop()) {
        pollfd p{fd, POLLIN, 0};
        const int r = ::poll(&p, 1, 10);
        if (r < 0 && errno != EINTR) break;
        if (r > 0) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) break;
        }
    }
}

}  // namespace detail

// 生成 EC P-256 自签名证书：CN 与 SAN（DNS/IP）按 opt；basicConstraints CA:TRUE（自身即信任锚），
// keyUsage digitalSignature + keyCertSign，extendedKeyUsage serverAuth，有效期从 1 小时前开始、为期 7 天。
// Generates an EC P-256 self-signed certificate: CN and SAN (DNS/IP) from opt; basicConstraints CA:TRUE
// (it is its own trust anchor), keyUsage digitalSignature + keyCertSign, extendedKeyUsage serverAuth,
// valid for 7 days starting 1 hour ago.
inline TlsIdentity generateSelfSigned(const CertificateOptions& opt = CertificateOptions()) {
    std::unique_ptr<EVP_PKEY, detail::FreeKey> key(EVP_EC_gen("P-256"));
    if (!key) detail::throwOpenSsl("EVP_EC_gen(P-256)");
    std::unique_ptr<X509, detail::FreeCert> cert(X509_new());
    if (!cert) detail::throwOpenSsl("X509_new");
    if (X509_set_version(cert.get(), 2) != 1) detail::throwOpenSsl("X509_set_version");  // X.509 v3
    std::uint64_t serial = 0;
    if (RAND_bytes(reinterpret_cast<unsigned char*>(&serial), sizeof(serial)) != 1) detail::throwOpenSsl("RAND_bytes");
    serial &= 0x7fffffffffffffffULL;
    if (serial == 0) serial = 1;
    if (ASN1_INTEGER_set_uint64(X509_get_serialNumber(cert.get()), serial) != 1) {
        detail::throwOpenSsl("ASN1_INTEGER_set_uint64");
    }
    if (X509_gmtime_adj(X509_getm_notBefore(cert.get()), -3600) == nullptr ||
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 7 * 86400L) == nullptr) {
        detail::throwOpenSsl("X509_gmtime_adj");
    }
    if (X509_set_pubkey(cert.get(), key.get()) != 1) detail::throwOpenSsl("X509_set_pubkey");
    X509_NAME* name = X509_get_subject_name(cert.get());
    detail::addNameEntry(name, "O", "crypto_hft_engine mock exchange");
    detail::addNameEntry(name, "CN", opt.common_name);
    if (X509_set_issuer_name(cert.get(), name) != 1) detail::throwOpenSsl("X509_set_issuer_name");
    detail::addExtension(cert.get(), NID_basic_constraints, "critical,CA:TRUE");
    detail::addExtension(cert.get(), NID_key_usage, "critical,digitalSignature,keyCertSign");
    detail::addExtension(cert.get(), NID_ext_key_usage, "serverAuth");
    detail::addExtension(cert.get(), NID_subject_key_identifier, "hash");
    // 自签名：AKID = 自身的 SKID（同名的其他 CA 会因密钥标识不同而被排除，不会被误当作签发者）
    // Self-signed: AKID = its own SKID (other CAs with the same name are excluded by their different key
    // identifiers and are not mistaken for the issuer)
    detail::addExtension(cert.get(), NID_authority_key_identifier, "keyid:always");
    std::string san;
    for (const std::string& d : opt.dns_names) san += (san.empty() ? "DNS:" : ",DNS:") + d;
    for (const std::string& ip : opt.ip_addresses) san += (san.empty() ? "IP:" : ",IP:") + ip;
    if (!san.empty()) detail::addExtension(cert.get(), NID_subject_alt_name, san);
    if (X509_sign(cert.get(), key.get(), EVP_sha256()) <= 0) detail::throwOpenSsl("X509_sign");

    std::unique_ptr<BIO, detail::FreeBio> cert_bio(BIO_new(BIO_s_mem()));
    std::unique_ptr<BIO, detail::FreeBio> key_bio(BIO_new(BIO_s_mem()));
    if (!cert_bio || !key_bio) detail::throwOpenSsl("BIO_new");
    if (PEM_write_bio_X509(cert_bio.get(), cert.get()) != 1) detail::throwOpenSsl("PEM_write_bio_X509");
    if (PEM_write_bio_PrivateKey(key_bio.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr) != 1) {
        detail::throwOpenSsl("PEM_write_bio_PrivateKey");
    }
    return TlsIdentity{detail::bioContents(cert_bio.get()), detail::bioContents(key_bio.get())};
}

// 临时文件（mkstemps 创建，析构时删除）：用于把证书写成客户端的 ca_file
// Temporary file (created with mkstemps, deleted on destruction): used to write the certificate out as
// the client's ca_file
class TempFile {
public:
    TempFile(const std::string& contents, const std::string& prefix, const std::string& suffix = ".pem") {
        const std::string pattern =
            (std::filesystem::temp_directory_path() / (prefix + "_XXXXXX" + suffix)).string();
        std::vector<char> buf(pattern.begin(), pattern.end());
        buf.push_back('\0');
        const int fd = ::mkstemps(buf.data(), static_cast<int>(suffix.size()));
        if (fd < 0) throw std::runtime_error("mock_exchange: cannot create a temp file from " + pattern);
        const char* p = contents.data();
        std::size_t left = contents.size();
        while (left > 0) {
            const ssize_t n = ::write(fd, p, left);
            if (n < 0) {
                if (errno == EINTR) continue;
                ::close(fd);
                ::unlink(buf.data());
                throw std::runtime_error("mock_exchange: cannot write temp file " + std::string(buf.data()));
            }
            p += n;
            left -= static_cast<std::size_t>(n);
        }
        ::close(fd);
        path_ = buf.data();
    }

    ~TempFile() { ::unlink(path_.c_str()); }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;

    const std::string& path() const noexcept { return path_; }

private:
    std::string path_;
};

// ---------------------------------------------------------------------------
// 脚本接口
// Script interface
// ---------------------------------------------------------------------------

// 连接已断开或服务器正在停止：脚本调用抛出，脚本随之结束（不算错误）
// The connection is closed or the server is stopping: script calls throw this and the script ends with
// it (not an error)
struct ConnectionClosed : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// 脚本的期望没有满足（如等待某帧超时）：记录到 MockExchangeServer::errors()
// A script's expectation was not met (e.g. timed out waiting for a frame): recorded in
// MockExchangeServer::errors()
struct ScriptError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// 新 TCP 连接的处理方式（setAcceptHandler）
// How a new TCP connection is handled (setAcceptHandler)
enum class AcceptAction {
    Serve,             // 正常：TLS 握手 → HTTP 请求 → WebSocket 脚本或 HTTP 应答
                       // normal: TLS handshake → HTTP request → WebSocket script or HTTP reply
    CloseImmediately,  // 接受后立刻关闭（客户端的 TLS 握手失败）
                       // close right after accepting (the client's TLS handshake fails)
    Silent,            // 接受后什么也不做（不进行 TLS 握手），直到客户端断开
                       // do nothing after accepting (no TLS handshake) until the client disconnects
};

struct HttpRequestInfo {
    std::size_t index = 0;       // 本服务器收到的第几个 HTTP 请求（从 0 开始，不含 WebSocket 升级）
                                 // index of the request among the HTTP requests this server received
                                 // (from 0, excluding WebSocket upgrades)
    std::size_t connection = 0;  // 所在 TCP 连接的编号
                                 // number of the TCP connection it arrived on
    std::string method;
    std::string target;
    std::string host;
    std::string user_agent;
    std::int64_t time_ns = 0;    // 收到请求的 hft::nowNs()
                                 // hft::nowNs() when the request was received
};

struct HttpReply {
    int status = 200;
    std::string body;
    std::string content_type = "application/json";
    int delay_ms = 0;            // 应答前等待（模拟慢服务器）
                                 // wait before replying (simulates a slow server)
};

using HttpHandler = std::function<HttpReply(const HttpRequestInfo&)>;

class WsSession;
using WsScript = std::function<void(WsSession&)>;

// TLS 握手 / 读取 HTTP 请求 / WebSocket 升级的时限
// Time limit for the TLS handshake / reading the HTTP request / the WebSocket upgrade
inline constexpr int kHandshakeTimeoutMs = 5000;

struct ServerOptions {
    std::string bind_address = "127.0.0.1";
    unsigned short port = 0;               // 0 = 临时端口
                                           // 0 = ephemeral port
    std::optional<TlsIdentity> identity;   // 未提供时按 certificate 生成
                                           // generated from certificate when not provided
    CertificateOptions certificate;
    // 初始行为：在开始监听之前就生效（长期运行的服务器不存在"已开始接受连接、行为尚未设置"的窗口）。
    // 之后仍可用 setWebSocketScript / setHttpHandler / setUpgradeFilter 修改。
    // Initial behavior: takes effect before listening starts (so a long-running server has no window where
    // "connections are already being accepted but the behavior is not set yet").
    // It can still be changed later with setWebSocketScript / setHttpHandler / setUpgradeFilter.
    WsScript script;
    HttpHandler http_handler;
    std::function<bool(const std::string&)> upgrade_filter;
};

class MockExchangeServer;

// 跨线程唤醒句柄（可复制）：wake() 可在任意线程调用，让该会话线程上正在进行的等待（waitUntil / sleepMs /
// expectText ...）立即重新检查条件，而不必等到下一个 10 ms 的轮询片。会话结束之后调用是空操作，
// 因此持有者无需与会话的生命周期同步。默认构造的句柄什么也不做。
// Cross-thread wake handle (copyable): wake() may be called from any thread and makes the wait in
// progress on that session's thread (waitUntil / sleepMs / expectText ...) re-check its condition at
// once instead of waiting for the next 10 ms poll slice. Calling it after the session has ended is a
// no-op, so holders need not synchronize with the session's lifetime. A default-constructed handle does
// nothing.
class SessionWaker {
public:
    SessionWaker() = default;

    void wake() const noexcept {
        if (!state_) return;
        std::lock_guard<std::mutex> lock(state_->mutex);
        if (state_->ioc == nullptr) return;
        try {
            asio::post(*state_->ioc, [] {});
        } catch (...) {
        }
    }

    explicit operator bool() const noexcept { return static_cast<bool>(state_); }

private:
    friend class WsSession;
    struct State {
        std::mutex mutex;
        asio::io_context* ioc = nullptr;  // 会话结束（teardown）时清空
                                          // cleared when the session ends (teardown)
    };
    explicit SessionWaker(std::shared_ptr<State> state) : state_(std::move(state)) {}

    std::shared_ptr<State> state_;
};

// 一个服务器端连接；WebSocket 升级后作为脚本的句柄。只在所属连接线程上使用（waker() 返回的句柄除外）。
// One server-side connection; after the WebSocket upgrade it serves as the script's handle. Used only
// on its own connection thread (except for the handle that waker() returns).
class WsSession {
public:
    WsSession(const WsSession&) = delete;
    WsSession& operator=(const WsSession&) = delete;
    ~WsSession() { detachWaker(); }

    std::size_t sessionIndex() const noexcept { return session_; }         // WebSocket 会话编号
                                                                           // WebSocket session number
    const std::string& target() const noexcept { return target_; }         // 升级请求的路径
                                                                           // path of the upgrade request
    MockExchangeServer& server() noexcept { return server_; }

    // 本会话的跨线程唤醒句柄（首次调用时创建；之后返回指向同一状态的句柄）
    // This session's cross-thread wake handle (created on the first call; later calls return handles to
    // the same state)
    SessionWaker waker() {
        if (!wake_state_) {
            wake_state_ = std::make_shared<SessionWaker::State>();
            wake_state_->ioc = &ioc_;
        }
        return SessionWaker(wake_state_);
    }

    // 发送一个文本帧（阻塞到写完）；连接已断开时抛出 ConnectionClosed
    // Sends a text frame (blocks until written); throws ConnectionClosed if the connection is closed
    void send(std::string_view text) {
        ensureOpen();
        wbuf_.assign(text.data(), text.size());
        runOp(write_, [this](auto handler) { ws_.async_write(asio::buffer(wbuf_), std::move(handler)); }, -1);
        if (write_.ec) {
            open_ = false;
            throw ConnectionClosed("mock_exchange: write failed: " + write_.ec.message());
        }
    }

    // 等待 ms 毫秒（期间继续接收客户端的帧）
    // Waits ms milliseconds (frames from the client are still received meanwhile)
    void sleepMs(int ms) {
        runUntil([] { return false; }, ms);
    }

    // 冻结 ms 毫秒：期间不运行 io_context（不接收帧，也不应答 WebSocket ping），模拟卡住的服务器；
    // 服务器停止时提前返回
    // Freezes for ms milliseconds: the io_context does not run meanwhile (no frames are received and
    // WebSocket pings are not answered), simulating a stuck server;
    // returns early when the server stops
    void freezeMs(int ms) {
        const auto deadline = Clock::now() + std::chrono::milliseconds(ms);
        while (Clock::now() < deadline && !stopping()) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // 等待一个满足 match 的文本帧（从上次 expect 之后的帧开始找，不匹配的帧被跳过），返回该帧。
    // 超时抛出 ScriptError（记入 errors()）；客户端断开抛出 ConnectionClosed。
    // Waits for a text frame that satisfies match (searching from the frames after the last expect,
    // skipping frames that do not match) and returns it.
    // Throws ScriptError on timeout (recorded in errors()); throws ConnectionClosed if the client
    // disconnects.
    std::string expectText(const std::function<bool(const std::string&)>& match, int timeout_ms,
                           const std::string& what) {
        bool matched = false;
        std::string found;
        runUntil(
            [&] {
                while (cursor_ < frames_.size()) {
                    const std::string& f = frames_[cursor_++];
                    if (match(f)) {
                        found = f;
                        matched = true;
                        return true;
                    }
                }
                return peer_closed_;
            },
            timeout_ms);
        if (matched) return found;
        if (peer_closed_) throw ConnectionClosed("mock_exchange: client disconnected while waiting for " + what);
        throw ScriptError("mock_exchange: session " + std::to_string(session_) + ": timed out after " +
                          std::to_string(timeout_ms) + " ms waiting for " + what);
    }

    std::string expectText(const std::string& exact, int timeout_ms = 3000) {
        return expectText([&exact](const std::string& f) { return f == exact; }, timeout_ms, "frame " + exact);
    }

    // 等待任意条件（期间继续处理连接上的 I/O）；超时返回 false
    // Waits for any condition (still handling I/O on the connection meanwhile); returns false on timeout
    bool waitUntil(const std::function<bool()>& pred, int timeout_ms) { return runUntil(pred, timeout_ms); }

    // 本会话目前收到的全部文本帧
    // All text frames this session has received so far
    const std::vector<std::string>& received() const noexcept { return frames_; }

    std::size_t countReceived(const std::string& exact) const {
        return static_cast<std::size_t>(std::count(frames_.begin(), frames_.end(), exact));
    }

    // 正常关闭：发送 close 帧并等待客户端回应（最多 2 s）
    // Normal close: sends a close frame and waits for the client's reply (at most 2 s)
    void close(std::uint16_t code = 1000, const std::string& reason = std::string()) {
        if (!open_) return;
        open_ = false;
        if (peer_closed_) return;
        close_done_ = false;
        ws_.async_close(websocket::close_reason(static_cast<websocket::close_code>(code), reason),
                        [this](error_code) { close_done_ = true; });
        runUntil([this] { return close_done_ && !reading_; }, 2000);
    }

    // 直接断开 TCP（不发 WebSocket close 帧）
    // Drops the TCP connection directly (without sending a WebSocket close frame)
    void abort() noexcept {
        open_ = false;
        error_code ec;
        beast::get_lowest_layer(ws_).shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(ws_).close(ec);
    }

    bool clientConnected() const noexcept { return open_ && !peer_closed_; }

private:
    friend class MockExchangeServer;
    using Stream = websocket::stream<beast::ssl_stream<tcp::socket>>;

    // 一个异步操作的完成状态（处理器只写这里）
    // Completion state of one async operation (handlers write only here)
    struct Op {
        bool done = false;
        error_code ec;
    };

    WsSession(MockExchangeServer& server, asio::io_context& ioc, tcp::socket&& sock, ssl::context& ctx,
              std::size_t connection)
        : server_(server), ioc_(ioc), guard_(asio::make_work_guard(ioc)), ws_(std::move(sock), ctx),
          connection_(connection) {}

    inline void serve();
    inline bool serveHttp();
    inline void serveWebSocket();
    inline bool stopping() const noexcept;
    inline void onFrame(const std::string& frame);
    inline void onPing();

    void ensureOpen() const {
        if (!open_ || peer_closed_) throw ConnectionClosed("mock_exchange: connection closed");
    }

    // 运行 io_context 直到 pred() 成立（true）或超时（false；timeout_ms < 0 不限时）；服务器停止时抛出 ConnectionClosed
    // Runs the io_context until pred() holds (true) or the timeout expires (false; timeout_ms < 0 = no
    // limit); throws ConnectionClosed when the server stops
    template <class Pred>
    bool runUntil(Pred&& pred, int timeout_ms) {
        const auto deadline =
            timeout_ms < 0 ? Clock::time_point::max() : Clock::now() + std::chrono::milliseconds(timeout_ms);
        for (;;) {
            if (pred()) return true;
            if (stopping()) throw ConnectionClosed("mock_exchange: server stopping");
            const auto now = Clock::now();
            if (now >= deadline) return false;
            const Clock::duration slice = std::chrono::milliseconds(10);
            ioc_.run_one_for(std::min(slice, deadline - now));
        }
    }

    // 用 start(handler) 发起一个异步操作，运行 io_context 直到它完成（true，结果在 op.ec）或超时（false）
    // Starts an async operation with start(handler) and runs the io_context until it completes (true,
    // result in op.ec) or times out (false)
    template <class Start>
    bool runOp(Op& op, Start&& start, int timeout_ms) {
        op.done = false;
        start([&op](error_code ec, auto&&...) {
            op.ec = ec;
            op.done = true;
        });
        return runUntil([&op] { return op.done; }, timeout_ms);
    }

    // 写一个 HTTP 应答（阻塞到写完）；返回是否写成功
    // Writes one HTTP response (blocks until fully written); returns whether the write succeeded
    bool writeResponse(unsigned status, const std::string& content_type, std::string body, bool keep_alive) {
        res_ = {};
        res_.version(req_.version());
        res_.result(status);
        res_.set(http::field::server, "mock_exchange");
        res_.set(http::field::content_type, content_type);
        res_.keep_alive(keep_alive);
        res_.body() = std::move(body);
        res_.prepare_payload();
        runOp(op_, [this](auto handler) { http::async_write(ws_.next_layer(), res_, std::move(handler)); }, -1);
        return !op_.ec;
    }

    void startRead() {
        reading_ = true;
        ws_.async_read(rbuf_, [this](error_code ec, std::size_t) {
            reading_ = false;
            if (ec) {
                peer_closed_ = true;
                return;
            }
            frames_.push_back(beast::buffers_to_string(rbuf_.cdata()));
            rbuf_.consume(rbuf_.size());
            onFrame(frames_.back());
            startRead();
        });
    }

    // 写完应答后的收尾：关闭发送方向，读到客户端 EOF（最多 timeout_ms）再关闭，避免未读数据触发 RST
    // Cleanup after writing the response: shuts down the sending direction, reads until the client's EOF
    // (at most timeout_ms) and then closes, so that unread data does not trigger an RST
    void gracefulTcpClose(int timeout_ms) noexcept {
        auto& sock = beast::get_lowest_layer(ws_);
        error_code ec;
        sock.shutdown(tcp::socket::shutdown_send, ec);
        detail::drainUntilPeerCloses(sock.native_handle(), Clock::now() + std::chrono::milliseconds(timeout_ms),
                                     [this] { return stopping(); });
        sock.close(ec);
    }

    // 唤醒句柄从此失效（之后的 wake() 为空操作）；io_context 销毁之前必须调用
    // From now on the wake handles are invalid (later wake() calls are no-ops); must be called before the
    // io_context is destroyed
    void detachWaker() noexcept {
        if (!wake_state_) return;
        std::lock_guard<std::mutex> lock(wake_state_->mutex);
        wake_state_->ioc = nullptr;
    }

    // 结束连接：关闭套接字并让所有挂起的处理器（已被取消）执行完毕
    // Ends the connection: closes the socket and lets all pending handlers (already cancelled) run to
    // completion
    void teardown() noexcept {
        detachWaker();
        open_ = false;
        error_code ec;
        beast::get_lowest_layer(ws_).shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(ws_).close(ec);
        guard_.reset();
        try {
            ioc_.restart();
            ioc_.run();
        } catch (...) {
        }
    }

    MockExchangeServer& server_;
    asio::io_context& ioc_;
    asio::executor_work_guard<asio::io_context::executor_type> guard_;  // 等待时 io_context 不会因无事可做而返回
                                                                        // while waiting, io_context does not return
                                                                        // just because it has nothing to do
    Stream ws_;
    beast::flat_buffer buffer_;                 // HTTP 请求缓冲
                                                // HTTP request buffer
    http::request<http::string_body> req_;
    http::response<http::string_body> res_;
    beast::flat_buffer rbuf_;                   // WebSocket 读缓冲
                                                // WebSocket read buffer
    std::string wbuf_;                          // 正在发送的帧
                                                // frame being sent
    std::vector<std::string> frames_;           // 收到的文本帧
                                                // received text frames
    std::size_t cursor_ = 0;                    // expectText 的读取位置
                                                // expectText's read position
    std::size_t connection_ = 0;
    std::size_t session_ = std::numeric_limits<std::size_t>::max();
    std::string target_;
    // 异步操作状态（处理器只写这些成员）
    // Async operation state (handlers write only these members)
    Op op_;                     // 握手、HTTP 读写、WebSocket 升级（依次进行）
                                // handshake, HTTP read/write, WebSocket upgrade (one after another)
    Op write_;                  // send()
    bool close_done_ = false;
    bool reading_ = false;
    bool peer_closed_ = false;  // WebSocket 读循环已结束（客户端关闭或连接出错）
                                // the WebSocket read loop has ended (client closed or connection error)
    bool open_ = false;         // WebSocket 已建立且尚未由服务器关闭
                                // WebSocket established and not yet closed by the server
    std::shared_ptr<SessionWaker::State> wake_state_;  // waker() 的共享状态（未调用时为空）
                                                       // shared state of waker() (empty until it is called)
};

// ---------------------------------------------------------------------------
// 服务器
// Server
// ---------------------------------------------------------------------------

class MockExchangeServer {
public:
    explicit MockExchangeServer(ServerOptions opt = ServerOptions())
        : identity_(opt.identity ? *opt.identity : generateSelfSigned(opt.certificate)),
          ssl_ctx_(ssl::context::tls_server),
          acceptor_(acceptor_ioc_),
          accept_socket_(acceptor_ioc_) {
        if (SSL_CTX_set_min_proto_version(ssl_ctx_.native_handle(), TLS1_2_VERSION) != 1) {
            detail::throwOpenSsl("SSL_CTX_set_min_proto_version");
        }
        ssl_ctx_.set_options(ssl::context::default_workarounds);
        ssl_ctx_.use_certificate_chain(asio::buffer(identity_.cert_pem));
        ssl_ctx_.use_private_key(asio::buffer(identity_.key_pem), ssl::context::pem);
        if (opt.script) scripts_.assign(1, std::move(opt.script));
        http_handler_ = std::move(opt.http_handler);
        upgrade_filter_ = std::move(opt.upgrade_filter);
        const tcp::endpoint ep(asio::ip::make_address(opt.bind_address), opt.port);
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen(asio::socket_base::max_listen_connections);
        protocol_ = ep.protocol();
        port_ = acceptor_.local_endpoint().port();
        doAccept();
        accept_thread_ = std::thread([this] {
            try {
                acceptor_ioc_.run();
            } catch (const std::exception& e) {
                recordError(std::string("mock_exchange: acceptor failed: ") + e.what());
            }
        });
    }

    ~MockExchangeServer() { stop(); }

    MockExchangeServer(const MockExchangeServer&) = delete;
    MockExchangeServer& operator=(const MockExchangeServer&) = delete;

    // 停止：不再接受连接；所有连接在约 10 ms 内结束（脚本调用抛出 ConnectionClosed），线程全部 join。幂等。
    // Stop: no more connections are accepted; all connections end within about 10 ms (script calls throw
    // ConnectionClosed) and all threads are joined. Idempotent.
    void stop() noexcept {
        if (stopping_.exchange(true)) return;
        asio::post(acceptor_ioc_, [this] {
            error_code ec;
            acceptor_.close(ec);
        });
        if (accept_thread_.joinable()) accept_thread_.join();
        std::vector<Worker> workers;
        withLock([&] { workers.swap(workers_); });
        for (Worker& w : workers) {
            if (w.thread.joinable()) w.thread.join();
        }
    }

    bool stopping() const noexcept { return stopping_.load(std::memory_order_acquire); }

    unsigned short port() const noexcept { return port_; }
    std::string portString() const { return std::to_string(port_); }

    // 把证书写到临时文件（首次调用时；服务器析构时删除），返回路径：客户端的 ca_file
    // Writes the certificate to a temporary file (on the first call; deleted when the server is destroyed)
    // and returns its path: the client's ca_file
    const std::string& caFile() {
        return withLock([&]() -> const std::string& {
            if (!ca_file_) ca_file_.emplace(identity_.cert_pem, "mock_exchange_ca");
            return ca_file_->path();
        });
    }

    // ---------------- 行为（任意时刻可设置；之后建立的连接/会话使用新设置）----------------
    // ---------------- Behavior (settable at any time; applies to connections/sessions opened later) ----------------

    // 所有 WebSocket 会话使用同一个脚本
    // All WebSocket sessions use the same script
    void setWebSocketScript(WsScript script) {
        withLock([&] { scripts_.assign(1, std::move(script)); });
    }

    // 第 i 个 WebSocket 会话使用 scripts[i]；超出部分重复最后一个
    // WebSocket session i uses scripts[i]; sessions beyond the end reuse the last one
    void setWebSocketScripts(std::vector<WsScript> scripts) {
        withLock([&] { scripts_ = std::move(scripts); });
    }

    // HTTP 请求处理（未设置时一律 404）
    // HTTP request handler (always 404 when not set)
    void setHttpHandler(HttpHandler handler) {
        withLock([&] { http_handler_ = std::move(handler); });
    }

    // 新 TCP 连接的处理方式（参数为连接编号；未设置时一律 Serve）
    // How new TCP connections are handled (argument: the connection number; always Serve when not set)
    void setAcceptHandler(std::function<AcceptAction(std::size_t)> handler) {
        withLock([&] { accept_handler_ = std::move(handler); });
    }

    // WebSocket 升级过滤：返回 false 时以 HTTP 404 拒绝升级（参数为请求路径）
    // WebSocket upgrade filter: returning false rejects the upgrade with HTTP 404 (argument: the request
    // path)
    void setUpgradeFilter(std::function<bool(const std::string&)> filter) {
        withLock([&] { upgrade_filter_ = std::move(filter); });
    }

    // 按请求顺序依次应答（第 i 个 HTTP 请求得到 replies[i]，超出部分重复最后一个）
    // Replies in request order (HTTP request i gets replies[i], requests beyond the end get the last one)
    static HttpHandler replySequence(std::vector<HttpReply> replies) {
        auto shared = std::make_shared<const std::vector<HttpReply>>(std::move(replies));
        return [shared](const HttpRequestInfo& info) {
            if (shared->empty()) return HttpReply{404, R"({"code":-1,"msg":"no reply configured"})", "application/json", 0};
            return (*shared)[std::min(info.index, shared->size() - 1)];
        };
    }

    // ---------------- 观测（线程安全，返回拷贝；时间均为 hft::nowNs()）----------------
    // ---------------- Observation (thread-safe, returns copies; all times are hft::nowNs()) ----------------

    std::size_t acceptCount() const { return withLock([&] { return accept_times_.size(); }); }
    std::size_t wsSessionCount() const { return withLock([&] { return ws_frames_.size(); }); }
    std::size_t httpRequestCount() const { return withLock([&] { return http_requests_.size(); }); }
    // 每个 TCP 连接被接受的时刻
    // Time at which each TCP connection was accepted
    std::vector<std::int64_t> acceptTimesNs() const { return withLock([&] { return accept_times_; }); }
    std::size_t openConnections() const {
        return withLock([&] {
            return static_cast<std::size_t>(std::count(end_times_.begin(), end_times_.end(), std::int64_t{0}));
        });
    }
    // 第 session 个 WebSocket 会话收到的文本帧
    // Text frames received by the WebSocket session with index session
    std::vector<std::string> framesReceived(std::size_t session) const { return sessionCopy(ws_frames_, session); }
    // 与 framesReceived(session) 一一对应的到达时刻
    // Arrival times, one-to-one with framesReceived(session)
    std::vector<std::int64_t> frameTimesNs(std::size_t session) const { return sessionCopy(ws_frame_times_, session); }
    // 第 session 个 WebSocket 会话收到的 WebSocket ping 控制帧的到达时刻（Beast 自动应答 pong）
    // Arrival times of the WebSocket ping control frames received by the WebSocket session with index
    // session (Beast answers with pong automatically)
    std::vector<std::int64_t> pingTimesNs(std::size_t session) const { return sessionCopy(ws_ping_times_, session); }
    std::vector<std::string> wsTargets() const { return withLock([&] { return ws_targets_; }); }
    // 每个 WebSocket 会话开始（收到升级请求）的时刻
    // Start time of each WebSocket session (when its upgrade request was received)
    std::vector<std::int64_t> wsSessionTimesNs() const { return withLock([&] { return ws_times_; }); }
    std::vector<HttpRequestInfo> httpRequests() const { return withLock([&] { return http_requests_; }); }
    // 脚本错误（期望未满足、异常）
    // Script errors (unmet expectations, exceptions)
    std::vector<std::string> errors() const { return withLock([&] { return errors_; }); }
    // TLS 握手失败（例如客户端不信任证书时发出的 alert）
    // TLS handshake failures (e.g. the alert a client sends when it does not trust the certificate)
    std::vector<std::string> tlsFailures() const { return withLock([&] { return tls_failures_; }); }

private:
    friend class WsSession;

    struct Worker {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> done;  // 连接线程函数已返回
                                                  // the connection thread function has returned
    };

    // 在 mutex_ 内执行 f 并返回其结果（处理器、脚本等回调一律在锁外调用：它们可以调用观测方法）
    // Runs f under mutex_ and returns its result (handlers, scripts and other callbacks are always called
    // outside the lock: they may call the observation methods)
    template <class F>
    auto withLock(F&& f) const -> decltype(f()) {
        std::lock_guard<std::mutex> lock(mutex_);
        return f();
    }

    template <class T>
    std::vector<T> sessionCopy(const std::vector<std::vector<T>>& per_session, std::size_t session) const {
        return withLock([&] { return session < per_session.size() ? per_session[session] : std::vector<T>(); });
    }

    void doAccept() {
        acceptor_.async_accept(accept_socket_, [this](error_code ec) {
            if (ec) {
                if (!stopping() && ec != asio::error::operation_aborted) doAccept();
                return;
            }
            error_code rec;
            const int fd = accept_socket_.release(rec);  // 所有权交给连接线程（它有自己的 io_context）
                                                         // ownership passes to the connection thread
                                                         // (which has its own io_context)
            if (rec) {
                accept_socket_.close(rec);
                doAccept();
                return;
            }
            std::vector<std::thread> finished;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                accept_times_.push_back(hft::nowNs());
                end_times_.push_back(0);
                const std::size_t index = accept_times_.size() - 1;
                if (stopping()) {
                    ::close(fd);
                    end_times_[index] = hft::nowNs();
                    return;
                }
                // 回收已结束的连接线程（长时间运行的模拟交易所不会积累未 join 的线程）
                // Reap the connection threads that have finished (so a long-running mock exchange does not
                // accumulate unjoined threads)
                for (auto it = workers_.begin(); it != workers_.end();) {
                    if (it->done->load(std::memory_order_acquire)) {
                        finished.push_back(std::move(it->thread));
                        it = workers_.erase(it);
                    } else {
                        ++it;
                    }
                }
                auto done = std::make_shared<std::atomic<bool>>(false);
                workers_.push_back(Worker{std::thread([this, index, fd, done] {
                                              runConnection(index, fd);
                                              done->store(true, std::memory_order_release);
                                          }),
                                          done});
            }
            for (std::thread& t : finished) t.join();  // 已经结束，立即返回
                                                       // already finished, returns immediately
            doAccept();
        });
    }

    void runConnection(std::size_t index, int fd) noexcept {
        try {
            asio::io_context ioc;
            tcp::socket sock(ioc);
            error_code ec;
            sock.assign(protocol_, fd, ec);
            if (ec) {
                ::close(fd);
            } else if (const AcceptAction action = acceptAction(index); action == AcceptAction::CloseImmediately) {
                sock.close(ec);
            } else if (action == AcceptAction::Silent) {  // 不做 TLS 握手，等待客户端断开（或服务器停止）
                                                          // no TLS handshake, wait for the client to disconnect
                                                          // (or for the server to stop)
                detail::drainUntilPeerCloses(sock.native_handle(), Clock::time_point::max(),
                                             [this] { return stopping(); });
                sock.close(ec);
            } else {
                WsSession session(*this, ioc, std::move(sock), ssl_ctx_, index);
                try {
                    session.serve();
                } catch (const ConnectionClosed&) {
                } catch (const std::exception& e) {
                    recordError(e.what());
                } catch (...) {
                    recordError("mock_exchange: connection " + std::to_string(index) + ": unknown exception");
                }
                session.teardown();
            }
        } catch (const std::exception& e) {
            recordError(std::string("mock_exchange: connection ") + std::to_string(index) + ": " + e.what());
        } catch (...) {
            recordError("mock_exchange: connection " + std::to_string(index) + ": unknown exception");
        }
        withLock([&] { end_times_[index] = hft::nowNs(); });
    }

    std::size_t registerWsSession(const std::string& target) {
        return withLock([&] {
            ws_frames_.emplace_back();
            ws_frame_times_.emplace_back();
            ws_ping_times_.emplace_back();
            ws_targets_.push_back(target);
            ws_times_.push_back(hft::nowNs());
            return ws_frames_.size() - 1;
        });
    }

    void recordFrame(std::size_t session, const std::string& frame) {
        withLock([&] {
            ws_frames_[session].push_back(frame);
            ws_frame_times_[session].push_back(hft::nowNs());
        });
    }

    void recordPing(std::size_t session) {
        withLock([&] { ws_ping_times_[session].push_back(hft::nowNs()); });
    }

    HttpRequestInfo recordHttpRequest(std::size_t connection, const http::request<http::string_body>& req) {
        HttpRequestInfo info;
        info.connection = connection;
        info.method = std::string(req.method_string());
        info.target = std::string(req.target());
        info.host = std::string(req[http::field::host]);
        info.user_agent = std::string(req[http::field::user_agent]);
        info.time_ns = hft::nowNs();
        return withLock([&] {
            info.index = http_requests_.size();
            http_requests_.push_back(info);
            return info;
        });
    }

    void recordError(const std::string& e) {
        withLock([&] { errors_.push_back(e); });
    }

    void recordTlsFailure(std::size_t connection, const std::string& e) {
        withLock([&] { tls_failures_.push_back("connection " + std::to_string(connection) + ": " + e); });
    }

    WsScript scriptFor(std::size_t session) const {
        return withLock([&] { return scripts_.empty() ? WsScript() : scripts_[std::min(session, scripts_.size() - 1)]; });
    }

    HttpReply httpReply(const HttpRequestInfo& info) const {
        const HttpHandler handler = withLock([&] { return http_handler_; });
        if (!handler) return HttpReply{404, R"({"code":-1,"msg":"not found"})", "application/json", 0};
        return handler(info);
    }

    AcceptAction acceptAction(std::size_t index) const {
        const auto handler = withLock([&] { return accept_handler_; });
        return handler ? handler(index) : AcceptAction::Serve;
    }

    bool acceptUpgrade(const std::string& target) const {
        const auto filter = withLock([&] { return upgrade_filter_; });
        return !filter || filter(target);
    }

    TlsIdentity identity_;
    ssl::context ssl_ctx_;                     // 各连接线程共享（OpenSSL 的 SSL_CTX 可并发创建 SSL）
                                               // shared by all connection threads (an OpenSSL SSL_CTX
                                               // can create SSL objects concurrently)
    asio::io_context acceptor_ioc_;
    tcp::acceptor acceptor_;
    tcp::socket accept_socket_;
    tcp protocol_ = tcp::v4();
    unsigned short port_ = 0;
    std::atomic<bool> stopping_{false};
    std::thread accept_thread_;

    mutable std::mutex mutex_;                 // 保护以下全部成员
                                               // guards all the members below
    std::vector<Worker> workers_;
    std::optional<TempFile> ca_file_;
    std::vector<WsScript> scripts_;
    HttpHandler http_handler_;
    std::function<AcceptAction(std::size_t)> accept_handler_;
    std::function<bool(const std::string&)> upgrade_filter_;
    std::vector<std::int64_t> accept_times_;
    std::vector<std::int64_t> end_times_;      // 每个 TCP 连接结束的时刻（仍在进行中为 0）
                                               // time each TCP connection ended (0 while still open)
    std::vector<std::vector<std::string>> ws_frames_;
    std::vector<std::vector<std::int64_t>> ws_frame_times_;
    std::vector<std::vector<std::int64_t>> ws_ping_times_;
    std::vector<std::string> ws_targets_;
    std::vector<std::int64_t> ws_times_;
    std::vector<HttpRequestInfo> http_requests_;
    std::vector<std::string> errors_;
    std::vector<std::string> tls_failures_;
};

// ---------------------------------------------------------------------------
// WsSession 中依赖服务器定义的部分
// The parts of WsSession that depend on the server's definition
// ---------------------------------------------------------------------------

inline bool WsSession::stopping() const noexcept { return server_.stopping(); }

inline void WsSession::onFrame(const std::string& frame) { server_.recordFrame(session_, frame); }

inline void WsSession::onPing() { server_.recordPing(session_); }

inline void WsSession::serve() {
    const bool handshake_done = runOp(
        op_, [this](auto handler) { ws_.next_layer().async_handshake(ssl::stream_base::server, std::move(handler)); },
        kHandshakeTimeoutMs);
    if (!handshake_done || op_.ec) {
        server_.recordTlsFailure(connection_, handshake_done ? op_.ec.message() : std::string("TLS handshake timed out"));
        return;
    }
    for (;;) {
        req_ = {};
        if (!runOp(op_, [this](auto handler) { http::async_read(ws_.next_layer(), buffer_, req_, std::move(handler)); },
                   kHandshakeTimeoutMs) ||
            op_.ec) {
            return;
        }
        if (websocket::is_upgrade(req_)) {
            serveWebSocket();
            return;
        }
        if (!serveHttp()) return;
    }
}

inline bool WsSession::serveHttp() {
    const HttpRequestInfo info = server_.recordHttpRequest(connection_, req_);
    HttpReply reply = server_.httpReply(info);
    if (reply.delay_ms > 0) sleepMs(reply.delay_ms);
    if (!writeResponse(static_cast<unsigned>(reply.status), reply.content_type, std::move(reply.body),
                       req_.keep_alive())) {
        return false;
    }
    if (!res_.keep_alive()) {
        gracefulTcpClose(1000);
        return false;
    }
    return true;
}

inline void WsSession::serveWebSocket() {
    target_ = std::string(req_.target());
    if (!server_.acceptUpgrade(target_)) {
        if (writeResponse(404, "text/plain", "no such stream: " + target_, false)) gracefulTcpClose(1000);
        return;
    }
    session_ = server_.registerWsSession(target_);
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res) { res.set(http::field::server, "mock_exchange"); }));
    if (!runOp(op_, [this](auto handler) { ws_.async_accept(req_, std::move(handler)); }, kHandshakeTimeoutMs) ||
        op_.ec) {
        server_.recordError("mock_exchange: session " + std::to_string(session_) + ": WebSocket accept failed: " +
                            (op_.done ? op_.ec.message() : std::string("timed out")));
        return;
    }
    open_ = true;
    ws_.text(true);
    ws_.control_callback([this](websocket::frame_type kind, beast::string_view) {
        if (kind == websocket::frame_type::ping) onPing();  // 只记录；pong 由 Beast 自动应答
                                                            // record only; Beast answers with pong automatically
    });
    startRead();
    const WsScript script = server_.scriptFor(session_);
    if (script) script(*this);
    if (clientConnected()) runUntil([this] { return peer_closed_; }, -1);  // 脚本结束：保持静默直到客户端断开
                                                                           // script finished: stay silent until the
                                                                           // client disconnects
}

}  // namespace mock_exchange
