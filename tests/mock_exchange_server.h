//
// mock_exchange_server.h
// 进程内模拟交易所：127.0.0.1 临时端口上的 TLS WebSocket + HTTPS 服务器（Boost.Beast），启动时用 OpenSSL 3 API 生成
// 自签名证书；每个连接一个线程，按脚本发送/接收帧、关闭、静默、应答 HTTP GET。供集成测试与独立的模拟交易所工具复用。
//

#pragma once

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
//   server.setWebSocketScript([](mock_exchange::WsSession& ws) {
//       ws.expectText(R"({"op":"subscribe",...})");                // 等待客户端的订阅帧
//       ws.send(snapshot); ws.sleepMs(20); ws.send(update);
//   });                                                          // 脚本返回后保持连接、静默，直到客户端断开
//   server.setHttpHandler(MockExchangeServer::replySequence({{200, body1}, {200, body2}}));
//   cfg.port = server.portString(); cfg.ca_file = server.caFile();
// 线程模型：一个接受线程（异步 accept）+ 每个连接一个线程，各自拥有独立的 io_context。脚本在连接线程上同步执行；
// 脚本的每个阻塞调用（send / sleepMs / expectText / waitUntil ...）都在等待期间运行该连接的 io_context，
// 因此客户端发来的帧随时被接收、记录（WebSocket ping 自动应答）。异步处理器只访问 WsSession 的成员，
// 脚本因异常提前结束也不会留下悬空引用。stop()（析构）关闭监听套接字，各连接在约 10 ms 内结束并被 join。
// 脚本必须通过 WsSession 的方法等待，不要直接 sleep 线程（否则停止时要等它返回）。
namespace mock_exchange {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using Clock = std::chrono::steady_clock;

// 与 hft::nowNs() 相同的时钟（steady_clock 纳秒），测试可以直接比较两边的时间戳
inline std::int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

// 轮询等待条件成立（测试线程用）；超时返回 pred() 的最终结果
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
// ---------------------------------------------------------------------------

struct TlsIdentity {
    std::string cert_pem;  // 自签名证书（同时充当它自己的 CA，可直接作为客户端的 ca_file）
    std::string key_pem;   // 私钥（PKCS#8 PEM）
};

struct CertificateOptions {
    std::string common_name = "localhost";
    std::vector<std::string> dns_names{"localhost"};
    std::vector<std::string> ip_addresses{"127.0.0.1"};
    int valid_days = 7;
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

}  // namespace detail

// 生成 EC P-256 自签名证书：CN 与 SAN（DNS/IP）按 opt；basicConstraints CA:TRUE（自身即信任锚），
// keyUsage digitalSignature + keyCertSign，extendedKeyUsage serverAuth，有效期从 1 小时前开始。
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
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), static_cast<long>(opt.valid_days) * 86400L) == nullptr) {
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
class TempFile {
public:
    TempFile() = default;

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

    ~TempFile() { reset(); }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
    TempFile(TempFile&& o) noexcept : path_(std::move(o.path_)) { o.path_.clear(); }
    TempFile& operator=(TempFile&& o) noexcept {
        if (this != &o) {
            reset();
            path_ = std::move(o.path_);
            o.path_.clear();
        }
        return *this;
    }

    void reset() noexcept {
        if (!path_.empty()) {
            ::unlink(path_.c_str());
            path_.clear();
        }
    }

    const std::string& path() const noexcept { return path_; }

private:
    std::string path_;
};

// ---------------------------------------------------------------------------
// 脚本接口
// ---------------------------------------------------------------------------

// 连接已断开或服务器正在停止：脚本调用抛出，脚本随之结束（不算错误）
struct ConnectionClosed : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// 脚本的期望没有满足（如等待某帧超时）：记录到 MockExchangeServer::errors()
struct ScriptError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// 新 TCP 连接的处理方式（setAcceptHandler）
enum class AcceptAction {
    Serve,             // 正常：TLS 握手 → HTTP 请求 → WebSocket 脚本或 HTTP 应答
    CloseImmediately,  // 接受后立刻关闭（客户端的 TLS 握手失败）
    Silent,            // 接受后什么也不做（不进行 TLS 握手），直到客户端断开
};

struct HttpRequestInfo {
    std::size_t index = 0;       // 本服务器收到的第几个 HTTP 请求（从 0 开始，不含 WebSocket 升级）
    std::size_t connection = 0;  // 所在 TCP 连接的编号
    std::string method;
    std::string target;
    std::string host;
    std::string user_agent;
    std::int64_t time_ns = 0;    // 收到请求的 nowNs()
};

struct HttpReply {
    int status = 200;
    std::string body;
    std::string content_type = "application/json";
    int delay_ms = 0;            // 应答前等待（模拟慢服务器）
};

using HttpHandler = std::function<HttpReply(const HttpRequestInfo&)>;

class WsSession;
using WsScript = std::function<void(WsSession&)>;

struct ServerOptions {
    std::string bind_address = "127.0.0.1";
    unsigned short port = 0;               // 0 = 临时端口
    std::optional<TlsIdentity> identity;   // 未提供时按 certificate 生成
    CertificateOptions certificate;
    int handshake_timeout_ms = 5000;       // TLS 握手 / 读取 HTTP 请求 / WebSocket 升级的时限
};

class MockExchangeServer;

// 一个服务器端连接；WebSocket 升级后作为脚本的句柄。只在所属连接线程上使用。
class WsSession {
public:
    WsSession(const WsSession&) = delete;
    WsSession& operator=(const WsSession&) = delete;

    std::size_t connectionIndex() const noexcept { return connection_; }   // TCP 连接编号（接受顺序）
    std::size_t sessionIndex() const noexcept { return session_; }         // WebSocket 会话编号
    const std::string& target() const noexcept { return target_; }         // 升级请求的路径
    inline MockExchangeServer& server() noexcept;

    // 发送一个文本帧（阻塞到写完）；连接已断开时抛出 ConnectionClosed
    void send(std::string_view text) {
        ensureOpen();
        wbuf_.assign(text.data(), text.size());
        write_done_ = false;
        ws_.async_write(asio::buffer(wbuf_), [this](error_code ec, std::size_t) {
            write_ec_ = ec;
            write_done_ = true;
        });
        runUntil([this] { return write_done_; }, -1);
        if (write_ec_) {
            open_ = false;
            throw ConnectionClosed("mock_exchange: write failed: " + write_ec_.message());
        }
    }

    // 依次发送，帧之间间隔 gap_ms
    void sendAll(const std::vector<std::string>& frames, int gap_ms = 0) {
        for (std::size_t i = 0; i < frames.size(); ++i) {
            if (i > 0 && gap_ms > 0) sleepMs(gap_ms);
            send(frames[i]);
        }
    }

    // 等待 ms 毫秒（期间继续接收客户端的帧）
    void sleepMs(int ms) {
        runUntil([] { return false; }, ms);
    }

    // 等待一个满足 match 的文本帧（从上次 expect 之后的帧开始找，不匹配的帧被跳过），返回该帧。
    // 超时抛出 ScriptError（记入 errors()）；客户端断开抛出 ConnectionClosed。
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
    bool waitUntil(const std::function<bool()>& pred, int timeout_ms) { return runUntil(pred, timeout_ms); }

    // 本会话目前收到的全部文本帧
    const std::vector<std::string>& received() const noexcept { return frames_; }

    std::size_t countReceived(const std::string& exact) const {
        return static_cast<std::size_t>(std::count(frames_.begin(), frames_.end(), exact));
    }

    // 正常关闭：发送 close 帧并等待客户端回应（最多 2 s）
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
    void abort() noexcept {
        open_ = false;
        error_code ec;
        beast::get_lowest_layer(ws_).shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(ws_).close(ec);
    }

    // 保持静默直到客户端断开（timeout_ms < 0：直到服务器停止）；返回客户端是否已断开
    bool waitForClientDisconnect(int timeout_ms = -1) {
        return runUntil([this] { return peer_closed_; }, timeout_ms);
    }

    bool clientConnected() const noexcept { return open_ && !peer_closed_; }

private:
    friend class MockExchangeServer;
    using Stream = websocket::stream<beast::ssl_stream<tcp::socket>>;

    WsSession(MockExchangeServer& server, asio::io_context& ioc, tcp::socket&& sock, ssl::context& ctx,
              std::size_t connection)
        : server_(server), ioc_(ioc), guard_(asio::make_work_guard(ioc)), ws_(std::move(sock), ctx),
          connection_(connection) {}

    inline void serve();
    inline bool serveHttp();
    inline void serveWebSocket();
    inline bool stopping() const noexcept;
    inline int handshakeTimeoutMs() const noexcept;

    void ensureOpen() const {
        if (!open_ || peer_closed_) throw ConnectionClosed("mock_exchange: connection closed");
    }

    // 运行 io_context 直到 pred() 成立（true）或超时（false）；服务器停止时抛出 ConnectionClosed
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

    inline void onFrame(const std::string& frame);

    // 写完应答后的收尾：关闭发送方向，读到客户端 EOF（最多 timeout_ms）再关闭，避免未读数据触发 RST
    void gracefulTcpClose(int timeout_ms) noexcept {
        auto& sock = beast::get_lowest_layer(ws_);
        error_code ec;
        sock.shutdown(tcp::socket::shutdown_send, ec);
        const int fd = sock.native_handle();
        const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
        char buf[1024];
        while (fd >= 0 && Clock::now() < deadline && !stopping()) {
            pollfd p{fd, POLLIN, 0};
            const int r = ::poll(&p, 1, 10);
            if (r < 0 && errno != EINTR) break;
            if (r > 0) {
                const ssize_t n = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
                if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) break;
            }
        }
        sock.close(ec);
    }

    // 结束连接：关闭套接字并让所有挂起的处理器（已被取消）执行完毕
    void teardown() noexcept {
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
    Stream ws_;
    beast::flat_buffer buffer_;                 // HTTP 请求缓冲
    http::request<http::string_body> req_;
    http::response<http::string_body> res_;
    beast::flat_buffer rbuf_;                   // WebSocket 读缓冲
    std::string wbuf_;                          // 正在发送的帧
    std::vector<std::string> frames_;           // 收到的文本帧
    std::size_t cursor_ = 0;                    // expectText 的读取位置
    std::size_t connection_ = 0;
    std::size_t session_ = std::numeric_limits<std::size_t>::max();
    std::string target_;
    // 异步操作状态（处理器只写这些成员）
    bool op_done_ = false;
    error_code op_ec_;
    bool write_done_ = false;
    error_code write_ec_;
    bool close_done_ = false;
    bool reading_ = false;
    bool peer_closed_ = false;  // WebSocket 读循环已结束（客户端关闭或连接出错）
    bool open_ = false;         // WebSocket 已建立且尚未由服务器关闭
};

// ---------------------------------------------------------------------------
// 服务器
// ---------------------------------------------------------------------------

class MockExchangeServer {
public:
    explicit MockExchangeServer(ServerOptions opt = ServerOptions())
        : opt_(std::move(opt)),
          identity_(opt_.identity ? *opt_.identity : generateSelfSigned(opt_.certificate)),
          ssl_ctx_(ssl::context::tls_server),
          acceptor_(acceptor_ioc_),
          accept_socket_(acceptor_ioc_) {
        if (SSL_CTX_set_min_proto_version(ssl_ctx_.native_handle(), TLS1_2_VERSION) != 1) {
            detail::throwOpenSsl("SSL_CTX_set_min_proto_version");
        }
        ssl_ctx_.set_options(ssl::context::default_workarounds);
        ssl_ctx_.use_certificate_chain(asio::buffer(identity_.cert_pem));
        ssl_ctx_.use_private_key(asio::buffer(identity_.key_pem), ssl::context::pem);
        const tcp::endpoint ep(asio::ip::make_address(opt_.bind_address), opt_.port);
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
    void stop() noexcept {
        if (stopping_.exchange(true)) return;
        asio::post(acceptor_ioc_, [this] {
            error_code ec;
            acceptor_.close(ec);
        });
        if (accept_thread_.joinable()) accept_thread_.join();
        std::vector<Worker> workers;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            workers.swap(workers_);
        }
        for (Worker& w : workers) {
            if (w.thread.joinable()) w.thread.join();
        }
    }

    bool stopping() const noexcept { return stopping_.load(std::memory_order_acquire); }

    unsigned short port() const noexcept { return port_; }
    std::string portString() const { return std::to_string(port_); }
    const TlsIdentity& identity() const noexcept { return identity_; }

    // 把证书写到临时文件（首次调用时；服务器析构时删除），返回路径：客户端的 ca_file
    const std::string& caFile() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ca_file_.path().empty()) ca_file_ = TempFile(identity_.cert_pem, "mock_exchange_ca");
        return ca_file_.path();
    }

    // ---------------- 行为（任意时刻可设置；之后建立的连接/会话使用新设置）----------------

    // 所有 WebSocket 会话使用同一个脚本
    void setWebSocketScript(WsScript script) {
        std::lock_guard<std::mutex> lock(mutex_);
        scripts_.assign(1, std::move(script));
    }

    // 第 i 个 WebSocket 会话使用 scripts[i]；超出部分重复最后一个
    void setWebSocketScripts(std::vector<WsScript> scripts) {
        std::lock_guard<std::mutex> lock(mutex_);
        scripts_ = std::move(scripts);
    }

    // HTTP 请求处理（未设置时一律 404）
    void setHttpHandler(HttpHandler handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        http_handler_ = std::move(handler);
    }

    // 新 TCP 连接的处理方式（参数为连接编号；未设置时一律 Serve）
    void setAcceptHandler(std::function<AcceptAction(std::size_t)> handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        accept_handler_ = std::move(handler);
    }

    // WebSocket 升级过滤：返回 false 时以 HTTP 404 拒绝升级（参数为请求路径）
    void setUpgradeFilter(std::function<bool(const std::string&)> filter) {
        std::lock_guard<std::mutex> lock(mutex_);
        upgrade_filter_ = std::move(filter);
    }

    // 按请求顺序依次应答（第 i 个 HTTP 请求得到 replies[i]，超出部分重复最后一个）
    static HttpHandler replySequence(std::vector<HttpReply> replies) {
        auto shared = std::make_shared<const std::vector<HttpReply>>(std::move(replies));
        return [shared](const HttpRequestInfo& info) {
            if (shared->empty()) return HttpReply{404, R"({"code":-1,"msg":"no reply configured"})", "application/json", 0};
            return (*shared)[std::min(info.index, shared->size() - 1)];
        };
    }

    // ---------------- 观测（线程安全，返回拷贝）----------------

    std::size_t acceptCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return accept_times_.size();
    }
    std::size_t wsSessionCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ws_frames_.size();
    }
    std::size_t httpRequestCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return http_requests_.size();
    }
    // 每个 TCP 连接被接受的时刻（nowNs）
    std::vector<std::int64_t> acceptTimesNs() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return accept_times_;
    }
    // 每个 TCP 连接结束的时刻（nowNs；仍在进行中为 0）
    std::vector<std::int64_t> endTimesNs() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return end_times_;
    }
    std::size_t openConnections() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<std::size_t>(std::count(end_times_.begin(), end_times_.end(), std::int64_t{0}));
    }
    // 第 session 个 WebSocket 会话收到的文本帧
    std::vector<std::string> framesReceived(std::size_t session) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return session < ws_frames_.size() ? ws_frames_[session] : std::vector<std::string>();
    }
    // 与 framesReceived(session) 一一对应的到达时刻（nowNs）
    std::vector<std::int64_t> frameTimesNs(std::size_t session) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return session < ws_frame_times_.size() ? ws_frame_times_[session] : std::vector<std::int64_t>();
    }
    std::vector<std::string> wsTargets() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ws_targets_;
    }
    // 每个 WebSocket 会话开始（收到升级请求）的时刻（nowNs）
    std::vector<std::int64_t> wsSessionTimesNs() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ws_times_;
    }
    std::vector<HttpRequestInfo> httpRequests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return http_requests_;
    }
    // 脚本错误（期望未满足、异常）
    std::vector<std::string> errors() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return errors_;
    }
    // TLS 握手失败（例如客户端不信任证书时发出的 alert）
    std::vector<std::string> tlsFailures() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tls_failures_;
    }

private:
    friend class WsSession;

    void doAccept() {
        acceptor_.async_accept(accept_socket_, [this](error_code ec) {
            if (ec) {
                if (!stopping() && ec != asio::error::operation_aborted) doAccept();
                return;
            }
            error_code rec;
            const int fd = accept_socket_.release(rec);  // 所有权交给连接线程（它有自己的 io_context）
            if (rec) {
                accept_socket_.close(rec);
                doAccept();
                return;
            }
            const std::size_t index = recordAccept();
            std::vector<std::thread> finished;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (stopping()) {
                    ::close(fd);
                    end_times_[index] = nowNs();
                    return;
                }
                // 回收已结束的连接线程（长时间运行的模拟交易所不会积累未 join 的线程）
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
            } else {
                const AcceptAction action = acceptAction(index);
                if (action == AcceptAction::CloseImmediately) {
                    sock.close(ec);
                } else if (action == AcceptAction::Silent) {
                    silentUntilPeerCloses(sock);
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
            }
        } catch (const std::exception& e) {
            recordError(std::string("mock_exchange: connection ") + std::to_string(index) + ": " + e.what());
        } catch (...) {
            recordError("mock_exchange: connection " + std::to_string(index) + ": unknown exception");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        end_times_[index] = nowNs();
    }

    // 不做 TLS 握手，等待客户端断开（或服务器停止）
    void silentUntilPeerCloses(tcp::socket& sock) noexcept {
        const int fd = sock.native_handle();
        char buf[1024];
        while (!stopping()) {
            pollfd p{fd, POLLIN, 0};
            const int r = ::poll(&p, 1, 10);
            if (r < 0 && errno != EINTR) break;
            if (r > 0) {
                const ssize_t n = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
                if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) break;
            }
        }
        error_code ec;
        sock.close(ec);
    }

    std::size_t recordAccept() {
        std::lock_guard<std::mutex> lock(mutex_);
        accept_times_.push_back(nowNs());
        end_times_.push_back(0);
        return accept_times_.size() - 1;
    }

    std::size_t registerWsSession(const std::string& target) {
        std::lock_guard<std::mutex> lock(mutex_);
        ws_frames_.emplace_back();
        ws_frame_times_.emplace_back();
        ws_targets_.push_back(target);
        ws_times_.push_back(nowNs());
        return ws_frames_.size() - 1;
    }

    void recordFrame(std::size_t session, const std::string& frame) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (session < ws_frames_.size()) {
            ws_frames_[session].push_back(frame);
            ws_frame_times_[session].push_back(nowNs());
        }
    }

    HttpRequestInfo recordHttpRequest(std::size_t connection, const http::request<http::string_body>& req) {
        HttpRequestInfo info;
        info.connection = connection;
        info.method = std::string(req.method_string());
        info.target = std::string(req.target());
        info.host = std::string(req[http::field::host]);
        info.user_agent = std::string(req[http::field::user_agent]);
        info.time_ns = nowNs();
        std::lock_guard<std::mutex> lock(mutex_);
        info.index = http_requests_.size();
        http_requests_.push_back(info);
        return info;
    }

    void recordError(const std::string& e) {
        std::lock_guard<std::mutex> lock(mutex_);
        errors_.push_back(e);
    }

    void recordTlsFailure(std::size_t connection, const std::string& e) {
        std::lock_guard<std::mutex> lock(mutex_);
        tls_failures_.push_back("connection " + std::to_string(connection) + ": " + e);
    }

    WsScript scriptFor(std::size_t session) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (scripts_.empty()) return WsScript();
        return scripts_[std::min(session, scripts_.size() - 1)];
    }

    // 处理器在锁外调用（处理器可以调用观测方法）
    HttpReply httpReply(const HttpRequestInfo& info) const {
        HttpHandler handler;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handler = http_handler_;
        }
        if (!handler) return HttpReply{404, R"({"code":-1,"msg":"not found"})", "application/json", 0};
        return handler(info);
    }

    AcceptAction acceptAction(std::size_t index) const {
        std::function<AcceptAction(std::size_t)> handler;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handler = accept_handler_;
        }
        return handler ? handler(index) : AcceptAction::Serve;
    }

    bool acceptUpgrade(const std::string& target) const {
        std::function<bool(const std::string&)> filter;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            filter = upgrade_filter_;
        }
        return filter ? filter(target) : true;
    }

    ServerOptions opt_;
    TlsIdentity identity_;
    ssl::context ssl_ctx_;                     // 各连接线程共享（OpenSSL 的 SSL_CTX 可并发创建 SSL）
    asio::io_context acceptor_ioc_;
    tcp::acceptor acceptor_;
    tcp::socket accept_socket_;
    tcp protocol_ = tcp::v4();
    unsigned short port_ = 0;
    std::atomic<bool> stopping_{false};
    std::thread accept_thread_;

    struct Worker {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> done;  // 连接线程函数已返回
    };

    mutable std::mutex mutex_;                 // 保护以下全部成员
    std::vector<Worker> workers_;
    TempFile ca_file_;
    std::vector<WsScript> scripts_;
    HttpHandler http_handler_;
    std::function<AcceptAction(std::size_t)> accept_handler_;
    std::function<bool(const std::string&)> upgrade_filter_;
    std::vector<std::int64_t> accept_times_;
    std::vector<std::int64_t> end_times_;
    std::vector<std::vector<std::string>> ws_frames_;
    std::vector<std::vector<std::int64_t>> ws_frame_times_;
    std::vector<std::string> ws_targets_;
    std::vector<std::int64_t> ws_times_;
    std::vector<HttpRequestInfo> http_requests_;
    std::vector<std::string> errors_;
    std::vector<std::string> tls_failures_;
};

// ---------------------------------------------------------------------------
// WsSession 中依赖服务器定义的部分
// ---------------------------------------------------------------------------

inline MockExchangeServer& WsSession::server() noexcept { return server_; }

inline bool WsSession::stopping() const noexcept { return server_.stopping(); }

inline int WsSession::handshakeTimeoutMs() const noexcept { return server_.opt_.handshake_timeout_ms; }

inline void WsSession::onFrame(const std::string& frame) { server_.recordFrame(session_, frame); }

inline void WsSession::serve() {
    op_done_ = false;
    ws_.next_layer().async_handshake(ssl::stream_base::server, [this](error_code ec) {
        op_ec_ = ec;
        op_done_ = true;
    });
    if (!runUntil([this] { return op_done_; }, handshakeTimeoutMs())) {
        server_.recordTlsFailure(connection_, "TLS handshake timed out");
        return;
    }
    if (op_ec_) {
        server_.recordTlsFailure(connection_, op_ec_.message());
        return;
    }
    for (;;) {
        req_ = {};
        op_done_ = false;
        http::async_read(ws_.next_layer(), buffer_, req_, [this](error_code ec, std::size_t) {
            op_ec_ = ec;
            op_done_ = true;
        });
        if (!runUntil([this] { return op_done_; }, handshakeTimeoutMs()) || op_ec_) return;
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
    res_ = {};
    res_.version(req_.version());
    res_.result(static_cast<unsigned>(reply.status));
    res_.set(http::field::server, "mock_exchange");
    res_.set(http::field::content_type, reply.content_type);
    res_.keep_alive(req_.keep_alive());
    res_.body() = std::move(reply.body);
    res_.prepare_payload();
    op_done_ = false;
    http::async_write(ws_.next_layer(), res_, [this](error_code ec, std::size_t) {
        op_ec_ = ec;
        op_done_ = true;
    });
    runUntil([this] { return op_done_; }, -1);
    if (op_ec_) return false;
    if (!res_.keep_alive()) {
        gracefulTcpClose(1000);
        return false;
    }
    return true;
}

inline void WsSession::serveWebSocket() {
    target_ = std::string(req_.target());
    if (!server_.acceptUpgrade(target_)) {
        res_ = {};
        res_.version(req_.version());
        res_.result(http::status::not_found);
        res_.set(http::field::server, "mock_exchange");
        res_.set(http::field::content_type, "text/plain");
        res_.keep_alive(false);
        res_.body() = "no such stream: " + target_;
        res_.prepare_payload();
        op_done_ = false;
        http::async_write(ws_.next_layer(), res_, [this](error_code ec, std::size_t) {
            op_ec_ = ec;
            op_done_ = true;
        });
        runUntil([this] { return op_done_; }, -1);
        if (!op_ec_) gracefulTcpClose(1000);
        return;
    }
    session_ = server_.registerWsSession(target_);
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res) { res.set(http::field::server, "mock_exchange"); }));
    op_done_ = false;
    ws_.async_accept(req_, [this](error_code ec) {
        op_ec_ = ec;
        op_done_ = true;
    });
    if (!runUntil([this] { return op_done_; }, handshakeTimeoutMs()) || op_ec_) {
        server_.recordError("mock_exchange: session " + std::to_string(session_) + ": WebSocket accept failed: " +
                            (op_done_ ? op_ec_.message() : std::string("timed out")));
        return;
    }
    open_ = true;
    ws_.text(true);
    startRead();
    const WsScript script = server_.scriptFor(session_);
    if (script) script(*this);
    if (open_ && !peer_closed_) waitForClientDisconnect(-1);  // 脚本结束：保持静默直到客户端断开
}

}  // namespace mock_exchange
