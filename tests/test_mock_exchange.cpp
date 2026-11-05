//
// test_mock_exchange.cpp
// 离线模拟交易所测试：报文格式与深度差分、市场仿真（确定性、与引擎一致、跨所套利机会），发布者报文经真实协议适配器往返
// （簿与引擎深度逐档相等、无缺口、OKX checksum 独立验证），网络层协议请求，以及三个真实 MarketDataFeed 的端到端运行。
//

// 启用库内调试断言（必须在包含任何 hft 头文件之前定义）
#define HFT_ENABLE_ASSERTS

#include "test_framework.h"

#include "../tools/mock_exchange.h"
#include "exchanges/adapters.h"
#include "exchanges/json_util.h"
#include "hft/timing.h"
#include "market_data_feed.h"
#include "net/https_client.h"

#include <boost/asio/connect.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace mock_exchange;
using sim::Side;

constexpr const char* kOkxBooksSub = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
constexpr const char* kBybitSub = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";
constexpr const char* kBinanceDepthPath = "/api/v3/depth?symbol=BTCUSDT&limit=1000";
constexpr std::int64_t kBaseTs = 1700000000000;

bool contains(const std::string& s, const std::string& part) { return s.find(part) != std::string::npos; }

double tickPrice(sim::Price p) { return static_cast<double>(p) / static_cast<double>(sim::kTicksPerUnit); }
double lotQty(sim::Qty q) { return static_cast<double>(q) / static_cast<double>(sim::kLotsPerUnit); }

// 适配器簿的一侧与引擎深度逐档比较：价格、数量用 double 精确相等（同一个十进制数，from_chars 与整数相除
// 都是正确舍入，结果逐位相同）。失败时在 why 里写明第一处差异。
bool sideEquals(const std::vector<PriceLevel>& book, const wire::Levels& want, const char* side, std::string* why) {
    if (book.size() != want.size()) {
        *why = std::string(side) + ": " + std::to_string(book.size()) + " levels, expected " +
               std::to_string(want.size());
        return false;
    }
    for (std::size_t i = 0; i < want.size(); ++i) {
        if (book[i].price != tickPrice(want[i].price) || book[i].quantity != lotQty(want[i].qty)) {
            char buf[200];
            std::snprintf(buf, sizeof(buf), "%s[%zu]: %.8f x %.8f, expected %.8f x %.8f", side, i, book[i].price,
                          book[i].quantity, tickPrice(want[i].price), lotQty(want[i].qty));
            *why = buf;
            return false;
        }
    }
    return true;
}

bool bookEquals(const OrderBook& book, const wire::Levels& bids, const wire::Levels& asks, std::string* why) {
    return sideEquals(book.getBids(), bids, "bids", why) && sideEquals(book.getAsks(), asks, "asks", why);
}

// 快照（feed 发布的前 N 档）与引擎深度的前 N 档比较
bool snapshotEquals(const BookSnapshot& s, const wire::Levels& bids, const wire::Levels& asks, std::string* why) {
    const std::vector<PriceLevel> b(s.bids, s.bids + s.bid_count);
    const std::vector<PriceLevel> a(s.asks, s.asks + s.ask_count);
    const auto nb = static_cast<std::ptrdiff_t>(std::min<std::size_t>(s.bid_count, bids.size()));
    const auto na = static_cast<std::ptrdiff_t>(std::min<std::size_t>(s.ask_count, asks.size()));
    const wire::Levels wb(bids.begin(), bids.begin() + nb);
    const wire::Levels wa(asks.begin(), asks.begin() + na);
    if (s.bid_count != std::min<std::size_t>(BookSnapshot::kMaxLevels, bids.size()) ||
        s.ask_count != std::min<std::size_t>(BookSnapshot::kMaxLevels, asks.size())) {
        *why = "snapshot has " + std::to_string(s.bid_count) + "/" + std::to_string(s.ask_count) + " levels";
        return false;
    }
    return sideEquals(b, wb, "bids", why) && sideEquals(a, wa, "asks", why);
}

// 独立的 CRC-32（逐位实现，不用 hft/crc32.h 的查表），以及按有符号 int32 的解释
std::uint32_t bitwiseCrc32(std::string_view s) {
    std::uint32_t c = 0xFFFFFFFFu;
    for (const unsigned char ch : s) {
        c ^= ch;
        for (int k = 0; k < 8; ++k) c = (c >> 1) ^ (0xEDB88320u & (0u - (c & 1u)));
    }
    return ~c;
}

std::int32_t asSigned(std::uint32_t v) {
    return static_cast<std::int32_t>(static_cast<std::int64_t>(v) - (v > 0x7FFFFFFFu ? (std::int64_t{1} << 32) : 0));
}

// 测试自己的 OKX 簿模型：只用报文里的原始字符串（价格 -> (价格串, 数量串)），按文档规则计算 checksum
struct OkxStringModel {
    std::map<double, std::pair<std::string, std::string>, std::greater<double>> bids;
    std::map<double, std::pair<std::string, std::string>> asks;

    template <class Map>
    static void apply(Map& side, simdjson::dom::array levels) {
        for (simdjson::dom::element e : levels) {
            simdjson::dom::array lv = e.get_array();
            const std::string px(std::string_view(lv.at(0).get_string()));
            const std::string sz(std::string_view(lv.at(1).get_string()));
            const double p = std::stod(px);
            if (std::stod(sz) == 0.0) {
                side.erase(p);
            } else {
                side[p] = {px, sz};
            }
        }
    }

    void apply(simdjson::dom::element data0, bool snapshot) {
        if (snapshot) {
            bids.clear();
            asks.clear();
        }
        apply(bids, data0["bids"].get_array());
        apply(asks, data0["asks"].get_array());
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
        return asSigned(bitwiseCrc32(s));
    }
};

AdapterConfig adapterConfig(const char* name, const std::string& subscribe = std::string()) {
    AdapterConfig c;
    c.name = name;
    c.symbol = "BTCUSDT";
    c.subscribe_msg = subscribe;
    c.verify_checksum = true;
    return c;
}

// 每家交易所在规范 5.1 的配置里使用的订阅；测试里同样使用
FeedConfig feedConfig(Exchange exchange, const MockExchange& ex, const std::string& ca_file) {
    FeedConfig c;
    c.exchange = exchange;
    c.venue_id = static_cast<std::uint32_t>(exchange);
    c.name = exchangeName(exchange);
    c.host = "localhost";
    c.symbol = "BTCUSDT";
    c.tls_verify = true;
    c.ca_file = ca_file;
    c.reconnect_initial_ms = 50;
    c.reconnect_max_ms = 500;
    c.log_to_stderr = false;
    switch (exchange) {
        case Exchange::BINANCE:
            c.port = std::to_string(ex.binancePort());
            c.path = "/ws/btcusdt@depth@100ms";
            c.rest_host = "localhost";
            c.rest_port = c.port;
            c.snapshot_path = kBinanceDepthPath;
            break;
        case Exchange::OKX:
            c.port = std::to_string(ex.okxPort());
            c.path = "/ws/v5/public";
            c.subscribe_msg = kOkxBooksSub;
            break;
        case Exchange::BYBIT:
            c.port = std::to_string(ex.bybitPort());
            c.path = "/v5/public/spot";
            c.subscribe_msg = kBybitSub;
            break;
    }
    return c;
}

// 在独立线程上运行 feed；析构时 stop + join
struct FeedRunner {
    MarketDataFeed feed;
    std::thread thread;

    explicit FeedRunner(const FeedConfig& cfg) : feed(cfg) { thread = std::thread([this] { feed.run(); }); }
    ~FeedRunner() { stop(); }
    FeedRunner(const FeedRunner&) = delete;
    FeedRunner& operator=(const FeedRunner&) = delete;
    void stop() {
        feed.stop();
        if (thread.joinable()) thread.join();
    }
};

// 所有测试用的交易所：临时端口、较短预热
MockExchangeOptions testOptions(std::uint64_t seed) {
    MockExchangeOptions o;
    o.binance_port = 0;
    o.okx_port = 0;
    o.bybit_port = 0;
    o.seed = seed;
    o.warmup_s = 3.0;
    return o;
}

// ---------------------------------------------------------------------------
// 简单的 TLS WebSocket 客户端（异步 Beast + io_context::run_for 实现每个操作的超时，测试不会卡死）
// ---------------------------------------------------------------------------
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;

class WsClient {
public:
    WsClient(unsigned short port, const std::string& path, const std::string& ca_file, int timeout_ms = 5000)
        : ctx_(hft::net::makeClientContext(hft::net::TlsOptions{true, ca_file})),
          ws_(ioc_, *ctx_),
          timeout_ms_(timeout_ms) {
        tcp::resolver resolver(ioc_);
        const auto endpoints = resolver.resolve("localhost", std::to_string(port));
        check(run([&](auto h) { asio::async_connect(beast::get_lowest_layer(ws_), endpoints, h); }, "connect"),
              "connect");
        hft::net::prepareTlsStream(ws_.next_layer(), "localhost", true);
        check(run([&](auto h) { ws_.next_layer().async_handshake(ssl::stream_base::client, h); }, "TLS handshake"),
              "TLS handshake");
        handshake_ec_ = run(
            [&](auto h) { ws_.async_handshake(response_, "localhost:" + std::to_string(port), path, h); },
            "WebSocket handshake");
        ws_.text(true);
    }

    ~WsClient() {
        error_code ignored;
        beast::get_lowest_layer(ws_).close(ignored);
    }

    bool open() const noexcept { return !handshake_ec_; }
    const error_code& handshakeError() const noexcept { return handshake_ec_; }
    int responseStatus() const { return static_cast<int>(response_.result_int()); }

    void send(const std::string& text) {
        check(run([&](auto h) { ws_.async_write(asio::buffer(text), h); }, "write"), "write");
    }

    std::string read() {
        beast::flat_buffer buf;
        check(run([&](auto h) { ws_.async_read(buf, h); }, "read"), "read");
        return beast::buffers_to_string(buf.data());
    }

    // 读到第一条满足 pred 的报文并返回（之前的报文交给 skipped，可为空）；最多读 max_messages 条
    template <class Pred>
    std::string readUntil(Pred&& pred, int max_messages = 2000,
                          const std::function<void(const std::string&)>& skipped = {}) {
        for (int i = 0; i < max_messages; ++i) {
            std::string m = read();
            if (pred(m)) return m;
            if (skipped) skipped(m);
        }
        throw std::runtime_error("WsClient: expected message not received");
    }

private:
    template <class Start>
    error_code run(Start&& start, const char* what) {
        bool done = false;
        error_code result;
        start([&done, &result](error_code ec, auto&&...) {
            result = ec;
            done = true;
        });
        ioc_.restart();
        ioc_.run_for(std::chrono::milliseconds(timeout_ms_));
        if (!done) {
            error_code ignored;
            beast::get_lowest_layer(ws_).close(ignored);
            ioc_.restart();
            ioc_.run();  // 让被取消的处理器执行完（它们引用本函数的局部变量）
            throw std::runtime_error(std::string("WsClient: ") + what + " timed out");
        }
        return result;
    }

    static void check(const error_code& ec, const char* what) {
        if (ec) throw std::runtime_error(std::string("WsClient: ") + what + " failed: " + ec.message());
    }

    asio::io_context ioc_;
    std::unique_ptr<ssl::context> ctx_;
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_;
    websocket::response_type response_;
    error_code handshake_ec_;
    int timeout_ms_;
};

// 用 feed 自己的 HTTPS 客户端发 REST 请求
hft::net::HttpResult restGet(unsigned short port, const std::string& target, const std::string& ca_file) {
    asio::io_context ioc;
    const auto ctx = hft::net::makeClientContext(hft::net::TlsOptions{true, ca_file});
    hft::net::Resolver resolver;
    hft::net::SocketRegistry registry;
    hft::net::HttpsGetRequest rq;
    rq.host = "localhost";
    rq.port = std::to_string(port);
    rq.target = target;
    return hft::net::httpsGet(ioc, *ctx, resolver, registry, rq);
}

// 跨所扫单（与规范 5.2 相同的规则，测试自带的独立实现）：在 i 买、在 j 卖，逐档推进直到边际单位不再盈利
double sweepNet(const sim::Venue& buy, const sim::Venue& sell, double fee, std::size_t max_levels = 20) {
    const wire::Levels asks = buy.depth(Side::Sell, max_levels);
    const wire::Levels bids = sell.depth(Side::Buy, max_levels);
    std::size_t ia = 0, ib = 0;
    double qa = asks.empty() ? 0.0 : lotQty(asks[0].qty);
    double qb = bids.empty() ? 0.0 : lotQty(bids[0].qty);
    double net = 0.0;
    while (ia < asks.size() && ib < bids.size()) {
        const double a = tickPrice(asks[ia].price);
        const double b = tickPrice(bids[ib].price);
        const double unit = b * (1.0 - fee) - a * (1.0 + fee);
        if (!(unit > 0.0)) break;
        const double q = std::min(qa, qb);
        net += unit * q;
        qa -= q;
        qb -= q;
        if (qa <= 0.0 && ++ia < asks.size()) qa = lotQty(asks[ia].qty);
        if (qb <= 0.0 && ++ib < bids.size()) qb = lotQty(bids[ib].qty);
    }
    return net;
}

}  // namespace

// ============================================================================
// 报文格式与差分
// ============================================================================

TEST_CASE("mock wire: decimal formatting per venue (Binance 8 dp, OKX shortest, Bybit fixed) round-trips exactly") {
    using wire::formatPrice;
    using wire::formatQty;
    CHECK_EQ(formatPrice(6500012, wire::kBinanceFormat), std::string("65000.12000000"));
    CHECK_EQ(formatPrice(6500012, wire::kOkxFormat), std::string("65000.12"));
    CHECK_EQ(formatPrice(6500012, wire::kBybitFormat), std::string("65000.12"));
    CHECK_EQ(formatPrice(6500010, wire::kOkxFormat), std::string("65000.1"));
    CHECK_EQ(formatPrice(6500000, wire::kOkxFormat), std::string("65000"));
    CHECK_EQ(formatPrice(6500010, wire::kBybitFormat), std::string("65000.10"));
    CHECK_EQ(formatPrice(5, wire::kBinanceFormat), std::string("0.05000000"));
    CHECK_EQ(formatQty(1, wire::kBinanceFormat), std::string("0.00000100"));
    CHECK_EQ(formatQty(1, wire::kOkxFormat), std::string("0.000001"));
    CHECK_EQ(formatQty(1500000, wire::kOkxFormat), std::string("1.5"));
    CHECK_EQ(formatQty(1500000, wire::kBybitFormat), std::string("1.500000"));
    CHECK_EQ(formatQty(0, wire::kBinanceFormat), std::string("0.00000000"));
    CHECK_EQ(formatQty(0, wire::kOkxFormat), std::string("0"));
    CHECK_EQ(formatQty(0, wire::kBybitFormat), std::string("0.000000"));
    // 随机值：适配器的十进制解析（与 from_chars 逐位一致）得到的 double 等于整数相除的结果
    std::mt19937_64 rng(42);
    for (int i = 0; i < 200000; ++i) {
        const sim::Price p = static_cast<sim::Price>(rng() % 20000000000ULL) + 1;
        const sim::Qty q = static_cast<sim::Qty>(rng() % 100000000000ULL);
        for (const wire::NumberFormat* f : {&wire::kBinanceFormat, &wire::kOkxFormat, &wire::kBybitFormat}) {
            double pv = 0.0, qv = 0.0;
            REQUIRE(adapter_detail::parseDecimal(formatPrice(p, *f), pv));
            REQUIRE(adapter_detail::parseDecimal(formatQty(q, *f), qv));
            if (pv != tickPrice(p) || qv != lotQty(q)) {
                FAIL("round trip failed for " + formatPrice(p, *f) + " / " + formatQty(q, *f));
            }
        }
    }
}

TEST_CASE("mock wire: diffSide applied to the previous window reproduces the current one (random windows)") {
    std::mt19937_64 rng(7);
    for (int round = 0; round < 3000; ++round) {
        const bool bids = round % 2 == 0;
        auto randomSide = [&](std::size_t n) {
            std::map<sim::Price, wire::Level> m;
            while (m.size() < n) {
                const sim::Price p = 1000 + static_cast<sim::Price>(rng() % 200);
                m[p] = wire::Level{p, static_cast<sim::Qty>(1 + rng() % 5), static_cast<std::uint32_t>(1 + rng() % 3)};
            }
            wire::Levels out;
            for (const auto& kv : m) out.push_back(kv.second);
            if (bids) std::reverse(out.begin(), out.end());  // 买盘价格降序
            return out;
        };
        const wire::Levels prev = randomSide(rng() % 40);
        const wire::Levels cur = randomSide(rng() % 40);
        const bool compare_orders = round % 3 == 0;
        wire::Levels diff;
        wire::diffSide(prev, cur, bids, compare_orders, diff);
        // 输出最优在前、价格不重复
        for (std::size_t i = 1; i < diff.size(); ++i) {
            REQUIRE(bids ? diff[i - 1].price > diff[i].price : diff[i - 1].price < diff[i].price);
        }
        std::map<sim::Price, wire::Level> applied;
        for (const wire::Level& l : prev) applied[l.price] = l;
        for (const wire::Level& l : diff) {
            if (l.qty == 0) {
                REQUIRE(applied.erase(l.price) == 1);  // 删除的一定是已有档位
            } else {
                applied[l.price] = l;
            }
        }
        std::map<sim::Price, wire::Level> want;
        for (const wire::Level& l : cur) want[l.price] = l;
        REQUIRE_EQ(applied.size(), want.size());
        for (const auto& kv : want) {
            REQUIRE(applied.count(kv.first) == 1);
            REQUIRE_EQ(applied[kv.first].qty, kv.second.qty);
            if (compare_orders) REQUIRE_EQ(applied[kv.first].orders, kv.second.orders);
        }
        // 没有多余的条目：每条差分都对应一次真实变化
        for (const wire::Level& l : diff) {
            const auto p =
                std::find_if(prev.begin(), prev.end(), [&](const wire::Level& x) { return x.price == l.price; });
            if (p != prev.end() && l.qty != 0) REQUIRE(p->qty != l.qty || (compare_orders && p->orders != l.orders));
        }
    }
}

TEST_CASE("mock wire: OKX checksum string uses the exact pushed strings (documented example and uneven sides)") {
    // OKX 文档示例：bids 3366.1:7, 3366:6；asks 3366.8:9, 3368:8
    const wire::Levels bids{{336610, 7000000, 1}, {336600, 6000000, 1}};
    const wire::Levels asks{{336680, 9000000, 1}, {336800, 8000000, 1}};
    CHECK_EQ(wire::okxChecksumString(bids, asks), std::string("3366.1:7:3366.8:9:3366:6:3368:8"));
    CHECK_EQ(wire::okxChecksum(bids, asks), -1881014294);
    // 一侧较短：先交错，再接上另一侧剩下的档位；只取前 25 档
    wire::Levels many;
    for (int i = 0; i < 30; ++i) many.push_back(wire::Level{6500000 - i, 1000 + i, 1});
    const std::string s = wire::okxChecksumString(many, {{6500100, 2500000, 1}});
    CHECK(s.rfind("65000:0.001:65001:2.5:64999.99:0.001001:", 0) == 0);
    CHECK_EQ(static_cast<std::size_t>(std::count(s.begin(), s.end(), ':')), std::size_t{(25 + 1) * 2 - 1});
    CHECK_EQ(wire::okxChecksum(many, {{6500100, 2500000, 1}}), asSigned(bitwiseCrc32(s)));
}

TEST_CASE("mock wire: Binance stream names, Bybit topics and HTTP targets parse like the real endpoints") {
    wire::BinanceStream s;
    REQUIRE(wire::parseBinanceStream("btcusdt@depth@100ms", "btcusdt", s));
    CHECK_EQ(s.levels, 0);
    CHECK_EQ(s.period_ms, 100);
    REQUIRE(wire::parseBinanceStream("btcusdt@depth", "btcusdt", s));
    CHECK_EQ(s.period_ms, 1000);
    REQUIRE(wire::parseBinanceStream("btcusdt@depth20@100ms", "btcusdt", s));
    CHECK_EQ(s.levels, 20);
    REQUIRE(wire::parseBinanceStream("btcusdt@depth5", "btcusdt", s));
    CHECK_EQ(s.levels, 5);
    CHECK_EQ(s.period_ms, 1000);
    CHECK(!wire::parseBinanceStream("btcusdt@depth15", "btcusdt", s));
    CHECK(!wire::parseBinanceStream("ethusdt@depth", "btcusdt", s));
    CHECK(!wire::parseBinanceStream("btcusdt@trade", "btcusdt", s));
    CHECK(!wire::parseBinanceStream("btcusdt@depth@250ms", "btcusdt", s));
    int depth = 0;
    CHECK(wire::parseBybitTopic("orderbook.50.BTCUSDT", "BTCUSDT", depth));
    CHECK_EQ(depth, 50);
    CHECK(!wire::parseBybitTopic("orderbook.7.BTCUSDT", "BTCUSDT", depth));
    CHECK(!wire::parseBybitTopic("orderbook.50.ETHUSDT", "BTCUSDT", depth));
    const wire::HttpTarget t = wire::parseHttpTarget("/api/v3/depth?symbol=BTCUSDT&limit=1000&x=%41+b&symbol=ETH");
    CHECK_EQ(t.path, std::string("/api/v3/depth"));
    REQUIRE(t.param("symbol") != nullptr);
    CHECK_EQ(*t.param("symbol"), std::string("BTCUSDT"));  // 重复参数取第一个
    CHECK_EQ(*t.param("limit"), std::string("1000"));
    CHECK_EQ(*t.param("x"), std::string("A b"));
    CHECK(t.param("nope") == nullptr);
}

TEST_CASE("mock exchange: StreamLog delivers messages after a cursor and reports cursors that fell out of retention") {
    StreamLog log(4);
    std::vector<MessagePtr> out;
    std::uint64_t cursor = 0;
    CHECK(log.collect(cursor, out));
    CHECK(out.empty());
    for (int i = 1; i <= 3; ++i) log.append("m" + std::to_string(i));
    CHECK(log.collect(cursor, out));
    REQUIRE_EQ(out.size(), std::size_t{3});
    CHECK_EQ(*out[0], std::string("m1"));
    CHECK_EQ(cursor, std::uint64_t{3});
    out.clear();
    for (int i = 4; i <= 9; ++i) log.append("m" + std::to_string(i));
    std::uint64_t stale = 3;
    CHECK(!log.collect(stale, out));  // m4、m5 已被淘汰
    CHECK(out.empty());
    CHECK_EQ(stale, std::uint64_t{3});
    std::uint64_t ok = 5;  // 下一条是 m6：仍在保留范围内
    CHECK(log.collect(ok, out));
    REQUIRE_EQ(out.size(), std::size_t{4});
    CHECK_EQ(*out.front(), std::string("m6"));
    CHECK_EQ(*out.back(), std::string("m9"));
    CHECK_EQ(log.size(), std::size_t{4});
}

// ============================================================================
// 市场仿真
// ============================================================================

TEST_CASE("mock market: the simulated order flow is deterministic for a seed and differs between seeds") {
    auto fingerprint = [](std::uint64_t seed) {
        sim::Market m(sim::defaultMarketParams(), seed);
        m.advance(20.0);
        std::string s;
        for (std::size_t i = 0; i < m.venueCount(); ++i) {
            const sim::Venue& v = m.venue(i);
            s += std::to_string(v.bookEvents()) + "/" + std::to_string(v.stats().trades) + "/" +
                 std::to_string(v.stats().volume) + ":";
            for (const Side side : {Side::Buy, Side::Sell}) {
                for (const auto& l : v.depth(side, 30)) s += std::to_string(l.price) + "x" + std::to_string(l.qty) + ",";
            }
        }
        return s;
    };
    const std::string a = fingerprint(5);
    CHECK_EQ(a, fingerprint(5));
    CHECK(a != fingerprint(6));
}

TEST_CASE("mock market: agents stay consistent with the engine; books stay two-sided, uncrossed and near fair value") {
    sim::Market m(sim::defaultMarketParams(), 3);
    for (int second = 0; second < 120; ++second) {
        m.advance(1.0);
        for (std::size_t i = 0; i < m.venueCount(); ++i) {
            const sim::Venue& v = m.venue(i);
            std::string why;
            if (!v.checkConsistency(&why)) FAIL(why);
            const auto bid = v.book().bestBid();
            const auto ask = v.book().bestAsk();
            REQUIRE(bid.has_value());
            REQUIRE(ask.has_value());
            REQUIRE(*bid < *ask);
            // 盘口围绕本地公允价（大错位期间也只差几十个基点）
            CHECK_LT(std::fabs(tickPrice(*bid) - v.fairPrice()) / v.fairPrice(), 0.01);
        }
        // 共享公允价缓慢游走，不会漂走
        CHECK_LT(std::fabs(m.fair() - 65000.0) / 65000.0, 0.05);
    }
    for (std::size_t i = 0; i < m.venueCount(); ++i) {
        CHECK_GT(m.venue(i).stats().trades, std::uint64_t{1000});
        CHECK_EQ(m.venue(i).stats().rejects, std::uint64_t{0});
    }
}

TEST_CASE("mock market: cross-venue arbitrage appears at fee 0 and, in occasional episodes, after 0.1% fees") {
    sim::Market m(sim::defaultMarketParams(), 1);
    m.advance(5.0);
    std::uint64_t samples = 0, with_fee0 = 0, with_fee = 0, episodes = 0;
    bool in_episode = false;
    std::array<std::uint64_t, 3> beyond_window{};   // 档位数超过发布窗口（1000 / 400 / 50）的采样数
    const std::array<std::size_t, 3> windows{1000, 400, 50};
    for (int k = 0; k < 3000; ++k) {  // 300 s，每 100 ms 采样一次
        m.advance(0.1);
        ++samples;
        bool any0 = false, any = false;
        for (std::size_t i = 0; i < 3; ++i) {
            for (std::size_t j = 0; j < 3; ++j) {
                if (i == j) continue;
                any0 = any0 || sweepNet(m.venue(i), m.venue(j), 0.0) > 0.01;      // min_profit = 0.01 USDT
                any = any || sweepNet(m.venue(i), m.venue(j), 0.001) > 0.01;
            }
            const auto& book = m.venue(i).book();
            const std::size_t levels = std::min(book.levelCount(Side::Buy), book.levelCount(Side::Sell));
            if (levels > windows[i]) ++beyond_window[i];
        }
        with_fee0 += any0 ? 1 : 0;
        with_fee += any ? 1 : 0;
        if (any && !in_episode) ++episodes;
        in_episode = any;
    }
    const double frac0 = static_cast<double>(with_fee0) / static_cast<double>(samples);
    const double frac = static_cast<double>(with_fee) / static_cast<double>(samples);
    std::printf("    300 s simulated: net > 0.01 USDT in %.1f%% of samples at fee 0, %.2f%% at fee 0.1%% (%llu episodes); "
                "levels beyond the publish window: Binance %.0f%%, OKX %.0f%%, Bybit %.0f%%\n",
                100.0 * frac0, 100.0 * frac, static_cast<unsigned long long>(episodes),
                100.0 * static_cast<double>(beyond_window[0]) / static_cast<double>(samples),
                100.0 * static_cast<double>(beyond_window[1]) / static_cast<double>(samples),
                100.0 * static_cast<double>(beyond_window[2]) / static_cast<double>(samples));
    CHECK_GT(frac0, 0.5);       // 不收费时几乎一直有跨所价差
    CHECK_GT(frac, 0.005);      // 收 0.1% 时偶尔出现（大错位）
    CHECK_LT(frac, 0.3);        // ……但只是偶尔
    CHECK_GE(episodes, std::uint64_t{5});
    // 簿足够深：大部分时间档位数超过各自的发布窗口，窗口边界上的进出在往返测试中都会被覆盖
    for (std::size_t i = 0; i < 3; ++i) CHECK_GT(beyond_window[i] * 2, samples);
}

// ============================================================================
// 发布者经真实适配器往返（无网络）
// ============================================================================

TEST_CASE("mock publishers: Binance diff stream + REST snapshot round-trip through BinanceAdapter, book == engine") {
    sim::Market m(sim::defaultMarketParams(), 11);
    m.advance(5.0);
    const sim::Venue& v = m.venue(0);
    pub::BinanceDepthPublisher pub;
    pub.reset(v.depth(Side::Buy, 1000), v.depth(Side::Sell, 1000), v.bookEvents());
    BinanceAdapter adapter(adapterConfig("Binance"));
    adapter.onConnected();
    AdapterResult out;
    simdjson::dom::parser parser;
    std::string msg;
    std::uint64_t last_u = pub.lastUpdateId();
    int published = 0, compared = 0, rest_at = -1;
    std::size_t max_levels = 0, boundary_deletes = 0;
    for (int tick = 1; tick <= 400; ++tick) {
        m.advance(0.1);
        const wire::Levels bids = v.depth(Side::Buy, 1000);
        const wire::Levels asks = v.depth(Side::Sell, 1000);
        max_levels = std::max({max_levels, v.book().levelCount(Side::Buy), v.book().levelCount(Side::Sell)});
        if (pub.publish(bids, asks, v.bookEvents(), kBaseTs + tick * 100, msg)) {
            ++published;
            // 独立检查报文：U = 上一条 u + 1，u >= U，E 为给定时间
            const simdjson::dom::element doc = parser.parse(msg);
            CHECK_EQ(std::string_view(doc["e"].get_string()), std::string_view("depthUpdate"));
            CHECK_EQ(std::int64_t(doc["E"].get_int64()), kBaseTs + tick * 100);
            const std::uint64_t U = doc["U"].get_uint64();
            const std::uint64_t u = doc["u"].get_uint64();
            CHECK_EQ(U, last_u + 1);
            CHECK_GE(u, U);
            last_u = u;
            // 被挤出窗口的删除：数量 0、但引擎里该价位仍有挂单
            for (simdjson::dom::element e : doc["b"].get_array()) {
                simdjson::dom::array lv = e.get_array();
                double px = 0.0, qty = 0.0;
                REQUIRE(adapter_detail::parseDecimal(std::string_view(lv.at(0).get_string()), px));
                REQUIRE(adapter_detail::parseDecimal(std::string_view(lv.at(1).get_string()), qty));
                if (qty == 0.0 && v.book().volumeAt(Side::Buy, static_cast<sim::Price>(std::llround(px * 100))) > 0) {
                    ++boundary_deletes;
                }
            }
            out.reset();
            adapter.onMessage(msg, out);
            CHECK(out.error.empty());
            if (out.need_rest_snapshot) {
                REQUIRE_EQ(rest_at, -1);
                rest_at = tick + 2;  // 客户端拉取 REST 需要时间：期间又有两条事件被缓冲
            }
        }
        if (tick == rest_at) {
            out.reset();
            adapter.onRestSnapshot(pub.restSnapshot(1000), out);
            CHECK(out.error.empty());
            CHECK(!out.need_rest_snapshot);
            REQUIRE(adapter.synced());
        }
        if (adapter.synced()) {
            std::string why;
            if (!bookEquals(adapter.book(), bids, asks, &why)) FAIL("tick " + std::to_string(tick) + ": " + why);
            CHECK_EQ(adapter.lastUpdateId(), pub.lastUpdateId());
            ++compared;
        }
    }
    const AdapterStats& s = adapter.stats();
    CHECK_GT(published, 390);
    CHECK_GT(compared, 390);
    CHECK_EQ(s.rest_snapshots, std::uint64_t{1});
    CHECK_EQ(s.gaps, std::uint64_t{0});
    CHECK_EQ(s.resyncs, std::uint64_t{0});
    CHECK_EQ(s.parse_errors, std::uint64_t{0});
    CHECK_EQ(s.crossed_books, std::uint64_t{0});
    CHECK_GT(max_levels, std::size_t{1000});      // 引擎簿比窗口深
    CHECK_GT(boundary_deletes, std::size_t{0});   // 窗口边界的进出确实发生过
}

TEST_CASE("mock publishers: Binance 1000 ms stream stays consistent with a REST snapshot taken mid-interval") {
    sim::Market m(sim::defaultMarketParams(), 13);
    m.advance(5.0);
    const sim::Venue& v = m.venue(0);
    pub::BinanceDepthPublisher pub;
    pub.reset(v.depth(Side::Buy, 1000), v.depth(Side::Sell, 1000), v.bookEvents());
    BinanceAdapter adapter(adapterConfig("Binance"));
    adapter.onConnected();
    AdapterResult out;
    std::string fast, slow;
    int rest_at = -1, slow_events = 0, compared = 0;
    for (int tick = 1; tick <= 400; ++tick) {  // 100 ms 一步，40 s
        m.advance(0.1);
        const wire::Levels bids = v.depth(Side::Buy, 1000);
        const wire::Levels asks = v.depth(Side::Sell, 1000);
        pub.publish(bids, asks, v.bookEvents(), kBaseTs + tick * 100, fast);  // 快速流（这个客户端不订阅）
        if (tick % 10 == 0 && pub.publishSlow(kBaseTs + tick * 100, slow)) {
            ++slow_events;
            out.reset();
            adapter.onMessage(slow, out);
            CHECK(out.error.empty());
            if (out.need_rest_snapshot) rest_at = tick + 3;  // REST 快照落在下一个慢速区间的中间
            if (adapter.synced()) {
                std::string why;
                if (!bookEquals(adapter.book(), bids, asks, &why)) FAIL("tick " + std::to_string(tick) + ": " + why);
                ++compared;
            }
        }
        if (tick == rest_at) {
            out.reset();
            adapter.onRestSnapshot(pub.restSnapshot(1000), out);
            REQUIRE(adapter.synced());
            CHECK(!out.need_rest_snapshot);
        }
    }
    CHECK_GE(slow_events, 39);
    CHECK_GE(compared, 37);
    CHECK_EQ(adapter.stats().gaps, std::uint64_t{0});
    CHECK_EQ(adapter.stats().resyncs, std::uint64_t{0});
}

TEST_CASE("mock publishers: OKX books snapshot + updates round-trip through OkxAdapter; every checksum verified independently") {
    sim::Market m(sim::defaultMarketParams(), 12);
    m.advance(5.0);
    const sim::Venue& v = m.venue(1);
    pub::OkxBooksPublisher pub;
    pub.reset(v.depth(Side::Buy, 400), v.depth(Side::Sell, 400), static_cast<std::int64_t>(v.bookEvents()), kBaseTs);
    std::string msg;
    for (int tick = 1; tick <= 5; ++tick) {  // 客户端订阅之前，推送流已经在走
        m.advance(0.1);
        pub.publish(v.depth(Side::Buy, 400), v.depth(Side::Sell, 400), static_cast<std::int64_t>(v.bookEvents()),
                    kBaseTs + tick * 100, msg);
    }
    OkxAdapter adapter(adapterConfig("OKX", kOkxBooksSub));
    adapter.onConnected();
    simdjson::dom::parser parser;
    OkxStringModel model;
    AdapterResult out;
    const std::string snap = pub.snapshot(kBaseTs + 600);
    {
        const simdjson::dom::element doc = parser.parse(snap);
        CHECK_EQ(std::string_view(doc["action"].get_string()), std::string_view("snapshot"));
        const simdjson::dom::element d = doc["data"].at(0);
        CHECK_EQ(std::int64_t(d["prevSeqId"].get_int64()), std::int64_t{-1});
        CHECK_EQ(std::int64_t(d["seqId"].get_int64()), pub.lastSeqId());
        model.apply(d, true);
        CHECK_EQ(model.checksum(), std::int32_t(std::int64_t(d["checksum"].get_int64())));
        CHECK_EQ(model.bids.size(), std::size_t{400});
    }
    adapter.onMessage(snap, out);
    REQUIRE(adapter.synced());
    std::int64_t last_seq = pub.lastSeqId();
    int updates = 0;
    std::size_t orders_checked = 0;
    for (int tick = 6; tick <= 400; ++tick) {
        m.advance(0.1);
        const wire::Levels bids = v.depth(Side::Buy, 400);
        const wire::Levels asks = v.depth(Side::Sell, 400);
        if (!pub.publish(bids, asks, static_cast<std::int64_t>(v.bookEvents()), kBaseTs + tick * 100, msg)) continue;
        ++updates;
        const simdjson::dom::element doc = parser.parse(msg);
        CHECK_EQ(std::string_view(doc["action"].get_string()), std::string_view("update"));
        const simdjson::dom::element d = doc["data"].at(0);
        CHECK_EQ(std::int64_t(d["prevSeqId"].get_int64()), last_seq);
        last_seq = d["seqId"].get_int64();
        CHECK_GT(last_seq, std::int64_t(d["prevSeqId"].get_int64()));
        CHECK_EQ(std::string(std::string_view(d["ts"].get_string())), std::to_string(kBaseTs + tick * 100));
        model.apply(d, false);
        const std::int32_t expected = std::int32_t(std::int64_t(d["checksum"].get_int64()));
        if (model.checksum() != expected) FAIL("independent checksum mismatch at tick " + std::to_string(tick));
        // 第 4 个字段是该价位的订单数（删除档为 "0"）
        for (simdjson::dom::element e : d["bids"].get_array()) {
            simdjson::dom::array lv = e.get_array();
            const std::string sz(std::string_view(lv.at(1).get_string()));
            const std::string n(std::string_view(lv.at(3).get_string()));
            const std::string px(std::string_view(lv.at(0).get_string()));
            const auto p = static_cast<sim::Price>(std::llround(std::stod(px) * 100));
            const auto it = std::find_if(bids.begin(), bids.end(), [p](const wire::Level& l) { return l.price == p; });
            if (std::stod(sz) == 0.0) {
                CHECK_EQ(n, std::string("0"));
            } else {
                REQUIRE(it != bids.end());
                CHECK_EQ(n, std::to_string(it->orders));
                ++orders_checked;
            }
        }
        out.reset();
        adapter.onMessage(msg, out);
        CHECK(out.error.empty());
        std::string why;
        if (!bookEquals(adapter.book(), bids, asks, &why)) FAIL("tick " + std::to_string(tick) + ": " + why);
        CHECK_EQ(adapter.lastSeqId(), last_seq);
    }
    CHECK_GT(updates, 380);
    CHECK_GT(orders_checked, std::size_t{100});
    CHECK(adapter.checksumEnabled());  // 快照校验也通过了（否则安全阀会关闭校验）
    CHECK_EQ(adapter.stats().checksum_failures, std::uint64_t{0});
    CHECK_EQ(adapter.stats().gaps, std::uint64_t{0});
    CHECK_EQ(adapter.stats().resyncs, std::uint64_t{0});
    CHECK_EQ(adapter.stats().parse_errors, std::uint64_t{0});
}

TEST_CASE("mock publishers: OKX heartbeat after an idle period passes the adapter's continuity and checksum checks") {
    const wire::Levels bids{{6500000, 1000000, 2}, {6499900, 2000000, 1}};
    const wire::Levels asks{{6500100, 1500000, 1}};
    pub::OkxBooksPublisher pub("BTC-USDT", 400, 1000);
    pub.reset(bids, asks, 500, kBaseTs);
    OkxAdapter adapter(adapterConfig("OKX", kOkxBooksSub));
    adapter.onConnected();
    AdapterResult out;
    adapter.onMessage(pub.snapshot(kBaseTs), out);
    REQUIRE(adapter.synced());
    std::string msg;
    CHECK(!pub.publish(bids, asks, 500, kBaseTs + 500, msg));   // 没有变化、未到心跳间隔：不推送
    REQUIRE(pub.publish(bids, asks, 500, kBaseTs + 1100, msg));  // 心跳
    CHECK(contains(msg, R"("asks":[],"bids":[])"));
    CHECK(contains(msg, R"("prevSeqId":500,"seqId":500)"));
    out.reset();
    adapter.onMessage(msg, out);
    CHECK(!out.book_updated);
    CHECK(out.send.empty());
    CHECK(out.error.empty());
    const wire::Levels bids2{{6500000, 1100000, 3}, {6499900, 2000000, 1}};
    REQUIRE(pub.publish(bids2, asks, 507, kBaseTs + 1200, msg));
    CHECK(contains(msg, R"("prevSeqId":500,"seqId":507)"));
    out.reset();
    adapter.onMessage(msg, out);
    CHECK(out.book_updated);
    CHECK_EQ(adapter.stats().checksum_failures, std::uint64_t{0});
    CHECK_EQ(adapter.stats().gaps, std::uint64_t{0});
    CHECK(adapter.checksumEnabled());
    std::string why;
    CHECK(bookEquals(adapter.book(), bids2, asks, &why));
}

TEST_CASE("mock publishers: OKX books5 / bbo-tbt push the full top levels only when they change") {
    sim::Market m(sim::defaultMarketParams(), 14);
    m.advance(5.0);
    const sim::Venue& v = m.venue(1);
    pub::OkxTopPublisher books5("books5", 5), bbo("bbo-tbt", 1);
    books5.reset(v.depth(Side::Buy, 5), v.depth(Side::Sell, 5), 1);
    bbo.reset(v.depth(Side::Buy, 1), v.depth(Side::Sell, 1), 1);
    OkxAdapter a5(adapterConfig("OKX", R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})"));
    OkxAdapter a1(adapterConfig("OKX", R"({"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT"}]})"));
    a5.onConnected();
    a1.onConnected();
    AdapterResult out;
    a5.onMessage(books5.current(kBaseTs), out);
    a1.onMessage(bbo.current(kBaseTs), out);
    std::string msg;
    int pushes5 = 0, pushes1 = 0;
    for (int tick = 1; tick <= 200; ++tick) {
        m.advance(0.01);
        const auto seq = static_cast<std::int64_t>(v.bookEvents());
        if (bbo.publish(v.depth(Side::Buy, 1), v.depth(Side::Sell, 1), seq, kBaseTs + tick * 10, msg)) {
            ++pushes1;
            CHECK(contains(msg, R"({"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"asks":)"));
            out.reset();
            a1.onMessage(msg, out);
        }
        if (tick % 10 == 0 &&
            books5.publish(v.depth(Side::Buy, 5), v.depth(Side::Sell, 5), seq, kBaseTs + tick * 10, msg)) {
            ++pushes5;
            out.reset();
            a5.onMessage(msg, out);
        }
        std::string why;
        if (!bookEquals(a1.book(), v.depth(Side::Buy, 1), v.depth(Side::Sell, 1), &why)) FAIL("bbo-tbt: " + why);
        if (tick % 10 == 0 && !bookEquals(a5.book(), v.depth(Side::Buy, 5), v.depth(Side::Sell, 5), &why)) {
            FAIL("books5: " + why);
        }
    }
    CHECK_GT(pushes1, 20);
    CHECK_LT(pushes1, 200);  // 只在变化时推送
    CHECK_GT(pushes5, 10);
    CHECK_EQ(a5.stats().parse_errors + a1.stats().parse_errors, std::uint64_t{0});
}

TEST_CASE("mock publishers: Bybit orderbook.{1,50,200} snapshot + deltas round-trip through BybitAdapter (u strictly +1)") {
    sim::Market m(sim::defaultMarketParams(), 15);
    m.advance(5.0);
    const sim::Venue& v = m.venue(2);
    for (const std::size_t depth : {std::size_t{1}, std::size_t{50}, std::size_t{200}}) {
        pub::BybitBookPublisher pub(depth);
        pub.reset(v.depth(Side::Buy, depth), v.depth(Side::Sell, depth), v.bookEvents());
        const std::string sub = R"({"op":"subscribe","args":[")" + pub.topic() + R"("]})";
        BybitAdapter adapter(adapterConfig("Bybit", sub));
        adapter.onConnected();
        AdapterResult out;
        simdjson::dom::parser parser;
        const std::string snap = pub.snapshot(kBaseTs, kBaseTs);
        {
            const simdjson::dom::element doc = parser.parse(snap);
            CHECK_EQ(std::string_view(doc["type"].get_string()), std::string_view("snapshot"));
            CHECK_EQ(std::string_view(doc["topic"].get_string()), std::string_view(pub.topic()));
            CHECK_EQ(std::uint64_t(doc["data"]["u"].get_uint64()), pub.lastUpdateId());
        }
        adapter.onMessage(snap, out);
        REQUIRE(adapter.synced());
        std::uint64_t u = pub.lastUpdateId();
        std::uint64_t seq = pub.seq();
        std::string msg;
        int deltas = 0;
        for (int tick = 1; tick <= 500; ++tick) {  // 20 ms 一步，10 s
            m.advance(0.02);
            const wire::Levels bids = v.depth(Side::Buy, depth);
            const wire::Levels asks = v.depth(Side::Sell, depth);
            if (!pub.publish(bids, asks, v.bookEvents(), kBaseTs + tick * 20, kBaseTs + tick * 20, msg)) continue;
            ++deltas;
            const simdjson::dom::element doc = parser.parse(msg);
            CHECK_EQ(std::string_view(doc["type"].get_string()), std::string_view("delta"));
            CHECK_EQ(std::uint64_t(doc["data"]["u"].get_uint64()), u + 1);
            const std::uint64_t new_seq = doc["data"]["seq"].get_uint64();
            CHECK_GT(new_seq, seq);
            u += 1;
            seq = new_seq;
            out.reset();
            adapter.onMessage(msg, out);
            CHECK(out.error.empty());
            std::string why;
            if (!bookEquals(adapter.book(), bids, asks, &why)) {
                FAIL("depth " + std::to_string(depth) + ", tick " + std::to_string(tick) + ": " + why);
            }
        }
        CHECK_GT(deltas, 100);
        CHECK(adapter.strictSequence());
        CHECK_EQ(adapter.lastUpdateId(), u);
        CHECK_EQ(adapter.stats().gaps, std::uint64_t{0});
        CHECK_EQ(adapter.stats().resyncs, std::uint64_t{0});
        CHECK_EQ(adapter.stats().parse_errors, std::uint64_t{0});
    }
}

TEST_CASE("mock publishers: a subscriber joining later (fresh snapshot) converges to the same book as an early one") {
    sim::Market m(sim::defaultMarketParams(), 16);
    m.advance(5.0);
    const sim::Venue& v = m.venue(2);
    pub::BybitBookPublisher pub(50);
    pub.reset(v.depth(Side::Buy, 50), v.depth(Side::Sell, 50), v.bookEvents());
    BybitAdapter early(adapterConfig("Bybit", kBybitSub)), late(adapterConfig("Bybit", kBybitSub));
    early.onConnected();
    late.onConnected();
    AdapterResult out;
    early.onMessage(pub.snapshot(kBaseTs, kBaseTs), out);
    std::string msg;
    for (int tick = 1; tick <= 300; ++tick) {
        m.advance(0.02);
        if (tick == 150) late.onMessage(pub.snapshot(kBaseTs + tick * 20, kBaseTs + tick * 20), out);
        if (!pub.publish(v.depth(Side::Buy, 50), v.depth(Side::Sell, 50), v.bookEvents(), kBaseTs + tick * 20,
                         kBaseTs + tick * 20, msg)) {
            continue;
        }
        early.onMessage(msg, out);
        if (tick >= 150) late.onMessage(msg, out);
    }
    std::string why;
    CHECK(bookEquals(early.book(), v.depth(Side::Buy, 50), v.depth(Side::Sell, 50), &why));
    CHECK(bookEquals(late.book(), v.depth(Side::Buy, 50), v.depth(Side::Sell, 50), &why));
    CHECK_EQ(early.stats().gaps + late.stats().gaps, std::uint64_t{0});
}

// ============================================================================
// 网络层
// ============================================================================

TEST_CASE("mock server: SessionWaker wakes a waiting script within the poll slice and is inert after the session ends") {
    MockExchangeServer server;
    constexpr int kRounds = 15;
    std::atomic<int> round{0};
    std::array<std::atomic<std::int64_t>, kRounds> woke_ns{};
    std::mutex mutex;
    SessionWaker waker;
    std::atomic<bool> ready{false};
    server.setWebSocketScript([&](WsSession& ws) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            waker = ws.waker();
        }
        ready.store(true);
        for (int i = 0; i < kRounds; ++i) {
            if (!ws.waitUntil([&] { return round.load() > i; }, 5000)) return;
            woke_ns[static_cast<std::size_t>(i)].store(nowNs());
            ws.send("woke " + std::to_string(i));
        }
    });
    auto client = std::make_unique<WsClient>(server.port(), "/", server.caFile());
    REQUIRE(client->open());
    REQUIRE(waitFor([&] { return ready.load(); }, 2000));
    std::vector<double> latency_ms;
    std::mt19937 rng(1);
    for (int i = 0; i < kRounds; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(3 + static_cast<int>(rng() % 8)));  // 让脚本停在轮询片中间
        const std::int64_t t0 = nowNs();
        round.store(i + 1);
        {
            std::lock_guard<std::mutex> lock(mutex);
            waker.wake();
        }
        CHECK_EQ(client->read(), "woke " + std::to_string(i));
        latency_ms.push_back(static_cast<double>(woke_ns[static_cast<std::size_t>(i)].load() - t0) / 1e6);
    }
    std::sort(latency_ms.begin(), latency_ms.end());
    const double median = latency_ms[latency_ms.size() / 2];
    std::printf("    wake latency median %.3f ms, max %.3f ms (without a wake the 10 ms poll slice gives ~5 ms)\n", median,
                latency_ms.back());
    CHECK_LT(median, 4.0);
    // 客户端断开 → 会话结束（teardown 使句柄失效）→ 之后的 wake() 是空操作（ASan 下验证不会访问已销毁的 io_context）
    client.reset();
    REQUIRE(waitFor([&] { return server.openConnections() == 0; }, 3000));
    {
        std::lock_guard<std::mutex> lock(mutex);
        CHECK(static_cast<bool>(waker));
        for (int i = 0; i < 3; ++i) waker.wake();
    }
    SessionWaker empty;
    empty.wake();
    CHECK(!empty);
}

TEST_CASE("mock exchange: OKX protocol over TLS (ping, subscribe ack + snapshot, chained updates, errors, unsubscribe)") {
    MockExchange ex(testOptions(21));
    const TempFile ca(ex.caPem(), "mock_ca");
    WsClient c(ex.okxPort(), "/ws/v5/public", ca.path());
    REQUIRE(c.open());
    c.send("ping");
    CHECK_EQ(c.read(), std::string("pong"));
    c.send(R"({"id":"req7","op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})");
    simdjson::dom::parser parser;
    {
        const std::string ack = c.read();
        const simdjson::dom::element doc = parser.parse(ack);
        CHECK_EQ(std::string_view(doc["event"].get_string()), std::string_view("subscribe"));
        CHECK_EQ(std::string_view(doc["id"].get_string()), std::string_view("req7"));
        CHECK_EQ(std::string_view(doc["arg"]["channel"].get_string()), std::string_view("books"));
        CHECK_EQ(std::string_view(doc["connId"].get_string()).size(), std::size_t{8});
    }
    OkxStringModel model;
    std::int64_t seq = 0;
    {
        const std::string snap = c.read();
        const simdjson::dom::element doc = parser.parse(snap);
        CHECK_EQ(std::string_view(doc["action"].get_string()), std::string_view("snapshot"));
        const simdjson::dom::element d = doc["data"].at(0);
        CHECK_EQ(std::int64_t(d["prevSeqId"].get_int64()), std::int64_t{-1});
        seq = d["seqId"].get_int64();
        model.apply(d, true);
        CHECK_EQ(model.checksum(), std::int32_t(std::int64_t(d["checksum"].get_int64())));
        CHECK_EQ(model.bids.size(), std::size_t{400});
        CHECK_EQ(model.asks.size(), std::size_t{400});
    }
    for (int i = 0; i < 10; ++i) {  // 增量严格衔接快照，每条的 checksum 都对
        const std::string upd = c.read();
        const simdjson::dom::element doc = parser.parse(upd);
        REQUIRE_EQ(std::string_view(doc["action"].get_string()), std::string_view("update"));
        const simdjson::dom::element d = doc["data"].at(0);
        CHECK_EQ(std::int64_t(d["prevSeqId"].get_int64()), seq);
        seq = d["seqId"].get_int64();
        model.apply(d, false);
        CHECK_EQ(model.checksum(), std::int32_t(std::int64_t(d["checksum"].get_int64())));
    }
    auto isError = [](const std::string& m) { return contains(m, R"("event":"error")"); };
    c.send(R"({"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]})");
    CHECK(contains(c.readUntil(isError), R"("code":"60018")"));
    c.send(R"({"op":"subscribe","args":[{"channel":"books","instId":"ETH-USDT"}]})");
    CHECK(contains(c.readUntil(isError), "instId:ETH-USDT doesn't exist"));
    c.send("{not json");
    CHECK(contains(c.readUntil(isError), R"("code":"60012")"));
    c.send(R"({"op":"unsubscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})");
    c.readUntil([](const std::string& m) { return contains(m, R"("event":"unsubscribe")"); });
    // 退订之后不再收到 books；订阅 books5，先收到它的 ack 与当前前 5 档
    c.send(R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})");
    CHECK(contains(c.read(), R"("event":"subscribe","arg":{"channel":"books5")"));
    for (int i = 0; i < 3; ++i) {
        const std::string m = c.read();
        CHECK(contains(m, R"({"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"asks":)"));
        CHECK(!contains(m, R"("action")"));
    }
    CHECK(ex.serverErrors().empty());
    CHECK_GE(ex.okx().counters().bad_requests, std::uint64_t{3});
}

TEST_CASE("mock exchange: Bybit protocol over TLS (ping/pong, subscribe response + snapshot, u+1 deltas, errors)") {
    MockExchange ex(testOptions(22));
    const TempFile ca(ex.caPem(), "mock_ca");
    WsClient c(ex.bybitPort(), "/v5/public/spot", ca.path());
    REQUIRE(c.open());
    simdjson::dom::parser parser;
    c.send(R"({"req_id":"p1","op":"ping"})");
    {
        const simdjson::dom::element doc = parser.parse(c.read());
        CHECK(bool(doc["success"].get_bool()));
        CHECK_EQ(std::string_view(doc["ret_msg"].get_string()), std::string_view("pong"));
        CHECK_EQ(std::string_view(doc["req_id"].get_string()), std::string_view("p1"));
        CHECK_EQ(std::string_view(doc["op"].get_string()), std::string_view("ping"));
    }
    c.send(R"({"req_id":"s1","op":"subscribe","args":["orderbook.50.BTCUSDT"]})");
    {
        const simdjson::dom::element doc = parser.parse(c.read());
        CHECK(bool(doc["success"].get_bool()));
        CHECK_EQ(std::string_view(doc["op"].get_string()), std::string_view("subscribe"));
    }
    std::uint64_t u = 0;
    {
        const simdjson::dom::element doc = parser.parse(c.read());
        CHECK_EQ(std::string_view(doc["type"].get_string()), std::string_view("snapshot"));
        CHECK_EQ(std::string_view(doc["topic"].get_string()), std::string_view("orderbook.50.BTCUSDT"));
        u = doc["data"]["u"].get_uint64();
        CHECK_EQ(doc["data"]["b"].get_array().size(), std::size_t{50});
        CHECK_EQ(doc["data"]["a"].get_array().size(), std::size_t{50});
    }
    for (int i = 0; i < 20; ++i) {
        const simdjson::dom::element doc = parser.parse(c.read());
        REQUIRE_EQ(std::string_view(doc["type"].get_string()), std::string_view("delta"));
        CHECK_EQ(std::uint64_t(doc["data"]["u"].get_uint64()), u + 1);
        u += 1;
    }
    auto isResponse = [](const std::string& m) { return contains(m, R"("success":)"); };
    c.send(R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})");
    CHECK(contains(c.readUntil(isResponse), "already subscribed"));
    c.send(R"({"op":"subscribe","args":["orderbook.7.BTCUSDT"]})");
    CHECK(contains(c.readUntil(isResponse), R"("success":false)"));
    c.send("not json");
    CHECK(contains(c.readUntil(isResponse), R"("success":false)"));
    c.send(R"({"op":"unsubscribe","args":["orderbook.50.BTCUSDT"]})");
    CHECK(contains(c.readUntil(isResponse), R"("success":true)"));
    // 退订之后立即订阅 orderbook.1：下一条推送是它的快照（不再有 orderbook.50 的 delta）
    c.send(R"({"op":"subscribe","args":["orderbook.1.BTCUSDT"]})");
    CHECK(contains(c.readUntil(isResponse), R"("success":true)"));
    const std::string first = c.read();
    CHECK(contains(first, R"("topic":"orderbook.1.BTCUSDT","type":"snapshot")"));
    CHECK(ex.serverErrors().empty());
}

TEST_CASE("mock exchange: Binance protocol over TLS (methods, combined streams, partial depth, REST endpoints, 404s)") {
    MockExchange ex(testOptions(23));
    const TempFile ca(ex.caPem(), "mock_ca");
    simdjson::dom::parser parser;
    {
        WsClient c(ex.binancePort(), "/ws", ca.path());
        REQUIRE(c.open());
        c.send(R"({"method":"SUBSCRIBE","params":["btcusdt@depth@100ms"],"id":1})");
        CHECK_EQ(c.read(), std::string(R"({"result":null,"id":1})"));
        std::uint64_t last_u = 0;
        for (int i = 0; i < 5; ++i) {
            const simdjson::dom::element doc = parser.parse(c.read());
            REQUIRE_EQ(std::string_view(doc["e"].get_string()), std::string_view("depthUpdate"));
            const std::uint64_t U = doc["U"].get_uint64();
            if (last_u != 0) CHECK_EQ(U, last_u + 1);
            last_u = doc["u"].get_uint64();
        }
        auto isReply = [](const std::string& m) { return contains(m, R"("id":)"); };
        c.send(R"({"method":"LIST_SUBSCRIPTIONS","id":"q2"})");
        CHECK_EQ(c.readUntil(isReply), std::string(R"({"result":["btcusdt@depth@100ms"],"id":"q2"})"));
        c.send(R"({"method":"SUBSCRIBE","params":["ethusdt@depth"],"id":3})");
        CHECK(contains(c.readUntil(isReply), R"("code":2)"));
        c.send(R"({"method":"FROBNICATE","id":4})");
        CHECK(contains(c.readUntil(isReply), "unknown variant"));
        c.send(R"({"method":"GET_PROPERTY","params":["combined"],"id":5})");
        CHECK_EQ(c.readUntil(isReply), std::string(R"({"result":false,"id":5})"));
        c.send(R"({"method":"UNSUBSCRIBE","params":["btcusdt@depth@100ms"],"id":6})");
        CHECK_EQ(c.readUntil(isReply), std::string(R"({"result":null,"id":6})"));
        c.send(R"({"method":"LIST_SUBSCRIPTIONS","id":7})");
        CHECK_EQ(c.read(), std::string(R"({"result":[],"id":7})"));  // 退订之后不再有推送插在中间
    }
    {
        WsClient c(ex.binancePort(), "/stream?streams=btcusdt@depth5@100ms/btcusdt@depth@100ms", ca.path());
        REQUIRE(c.open());
        bool partial = false, diff = false;
        for (int i = 0; i < 20 && !(partial && diff); ++i) {
            const simdjson::dom::element doc = parser.parse(c.read());
            const std::string_view stream = doc["stream"].get_string();
            if (stream == "btcusdt@depth5@100ms") {
                partial = true;
                CHECK_EQ(doc["data"]["bids"].get_array().size(), std::size_t{5});
                CHECK_EQ(doc["data"]["asks"].get_array().size(), std::size_t{5});
            } else {
                CHECK_EQ(stream, std::string_view("btcusdt@depth@100ms"));
                CHECK_EQ(std::string_view(doc["data"]["e"].get_string()), std::string_view("depthUpdate"));
                diff = true;
            }
        }
        CHECK(partial);
        CHECK(diff);
    }
    {
        WsClient bad(ex.binancePort(), "/ws/ethusdt@depth", ca.path());
        CHECK(!bad.open());
        CHECK_EQ(bad.responseStatus(), 404);
        WsClient wrong(ex.binancePort(), "/ws/v5/public", ca.path());
        CHECK(!wrong.open());
        WsClient okx_path(ex.okxPort(), "/ws/btcusdt@depth", ca.path());
        CHECK(!okx_path.open());
    }
    const hft::net::HttpResult snap = restGet(ex.binancePort(), "/api/v3/depth?symbol=BTCUSDT&limit=5", ca.path());
    REQUIRE(snap.ok);
    CHECK_EQ(snap.status, 200);
    {
        const simdjson::dom::element doc = parser.parse(snap.body);
        CHECK_EQ(doc["bids"].get_array().size(), std::size_t{5});
        CHECK_EQ(doc["asks"].get_array().size(), std::size_t{5});
        CHECK_GT(std::uint64_t(doc["lastUpdateId"].get_uint64()), std::uint64_t{0});
    }
    const hft::net::HttpResult deflt = restGet(ex.binancePort(), "/api/v3/depth?symbol=BTCUSDT", ca.path());
    CHECK_EQ(parser.parse(deflt.body)["bids"].get_array().size(), std::size_t{100});  // 默认 limit = 100
    struct Case {
        const char* target;
        int status;
        const char* code;
    };
    for (const Case& k : {Case{"/api/v3/depth?symbol=ETHUSDT", 400, "-1121"}, Case{"/api/v3/depth?symbol=btcusdt", 400, "-1100"},
                          Case{"/api/v3/depth", 400, "-1102"}, Case{"/api/v3/depth?symbol=BTCUSDT&limit=abc", 400, "-1100"},
                          Case{"/api/v3/nope", 404, "-1"}}) {
        const hft::net::HttpResult r = restGet(ex.binancePort(), k.target, ca.path());
        REQUIRE(r.ok);
        CHECK_EQ(r.status, k.status);
        CHECK(contains(r.body, std::string(R"("code":)") + k.code + ","));
    }
    CHECK_EQ(restGet(ex.binancePort(), "/api/v3/ping", ca.path()).body, std::string("{}"));
    CHECK(contains(restGet(ex.binancePort(), "/api/v3/time", ca.path()).body, R"("serverTime":)"));
    CHECK_EQ(restGet(ex.okxPort(), "/api/v3/depth?symbol=BTCUSDT", ca.path()).status, 404);
    CHECK(ex.serverErrors().empty());
}

TEST_CASE("mock exchange e2e: three real MarketDataFeeds go LIVE over TLS and their books match the engines exactly") {
    MockExchange ex(testOptions(7));
    const TempFile ca(ex.caPem(), "mock_ca");
    std::array<std::unique_ptr<FeedRunner>, 3> feeds;
    const std::array<Exchange, 3> venues{Exchange::BINANCE, Exchange::OKX, Exchange::BYBIT};
    for (std::size_t i = 0; i < 3; ++i) feeds[i] = std::make_unique<FeedRunner>(feedConfig(venues[i], ex, ca.path()));
    std::array<BookSnapshot, 3> last{};
    std::array<std::uint64_t, 3> popped{};
    auto pump = [&] {
        for (std::size_t i = 0; i < 3; ++i) {
            BookSnapshot s;
            while (feeds[i]->feed.queue().tryPop(s)) {
                last[i] = s;
                ++popped[i];
            }
            feeds[i]->feed.checkHealth(hft::nowNs());
        }
    };
    const std::int64_t t0 = hft::nowNs();
    while (hft::nowNs() - t0 < 3'000'000'000LL) {  // 实时运行 3 s
        pump();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    for (std::size_t i = 0; i < 3; ++i) {
        CHECK_EQ(feedStatusName(feeds[i]->feed.status()), std::string("LIVE"));
        if (!feeds[i]->feed.lastError().empty()) {
            std::printf("    %s lastError: %s\n", exchangeName(venues[i]), feeds[i]->feed.lastError().c_str());
        }
    }
    // 暂停并立即发布一次：各发布者的窗口 = 引擎此刻的深度。等 feed 消费完（快照的更新号追上发布者）再逐档比较。
    ex.pause(true);
    const std::array<std::uint64_t, 3> want{ex.binance().lastUpdateId(), static_cast<std::uint64_t>(ex.okx().lastSeqId()),
                                            ex.bybit().lastUpdateId(1)};
    const bool caught_up = waitFor(
        [&] {
            pump();
            return last[0].update_id == want[0] && last[1].update_id == want[1] && last[2].update_id == want[2];
        },
        5000);
    CHECK(caught_up);
    ex.withMarket([&](const sim::Market& m) {
        for (std::size_t i = 0; i < 3; ++i) {
            std::string why;
            const wire::Levels bids = m.venue(i).depth(Side::Buy, BookSnapshot::kMaxLevels);
            const wire::Levels asks = m.venue(i).depth(Side::Sell, BookSnapshot::kMaxLevels);
            if (!snapshotEquals(last[i], bids, asks, &why)) {
                FAIL(std::string(exchangeName(venues[i])) + " published book differs from the engine: " + why);
            }
            CHECK_EQ(last[i].venue, static_cast<std::uint32_t>(venues[i]));
        }
    });
    std::printf("    %-8s %6s %9s %6s %5s %8s %8s %9s %7s\n", "feed", "msgs", "published", "gaps", "csum", "resyncs",
                "rest", "proc p50", "exch p50");
    for (std::size_t i = 0; i < 3; ++i) {
        const FeedStats s = feeds[i]->feed.stats();
        std::printf("    %-8s %6llu %9llu %6llu %5llu %8llu %8llu %7.2fus %5.2fms\n", exchangeName(venues[i]),
                    static_cast<unsigned long long>(s.messages), static_cast<unsigned long long>(s.published),
                    static_cast<unsigned long long>(s.gaps), static_cast<unsigned long long>(s.checksum_failures),
                    static_cast<unsigned long long>(s.resyncs), static_cast<unsigned long long>(s.rest_snapshots),
                    feeds[i]->feed.getLatencyTracker().getP50Us(), feeds[i]->feed.getExchangeLatency().getP50Us() / 1000.0);
        CHECK_GT(s.published, std::uint64_t{10});
        CHECK_EQ(s.gaps, std::uint64_t{0});
        CHECK_EQ(s.checksum_failures, std::uint64_t{0});
        CHECK_EQ(s.resyncs, std::uint64_t{0});
        CHECK_EQ(s.parse_errors, std::uint64_t{0});
        CHECK_EQ(s.reconnects, std::uint64_t{0});
        CHECK_EQ(s.connect_failures, std::uint64_t{0});
        CHECK_EQ(s.dropped_snapshots, std::uint64_t{0});
        CHECK_LT(feeds[i]->feed.getExchangeLatency().getP50Us(), 50000.0);  // 本机：交易所时间戳与接收相差几毫秒
    }
    CHECK_GT(feeds[2]->feed.stats().messages, feeds[1]->feed.stats().messages);  // Bybit 20 ms、OKX 100 ms
    CHECK_EQ(feeds[0]->feed.stats().rest_snapshots, std::uint64_t{1});
    CHECK_EQ(ex.binance().counters().rest_requests, std::uint64_t{1});
    for (std::size_t i = 0; i < 3; ++i) feeds[i]->stop();
    ex.resume();
    CHECK(ex.serverErrors().empty());
}

TEST_CASE("mock exchange e2e: injected drops -> every feed detects gaps, resyncs through the mock (REST / resubscribe) and converges") {
    MockExchange ex(testOptions(41));
    const TempFile ca(ex.caPem(), "mock_ca");
    // Bybit 每 20 ms 一条，丢弃率取低一些（各家每秒的缺口数相近，Bybit 适配器的安全阀也不会被连续丢弃误触发）
    ex.setDropRate(0, 0.08);
    ex.setDropRate(1, 0.08);
    ex.setDropRate(2, 0.02);
    std::array<std::unique_ptr<FeedRunner>, 3> feeds;
    const std::array<Exchange, 3> venues{Exchange::BINANCE, Exchange::OKX, Exchange::BYBIT};
    for (std::size_t i = 0; i < 3; ++i) feeds[i] = std::make_unique<FeedRunner>(feedConfig(venues[i], ex, ca.path()));
    std::array<BookSnapshot, 3> last{};
    auto pump = [&] {
        for (std::size_t i = 0; i < 3; ++i) {
            BookSnapshot s;
            while (feeds[i]->feed.queue().tryPop(s)) last[i] = s;
            feeds[i]->feed.checkHealth(hft::nowNs());
        }
    };
    const bool gaps_seen = waitFor(
        [&] {
            pump();
            for (const auto& f : feeds) {
                if (f->feed.stats().gaps < 2) return false;
            }
            return true;
        },
        20000, 2);
    CHECK(gaps_seen);
    // 停止丢弃，让每个 feed 用干净的推送发现可能残留的缺口并完成重同步
    ex.setDropRate(0.0);
    const std::int64_t t0 = hft::nowNs();
    while (hft::nowNs() - t0 < 800'000'000LL) {
        pump();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ex.pause(true);
    const std::array<std::uint64_t, 3> want{ex.binance().lastUpdateId(), static_cast<std::uint64_t>(ex.okx().lastSeqId()),
                                            ex.bybit().lastUpdateId(1)};
    CHECK(waitFor(
        [&] {
            pump();
            return last[0].update_id == want[0] && last[1].update_id == want[1] && last[2].update_id == want[2];
        },
        5000));
    ex.withMarket([&](const sim::Market& m) {
        for (std::size_t i = 0; i < 3; ++i) {
            std::string why;
            if (!snapshotEquals(last[i], m.venue(i).depth(Side::Buy, BookSnapshot::kMaxLevels),
                                m.venue(i).depth(Side::Sell, BookSnapshot::kMaxLevels), &why)) {
                FAIL(std::string(exchangeName(venues[i])) + " did not converge after resyncs: " + why);
            }
        }
    });
    for (std::size_t i = 0; i < 3; ++i) {
        const FeedStats s = feeds[i]->feed.stats();
        const SessionHub::Counters net =
            i == 0 ? ex.binance().counters() : i == 1 ? ex.okx().counters() : ex.bybit().counters();
        std::printf("    %-8s dropped %3llu -> gaps %3llu, resyncs %3llu, REST snapshots %3llu, client frames %3llu, "
                    "reconnects %llu, status %s\n",
                    exchangeName(venues[i]), static_cast<unsigned long long>(net.dropped),
                    static_cast<unsigned long long>(s.gaps), static_cast<unsigned long long>(s.resyncs),
                    static_cast<unsigned long long>(s.rest_snapshots), static_cast<unsigned long long>(net.requests),
                    static_cast<unsigned long long>(s.reconnects), feedStatusName(feeds[i]->feed.status()));
        CHECK_EQ(feeds[i]->feed.status(), FeedStatus::Live);
        CHECK_GE(s.gaps, std::uint64_t{2});
        CHECK_GE(s.resyncs, s.gaps);
        CHECK_GE(net.dropped, std::uint64_t{2});
        CHECK_EQ(s.checksum_failures, std::uint64_t{0});  // 缺口先于校验和被发现
        CHECK_EQ(s.parse_errors, std::uint64_t{0});
        CHECK_EQ(s.reconnects, std::uint64_t{0});          // 全部在同一连接内恢复
    }
    // Binance：每个缺口重新拉一次 REST 快照；OKX / Bybit：每个缺口一对 unsubscribe + subscribe
    CHECK_GE(feeds[0]->feed.stats().rest_snapshots, std::uint64_t{3});
    CHECK_GE(ex.okx().counters().requests, std::uint64_t{1 + 2 * 2});
    CHECK_GE(ex.bybit().counters().requests, std::uint64_t{1 + 2 * 2});
    for (auto& f : feeds) f->stop();
    ex.resume();
    CHECK(ex.serverErrors().empty());
}

TEST_CASE("mock exchange: stop() with connected feeds is prompt; the feeds notice and go RECONNECTING") {
    auto ex = std::make_unique<MockExchange>(testOptions(31));
    const TempFile ca(ex->caPem(), "mock_ca");
    FeedRunner okx(feedConfig(Exchange::OKX, *ex, ca.path()));
    FeedRunner bybit(feedConfig(Exchange::BYBIT, *ex, ca.path()));
    REQUIRE(waitFor([&] { return okx.feed.status() == FeedStatus::Live && bybit.feed.status() == FeedStatus::Live; },
                    5000));
    const std::int64_t t0 = hft::nowNs();
    ex->stop();
    const double stop_ms = static_cast<double>(hft::nowNs() - t0) / 1e6;
    std::printf("    MockExchange::stop() with 2 live sessions took %.1f ms\n", stop_ms);
    CHECK_LT(stop_ms, 1000.0);
    CHECK(waitFor(
        [&] {
            return okx.feed.status() == FeedStatus::Reconnecting && bybit.feed.status() == FeedStatus::Reconnecting;
        },
        3000));
    ex.reset();
}

TEST_CASE("mock exchange: a port already in use fails construction with a clear error and no leaked threads") {
    MockExchange first(testOptions(33));
    MockExchangeOptions o = testOptions(34);
    o.okx_port = first.okxPort();  // Binance 端口（临时端口）已经开始监听之后，OKX 的 bind 才失败
    const std::int64_t t0 = hft::nowNs();
    std::string error;
    try {
        MockExchange second(o);
    } catch (const std::runtime_error& e) {
        error = e.what();
    }
    CHECK(contains(error, "cannot listen on 127.0.0.1:" + std::to_string(first.okxPort()) + " (OKX)"));
    CHECK_LT(static_cast<double>(hft::nowNs() - t0) / 1e6, 5000.0);  // 已启动的服务器在异常路径上被干净地停止
    // 第一个交易所不受影响
    const TempFile ca(first.caPem(), "mock_ca");
    WsClient c(first.okxPort(), "/ws/v5/public", ca.path());
    REQUIRE(c.open());
    c.send("ping");
    CHECK_EQ(c.read(), std::string("pong"));
}

TEST_CASE("mock exchange: the CA file is written atomically and lets the real HTTPS client verify the server") {
    MockExchange ex(testOptions(32));
    const TempFile placeholder("old contents", "mock_ca_out");
    ex.writeCaFile(placeholder.path());
    std::FILE* f = std::fopen(placeholder.path().c_str(), "rb");
    REQUIRE(f != nullptr);
    std::string contents;
    char buf[4096];
    for (std::size_t n; (n = std::fread(buf, 1, sizeof(buf), f)) > 0;) contents.append(buf, n);
    std::fclose(f);
    CHECK_EQ(contents, ex.caPem());
    CHECK(contains(contents, "-----BEGIN CERTIFICATE-----"));
    const hft::net::HttpResult r = restGet(ex.binancePort(), "/api/v3/ping", placeholder.path());
    CHECK(r.ok);
    CHECK_EQ(r.status, 200);
    CHECK_THROWS(ex.writeCaFile("/nonexistent-dir/ca.pem"));
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
