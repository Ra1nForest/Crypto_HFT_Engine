//
// test_app.cpp
// 应用集成测试：配置逐字段回退 / 钳制 / 告警，深度扫单数学（含与 v1 公式逐位等价）、去重、N 个交易所的交易所对覆盖，
// 文本可见宽度工具，帧与最终报告的框线对齐，线程安全日志，命令行解析，以及用进程内模拟交易所端到端驱动 MonitorApp。
//

// 启用库内调试断言（必须在包含任何 hft 头文件之前定义）
#define HFT_ENABLE_ASSERTS

#include "test_framework.h"

#include "app.h"
#include "arbitrage_detector.h"
#include "cli_util.h"
#include "config.h"
#include "display.h"
#include "hft/crc32.h"
#include "hft/timing.h"
#include "logger.h"
#include "mock_exchange_server.h"
#include "orderbook.h"
#include "text_util.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace fs = std::filesystem;
using mock_exchange::HttpReply;
using mock_exchange::MockExchangeServer;
using mock_exchange::WsSession;
using mock_exchange::waitFor;

bool contains(const std::string& s, const std::string& part) { return s.find(part) != std::string::npos; }

std::size_t countOf(const std::string& s, const std::string& part) {
    std::size_t n = 0;
    for (std::size_t p = s.find(part); p != std::string::npos; p = s.find(part, p + part.size())) ++n;
    return n;
}

std::vector<std::string> splitLines(const std::string& s) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start < s.size()) {
        std::size_t nl = s.find('\n', start);
        if (nl == std::string::npos) nl = s.size();
        out.push_back(s.substr(start, nl - start));
        start = nl + 1;
    }
    return out;
}

std::string readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// 读取 tmpfile() 的全部内容
std::string readAll(std::FILE* f) {
    std::fflush(f);
    std::rewind(f);
    std::string out;
    char buf[4096];
    std::size_t n = 0;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) out.append(buf, n);
    return out;
}

// 源码树中的文件（与当前目录无关）
std::string repoPath(const std::string& rel) {
    return (fs::path(__FILE__).parent_path().parent_path() / rel).string();
}

std::string uniqueName(const std::string& prefix) {
    static std::atomic<int> counter{0};
    return prefix + "_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
}

// 临时路径：析构时连同内容一起删除
struct TempPath {
    fs::path path;
    explicit TempPath(const std::string& prefix, const std::string& suffix = "")
        : path(fs::temp_directory_path() / (uniqueName(prefix) + suffix)) {}
    ~TempPath() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempPath(const TempPath&) = delete;
    TempPath& operator=(const TempPath&) = delete;
    std::string str() const { return path.string(); }
};

// tmpfile() 的 RAII 包装
struct TempStream {
    std::FILE* f = std::tmpfile();
    ~TempStream() {
        if (f != nullptr) std::fclose(f);
    }
    TempStream() = default;
    TempStream(const TempStream&) = delete;
    TempStream& operator=(const TempStream&) = delete;
};

std::string joinLines(const std::vector<std::string>& v) {
    std::string s;
    for (const std::string& x : v) s += "    " + x + "\n";
    return s;
}

// ============================================================================
// 配置
// ============================================================================

std::string jsonString(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    return out + "\"";
}

// 以 spec 5.1 的完整配置为基础，按块 / 键覆盖原始 JSON 值，生成配置文本
class ConfigText {
public:
    ConfigText() {
        blocks_ = {
            {"binance",
             {{"enabled", "true"},
              {"host", "\"stream.binance.com\""},
              {"port", "\"9443\""},
              {"path", "\"/ws/btcusdt@depth@100ms\""},
              {"symbol", "\"BTCUSDT\""},
              {"rest_host", "\"api.binance.com\""},
              {"rest_port", "\"443\""},
              {"snapshot_path", "\"/api/v3/depth?symbol=BTCUSDT&limit=1000\""}}},
            {"okx",
             {{"enabled", "true"},
              {"host", "\"ws.okx.com\""},
              {"port", "\"8443\""},
              {"path", "\"/ws/v5/public\""},
              {"symbol", "\"BTCUSDT\""},
              {"subscribe", jsonString(R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})")}}},
            {"bybit",
             {{"enabled", "true"},
              {"host", "\"stream.bybit.com\""},
              {"port", "\"443\""},
              {"path", "\"/v5/public/spot\""},
              {"symbol", "\"BTCUSDT\""},
              {"subscribe", jsonString(R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})")}}},
            {"arbitrage", {{"fee_rate", "0.0"}, {"min_profit", "0.01"}, {"max_qty", "0.0"}, {"max_levels", "20"}}},
            {"display", {{"refresh_ms", "200"}, {"levels", "5"}, {"spread_log_interval", "50"}}},
            {"connection",
             {{"tls_verify", "true"},
              {"ca_file", "\"\""},
              {"stall_timeout_ms", "15000"},
              {"reconnect_initial_ms", "500"},
              {"reconnect_max_ms", "30000"}}},
        };
    }

    // 设置（或新增）一个键的原始 JSON 值
    ConfigText& set(const std::string& block, const std::string& key, const std::string& raw) {
        Block& b = find(block);
        for (auto& kv : b.fields) {
            if (kv.first == key) {
                kv.second = raw;
                return *this;
            }
        }
        b.fields.emplace_back(key, raw);
        return *this;
    }

    ConfigText& erase(const std::string& block, const std::string& key) {
        Block& b = find(block);
        b.fields.erase(std::remove_if(b.fields.begin(), b.fields.end(), [&](const auto& kv) { return kv.first == key; }),
                       b.fields.end());
        return *this;
    }

    // 用原始 JSON 替换整个块（如 "null"、"[]"、"{\"enabled\": false}"）；块不存在时新增
    ConfigText& setBlock(const std::string& block, const std::string& raw) {
        Block& b = find(block);
        b.raw = raw;
        return *this;
    }

    ConfigText& eraseBlock(const std::string& block) {
        blocks_.erase(std::remove_if(blocks_.begin(), blocks_.end(), [&](const Block& b) { return b.name == block; }),
                      blocks_.end());
        return *this;
    }

    std::string str() const {
        std::string s = "{\n";
        for (std::size_t i = 0; i < blocks_.size(); ++i) {
            const Block& b = blocks_[i];
            s += "  " + jsonString(b.name) + ": ";
            if (!b.raw.empty()) {
                s += b.raw;
            } else {
                s += "{";
                for (std::size_t k = 0; k < b.fields.size(); ++k) {
                    s += (k > 0 ? ", " : "") + jsonString(b.fields[k].first) + ": " + b.fields[k].second;
                }
                s += "}";
            }
            s += i + 1 < blocks_.size() ? ",\n" : "\n";
        }
        return s + "}\n";
    }

private:
    struct Block {
        Block(std::string n, std::vector<std::pair<std::string, std::string>> f, std::string r = std::string())
            : name(std::move(n)), fields(std::move(f)), raw(std::move(r)) {}
        std::string name;
        std::vector<std::pair<std::string, std::string>> fields;
        std::string raw;  // 非空时整块用这段原始 JSON
    };

    Block& find(const std::string& name) {
        for (Block& b : blocks_) {
            if (b.name == name) return b;
        }
        blocks_.emplace_back(name, std::vector<std::pair<std::string, std::string>>{});
        return blocks_.back();
    }

    std::vector<Block> blocks_;
};

// 写入临时文件并加载（不打印）
AppConfig loadText(const std::string& json) {
    TempPath file("test_app_config", ".json");
    {
        std::ofstream out(file.path, std::ios::binary);
        out << json;
    }
    return ConfigReader::load(file.str(), false);
}

AppConfig loadText(const ConfigText& t) { return loadText(t.str()); }

bool hasWarning(const AppConfig& c, const std::string& part) {
    for (const std::string& w : c.warnings) {
        if (contains(w, part)) return true;
    }
    std::cerr << "    no warning contains \"" << part << "\"; warnings were:\n" << joinLines(c.warnings);
    return false;
}

const VenueConfig& venueOf(const AppConfig& c, Exchange e) {
    const VenueConfig* v = c.venue(e);
    if (v == nullptr) throw std::runtime_error("venue missing from config");
    return *v;
}

void checkSameVenue(const VenueConfig& a, const VenueConfig& b) {
    CHECK(a.exchange == b.exchange);
    CHECK_EQ(a.key, b.key);
    CHECK_EQ(a.name, b.name);
    CHECK_EQ(a.enabled, b.enabled);
    CHECK_EQ(a.host, b.host);
    CHECK_EQ(a.port, b.port);
    CHECK_EQ(a.path, b.path);
    CHECK_EQ(a.symbol, b.symbol);
    CHECK_EQ(a.subscribe_msg, b.subscribe_msg);
    CHECK_EQ(a.rest_host, b.rest_host);
    CHECK_EQ(a.rest_port, b.rest_port);
    CHECK_EQ(a.snapshot_path, b.snapshot_path);
    CHECK_EQ(a.fee_rate.has_value(), b.fee_rate.has_value());
}

TEST_CASE("config: the shipped config.json loads without warnings and equals the defaults except the global fee_rate") {
    const std::string path = repoPath("config.json");
    const AppConfig c = ConfigReader::load(path, false);
    if (!c.warnings.empty()) std::cerr << joinLines(c.warnings);
    CHECK(c.warnings.empty());
    CHECK_EQ(c.source, path);
    const AppConfig d = ConfigReader::getDefaults();
    REQUIRE_EQ(c.venues.size(), 3u);
    REQUIRE_EQ(d.venues.size(), 3u);
    CHECK(c.venues[0].exchange == Exchange::BINANCE);
    CHECK(c.venues[1].exchange == Exchange::OKX);
    CHECK(c.venues[2].exchange == Exchange::BYBIT);
    for (std::size_t i = 0; i < 3; ++i) checkSameVenue(c.venues[i], d.venues[i]);
    CHECK_EQ(c.arbitrage.fee_rate, 0.0);
    CHECK_EQ(d.arbitrage.fee_rate, 0.001);
    CHECK_EQ(c.arbitrage.min_profit, d.arbitrage.min_profit);
    CHECK_EQ(c.arbitrage.max_qty, d.arbitrage.max_qty);
    CHECK_EQ(c.arbitrage.max_levels, d.arbitrage.max_levels);
    CHECK_EQ(c.display.refresh_ms, d.display.refresh_ms);
    CHECK_EQ(c.display.levels, d.display.levels);
    CHECK_EQ(c.display.spread_log_interval, d.display.spread_log_interval);
    CHECK_EQ(c.connection.tls_verify, d.connection.tls_verify);
    CHECK_EQ(c.connection.ca_file, d.connection.ca_file);
    CHECK_EQ(c.connection.stall_timeout_ms, d.connection.stall_timeout_ms);
    CHECK_EQ(c.connection.reconnect_initial_ms, d.connection.reconnect_initial_ms);
    CHECK_EQ(c.connection.reconnect_max_ms, d.connection.reconnect_max_ms);
    // 默认值即 spec 5.1 的取值
    CHECK_EQ(d.venues[0].path, std::string("/ws/btcusdt@depth@100ms"));
    CHECK_EQ(d.venues[0].snapshot_path, std::string("/api/v3/depth?symbol=BTCUSDT&limit=1000"));
    CHECK_EQ(d.venues[2].port, std::string("443"));
    CHECK_EQ(d.arbitrage.max_levels, 20);
    CHECK_EQ(d.connection.stall_timeout_ms, 15000);
    CHECK_EQ(c.enabledVenues().size(), 3u);
    // 测试自己的生成器与仓库文件等价
    const AppConfig g = loadText(ConfigText());
    if (!g.warnings.empty()) std::cerr << joinLines(g.warnings);
    CHECK(g.warnings.empty());
}

TEST_CASE("config: unreadable file, invalid JSON or a non-object document -> full defaults and one warning") {
    {
        const AppConfig c = ConfigReader::load("/nonexistent-dir/config.json", false);
        CHECK(c.source.empty());
        REQUIRE_EQ(c.warnings.size(), 1u);
        CHECK(contains(c.warnings[0], "[Config] Cannot open: /nonexistent-dir/config.json"));
        CHECK(contains(c.warnings[0], "using defaults"));
        CHECK_EQ(c.arbitrage.fee_rate, 0.001);
        CHECK_EQ(c.enabledVenues().size(), 3u);
    }
    for (const char* bad : {"{\"binance\": ", "[1, 2, 3]", "", "not json", "42", "{\"a\": 1} trailing"}) {
        const AppConfig c = loadText(std::string(bad));
        CHECK(c.source.empty());
        CHECK_EQ(c.warnings.size(), 1u);
        CHECK(hasWarning(c, "using defaults"));
        CHECK_EQ(c.arbitrage.fee_rate, 0.001);
        CHECK_EQ(c.display.refresh_ms, 200);
    }
    const AppConfig arr = loadText(std::string("[1]"));
    CHECK(hasWarning(arr, "top-level value is not a JSON object"));
    const AppConfig inv = loadText(std::string("{oops}"));
    CHECK(hasWarning(inv, "invalid JSON"));
}

TEST_CASE("config: a mistyped field falls back to its own default with a warning naming it; other fields are kept") {
    ConfigText t;
    t.set("binance", "port", "9443")
        .set("binance", "path", "\"/ws/btcusdt@depth20@100ms\"")
        .set("binance", "rest_host", "\"\"")
        .set("binance", "snapshot_path", "\"\"")
        .set("okx", "host", "\"okx.example.com\"")
        .set("okx", "port", "true")
        .setBlock("bybit", "{\"enabled\": false}")
        .set("arbitrage", "fee_rate", "0.0005")
        .set("arbitrage", "min_profit", "\"lots\"")
        .set("arbitrage", "max_qty", "0.5")
        .set("arbitrage", "max_levels", "10")
        .set("display", "refresh_ms", "100")
        .set("display", "levels", "3")
        .set("display", "spread_log_interval", "10")
        .set("connection", "tls_verify", "false")
        .set("connection", "ca_file", "\"/tmp/mock_ca.pem\"")
        .set("connection", "stall_timeout_ms", "5000")
        .set("connection", "reconnect_initial_ms", "100")
        .set("connection", "reconnect_max_ms", "1000");
    const AppConfig c = loadText(t);
    CHECK_EQ(c.warnings.size(), 3u);
    CHECK(hasWarning(c, "[Config] connection.ca_file: cannot read \"/tmp/mock_ca.pem\", every TLS connection will fail"));
    CHECK(hasWarning(c, "[Config] okx.port: expected string or integer, using default \"8443\""));
    CHECK(hasWarning(c, "[Config] arbitrage.min_profit: expected a number, using default 0.01"));
    const VenueConfig& b = venueOf(c, Exchange::BINANCE);
    CHECK_EQ(b.port, std::string("9443"));
    CHECK_EQ(b.path, std::string("/ws/btcusdt@depth20@100ms"));
    CHECK(b.rest_host.empty());
    CHECK(b.snapshot_path.empty());
    const VenueConfig& o = venueOf(c, Exchange::OKX);
    CHECK_EQ(o.host, std::string("okx.example.com"));
    CHECK_EQ(o.port, std::string("8443"));
    CHECK(!venueOf(c, Exchange::BYBIT).enabled);
    CHECK_EQ(c.enabledVenues().size(), 2u);
    CHECK_EQ(c.arbitrage.fee_rate, 0.0005);
    CHECK_EQ(c.arbitrage.min_profit, 0.01);
    CHECK_EQ(c.arbitrage.max_qty, 0.5);
    CHECK_EQ(c.arbitrage.max_levels, 10);
    CHECK_EQ(c.display.refresh_ms, 100);
    CHECK_EQ(c.display.levels, 3);
    CHECK_EQ(c.display.spread_log_interval, 10);
    CHECK(!c.connection.tls_verify);
    CHECK_EQ(c.connection.ca_file, std::string("/tmp/mock_ca.pem"));  // 文件不存在：值保留，另有告警（见下一行的计数）
    CHECK_EQ(c.connection.stall_timeout_ms, 5000);
    CHECK_EQ(c.connection.reconnect_initial_ms, 100);
    CHECK_EQ(c.connection.reconnect_max_ms, 1000);
}

TEST_CASE("config: port and rest_port accept a JSON string or integer in 1..65535") {
    ConfigText t;
    t.set("binance", "port", "9443").set("binance", "rest_port", "8443").set("okx", "port", "\"0\"").set("bybit", "port", "70000");
    const AppConfig c = loadText(t);
    CHECK_EQ(venueOf(c, Exchange::BINANCE).port, std::string("9443"));
    CHECK_EQ(venueOf(c, Exchange::BINANCE).rest_port, std::string("8443"));
    CHECK_EQ(venueOf(c, Exchange::OKX).port, std::string("8443"));
    CHECK_EQ(venueOf(c, Exchange::BYBIT).port, std::string("443"));
    CHECK(hasWarning(c, "[Config] okx.port: \"0\" is not a valid port (1-65535), using default \"8443\""));
    CHECK(hasWarning(c, "[Config] bybit.port: \"70000\" is not a valid port"));
    CHECK_EQ(c.warnings.size(), 2u);

    for (const char* raw : {"-1", "\"abc\"", "\" 443\"", "\"443x\"", "1.5", "null", "[443]"}) {
        ConfigText u;
        u.set("bybit", "port", raw);
        const AppConfig d = loadText(u);
        CHECK_EQ(venueOf(d, Exchange::BYBIT).port, std::string("443"));
        CHECK_EQ(d.warnings.size(), 1u);
        CHECK(hasWarning(d, "[Config] bybit.port: "));
    }
    ConfigText edge;
    edge.set("okx", "port", "1").set("bybit", "port", "\"65535\"");
    const AppConfig e = loadText(edge);
    CHECK(e.warnings.empty());
    CHECK_EQ(venueOf(e, Exchange::OKX).port, std::string("1"));
    CHECK_EQ(venueOf(e, Exchange::BYBIT).port, std::string("65535"));
}

TEST_CASE("config: out-of-range values are clamped with a warning naming the field and the clamped value") {
    ConfigText t;
    t.set("display", "refresh_ms", "5")
        .set("display", "levels", "50")
        .set("display", "spread_log_interval", "0")
        .set("arbitrage", "max_levels", "0")
        .set("arbitrage", "min_profit", "-1")
        .set("arbitrage", "max_qty", "-2.5")
        .set("arbitrage", "fee_rate", "-0.001")
        .set("connection", "stall_timeout_ms", "10")
        .set("connection", "reconnect_initial_ms", "2000")
        .set("connection", "reconnect_max_ms", "100")
        .set("okx", "fee_rate", "0.5");
    const AppConfig c = loadText(t);
    CHECK_EQ(c.display.refresh_ms, 10);
    CHECK_EQ(c.display.levels, 20);
    CHECK_EQ(c.display.spread_log_interval, 1);
    CHECK_EQ(c.arbitrage.max_levels, 1);
    CHECK_EQ(c.arbitrage.min_profit, 0.0);
    CHECK_EQ(c.arbitrage.max_qty, 0.0);
    CHECK_EQ(c.arbitrage.fee_rate, 0.0);
    CHECK_EQ(c.connection.stall_timeout_ms, 1000);
    CHECK_EQ(c.connection.reconnect_initial_ms, 2000);
    CHECK_EQ(c.connection.reconnect_max_ms, 2000);
    CHECK(!venueOf(c, Exchange::OKX).fee_rate.has_value());
    CHECK(hasWarning(c, "[Config] display.refresh_ms: 5 is out of range [10, 10000], clamped to 10"));
    CHECK(hasWarning(c, "[Config] display.levels: 50 is out of range [1, 20], clamped to 20"));
    CHECK(hasWarning(c, "[Config] display.spread_log_interval: 0 is out of range [1, "));
    CHECK(hasWarning(c, "[Config] arbitrage.max_levels: 0 is out of range [1, 20], clamped to 1"));
    CHECK(hasWarning(c, "[Config] arbitrage.min_profit: -1 is out of range [0, inf], clamped to 0"));
    CHECK(hasWarning(c, "[Config] arbitrage.max_qty: -2.5 is out of range [0, inf], clamped to 0"));
    CHECK(hasWarning(c, "[Config] arbitrage.fee_rate: -0.001 is negative, clamped to 0"));
    CHECK(hasWarning(c, "[Config] connection.stall_timeout_ms: 10 is out of range [1000, "));
    CHECK(hasWarning(c, "[Config] connection.reconnect_max_ms: 100 is out of range [2000, "));
    CHECK(hasWarning(c, "[Config] okx.fee_rate: 0.5 must be below 0.1"));
    CHECK(hasWarning(c, "using the global fee_rate"));
    CHECK_EQ(c.warnings.size(), 10u);

    // reconnect_max_ms 缺失、默认值 30000 小于给出的初始值：同样钳制
    ConfigText rc;
    rc.set("connection", "reconnect_initial_ms", "60000").erase("connection", "reconnect_max_ms");
    const AppConfig r = loadText(rc);
    CHECK_EQ(r.connection.reconnect_max_ms, 60000);
    CHECK(hasWarning(r, "[Config] connection.reconnect_max_ms: missing, using default 30000"));
    CHECK(hasWarning(r, "[Config] connection.reconnect_max_ms: 30000 is below connection.reconnect_initial_ms, using 60000"));

    // 数值很大（int64 / uint64 范围）也只钳制，不溢出
    ConfigText big;
    big.set("display", "spread_log_interval", "99999999999").set("display", "refresh_ms", "18446744073709551615");
    const AppConfig b = loadText(big);
    CHECK_EQ(b.display.spread_log_interval, ConfigReader::kMaxSpreadLogInterval);
    CHECK_EQ(b.display.refresh_ms, 10000);
    CHECK_EQ(b.warnings.size(), 2u);

    // 全局 fee_rate >= 0.1：用默认值 0.001
    ConfigText fee;
    fee.set("arbitrage", "fee_rate", "0.1");
    const AppConfig f = loadText(fee);
    CHECK_EQ(f.arbitrage.fee_rate, 0.001);
    CHECK(hasWarning(f, "[Config] arbitrage.fee_rate: 0.1 must be below 0.1"));
    CHECK(hasWarning(f, "using default 0.001"));
}

TEST_CASE("config: integer fields reject fractions and strings but accept integral doubles") {
    ConfigText t;
    t.set("display", "levels", "2.5").set("display", "refresh_ms", "200.0").set("display", "spread_log_interval", "\"7\"");
    const AppConfig c = loadText(t);
    CHECK_EQ(c.display.levels, 5);
    CHECK_EQ(c.display.refresh_ms, 200);
    CHECK_EQ(c.display.spread_log_interval, 50);
    CHECK(hasWarning(c, "[Config] display.levels: expected an integer, using default 5"));
    CHECK(hasWarning(c, "[Config] display.spread_log_interval: expected an integer, using default 50"));
    CHECK_EQ(c.warnings.size(), 2u);
    ConfigText e;
    e.set("display", "refresh_ms", "1e3");
    const AppConfig d = loadText(e);
    CHECK_EQ(d.display.refresh_ms, 1000);
    CHECK(d.warnings.empty());
}

TEST_CASE("config: missing fields and blocks fall back with one warning each; wrong block types too") {
    ConfigText t;
    t.erase("okx", "host")
        .erase("arbitrage", "fee_rate")
        .eraseBlock("display")
        .setBlock("connection", "[]")
        .set("bybit", "enabled", "\"yes\"");
    const AppConfig c = loadText(t);
    CHECK(hasWarning(c, "[Config] okx.host: missing, using default \"ws.okx.com\""));
    CHECK(hasWarning(c, "[Config] arbitrage.fee_rate: missing, using default 0.001"));
    CHECK(hasWarning(c, "[Config] display: missing, using defaults"));
    CHECK(hasWarning(c, "[Config] connection: expected an object, using defaults"));
    CHECK(hasWarning(c, "[Config] bybit.enabled: expected true or false, using default true"));
    CHECK_EQ(c.warnings.size(), 5u);
    CHECK_EQ(venueOf(c, Exchange::OKX).host, std::string("ws.okx.com"));
    CHECK_EQ(c.arbitrage.fee_rate, 0.001);
    CHECK_EQ(c.display.refresh_ms, 200);
    CHECK(c.connection.tls_verify);
    CHECK(venueOf(c, Exchange::BYBIT).enabled);

    // 新的块缺失（旧版配置文件）：只告警一次，使用默认值
    ConfigText old;
    old.eraseBlock("bybit").eraseBlock("connection");
    const AppConfig o = loadText(old);
    CHECK(hasWarning(o, "[Config] bybit: missing, using defaults"));
    CHECK(hasWarning(o, "[Config] connection: missing, using defaults"));
    CHECK_EQ(o.warnings.size(), 2u);
    CHECK(venueOf(o, Exchange::BYBIT).enabled);
    CHECK_EQ(venueOf(o, Exchange::BYBIT).host, std::string("stream.bybit.com"));

    ConfigText nul;
    nul.setBlock("okx", "null");
    const AppConfig n = loadText(nul);
    CHECK(hasWarning(n, "[Config] okx: expected an object, using defaults"));
}

TEST_CASE("config: per-venue enabled is honoured and a disabled venue's other fields are not validated") {
    ConfigText t;
    t.setBlock("bybit", "{\"enabled\": false}").setBlock("okx", "{\"enabled\": false, \"port\": true, \"host\": 7}");
    const AppConfig c = loadText(t);
    if (!c.warnings.empty()) std::cerr << joinLines(c.warnings);
    CHECK(c.warnings.empty());
    const std::vector<const VenueConfig*> en = c.enabledVenues();
    REQUIRE_EQ(en.size(), 1u);
    CHECK(en[0]->exchange == Exchange::BINANCE);
    CHECK(!venueOf(c, Exchange::OKX).enabled);
    CHECK(contains(ConfigReader::summary(c), "OKX (disabled)"));
    CHECK(contains(ConfigReader::summary(c), "Bybit (disabled)"));

    ConfigText none;
    none.set("binance", "enabled", "false").set("okx", "enabled", "false").set("bybit", "enabled", "false");
    CHECK(loadText(none).enabledVenues().empty());
}

TEST_CASE("config: per-venue fee_rate overrides the global one; invalid overrides fall back to it") {
    ConfigText t;
    t.set("arbitrage", "fee_rate", "0.001").set("okx", "fee_rate", "0.0008").set("bybit", "fee_rate", "0");
    const AppConfig c = loadText(t);
    CHECK(c.warnings.empty());
    CHECK_EQ(c.feeRate(venueOf(c, Exchange::BINANCE)), 0.001);
    CHECK_EQ(c.feeRate(venueOf(c, Exchange::OKX)), 0.0008);
    CHECK_EQ(c.feeRate(venueOf(c, Exchange::BYBIT)), 0.0);
    CHECK(contains(ConfigReader::summary(c), "Fee: 0.100% (OKX 0.080%, Bybit 0.000%)"));

    ConfigText bad;
    bad.set("okx", "fee_rate", "\"x\"").set("bybit", "fee_rate", "-0.5");
    const AppConfig d = loadText(bad);
    CHECK(hasWarning(d, "[Config] okx.fee_rate: expected a number, using the global fee_rate"));
    CHECK(hasWarning(d, "[Config] bybit.fee_rate: -0.5 is negative, clamped to 0"));
    CHECK(!venueOf(d, Exchange::OKX).fee_rate.has_value());
    CHECK_EQ(d.feeRate(venueOf(d, Exchange::BYBIT)), 0.0);
}

TEST_CASE("config: a readable connection.ca_file is accepted without warnings") {
    TempPath ca("test_app_ca", ".pem");
    {
        std::ofstream out(ca.path);
        out << "-----BEGIN CERTIFICATE-----\n";
    }
    ConfigText t;
    t.set("connection", "ca_file", jsonString(ca.str()));
    const AppConfig c = loadText(t);
    if (!c.warnings.empty()) std::cerr << joinLines(c.warnings);
    CHECK(c.warnings.empty());
    CHECK_EQ(c.connection.ca_file, ca.str());
}

TEST_CASE("config: unknown keys are ignored silently") {
    ConfigText t;
    t.set("binance", "extra", "123")
        .set("arbitrage", "comment", "\"hello\"")
        .setBlock("future_feature", "{\"x\": [1, 2, {\"y\": null}]}")
        .set("connection", "connect_timeout_ms", "5");
    const AppConfig c = loadText(t);
    if (!c.warnings.empty()) std::cerr << joinLines(c.warnings);
    CHECK(c.warnings.empty());
}

TEST_CASE("config: host, path, subscribe and snapshot_path are validated") {
    ConfigText t;
    t.set("okx", "host", "\"wss://ws.okx.com\"")
        .set("bybit", "path", "\"v5/public/spot\"")
        .set("okx", "subscribe", "\"{not json\"")
        .set("binance", "host", "\"\"")
        .set("binance", "rest_host", "\"api binance com\"")
        .set("binance", "snapshot_path", "\"api/v3/depth\"");
    const AppConfig c = loadText(t);
    CHECK_EQ(venueOf(c, Exchange::OKX).host, std::string("ws.okx.com"));
    CHECK_EQ(venueOf(c, Exchange::BYBIT).path, std::string("/v5/public/spot"));
    CHECK_EQ(venueOf(c, Exchange::OKX).subscribe_msg,
             std::string(R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})"));
    CHECK_EQ(venueOf(c, Exchange::BINANCE).host, std::string("stream.binance.com"));
    CHECK_EQ(venueOf(c, Exchange::BINANCE).rest_host, std::string("api.binance.com"));
    CHECK(venueOf(c, Exchange::BINANCE).snapshot_path.empty());
    CHECK(hasWarning(c, "[Config] okx.host: \"wss://ws.okx.com\" is not a host name"));
    CHECK(hasWarning(c, "[Config] bybit.path: \"v5/public/spot\" must start with '/', using default \"/v5/public/spot\""));
    CHECK(hasWarning(c, "[Config] okx.subscribe: is not valid JSON"));
    CHECK(hasWarning(c, "[Config] binance.host: must not be empty, using default \"stream.binance.com\""));
    CHECK(hasWarning(c, "[Config] binance.rest_host: \"api binance com\" is not a host name"));
    CHECK(hasWarning(c, "[Config] binance.snapshot_path: \"api/v3/depth\" must start with '/'"));
    // snapshot_path 被清空后，增量深度流缺少 REST 快照配置：一致性检查再告警一次
    CHECK(hasWarning(c, "[Config] binance.snapshot_path: required by the diff-depth stream"));
    CHECK_EQ(c.warnings.size(), 7u);
}

TEST_CASE("config: consistency warnings for symbol labels and the Binance diff stream without REST settings") {
    ConfigText t;
    t.set("okx", "symbol", "\"ETHUSDT\"");
    const AppConfig c = loadText(t);
    CHECK(hasWarning(c, "[Config] okx.symbol: \"ETHUSDT\" does not appear in okx.subscribe"));
    CHECK(hasWarning(c, "[Config] symbol: enabled venues use different symbols"));
    CHECK_EQ(c.warnings.size(), 2u);

    ConfigText rest;
    rest.set("binance", "rest_host", "\"\"");
    const AppConfig r = loadText(rest);
    CHECK(hasWarning(r, "[Config] binance.rest_host: required by the diff-depth stream in binance.path"));
    CHECK_EQ(r.warnings.size(), 1u);

    // 部分深度流不需要 REST
    ConfigText partial;
    partial.set("binance", "path", "\"/ws/btcusdt@depth20@100ms\"").set("binance", "rest_host", "\"\"").set("binance", "snapshot_path", "\"\"");
    const AppConfig p = loadText(partial);
    if (!p.warnings.empty()) std::cerr << joinLines(p.warnings);
    CHECK(p.warnings.empty());
    CHECK(config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth"));
    CHECK(config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth@100ms"));
    CHECK(!config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth20@100ms"));
    CHECK(!config_detail::isBinanceDiffDepthPath("/ws/btcusdt@bookTicker"));
    CHECK(config_detail::isBinanceDiffDepthPath("/stream?streams=btcusdt@depth5/ethusdt@depth@100ms"));

    // 关闭的交易所不参与一致性检查
    ConfigText dis;
    dis.set("okx", "symbol", "\"ETHUSDT\"").set("okx", "enabled", "false");
    CHECK(loadText(dis).warnings.empty());
}

TEST_CASE("config: load() prints the Loaded line and summary on stdout and the warnings on stderr") {
    TempPath file("test_app_config", ".json");
    {
        ConfigText t;
        t.set("okx", "port", "true");
        std::ofstream out(file.path);
        out << t.str();
    }
    std::ostringstream out, err;
    std::streambuf* old_out = std::cout.rdbuf(out.rdbuf());
    std::streambuf* old_err = std::cerr.rdbuf(err.rdbuf());
    const AppConfig c = ConfigReader::load(file.str());
    std::cout.rdbuf(old_out);
    std::cerr.rdbuf(old_err);
    CHECK(contains(out.str(), "[Config] Loaded: " + file.str() + "\n"));
    CHECK(contains(out.str(), "[Config] Venues: Binance, OKX, Bybit  Fee: 0.000%  Min profit: 0.01  Refresh: 200ms"));
    CHECK(contains(err.str(), "[Config] okx.port: expected string or integer, using default \"8443\"\n"));
    CHECK(!contains(out.str(), "okx.port"));
    CHECK_EQ(c.warnings.size(), 1u);

    std::ostringstream out2, err2;
    old_out = std::cout.rdbuf(out2.rdbuf());
    old_err = std::cerr.rdbuf(err2.rdbuf());
    ConfigReader::load("/nonexistent-dir/x.json");
    std::cout.rdbuf(old_out);
    std::cerr.rdbuf(old_err);
    CHECK(!contains(out2.str(), "Loaded"));
    CHECK(contains(err2.str(), "[Config] Cannot open: /nonexistent-dir/x.json"));
}

// ============================================================================
// 套利：扫单数学
// ============================================================================

std::vector<PriceLevel> L(std::initializer_list<PriceLevel> levels) { return std::vector<PriceLevel>(levels); }

SweepResult sweepOf(const std::vector<PriceLevel>& asks, const std::vector<PriceLevel>& bids, double fb, double fs,
                    double max_qty = 0.0, int max_levels = 0) {
    return ArbitrageDetector::sweep(asks.data(), asks.size(), bids.data(), bids.size(), fb, fs, max_qty, max_levels);
}

OrderBook makeBook(const std::string& label, const std::vector<PriceLevel>& bids, const std::vector<PriceLevel>& asks) {
    OrderBook b(label);
    for (const PriceLevel& l : bids) b.updateBid(l.price, l.quantity);
    for (const PriceLevel& l : asks) b.updateAsk(l.price, l.quantity);
    return b;
}

TEST_CASE("sweep: walks several levels on both sides while the marginal unit is profitable (no fees)") {
    const auto asks = L({{100.0, 1.0}, {101.0, 2.0}, {102.0, 5.0}});
    const auto bids = L({{103.0, 1.5}, {102.5, 1.0}, {101.5, 3.0}});
    const SweepResult r = sweepOf(asks, bids, 0.0, 0.0);
    // 100x1 @103, 101x0.5 @103, 101x1 @102.5, 101x0.5 @101.5；然后 102 > 101.5 停止
    CHECK_EQ(r.qty, 3.0);
    CHECK_EQ(r.cost, 302.0);
    CHECK_EQ(r.revenue, 307.75);
    CHECK_EQ(r.gross, 5.75);
    CHECK_EQ(r.fees, 0.0);
    CHECK_EQ(r.net, 5.75);
    CHECK_EQ(r.buy_levels, 2);
    CHECK_EQ(r.sell_levels, 3);
    CHECK_EQ(r.levelsUsed(), 3);
    CHECK_NEAR(r.vwapBuy(), 302.0 / 3.0, 1e-12);
    CHECK_NEAR(r.vwapSell(), 307.75 / 3.0, 1e-12);
    CHECK_NEAR(r.revenue - r.cost - r.fees, r.net, 1e-9);
}

TEST_CASE("sweep: fees stop the walk at the first unprofitable marginal unit (equal and unequal fees)") {
    const auto asks = L({{100.0, 1.0}, {101.0, 2.0}, {102.0, 5.0}});
    const auto bids = L({{103.0, 1.5}, {102.5, 1.0}, {101.5, 3.0}});
    // f = 0.5%：第 4 步 101 -> 101.5 扣费后为负
    const SweepResult r = sweepOf(asks, bids, 0.005, 0.005);
    CHECK_EQ(r.qty, 2.5);
    CHECK_NEAR(r.gross, 5.5, 1e-12);
    CHECK_NEAR(r.fees, (203.0 * 1.0 + 204.0 * 0.5 + 203.5 * 1.0) * 0.005, 1e-12);
    CHECK_NEAR(r.net, 5.5 - 2.5425, 1e-12);
    CHECK_NEAR(r.revenue - r.cost - r.fees, r.net, 1e-9);
    CHECK_EQ(r.buy_levels, 2);
    CHECK_EQ(r.sell_levels, 2);

    // 买方 0.1%、卖方 0.4%：手续费按方向计算
    const SweepResult u = sweepOf(asks, bids, 0.001, 0.004);
    CHECK_EQ(u.qty, 2.5);
    const double fees_u = (100 * 0.001 + 103 * 0.004) * 1.0 + (101 * 0.001 + 103 * 0.004) * 0.5 +
                          (101 * 0.001 + 102.5 * 0.004) * 1.0;
    CHECK_NEAR(u.fees, fees_u, 1e-12);
    CHECK_NEAR(u.net, 5.5 - fees_u, 1e-12);
    const SweepResult w = sweepOf(asks, bids, 0.004, 0.001);
    const double fees_w = (100 * 0.004 + 103 * 0.001) * 1.0 + (101 * 0.004 + 103 * 0.001) * 0.5 +
                          (101 * 0.004 + 102.5 * 0.001) * 1.0;
    CHECK_EQ(w.qty, 2.5);
    CHECK_NEAR(w.fees, fees_w, 1e-12);
    CHECK(w.fees != u.fees);

    // 手续费高到第一档就不赚钱：完全不交易
    const SweepResult none = sweepOf(asks, bids, 0.02, 0.02);
    CHECK_EQ(none.qty, 0.0);
    CHECK_EQ(none.net, 0.0);
    CHECK_EQ(none.levelsUsed(), 0);
    // 边际判断：bid*(1-fs) - ask*(1+fb) > 0 恰好不成立时不交易（103*0.99 - 100*1.0197 = 1.97 - 1.97 < 0）
    const SweepResult edge = sweepOf(L({{100.0, 1.0}}), L({{103.0, 1.0}}), 0.0198, 0.01);
    CHECK_EQ(edge.qty, 0.0);
}

TEST_CASE("sweep: max_qty caps the total quantity, max_levels caps the levels per side") {
    const auto asks = L({{100.0, 1.0}, {101.0, 2.0}, {102.0, 5.0}});
    const auto bids = L({{103.0, 1.5}, {102.5, 1.0}, {101.5, 3.0}});
    const SweepResult q = sweepOf(asks, bids, 0.0, 0.0, 1.25);
    CHECK_EQ(q.qty, 1.25);
    CHECK_EQ(q.cost, 100.0 + 101.0 * 0.25);
    CHECK_EQ(q.revenue, 103.0 * 1.25);
    CHECK_EQ(q.gross, 3.5);
    CHECK_EQ(q.buy_levels, 2);
    CHECK_EQ(q.sell_levels, 1);
    const SweepResult q2 = sweepOf(asks, bids, 0.0, 0.0, 0.4);
    CHECK_EQ(q2.qty, 0.4);
    CHECK_EQ(q2.levelsUsed(), 1);
    // max_qty <= 0 不限
    CHECK_EQ(sweepOf(asks, bids, 0.0, 0.0, -1.0).qty, 3.0);

    const SweepResult l1 = sweepOf(asks, bids, 0.0, 0.0, 0.0, 1);
    CHECK_EQ(l1.qty, 1.0);
    CHECK_EQ(l1.gross, 3.0);
    CHECK_EQ(l1.levelsUsed(), 1);
    const SweepResult l2 = sweepOf(asks, bids, 0.0, 0.0, 0.0, 2);
    CHECK_EQ(l2.qty, 2.5);  // 100x1@103, 101x0.5@103, 101x1@102.5，然后买盘只剩 2 档
    CHECK_EQ(l2.sell_levels, 2);
    CHECK_EQ(sweepOf(asks, bids, 0.0, 0.0, 0.0, 0).qty, 3.0);  // 0 = 不限
}

TEST_CASE("sweep: unprofitable, crossed-equal, empty and malformed books") {
    CHECK_EQ(sweepOf(L({{100.0, 1.0}}), L({{99.99, 1.0}}), 0.0, 0.0).qty, 0.0);
    CHECK_EQ(sweepOf(L({{100.0, 1.0}}), L({{100.0, 1.0}}), 0.0, 0.0).qty, 0.0);  // 平价不赚钱
    const SweepResult e1 = sweepOf({}, L({{100.0, 1.0}}), 0.0, 0.0);
    const SweepResult e2 = sweepOf(L({{100.0, 1.0}}), {}, 0.0, 0.0);
    const SweepResult e3 = sweepOf({}, {}, 0.0, 0.0);
    for (const SweepResult* r : {&e1, &e2, &e3}) {
        CHECK_EQ(r->qty, 0.0);
        CHECK_EQ(r->net, 0.0);
        CHECK_EQ(r->fees, 0.0);
        CHECK_EQ(r->vwapBuy(), 0.0);
        CHECK_EQ(r->levelsUsed(), 0);
    }
    CHECK_EQ(ArbitrageDetector::sweep(nullptr, 3, nullptr, 3, 0.0, 0.0).qty, 0.0);
    // 非正数量的档位被跳过；非法价格终止扫单
    const SweepResult skip = sweepOf(L({{100.0, 0.0}, {100.5, -1.0}, {101.0, 1.0}}), L({{102.0, 2.0}}), 0.0, 0.0);
    CHECK_EQ(skip.qty, 1.0);
    CHECK_EQ(skip.cost, 101.0);
    CHECK_EQ(skip.buy_levels, 3);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    CHECK_EQ(sweepOf(L({{nan, 1.0}}), L({{102.0, 2.0}}), 0.0, 0.0).qty, 0.0);
    CHECK_EQ(sweepOf(L({{100.0, 1.0}, {-1.0, 1.0}}), L({{102.0, 5.0}}), 0.0, 0.0).qty, 1.0);
    // OrderBook 重载
    const OrderBook a = makeBook("A", {{99.0, 1.0}}, {{100.0, 2.0}});
    const OrderBook b = makeBook("B", {{101.0, 1.0}}, {{102.0, 1.0}});
    CHECK_EQ(ArbitrageDetector::sweep(a, b, 0.0, 0.0).qty, 1.0);
    CHECK_EQ(ArbitrageDetector::sweep(b, a, 0.0, 0.0).qty, 0.0);
}

// v1 的检测逻辑原样复刻（含去重），用来证明新实现在"每侧一档 + 相同手续费"时逐位等价
struct V1Detector {
    struct Signal {
        std::string buy, sell;
        double buy_price, sell_price, qty, gross, net;
    };
    V1Detector(double fee, double min) : fee_rate(fee), min_profit(min) {}
    double fee_rate, min_profit;
    double last_d1_buy = 0, last_d1_sell = 0, last_d2_buy = 0, last_d2_sell = 0;
    std::vector<Signal> signals;

    bool detect(const OrderBook& a, const OrderBook& b) {
        bool found = false;
        found |= check(a.getSymbol(), a.getBestAsk(), b.getSymbol(), b.getBestBid(), last_d1_buy, last_d1_sell);
        found |= check(b.getSymbol(), b.getBestAsk(), a.getSymbol(), a.getBestBid(), last_d2_buy, last_d2_sell);
        return found;
    }

    bool check(const std::string& bs, PriceLevel ask, const std::string& ss, PriceLevel bid, double& lb, double& ls) {
        if (ask.price <= 0 || bid.price <= 0) return false;
        const double gross = bid.price - ask.price;
        if (gross <= 0) return false;
        const double qty = std::min(ask.quantity, bid.quantity);
        const double fee = (ask.price + bid.price) * qty * fee_rate;
        const double net = gross * qty - fee;
        if (net <= min_profit) return false;
        if (std::abs(ask.price - lb) < 0.01 && std::abs(bid.price - ls) < 0.01) return false;
        lb = ask.price;
        ls = bid.price;
        signals.push_back({bs, ss, ask.price, bid.price, qty, gross * qty, net});
        return true;
    }
};

TEST_CASE("sweep: one level per side with equal fees reproduces the v1 formula and decisions exactly") {
    std::mt19937_64 rng(20261105);
    std::uniform_int_distribution<int> step(-3, 3);
    std::uniform_int_distribution<int> lots(1, 400);
    std::uniform_int_distribution<int> coin(0, 3);
    for (double fee : {0.0, 0.0001, 0.001}) {
        V1Detector v1(fee, 0.01);
        ArbitrageDetector det(fee, 0.01);  // v1 兼容构造：两个交易所、只看最优档
        CHECK_EQ(det.maxLevels(), 1);
        // 价格以 5 美分为步长随机游走（变化要么为 0，要么 >= 0.05，远离 0.01 的去重阈值）；
        // B 相对 A 的偏移偶尔跳到 ±250 美元以内的随机值，任何手续费水平下都会出现可盈利与不可盈利的时段
        std::uniform_int_distribution<long> offset_dist(-5000, 5000);
        std::uniform_int_distribution<int> jump(0, 19);
        long a_mid = 6700000, b_offset = 0;
        std::size_t matched = 0;
        for (int i = 0; i < 20000; ++i) {
            a_mid += 5 * step(rng);
            if (jump(rng) == 0) b_offset = 5 * offset_dist(rng);
            const long b_mid = a_mid + b_offset + 5 * step(rng);
            const double a_bid = static_cast<double>(a_mid - 5 * (1 + coin(rng))) / 100.0;
            const double a_ask = static_cast<double>(a_mid + 5 * (1 + coin(rng))) / 100.0;
            const double b_bid = static_cast<double>(b_mid - 5 * (1 + coin(rng))) / 100.0;
            const double b_ask = static_cast<double>(b_mid + 5 * (1 + coin(rng))) / 100.0;
            // 第二档不影响 v1，也不影响 max_levels = 1 的新实现
            const OrderBook a = makeBook("A:BTCUSDT", {{a_bid, lots(rng) / 100.0}, {a_bid - 1, 9.0}},
                                         {{a_ask, lots(rng) / 100.0}, {a_ask + 1, 9.0}});
            const OrderBook b = makeBook("B:BTCUSDT", {{b_bid, lots(rng) / 100.0}, {b_bid - 1, 9.0}},
                                         {{b_ask, lots(rng) / 100.0}, {b_ask + 1, 9.0}});
            const bool old_found = v1.detect(a, b);
            const bool new_found = det.detect(a, b);
            REQUIRE_EQ(old_found, new_found);
        }
        REQUIRE_EQ(static_cast<std::uint64_t>(v1.signals.size()), det.getTotalSignals());
        const std::deque<ArbitrageSignal>& sigs = det.getSignals();
        const std::size_t offset = v1.signals.size() - sigs.size();
        for (std::size_t k = 0; k < sigs.size(); ++k) {
            const V1Detector::Signal& o = v1.signals[offset + k];
            const ArbitrageSignal& s = sigs[k];
            CHECK_EQ(s.buy_exchange, o.buy);
            CHECK_EQ(s.sell_exchange, o.sell);
            CHECK_EQ(s.buy_price, o.buy_price);
            CHECK_EQ(s.sell_price, o.sell_price);
            CHECK_EQ(s.quantity, o.qty);
            CHECK_EQ(s.gross_profit, o.gross);   // 逐位相等
            CHECK_EQ(s.net_profit, o.net);
            CHECK_EQ(s.levels_used, 1);
            CHECK_NEAR(s.vwap_buy, o.buy_price, 1e-9);   // cost / qty：与价格只差舍入
            CHECK_NEAR(s.vwap_sell, o.sell_price, 1e-9);
            if (s.net_profit == o.net && s.gross_profit == o.gross) ++matched;
        }
        CHECK_EQ(matched, sigs.size());
        CHECK_GT(v1.signals.size(), 100u);  // 场景确实产生了大量信号
        double total = 0.0;
        for (const V1Detector::Signal& o : v1.signals) total += o.net;
        CHECK_NEAR(det.getTotalNetProfit(), total, 1e-6);
    }
    // 直接对公式：gross = (bid - ask) * min(q)，fee = (ask + bid) * qty * f
    const double f = 0.00005;
    const SweepResult r = sweepOf(L({{67000.10, 0.3}}), L({{67012.35, 0.7}}), f, f);
    const double qty = std::min(0.3, 0.7);
    CHECK_EQ(r.qty, qty);
    CHECK_EQ(r.gross, (67012.35 - 67000.10) * qty);
    CHECK_EQ(r.fees, (67000.10 + 67012.35) * qty * f);
    CHECK_EQ(r.net, (67012.35 - 67000.10) * qty - (67000.10 + 67012.35) * qty * f);
    CHECK_GT(r.net, 0.0);
}

// ============================================================================
// 套利：检测器（去重、N 个交易所、有界信号队列）
// ============================================================================

TEST_CASE("detector: dedup per ordered pair - a one-cent move counts, sub-cent moves and repeats do not") {
    ArbitrageDetector det(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> books = {makeBook("A:X", {{66990.00, 1.0}}, {{67000.00, 1.0}}),
                                    makeBook("B:X", {{67010.00, 1.0}}, {{67020.00, 1.0}})};
    CHECK_EQ(det.detectAll(books), 1u);
    CHECK_EQ(det.detectAll(books), 0u);
    CHECK_EQ(det.detectVenue(0, books), 0u);
    // 67000.01 - 67000.00 在二进制浮点里是 0.0099999999947...，仍然算 1 分的变动
    books[0] = makeBook("A:X", {{66990.00, 1.0}}, {{67000.01, 1.0}});
    CHECK_EQ(det.detectVenue(0, books), 1u);
    books[0] = makeBook("A:X", {{66990.00, 1.0}}, {{67000.015, 1.0}});
    CHECK_EQ(det.detectVenue(0, books), 0u);  // 半分：不算变动
    books[1] = makeBook("B:X", {{67010.01, 1.0}}, {{67020.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, books), 1u);  // 买一变动 1 分
    // 数量变化不算价格变动
    books[1] = makeBook("B:X", {{67010.01, 5.0}}, {{67020.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, books), 0u);
    CHECK_EQ(det.pairState(0, 1).signals, 3u);
    // 另一个方向独立去重：B 便宜下来后 B->A 第一次出现机会立即出信号
    books[1] = makeBook("B:X", {{66900.00, 1.0}}, {{66950.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, books), 1u);
    CHECK_EQ(det.pairState(1, 0).signals, 1u);
    CHECK_EQ(det.getTotalSignals(), 4u);
    CHECK(ArbitrageDetector::moved(67000.01, 67000.00));
    CHECK(!ArbitrageDetector::moved(67000.005, 67000.00));
    CHECK(ArbitrageDetector::moved(0.02, 0.01));
    CHECK(!ArbitrageDetector::moved(5.0, 5.0));

    // 无数据（断线后的空簿）不出信号；恢复后价格未变 -> 仍被去重
    ArbitrageDetector d2(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> b2 = {makeBook("A:X", {{99.0, 1.0}}, {{100.0, 1.0}}), makeBook("B:X", {{101.0, 1.0}}, {{102.0, 1.0}})};
    CHECK_EQ(d2.detectAll(b2), 1u);
    const OrderBook saved = b2[1];
    b2[1] = OrderBook("B:X");
    CHECK_EQ(d2.detectVenue(1, b2), 0u);
    CHECK(!d2.pairState(0, 1).has_data);
    CHECK_EQ(d2.pairState(0, 1).sweep.qty, 0.0);
    b2[1] = saved;
    CHECK_EQ(d2.detectVenue(1, b2), 0u);
    CHECK(d2.pairState(0, 1).has_data);
    CHECK_GT(d2.pairState(0, 1).sweep.net, 0.0);
}

TEST_CASE("detector: min_profit is a strict threshold on the net profit after fees") {
    std::vector<OrderBook> books = {makeBook("A", {{99.0, 1.0}}, {{100.0, 1.0}}), makeBook("B", {{100.5, 1.0}}, {{101.0, 1.0}})};
    ArbitrageDetector at(std::vector<double>{0.0, 0.0}, 0.5, 0.0, 20);
    CHECK_EQ(at.detectAll(books), 0u);  // net == 0.5，不大于阈值
    CHECK_EQ(at.pairState(0, 1).sweep.net, 0.5);
    ArbitrageDetector below(std::vector<double>{0.0, 0.0}, 0.4999, 0.0, 20);
    CHECK_EQ(below.detectAll(books), 1u);
    // 手续费把净利润压到阈值以下
    ArbitrageDetector fee(std::vector<double>{0.001, 0.0015}, 0.3, 0.0, 20);
    CHECK_EQ(fee.detectAll(books), 0u);
    CHECK_NEAR(fee.pairState(0, 1).sweep.net, 0.5 - (100.0 * 0.001 + 100.5 * 0.0015), 1e-12);
    ArbitrageDetector fee2(std::vector<double>{0.001, 0.0015}, 0.2, 0.0, 20);
    CHECK_EQ(fee2.detectAll(books), 1u);
    CHECK_NEAR(fee2.getSignals().back().fees, 100.0 * 0.001 + 100.5 * 0.0015, 1e-12);
}

TEST_CASE("detector: N venues - detectVenue touches exactly the 2(N-1) pairs of that venue, detectAll all N(N-1)") {
    const std::size_t n = 4;
    ArbitrageDetector det(std::vector<double>(n, 0.0), 0.01, 0.0, 20);
    std::vector<OrderBook> books;
    for (std::size_t i = 0; i < n; ++i) books.push_back(makeBook("V" + std::to_string(i), {{99.0, 1.0}}, {{101.0, 1.0}}));
    CHECK_EQ(det.detectVenue(2, books), 0u);
    for (std::size_t b = 0; b < n; ++b) {
        for (std::size_t s = 0; s < n; ++s) {
            if (b == s) continue;
            const std::uint64_t expected = (b == 2 || s == 2) ? 1u : 0u;
            CHECK_EQ(det.pairState(b, s).evaluations, expected);
        }
    }
    det.detectAll(books);
    for (std::size_t b = 0; b < n; ++b) {
        for (std::size_t s = 0; s < n; ++s) {
            if (b == s) {
                CHECK_EQ(det.pairState(b, s).evaluations, 0u);  // 对角线从不评估
                continue;
            }
            const std::uint64_t expected = (b == 2 || s == 2) ? 2u : 1u;
            CHECK_EQ(det.pairState(b, s).evaluations, expected);
        }
    }
    ArbitrageDetector mask(std::vector<double>(n, 0.0), 0.01, 0.0, 20);
    mask.detectChanged({true, false, false, true}, books);
    for (std::size_t b = 0; b < n; ++b) {
        for (std::size_t s = 0; s < n; ++s) {
            if (b == s) continue;
            const bool involved = b == 0 || b == 3 || s == 0 || s == 3;
            CHECK_EQ(mask.pairState(b, s).evaluations, involved ? 1u : 0u);
        }
    }
    // 越界与 buy == sell 什么也不做
    CHECK(det.detectPair(1, 1, books[1], books[1]) == nullptr);
    CHECK(det.detectPair(7, 1, books[1], books[1]) == nullptr);
    CHECK_EQ(det.detectVenue(9, books), 0u);
    CHECK_THROWS(det.pairState(4, 0));
    CHECK_THROWS(ArbitrageDetector(std::vector<double>(ArbitrageDetector::kMaxVenues + 1, 0.0), 0.01));

    // 三个交易所：便宜的 A、中间的 B、贵的 C -> 恰好 A->B、A->C、B->C 三个有序对出信号，标签与编号正确
    ArbitrageDetector three(std::vector<double>{0.0, 0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> tb = {makeBook("A:BTC", {{99.0, 1.0}}, {{100.0, 1.0}}),
                                 makeBook("B:BTC", {{101.0, 2.0}}, {{101.5, 2.0}}),
                                 makeBook("C:BTC", {{102.0, 0.5}}, {{103.0, 1.0}})};
    CHECK_EQ(three.detectAll(tb), 3u);
    std::vector<std::pair<std::size_t, std::size_t>> got;
    for (const ArbitrageSignal& s : three.getSignals()) got.emplace_back(s.buy_venue, s.sell_venue);
    std::sort(got.begin(), got.end());
    const std::vector<std::pair<std::size_t, std::size_t>> want = {{0, 1}, {0, 2}, {1, 2}};
    CHECK(got == want);
    for (const ArbitrageSignal& s : three.getSignals()) {
        CHECK_EQ(s.buy_exchange, tb[s.buy_venue].getSymbol());
        CHECK_EQ(s.sell_exchange, tb[s.sell_venue].getSymbol());
        CHECK_EQ(s.buy_price, tb[s.buy_venue].getBestAsk().price);
        CHECK_EQ(s.sell_price, tb[s.sell_venue].getBestBid().price);
        CHECK_GT(s.timestamp_ms, 0);
    }
    const ArbitrageDetector::PairState& ac = three.pairState(0, 2);
    CHECK_EQ(ac.sweep.qty, 0.5);
    CHECK_EQ(ac.net_total, 1.0);
    CHECK_EQ(three.getTotalQty(), 1.0 + 0.5 + 0.5);
    CHECK_NEAR(three.getTotalNetProfit(), 1.0 + 1.0 + 0.25, 1e-12);
}

TEST_CASE("detector: the recent-signal deque is bounded at 1000 while totals keep counting; no stdout output") {
    ArbitrageDetector det(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> books = {makeBook("A", {{99.0, 1.0}}, {{100.0, 1.0}}), makeBook("B", {{101.0, 1.0}}, {{102.0, 1.0}})};
    std::ostringstream captured;
    std::streambuf* old = std::cout.rdbuf(captured.rdbuf());
    double expected_net = 0.0;
    for (int i = 0; i < 1500; ++i) {
        const double ask = i % 2 == 0 ? 100.0 : 100.05;
        books[0] = makeBook("A", {{99.0, 1.0}}, {{ask, 1.0}});
        if (det.detectVenue(0, books) == 1) expected_net += (101.0 - ask) * 1.0;
    }
    std::cout.rdbuf(old);
    CHECK(captured.str().empty());
    CHECK_EQ(det.getTotalSignals(), 1500u);
    CHECK_EQ(det.getSignals().size(), ArbitrageDetector::kMaxSignals);
    CHECK_NEAR(det.getTotalNetProfit(), expected_net, 1e-9);
    // 最旧的 500 条被丢弃：现在第一条是第 501 条（下标 500，偶数 -> ask 100）
    CHECK_EQ(det.getSignals().front().buy_price, 100.0);
    CHECK_EQ(det.getSignals().back().buy_price, 100.05);
    CHECK_EQ(det.pairState(0, 1).signals, 1500u);
}

TEST_CASE("detector: per-venue fee rates and depth caps flow from the constructor into every pair") {
    ArbitrageDetector det(std::vector<double>{0.001, 0.002, 0.0}, 0.0, 1.5, 2);
    CHECK_EQ(det.venueCount(), 3u);
    CHECK_EQ(det.feeRate(1), 0.002);
    CHECK_EQ(det.maxQty(), 1.5);
    CHECK_EQ(det.maxLevels(), 2);
    std::vector<OrderBook> books = {makeBook("A", {{90.0, 1.0}}, {{100.0, 1.0}, {100.5, 1.0}, {100.6, 1.0}}),
                                    makeBook("B", {{105.0, 1.0}, {104.0, 1.0}, {103.0, 1.0}}, {{110.0, 1.0}}),
                                    makeBook("C", {{80.0, 1.0}}, {{120.0, 1.0}})};
    det.detectAll(books);
    const SweepResult& r = det.pairState(0, 1).sweep;
    const SweepResult expect = ArbitrageDetector::sweep(books[0], books[1], 0.001, 0.002, 1.5, 2);
    CHECK_EQ(r.qty, 1.5);
    CHECK_EQ(r.qty, expect.qty);
    CHECK_EQ(r.net, expect.net);
    CHECK_EQ(r.fees, expect.fees);
    CHECK_EQ(det.getSignals().back().levels_used, 2);
}

// ============================================================================
// 文本宽度工具
// ============================================================================

// 独立的严格 UTF-8 校验（RFC 3629），用于检查 sanitize 的输出
bool isValidUtf8(const std::string& s) {
    std::size_t i = 0;
    while (i < s.size()) {
        const unsigned char b = static_cast<unsigned char>(s[i]);
        std::size_t n = 0;
        std::uint32_t cp = 0;
        if (b < 0x80) {
            ++i;
            continue;
        } else if ((b & 0xE0) == 0xC0) {
            n = 1;
            cp = b & 0x1F;
        } else if ((b & 0xF0) == 0xE0) {
            n = 2;
            cp = b & 0x0F;
        } else if ((b & 0xF8) == 0xF0) {
            n = 3;
            cp = b & 0x07;
        } else {
            return false;
        }
        for (std::size_t k = 1; k <= n; ++k) {
            if (i + k >= s.size()) return false;
            const unsigned char c = static_cast<unsigned char>(s[i + k]);
            if ((c & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (c & 0x3F);
        }
        const std::uint32_t min[] = {0, 0x80, 0x800, 0x10000};
        if (cp < min[n] || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += n + 1;
    }
    return true;
}

TEST_CASE("text_util: visibleWidth counts UTF-8 code points and skips ANSI escape sequences") {
    using text_util::visibleWidth;
    CHECK_EQ(visibleWidth(""), 0u);
    CHECK_EQ(visibleWidth("abc"), 3u);
    CHECK_EQ(visibleWidth("12.3 µs"), 7u);        // U+00B5 微符号（2 字节）
    CHECK_EQ(visibleWidth("12.3 μs"), 7u);        // U+03BC 希腊字母 mu（2 字节）
    CHECK_EQ(visibleWidth("╔═══╗"), 5u);          // 框线字符（每个 3 字节）
    CHECK_EQ(visibleWidth("…€"), 2u);
    CHECK_EQ(visibleWidth("\xF0\x9F\x98\x80"), 1u);  // 4 字节码点（按文档约定计 1 列）
    CHECK_EQ(visibleWidth("\033[32mLIVE\033[0m"), 4u);
    CHECK_EQ(visibleWidth("\033[1;38;5;208mX\033[0m"), 1u);
    CHECK_EQ(visibleWidth("\033[?25l\033[2J\033[H"), 0u);  // 私有模式 CSI
    CHECK_EQ(visibleWidth("\033]0;window title\007ok"), 2u);   // OSC ... 以 BEL 结束
    CHECK_EQ(visibleWidth("\033]8;;http://x\033\\link\033]8;;\033\\"), 4u);  // OSC ... 以 ST 结束
    CHECK_EQ(visibleWidth("a\tb\r\n"), 2u);       // C0 控制字符宽度为 0
    CHECK_EQ(visibleWidth("\x7f"), 0u);
    CHECK_EQ(visibleWidth("\033["), 0u);          // 未结束的 CSI
    CHECK_EQ(visibleWidth("\033[31"), 0u);
    CHECK_EQ(visibleWidth("x\033"), 1u);
    // 非法 UTF-8：每个坏字节按 1 列计（终端显示为 U+FFFD）
    CHECK_EQ(visibleWidth("\xff"), 1u);
    CHECK_EQ(visibleWidth("\xc3"), 1u);           // 缺少续字节的 2 字节序列
    CHECK_EQ(visibleWidth("\xc3("), 2u);
    CHECK_EQ(visibleWidth("\x80\x80"), 2u);       // 孤立的续字节
    CHECK_EQ(visibleWidth("\xc0\xaf"), 2u);       // 过长编码的 '/'
    CHECK_EQ(visibleWidth("\xe0\x80\xaf"), 3u);   // 过长的 3 字节编码
    CHECK_EQ(visibleWidth("\xed\xa0\x80"), 3u);   // UTF-16 代理码点
    CHECK_EQ(visibleWidth("\xf4\x90\x80\x80"), 4u);  // 大于 U+10FFFF
    CHECK_EQ(visibleWidth("\xe2\x94"), 2u);       // 从中间切开的框线字符
}

TEST_CASE("text_util: padRight / padLeft / center / fit pad to visible columns, colours and multi-byte included") {
    using namespace text_util;
    const std::string green = "\033[32mok\033[0m";
    CHECK_EQ(visibleWidth(padRight(green, 6)), 6u);
    CHECK_EQ(padRight(green, 6), green + "    ");
    CHECK_EQ(padLeft("µs", 4), std::string("  µs"));
    CHECK_EQ(padRight("toolong", 3), std::string("toolong"));  // 从不截断
    CHECK_EQ(center("ab", 5), std::string(" ab  "));            // 多出的一列放在右侧
    CHECK_EQ(center("abcdef", 4), std::string("abcd"));
    CHECK_EQ(visibleWidth(center("═µ═", 10)), 10u);
    CHECK_EQ(center(green, 6), "  " + green + "  ");
    CHECK_EQ(fit("abc", 5), std::string("abc  "));
    CHECK_EQ(fit("abcdef", 4, "…"), std::string("abc…"));
    CHECK_EQ(fit("abcdef", 4), std::string("abcd"));
    CHECK_EQ(repeat("─", 3), std::string("───"));
    CHECK_EQ(repeat("ab", 0), std::string());
    CHECK_EQ(stripAnsi("\033[1mA\033[0mB\033[?25lC"), std::string("ABC"));
}

TEST_CASE("text_util: truncateVisible never splits a code point or an escape and keeps trailing escapes") {
    using namespace text_util;
    CHECK_EQ(truncateVisible("héllo wörld", 5), std::string("héllo"));
    CHECK_EQ(truncateVisible("héllo wörld", 5, "…"), std::string("héll…"));
    CHECK_EQ(truncateVisible("µµµ", 2), std::string("µµ"));
    CHECK_EQ(truncateVisible("short", 10, "…"), std::string("short"));
    CHECK_EQ(truncateVisible("abc", 0), std::string());
    CHECK_EQ(truncateVisible("abc", 1, "……"), std::string("a"));  // 省略号比 width 还宽：不放省略号
    // 截断处之后的完整转义序列（复位码）保留，颜色不会泄漏
    CHECK_EQ(truncateVisible("\033[31mred text\033[0m", 3), std::string("\033[31mred\033[0m"));
    CHECK_EQ(truncateVisible("\033[31mred text\033[0m", 4, "…"), std::string("\033[31mred…\033[0m"));
}

TEST_CASE("text_util: a dangling escape sequence at the end is dropped, so appended padding is never swallowed") {
    using namespace text_util;
    // 不处理时："ab\033[" 后面补的空格会被当作 CSI 的中间字节吞掉，宽度只有 2
    CHECK_EQ(visibleWidth(std::string("ab\033[") + "   "), 2u);
    CHECK_EQ(padRight("ab\033[", 5), std::string("ab   "));
    CHECK_EQ(padLeft("ab\033[31", 4), std::string("  ab"));
    CHECK_EQ(fit("x\033", 3), std::string("x  "));
    CHECK_EQ(center("\033", 3), std::string("   "));
    CHECK_EQ(visibleWidth(padRight("a\033]0;title", 4)), 4u);
    CHECK_EQ(truncateVisible("abc\033[31", 10), std::string("abc"));
    CHECK_EQ(truncateVisible("abcdef\033[1", 3, "…"), std::string("ab…"));
    // 被后一个 ESC 打断的 ESC：去掉结尾的悬空 ESC 后它自己也悬空，一起去掉
    CHECK_EQ(padRight("a\033\033", 4), std::string("a   "));
    CHECK_EQ(padRight("a\033[\033[3", 4), std::string("a   "));
    CHECK_EQ(padRight("a\033\033[0m", 2), std::string("a\033\033[0m "));  // 结尾是完整序列：不动
    // 截断后截断点之前被打断的序列变成悬空
    CHECK_EQ(visibleWidth(truncateVisible("ab\033[\xc3xyz", 2) + "  "), 4u);
    // 完整的序列与中途被打断的序列都保留
    CHECK_EQ(padRight("\033[0m", 2), std::string("\033[0m  "));
    CHECK_EQ(padRight("\033[3\xc3\xa9", 2), std::string("\033[3\xc3\xa9 "));
    // 拼接后的框线不会被吞掉
    const std::string row = "│" + fit("bad\033[", 6) + "│";
    CHECK_EQ(visibleWidth(row), 8u);
    CHECK_EQ(stripAnsi(row), std::string("│bad   │"));
}

TEST_CASE("text_util: sanitize turns control characters and invalid UTF-8 into harmless text") {
    using text_util::sanitize;
    CHECK_EQ(sanitize("a\nb\tc\033[31md"), std::string("a b c [31md"));
    CHECK_EQ(sanitize("\xff"), std::string("?"));
    CHECK_EQ(sanitize("\xc2\x9b" "31m"), std::string(" 31m"));  // C1 CSI（U+009B）
    CHECK_EQ(sanitize("µ═…"), std::string("µ═…"));
    CHECK_EQ(sanitize("\xc0\xaf"), std::string("??"));
    CHECK_EQ(sanitize("\xed\xa0\x80"), std::string("???"));
    CHECK_EQ(sanitize("\x7f"), std::string(" "));
}

TEST_CASE("text_util: randomized invariants for width, truncation, fitting and sanitizing") {
    std::mt19937_64 rng(42);
    // 前 kValid 个片段是合法 UTF-8 / 完整的转义序列 / 控制字符，宽度已知且可相加；其后是非法字节与残缺序列
    // （它们与相邻片段可能组合成别的词法单元，宽度不可相加，只检查结构性质）
    const std::vector<std::pair<std::string, std::size_t>> pieces = {
        {"a", 1}, {"Z", 1}, {" ", 1}, {"µ", 1}, {"═", 1}, {"…", 1}, {"\xF0\x9F\x98\x80", 1}, {"\033[32m", 0},
        {"\033[0m", 0}, {"\033[1;31m", 0}, {"\033[?25l", 0}, {"\033]0;t\007", 0}, {"\t", 0}, {"\n", 0},
        {"\xff", 1}, {"\xc3", 1}, {"\x80", 1}, {"\xe2\x94", 2}, {"\033", 0}, {"\033[", 0}, {"\xed\xa0\x80", 3},
        {"\xc2\x9b", 1}};
    constexpr std::size_t kValid = 14;
    std::uniform_int_distribution<std::size_t> pick_valid(0, kValid - 1);
    std::uniform_int_distribution<std::size_t> pick_any(0, pieces.size() - 1);
    std::uniform_int_distribution<int> len(0, 24);
    std::uniform_int_distribution<std::size_t> width(0, 30);
    for (int iter = 0; iter < 20000; ++iter) {
        const bool valid_only = iter % 2 == 0;
        std::string s;
        std::size_t expected = 0;
        const int n = len(rng);
        for (int i = 0; i < n; ++i) {
            const auto& p = pieces[valid_only ? pick_valid(rng) : pick_any(rng)];
            s += p.first;
            expected += p.second;
        }
        const std::size_t vw = text_util::visibleWidth(s);
        const std::size_t w = width(rng);
        if (valid_only) {
            REQUIRE_EQ(vw, expected);
            REQUIRE_EQ(text_util::visibleWidth(text_util::stripAnsi(s)), vw);
        }
        const std::string t = text_util::truncateVisible(s, w);
        REQUIRE_EQ(text_util::visibleWidth(t), std::min(w, vw));
        // 截断结果的可见内容是原文可见内容的前缀（整码点）
        const std::string plain = text_util::stripAnsi(s);
        const std::string tplain = text_util::stripAnsi(t);
        REQUIRE(plain.compare(0, tplain.size(), tplain) == 0);
        const std::string te = text_util::truncateVisible(s, w, "…");
        REQUIRE_EQ(text_util::visibleWidth(te), std::min(w, vw));
        REQUIRE_EQ(text_util::visibleWidth(text_util::fit(s, w, "…")), w);
        REQUIRE_EQ(text_util::visibleWidth(text_util::center(s, w)), w);
        REQUIRE_EQ(text_util::visibleWidth(text_util::padRight(s, w)), std::max(w, vw));
        REQUIRE_EQ(text_util::visibleWidth(text_util::padLeft(s, w)), std::max(w, vw));
        // 结果可以安全拼接：后面追加的框线 / 文本不会被吞掉
        REQUIRE_EQ(text_util::visibleWidth(text_util::fit(s, w, "…") + "│"), w + 1);
        REQUIRE_EQ(text_util::visibleWidth(text_util::truncateVisible(s, w) + "x"), std::min(w, vw) + 1);
        REQUIRE_EQ(text_util::visibleWidth(text_util::padLeft(s, w) + "  "), std::max(w, vw) + 2);
        REQUIRE_EQ(text_util::visibleWidth(text_util::center(s, w) + "│"), w + 1);
        const std::string clean = text_util::sanitize(s);
        REQUIRE(isValidUtf8(clean));
        for (char c : clean) {
            const unsigned char u = static_cast<unsigned char>(c);
            REQUIRE(u >= 0x20 && u != 0x7F);
        }
    }
}

// ============================================================================
// 显示：帧与最终报告的框线对齐
// ============================================================================

std::u32string decodeUtf8(const std::string& s) {
    std::u32string out;
    for (std::size_t i = 0; i < s.size();) {
        const unsigned char b = static_cast<unsigned char>(s[i]);
        std::size_t n = b < 0x80 ? 0 : (b & 0xE0) == 0xC0 ? 1 : (b & 0xF0) == 0xE0 ? 2 : 3;
        char32_t cp = n == 0 ? b : n == 1 ? (b & 0x1F) : n == 2 ? (b & 0x0F) : (b & 0x07);
        for (std::size_t k = 1; k <= n && i + k < s.size(); ++k) cp = (cp << 6) | (s[i + k] & 0x3F);
        out.push_back(cp);
        i += n + 1;
    }
    return out;
}

// 只允许换行与 SGR 颜色序列（ESC [ 数字/分号 m）；不允许制表符、光标控制等其他控制字节
bool onlySgrControls(const std::string& s, std::string& why) {
    for (std::size_t i = 0; i < s.size(); ++i) {
        const unsigned char b = static_cast<unsigned char>(s[i]);
        if (b == '\n') continue;
        if (b == 0x1B) {
            std::size_t j = i + 1;
            if (j >= s.size() || s[j] != '[') {
                why = "non-CSI escape at byte " + std::to_string(i);
                return false;
            }
            ++j;
            while (j < s.size() && (std::isdigit(static_cast<unsigned char>(s[j])) || s[j] == ';')) ++j;
            if (j >= s.size() || s[j] != 'm') {
                why = "non-SGR CSI sequence at byte " + std::to_string(i);
                return false;
            }
            i = j;
            continue;
        }
        if (b < 0x20 || b == 0x7F) {
            why = "control byte " + std::to_string(b) + " at byte " + std::to_string(i);
            return false;
        }
    }
    return true;
}

// 检查文本中的每一个框：从每个左上角（┌ / ╔）沿顶边找到右上角，沿左边向下直到左下角；中间每一行的左右两列
// 必须是竖边（或 ├ ┤ 分隔线），底行必须是对应的两个底角。返回找到的框数；出错时 why 非空。
int checkBoxes(const std::string& text, std::string& why) {
    std::vector<std::u32string> grid;
    for (const std::string& line : splitLines(text_util::stripAnsi(text))) grid.push_back(decodeUtf8(line));
    auto at = [&](std::size_t r, std::size_t c) -> char32_t { return c < grid[r].size() ? grid[r][c] : U' '; };
    auto rowText = [&](std::size_t r) {
        std::string s;
        for (const std::string& line : splitLines(text_util::stripAnsi(text))) {
            if (r-- == 0) return line;
        }
        return s;
    };
    int boxes = 0;
    for (std::size_t r = 0; r < grid.size(); ++r) {
        for (std::size_t c = 0; c < grid[r].size(); ++c) {
            const char32_t tl = grid[r][c];
            if (tl != U'┌' && tl != U'╔') continue;
            const bool dbl = tl == U'╔';
            const char32_t tr = dbl ? U'╗' : U'┐', bl = dbl ? U'╚' : U'└', br = dbl ? U'╝' : U'┘';
            std::size_t c2 = c + 1;
            while (c2 < grid[r].size() && grid[r][c2] != tr) ++c2;
            if (c2 >= grid[r].size()) {
                why = "row " + std::to_string(r) + ": top-left corner without a top-right corner: " + rowText(r);
                return boxes;
            }
            std::size_t r2 = r + 1;
            for (; r2 < grid.size(); ++r2) {
                const char32_t lft = at(r2, c), rgt = at(r2, c2);
                if (lft == bl) {
                    if (rgt != br) {
                        why = "row " + std::to_string(r2) + ": bottom edge misaligned: " + rowText(r2);
                        return boxes;
                    }
                    break;
                }
                const bool ok_l = dbl ? lft == U'║' : (lft == U'│' || lft == U'├');
                const bool ok_r = dbl ? rgt == U'║' : (rgt == U'│' || rgt == U'┤');
                if (!ok_l || !ok_r) {
                    why = "row " + std::to_string(r2) + ": side borders not at columns " + std::to_string(c) + "/" +
                          std::to_string(c2) + ": " + rowText(r2);
                    return boxes;
                }
            }
            if (r2 >= grid.size()) {
                why = "box at row " + std::to_string(r) + " has no bottom edge";
                return boxes;
            }
            ++boxes;
        }
    }
    return boxes;
}

// 显示测试用的数据：books / 延迟统计 / 检测器，以及指向它们的 VenueView
struct DisplayFixture {
    std::vector<OrderBook> books;
    std::vector<std::unique_ptr<LatencyTracker>> trackers;
    std::unique_ptr<ArbitrageDetector> detector;
    std::vector<VenueView> views;

    explicit DisplayFixture(std::size_t n) {
        const char* names[] = {"Binance", "OKX", "Bybit"};
        books.push_back(makeBook("Binance:BTCUSDT",
                                 {{67000.10, 0.5}, {67000.00, 1.25}, {66999.50, 12.3456}, {66990.00, 0.001},
                                  {66980.00, 123456.789}, {66970.00, 2.0}},
                                 {{67000.20, 0.75}, {67000.30, 1.0}, {67001.00, 3.5}, {67002.00, 0.0101}}));
        books.push_back(OrderBook("OKX:BTCUSDT"));  // 断线后的空簿
        books.push_back(makeBook("Bybit:BTCUSDT", {{67010.00, 0.25}, {67009.90, 4.0}, {67009.00, 1.0}},
                                 {{67010.10, 0.3}, {67011.00, 2.0}, {67012.00, 1.5}}));
        books.resize(n);
        std::vector<double> fees = {0.0, 0.001, 0.0001};
        fees.resize(n);
        detector = std::make_unique<ArbitrageDetector>(fees, 0.01, 0.0, 20);
        detector->detectAll(books);
        if (n >= 3) {
            // 再制造几条信号：Binance 卖一下移
            books[0].updateAsk(67000.20, 0.0);
            detector->detectVenue(0, books);
        }
        const FeedStatus statuses[] = {FeedStatus::Live, FeedStatus::Reconnecting, FeedStatus::Syncing};
        for (std::size_t i = 0; i < n; ++i) {
            for (int k = 0; k < 3; ++k) trackers.push_back(std::make_unique<LatencyTracker>(std::string(names[i]) + " t"));
            LatencyTracker& proc = *trackers[3 * i];
            LatencyTracker& exch = *trackers[3 * i + 1];
            LatencyTracker& queue = *trackers[3 * i + 2];
            if (i == 0) {
                proc.recordNs(12345);  // 单个样本：p50 = p99 = 12.3 µs
                for (int k = 0; k < 100; ++k) exch.recordNs(static_cast<std::uint64_t>(40 + k) * 1000000);
                for (int k = 0; k < 50; ++k) queue.recordNs(static_cast<std::uint64_t>(1000 + 100 * k));
            } else if (i == 2) {
                for (int k = 1; k <= 1000; ++k) proc.recordNs(static_cast<std::uint64_t>(k) * 150);
            }
            VenueView v;
            v.name = names[i];
            v.book = &books[i];
            v.status = statuses[i];
            v.stats.messages = 1000 * (i + 1);
            v.stats.reconnects = i;
            v.stats.gaps = i;
            v.stats.checksum_failures = i == 1 ? 2 : 0;
            v.stats.resyncs = i * 2;
            v.stats.dropped_snapshots = i == 2 ? 7 : 0;
            if (i == 1) v.last_error = "connection lost: \033[31mend of\tfile\nnext line";
            if (i == 0) v.last_error = "old error that must stay hidden while LIVE";
            v.processing = &proc;
            v.exchange = &exch;
            v.queue = &queue;
            v.fee_rate = fees[i];
            views.push_back(v);
        }
    }
};

void checkFrame(const std::string& frame, std::size_t venues, bool color) {
    std::string why;
    CHECK(onlySgrControls(frame, why));
    if (!why.empty()) std::cerr << "    " << why << std::endl;
    if (!color) CHECK(frame.find('\033') == std::string::npos);
    const std::vector<std::string> lines = splitLines(frame);
    REQUIRE(!lines.empty());
    for (std::size_t i = 0; i < lines.size(); ++i) {
        if (text_util::visibleWidth(lines[i]) != Display::kFrameWidth) {
            std::cerr << "    line " << i << " has width " << text_util::visibleWidth(lines[i]) << ": "
                      << text_util::stripAnsi(lines[i]) << std::endl;
        }
        CHECK_EQ(text_util::visibleWidth(lines[i]), Display::kFrameWidth);
    }
    why.clear();
    const int boxes = checkBoxes(frame, why);
    if (!why.empty()) std::cerr << "    " << why << std::endl;
    CHECK(why.empty());
    // 标题栏 + 每个交易所一个订单簿框 + 套利监控 + 行情源性能
    CHECK_EQ(boxes, static_cast<int>(venues) + 3);
}

TEST_CASE("display: frames for 1, 2 and 3 venues keep every boxed line at the same visible width (colour on/off)") {
    for (std::size_t n = 1; n <= 3; ++n) {
        DisplayFixture fx(n);
        for (bool color : {true, false}) {
            FrameOptions fo;
            fo.levels = 5;
            fo.color = color;
            fo.uptime_s = 3723.9;
            fo.config_warnings = n == 2 ? 1 : 0;
            const std::string frame = Display::renderFrame(fx.views, *fx.detector, fo);
            checkFrame(frame, n, color);
            const std::string plain = text_util::stripAnsi(frame);
            CHECK(contains(plain, "Crypto HFT Engine v2.0"));
            CHECK(contains(plain, "uptime 01:02:03"));
            CHECK(contains(plain, std::to_string(n) + (n == 1 ? " venue, 1 live" : " venues, 1 live")));
            CHECK(contains(plain, "Binance:BTCUSDT  LIVE"));
            CHECK(contains(plain, "67000.10"));
            CHECK(contains(plain, "12.3/12.3"));  // 处理延迟 µs，一位小数
            CHECK(!contains(plain, "old error"));  // LIVE 时不显示 lastError
            if (n == 1) CHECK(contains(plain, "needs at least two venues"));
            if (n >= 2) {
                CHECK(contains(plain, "OKX:BTCUSDT  RECONNECTING"));
                CHECK(contains(plain, "-- no data --"));
                CHECK(contains(plain, "Spread --  Mid --"));
                CHECK(contains(plain, "! connection lost:  [31mend of file next line"));  // 已清洗
                CHECK(contains(plain, "Binance  OKX"));
            }
            if (n == 3) {
                CHECK(contains(plain, "Bybit    OKX"));
                CHECK(contains(plain, "Binance -> Bybit"));  // 最近信号
                CHECK(countOf(plain, "\n  │  Binance  ") == 2);  // Binance 作为买方的两个交易所对
            }
            if (n == 2) CHECK(contains(plain, "1 config warning (see the log)"));
            if (color) {
                CHECK(contains(frame, "\033[32mLIVE\033[0m"));
                if (n >= 2) CHECK(contains(frame, "\033[33mRECONNECTING\033[0m"));
                if (n >= 3) CHECK(contains(frame, "\033[33mSYNCING\033[0m"));
            }
        }
    }
}

TEST_CASE("display: status colours (LIVE green, CONNECTING/SYNCING/RECONNECTING yellow, STOPPED red)") {
    const display_detail::Palette pal = display_detail::Palette::make(true);
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Live, pal)), std::string("\033[32m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Connecting, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Syncing, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Reconnecting, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Stopped, pal)), std::string("\033[31m"));
    const display_detail::Palette none = display_detail::Palette::make(false);
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Live, none)), std::string());

    DisplayFixture fx(3);
    fx.views[1].status = FeedStatus::Stopped;
    fx.views[2].status = FeedStatus::Connecting;
    FrameOptions fo;
    const std::string frame = Display::renderFrame(fx.views, *fx.detector, fo);
    CHECK(contains(frame, "\033[31mSTOPPED\033[0m"));
    CHECK(contains(frame, "\033[33mCONNECTING\033[0m"));
    checkFrame(frame, 3, true);
}

TEST_CASE("display: number formatting helpers keep fixed widths") {
    CHECK_EQ(Display::formatNumber(67000.1, 2, 10), std::string("  67000.10"));
    CHECK_EQ(Display::formatNumber(123456.789, 4, 8), std::string("123456.8"));
    CHECK_EQ(Display::formatNumber(1.5, 2, 8, true), std::string("   +1.50"));
    CHECK_EQ(Display::formatNumber(-0.25, 2, 6), std::string(" -0.25"));
    CHECK_EQ(Display::formatNumber(1e30, 2, 6).size(), 6u);
    CHECK_EQ(Display::formatNumber(std::numeric_limits<double>::infinity(), 2, 5), std::string("   --"));
    CHECK_EQ(Display::pricePrecision(67000.0), 2);
    CHECK_EQ(Display::pricePrecision(2.5), 4);
    CHECK_EQ(Display::pricePrecision(0.12), 5);
    CHECK_EQ(Display::pricePrecision(0.0), 2);
    CHECK_EQ(Display::formatUs(12345.0), std::string("12.3"));
    CHECK_EQ(Display::formatUs(950.0), std::string("0.9"));
    CHECK_EQ(Display::formatDuration(3723.9), std::string("01:02:03"));
    CHECK_EQ(Display::formatDuration(-5.0), std::string("00:00:00"));
}

TEST_CASE("display: pair rows show '--' for a venue that is not LIVE, even while its last book is still there") {
    // 两个交易所的簿交叉：在 A 以 100.00 买、在 B 以 100.50 卖，两者都 LIVE 时检测器出过一次信号
    std::vector<OrderBook> books;
    books.push_back(makeBook("A:BTCUSDT", {{99.90, 1.0}}, {{100.00, 1.0}}));
    books.push_back(makeBook("B:BTCUSDT", {{100.50, 2.0}}, {{100.60, 2.0}}));
    ArbitrageDetector det(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    det.detectAll(books);
    REQUIRE_EQ(det.getTotalSignals(), 1u);
    std::vector<VenueView> views(2);
    for (std::size_t i = 0; i < 2; ++i) {
        views[i].name = i == 0 ? "A" : "B";
        views[i].book = &books[i];
        views[i].status = FeedStatus::Live;
    }
    // 交易所对行（买方、卖方两列名字之后是 Best Ask / Best Bid / Diff / Net / Qty / Signals）
    auto pairRow = [&](const std::string& buy, const std::string& sell) {
        FrameOptions fo;
        fo.color = false;
        const std::string frame = Display::renderFrame(views, det, fo);
        const std::string key = "│  " + text_util::padRight(buy, 9) + text_util::padRight(sell, 9);
        for (const std::string& line : splitLines(frame)) {
            if (contains(line, key)) return line;
        }
        return std::string();
    };
    std::string ab = pairRow("A", "B");
    REQUIRE(!ab.empty());
    CHECK(contains(ab, "100.00"));
    CHECK(contains(ab, "100.50"));
    CHECK(contains(ab, "+0.50"));
    CHECK_EQ(countOf(ab, " --"), 0u);
    for (FeedStatus s : {FeedStatus::Reconnecting, FeedStatus::Syncing, FeedStatus::Connecting, FeedStatus::Stopped}) {
        views[1].status = s;
        ab = pairRow("A", "B");
        REQUIRE(!ab.empty());
        CHECK(contains(ab, "100.00"));    // A 仍是 LIVE：它的卖一照常显示
        CHECK(!contains(ab, "100.50"));   // B 的买一（已过期）不显示
        CHECK(!contains(ab, "+0.50"));
        CHECK_EQ(countOf(ab, " --"), 4u);  // Best Bid、Diff、Net、Qty
        CHECK(contains(ab, "--        1"));  // 累计信号数照常显示
        const std::string ba = pairRow("B", "A");
        CHECK(!contains(ba, "100.60"));
        CHECK(contains(ba, "99.90"));
        CHECK_EQ(countOf(ba, " --"), 4u);  // Best Ask、Diff、Net、Qty
    }
    // 订单簿框照常显示 B 最后的簿与状态
    FrameOptions fo;
    fo.color = false;
    const std::string frame = Display::renderFrame(views, det, fo);
    CHECK(contains(frame, "B:BTCUSDT  STOPPED"));
    CHECK(contains(frame, "100.60"));
    checkFrame(frame, 2, false);
}

TEST_CASE("display: one shared plain-number formatter and cursor sequences usable with write(2)") {
    // headless 状态行与报告共用同一个格式化函数
    CHECK_EQ(Display::plainNumber(99.9, 3), std::string("99.900"));
    CHECK_EQ(Display::plainNumber(-1.005, 2), std::string("-1.00"));
    CHECK_EQ(Display::plainNumber(1234.4, 0), std::string("1234"));
    // 光标序列是字符数组：sizeof - 1 是长度，信号处理函数里可以直接 write(fd, kShowCursor, sizeof - 1)
    static_assert(sizeof(Display::kHideCursor) == 7, "kHideCursor must be a char array");
    static_assert(sizeof(Display::kShowCursor) == 7, "kShowCursor must be a char array");
    CHECK_EQ(std::string(Display::kHideCursor), std::string("\033[?25l"));
    CHECK_EQ(std::string(Display::kShowCursor), std::string("\033[?25h"));
}

TEST_CASE("display: max_rows shrinks the frame step by step and cuts it with a notice only as a last resort") {
    DisplayFixture fx(3);
    FrameOptions fo;
    fo.levels = 20;
    const std::string full = Display::renderFrame(fx.views, *fx.detector, fo);
    const std::size_t full_lines = splitLines(full).size();
    CHECK(contains(full, "! connection lost"));
    for (int rows : {60, 45, 40, 35, 31, 30}) {
        fo.max_rows = rows;
        const std::string fitted = Display::renderFrame(fx.views, *fx.detector, fo);
        CHECK_LT(splitLines(fitted).size(), full_lines);
        CHECK_LE(splitLines(fitted).size(), static_cast<std::size_t>(rows));
        checkFrame(fitted, 3, true);  // 精简后的帧仍然是完整、对齐的框
        CHECK(!contains(fitted, "terminal too small"));
    }
    // 30 行恰好放得下最精简的 3 交易所帧：没有错误行、没有最近信号区、每个簿 1 档
    fo.max_rows = 30;
    const std::string minimal = Display::renderFrame(fx.views, *fx.detector, fo);
    CHECK(!contains(minimal, "! connection lost"));
    CHECK(!contains(minimal, "VWAP Buy"));
    CHECK_EQ(splitLines(minimal).size(), 30u);
    for (int rows : {29, 5, 1}) {  // 放不下：保留前 rows - 1 行，最后一行提示
        fo.max_rows = rows;
        const std::string cut = Display::renderFrame(fx.views, *fx.detector, fo);
        const std::vector<std::string> lines = splitLines(cut);
        REQUIRE_EQ(lines.size(), static_cast<std::size_t>(rows));
        CHECK(contains(lines.back(), "terminal too small: this view needs 30 rows, " + std::to_string(rows) + " available"));
        for (const std::string& line : lines) CHECK_EQ(text_util::visibleWidth(line), Display::kFrameWidth);
        std::string why;
        CHECK(onlySgrControls(cut, why));
    }
}

TEST_CASE("display: present() writes one frame: cursor home (first frame clears), erase per line, optional clipping") {
    DisplayFixture fx(2);
    FrameOptions fo;
    const std::string frame = Display::renderFrame(fx.views, *fx.detector, fo);
    const std::size_t n = splitLines(frame).size();
    TempStream ts;
    REQUIRE(ts.f != nullptr);
    Display::present(frame, true, 0, ts.f);
    const std::string first = readAll(ts.f);
    CHECK_EQ(first.rfind("\033[2J\033[H", 0), 0u);
    CHECK_EQ(first.substr(first.size() - 6), std::string("\033[K\033[J"));
    CHECK_EQ(countOf(first, "\033[K\n"), n - 1);  // 最后一行之后不换行
    CHECK_EQ(countOf(first, "\n"), n - 1);

    // 终端宽度 40：每行裁到 40 列；占满整行的行不发擦除到行尾
    TempStream ts2;
    Display::present(frame, false, 40, ts2.f);
    const std::string clipped = readAll(ts2.f);
    CHECK_EQ(clipped.rfind("\033[H", 0), 0u);
    CHECK(!contains(clipped, "\033[2J"));
    CHECK(!contains(clipped, "\033[K"));  // 这一帧每行都至少 40 列宽
    const std::string body = clipped.substr(3, clipped.size() - 6);
    for (const std::string& line : splitLines(body)) CHECK_EQ(text_util::visibleWidth(line), 40u);

    // 终端恰好与帧同宽：不裁剪，也不发擦除到行尾（否则会擦掉最后一列的右边框）
    TempStream ts3;
    Display::present(frame, false, Display::kFrameWidth, ts3.f);
    const std::string exact = readAll(ts3.f);
    CHECK(!contains(exact, "\033[K"));
    CHECK_EQ(exact.size(), frame.size() - 1 + 3 + 3);  // 原样 + 归位 + 末尾擦除，最后的换行去掉
    // 更宽的终端：每行后擦除到行尾
    TempStream ts4;
    Display::present(frame, false, 120, ts4.f);
    CHECK_EQ(countOf(readAll(ts4.f), "\033[K"), n);
}

TEST_CASE("display: final report for 1, 2 and 3 venues has aligned boxes and lists every venue and pair") {
    for (std::size_t n = 1; n <= 3; ++n) {
        DisplayFixture fx(n);
        for (bool color : {true, false}) {
            const std::string report = Display::renderFinalReport(fx.views, *fx.detector, color);
            std::string why;
            CHECK(onlySgrControls(report, why));
            if (!color) CHECK(report.find('\033') == std::string::npos);
            why.clear();
            const int boxes = checkBoxes(report, why);
            if (!why.empty()) std::cerr << "    " << why << std::endl;
            CHECK(why.empty());
            CHECK_EQ(boxes, static_cast<int>(n) + 2);  // 每个交易所的延迟表 + 行情源统计 + 套利汇总
            const std::string plain = text_util::stripAnsi(report);
            CHECK(contains(plain, "═══ Final Report ═══"));
            for (std::size_t i = 0; i < n; ++i) CHECK(contains(plain, fx.views[i].name + " Latency"));
            CHECK(contains(plain, "Arbitrage summary"));
            CHECK_EQ(countOf(plain, " -> "), n * (n - 1));
            CHECK(contains(plain, "P50:" + std::string(20, ' ') + "12.3 µs"));  // 标签 18 列 + 数值 10 列右对齐
            if (n >= 2) CHECK(contains(plain, "OKX last error: connection lost:  [31mend of file next line"));
            // 每行宽度：表格的每一行与其边框等宽
            for (const std::string& line : splitLines(plain)) CHECK_LE(text_util::visibleWidth(line), Display::kFrameWidth);
        }
    }
}

// ============================================================================
// 日志
// ============================================================================

TEST_CASE("logger: concurrent writers never interleave lines; timestamps are monotonic; messages stay one line") {
    TempPath dir("test_app_logs");
    std::string path;
    {
        Logger logger(dir.str(), false);
        REQUIRE(logger.isOpen());
        path = logger.path();
        CHECK(contains(path, "/arb_"));
        std::vector<std::thread> threads;
        for (int t = 0; t < 4; ++t) {
            threads.emplace_back([&logger, t] {
                for (int i = 0; i < 2000; ++i) {
                    logger.logFeed("[T" + std::to_string(t) + "] event " + std::to_string(i) + " a\nb\033[31m");
                }
            });
        }
        for (std::thread& th : threads) th.join();
    }
    const std::vector<std::string> lines = splitLines(readFile(path));
    std::vector<int> next(4, 0);
    std::size_t feed_lines = 0;
    std::string last_stamp;
    for (const std::string& line : lines) {
        REQUIRE(line.size() >= 24u);
        const std::string stamp = line.substr(0, 23);  // "YYYY-MM-DD HH:MM:SS.mmm"
        CHECK(stamp >= last_stamp);
        last_stamp = stamp;
        const std::size_t p = line.find(" [FEED] [T");
        if (p == std::string::npos) continue;
        ++feed_lines;
        const int t = line[p + 10] - '0';
        REQUIRE(t >= 0 && t < 4);
        const std::string expect = " [FEED] [T" + std::to_string(t) + "] event " + std::to_string(next[t]) + " a b [31m";
        CHECK_EQ(line.substr(p), expect);
        ++next[t];
    }
    CHECK_EQ(feed_lines, 8000u);
    for (int t = 0; t < 4; ++t) CHECK_EQ(next[t], 2000);
    CHECK(contains(lines.front(), "[INFO] === Crypto HFT Engine v2.0 Started ==="));
    CHECK(contains(lines.back(), "[INFO] === Crypto HFT Engine v2.0 Stopped ==="));
}

TEST_CASE("logger: signal line spacing, echo of selected tags, and an N-venue final report") {
    TempPath dir("test_app_logs");
    TempStream echo;
    std::string path;
    DisplayFixture fx(3);
    {
        Logger logger(dir.str(), false);
        path = logger.path();
        logger.setEcho(echo.f);
        REQUIRE(!fx.detector->getSignals().empty());
        logger.logSignal(fx.detector->getSignals().front());
        logger.logSpread("Binance", 67000.2, "OKX", 67001.5, 1.3);
        logger.log("hello");
        logger.warn("careful");
        logger.setEcho(nullptr);
        logger.log("not echoed");
        logger.logFinalReport(fx.views, *fx.detector);
    }
    const std::string file = readFile(path);
    const std::string echoed = readAll(echo.f);
    CHECK(contains(file, "[SIGNAL] SIGNAL | Buy Binance:BTCUSDT @ "));
    CHECK(contains(file, " | Sell "));
    CHECK(!contains(file, "| Sell" + std::string("B")));
    CHECK(!contains(file, "|  Buy"));
    CHECK(contains(file, "[SPREAD] SPREAD | Binance Ask: 67000.20 | OKX Bid: 67001.50 | Diff: $1.30"));
    CHECK(contains(echoed, "[SIGNAL] SIGNAL | Buy"));
    CHECK(contains(echoed, "[INFO] hello"));
    CHECK(contains(echoed, "[WARN] careful"));
    CHECK(!contains(echoed, "SPREAD"));
    CHECK(!contains(echoed, "not echoed"));
    CHECK(contains(file, "[INFO] === Final Report ==="));
    for (const char* name : {"Binance", "OKX", "Bybit"}) {
        CHECK(contains(file, std::string("LATENCY | ") + name + " | Msgs: "));
        CHECK(contains(file, std::string("FEED | ") + name + " | Status: "));
    }
    CHECK_EQ(countOf(file, "[SUMMARY] PAIR | "), 6u);
    CHECK(contains(file, "PAIR | Binance -> Bybit | Signals: "));
    CHECK(contains(file, "[SUMMARY] SUMMARY | Signals: " + std::to_string(fx.detector->getTotalSignals())));
    CHECK(contains(file, "FEED | OKX | Last error: connection lost:  [31mend of file next line"));
}

// ============================================================================
// 命令行
// ============================================================================

bool parse(std::initializer_list<const char*> args, CliOptions& out, std::string& error) {
    std::vector<const char*> argv = {"crypto_hft_engine"};
    argv.insert(argv.end(), args.begin(), args.end());
    return parseCommandLine(static_cast<int>(argv.size()), argv.data(), out, error);
}

TEST_CASE("cli: --config/--headless/--duration/--help in both '--opt value' and '--opt=value' forms") {
    CliOptions o;
    std::string err;
    REQUIRE(parse({}, o, err));
    CHECK_EQ(o.config_path, std::string("config.json"));
    CHECK(!o.config_given);
    CHECK(!o.headless);
    CHECK_EQ(o.duration_s, 0.0);
    CHECK(!o.help);
    REQUIRE(parse({"--config", "tools/mock_config.json", "--headless", "--duration", "2.5"}, o, err));
    CHECK_EQ(o.config_path, std::string("tools/mock_config.json"));
    CHECK(o.config_given);
    CHECK(o.headless);
    CHECK_EQ(o.duration_s, 2.5);
    REQUIRE(parse({"--config=a.json", "--duration=3"}, o, err));
    CHECK_EQ(o.config_path, std::string("a.json"));
    CHECK_EQ(o.duration_s, 3.0);
    REQUIRE(parse({"-h"}, o, err));
    CHECK(o.help);
    REQUIRE(parse({"--help", "--headless"}, o, err));
    CHECK(o.help);
    const std::string usage = usageText("crypto_hft_engine");
    for (const char* opt : {"--config <path>", "--headless", "--duration <seconds>", "--help"}) CHECK(contains(usage, opt));
}

TEST_CASE("cli: errors for unknown options, missing values, bad durations and positional arguments") {
    CliOptions o;
    std::string err;
    CHECK(!parse({"--bogus"}, o, err));
    CHECK(contains(err, "unknown option \"--bogus\""));
    CHECK(!parse({"--config"}, o, err));
    CHECK(contains(err, "--config requires a value"));
    CHECK(!parse({"--config="}, o, err));
    CHECK(contains(err, "non-empty"));
    CHECK(!parse({"--duration"}, o, err));
    for (const char* bad : {"0", "-1", "abc", "2s", "inf", "nan", "1e20", ""}) {
        err.clear();
        const std::string arg = std::string("--duration=") + bad;
        CHECK(!parse({arg.c_str()}, o, err));
        CHECK(!err.empty());
    }
    CHECK(!parse({"config.json"}, o, err));
    CHECK(contains(err, "unexpected argument \"config.json\""));
    CHECK(!parse({"--headlessly"}, o, err));
    CHECK(!parse({"--configs", "x"}, o, err));
}

TEST_CASE("cli: cli_util::parseUnsigned shared by exchange_sim and mock_exchange") {
    std::uint64_t v = 7;
    CHECK(cli_util::parseUnsigned("0", v));
    CHECK_EQ(v, 0u);
    CHECK(cli_util::parseUnsigned("65535", v));
    CHECK_EQ(v, 65535u);
    CHECK(cli_util::parseUnsigned("18446744073709551615", v));
    CHECK_EQ(v, std::numeric_limits<std::uint64_t>::max());
    v = 7;
    for (const char* bad : {"", "-1", "+1", "12x", "0x10", "1.5", "18446744073709551616"}) {
        CHECK(!cli_util::parseUnsigned(bad, v));
    }
    CHECK(!cli_util::parseUnsigned(nullptr, v));
    CHECK_EQ(v, 7u);  // 失败时不改写输出
}

// ============================================================================
// 端到端：模拟交易所 → 真实 MarketDataFeed → MonitorApp 事件循环
// ============================================================================

using Levels = std::vector<std::pair<std::string, std::string>>;

constexpr const char* kBinanceDiffPath = "/ws/btcusdt@depth@100ms";
constexpr const char* kBinancePartialPath = "/ws/btcusdt@depth20@100ms";
constexpr const char* kOkxPath = "/ws/v5/public";
constexpr const char* kBybitPath = "/v5/public/spot";
constexpr const char* kOkxBooksSub = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
constexpr const char* kOkxBooks5Sub = R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})";
constexpr const char* kBybitSub = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";

std::string levelsJson(const Levels& levels, bool okx) {
    std::string s = "[";
    for (std::size_t i = 0; i < levels.size(); ++i) {
        s += (i > 0 ? ",[\"" : "[\"") + levels[i].first + "\",\"" + levels[i].second + "\"" + (okx ? ",\"0\",\"1\"]" : "]");
    }
    return s + "]";
}

std::string binanceDepth(std::uint64_t id, const Levels& bids, const Levels& asks) {
    return R"({"lastUpdateId":)" + std::to_string(id) + R"(,"bids":)" + levelsJson(bids, false) + R"(,"asks":)" +
           levelsJson(asks, false) + "}";
}

std::string binanceDiff(std::uint64_t first, std::uint64_t last, const Levels& bids, const Levels& asks) {
    return R"({"e":"depthUpdate","E":)" + std::to_string(hft::wallClockMs()) + R"(,"s":"BTCUSDT","U":)" +
           std::to_string(first) + R"(,"u":)" + std::to_string(last) + R"(,"b":)" + levelsJson(bids, false) +
           R"(,"a":)" + levelsJson(asks, false) + "}";
}

std::int32_t okxChecksum(const Levels& bids, const Levels& asks) {
    std::string s;
    for (std::size_t i = 0; i < 25; ++i) {
        if (i < bids.size()) s += (s.empty() ? "" : ":") + bids[i].first + ":" + bids[i].second;
        if (i < asks.size()) s += (s.empty() ? "" : ":") + asks[i].first + ":" + asks[i].second;
    }
    return hft::crc32Signed(s);
}

std::string okxBooksSnapshot(const Levels& bids, const Levels& asks, std::int64_t seq) {
    return R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":)" +
           levelsJson(asks, true) + R"(,"bids":)" + levelsJson(bids, true) + R"(,"ts":")" +
           std::to_string(hft::wallClockMs()) + R"(","checksum":)" + std::to_string(okxChecksum(bids, asks)) +
           R"(,"prevSeqId":-1,"seqId":)" + std::to_string(seq) + "}]}";
}

std::string okxBooks5(const Levels& bids, const Levels& asks) {
    return R"({"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"asks":)" + levelsJson(asks, true) +
           R"(,"bids":)" + levelsJson(bids, true) + R"(,"instId":"BTC-USDT","ts":")" +
           std::to_string(hft::wallClockMs()) + R"("}]})";
}

std::string bybitSnapshot(const Levels& bids, const Levels& asks, std::uint64_t u) {
    const std::string ts = std::to_string(hft::wallClockMs());
    return R"({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":)" + ts + R"(,"data":{"s":"BTCUSDT","b":)" +
           levelsJson(bids, false) + R"(,"a":)" + levelsJson(asks, false) + R"(,"u":)" + std::to_string(u) +
           R"(,"seq":)" + std::to_string(u * 10) + R"(},"cts":)" + ts + "}";
}

// 指向模拟服务器的配置（三个交易所共用一个服务器，脚本按升级路径分派）
AppConfig mockAppConfig(MockExchangeServer& server) {
    AppConfig c = ConfigReader::getDefaults();
    for (VenueConfig& v : c.venues) {
        v.host = "127.0.0.1";
        v.port = server.portString();
    }
    VenueConfig& b = c.venues[0];
    b.rest_host = "127.0.0.1";
    b.rest_port = server.portString();
    c.arbitrage.fee_rate = 0.0;
    c.display.refresh_ms = 20;
    c.display.spread_log_interval = 5;
    c.connection.tls_verify = true;
    c.connection.ca_file = server.caFile();
    c.connection.reconnect_initial_ms = 20;
    c.connection.reconnect_max_ms = 200;
    c.source = "<test>";
    return c;
}

// 在测试线程上运行 app.run()；另一个线程在 done() 成立且至少运行了 min_ms 后设置停止标志（最长 timeout_ms）
template <class Done>
int runApp(MonitorApp& app, Done done, int min_ms, int timeout_ms, double* stop_to_return_ms = nullptr) {
    std::atomic<bool> stop{false};
    std::atomic<std::int64_t> stop_ns{0};
    const std::int64_t start = hft::nowNs();
    std::thread watcher([&] {
        waitFor([&] { return done(); }, timeout_ms, 2);
        while (hft::nowNs() - start < static_cast<std::int64_t>(min_ms) * 1000000) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        stop_ns.store(hft::nowNs());
        stop.store(true);
    });
    const int rc = app.run(stop);
    const std::int64_t end = hft::nowNs();
    watcher.join();
    if (stop_to_return_ms != nullptr) *stop_to_return_ms = static_cast<double>(end - stop_ns.load()) / 1e6;
    return rc;
}

// 管道：写端交给被测代码（fdopen 成 FILE*，或 dup2 成子进程的 stdout/stderr），后台线程读取读端。
// 可以暂停读取来模拟被阻塞的终端（Ctrl+S 流控、卡住的 SSH 会话）：管道写满后写入方阻塞在 write(2) 里。
class PipeReader {
public:
    // capacity > 0：把管道容量设为至少 capacity 字节（F_SETPIPE_SZ，按页取整）
    explicit PipeReader(int capacity = 0, bool start_reading = true) : reading_(start_reading) {
        int fds[2];
        if (::pipe2(fds, O_CLOEXEC) != 0) throw std::runtime_error("pipe2 failed");
        read_fd_ = fds[0];
        write_fd_ = fds[1];
        if (capacity > 0) ::fcntl(write_fd_, F_SETPIPE_SZ, capacity);
        capacity_ = ::fcntl(write_fd_, F_GETPIPE_SZ);
        reader_ = std::thread([this] { readLoop(); });
    }

    ~PipeReader() {
        closeWriteEnd();
        quit_.store(true);
        reader_.join();
        ::close(read_fd_);
    }

    PipeReader(const PipeReader&) = delete;
    PipeReader& operator=(const PipeReader&) = delete;

    int writeFd() const noexcept { return write_fd_; }
    // 写端已交给别人（例如 fdopen 之后由 fclose 关闭）：本对象不再关闭它
    int releaseWriteFd() noexcept { return std::exchange(write_fd_, -1); }
    void closeWriteEnd() noexcept {
        if (write_fd_ >= 0) ::close(std::exchange(write_fd_, -1));
    }

    int capacity() const noexcept { return capacity_; }
    void setReading(bool on) noexcept { reading_.store(on); }
    // 管道中尚未读取的字节数
    int buffered() const noexcept {
        int n = 0;
        return ::ioctl(read_fd_, FIONREAD, &n) == 0 ? n : -1;
    }
    bool eof() const noexcept { return eof_.load(); }

    std::string text() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return text_;
    }
    // 等到输出（从 from 字节起）出现 part
    bool waitForText(const std::string& part, int timeout_ms, std::size_t from = 0) const {
        return mock_exchange::waitFor(
            [&] {
                std::lock_guard<std::mutex> lock(mutex_);
                return text_.find(part, std::min(from, text_.size())) != std::string::npos;
            },
            timeout_ms, 2);
    }
    bool waitEof(int timeout_ms) const {
        return mock_exchange::waitFor([this] { return eof_.load(); }, timeout_ms, 2);
    }

private:
    void readLoop() {
        char buf[16384];
        while (!quit_.load()) {
            if (!reading_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            pollfd p{read_fd_, POLLIN, 0};
            if (::poll(&p, 1, 10) <= 0) continue;
            const ssize_t n = ::read(read_fd_, buf, sizeof(buf));
            if (n <= 0) {
                eof_.store(true);
                return;
            }
            std::lock_guard<std::mutex> lock(mutex_);
            text_.append(buf, static_cast<std::size_t>(n));
        }
    }

    int read_fd_ = -1;
    int write_fd_ = -1;
    int capacity_ = 0;
    std::atomic<bool> reading_;
    std::atomic<bool> quit_{false};
    std::atomic<bool> eof_{false};
    mutable std::mutex mutex_;
    std::string text_;
    std::thread reader_;
};

TEST_CASE("app: feedConfigFor carries venue, connection and logging settings into FeedConfig") {
    AppConfig c = ConfigReader::getDefaults();
    c.connection.tls_verify = false;
    c.connection.ca_file = "/tmp/ca.pem";
    c.connection.stall_timeout_ms = 4321;
    c.connection.reconnect_initial_ms = 77;
    c.connection.reconnect_max_ms = 888;
    const FeedConfig f = MonitorApp::feedConfigFor(c, c.venues[0], 2);
    CHECK(f.exchange == Exchange::BINANCE);
    CHECK_EQ(f.venue_id, 2u);
    CHECK_EQ(f.name, std::string("Binance"));
    CHECK_EQ(f.host, c.venues[0].host);
    CHECK_EQ(f.port, c.venues[0].port);
    CHECK_EQ(f.path, c.venues[0].path);
    CHECK_EQ(f.rest_host, std::string("api.binance.com"));
    CHECK_EQ(f.rest_port, std::string("443"));
    CHECK_EQ(f.snapshot_path, c.venues[0].snapshot_path);
    CHECK(!f.tls_verify);
    CHECK_EQ(f.ca_file, std::string("/tmp/ca.pem"));
    CHECK_EQ(f.stall_timeout_ms, 4321);
    CHECK_EQ(f.reconnect_initial_ms, 77);
    CHECK_EQ(f.reconnect_max_ms, 888);
    CHECK_EQ(f.publish_levels, BookSnapshot::kMaxLevels);
    CHECK(!f.log_to_stderr);
    const FeedConfig o = MonitorApp::feedConfigFor(c, c.venues[1], 0);
    CHECK_EQ(o.subscribe_msg, c.venues[1].subscribe_msg);
    CHECK(o.exchange == Exchange::OKX);
}

TEST_CASE("app: the venue list is AppConfig::enabledVenues() of the app's own copy, so a temporary config is safe") {
    TempPath logs("test_app_logs");
    Logger logger(logs.str(), false);
    auto disabledBinance = [] {
        AppConfig c = ConfigReader::getDefaults();
        c.venues[0].enabled = false;
        c.venues[2].name = "Bybit-X";
        c.venues[2].fee_rate = 0.0025;
        return c;
    };
    // 临时 AppConfig：构造结束后即被销毁，MonitorApp 只能引用自己的副本（ASan 下悬空指针会被发现）
    MonitorApp app(disabledBinance(), logger, RunOptions{});
    const AppConfig expect = disabledBinance();
    const std::vector<const VenueConfig*> enabled = expect.enabledVenues();
    REQUIRE_EQ(app.venueCount(), enabled.size());
    REQUIRE_EQ(app.venueCount(), 2u);
    for (std::size_t i = 0; i < enabled.size(); ++i) {
        CHECK_EQ(app.venueConfig(i).name, enabled[i]->name);
        CHECK(app.venueConfig(i).exchange == enabled[i]->exchange);
        CHECK_EQ(app.feed(i).getName(), enabled[i]->name);
        CHECK_EQ(app.feed(i).venueId(), static_cast<std::uint32_t>(i));
        CHECK_EQ(app.book(i).getSymbol(), enabled[i]->name + ":" + enabled[i]->symbol);
    }
    CHECK_EQ(app.venueConfig(1).name, std::string("Bybit-X"));
    CHECK_EQ(app.detector().feeRate(1), 0.0025);
    const std::vector<VenueView> views = app.collectViews();
    CHECK_EQ(views[1].fee_rate, 0.0025);
}

TEST_CASE("app: three mock venues end to end - REST sync, checksummed books, event-driven signals, logs, report") {
    MockExchangeServer server;
    std::atomic<int> okx_sent{0}, bybit_sent{0};
    std::atomic<bool> done{false};
    server.setHttpHandler(MockExchangeServer::replySequence(
        {HttpReply{200, binanceDepth(103, {{"99.90", "1"}}, {{"100.00", "1"}}), "application/json", 0}}));
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() == kBinanceDiffPath) {
            // 第一条增量被缓冲 → REST 快照 L=103 → 回放 U=101..u=105（加一档买盘）→ LIVE
            ws.send(binanceDiff(101, 105, {{"99.80", "2"}}, {}));
            ws.waitUntil([&] { return okx_sent.load() > 0 && bybit_sent.load() > 0; }, 5000);
            ws.sleepMs(300);
            // 卖一从 100.00 移到 100.20：Binance->OKX 价格变动 -> 新信号；Binance->Bybit 不再盈利
            ws.send(binanceDiff(106, 106, {}, {{"100.00", "0"}, {"100.20", "1"}}));
            ws.sleepMs(300);
            done.store(true);
        } else if (ws.target() == kOkxPath) {
            ws.expectText(kOkxBooksSub);
            ws.send(okxBooksSnapshot({{"100.5", "2"}}, {{"100.6", "2"}}, 1000));
            okx_sent.fetch_add(1);
        } else if (ws.target() == kBybitPath) {
            ws.expectText(kBybitSub);
            ws.send(bybitSnapshot({{"100.1", "3"}}, {{"100.3", "3"}}, 1));
            bybit_sent.fetch_add(1);
        }
    });
    TempPath logs("test_app_e2e_logs");
    TempStream out;
    std::string log_path;
    const AppConfig cfg = mockAppConfig(server);
    {
        Logger logger(logs.str(), false);
        log_path = logger.path();
        RunOptions opt;
        opt.headless = true;
        opt.out = out.f;
        MonitorApp app(cfg, logger, opt);
        REQUIRE_EQ(app.venueCount(), 3u);
        double stop_ms = 0.0;
        const int rc = runApp(app, [&] { return done.load(); }, 1300, 15000, &stop_ms);
        CHECK_EQ(rc, 0);
        CHECK(done.load());
        CHECK_LT(stop_ms, 1000.0);  // 停止请求到 run() 返回（含停止 3 个 feed 与最终报告）

        const ArbitrageDetector& det = app.detector();
        CHECK_EQ(det.getTotalSignals(), 4u);
        CHECK_NEAR(det.getTotalNetProfit(), 0.5 + 0.1 + 0.4 + 0.3, 1e-6);
        CHECK_EQ(det.pairState(0, 1).signals, 2u);  // Binance -> OKX：100.00 与 100.20 各一次
        CHECK_EQ(det.pairState(0, 2).signals, 1u);  // Binance -> Bybit
        CHECK_EQ(det.pairState(2, 1).signals, 1u);  // Bybit -> OKX（数量受 OKX 买一的 2 限制）
        CHECK_EQ(det.pairState(1, 0).signals + det.pairState(1, 2).signals + det.pairState(2, 0).signals, 0u);
        CHECK_NEAR(det.pairState(2, 1).net_total, 0.4, 1e-9);
        CHECK_EQ(det.pairState(2, 1).qty_total, 2.0);
        bool saw_first = false;
        for (const ArbitrageSignal& s : det.getSignals()) {
            if (s.buy_venue == 0 && s.sell_venue == 1 && s.buy_price == 100.0) {
                saw_first = true;
                CHECK_EQ(s.buy_exchange, std::string("Binance:BTCUSDT"));
                CHECK_EQ(s.sell_exchange, std::string("OKX:BTCUSDT"));
                CHECK_EQ(s.sell_price, 100.5);
                CHECK_EQ(s.quantity, 1.0);
                CHECK_EQ(s.levels_used, 1);
            }
        }
        CHECK(saw_first);

        const OrderBook& binance = app.book(0);
        CHECK_EQ(binance.getSymbol(), std::string("Binance:BTCUSDT"));
        CHECK_EQ(binance.getBestAsk().price, 100.2);
        CHECK_EQ(binance.getBestBid().price, 99.9);
        CHECK_EQ(binance.getBidCount(), 2);
        CHECK_EQ(app.book(1).getBestBid().price, 100.5);
        CHECK_EQ(app.book(2).getBestAsk().price, 100.3);
        for (std::size_t i = 0; i < 3; ++i) {
            CHECK_GE(app.snapshotsApplied(i), 1u);
            CHECK_EQ(app.queueLatency(i).count(), app.snapshotsApplied(i));
            CHECK_GE(app.feed(i).stats().published, app.snapshotsApplied(i));
            CHECK(app.feed(i).status() == FeedStatus::Stopped);
        }
        CHECK_EQ(app.feed(0).stats().rest_snapshots, 1u);
        CHECK_EQ(app.feed(1).stats().checksum_failures, 0u);
        CHECK_GE(app.feed(1).getExchangeLatency().count(), 1u);
        CHECK_GT(app.ticks(), 10u);
        CHECK_EQ(app.renders(), 0u);
    }
    const std::string text = readAll(out.f);
    const std::string port = server.portString();
    CHECK(contains(text, "[FEED] [Binance] connecting to wss://127.0.0.1:" + port + kBinanceDiffPath));
    CHECK(contains(text, "[FEED] [OKX] book synced (LIVE)"));
    CHECK(contains(text, "[FEED] [Bybit] book synced (LIVE)"));
    CHECK(contains(text, "[SIGNAL] SIGNAL | Buy Binance:BTCUSDT @ 100.0000 | Sell OKX:BTCUSDT @ 100.5000 | Qty: 1.0000"));
    CHECK_EQ(countOf(text, "[SIGNAL] SIGNAL | "), 4u);
    CHECK(contains(text, "[00:00:01] Binance LIVE 99.900/100."));  // 至少 5 位有效数字
    CHECK(contains(text, "| OKX LIVE 100.50/100.60"));
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK_EQ(countOf(text, "Shutting down"), 1u);
    CHECK(contains(text, "═══ Final Report ═══"));
    CHECK(contains(text, "Binance -> OKX"));
    CHECK(text.find('\033') == std::string::npos);  // headless 输出无颜色
    CHECK(contains(text, "Goodbye!"));
    std::string why;
    const std::size_t report_at = text.find("═══ Final Report ═══");
    REQUIRE(report_at != std::string::npos);
    CHECK_EQ(checkBoxes(text.substr(report_at), why), 5);
    CHECK(why.empty());

    const std::string log = readFile(log_path);
    CHECK_EQ(countOf(log, "[SIGNAL] SIGNAL | Buy "), 4u);
    CHECK(contains(log, "[INFO] Venue Binance: wss://127.0.0.1:" + port + kBinanceDiffPath));
    CHECK(contains(log, "REST https://127.0.0.1:" + port + "/api/v3/depth?symbol=BTCUSDT&limit=1000"));
    CHECK(contains(log, "[FEED] [Binance] book synced (LIVE)"));
    CHECK(contains(log, "[SPREAD] SPREAD | Binance Ask: 100.20 | OKX Bid: 100.50 | Diff: $0.30"));
    CHECK(contains(log, "PAIR | Binance -> OKX | Signals: 2"));
    CHECK(contains(log, "[SUMMARY] SUMMARY | Signals: 4 | Net Profit: $1.30"));
    CHECK(server.errors().empty());
}

TEST_CASE("app: a venue that disconnects publishes an empty book -> no data, no stale signals; TUI frames stay aligned") {
    MockExchangeServer server;
    std::atomic<bool> okx_closed{false}, done{false};
    std::atomic<int> okx_sessions{0};
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() == kBinancePartialPath) {
            ws.send(binanceDepth(1, {{"99.90", "1"}}, {{"100.00", "1"}}));
            ws.waitUntil([&] { return okx_closed.load(); }, 5000);
            ws.sleepMs(300);
            // OKX 已断线（空簿）：卖一变动本来会触发 Binance->OKX 的新信号，但对方无数据
            ws.send(binanceDepth(2, {{"99.95", "1"}}, {{"100.05", "1"}}));
            ws.sleepMs(300);
            done.store(true);
        } else if (ws.target() == kOkxPath) {
            ws.expectText(kOkxBooks5Sub);
            if (okx_sessions.fetch_add(1) == 0) {
                ws.send(okxBooks5({{"100.5", "2"}}, {{"100.6", "2"}}));
                ws.sleepMs(300);
                ws.close(1001, "going away");
                okx_closed.store(true);
            }
            // 重连后的会话保持静默
        }
    });
    AppConfig cfg = mockAppConfig(server);
    cfg.venues[0].path = kBinancePartialPath;
    cfg.venues[0].rest_host.clear();
    cfg.venues[0].snapshot_path.clear();
    cfg.venues[1].subscribe_msg = kOkxBooks5Sub;
    cfg.venues[2].enabled = false;
    TempPath logs("test_app_e2e_logs");
    TempStream out;
    {
        Logger logger(logs.str(), false);
        RunOptions opt;
        opt.headless = false;  // 终端模式：帧写到 out（不是终端：不限制尺寸）
        opt.color = false;
        opt.out = out.f;
        MonitorApp app(cfg, logger, opt);
        REQUIRE_EQ(app.venueCount(), 2u);
        // 等 Binance 脚本结束，并且 OKX 已经重连上（第二个会话）
        const int rc = runApp(app, [&] { return done.load() && okx_sessions.load() >= 2; }, 500, 15000);
        CHECK_EQ(rc, 0);
        CHECK(done.load());
        const ArbitrageDetector& det = app.detector();
        CHECK_EQ(det.getTotalSignals(), 1u);
        CHECK_EQ(det.pairState(0, 1).signals, 1u);
        CHECK(!det.pairState(0, 1).has_data);
        CHECK(app.book(1).empty());
        CHECK_EQ(app.book(0).getBestAsk().price, 100.05);
        CHECK_GE(app.feed(1).stats().reconnects, 1u);
        CHECK_GE(okx_sessions.load(), 2);
        CHECK_GT(app.renders(), 5u);
        CHECK_EQ(app.renders(), app.ticks());
    }
    const std::string text = readAll(out.f);
    CHECK(contains(text, Display::kHideCursor));
    CHECK(contains(text, Display::kShowCursor));
    // 最后一帧：最后一个 "光标归位" 到显示光标之间
    const std::size_t home = text.rfind("\033[H");
    const std::size_t show = text.find(Display::kShowCursor, home == std::string::npos ? 0 : home);
    REQUIRE(home != std::string::npos);
    REQUIRE(show != std::string::npos);
    std::string frame = text.substr(home + 3, show - home - 3);
    std::string plain;
    for (const std::string& line : splitLines(frame)) {
        std::string l = line;
        for (const char* seq : {"\033[K", "\033[J"}) {
            for (std::size_t p = l.find(seq); p != std::string::npos; p = l.find(seq)) l.erase(p, 3);
        }
        if (!l.empty()) plain += l + "\n";
    }
    std::string why;
    CHECK(onlySgrControls(plain, why));
    CHECK_EQ(checkBoxes(plain, why), 5);
    if (!why.empty()) std::cerr << "    " << why << std::endl;
    for (const std::string& line : splitLines(plain)) CHECK_EQ(text_util::visibleWidth(line), Display::kFrameWidth);
    CHECK(contains(plain, "Binance:BTCUSDT  LIVE"));
    CHECK(contains(plain, "100.05"));
    CHECK(contains(plain, "-- no data --"));
    CHECK(contains(plain, "OKX:BTCUSDT  SYNCING") || contains(plain, "OKX:BTCUSDT  CONNECTING") ||
          contains(plain, "OKX:BTCUSDT  RECONNECTING"));  // 重连后的会话静默：不会回到 LIVE
    CHECK(contains(plain, "Binance -> OKX"));  // 最近信号
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK(contains(text, "Final Report"));
}

TEST_CASE("app: a stale book drained after its venue left LIVE never pairs for arbitrage (blocked terminal output)") {
    // 终端输出被阻塞（Ctrl+S 流控、卡住的 SSH）：管道事先写满，主线程在第一次输出（隐藏光标）时就阻塞，还没有排空过队列。
    // 期间 OKX 推送一本与 Binance 交叉的簿（在 Binance 以 100.00 买、在 OKX 以 100.50 卖）后断线，之后的会话保持静默
    // （状态停在 SYNCING）。输出恢复后主线程才依次取出 Binance 的簿、OKX 过期的簿与断线空簿：这时 OKX 已不是 LIVE，
    // 过期的簿不能配对出信号。
    MockExchangeServer server;
    std::atomic<bool> okx_closed{false};
    std::atomic<int> okx_sessions{0};
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() == kBinancePartialPath) {
            ws.send(binanceDepth(1, {{"99.90", "1"}}, {{"100.00", "1"}}));
        } else if (ws.target() == kOkxPath) {
            ws.expectText(kOkxBooks5Sub);
            if (okx_sessions.fetch_add(1) == 0) {
                ws.send(okxBooks5({{"100.5", "2"}}, {{"100.6", "2"}}));
                ws.sleepMs(100);
                ws.close(1001, "going away");
                okx_closed.store(true);
            }
            // 重连后的会话保持静默
        }
    });
    AppConfig cfg = mockAppConfig(server);
    cfg.venues[0].path = kBinancePartialPath;
    cfg.venues[0].rest_host.clear();
    cfg.venues[0].snapshot_path.clear();
    cfg.venues[1].subscribe_msg = kOkxBooks5Sub;
    cfg.venues[2].enabled = false;
    TempPath logs("test_app_e2e_logs");
    PipeReader pipe(4096, false);  // 先不读
    REQUIRE(pipe.capacity() > 0);
    const std::string filler(static_cast<std::size_t>(pipe.capacity()), '.');
    REQUIRE_EQ(::write(pipe.writeFd(), filler.data(), filler.size()), static_cast<ssize_t>(filler.size()));
    REQUIRE_EQ(pipe.buffered(), pipe.capacity());  // 管道已满：之后的任何写入都会阻塞
    std::FILE* out = ::fdopen(pipe.releaseWriteFd(), "w");
    REQUIRE(out != nullptr);
    std::string log_path;
    {
        Logger logger(logs.str(), false);
        log_path = logger.path();
        RunOptions opt;
        opt.headless = false;
        opt.color = false;
        opt.out = out;
        MonitorApp app(cfg, logger, opt);
        REQUIRE_EQ(app.venueCount(), 2u);
        std::atomic<bool> stop{false};
        std::atomic<bool> sequence_ok{false};
        std::thread controller([&] {
            // 1) 两本簿与断线空簿都已入队，OKX 已重连到静默会话（不再是 LIVE）
            sequence_ok.store(waitFor(
                [&] {
                    return okx_closed.load() && okx_sessions.load() >= 2 && app.feed(1).status() != FeedStatus::Live &&
                           app.feed(1).stats().published >= 2 && app.feed(0).stats().published >= 1;
                },
                10000, 2));
            // 2) 恢复输出：主线程（进入事件循环之前就阻塞在隐藏光标的写入里）这才开始排空积压的快照
            pipe.setReading(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            stop.store(true);
        });
        const int rc = app.run(stop);
        controller.join();
        CHECK_EQ(rc, 0);
        CHECK(sequence_ok.load());
        const ArbitrageDetector& det = app.detector();
        CHECK_EQ(det.getTotalSignals(), 0u);
        CHECK_EQ(det.pairState(0, 1).signals, 0u);
        CHECK(!det.pairState(0, 1).has_data);
        CHECK_GE(app.snapshotsApplied(1), 2u);  // 过期的簿与空簿都被应用过
        CHECK(app.book(1).empty());
        CHECK_EQ(app.book(0).getBestAsk().price, 100.0);
        CHECK(app.feed(0).stats().dropped_snapshots == 0 && app.feed(1).stats().dropped_snapshots == 0);
    }
    std::fclose(out);
    CHECK(pipe.waitEof(5000));
    const std::string text = text_util::stripAnsi(pipe.text());
    CHECK(contains(text, "Binance:BTCUSDT  LIVE"));
    CHECK(contains(text, "-- no data --"));
    CHECK(contains(text, "Final Report"));
    CHECK_EQ(countOf(readFile(log_path), "[SIGNAL]"), 0u);
    CHECK(server.errors().empty());
}

TEST_CASE("app: no enabled venue -> exit code 1 without starting anything") {
    AppConfig cfg = ConfigReader::getDefaults();
    for (VenueConfig& v : cfg.venues) v.enabled = false;
    TempPath logs("test_app_logs");
    TempStream out;
    Logger logger(logs.str(), false);
    RunOptions opt;
    opt.headless = true;
    opt.out = out.f;
    MonitorApp app(cfg, logger, opt);
    CHECK_EQ(app.venueCount(), 0u);
    std::atomic<bool> stop{false};
    CHECK_EQ(app.run(stop), 1);
    CHECK(readAll(out.f).empty());
}

TEST_CASE("app: --duration ends the run on time even while every venue keeps failing to connect") {
    // 一个刚刚关闭的端口：连接被拒绝，feed 不停退避重连
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    const std::string port = std::to_string(ntohs(addr.sin_port));
    ::close(fd);

    AppConfig cfg = ConfigReader::getDefaults();
    for (VenueConfig& v : cfg.venues) {
        v.host = "127.0.0.1";
        v.port = port;
    }
    cfg.connection.reconnect_initial_ms = 20;
    cfg.connection.reconnect_max_ms = 50;
    cfg.display.refresh_ms = 50;
    TempPath logs("test_app_logs");
    TempStream out;
    Logger logger(logs.str(), false);
    RunOptions opt;
    opt.headless = true;
    opt.duration_s = 0.6;
    opt.out = out.f;
    MonitorApp app(cfg, logger, opt);
    std::atomic<bool> stop{false};
    const std::int64_t t0 = hft::nowNs();
    CHECK_EQ(app.run(stop), 0);
    const double ms = static_cast<double>(hft::nowNs() - t0) / 1e6;
    CHECK_GE(ms, 600.0);
    CHECK_LT(ms, 1600.0);
    const std::string text = readAll(out.f);
    CHECK(contains(text, "Shutting down (duration elapsed after 0.6 s)"));
    CHECK(contains(text, "connection refused") || contains(text, "Connection refused"));
    CHECK(contains(text, "reconnecting in "));
    CHECK(contains(text, "═══ Final Report ═══"));
    for (std::size_t i = 0; i < app.venueCount(); ++i) {
        CHECK_GE(app.feed(i).stats().connect_failures, 2u);
        CHECK(app.feed(i).status() == FeedStatus::Stopped);
    }
    CHECK_EQ(app.detector().getTotalSignals(), 0u);
}

}  // namespace

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
