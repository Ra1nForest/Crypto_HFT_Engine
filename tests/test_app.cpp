//
// test_app.cpp
// 应用集成测试：配置逐字段回退 / 钳制 / 告警，深度扫单数学（含与 v1 公式逐位等价）、去重、N 个交易所的交易所对覆盖，
// 文本可见宽度工具，帧与最终报告的框线对齐，线程安全日志，命令行解析，以及用进程内模拟交易所端到端驱动 MonitorApp。
// Application integration tests: per-field config fallback / clamping / warnings, depth sweep math
// (including bit-for-bit equivalence with the v1 formula), dedup, venue-pair coverage for N venues,
// visible-width text utilities, box alignment of frames and the final report, thread-safe logging,
// command-line parsing, and driving MonitorApp end to end with an in-process mock exchange.
//

// 启用库内调试断言（必须在包含任何 hft 头文件之前定义）
// Enable the library's debug asserts (must be defined before any hft header is included)
#define HFT_ENABLE_ASSERTS

#include "test_framework.h"

#include "app.h"
#include "arbitrage_detector.h"
#include "cli_util.h"
#include "config.h"
#include "display.h"
#include "hft/common.h"
#include "hft/crc32.h"
#include "hft/spsc_queue.h"
#include "hft/timing.h"
#include "logger.h"
#include "mock_exchange_server.h"
#include "orderbook.h"
#include "text_util.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
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
#include <functional>
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
// Reads the entire contents of a tmpfile()
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
// A file in the source tree (independent of the current directory)
std::string repoPath(const std::string& rel) {
    return (fs::path(__FILE__).parent_path().parent_path() / rel).string();
}

// 临时路径（名称在进程内唯一）：析构时连同内容一起删除
// Temporary path (name unique within the process): removed together with its contents on destruction
struct TempPath {
    fs::path path;
    explicit TempPath(const std::string& prefix, const std::string& suffix = "") {
        static std::atomic<int> counter{0};
        path = fs::temp_directory_path() /
               (prefix + "_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)) + suffix);
    }
    ~TempPath() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempPath(const TempPath&) = delete;
    TempPath& operator=(const TempPath&) = delete;
    std::string str() const { return path.string(); }
};

// tmpfile() 的 RAII 包装
// RAII wrapper for tmpfile()
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

constexpr const char* kOkxBooksSub = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";
constexpr const char* kBybitSub = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";

// ============================================================================
// 配置
// Configuration
// ============================================================================

std::string jsonString(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    return out + "\"";
}

// 配置文本：以随仓库发布的 config.json 为基础（块与键保持文件中的顺序），按块 / 键覆盖原始 JSON 值
// Config text: starts from the config.json shipped with the repo (blocks and keys keep the file's order)
// and overrides raw JSON values per block / key
class ConfigText {
public:
    ConfigText() {
        simdjson::dom::parser parser;
        const simdjson::padded_string json(readFile(repoPath("config.json")));
        for (auto [name, block] : simdjson::dom::object(parser.parse(json))) {
            blocks_.push_back(Block{std::string(name), {}, {}});
            for (auto [key, value] : simdjson::dom::object(block)) {
                blocks_.back().fields.emplace_back(std::string(key), simdjson::minify(value));
            }
        }
    }

    // 设置（或新增）一个键的原始 JSON 值
    // Sets (or adds) the raw JSON value of a key
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
    // Replaces the whole block with raw JSON (e.g. "null", "[]", "{\"enabled\": false}"); adds the block if it
    // does not exist
    ConfigText& setBlock(const std::string& block, const std::string& raw) {
        find(block).raw = raw;
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
            if (b.raw.empty()) {
                s += "{";
                for (std::size_t k = 0; k < b.fields.size(); ++k) {
                    s += (k > 0 ? ", " : "") + jsonString(b.fields[k].first) + ": " + b.fields[k].second;
                }
                s += "}";
            } else {
                s += b.raw;
            }
            s += i + 1 < blocks_.size() ? ",\n" : "\n";
        }
        return s + "}\n";
    }

private:
    struct Block {
        std::string name;
        std::vector<std::pair<std::string, std::string>> fields;
        std::string raw;  // 非空时整块用这段原始 JSON
                          // if non-empty, the whole block is this raw JSON
    };

    Block& find(const std::string& name) {
        for (Block& b : blocks_) {
            if (b.name == name) return b;
        }
        blocks_.push_back(Block{name, {}, {}});
        return blocks_.back();
    }

    std::vector<Block> blocks_;
};

// 写入临时文件并加载（不打印）
// Writes to a temporary file and loads it (without printing)
AppConfig loadText(const std::string& json) {
    TempPath file("test_app_config", ".json");
    std::ofstream(file.path, std::ios::binary) << json;
    return ConfigReader::load(file.str(), false);
}

AppConfig loadText(const ConfigText& t) { return loadText(t.str()); }

// 随仓库发布的 config.json 的取值：内置默认值，只是全局 fee_rate = 0.0
// The values of the config.json shipped with the repo: the built-in defaults, except global fee_rate = 0.0
AppConfig shipped() {
    AppConfig c = ConfigReader::getDefaults();
    c.arbitrage.fee_rate = 0.0;
    return c;
}

// 配置的全部取值（不含 warnings / source），用于整体比较
// All values of a config (excluding warnings / source), for comparing as a whole
std::string describe(const AppConfig& c) {
    std::ostringstream s;
    s.precision(17);
    for (const VenueConfig& v : c.venues) {
        s << v.key << ": exchange " << static_cast<int>(v.exchange) << " name " << v.name << " enabled " << v.enabled
          << " host " << v.host << " port " << v.port << " path " << v.path << " symbol " << v.symbol << " subscribe "
          << v.subscribe_msg << " rest " << v.rest_host << ":" << v.rest_port << v.snapshot_path << " fee ";
        if (v.fee_rate) {
            s << *v.fee_rate << "\n";
        } else {
            s << "global\n";
        }
    }
    const ArbitrageConfig& a = c.arbitrage;
    s << "arbitrage: " << a.fee_rate << " " << a.min_profit << " " << a.max_qty << " " << a.max_levels << "\n";
    const DisplayConfig& d = c.display;
    s << "display: " << d.refresh_ms << " " << d.levels << " " << d.spread_log_interval << "\n";
    const ConnectionConfig& k = c.connection;
    s << "connection: " << k.tls_verify << " " << k.ca_file << " " << k.stall_timeout_ms << " "
      << k.reconnect_initial_ms << " " << k.reconnect_max_ms << "\n";
    return s.str();
}

// 告警列表完整且按顺序等于 want，取值等于 values（不一致时打印用例名与两边）
// The warning list equals want, complete and in order, and the values equal values (on a mismatch, prints
// the case name and both sides)
void checkConfig(const std::string& what, const AppConfig& c, const std::vector<std::string>& want,
                 const AppConfig& values) {
    if (c.warnings != want) {
        std::cerr << "    " << what << ": warnings were:\n" << joinLines(c.warnings) << "    expected:\n" << joinLines(want);
    }
    CHECK(c.warnings == want);
    if (describe(c) != describe(values)) std::cerr << "    " << what << ": values differ" << std::endl;
    CHECK_EQ(describe(c), describe(values));
}

// 一个配置用例：对随仓库发布的 config.json 做 edit，期望恰好得到 warnings（读取顺序），
// 取值等于 shipped() 再做 expect 的修改；also 是额外的检查（可为空）
// A config case: apply edit to the config.json shipped with the repo and expect exactly warnings
// (in reading order), with the values equal to shipped() plus expect's changes; also is an extra
// check (may be empty)
struct ConfigCase {
    std::string name;
    std::function<void(ConfigText&)> edit;
    std::vector<std::string> warnings;
    std::function<void(AppConfig&)> expect;
    std::function<void(const AppConfig&)> also;
};

void runConfigCases(const std::vector<ConfigCase>& cases) {
    for (const ConfigCase& k : cases) {
        ConfigText t;
        k.edit(t);
        const AppConfig c = loadText(t);
        AppConfig values = shipped();
        if (k.expect) k.expect(values);
        CHECK(!c.source.empty());
        checkConfig(k.name, c, k.warnings, values);
        if (k.also) k.also(c);
    }
}

// 主机名不合法的告警："[Config] <field>: \"<value>\" is not a host name ..., using default \"<def>\""
// Warning for an invalid host name:
// "[Config] <field>: \"<value>\" is not a host name ..., using default \"<def>\""
std::string notHost(const std::string& field, const std::string& value, const std::string& def) {
    return "[Config] " + field + ": \"" + value +
           "\" is not a host name (no scheme, path or spaces, e.g. \"ws.okx.com\"), using default \"" + def + "\"";
}

TEST_CASE("config: the shipped config.json loads without warnings and equals the defaults except the global fee_rate") {
    const std::string path = repoPath("config.json");
    const AppConfig c = ConfigReader::load(path, false);
    checkConfig("shipped config.json", c, {}, shipped());
    CHECK_EQ(c.source, path);
    REQUIRE_EQ(c.venues.size(), 3u);
    CHECK(c.venues[0].exchange == Exchange::BINANCE);
    CHECK(c.venues[1].exchange == Exchange::OKX);
    CHECK(c.venues[2].exchange == Exchange::BYBIT);
    CHECK_EQ(c.enabledVenues().size(), 3u);
    // 默认值即随仓库发布的 config.json 的取值；全局 fee_rate 的默认值是 0.001
    // The defaults are the values of the shipped config.json; the global fee_rate defaults to 0.001
    const AppConfig d = ConfigReader::getDefaults();
    CHECK_EQ(d.arbitrage.fee_rate, 0.001);
    CHECK_EQ(d.venues[0].path, std::string("/ws/btcusdt@depth@100ms"));
    CHECK_EQ(d.venues[0].snapshot_path, std::string("/api/v3/depth?symbol=BTCUSDT&limit=1000"));
    CHECK_EQ(d.venues[1].subscribe_msg, std::string(kOkxBooksSub));
    CHECK_EQ(d.venues[2].port, std::string("443"));
    CHECK_EQ(d.arbitrage.max_levels, 20);
    CHECK_EQ(d.connection.stall_timeout_ms, 15000);
    // 测试的配置文本生成器原样写回仓库文件的内容
    // The test's config text generator writes back the repo file's content unchanged
    checkConfig("ConfigText()", loadText(ConfigText()), {}, shipped());
}

TEST_CASE("config: unreadable file, invalid JSON or a non-object document -> full defaults and one warning") {
    const std::string defaults = describe(ConfigReader::getDefaults());
    const AppConfig missing = ConfigReader::load("/nonexistent-dir/config.json", false);
    CHECK(missing.source.empty());
    REQUIRE_EQ(missing.warnings.size(), 1u);
    CHECK(contains(missing.warnings[0], "[Config] Cannot open: /nonexistent-dir/config.json ("));
    CHECK(contains(missing.warnings[0], "), using defaults"));
    CHECK_EQ(describe(missing), defaults);
    for (const char* bad : {"{\"binance\": ", "", "not json", "{\"a\": 1} trailing", "{oops}"}) {
        const AppConfig c = ConfigReader::parse(bad, "<src>");
        CHECK(c.source.empty());
        REQUIRE_EQ(c.warnings.size(), 1u);
        CHECK_EQ(c.warnings[0].rfind("[Config] <src>: invalid JSON (", 0), 0u);
        CHECK(contains(c.warnings[0], "), using defaults"));
        CHECK_EQ(describe(c), defaults);
    }
    for (const char* bad : {"[1, 2, 3]", "[1]", "42"}) {
        const AppConfig c = loadText(std::string(bad));
        CHECK(c.source.empty());
        REQUIRE_EQ(c.warnings.size(), 1u);
        CHECK(contains(c.warnings[0], ": top-level value is not a JSON object, using defaults"));
        CHECK_EQ(describe(c), defaults);
    }
}

TEST_CASE("config: mistyped, missing and out-of-range fields fall back or clamp with one warning each, in reading order") {
    std::vector<ConfigCase> cases = {
        {"mistyped fields fall back to their own defaults; other fields are kept",
         [](ConfigText& t) {
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
         },
         {"[Config] okx.port: expected string or integer, using default \"8443\"",
          "[Config] arbitrage.min_profit: expected a number, using default 0.01",
          // 文件不存在：值保留，只告警
          // File does not exist: the value is kept, only a warning
          "[Config] connection.ca_file: cannot read \"/tmp/mock_ca.pem\", every TLS connection will fail"},
         [](AppConfig& c) {
             c.venues[0].path = "/ws/btcusdt@depth20@100ms";
             c.venues[0].rest_host.clear();
             c.venues[0].snapshot_path.clear();
             c.venues[1].host = "okx.example.com";
             c.venues[2].enabled = false;
             c.arbitrage = {0.0005, 0.01, 0.5, 10};
             c.display = {100, 3, 10};
             c.connection = {false, "/tmp/mock_ca.pem", 5000, 100, 1000};
         },
         [](const AppConfig& c) { CHECK_EQ(c.enabledVenues().size(), 2u); }},
        {"out-of-range values are clamped and name the clamped value",
         [](ConfigText& t) {
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
         },
         {"[Config] okx.fee_rate: 0.5 must be below 0.1 (fee rates are fractions: 0.001 = 0.1%), using the global "
          "fee_rate",
          "[Config] arbitrage.fee_rate: -0.001 is negative, clamped to 0",
          "[Config] arbitrage.min_profit: -1 is out of range [0, inf], clamped to 0",
          "[Config] arbitrage.max_qty: -2.5 is out of range [0, inf], clamped to 0",
          "[Config] arbitrage.max_levels: 0 is out of range [1, 20], clamped to 1",
          "[Config] display.refresh_ms: 5 is out of range [10, 10000], clamped to 10",
          "[Config] display.levels: 50 is out of range [1, 20], clamped to 20",
          // 边界用最短往返表示（std::to_chars 选更短的一种：1000000 → "1e+06"）
          // Bounds are printed in their shortest round-trip form
          // (std::to_chars picks the shorter one: 1000000 → "1e+06")
          "[Config] display.spread_log_interval: 0 is out of range [1, 1e+06], clamped to 1",
          "[Config] connection.stall_timeout_ms: 10 is out of range [1000, 3600000], clamped to 1000",
          "[Config] connection.reconnect_max_ms: 100 is out of range [2000, 6e+05], clamped to 2000"},
         [](AppConfig& c) {
             c.arbitrage = {0.0, 0.0, 0.0, 1};
             c.display = {10, 20, 1};
             c.connection.stall_timeout_ms = 1000;
             c.connection.reconnect_initial_ms = 2000;
             c.connection.reconnect_max_ms = 2000;
         },
         nullptr},
        {"reconnect_max_ms missing while its default is below the given reconnect_initial_ms",
         [](ConfigText& t) { t.set("connection", "reconnect_initial_ms", "60000").erase("connection", "reconnect_max_ms"); },
         {"[Config] connection.reconnect_max_ms: missing, using default 30000",
          "[Config] connection.reconnect_max_ms: 30000 is below connection.reconnect_initial_ms, using 60000"},
         [](AppConfig& c) { c.connection.reconnect_initial_ms = c.connection.reconnect_max_ms = 60000; }, nullptr},
        {"huge integers (int64 / uint64 range) are clamped without overflow",
         [](ConfigText& t) {
             t.set("display", "spread_log_interval", "99999999999").set("display", "refresh_ms", "18446744073709551615");
         },
         {"[Config] display.refresh_ms: 18446744073709551616 is out of range [10, 10000], clamped to 10000",
          "[Config] display.spread_log_interval: 99999999999 is out of range [1, 1e+06], clamped to 1e+06"},
         [](AppConfig& c) { c.display = {10000, 5, ConfigReader::kMaxSpreadLogInterval}; }, nullptr},
        {"a global fee_rate >= 0.1 uses the default 0.001",
         [](ConfigText& t) { t.set("arbitrage", "fee_rate", "0.1"); },
         {"[Config] arbitrage.fee_rate: 0.1 must be below 0.1 (fee rates are fractions: 0.001 = 0.1%), using default "
          "0.001"},
         [](AppConfig& c) { c.arbitrage.fee_rate = 0.001; }, nullptr},
        {"integer fields reject fractions and strings but accept integral doubles",
         [](ConfigText& t) {
             t.set("display", "levels", "2.5").set("display", "refresh_ms", "200.0").set("display", "spread_log_interval", "\"7\"");
         },
         {"[Config] display.levels: expected an integer, using default 5",
          "[Config] display.spread_log_interval: expected an integer, using default 50"},
         nullptr, nullptr},
        {"an integer written as 1e3", [](ConfigText& t) { t.set("display", "refresh_ms", "1e3"); }, {},
         [](AppConfig& c) { c.display.refresh_ms = 1000; }, nullptr},
        {"missing fields and blocks warn once each; wrong block types too",
         [](ConfigText& t) {
             t.erase("okx", "host")
                 .erase("arbitrage", "fee_rate")
                 .eraseBlock("display")
                 .setBlock("connection", "[]")
                 .set("bybit", "enabled", "\"yes\"");
         },
         {"[Config] okx.host: missing, using default \"ws.okx.com\"",
          "[Config] bybit.enabled: expected true or false, using default true",
          "[Config] arbitrage.fee_rate: missing, using default 0.001", "[Config] display: missing, using defaults",
          "[Config] connection: expected an object, using defaults"},
         [](AppConfig& c) { c.arbitrage.fee_rate = 0.001; }, nullptr},
        {"blocks missing from an older config file",
         [](ConfigText& t) { t.eraseBlock("bybit").eraseBlock("connection"); },
         {"[Config] bybit: missing, using defaults", "[Config] connection: missing, using defaults"}, nullptr, nullptr},
        {"a venue block that is not an object", [](ConfigText& t) { t.setBlock("okx", "null"); },
         {"[Config] okx: expected an object, using defaults"}, nullptr, nullptr},
        {"unknown keys are ignored silently",
         [](ConfigText& t) {
             t.set("binance", "extra", "123")
                 .set("arbitrage", "comment", "\"hello\"")
                 .setBlock("future_feature", "{\"x\": [1, 2, {\"y\": null}]}")
                 .set("connection", "connect_timeout_ms", "5");
         },
         {}, nullptr, nullptr},
    };
    runConfigCases(cases);
}

TEST_CASE("config: venues - enabled, per-venue fee_rate, ports, hosts, paths, subscribe and consistency checks") {
    std::vector<ConfigCase> cases = {
        {"a disabled venue's other fields are not validated",
         [](ConfigText& t) {
             t.setBlock("bybit", "{\"enabled\": false}").setBlock("okx", "{\"enabled\": false, \"port\": true, \"host\": 7}");
         },
         {}, [](AppConfig& c) { c.venues[1].enabled = c.venues[2].enabled = false; },
         [](const AppConfig& c) {
             const std::vector<const VenueConfig*> en = c.enabledVenues();
             REQUIRE_EQ(en.size(), 1u);
             CHECK(en[0]->exchange == Exchange::BINANCE);
             CHECK(contains(ConfigReader::summary(c), "Venues: Binance, OKX (disabled), Bybit (disabled)  Fee: "));
         }},
        {"every venue disabled",
         [](ConfigText& t) {
             t.set("binance", "enabled", "false").set("okx", "enabled", "false").set("bybit", "enabled", "false");
         },
         {}, [](AppConfig& c) { c.venues[0].enabled = c.venues[1].enabled = c.venues[2].enabled = false; },
         [](const AppConfig& c) { CHECK(c.enabledVenues().empty()); }},
        {"per-venue fee_rate overrides the global one",
         [](ConfigText& t) {
             t.set("arbitrage", "fee_rate", "0.001").set("okx", "fee_rate", "0.0008").set("bybit", "fee_rate", "0");
         },
         {},
         [](AppConfig& c) {
             c.arbitrage.fee_rate = 0.001;
             c.venues[1].fee_rate = 0.0008;
             c.venues[2].fee_rate = 0.0;
         },
         [](const AppConfig& c) {
             CHECK_EQ(c.feeRate(c.venues[0]), 0.001);
             CHECK_EQ(c.feeRate(c.venues[1]), 0.0008);
             CHECK_EQ(c.feeRate(c.venues[2]), 0.0);
             CHECK(contains(ConfigReader::summary(c), "Fee: 0.100% (OKX 0.080%, Bybit 0.000%)"));
         }},
        {"invalid per-venue fee_rate overrides fall back to the global one or clamp",
         [](ConfigText& t) { t.set("okx", "fee_rate", "\"x\"").set("bybit", "fee_rate", "-0.5"); },
         {"[Config] okx.fee_rate: expected a number, using the global fee_rate",
          "[Config] bybit.fee_rate: -0.5 is negative, clamped to 0"},
         [](AppConfig& c) { c.venues[2].fee_rate = 0.0; },
         [](const AppConfig& c) { CHECK_EQ(c.feeRate(c.venues[2]), 0.0); }},
        {"port and rest_port accept a JSON string or integer in 1..65535",
         [](ConfigText& t) {
             t.set("binance", "port", "9443").set("binance", "rest_port", "8443").set("okx", "port", "\"0\"").set("bybit", "port", "70000");
         },
         {"[Config] okx.port: \"0\" is not a valid port (1-65535), using default \"8443\"",
          "[Config] bybit.port: \"70000\" is not a valid port (1-65535), using default \"443\""},
         [](AppConfig& c) { c.venues[0].rest_port = "8443"; }, nullptr},
        {"the port range edges", [](ConfigText& t) { t.set("okx", "port", "1").set("bybit", "port", "\"65535\""); }, {},
         [](AppConfig& c) {
             c.venues[1].port = "1";
             c.venues[2].port = "65535";
         },
         nullptr},
        {"host, path, subscribe and snapshot_path are validated",
         [](ConfigText& t) {
             t.set("okx", "host", "\"wss://ws.okx.com\"")
                 .set("bybit", "path", "\"v5/public/spot\"")
                 .set("okx", "subscribe", "\"{not json\"")
                 .set("binance", "host", "\"\"")
                 .set("binance", "rest_host", "\"api binance com\"")
                 .set("binance", "snapshot_path", "\"api/v3/depth\"");
         },
         {"[Config] binance.host: must not be empty, using default \"stream.binance.com\"",
          notHost("binance.rest_host", "api binance com", "api.binance.com"),
          "[Config] binance.snapshot_path: \"api/v3/depth\" must start with '/', using \"\"",
          notHost("okx.host", "wss://ws.okx.com", "ws.okx.com"),
          "[Config] okx.subscribe: is not valid JSON, using default \"" + std::string(kOkxBooksSub) + "\"",
          "[Config] bybit.path: \"v5/public/spot\" must start with '/', using default \"/v5/public/spot\"",
          // snapshot_path 被清空后，增量深度流缺少 REST 快照配置：一致性检查再告警一次
          // With snapshot_path cleared, the diff-depth stream has no REST snapshot settings:
          // the consistency check warns once more
          "[Config] binance.snapshot_path: required by the diff-depth stream in binance.path "
          "\"/ws/btcusdt@depth@100ms\" (REST snapshot); the feed cannot sync without it"},
         [](AppConfig& c) { c.venues[0].snapshot_path.clear(); }, nullptr},
        {"a symbol label that the subscription does not stream", [](ConfigText& t) { t.set("okx", "symbol", "\"ETHUSDT\""); },
         {"[Config] okx.symbol: \"ETHUSDT\" does not appear in okx.subscribe (symbol is only a label; the streamed "
          "instrument comes from okx.subscribe)",
          "[Config] symbol: enabled venues use different symbols (Binance BTCUSDT, OKX ETHUSDT, Bybit BTCUSDT): "
          "arbitrage signals would compare different instruments"},
         [](AppConfig& c) { c.venues[1].symbol = "ETHUSDT"; }, nullptr},
        {"a disabled venue is not part of the consistency checks",
         [](ConfigText& t) { t.set("okx", "symbol", "\"ETHUSDT\"").set("okx", "enabled", "false"); }, {},
         [](AppConfig& c) { c.venues[1].enabled = false; }, nullptr},
        {"the Binance diff-depth stream without rest_host", [](ConfigText& t) { t.set("binance", "rest_host", "\"\""); },
         {"[Config] binance.rest_host: required by the diff-depth stream in binance.path \"/ws/btcusdt@depth@100ms\" "
          "(REST snapshot); the feed cannot sync without it"},
         [](AppConfig& c) { c.venues[0].rest_host.clear(); }, nullptr},
        {"a partial-depth Binance stream needs no REST settings",
         [](ConfigText& t) {
             t.set("binance", "path", "\"/ws/btcusdt@depth20@100ms\"").set("binance", "rest_host", "\"\"").set("binance", "snapshot_path", "\"\"");
         },
         {},
         [](AppConfig& c) {
             c.venues[0].path = "/ws/btcusdt@depth20@100ms";
             c.venues[0].rest_host.clear();
             c.venues[0].snapshot_path.clear();
         },
         nullptr},
    };
    TempPath ca("test_app_ca", ".pem");
    std::ofstream(ca.path) << "-----BEGIN CERTIFICATE-----\n";
    cases.push_back({"a readable connection.ca_file is accepted without warnings",
                     [&](ConfigText& t) { t.set("connection", "ca_file", jsonString(ca.str())); }, {},
                     [&](AppConfig& c) { c.connection.ca_file = ca.str(); }, nullptr});
    // bybit.port 的各种非法写法：每种一条告警，值保持默认
    // Various invalid ways to write bybit.port: one warning each, the value stays at its default
    const std::pair<const char*, const char*> bad_ports[] = {
        {"-1", "\"-1\" is not a valid port (1-65535)"},   {"\"abc\"", "\"abc\" is not a valid port (1-65535)"},
        {"\" 443\"", "\" 443\" is not a valid port (1-65535)"}, {"\"443x\"", "\"443x\" is not a valid port (1-65535)"},
        {"1.5", "expected string or integer"},             {"null", "expected string or integer"},
        {"[443]", "expected string or integer"}};
    for (const auto& port : bad_ports) {
        const std::string raw = port.first;
        cases.push_back({"bybit.port = " + raw, [raw](ConfigText& t) { t.set("bybit", "port", raw); },
                         {std::string("[Config] bybit.port: ") + port.second + ", using default \"443\""}, nullptr,
                         nullptr});
    }
    runConfigCases(cases);

    CHECK(config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth"));
    CHECK(config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth@100ms"));
    CHECK(!config_detail::isBinanceDiffDepthPath("/ws/btcusdt@depth20@100ms"));
    CHECK(!config_detail::isBinanceDiffDepthPath("/ws/btcusdt@bookTicker"));
    CHECK(config_detail::isBinanceDiffDepthPath("/stream?streams=btcusdt@depth5/ethusdt@depth@100ms"));
}

TEST_CASE("config: load() prints the Loaded line and summary on stdout and the warnings on stderr") {
    TempPath file("test_app_config", ".json");
    std::ofstream(file.path) << ConfigText().set("okx", "port", "true").str();
    auto capture = [](const std::string& path, std::string& out, std::string& err) {
        std::ostringstream o, e;
        std::streambuf* old_out = std::cout.rdbuf(o.rdbuf());
        std::streambuf* old_err = std::cerr.rdbuf(e.rdbuf());
        const AppConfig c = ConfigReader::load(path);
        std::cout.rdbuf(old_out);
        std::cerr.rdbuf(old_err);
        out = o.str();
        err = e.str();
        return c;
    };
    std::string out, err;
    const AppConfig c = capture(file.str(), out, err);
    CHECK_EQ(out, "[Config] Loaded: " + file.str() +
                      "\n[Config] Venues: Binance, OKX, Bybit  Fee: 0.000%  Min profit: 0.01  Refresh: 200ms\n");
    CHECK_EQ(err, std::string("[Config] okx.port: expected string or integer, using default \"8443\"\n"));
    CHECK_EQ(c.warnings.size(), 1u);
    capture("/nonexistent-dir/x.json", out, err);
    CHECK(!contains(out, "Loaded"));
    CHECK(contains(err, "[Config] Cannot open: /nonexistent-dir/x.json"));
}

// ============================================================================
// 套利：扫单数学
// Arbitrage: sweep math
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

// ArbitrageDetector::detectVenue 的 book_of：交易所 i 的簿是 books[i]
// book_of for ArbitrageDetector::detectVenue: venue i's book is books[i]
auto bookAt(const std::vector<OrderBook>& books) {
    return [&books](std::size_t i) -> const OrderBook& { return books[i]; };
}

// 评估所有有序交易所对（按买方、再按卖方的顺序）；返回新信号数
// Evaluates every ordered venue pair (by buy venue, then by sell venue); returns the number of new signals
std::size_t detectAll(ArbitrageDetector& det, const std::vector<OrderBook>& books) {
    std::size_t found = 0;
    for (std::size_t buy = 0; buy < books.size(); ++buy) {
        for (std::size_t sell = 0; sell < books.size(); ++sell) {
            if (buy != sell && det.detectPair(buy, sell, books[buy], books[sell]) != nullptr) ++found;
        }
    }
    return found;
}

TEST_CASE("sweep: walks several levels on both sides while the marginal unit is profitable (no fees)") {
    const auto asks = L({{100.0, 1.0}, {101.0, 2.0}, {102.0, 5.0}});
    const auto bids = L({{103.0, 1.5}, {102.5, 1.0}, {101.5, 3.0}});
    const SweepResult r = sweepOf(asks, bids, 0.0, 0.0);
    // 100x1 @103, 101x0.5 @103, 101x1 @102.5, 101x0.5 @101.5；然后 102 > 101.5 停止
    // 100x1 @103, 101x0.5 @103, 101x1 @102.5, 101x0.5 @101.5; then 102 > 101.5 stops
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
    // f = 0.5%: step 4, 101 -> 101.5, is negative after fees
    const SweepResult r = sweepOf(asks, bids, 0.005, 0.005);
    CHECK_EQ(r.qty, 2.5);
    CHECK_NEAR(r.gross, 5.5, 1e-12);
    CHECK_NEAR(r.fees, (203.0 * 1.0 + 204.0 * 0.5 + 203.5 * 1.0) * 0.005, 1e-12);
    CHECK_NEAR(r.net, 5.5 - 2.5425, 1e-12);
    CHECK_NEAR(r.revenue - r.cost - r.fees, r.net, 1e-9);
    CHECK_EQ(r.buy_levels, 2);
    CHECK_EQ(r.sell_levels, 2);

    // 买方 0.1%、卖方 0.4%：手续费按方向计算
    // Buy venue 0.1%, sell venue 0.4%: fees are computed per direction
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
    // Fees so high that even the first level is unprofitable: no trade at all
    const SweepResult none = sweepOf(asks, bids, 0.02, 0.02);
    CHECK_EQ(none.qty, 0.0);
    CHECK_EQ(none.net, 0.0);
    CHECK_EQ(none.levelsUsed(), 0);
    // 边际判断：bid*(1-fs) - ask*(1+fb) > 0 恰好不成立时不交易（103*0.99 - 100*1.0197 = 1.97 - 1.97 < 0）
    // Marginal check: no trade when bid*(1-fs) - ask*(1+fb) > 0 only just fails to hold
    // (103*0.99 - 100*1.0197 = 1.97 - 1.97 < 0)
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
    // max_qty <= 0 means unlimited
    CHECK_EQ(sweepOf(asks, bids, 0.0, 0.0, -1.0).qty, 3.0);

    const SweepResult l1 = sweepOf(asks, bids, 0.0, 0.0, 0.0, 1);
    CHECK_EQ(l1.qty, 1.0);
    CHECK_EQ(l1.gross, 3.0);
    CHECK_EQ(l1.levelsUsed(), 1);
    const SweepResult l2 = sweepOf(asks, bids, 0.0, 0.0, 0.0, 2);
    CHECK_EQ(l2.qty, 2.5);  // 100x1@103, 101x0.5@103, 101x1@102.5，然后买盘只剩 2 档
                            // 100x1@103, 101x0.5@103, 101x1@102.5, then the bid side only has 2 levels
    CHECK_EQ(l2.sell_levels, 2);
    CHECK_EQ(sweepOf(asks, bids, 0.0, 0.0, 0.0, 0).qty, 3.0);  // 0 = 不限
                                                               // 0 = unlimited
}

TEST_CASE("sweep: unprofitable, crossed-equal, empty and malformed books") {
    CHECK_EQ(sweepOf(L({{100.0, 1.0}}), L({{99.99, 1.0}}), 0.0, 0.0).qty, 0.0);
    CHECK_EQ(sweepOf(L({{100.0, 1.0}}), L({{100.0, 1.0}}), 0.0, 0.0).qty, 0.0);  // 平价不赚钱
                                                                                 // no profit at equal prices
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
    // Levels with a non-positive quantity are skipped; an invalid price ends the sweep
    const SweepResult skip = sweepOf(L({{100.0, 0.0}, {100.5, -1.0}, {101.0, 1.0}}), L({{102.0, 2.0}}), 0.0, 0.0);
    CHECK_EQ(skip.qty, 1.0);
    CHECK_EQ(skip.cost, 101.0);
    CHECK_EQ(skip.buy_levels, 3);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    CHECK_EQ(sweepOf(L({{nan, 1.0}}), L({{102.0, 2.0}}), 0.0, 0.0).qty, 0.0);
    CHECK_EQ(sweepOf(L({{100.0, 1.0}, {-1.0, 1.0}}), L({{102.0, 5.0}}), 0.0, 0.0).qty, 1.0);
}

// v1 的检测逻辑原样复刻（含去重），用来证明新实现在"每侧一档 + 相同手续费"时逐位等价
// An exact replica of the v1 detection logic (dedup included), used to prove that the new implementation is
// bit-for-bit equivalent under "one level per side + equal fees"
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
        ArbitrageDetector det({fee, fee}, 0.01, 0.0, 1);  // v1：两个交易所、相同手续费、只看最优档
                                                          // v1: two venues, equal fees, best level only
        CHECK_EQ(det.maxLevels(), 1);
        // 价格以 5 美分为步长随机游走（变化要么为 0，要么 >= 0.05，远离 0.01 的去重阈值）；
        // B 相对 A 的偏移偶尔跳到 ±250 美元以内的随机值，任何手续费水平下都会出现可盈利与不可盈利的时段
        // Prices random-walk in steps of 5 cents (a change is either 0 or >= 0.05, far from the 0.01 dedup
        // threshold); B's offset from A occasionally jumps to a random value within ±250 dollars, so profitable
        // and unprofitable periods occur at every fee level
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
            // The second level affects neither v1 nor the new implementation with max_levels = 1
            const OrderBook a = makeBook("A:BTCUSDT", {{a_bid, lots(rng) / 100.0}, {a_bid - 1, 9.0}},
                                         {{a_ask, lots(rng) / 100.0}, {a_ask + 1, 9.0}});
            const OrderBook b = makeBook("B:BTCUSDT", {{b_bid, lots(rng) / 100.0}, {b_bid - 1, 9.0}},
                                         {{b_ask, lots(rng) / 100.0}, {b_ask + 1, 9.0}});
            const bool old_found = v1.detect(a, b);
            const bool d1 = det.detectPair(0, 1, a, b) != nullptr;  // v1 的 detect()：两个方向都评估
                                                                    // v1's detect(): evaluates both directions
            const bool d2 = det.detectPair(1, 0, b, a) != nullptr;
            const bool new_found = d1 || d2;
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
                                                 // bit-for-bit equal
            CHECK_EQ(s.net_profit, o.net);
            CHECK_EQ(s.levels_used, 1);
            CHECK_NEAR(s.vwap_buy, o.buy_price, 1e-9);   // cost / qty：与价格只差舍入
                                                         // cost / qty: differs from the price only by rounding
            CHECK_NEAR(s.vwap_sell, o.sell_price, 1e-9);
            if (s.net_profit == o.net && s.gross_profit == o.gross) ++matched;
        }
        CHECK_EQ(matched, sigs.size());
        CHECK_GT(v1.signals.size(), 100u);  // 场景确实产生了大量信号
                                            // the scenario really produced many signals
        double total = 0.0;
        for (const V1Detector::Signal& o : v1.signals) total += o.net;
        CHECK_NEAR(det.getTotalNetProfit(), total, 1e-6);
    }
    // 直接对公式：gross = (bid - ask) * min(q)，fee = (ask + bid) * qty * f
    // Directly against the formulas: gross = (bid - ask) * min(q), fee = (ask + bid) * qty * f
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
// Arbitrage: detector (dedup, N venues, bounded signal queue)
// ============================================================================

TEST_CASE("detector: dedup per ordered pair - a one-cent move counts, sub-cent moves and repeats do not") {
    ArbitrageDetector det(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> books = {makeBook("A:X", {{66990.00, 1.0}}, {{67000.00, 1.0}}),
                                    makeBook("B:X", {{67010.00, 1.0}}, {{67020.00, 1.0}})};
    CHECK_EQ(detectAll(det, books), 1u);
    CHECK_EQ(detectAll(det, books), 0u);
    CHECK_EQ(det.detectVenue(0, bookAt(books)), 0u);
    // 67000.01 - 67000.00 在二进制浮点里是 0.0099999999947...，仍然算 1 分的变动
    // 67000.01 - 67000.00 is 0.0099999999947... in binary floating point and still counts as a one-cent move
    books[0] = makeBook("A:X", {{66990.00, 1.0}}, {{67000.01, 1.0}});
    CHECK_EQ(det.detectVenue(0, bookAt(books)), 1u);
    books[0] = makeBook("A:X", {{66990.00, 1.0}}, {{67000.015, 1.0}});
    CHECK_EQ(det.detectVenue(0, bookAt(books)), 0u);  // 半分：不算变动
                                                      // half a cent: not a move
    books[1] = makeBook("B:X", {{67010.01, 1.0}}, {{67020.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, bookAt(books)), 1u);  // 买一变动 1 分
                                                      // best bid moves by 1 cent
    // 数量变化不算价格变动
    // A quantity change is not a price move
    books[1] = makeBook("B:X", {{67010.01, 5.0}}, {{67020.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, bookAt(books)), 0u);
    CHECK_EQ(det.pairState(0, 1).signals, 3u);
    // 另一个方向独立去重：B 便宜下来后 B->A 第一次出现机会立即出信号
    // The other direction is deduplicated independently: after B gets cheaper, the first B->A opportunity
    // signals immediately
    books[1] = makeBook("B:X", {{66900.00, 1.0}}, {{66950.00, 1.0}});
    CHECK_EQ(det.detectVenue(1, bookAt(books)), 1u);
    CHECK_EQ(det.pairState(1, 0).signals, 1u);
    CHECK_EQ(det.getTotalSignals(), 4u);
    CHECK(ArbitrageDetector::moved(67000.01, 67000.00));
    CHECK(!ArbitrageDetector::moved(67000.005, 67000.00));
    CHECK(ArbitrageDetector::moved(0.02, 0.01));
    CHECK(!ArbitrageDetector::moved(5.0, 5.0));

    // 无数据（断线后的空簿）不出信号；恢复后价格未变 -> 仍被去重
    // No data (the empty book after a disconnect): no signal; prices unchanged after recovery
    // -> still deduplicated
    ArbitrageDetector d2(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> b2 = {makeBook("A:X", {{99.0, 1.0}}, {{100.0, 1.0}}), makeBook("B:X", {{101.0, 1.0}}, {{102.0, 1.0}})};
    CHECK_EQ(detectAll(d2, b2), 1u);
    const OrderBook saved = b2[1];
    b2[1] = OrderBook("B:X");
    CHECK_EQ(d2.detectVenue(1, bookAt(b2)), 0u);
    CHECK(!d2.pairState(0, 1).has_data);
    CHECK_EQ(d2.pairState(0, 1).sweep.qty, 0.0);
    b2[1] = saved;
    CHECK_EQ(d2.detectVenue(1, bookAt(b2)), 0u);
    CHECK(d2.pairState(0, 1).has_data);
    CHECK_GT(d2.pairState(0, 1).sweep.net, 0.0);
}

TEST_CASE("detector: min_profit is a strict threshold on the net profit after fees") {
    std::vector<OrderBook> books = {makeBook("A", {{99.0, 1.0}}, {{100.0, 1.0}}), makeBook("B", {{100.5, 1.0}}, {{101.0, 1.0}})};
    ArbitrageDetector at(std::vector<double>{0.0, 0.0}, 0.5, 0.0, 20);
    CHECK_EQ(detectAll(at, books), 0u);  // net == 0.5，不大于阈值
                                         // net == 0.5, not above the threshold
    CHECK_EQ(at.pairState(0, 1).sweep.net, 0.5);
    ArbitrageDetector below(std::vector<double>{0.0, 0.0}, 0.4999, 0.0, 20);
    CHECK_EQ(detectAll(below, books), 1u);
    // 手续费把净利润压到阈值以下
    // Fees push the net profit below the threshold
    ArbitrageDetector fee(std::vector<double>{0.001, 0.0015}, 0.3, 0.0, 20);
    CHECK_EQ(detectAll(fee, books), 0u);
    CHECK_NEAR(fee.pairState(0, 1).sweep.net, 0.5 - (100.0 * 0.001 + 100.5 * 0.0015), 1e-12);
    ArbitrageDetector fee2(std::vector<double>{0.001, 0.0015}, 0.2, 0.0, 20);
    CHECK_EQ(detectAll(fee2, books), 1u);
    CHECK_NEAR(fee2.getSignals().back().fees, 100.0 * 0.001 + 100.5 * 0.0015, 1e-12);
}

TEST_CASE("detector: N venues - detectVenue touches exactly the 2(N-1) pairs of that venue, detectAll all N(N-1)") {
    const std::size_t n = 4;
    ArbitrageDetector det(std::vector<double>(n, 0.0), 0.01, 0.0, 20);
    std::vector<OrderBook> books;
    for (std::size_t i = 0; i < n; ++i) books.push_back(makeBook("V" + std::to_string(i), {{99.0, 1.0}}, {{101.0, 1.0}}));
    CHECK_EQ(det.detectVenue(2, bookAt(books)), 0u);
    for (std::size_t b = 0; b < n; ++b) {
        for (std::size_t s = 0; s < n; ++s) {
            if (b == s) continue;
            const std::uint64_t expected = (b == 2 || s == 2) ? 1u : 0u;
            CHECK_EQ(det.pairState(b, s).evaluations, expected);
        }
    }
    detectAll(det, books);
    for (std::size_t b = 0; b < n; ++b) {
        for (std::size_t s = 0; s < n; ++s) {
            if (b == s) {
                CHECK_EQ(det.pairState(b, s).evaluations, 0u);  // 对角线从不评估
                                                                // the diagonal is never evaluated
                continue;
            }
            const std::uint64_t expected = (b == 2 || s == 2) ? 2u : 1u;
            CHECK_EQ(det.pairState(b, s).evaluations, expected);
        }
    }
    // 越界与 buy == sell 什么也不做
    // Out of range and buy == sell do nothing
    CHECK(det.detectPair(1, 1, books[1], books[1]) == nullptr);
    CHECK(det.detectPair(7, 1, books[1], books[1]) == nullptr);
    CHECK_EQ(det.detectVenue(9, bookAt(books)), 0u);
    CHECK_THROWS(det.pairState(4, 0));
    CHECK_THROWS(ArbitrageDetector(std::vector<double>(ArbitrageDetector::kMaxVenues + 1, 0.0), 0.01));

    // 三个交易所：便宜的 A、中间的 B、贵的 C -> 恰好 A->B、A->C、B->C 三个有序对出信号，标签与编号正确
    // Three venues: cheap A, middle B, expensive C -> exactly the three ordered pairs A->B, A->C, B->C signal,
    // with correct labels and indices
    ArbitrageDetector three(std::vector<double>{0.0, 0.0, 0.0}, 0.01, 0.0, 20);
    std::vector<OrderBook> tb = {makeBook("A:BTC", {{99.0, 1.0}}, {{100.0, 1.0}}),
                                 makeBook("B:BTC", {{101.0, 2.0}}, {{101.5, 2.0}}),
                                 makeBook("C:BTC", {{102.0, 0.5}}, {{103.0, 1.0}})};
    CHECK_EQ(detectAll(three, tb), 3u);
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
        if (det.detectVenue(0, bookAt(books)) == 1) expected_net += (101.0 - ask) * 1.0;
    }
    std::cout.rdbuf(old);
    CHECK(captured.str().empty());
    CHECK_EQ(det.getTotalSignals(), 1500u);
    CHECK_EQ(det.getSignals().size(), ArbitrageDetector::kMaxSignals);
    CHECK_NEAR(det.getTotalNetProfit(), expected_net, 1e-9);
    // 最旧的 500 条被丢弃：现在第一条是第 501 条（下标 500，偶数 -> ask 100）
    // The oldest 500 are dropped: the first one is now the 501st (index 500, even -> ask 100)
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
    detectAll(det, books);
    const SweepResult& r = det.pairState(0, 1).sweep;
    const SweepResult expect = sweepOf(books[0].getAsks(), books[1].getBids(), 0.001, 0.002, 1.5, 2);
    CHECK_EQ(r.qty, 1.5);
    CHECK_EQ(r.qty, expect.qty);
    CHECK_EQ(r.net, expect.net);
    CHECK_EQ(r.fees, expect.fees);
    CHECK_EQ(det.getSignals().back().levels_used, 2);
}

// ============================================================================
// 文本宽度工具
// Text width utilities
// ============================================================================

// 独立的严格 UTF-8 校验（RFC 3629），用于检查 sanitize 的输出
// Independent strict UTF-8 validation (RFC 3629), used to check the output of sanitize
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
                                                  // U+00B5 micro sign (2 bytes)
    CHECK_EQ(visibleWidth("12.3 μs"), 7u);        // U+03BC 希腊字母 mu（2 字节）
                                                  // U+03BC Greek letter mu (2 bytes)
    CHECK_EQ(visibleWidth("╔═══╗"), 5u);          // 框线字符（每个 3 字节）
                                                  // box-drawing characters (3 bytes each)
    CHECK_EQ(visibleWidth("…€"), 2u);
    CHECK_EQ(visibleWidth("\xF0\x9F\x98\x80"), 1u);  // 4 字节码点（按文档约定计 1 列）
                                                     // 4-byte code point (counts as 1 column, as documented)
    CHECK_EQ(visibleWidth("\033[32mLIVE\033[0m"), 4u);
    CHECK_EQ(visibleWidth("\033[1;38;5;208mX\033[0m"), 1u);
    CHECK_EQ(visibleWidth("\033[?25l\033[2J\033[H"), 0u);  // 私有模式 CSI
                                                           // private-mode CSI
    CHECK_EQ(visibleWidth("\033]0;window title\007ok"), 2u);   // OSC ... 以 BEL 结束
                                                               // OSC ... terminated by BEL
    CHECK_EQ(visibleWidth("\033]8;;http://x\033\\link\033]8;;\033\\"), 4u);  // OSC ... 以 ST 结束
                                                                             // OSC ... terminated by ST
    CHECK_EQ(visibleWidth("a\tb\r\n"), 2u);       // C0 控制字符宽度为 0
                                                  // C0 control characters have width 0
    CHECK_EQ(visibleWidth("\x7f"), 0u);
    CHECK_EQ(visibleWidth("\033["), 0u);          // 未结束的 CSI
                                                  // unterminated CSI
    CHECK_EQ(visibleWidth("\033[31"), 0u);
    CHECK_EQ(visibleWidth("x\033"), 1u);
    // 非法 UTF-8：每个坏字节按 1 列计（终端显示为 U+FFFD）
    // Invalid UTF-8: each bad byte counts as 1 column (terminals show U+FFFD)
    CHECK_EQ(visibleWidth("\xff"), 1u);
    CHECK_EQ(visibleWidth("\xc3"), 1u);           // 缺少续字节的 2 字节序列
                                                  // 2-byte sequence missing its continuation byte
    CHECK_EQ(visibleWidth("\xc3("), 2u);
    CHECK_EQ(visibleWidth("\x80\x80"), 2u);       // 孤立的续字节
                                                  // lone continuation bytes
    CHECK_EQ(visibleWidth("\xc0\xaf"), 2u);       // 过长编码的 '/'
                                                  // overlong encoding of '/'
    CHECK_EQ(visibleWidth("\xe0\x80\xaf"), 3u);   // 过长的 3 字节编码
                                                  // overlong 3-byte encoding
    CHECK_EQ(visibleWidth("\xed\xa0\x80"), 3u);   // UTF-16 代理码点
                                                  // UTF-16 surrogate code point
    CHECK_EQ(visibleWidth("\xf4\x90\x80\x80"), 4u);  // 大于 U+10FFFF
                                                     // above U+10FFFF
    CHECK_EQ(visibleWidth("\xe2\x94"), 2u);       // 从中间切开的框线字符
                                                  // box-drawing character cut in the middle
}

TEST_CASE("text_util: padRight / padLeft / center / fit pad to visible columns, colours and multi-byte included") {
    using namespace text_util;
    const std::string green = "\033[32mok\033[0m";
    CHECK_EQ(visibleWidth(padRight(green, 6)), 6u);
    CHECK_EQ(padRight(green, 6), green + "    ");
    CHECK_EQ(padLeft("µs", 4), std::string("  µs"));
    CHECK_EQ(padRight("toolong", 3), std::string("toolong"));  // 从不截断
                                                               // never truncates
    CHECK_EQ(center("ab", 5), std::string(" ab  "));            // 多出的一列放在右侧
                                                                // the extra column goes on the right
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
                                                                  // ellipsis wider than width: no ellipsis
    // 截断处之后的完整转义序列（复位码）保留，颜色不会泄漏
    // Complete escape sequences after the cut (reset codes) are kept, so colours do not leak
    CHECK_EQ(truncateVisible("\033[31mred text\033[0m", 3), std::string("\033[31mred\033[0m"));
    CHECK_EQ(truncateVisible("\033[31mred text\033[0m", 4, "…"), std::string("\033[31mred…\033[0m"));
}

TEST_CASE("text_util: a dangling escape sequence at the end is dropped, so appended padding is never swallowed") {
    using namespace text_util;
    // 不处理时："ab\033[" 后面补的空格会被当作 CSI 的中间字节吞掉，宽度只有 2
    // Without this handling, the spaces padded after "ab\033[" are swallowed as CSI intermediate bytes,
    // and the width is only 2
    CHECK_EQ(visibleWidth(std::string("ab\033[") + "   "), 2u);
    CHECK_EQ(padRight("ab\033[", 5), std::string("ab   "));
    CHECK_EQ(padLeft("ab\033[31", 4), std::string("  ab"));
    CHECK_EQ(fit("x\033", 3), std::string("x  "));
    CHECK_EQ(center("\033", 3), std::string("   "));
    CHECK_EQ(visibleWidth(padRight("a\033]0;title", 4)), 4u);
    CHECK_EQ(truncateVisible("abc\033[31", 10), std::string("abc"));
    CHECK_EQ(truncateVisible("abcdef\033[1", 3, "…"), std::string("ab…"));
    // 被后一个 ESC 打断的 ESC：去掉结尾的悬空 ESC 后它自己也悬空，一起去掉
    // An ESC interrupted by the following ESC: once the dangling ESC at the end is removed, it dangles too,
    // so both are removed
    CHECK_EQ(padRight("a\033\033", 4), std::string("a   "));
    CHECK_EQ(padRight("a\033[\033[3", 4), std::string("a   "));
    CHECK_EQ(padRight("a\033\033[0m", 2), std::string("a\033\033[0m "));  // 结尾是完整序列：不动
                                                                          // ends in a complete sequence: untouched
    // 截断后截断点之前被打断的序列变成悬空
    // After truncation, a sequence interrupted before the cut point becomes dangling
    CHECK_EQ(visibleWidth(truncateVisible("ab\033[\xc3xyz", 2) + "  "), 4u);
    // 完整的序列与中途被打断的序列都保留
    // Both complete sequences and sequences interrupted midway are kept
    CHECK_EQ(padRight("\033[0m", 2), std::string("\033[0m  "));
    CHECK_EQ(padRight("\033[3\xc3\xa9", 2), std::string("\033[3\xc3\xa9 "));
    // 拼接后的框线不会被吞掉
    // Box borders appended afterwards are not swallowed
    const std::string row = "│" + fit("bad\033[", 6) + "│";
    CHECK_EQ(visibleWidth(row), 8u);
    CHECK_EQ(stripAnsi(row), std::string("│bad   │"));
}

TEST_CASE("text_util: sanitize turns control characters and invalid UTF-8 into harmless text") {
    using text_util::sanitize;
    CHECK_EQ(sanitize("a\nb\tc\033[31md"), std::string("a b c [31md"));
    CHECK_EQ(sanitize("\xff"), std::string("?"));
    CHECK_EQ(sanitize("\xc2\x9b" "31m"), std::string(" 31m"));  // C1 CSI（U+009B）
                                                                // C1 CSI (U+009B)
    CHECK_EQ(sanitize("µ═…"), std::string("µ═…"));
    CHECK_EQ(sanitize("\xc0\xaf"), std::string("??"));
    CHECK_EQ(sanitize("\xed\xa0\x80"), std::string("???"));
    CHECK_EQ(sanitize("\x7f"), std::string(" "));
}

TEST_CASE("text_util: randomized invariants for width, truncation, fitting and sanitizing") {
    std::mt19937_64 rng(42);
    // 前 kValid 个片段是合法 UTF-8 / 完整的转义序列 / 控制字符，宽度已知且可相加；其后是非法字节与残缺序列
    // （它们与相邻片段可能组合成别的词法单元，宽度不可相加，只检查结构性质）
    // The first kValid pieces are valid UTF-8 / complete escape sequences / control characters, whose widths
    // are known and additive; after them come invalid bytes and incomplete sequences (they may combine with
    // adjacent pieces into other tokens, so their widths are not additive and only structural properties are
    // checked)
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
        // The visible content of the truncated result is a prefix of the original's visible content
        // (whole code points)
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
        // The results can be concatenated safely: box borders / text appended afterwards are not swallowed
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
// Display: box alignment of frames and the final report
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
// Allows only newlines and SGR colour sequences (ESC [ digits/semicolons m); no tabs, cursor controls or
// other control bytes
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
// Checks every box in the text: from each top-left corner (┌ / ╔), follows the top edge to the top-right
// corner and the left edge down to the bottom-left corner; on every row in between, the left and right
// columns must be vertical edges (or ├ ┤ separators), and the bottom row must hold the two matching bottom
// corners. Returns the number of boxes found; on an error, why is non-empty.
int checkBoxes(const std::string& text, std::string& why) {
    const std::vector<std::string> lines = splitLines(text_util::stripAnsi(text));
    std::vector<std::u32string> grid;
    for (const std::string& line : lines) grid.push_back(decodeUtf8(line));
    auto at = [&](std::size_t r, std::size_t c) -> char32_t { return c < grid[r].size() ? grid[r][c] : U' '; };
    auto rowText = [&](std::size_t r) { return lines[r]; };
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
// Data for the display tests: books / latency stats / detector, and the VenueView values that point to them
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
                                                    // empty book after a disconnect
        books.push_back(makeBook("Bybit:BTCUSDT", {{67010.00, 0.25}, {67009.90, 4.0}, {67009.00, 1.0}},
                                 {{67010.10, 0.3}, {67011.00, 2.0}, {67012.00, 1.5}}));
        books.resize(n);
        std::vector<double> fees = {0.0, 0.001, 0.0001};
        fees.resize(n);
        detector = std::make_unique<ArbitrageDetector>(fees, 0.01, 0.0, 20);
        detectAll(*detector, books);
        if (n >= 3) {
            // 再制造几条信号：Binance 卖一下移
            // Create a few more signals: Binance's best ask moves to the next level
            books[0].updateAsk(67000.20, 0.0);
            detector->detectVenue(0, bookAt(books));
        }
        const FeedStatus statuses[] = {FeedStatus::Live, FeedStatus::Reconnecting, FeedStatus::Syncing};
        for (std::size_t i = 0; i < n; ++i) {
            for (int k = 0; k < 3; ++k) trackers.push_back(std::make_unique<LatencyTracker>(std::string(names[i]) + " t"));
            LatencyTracker& proc = *trackers[3 * i];
            LatencyTracker& exch = *trackers[3 * i + 1];
            LatencyTracker& queue = *trackers[3 * i + 2];
            if (i == 0) {
                proc.recordNs(12345);  // 单个样本：p50 = p99 = 12.3 µs
                                       // single sample: p50 = p99 = 12.3 µs
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
    // Title bar + one order book box per venue + arbitrage monitor + feed performance
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
                                                  // processing latency in µs, one decimal
            CHECK(!contains(plain, "old error"));  // LIVE 时不显示 lastError
                                                   // lastError is hidden while LIVE
            if (n == 1) CHECK(contains(plain, "needs at least two venues"));
            if (n >= 2) {
                CHECK(contains(plain, "OKX:BTCUSDT  RECONNECTING"));
                CHECK(contains(plain, "-- no data --"));
                CHECK(contains(plain, "Spread --  Mid --"));
                CHECK(contains(plain, "! connection lost:  [31mend of file next line"));  // 已清洗
                                                                                          // sanitized
                CHECK(contains(plain, "Binance  OKX"));
            }
            if (n == 3) {
                CHECK(contains(plain, "Bybit    OKX"));
                CHECK(contains(plain, "Binance -> Bybit"));  // 最近信号
                                                             // recent signals
                CHECK(countOf(plain, "\n  │  Binance  ") == 2);  // Binance 作为买方的两个交易所对
                                                                 // the two pairs with Binance as the buy venue
            }
            if (n == 2) CHECK(contains(plain, "1 config warning (see the log)"));
            if (color) {
                CHECK(contains(frame, "\033[32mLIVE\033[0m"));
                if (n >= 2) CHECK(contains(frame, "\033[33mRECONNECTING\033[0m"));
                if (n >= 3) CHECK(contains(frame, "\033[33mSYNCING\033[0m"));
            }
        }
    }
    // STOPPED 红、CONNECTING 黄
    // STOPPED red, CONNECTING yellow
    DisplayFixture fx(3);
    fx.views[1].status = FeedStatus::Stopped;
    fx.views[2].status = FeedStatus::Connecting;
    const std::string frame = Display::renderFrame(fx.views, *fx.detector, FrameOptions());
    CHECK(contains(frame, "\033[31mSTOPPED\033[0m"));
    CHECK(contains(frame, "\033[33mCONNECTING\033[0m"));
    checkFrame(frame, 3, true);
}

TEST_CASE("display: formatting helpers keep fixed widths; status colours; cursor sequences usable with write(2)") {
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
    // headless 状态行与报告共用同一个格式化函数
    // The headless status line and the report share the same formatting function
    CHECK_EQ(Display::plainNumber(99.9, 3), std::string("99.900"));
    CHECK_EQ(Display::plainNumber(-1.005, 2), std::string("-1.00"));
    CHECK_EQ(Display::plainNumber(1234.4, 0), std::string("1234"));
    // 状态的颜色：LIVE 绿，CONNECTING / SYNCING / RECONNECTING 黄，STOPPED 红；无色时为空
    // Status colours: LIVE green, CONNECTING / SYNCING / RECONNECTING yellow, STOPPED red; empty without colour
    const display_detail::Palette pal = display_detail::Palette::make(true);
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Live, pal)), std::string("\033[32m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Connecting, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Syncing, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Reconnecting, pal)), std::string("\033[33m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Stopped, pal)), std::string("\033[31m"));
    CHECK_EQ(std::string(Display::statusColor(FeedStatus::Live, display_detail::Palette::make(false))), std::string());
    // 光标序列是字符数组：sizeof - 1 是长度，信号处理函数里可以直接 write(fd, kShowCursor, sizeof - 1)
    // The cursor sequences are char arrays: sizeof - 1 is the length, so a signal handler can directly call
    // write(fd, kShowCursor, sizeof - 1)
    static_assert(sizeof(Display::kHideCursor) == 7, "kHideCursor must be a char array");
    static_assert(sizeof(Display::kShowCursor) == 7, "kShowCursor must be a char array");
    CHECK_EQ(std::string(Display::kHideCursor), std::string("\033[?25l"));
    CHECK_EQ(std::string(Display::kShowCursor), std::string("\033[?25h"));
}

TEST_CASE("display: pair rows show '--' for a venue that is not LIVE, even while its last book is still there") {
    // 两个交易所的簿交叉：在 A 以 100.00 买、在 B 以 100.50 卖，两者都 LIVE 时检测器出过一次信号
    // The books of the two venues cross: buy on A at 100.00, sell on B at 100.50; while both were LIVE the
    // detector emitted one signal
    std::vector<OrderBook> books;
    books.push_back(makeBook("A:BTCUSDT", {{99.90, 1.0}}, {{100.00, 1.0}}));
    books.push_back(makeBook("B:BTCUSDT", {{100.50, 2.0}}, {{100.60, 2.0}}));
    ArbitrageDetector det(std::vector<double>{0.0, 0.0}, 0.01, 0.0, 20);
    detectAll(det, books);
    REQUIRE_EQ(det.getTotalSignals(), 1u);
    std::vector<VenueView> views(2);
    for (std::size_t i = 0; i < 2; ++i) {
        views[i].name = i == 0 ? "A" : "B";
        views[i].book = &books[i];
        views[i].status = FeedStatus::Live;
    }
    // 交易所对行（买方、卖方两列名字之后是 Best Ask / Best Bid / Diff / Net / Qty / Signals）
    // Venue-pair row (the buy venue and sell venue name columns are followed by
    // Best Ask / Best Bid / Diff / Net / Qty / Signals)
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
                                          // A is still LIVE: its best ask is shown as usual
        CHECK(!contains(ab, "100.50"));   // B 的买一（已过期）不显示
                                          // B's best bid (stale) is not shown
        CHECK(!contains(ab, "+0.50"));
        CHECK_EQ(countOf(ab, " --"), 4u);  // Best Bid、Diff、Net、Qty
                                           // Best Bid, Diff, Net, Qty
        CHECK(contains(ab, "--        1"));  // 累计信号数照常显示
                                             // the cumulative signal count is shown as usual
        const std::string ba = pairRow("B", "A");
        CHECK(!contains(ba, "100.60"));
        CHECK(contains(ba, "99.90"));
        CHECK_EQ(countOf(ba, " --"), 4u);  // Best Ask、Diff、Net、Qty
                                           // Best Ask, Diff, Net, Qty
    }
    // 订单簿框照常显示 B 最后的簿与状态
    // The order book box still shows B's last book and status as usual
    FrameOptions fo;
    fo.color = false;
    const std::string frame = Display::renderFrame(views, det, fo);
    CHECK(contains(frame, "B:BTCUSDT  STOPPED"));
    CHECK(contains(frame, "100.60"));
    checkFrame(frame, 2, false);
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
                                      // the reduced frame still consists of complete, aligned boxes
        CHECK(!contains(fitted, "terminal too small"));
    }
    // 30 行恰好放得下最精简的 3 交易所帧：没有错误行、没有最近信号区、每个簿 1 档
    // 30 rows exactly fit the most compact 3-venue frame: no error lines, no recent-signals section, 1 level
    // per book
    fo.max_rows = 30;
    const std::string minimal = Display::renderFrame(fx.views, *fx.detector, fo);
    CHECK(!contains(minimal, "! connection lost"));
    CHECK(!contains(minimal, "VWAP Buy"));
    CHECK_EQ(splitLines(minimal).size(), 30u);
    for (int rows : {29, 5, 1}) {  // 放不下：保留前 rows - 1 行，最后一行提示
                                   // does not fit: keep the first rows - 1 lines, the last line is a notice
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
                                                  // no newline after the last line
    CHECK_EQ(countOf(first, "\n"), n - 1);

    // 终端宽度 40：每行裁到 40 列；占满整行的行不发擦除到行尾
    // Terminal width 40: each line is clipped to 40 columns; a line that fills the whole row gets no
    // erase-to-end-of-line
    TempStream ts2;
    Display::present(frame, false, 40, ts2.f);
    const std::string clipped = readAll(ts2.f);
    CHECK_EQ(clipped.rfind("\033[H", 0), 0u);
    CHECK(!contains(clipped, "\033[2J"));
    CHECK(!contains(clipped, "\033[K"));  // 这一帧每行都至少 40 列宽
                                          // every line of this frame is at least 40 columns wide
    const std::string body = clipped.substr(3, clipped.size() - 6);
    for (const std::string& line : splitLines(body)) CHECK_EQ(text_util::visibleWidth(line), 40u);

    // 终端恰好与帧同宽：不裁剪，也不发擦除到行尾（否则会擦掉最后一列的右边框）
    // Terminal exactly as wide as the frame: no clipping, and no erase-to-end-of-line either (it would erase
    // the right border in the last column)
    TempStream ts3;
    Display::present(frame, false, Display::kFrameWidth, ts3.f);
    const std::string exact = readAll(ts3.f);
    CHECK(!contains(exact, "\033[K"));
    CHECK_EQ(exact.size(), frame.size() - 1 + 3 + 3);  // 原样 + 归位 + 末尾擦除，最后的换行去掉
                                                       // verbatim + home + final erase, minus the last newline
    // 更宽的终端：每行后擦除到行尾
    // Wider terminal: erase to end of line after every line
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
                                                       // per-venue latency tables + feed statistics
                                                       // + arbitrage summary
            const std::string plain = text_util::stripAnsi(report);
            CHECK(contains(plain, "═══ Final Report ═══"));
            for (std::size_t i = 0; i < n; ++i) CHECK(contains(plain, fx.views[i].name + " Latency"));
            CHECK(contains(plain, "Arbitrage summary"));
            CHECK_EQ(countOf(plain, " -> "), n * (n - 1));
            CHECK(contains(plain, "P50:" + std::string(20, ' ') + "12.3 µs"));  // 标签 18 列 + 数值 10 列右对齐
                                                                                // label 18 columns + value 10 columns
                                                                                // right-aligned
            if (n >= 2) CHECK(contains(plain, "OKX last error: connection lost:  [31mend of file next line"));
            // 每行宽度：表格的每一行与其边框等宽
            // Line width: every row of a table is as wide as its borders
            for (const std::string& line : splitLines(plain)) CHECK_LE(text_util::visibleWidth(line), Display::kFrameWidth);
        }
    }
}

// ============================================================================
// 日志
// Logging
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
// Command line
// ============================================================================

bool parse(std::vector<const char*> args, CliOptions& out, std::string& error) {
    args.insert(args.begin(), "crypto_hft_engine");
    return parseCommandLine(static_cast<int>(args.size()), args.data(), out, error);
}

TEST_CASE("cli: --config/--headless/--duration/--busy-poll/--help in both '--opt value' and '--opt=value' forms") {
    CliOptions o;
    std::string err;
    REQUIRE(parse({}, o, err));
    CHECK_EQ(o.config_path, std::string("config.json"));
    CHECK(!o.config_given);
    CHECK(!o.headless);
    CHECK_EQ(o.duration_s, 0.0);
    CHECK(!o.busy_poll);
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
    REQUIRE(parse({"--busy-poll", "--headless"}, o, err));
    CHECK(o.busy_poll);
    CHECK(o.headless);
    const std::string usage = usageText("crypto_hft_engine");
    for (const char* opt : {"--config <path>", "--headless", "--duration <seconds>", "--help", "[--busy-poll]",
                            "\n  --busy-poll  "}) {
        CHECK(contains(usage, opt));
    }
}

TEST_CASE("cli: errors for unknown options, missing values, bad durations and positional arguments") {
    std::vector<std::pair<std::vector<const char*>, std::string>> cases = {
        {{"--bogus"}, "unknown option \"--bogus\""},
        {{"--headlessly"}, "unknown option \"--headlessly\""},
        {{"--configs", "x"}, "unknown option \"--configs\""},
        {{"--busy-poll=1"}, "unknown option \"--busy-poll=1\""},  // 开关选项不接受 "=value"
                                                                  // flag options do not accept "=value"
        {{"--config"}, "--config requires a value"},
        {{"--config="}, "--config requires a non-empty value"},
        {{"--duration"}, "--duration requires a value"},
        {{"--duration="}, "--duration requires a non-empty value"},
        {{"config.json"}, "unexpected argument \"config.json\""},
    };
    std::vector<std::string> durations;
    for (const char* bad : {"0", "-1", "abc", "2s", "inf", "nan", "1e20"}) durations.push_back(std::string("--duration=") + bad);
    for (const std::string& arg : durations) {
        cases.push_back({{arg.c_str()}, "--duration expects a positive number of seconds, got \"" + arg.substr(11) + "\""});
    }
    for (const auto& c : cases) {
        CliOptions o;
        std::string err;
        CHECK(!parse(c.first, o, err));
        CHECK_EQ(err, c.second);
    }
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
                      // the output is not modified on failure
}

// ============================================================================
// 端到端：模拟交易所 → 真实 MarketDataFeed → MonitorApp 事件循环
// End to end: mock exchange → real MarketDataFeed → MonitorApp event loop
// ============================================================================

using Levels = std::vector<std::pair<std::string, std::string>>;

constexpr const char* kBinanceDiffPath = "/ws/btcusdt@depth@100ms";
constexpr const char* kBinancePartialPath = "/ws/btcusdt@depth20@100ms";
constexpr const char* kOkxPath = "/ws/v5/public";
constexpr const char* kBybitPath = "/v5/public/spot";
constexpr const char* kOkxBooks5Sub = R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})";

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
// Config pointing at the mock server (the three venues share one server, and the script dispatches on the
// upgrade path)
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

// 两个交易所：Binance 部分深度流（不需要 REST 快照）与 OKX books5；Bybit 关闭
// Two venues: the Binance partial-depth stream (no REST snapshot needed) and OKX books5; Bybit disabled
AppConfig twoVenueConfig(MockExchangeServer& server) {
    AppConfig c = mockAppConfig(server);
    c.venues[0].path = kBinancePartialPath;
    c.venues[0].rest_host.clear();
    c.venues[0].snapshot_path.clear();
    c.venues[1].subscribe_msg = kOkxBooks5Sub;
    c.venues[2].enabled = false;
    return c;
}

// OKX books5 会话：第一个会话推送一本簿（买一 100.5、卖一 100.6），linger_ms 后断线并置位 closed；
// 重连后的会话保持静默（状态不会回到 LIVE）
// OKX books5 sessions: the first session pushes one book (best bid 100.5, best ask 100.6), then after
// linger_ms disconnects and sets closed; sessions after a reconnect stay silent (the status does not
// return to LIVE)
void okxOneBookThenClose(WsSession& ws, std::atomic<int>& sessions, std::atomic<bool>& closed, int linger_ms) {
    ws.expectText(kOkxBooks5Sub);
    if (sessions.fetch_add(1) != 0) return;
    ws.send(okxBooks5({{"100.5", "2"}}, {{"100.6", "2"}}));
    ws.sleepMs(linger_ms);
    ws.close(1001, "going away");
    closed.store(true);
}

// 在测试线程上运行 app.run()；另一个线程在 done() 成立且至少运行了 min_ms 后设置停止标志（最长 timeout_ms）
// Runs app.run() on the test thread; another thread sets the stop flag once done() holds and the run has
// lasted at least min_ms (at most timeout_ms)
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

// MonitorApp 的运行环境：临时日志目录、日志器与输出流（RunOptions::out）。
// log() 关闭日志器（写入 Stopped 行）后返回日志文件的内容，只能在 MonitorApp 销毁之后调用。
// Run environment for MonitorApp: temporary log directory, logger and output stream (RunOptions::out).
// log() closes the logger (which writes the Stopped line) and returns the log file's contents; it may only
// be called after the MonitorApp is destroyed.
struct AppEnv {
    TempPath logs{"test_app_logs"};
    TempStream out;
    std::unique_ptr<Logger> logger = std::make_unique<Logger>(logs.str(), false);
    const std::string log_path = logger->path();

    RunOptions options(bool headless = true) const {
        RunOptions o;
        o.headless = headless;
        o.out = out.f;
        return o;
    }
    std::string output() const { return readAll(out.f); }
    std::string log() {
        logger.reset();
        return readFile(log_path);
    }
};

// 管道：写端交给被测代码（fdopen 成 FILE*，或 dup2 成子进程的 stdout/stderr），后台线程读取读端。
// 可以暂停读取来模拟被阻塞的终端（Ctrl+S 流控、卡住的 SSH 会话）：管道写满后写入方阻塞在 write(2) 里。
// Pipe: the write end is handed to the code under test (fdopen'ed into a FILE*, or dup2'ed onto a child's
// stdout/stderr), and a background thread reads the read end.
// Reading can be paused to simulate a blocked terminal (Ctrl+S flow control, a stuck SSH session): once
// the pipe is full, the writer blocks inside write(2).
class PipeReader {
public:
    // capacity > 0：把管道容量设为至少 capacity 字节（F_SETPIPE_SZ，按页取整）
    // capacity > 0: set the pipe capacity to at least capacity bytes (F_SETPIPE_SZ, rounded to whole pages)
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
    // Write end handed to someone else (e.g. closed by fclose after fdopen): this object no longer closes it
    int releaseWriteFd() noexcept { return std::exchange(write_fd_, -1); }
    void closeWriteEnd() noexcept {
        if (write_fd_ >= 0) ::close(std::exchange(write_fd_, -1));
    }

    int capacity() const noexcept { return capacity_; }
    // 写满管道：读取暂停时，之后的任何写入都会阻塞
    // Fills the pipe: while reading is paused, any later write blocks
    bool fill() const {
        const std::string filler(static_cast<std::size_t>(capacity_), '.');
        return ::write(write_fd_, filler.data(), filler.size()) == static_cast<ssize_t>(filler.size());
    }
    void setReading(bool on) noexcept { reading_.store(on); }
    // 管道中尚未读取的字节数
    // Number of bytes in the pipe that have not been read yet
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
    // Waits until part appears in the output (searching from byte offset from)
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
    AppEnv env;
    auto disabledBinance = [] {
        AppConfig c = ConfigReader::getDefaults();
        c.venues[0].enabled = false;
        c.venues[2].name = "Bybit-X";
        c.venues[2].fee_rate = 0.0025;
        return c;
    };
    // 临时 AppConfig：构造结束后即被销毁，MonitorApp 只能引用自己的副本（ASan 下悬空指针会被发现）
    // A temporary AppConfig: destroyed right after construction, so MonitorApp may only refer to its own copy
    // (ASan catches a dangling pointer)
    MonitorApp app(disabledBinance(), *env.logger, RunOptions{});
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
            // The first delta is buffered → REST snapshot L=103 → replay U=101..u=105 (adds one bid level) → LIVE
            ws.send(binanceDiff(101, 105, {{"99.80", "2"}}, {}));
            ws.waitUntil([&] { return okx_sent.load() > 0 && bybit_sent.load() > 0; }, 5000);
            ws.sleepMs(300);
            // 卖一从 100.00 移到 100.20：Binance->OKX 价格变动 -> 新信号；Binance->Bybit 不再盈利
            // Best ask moves from 100.00 to 100.20: Binance->OKX price move -> new signal;
            // Binance->Bybit is no longer profitable
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
    AppEnv env;
    {
        MonitorApp app(mockAppConfig(server), *env.logger, env.options());
        REQUIRE_EQ(app.venueCount(), 3u);
        double stop_ms = 0.0;
        const int rc = runApp(app, [&] { return done.load(); }, 1300, 15000, &stop_ms);
        CHECK_EQ(rc, 0);
        CHECK(done.load());
        CHECK_LT(stop_ms, 1000.0);  // 停止请求到 run() 返回（含停止 3 个 feed 与最终报告）
                                    // from the stop request until run() returns
                                    // (includes stopping 3 feeds and the final report)

        const ArbitrageDetector& det = app.detector();
        CHECK_EQ(det.getTotalSignals(), 4u);
        CHECK_NEAR(det.getTotalNetProfit(), 0.5 + 0.1 + 0.4 + 0.3, 1e-6);
        CHECK_EQ(det.pairState(0, 1).signals, 2u);  // Binance -> OKX：100.00 与 100.20 各一次
                                                    // Binance -> OKX: once each at 100.00 and 100.20
        CHECK_EQ(det.pairState(0, 2).signals, 1u);  // Binance -> Bybit
        CHECK_EQ(det.pairState(2, 1).signals, 1u);  // Bybit -> OKX（数量受 OKX 买一的 2 限制）
                                                    // Bybit -> OKX (quantity limited by the 2 at OKX's best bid)
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
    const std::string text = env.output();
    const std::string port = server.portString();
    CHECK(contains(text, "[FEED] [Binance] connecting to wss://127.0.0.1:" + port + kBinanceDiffPath));
    CHECK(contains(text, "[FEED] [OKX] book synced (LIVE)"));
    CHECK(contains(text, "[FEED] [Bybit] book synced (LIVE)"));
    CHECK(contains(text, "[SIGNAL] SIGNAL | Buy Binance:BTCUSDT @ 100.0000 | Sell OKX:BTCUSDT @ 100.5000 | Qty: 1.0000"));
    CHECK_EQ(countOf(text, "[SIGNAL] SIGNAL | "), 4u);
    CHECK(contains(text, "[00:00:01] Binance LIVE 99.900/100."));  // 至少 5 位有效数字
                                                                   // at least 5 significant digits
    CHECK(contains(text, "| OKX LIVE 100.50/100.60"));
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK_EQ(countOf(text, "Shutting down"), 1u);
    CHECK(contains(text, "═══ Final Report ═══"));
    CHECK(contains(text, "Binance -> OKX"));
    CHECK(text.find('\033') == std::string::npos);  // headless 输出无颜色
                                                    // headless output has no colour
    CHECK(contains(text, "Goodbye!"));
    std::string why;
    const std::size_t report_at = text.find("═══ Final Report ═══");
    REQUIRE(report_at != std::string::npos);
    CHECK_EQ(checkBoxes(text.substr(report_at), why), 5);
    CHECK(why.empty());

    const std::string log = env.log();
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
            // OKX has disconnected (empty book): the best ask move would trigger a new Binance->OKX signal, but
            // the other side has no data
            ws.send(binanceDepth(2, {{"99.95", "1"}}, {{"100.05", "1"}}));
            ws.sleepMs(300);
            done.store(true);
        } else if (ws.target() == kOkxPath) {
            okxOneBookThenClose(ws, okx_sessions, okx_closed, 300);
        }
    });
    AppEnv env;
    {
        RunOptions opt = env.options(false);  // 终端模式：帧写到 out（不是终端：不限制尺寸）
                                              // terminal mode: frames go to out (not a terminal: no size limit)
        opt.color = false;
        MonitorApp app(twoVenueConfig(server), *env.logger, opt);
        REQUIRE_EQ(app.venueCount(), 2u);
        // 等 Binance 脚本结束，并且 OKX 已经重连上（第二个会话）
        // Wait until the Binance script ends and OKX has reconnected (second session)
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
    const std::string text = env.output();
    CHECK(contains(text, Display::kHideCursor));
    CHECK(contains(text, Display::kShowCursor));
    // 最后一帧：最后一个 "光标归位" 到显示光标之间
    // The last frame: between the last "cursor home" and the show-cursor sequence
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
                                                          // the reconnected session is silent: never LIVE again
    CHECK(contains(plain, "Binance -> OKX"));  // 最近信号
                                               // recent signals
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK(contains(text, "Final Report"));
}

TEST_CASE("app: a stale book drained after its venue left LIVE never pairs for arbitrage (blocked terminal output)") {
    // 终端输出被阻塞（Ctrl+S 流控、卡住的 SSH）：管道事先写满，主线程在第一次输出（隐藏光标）时就阻塞，还没有排空过队列。
    // 期间 OKX 推送一本与 Binance 交叉的簿（在 Binance 以 100.00 买、在 OKX 以 100.50 卖）后断线，之后的会话保持静默
    // （状态停在 SYNCING）。输出恢复后主线程才依次取出 Binance 的簿、OKX 过期的簿与断线空簿：这时 OKX 已不是 LIVE，
    // 过期的簿不能配对出信号。
    // Blocked terminal output (Ctrl+S flow control, a stuck SSH session): the pipe is filled beforehand, so the
    // main thread blocks on its first output (hiding the cursor) before it has ever drained the queues.
    // Meanwhile OKX pushes a book that crosses Binance's (buy on Binance at 100.00, sell on OKX at 100.50) and
    // disconnects; its later sessions stay silent (the status stays at SYNCING). Only once the output resumes
    // does the main thread take, in order, Binance's book, OKX's stale book and the disconnect's empty book: by
    // then OKX is no longer LIVE, so the stale book must not pair into a signal.
    MockExchangeServer server;
    std::atomic<bool> okx_closed{false};
    std::atomic<int> okx_sessions{0};
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() == kBinancePartialPath) {
            ws.send(binanceDepth(1, {{"99.90", "1"}}, {{"100.00", "1"}}));
        } else if (ws.target() == kOkxPath) {
            okxOneBookThenClose(ws, okx_sessions, okx_closed, 100);
        }
    });
    AppEnv env;
    PipeReader pipe(4096, false);  // 先不读
                                   // not reading yet
    REQUIRE(pipe.capacity() > 0);
    REQUIRE(pipe.fill());
    REQUIRE_EQ(pipe.buffered(), pipe.capacity());  // 管道已满：之后的任何写入都会阻塞
                                                   // pipe full: any later write blocks
    std::FILE* out = ::fdopen(pipe.releaseWriteFd(), "w");
    REQUIRE(out != nullptr);
    {
        RunOptions opt = env.options(false);
        opt.color = false;
        opt.out = out;
        MonitorApp app(twoVenueConfig(server), *env.logger, opt);
        REQUIRE_EQ(app.venueCount(), 2u);
        std::atomic<bool> stop{false};
        std::atomic<bool> sequence_ok{false};
        std::thread controller([&] {
            // 1) 两本簿与断线空簿都已入队，OKX 已重连到静默会话（不再是 LIVE）
            // 1) Both books and the disconnect's empty book are queued, and OKX has reconnected to the silent
            //    session (no longer LIVE)
            sequence_ok.store(waitFor(
                [&] {
                    return okx_closed.load() && okx_sessions.load() >= 2 && app.feed(1).status() != FeedStatus::Live &&
                           app.feed(1).stats().published >= 2 && app.feed(0).stats().published >= 1;
                },
                10000, 2));
            // 2) 恢复输出：主线程（进入事件循环之前就阻塞在隐藏光标的写入里）这才开始排空积压的快照
            // 2) Resume output: only now does the main thread (blocked in the hide-cursor write since before it
            //    entered the event loop) start draining the backlogged snapshots
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
                                                // both the stale book and the empty book were applied
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
    CHECK_EQ(countOf(env.log(), "[SIGNAL]"), 0u);
    CHECK(server.errors().empty());
}

// 调用线程自身的资源用量（CPU 时间与自愿上下文切换次数：每次睡眠 / 阻塞等待记一次）
// Resource usage of the calling thread itself (CPU time and voluntary context switches: one per sleep /
// blocking wait)
struct ThreadUsage {
    double cpu_s = 0.0;
    long voluntary_switches = 0;
};

ThreadUsage threadUsage() {
    rusage u{};
    ::getrusage(RUSAGE_THREAD, &u);
    ThreadUsage t;
    t.cpu_s = static_cast<double>(u.ru_utime.tv_sec + u.ru_stime.tv_sec) +
              static_cast<double>(u.ru_utime.tv_usec + u.ru_stime.tv_usec) / 1e6;
    t.voluntary_switches = u.ru_nvcsw;
    return t;
}

// 一个刚刚关闭的本地端口：连接会被拒绝
// A local port that was just closed: connections are refused
std::string closedPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    socklen_t len = sizeof(addr);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        throw std::runtime_error("bind failed");
    }
    ::close(fd);
    return std::to_string(ntohs(addr.sin_port));
}

// 所有交易所都连不上（端口已关闭），按 reconnect_ms 退避重连：没有任何数据的监控程序
// Every venue is unreachable (closed port) and reconnects with a reconnect_ms backoff: a monitor that
// has no data at all
AppConfig unreachableConfig(int reconnect_ms) {
    AppConfig cfg = ConfigReader::getDefaults();
    const std::string port = closedPort();
    for (VenueConfig& v : cfg.venues) {
        v.host = "127.0.0.1";
        v.port = port;
    }
    cfg.connection.reconnect_initial_ms = cfg.connection.reconnect_max_ms = reconnect_ms;
    cfg.source = "<test>";
    return cfg;
}

TEST_CASE("app: WakeSignal never loses a wakeup (ping-pong and burst stress with three producers)") {
    constexpr int kProducers = 3;
    constexpr std::int64_t kDeadlineNs = 2000000000;  // 丢失一次唤醒 = 消费者睡满这个期限
                                                      // a lost wakeup = the consumer sleeps until this deadline
    WakeSignal wake;
    std::vector<std::unique_ptr<hft::SpscQueue<int>>> queues;
    for (int p = 0; p < kProducers; ++p) queues.push_back(std::make_unique<hft::SpscQueue<int>>(64));
    auto ready = [&] {
        for (auto& q : queues) {
            if (q->front() != nullptr) return true;
        }
        return false;
    };
    // 消费者：排空；没有数据时等待。timeouts = 等到期限（生产者一直有数据要送，任何一次到期都是丢失的唤醒）；
    // slept = 真正睡过的等待（超过 30 µs，即进入了条件变量等待并被 notify 唤醒）
    // Consumer: drains, and waits when there is no data. timeouts = waits that reached the deadline
    // (the producers always have data to send, so any expiry is a lost wakeup);
    // slept = waits that really slept (over 30 µs, i.e. entered the condition variable wait and were woken
    // by notify)
    auto consume = [&](int total, std::vector<std::atomic<int>>& consumed, int& timeouts, int& slept,
                       double& max_wait_ms) {
        int done = 0;
        while (done < total) {
            bool any = false;
            for (int p = 0; p < kProducers; ++p) {
                while (queues[static_cast<std::size_t>(p)]->front() != nullptr) {
                    queues[static_cast<std::size_t>(p)]->pop();
                    consumed[static_cast<std::size_t>(p)].fetch_add(1, std::memory_order_release);
                    ++done;
                    any = true;
                }
            }
            if (any) continue;
            const std::int64_t t0 = hft::nowNs();
            if (!wake.waitUntil(ready, t0 + kDeadlineNs)) ++timeouts;
            const std::int64_t waited = hft::nowNs() - t0;
            if (waited > 30000) ++slept;
            max_wait_ms = std::max(max_wait_ms, static_cast<double>(waited) / 1e6);
        }
    };
    // 生产者自旋等待时偶尔让出 CPU（测试线程数可能多于 CPU 数）
    // Producers occasionally yield the CPU while spin-waiting (the test may have more threads than CPUs)
    auto relax = [](int& spins) {
        hft::cpuRelax();
        if (++spins % 64 == 0) std::this_thread::yield();
    };

    // 1) 乒乓：每个生产者推入一个元素，等它被取走，随机停顿后再推下一个。消费者每轮都要重新宣布睡眠：
    //    立即推入时命中"宣布睡眠 → 复查"与"入队 → 读 sleeping"的竞争窗口，停顿较长时走条件变量的睡眠与唤醒
    // 1) Ping-pong: each producer pushes one element, waits until it is taken, pauses randomly, then pushes
    //    the next. The consumer has to announce sleeping again every round: an immediate push hits the race
    //    window between "announce sleep → recheck" and "enqueue → read sleeping", and a longer pause goes
    //    through the condition variable's sleep and wakeup
    {
        constexpr int kRounds = 1500;
        std::vector<std::atomic<int>> consumed(kProducers);
        std::vector<std::thread> producers;
        for (int p = 0; p < kProducers; ++p) {
            producers.emplace_back([&, p] {
                std::mt19937 rng(static_cast<unsigned>(p) + 1);
                int spins = 0;
                for (int i = 0; i < kRounds; ++i) {
                    switch (rng() % 4) {
                        case 0: break;  // 立即推入
                                        // push immediately
                        case 1:
                            for (unsigned k = rng() % 512; k > 0; --k) hft::cpuRelax();
                            break;
                        case 2: std::this_thread::yield(); break;
                        default: std::this_thread::sleep_for(std::chrono::microseconds(rng() % 100)); break;
                    }
                    while (!queues[static_cast<std::size_t>(p)]->tryPush(i)) relax(spins);
                    wake.notify();
                    while (consumed[static_cast<std::size_t>(p)].load(std::memory_order_acquire) <= i) relax(spins);
                }
            });
        }
        int timeouts = 0, slept = 0;
        double max_wait_ms = 0.0;
        consume(kProducers * kRounds, consumed, timeouts, slept, max_wait_ms);
        for (std::thread& t : producers) t.join();
        CHECK_EQ(timeouts, 0);
        CHECK_LT(max_wait_ms, 1000.0);
        CHECK_GT(slept, 0);  // 睡眠路径确实被走过
                             // the sleep path was really taken
    }
    // 2) 突发：生产者尽快推入（队列满时重试），消费者大多在忙：notify 走无锁快路径
    // 2) Burst: producers push as fast as possible (retrying while a queue is full) and the consumer is mostly
    //    busy: notify takes the lock-free fast path
    {
        constexpr int kItems = 20000;
        std::vector<std::atomic<int>> consumed(kProducers);
        std::vector<std::thread> producers;
        for (int p = 0; p < kProducers; ++p) {
            producers.emplace_back([&, p] {
                int spins = 0;
                for (int i = 0; i < kItems; ++i) {
                    while (!queues[static_cast<std::size_t>(p)]->tryPush(i)) relax(spins);
                    wake.notify();
                    if (i % 1000 == 999) std::this_thread::sleep_for(std::chrono::microseconds(200));
                }
            });
        }
        int timeouts = 0, slept = 0;
        double max_wait_ms = 0.0;
        consume(kProducers * kItems, consumed, timeouts, slept, max_wait_ms);
        for (std::thread& t : producers) t.join();
        CHECK_EQ(timeouts, 0);
        CHECK_LT(max_wait_ms, 1000.0);
        for (int p = 0; p < kProducers; ++p) CHECK_EQ(consumed[static_cast<std::size_t>(p)].load(), kItems);
    }
    // 3) 没有生产者：之前的 notify 可能留下一次多余的唤醒（消费者复查时已看到数据、没有进入等待），
    //    之后必然等到期限返回 false
    // 3) No producers: an earlier notify may leave one extra wakeup (the consumer saw the data on its recheck
    //    and did not enter the wait); after that, waiting must reach the deadline and return false
    const bool spurious = wake.waitUntil(ready, hft::nowNs() + 20000000);
    const std::int64_t t0 = hft::nowNs();
    CHECK(!wake.waitUntil(ready, t0 + 20000000));
    CHECK_GE(hft::nowNs() - t0, 20000000);
    if (!spurious) CHECK(!wake.waitUntil(ready, hft::nowNs() + 1000000));
}

TEST_CASE("app: an idle main loop blocks until a feed publishes or a timer is due instead of polling") {
    // 修复前：没有数据时每轮 sleep_for(200 µs)，每秒约 3300 次唤醒（4 vCPU 虚拟机上约 10% 个核）。
    // 现在：只在 tick（refresh_ms = 200）与等待上限（50 ms）时醒来，每秒约 20 次
    // Before the fix: with no data, every round did sleep_for(200 µs), about 3300 wakeups per second (about
    // 10% of a core on a 4 vCPU VM).
    // Now: it wakes only for a tick (refresh_ms = 200) and at the wait cap (50 ms), about 20 times per second
    AppEnv env;
    RunOptions opt = env.options();
    opt.duration_s = 1.5;
    MonitorApp app(unreachableConfig(30000), *env.logger, opt);  // 长时间退避：没有任何数据
                                                                 // long backoff: no data at all
    std::atomic<bool> stop{false};
    const ThreadUsage u0 = threadUsage();
    const std::int64_t t0 = hft::nowNs();
    CHECK_EQ(app.run(stop), 0);
    const double wall_s = static_cast<double>(hft::nowNs() - t0) / 1e9;
    const ThreadUsage u1 = threadUsage();
    const double switches_per_s = static_cast<double>(u1.voluntary_switches - u0.voluntary_switches) / wall_s;
    const double cpu_percent = 100.0 * (u1.cpu_s - u0.cpu_s) / wall_s;
    CHECK_LT(switches_per_s, 200.0);
    CHECK_LT(cpu_percent, 5.0);
    CHECK_GE(app.ticks(), 7u);  // tick 照常按 refresh_ms 运行
                                // ticks still run every refresh_ms as usual
    for (std::size_t i = 0; i < app.venueCount(); ++i) CHECK_EQ(app.snapshotsApplied(i), 0u);
}

TEST_CASE("app: a published snapshot wakes the waiting main loop at once, far below the idle wait cap") {
    // refresh_ms = 1000：tick 几乎不会恰好唤醒主循环；快照必须靠发布回调唤醒，否则要等到 50 ms 的上限
    // refresh_ms = 1000: a tick almost never happens to wake the main loop; a snapshot has to wake it via the
    // publish callback, otherwise it waits for the 50 ms cap
    MockExchangeServer server;
    std::atomic<bool> done{false};
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() != kBybitPath) return;
        ws.expectText(kBybitSub);
        for (std::uint64_t u = 1; u <= 40; ++u) {
            const std::string bid = "100." + std::to_string(u % 10);
            ws.send(bybitSnapshot({{bid, "1"}}, {{"101.0", "1"}}, u));
            ws.sleepMs(23);  // 与 tick、等待上限都不同步
                             // in sync with neither the tick nor the wait cap
        }
        done.store(true);
    });
    AppConfig cfg = mockAppConfig(server);
    cfg.venues[0].enabled = false;
    cfg.venues[1].enabled = false;
    cfg.display.refresh_ms = 1000;
    AppEnv env;
    MonitorApp app(cfg, *env.logger, env.options());
    REQUIRE_EQ(app.venueCount(), 1u);
    CHECK_EQ(runApp(app, [&] { return done.load(); }, 100, 15000), 0);
    const LatencyTracker& q = app.queueLatency(0);
    CHECK_EQ(app.snapshotsApplied(0), 40u);
    CHECK_EQ(q.count(), 40u);
    // 唤醒失效时延迟在 0..50 ms 之间均匀分布（p50 约 25 ms）；正常唤醒是零点几毫秒
    // With a broken wakeup the latency is spread uniformly over 0..50 ms (p50 about 25 ms); a working wakeup
    // takes a fraction of a millisecond
    CHECK_LT(static_cast<double>(q.percentileNs(0.50)), 5e6);
    CHECK_LT(static_cast<double>(q.percentileNs(0.90)), 20e6);
}

TEST_CASE("app: --busy-poll spins on the queues instead of sleeping and still processes every snapshot") {
    MockExchangeServer server;
    std::atomic<bool> done{false};
    server.setWebSocketScript([&](WsSession& ws) {
        if (ws.target() != kBybitPath) return;
        ws.expectText(kBybitSub);
        for (std::uint64_t u = 1; u <= 30; ++u) {
            ws.send(bybitSnapshot({{"100.0", "1"}}, {{"101.0", "1"}}, u));
            ws.sleepMs(10);
        }
        done.store(true);
    });
    AppConfig cfg = mockAppConfig(server);
    cfg.venues[0].enabled = false;
    cfg.venues[1].enabled = false;
    AppEnv env;
    {
        RunOptions opt = env.options();
        opt.busy_poll = true;
        MonitorApp app(cfg, *env.logger, opt);
        const ThreadUsage u0 = threadUsage();
        const std::int64_t t0 = hft::nowNs();
        CHECK_EQ(runApp(app, [&] { return done.load() && app.feed(0).stats().published >= 30; }, 300, 15000), 0);
        const double wall_s = static_cast<double>(hft::nowNs() - t0) / 1e9;
        const ThreadUsage u1 = threadUsage();
        CHECK_EQ(app.snapshotsApplied(0), 30u);
        CHECK_LT(static_cast<double>(app.queueLatency(0).percentileNs(0.50)), 1e6);
        // 自旋：主线程一直在运行（被抢占时除外），CPU 时间远高于事件驱动模式（约 0.2%）
        // Spinning: the main thread is always running (except while preempted), and its CPU time is far above
        // that of the event-driven mode (about 0.2%)
        CHECK_GT(100.0 * (u1.cpu_s - u0.cpu_s) / wall_s, 20.0);
    }
    CHECK(contains(env.log(), "Mode: headless, refresh 20 ms, busy-poll"));
}

TEST_CASE("app: no enabled venue -> exit code 1 without starting anything") {
    AppConfig cfg = ConfigReader::getDefaults();
    for (VenueConfig& v : cfg.venues) v.enabled = false;
    AppEnv env;
    MonitorApp app(cfg, *env.logger, env.options());
    CHECK_EQ(app.venueCount(), 0u);
    std::atomic<bool> stop{false};
    CHECK_EQ(app.run(stop), 1);
    CHECK(env.output().empty());
}

TEST_CASE("app: --duration ends the run on time even while every venue keeps failing to connect") {
    // 连接被拒绝，feed 不停退避重连
    // Connections are refused, so the feeds keep reconnecting with backoff
    AppConfig cfg = unreachableConfig(20);
    cfg.connection.reconnect_max_ms = 50;
    cfg.display.refresh_ms = 50;
    AppEnv env;
    RunOptions opt = env.options();
    opt.duration_s = 0.6;
    MonitorApp app(cfg, *env.logger, opt);
    std::atomic<bool> stop{false};
    const std::int64_t t0 = hft::nowNs();
    CHECK_EQ(app.run(stop), 0);
    const double ms = static_cast<double>(hft::nowNs() - t0) / 1e6;
    CHECK_GE(ms, 600.0);
    CHECK_LT(ms, 1600.0);
    const std::string text = env.output();
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

// ============================================================================
// 主程序的信号处理：运行真实的 crypto_hft_engine 子进程
// Signal handling of the main program: runs the real crypto_hft_engine as a child process
// ============================================================================

// 与测试程序同一构建目录中的主程序（CMake 让 test_app 依赖它）
// The main program in the same build directory as the test program (CMake makes test_app depend on it)
std::string engineBinary() {
    std::error_code ec;
    const fs::path self = fs::read_symlink("/proc/self/exe", ec);
    return ec ? std::string("./crypto_hft_engine") : (self.parent_path() / "crypto_hft_engine").string();
}

// /proc/<pid>/status 中的信号掩码（SigPnd、ShdPnd、SigCgt ...，十六进制）；读不到时为 0
// A signal mask from /proc/<pid>/status (SigPnd, ShdPnd, SigCgt ..., hexadecimal); 0 when it cannot be read
std::uint64_t procSignalMask(pid_t pid, const std::string& field) {
    std::ifstream f("/proc/" + std::to_string(pid) + "/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.compare(0, field.size() + 1, field + ":") == 0) return std::stoull(line.substr(field.size() + 1), nullptr, 16);
    }
    return 0;
}

bool maskHas(std::uint64_t mask, int sig) { return ((mask >> (sig - 1)) & 1u) != 0; }

// crypto_hft_engine 子进程：stdout 与 stderr 合并到一个 PipeReader，工作目录 cwd（日志写到 cwd/logs）。
// 子进程放进自己的进程组：孤儿进程组里作业控制信号（SIGTSTP）的默认动作会被内核丢弃，而新进程组的父进程
// （测试程序）在同一会话的另一个进程组里，所以它不是孤儿进程组。
// crypto_hft_engine child process: stdout and stderr are merged into one PipeReader, the working directory
// is cwd (logs go to cwd/logs).
// The child is put into its own process group: in an orphaned process group the kernel discards the
// default action of job-control signals (SIGTSTP), but the new group's parent (the test program) is in
// another process group of the same session, so the new group is not orphaned.
class EngineProcess {
public:
    struct Options {
        std::vector<std::string> args;
        std::string cwd;
        int pipe_capacity = 4096;
        bool prefill = false;        // 启动前写满管道（且不读）：子进程的第一次输出（启动横幅）就阻塞
                                     // fill the pipe before the start (without reading):
                                     // the child's first output (startup banner) blocks
        bool ignore_hangup = false;  // 像 nohup 一样以忽略 SIGHUP 的状态启动
                                     // start with SIGHUP ignored, like nohup
    };

    explicit EngineProcess(const Options& o) : out_(o.pipe_capacity, !o.prefill) {
        if (o.prefill && !out_.fill()) throw std::runtime_error("cannot fill the pipe");
        const std::string bin = engineBinary();
        std::vector<std::string> args = {bin};
        args.insert(args.end(), o.args.begin(), o.args.end());
        std::vector<char*> argv;
        for (std::string& a : args) argv.push_back(a.data());
        argv.push_back(nullptr);
        const int wfd = out_.writeFd();
        pid_ = ::fork();
        if (pid_ < 0) throw std::runtime_error("fork failed");
        if (pid_ == 0) {
            // 子进程：exec 之前只调用 async-signal-safe 的函数
            // Child: only async-signal-safe functions are called before exec
            ::setpgid(0, 0);
            for (int sig : {SIGINT, SIGTERM, SIGTSTP, SIGCONT, SIGPIPE}) ::signal(sig, SIG_DFL);
            ::signal(SIGHUP, o.ignore_hangup ? SIG_IGN : SIG_DFL);
            sigset_t none;
            sigemptyset(&none);
            ::sigprocmask(SIG_SETMASK, &none, nullptr);
            ::dup2(wfd, STDOUT_FILENO);
            ::dup2(wfd, STDERR_FILENO);
            if (::chdir(o.cwd.c_str()) != 0) ::_exit(126);
            ::execv(bin.c_str(), argv.data());
            ::_exit(127);
        }
        ::setpgid(pid_, pid_);  // 父进程也设一次：不论谁先执行，信号发出之前进程组都已建立
                                // the parent sets it too: whichever runs first, the
                                // process group exists before any signal is sent
        out_.closeWriteEnd();
    }

    ~EngineProcess() {
        if (pid_ > 0 && !reaped_) {
            ::kill(pid_, SIGKILL);
            int st = 0;
            ::waitpid(pid_, &st, 0);
        }
    }

    EngineProcess(const EngineProcess&) = delete;
    EngineProcess& operator=(const EngineProcess&) = delete;

    pid_t pid() const noexcept { return pid_; }
    PipeReader& output() noexcept { return out_; }

    // 子进程已安装 sig 的处理函数（/proc 的 SigCgt）
    // The child has installed its handler for sig (SigCgt in /proc)
    bool waitCaught(int sig, int timeout_ms) const {
        return waitFor([&] { return maskHas(procSignalMask(pid_, "SigCgt"), sig); }, timeout_ms, 2);
    }

    // 发送 sig 并等到它被取走（不再挂起），之后再发的同一信号不会与它合并
    // Sends sig and waits until it has been taken (no longer pending), so that the same signal sent later is
    // not merged with it
    bool signalTaken(int sig, int timeout_ms) const {
        if (::kill(pid_, sig) != 0) return false;
        return waitFor(
            [&] { return !maskHas(procSignalMask(pid_, "ShdPnd"), sig) && !maskHas(procSignalMask(pid_, "SigPnd"), sig); },
            timeout_ms, 1);
    }

    // 等待退出，返回 waitpid 的状态；超时返回 -1
    // Waits for the exit and returns the waitpid status; returns -1 on timeout
    int waitExit(int timeout_ms) {
        int st = 0;
        const bool done = waitFor(
            [&] {
                const pid_t r = ::waitpid(pid_, &st, WNOHANG);
                return r == pid_;
            },
            timeout_ms, 5);
        if (!done) return -1;
        reaped_ = true;
        return st;
    }

    // 等到子进程停止（WIFSTOPPED）或继续运行（WIFCONTINUED）
    // Waits until the child stops (WIFSTOPPED) or continues (WIFCONTINUED)
    bool waitStopped(int timeout_ms) { return waitState(WUNTRACED, timeout_ms); }
    bool waitContinued(int timeout_ms) { return waitState(WCONTINUED, timeout_ms); }

private:
    bool waitState(int flag, int timeout_ms) {
        return waitFor(
            [&] {
                int st = 0;
                if (::waitpid(pid_, &st, flag | WNOHANG) != pid_) return false;
                return flag == WUNTRACED ? WIFSTOPPED(st) : WIFCONTINUED(st);
            },
            timeout_ms, 2);
    }

    PipeReader out_;
    pid_t pid_ = -1;
    bool reaped_ = false;
};

// 子进程的临时工作目录与配置：所有交易所都连不上（端口已关闭）、退避 30 s，没有任何数据
// Temporary working directory and config for the child: no venue can be reached (closed port), 30 s
// backoff, no data at all
struct EngineSandbox {
    TempPath dir{"test_app_engine"};
    std::string config;

    explicit EngineSandbox(int refresh_ms) {
        fs::create_directories(dir.path);
        const std::string port = jsonString(closedPort());
        ConfigText t;
        for (const char* v : {"binance", "okx", "bybit"}) t.set(v, "host", "\"127.0.0.1\"").set(v, "port", port);
        t.set("binance", "rest_host", "\"127.0.0.1\"").set("binance", "rest_port", port);
        t.set("connection", "reconnect_initial_ms", "30000").set("connection", "reconnect_max_ms", "30000");
        t.set("display", "refresh_ms", std::to_string(refresh_ms));
        config = (dir.path / "config.json").string();
        std::ofstream(config, std::ios::binary) << t.str();
    }

    EngineProcess::Options options(std::vector<std::string> extra) const {
        EngineProcess::Options o;
        o.args = {"--config", config};
        o.args.insert(o.args.end(), extra.begin(), extra.end());
        o.cwd = dir.str();
        return o;
    }

    // logs/ 下所有日志文件的内容
    // The contents of all log files under logs/
    std::string logs() const {
        std::string all;
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(dir.path / "logs", ec)) all += readFile(e.path().string());
        return all;
    }
};

bool exitedWith(int status, int code) { return status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == code; }

std::string describeStatus(int status) {
    if (status == -1) return "still running";
    if (WIFEXITED(status)) return "exit " + std::to_string(WEXITSTATUS(status));
    if (WIFSIGNALED(status)) return "killed by signal " + std::to_string(WTERMSIG(status));
    return "status " + std::to_string(status);
}

// 子进程以 code 退出，且输出里没有 sanitizer 报告（子进程与测试程序是同一种 sanitizer 构建；
// 强制退出（_Exit）时 sanitizer 的退出码不起作用，所以检查输出）
// The child exited with code, and its output has no sanitizer report (the child is the same kind of
// sanitizer build as the test program; on a forced exit (_Exit) the sanitizer's exit code has no effect,
// so the output is checked)
void checkChildExit(int status, int code, const std::string& out) {
    CHECK(exitedWith(status, code));
    if (!exitedWith(status, code)) std::cerr << "    child: " << describeStatus(status) << std::endl;
    CHECK(!contains(out, "Sanitizer"));
    CHECK(!contains(out, "runtime error:"));
}

// 光标显示着：最后一个隐藏序列之后有显示序列（或从未隐藏）
// The cursor is visible: a show sequence follows the last hide sequence (or it was never hidden)
bool cursorVisibleAtEnd(const std::string& out) {
    const std::size_t hide = out.rfind(Display::kHideCursor);
    return hide == std::string::npos || out.find(Display::kShowCursor, hide) != std::string::npos;
}

TEST_CASE("main: a duplicate SIGINT within 1 s (terminal plus a forwarding wrapper) still ends with the final report") {
    // 一次 Ctrl+C 到达两次：终端发给前台进程组一次，timeout 转发一次，而且第一次已被处理（两次不会合并）。
    // 管道事先写满，子进程装好信号处理函数后阻塞在启动横幅的输出里，两次 SIGINT 都等前一次被取走后才发出。
    // One Ctrl+C arrives twice: the terminal sends it to the foreground process group once, timeout forwards
    // it once, and the first one has already been handled (so the two are not merged).
    // The pipe is filled beforehand, so after installing its signal handlers the child blocks writing the
    // startup banner; each SIGINT is sent only once the previous one has been taken.
    EngineSandbox box(200);
    EngineProcess::Options o = box.options({"--headless", "--duration", "30"});
    o.prefill = true;
    EngineProcess p(o);
    REQUIRE(p.waitCaught(SIGINT, 10000));
    REQUIRE(p.signalTaken(SIGINT, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    REQUIRE(p.signalTaken(SIGINT, 5000));
    p.output().setReading(true);
    const int status = p.waitExit(30000);
    const std::string text = p.output().text();
    checkChildExit(status, 0, text);
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK(contains(text, "═══ Final Report ═══"));
    CHECK(contains(text, "Goodbye!"));
    CHECK(!contains(text, "second signal received"));
    const std::string log = box.logs();
    CHECK(contains(log, "[INFO] === Final Report ==="));
    CHECK(contains(log, "=== Crypto HFT Engine v2.0 Stopped ==="));
}

// 停止读取之后，子进程的输出停滞了 stable_ms（管道里有数据且不再增长）：它阻塞在写 stdout 上
// After reading stops, the child's output has stalled for stable_ms (the pipe holds data that no longer
// grows): the child is blocked writing to stdout
bool waitOutputBlocked(const PipeReader& out, int stable_ms, int timeout_ms) {
    const std::int64_t end = hft::nowNs() + static_cast<std::int64_t>(timeout_ms) * 1000000;
    int last = out.buffered();
    std::int64_t since = hft::nowNs();
    while (hft::nowNs() < end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const int n = out.buffered();
        if (n != last) {
            last = n;
            since = hft::nowNs();
        } else if (n > 0 && hft::nowNs() - since >= static_cast<std::int64_t>(stable_ms) * 1000000) {
            return true;
        }
    }
    return false;
}

// TSan 构建：TSan 把线程阻塞在 libc 内部的 write（fwrite 里，不经过它的拦截函数）期间到达的信号推迟到调用返回后
// 才交给处理函数，并把推迟期间重复到达的同一信号合并成一个
// TSan build: while a thread is blocked in a write inside libc (in fwrite, bypassing TSan's
// interceptors), TSan defers arriving signals until the call returns before handing them to the
// handler, and merges repeats of the same signal during the deferral into one
#if defined(__SANITIZE_THREAD__)
constexpr bool kThreadSanitizer = true;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
constexpr bool kThreadSanitizer = true;
#else
constexpr bool kThreadSanitizer = false;
#endif
#else
constexpr bool kThreadSanitizer = false;
#endif

TEST_CASE("main: a second SIGINT more than 1 s after the first still exits at once, after restoring the cursor") {
    // 终端界面运行中停止读取输出（如同 Ctrl+S）：
    // - 默认：主线程卡在写一帧上（光标仍隐藏），第一次 SIGINT 的优雅退出无法进行；1.2 s 后的第二次 SIGINT 由
    //   处理函数先写显示光标序列，再以 128 + SIGINT 退出。
    // - TSan 构建（见 kThreadSanitizer）：refresh 2 s，在两帧之间（主线程在等待）发第一次 SIGINT，优雅退出卡在
    //   写最终报告上（光标已由 restoreCursor 恢复）；第二次 SIGINT 同样立即退出。
    // Stop reading the output while the terminal UI is running (as with Ctrl+S):
    // - Default: the main thread is stuck writing a frame (cursor still hidden), so the graceful exit for the
    //   first SIGINT cannot proceed; on the second SIGINT 1.2 s later the handler first writes the show-cursor
    //   sequence and then exits with 128 + SIGINT.
    // - TSan build (see kThreadSanitizer): refresh 2 s; the first SIGINT is sent between two frames (while the
    //   main thread waits), and the graceful exit gets stuck writing the final report (the cursor was already
    //   restored by restoreCursor); the second SIGINT also exits at once.
    EngineSandbox box(kThreadSanitizer ? 2000 : 50);
    EngineProcess p(box.options({"--duration", "30"}));
    REQUIRE(p.output().waitForText("Press Ctrl+C to exit", 10000));
    // 第一帧写完
    // first frame fully written
    REQUIRE(p.output().waitForText("\033[J", 5000, p.output().text().rfind("Press Ctrl+C to exit")));
    p.output().setReading(false);
    if (!kThreadSanitizer) REQUIRE(waitOutputBlocked(p.output(), 300, 5000));
    REQUIRE(p.signalTaken(SIGINT, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    REQUIRE(p.signalTaken(SIGINT, 5000));
    p.output().setReading(true);
    const int status = p.waitExit(10000);
    const std::string text = p.output().text();
    checkChildExit(status, 128 + SIGINT, text);
    CHECK(contains(text, "[Signal] second signal received, exiting immediately"));
    CHECK(contains(text, Display::kHideCursor));
    CHECK(cursorVisibleAtEnd(text));
    if (!kThreadSanitizer) {
        // 光标由处理函数恢复：显示序列 + 换行紧接在提示之前，之后没有最终报告
        // The cursor is restored by the handler: show sequence + newline right before the message, and no final
        // report after it
        CHECK(contains(text, std::string(Display::kShowCursor) + "\n[Signal] second signal received"));
        CHECK(!contains(text, "Final Report"));
    }
}

TEST_CASE("main: SIGHUP (even twice, 1.2 s apart) shuts down gracefully with the final report on stdout and in the log") {
    // 终端关闭 / SSH 断开时的 SIGHUP 与 SIGTERM 一样优雅退出；挂断常常带来两个 SIGHUP，从不触发强制退出
    // A SIGHUP from a closed terminal / dropped SSH connection exits gracefully like SIGTERM; a hangup often
    // brings two SIGHUPs, and they never trigger a forced exit
    EngineSandbox box(200);
    EngineProcess::Options o = box.options({"--headless", "--duration", "30"});
    o.prefill = true;
    EngineProcess p(o);
    REQUIRE(p.waitCaught(SIGHUP, 10000));
    REQUIRE(p.signalTaken(SIGHUP, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    REQUIRE(p.signalTaken(SIGHUP, 5000));
    p.output().setReading(true);
    const int status = p.waitExit(30000);
    const std::string text = p.output().text();
    checkChildExit(status, 0, text);
    CHECK(contains(text, "Shutting down (stop requested after"));
    CHECK(contains(text, "═══ Final Report ═══"));
    CHECK(!contains(text, "second signal received"));
    const std::string log = box.logs();
    CHECK(contains(log, "[INFO] === Final Report ==="));
    CHECK(contains(log, "[SUMMARY] SUMMARY | Signals: 0"));
    CHECK(contains(log, "=== Crypto HFT Engine v2.0 Stopped ==="));
}

TEST_CASE("main: a SIGHUP that was ignored at startup (nohup) stays ignored") {
    EngineSandbox box(200);
    EngineProcess::Options o = box.options({"--headless", "--duration", "1.5"});
    o.ignore_hangup = true;
    EngineProcess p(o);
    REQUIRE(p.output().waitForText("Started 3 market data feeds", 10000));
    CHECK(!maskHas(procSignalMask(p.pid(), "SigCgt"), SIGHUP));
    CHECK(maskHas(procSignalMask(p.pid(), "SigIgn"), SIGHUP));
    ::kill(p.pid(), SIGHUP);
    const int status = p.waitExit(15000);
    checkChildExit(status, 0, p.output().text());
    CHECK(contains(p.output().text(), "Shutting down (duration elapsed after"));
}

TEST_CASE("main: Ctrl+Z restores the cursor before the process stops; SIGCONT hides it again and forces a full redraw") {
    EngineSandbox box(200);
    EngineProcess p(box.options({"--duration", "30"}));
    REQUIRE(p.output().waitForText("Feed Performance", 10000));
    std::string text = p.output().text();
    REQUIRE(contains(text, Display::kHideCursor));
    CHECK_EQ(countOf(text, "\033[2J"), 1u);  // 只有第一帧清屏
                                             // only the first frame clears the screen
    // Ctrl+Z：停止之前恢复光标
    // Ctrl+Z: the cursor is restored before stopping
    REQUIRE(p.signalTaken(SIGTSTP, 5000));
    REQUIRE(p.waitStopped(5000));
    CHECK(mock_exchange::waitFor([&] { return cursorVisibleAtEnd(p.output().text()); }, 2000, 5));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));  // 停止期间没有任何输出
                                                                  // no output at all while stopped
    text = p.output().text();
    CHECK(cursorVisibleAtEnd(text));
    const std::size_t stopped_at = text.size();
    // fg：重新隐藏光标，下一帧清屏后整屏重画
    // fg: the cursor is hidden again, and the next frame clears the screen and redraws it in full
    REQUIRE(::kill(p.pid(), SIGCONT) == 0);
    REQUIRE(p.waitContinued(5000));
    CHECK(p.output().waitForText(Display::kHideCursor, 5000, stopped_at));
    CHECK(p.output().waitForText("\033[2J", 5000, stopped_at));
    CHECK(p.output().waitForText("Feed Performance", 5000, stopped_at));
    // 第二次 Ctrl+Z 同样有效（SIGCONT 重新接管了 SIGTSTP）
    // A second Ctrl+Z works too (SIGCONT took over SIGTSTP again)
    REQUIRE(p.signalTaken(SIGTSTP, 5000));
    REQUIRE(p.waitStopped(5000));
    CHECK(mock_exchange::waitFor([&] { return cursorVisibleAtEnd(p.output().text()); }, 2000, 5));
    REQUIRE(::kill(p.pid(), SIGCONT) == 0);
    REQUIRE(p.waitContinued(5000));
    // 最后 Ctrl+C：优雅退出，光标恢复
    // Finally Ctrl+C: graceful exit, cursor restored
    REQUIRE(p.signalTaken(SIGINT, 5000));
    const int status = p.waitExit(15000);
    text = p.output().text();
    checkChildExit(status, 0, text);
    CHECK(cursorVisibleAtEnd(text));
    CHECK(contains(text, "Final Report"));
}

}  // namespace

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
