//
// config.h
// 配置加载：从 JSON 逐字段读取交易所 / 套利 / 显示 / 连接配置；缺失、类型错误或越界的字段回退到该字段的默认值
// （或钳制到合法范围）并给出指明字段的告警。
//

#pragma once

#include "book_snapshot.h"
#include "exchanges/exchange_adapter.h"
#include "simdjson.h"

#include <cctype>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

// 单个交易所（venue）的连接配置
struct VenueConfig {
    Exchange exchange = Exchange::BINANCE;
    std::string key;             // JSON 中的键："binance" / "okx" / "bybit"
    std::string name;            // 显示名称："Binance" / "OKX" / "Bybit"
    bool enabled = true;
    std::string host, port, path;
    std::string symbol;          // 只是显示标签：真正订阅的品种来自 path（Binance）或 subscribe（OKX / Bybit）
    std::string subscribe_msg;   // 连接后发送的订阅 JSON（Binance 通过 URL 路径订阅，默认为空，可选）
    std::string rest_host, rest_port = "443", snapshot_path;  // 只有 Binance 增量深度（@depth）需要 REST 快照
    std::optional<double> fee_rate;  // 该交易所的单边手续费，覆盖全局 arbitrage.fee_rate（可选）
};

// 套利参数
struct ArbitrageConfig {
    double fee_rate = 0.001;   // 全局单边手续费（taker），0.001 = 0.1%
    double min_profit = 0.01;  // 触发信号的最小净利润（报价货币）
    double max_qty = 0.0;      // 单次扫单的数量上限（0 = 不限）
    int max_levels = 20;       // 扫单时每侧最多走的档位数
};

// 终端显示参数
struct DisplayConfig {
    int refresh_ms = 200;          // 刷新周期（同时是 checkHealth 的周期）
    int levels = 5;                // 每个订单簿显示的档位数
    int spread_log_interval = 50;  // 每这么多次刷新写一次价差日志（次数，不是时间）
};

// 连接参数（对所有交易所生效）
struct ConnectionConfig {
    bool tls_verify = true;          // 校验证书链与主机名
    std::string ca_file;             // 额外信任的 CA（PEM）；"" = 只用系统 CA（离线模拟交易所演示用）
    int stall_timeout_ms = 15000;    // 已连接但这么久没有数据 → 踢断重连
    int reconnect_initial_ms = 500;  // 指数退避初始值
    int reconnect_max_ms = 30000;    // 指数退避上限
};

// 应用配置汇总
struct AppConfig {
    std::vector<VenueConfig> venues;     // 固定顺序：Binance, OKX, Bybit（各自带 enabled）
    ArbitrageConfig arbitrage;
    DisplayConfig display;
    ConnectionConfig connection;
    std::vector<std::string> warnings;   // 每条是完整的一行，形如 "[Config] okx.port: ..."
    std::string source;                  // 实际读取的文件；"" = 内置默认值

    // 某交易所生效的手续费：单独配置的优先，否则用全局值
    double feeRate(const VenueConfig& v) const { return v.fee_rate ? *v.fee_rate : arbitrage.fee_rate; }

    // 已启用的交易所（保持固定顺序）
    std::vector<const VenueConfig*> enabledVenues() const {
        std::vector<const VenueConfig*> out;
        for (const VenueConfig& v : venues) {
            if (v.enabled) out.push_back(&v);
        }
        return out;
    }

    const VenueConfig* venue(Exchange exchange) const {
        for (const VenueConfig& v : venues) {
            if (v.exchange == exchange) return &v;
        }
        return nullptr;
    }
};

namespace config_detail {

using simdjson::dom::element;
using simdjson::dom::element_type;
using simdjson::dom::object;

// double 的最短往返表示（0.001 → "0.001"）
inline std::string formatNumber(double v) {
    char buf[64];
    const auto r = std::to_chars(buf, buf + sizeof(buf), v);
    if (r.ec != std::errc()) return "?";
    return std::string(buf, r.ptr);
}

inline std::string quote(const std::string& s) { return "\"" + s + "\""; }

// 只保留字母数字并转大写："BTC-USDT" / "btcusdt@depth" 中的 "BTCUSDT" 可以直接比较
inline std::string normalizeSymbol(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        const unsigned char u = static_cast<unsigned char>(c);
        if (std::isalnum(u)) out.push_back(static_cast<char>(std::toupper(u)));
    }
    return out;
}

// 端口：1-65535 的十进制数字串
inline bool validPort(std::string_view s) {
    if (s.empty() || s.size() > 5) return false;
    unsigned value = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), value);
    return r.ec == std::errc() && r.ptr == s.data() + s.size() && value >= 1 && value <= 65535;
}

// 主机名：不含空白 / 控制字符与 '/'（常见错误是写成 URL，如 "wss://ws.okx.com/ws"）。
// 允许 ':'，以便使用 IPv6 字面量。
inline bool validHost(std::string_view s) {
    if (s.empty()) return false;
    for (char c : s) {
        const unsigned char u = static_cast<unsigned char>(c);
        if (u <= 0x20 || u == 0x7F || c == '/') return false;
    }
    return true;
}

// Binance 路径是否订阅了增量深度流（"@depth" 后面不是档位数字；"@depth20" 是部分深度快照流）
inline bool isBinanceDiffDepthPath(std::string_view path) {
    constexpr std::string_view kToken = "@depth";
    for (std::size_t pos = path.find(kToken); pos != std::string_view::npos; pos = path.find(kToken, pos + 1)) {
        const std::size_t after = pos + kToken.size();
        if (after >= path.size() || !std::isdigit(static_cast<unsigned char>(path[after]))) return true;
    }
    return false;
}

// 逐字段读取器：每个读取函数在字段缺失 / 类型错误 / 越界时保留默认值（或钳制）并记录一条告警。
// value 参数进来时就是默认值。
class FieldReader {
public:
    explicit FieldReader(std::vector<std::string>& warnings) : warnings_(warnings) {}

    void warn(const std::string& field, const std::string& message) {
        warnings_.push_back("[Config] " + field + ": " + message);
    }

    // 子对象：缺失或不是对象时告警（整块用默认值）并返回 false
    bool block(const object& root, const char* key, object& out) {
        element el;
        if (root.at_key(key).get(el) != simdjson::SUCCESS) {
            warn(key, "missing, using defaults");
            return false;
        }
        if (el.get_object().get(out) != simdjson::SUCCESS) {
            warn(key, "expected an object, using defaults");
            return false;
        }
        return true;
    }

    void readBool(const object& o, const std::string& prefix, const char* key, bool& value) {
        element el;
        if (!find(o, prefix, key, el, value ? "true" : "false")) return;
        bool b = false;
        if (el.get_bool().get(b) != simdjson::SUCCESS) {
            warn(field(prefix, key), std::string("expected true or false, using default ") + (value ? "true" : "false"));
            return;
        }
        value = b;
    }

    // 字符串。required = false 时缺失不告警；allow_empty = false 时空串告警并用默认值
    void readString(const object& o, const std::string& prefix, const char* key, std::string& value,
                    bool required = true, bool allow_empty = false) {
        element el;
        if (!find(o, prefix, key, el, quote(value), required)) return;
        std::string_view sv;
        if (el.get_string().get(sv) != simdjson::SUCCESS) {
            warn(field(prefix, key), "expected a string, using default " + quote(value));
            return;
        }
        if (sv.empty() && !allow_empty) {
            warn(field(prefix, key), "must not be empty, using default " + quote(value));
            return;
        }
        value.assign(sv.data(), sv.size());
    }

    // URL 路径：非空且以 '/' 开头
    void readPath(const object& o, const std::string& prefix, const char* key, std::string& value) {
        std::string candidate = value;
        readString(o, prefix, key, candidate);
        if (candidate != value && candidate.front() != '/') {
            warn(field(prefix, key), quote(candidate) + " must start with '/', using default " + quote(value));
            return;
        }
        value = std::move(candidate);
    }

    // 主机名（见 validHost）。allow_empty：空串合法（如可选的 rest_host）
    void readHost(const object& o, const std::string& prefix, const char* key, std::string& value,
                  bool allow_empty = false) {
        std::string candidate = value;
        readString(o, prefix, key, candidate, true, allow_empty);
        if (candidate == value || (allow_empty && candidate.empty())) {
            value = std::move(candidate);
            return;
        }
        if (!validHost(candidate)) {
            warn(field(prefix, key), quote(candidate) +
                                         " is not a host name (no scheme, path or spaces, e.g. \"ws.okx.com\"), "
                                         "using default " +
                                         quote(value));
            return;
        }
        value = std::move(candidate);
    }

    // 端口：JSON 字符串或整数，取值 1-65535
    void readPort(const object& o, const std::string& prefix, const char* key, std::string& value) {
        element el;
        if (!find(o, prefix, key, el, quote(value))) return;
        std::string_view sv;
        std::int64_t i = 0;
        std::string candidate;
        if (el.get_string().get(sv) == simdjson::SUCCESS) {
            candidate.assign(sv.data(), sv.size());
        } else if (el.get_int64().get(i) == simdjson::SUCCESS) {
            candidate = std::to_string(i);
        } else {
            warn(field(prefix, key), "expected string or integer, using default " + quote(value));
            return;
        }
        if (!validPort(candidate)) {
            warn(field(prefix, key), quote(candidate) + " is not a valid port (1-65535), using default " + quote(value));
            return;
        }
        value = std::move(candidate);
    }

    // 数字（整数或小数），钳制到 [lo, hi]
    void readDouble(const object& o, const std::string& prefix, const char* key, double& value, double lo, double hi) {
        double d = 0.0;
        if (!readNumber(o, prefix, key, value, d)) return;
        value = clamp(prefix, key, d, lo, hi);
    }

    // 整数（也接受值为整数的小数，如 200.0），钳制到 [lo, hi]
    void readInt(const object& o, const std::string& prefix, const char* key, int& value, int lo, int hi) {
        element el;
        if (!find(o, prefix, key, el, std::to_string(value))) return;
        double d = 0.0;
        std::int64_t i = 0;
        std::uint64_t u = 0;
        if (el.get_int64().get(i) == simdjson::SUCCESS) {
            d = static_cast<double>(i);
        } else if (el.get_uint64().get(u) == simdjson::SUCCESS) {
            d = static_cast<double>(u);
        } else if (el.get_double().get(d) != simdjson::SUCCESS || d != std::floor(d)) {
            warn(field(prefix, key), "expected an integer, using default " + std::to_string(value));
            return;
        }
        value = static_cast<int>(clamp(prefix, key, d, static_cast<double>(lo), static_cast<double>(hi)));
    }

    // 手续费：[0, 0.1)。负数钳制为 0；>= 0.1（10%）几乎一定是单位写错（0.001 = 0.1%），
    // 上界是开区间无法钳制到边界，改用默认值。fallback_desc 描述默认值（如 "0.001" 或 "the global fee_rate"）。
    // 返回是否读到了可用的值（out 被赋值）。
    bool readFeeRate(const object& o, const std::string& prefix, const char* key, double& out, bool required,
                     const std::string& fallback_desc) {
        element el;
        if (o.at_key(key).get(el) != simdjson::SUCCESS) {
            if (required) warn(field(prefix, key), "missing, using " + fallback_desc);
            return false;
        }
        double d = 0.0;
        if (el.get_double().get(d) != simdjson::SUCCESS) {
            warn(field(prefix, key), "expected a number, using " + fallback_desc);
            return false;
        }
        if (d < 0.0) {
            warn(field(prefix, key), formatNumber(d) + " is negative, clamped to 0");
            d = 0.0;
        } else if (d >= kMaxFeeRate) {
            warn(field(prefix, key), formatNumber(d) + " must be below " + formatNumber(kMaxFeeRate) +
                                         " (fee rates are fractions: 0.001 = 0.1%), using " + fallback_desc);
            return false;
        }
        out = d;
        return true;
    }

    static constexpr double kMaxFeeRate = 0.1;

private:
    static std::string field(const std::string& prefix, const char* key) { return prefix + "." + key; }

    // 查找字段；缺失时（required）告警。found 返回 true。
    bool find(const object& o, const std::string& prefix, const char* key, element& el, const std::string& default_desc,
              bool required = true) {
        if (o.at_key(key).get(el) == simdjson::SUCCESS) return true;
        if (required) warn(field(prefix, key), "missing, using default " + default_desc);
        return false;
    }

    bool readNumber(const object& o, const std::string& prefix, const char* key, double default_value, double& out) {
        element el;
        if (!find(o, prefix, key, el, formatNumber(default_value))) return false;
        if (el.get_double().get(out) != simdjson::SUCCESS) {
            warn(field(prefix, key), "expected a number, using default " + formatNumber(default_value));
            return false;
        }
        return true;
    }

    double clamp(const std::string& prefix, const char* key, double d, double lo, double hi) {
        if (d < lo || d > hi) {
            const double c = d < lo ? lo : hi;
            const std::string range = "[" + formatNumber(lo) + ", " +
                                      (hi >= std::numeric_limits<double>::max() ? std::string("inf") : formatNumber(hi)) +
                                      "]";
            warn(field(prefix, key), formatNumber(d) + " is out of range " + range + ", clamped to " + formatNumber(c));
            return c;
        }
        return d;
    }

    std::vector<std::string>& warnings_;
};

}  // namespace config_detail

// 配置读取器：
// - 逐字段加载：缺失或类型错误的字段回退到该字段的默认值并告警（告警指明字段，如
//   "[Config] okx.port: expected string or integer, using default \"8443\""）；整块缺失时只告警一次。
// - 越界的数值钳制到合法范围并告警（见下面的 kMin* / kMax* 常量）；fee_rate 的规则见 FieldReader::readFeeRate。
// - 文件不可读 / 不是合法 JSON / 顶层不是对象 → 全部用默认值并告警。未知的键静默忽略。
// - 每个交易所的 enabled 生效（关闭的交易所其余字段不读取、不告警）；可选的每交易所 fee_rate 覆盖全局值；
//   port / rest_port 可以是字符串或整数；host 不能写成 URL。
// - 一致性检查（只告警，不改值）：已启用交易所的 symbol 是否出现在它真正订阅的 path / subscribe 中，
//   各交易所的 symbol 是否相同（不同则套利比较的是不同品种），Binance 增量深度流是否配置了 REST 快照。
// - 默认值 = 仓库里的 config.json，只有全局 fee_rate 的默认值是 0.001（config.json 里是 0.0）。
class ConfigReader {
public:
    static constexpr int kMinRefreshMs = 10, kMaxRefreshMs = 10000;
    static constexpr int kMinLevels = 1, kMaxLevels = BookSnapshot::kMaxLevels;
    static constexpr int kMinSpreadLogInterval = 1, kMaxSpreadLogInterval = 1000000;
    static constexpr int kMinMaxLevels = 1, kMaxMaxLevels = BookSnapshot::kMaxLevels;  // 快照每侧最多 20 档
    static constexpr int kMinStallTimeoutMs = 1000, kMaxStallTimeoutMs = 3600000;
    static constexpr int kMinReconnectMs = 10, kMaxReconnectMs = 600000;

    // 读取并解析配置文件。print = true 时调用 printReport()。
    static AppConfig load(const std::string& filepath, bool print = true) {
        AppConfig config;
        std::string text;
        std::string error;
        if (!readFile(filepath, text, error)) {
            config = getDefaults();
            config.warnings.push_back("[Config] Cannot open: " + filepath + " (" + error + "), using defaults");
        } else {
            config = parse(text, filepath);
        }
        if (print) printReport(config);
        return config;
    }

    // 成功读到文件时在 stdout 打印 "[Config] Loaded: <path>"；告警逐行写到 stderr；最后在 stdout 打印一行摘要
    static void printReport(const AppConfig& config) {
        if (!config.source.empty()) std::cout << "[Config] Loaded: " << config.source << std::endl;
        for (const std::string& w : config.warnings) std::cerr << w << std::endl;
        std::cout << "[Config] " << summary(config) << std::endl;
    }

    // 从 JSON 文本解析（不打印）。source 用于告警文本，并在解析成功时记入 AppConfig::source。
    static AppConfig parse(std::string_view json, const std::string& source = "<string>") {
        using namespace config_detail;
        AppConfig config = getDefaults();
        simdjson::dom::parser parser;
        const simdjson::padded_string padded(json);
        element doc;
        const simdjson::error_code err = parser.parse(padded).get(doc);
        if (err != simdjson::SUCCESS) {
            config.warnings.push_back("[Config] " + source + ": invalid JSON (" + simdjson::error_message(err) +
                                      "), using defaults");
            return config;
        }
        object root;
        if (doc.get_object().get(root) != simdjson::SUCCESS) {
            config.warnings.push_back("[Config] " + source + ": top-level value is not a JSON object, using defaults");
            return config;
        }
        config.source = source;
        FieldReader r(config.warnings);

        for (VenueConfig& v : config.venues) readVenue(r, root, v);

        object o;
        if (r.block(root, "arbitrage", o)) {
            ArbitrageConfig& a = config.arbitrage;
            double fee = a.fee_rate;
            if (r.readFeeRate(o, "arbitrage", "fee_rate", fee, true, "default " + formatNumber(a.fee_rate))) {
                a.fee_rate = fee;
            }
            r.readDouble(o, "arbitrage", "min_profit", a.min_profit, 0.0, std::numeric_limits<double>::max());
            r.readDouble(o, "arbitrage", "max_qty", a.max_qty, 0.0, std::numeric_limits<double>::max());
            r.readInt(o, "arbitrage", "max_levels", a.max_levels, kMinMaxLevels, kMaxMaxLevels);
        }
        if (r.block(root, "display", o)) {
            DisplayConfig& d = config.display;
            r.readInt(o, "display", "refresh_ms", d.refresh_ms, kMinRefreshMs, kMaxRefreshMs);
            r.readInt(o, "display", "levels", d.levels, kMinLevels, kMaxLevels);
            r.readInt(o, "display", "spread_log_interval", d.spread_log_interval, kMinSpreadLogInterval,
                      kMaxSpreadLogInterval);
        }
        if (r.block(root, "connection", o)) {
            ConnectionConfig& c = config.connection;
            r.readBool(o, "connection", "tls_verify", c.tls_verify);
            r.readString(o, "connection", "ca_file", c.ca_file, true, true);
            r.readInt(o, "connection", "stall_timeout_ms", c.stall_timeout_ms, kMinStallTimeoutMs, kMaxStallTimeoutMs);
            r.readInt(o, "connection", "reconnect_initial_ms", c.reconnect_initial_ms, kMinReconnectMs, kMaxReconnectMs);
            // 上限不能小于初始值（给出的值按此钳制；缺失时默认值也可能小于给出的初始值）
            r.readInt(o, "connection", "reconnect_max_ms", c.reconnect_max_ms, c.reconnect_initial_ms, kMaxReconnectMs);
            if (c.reconnect_max_ms < c.reconnect_initial_ms) {
                r.warn("connection.reconnect_max_ms", std::to_string(c.reconnect_max_ms) +
                                                          " is below connection.reconnect_initial_ms, using " +
                                                          std::to_string(c.reconnect_initial_ms));
                c.reconnect_max_ms = c.reconnect_initial_ms;
            }
            // CA 文件读不了时每次连接都会在 TLS 设置阶段失败：提前告警（值保留）
            if (!c.ca_file.empty() && !std::ifstream(c.ca_file).is_open()) {
                r.warn("connection.ca_file", "cannot read " + quote(c.ca_file) + ", every TLS connection will fail");
            }
        }
        checkSymbols(r, config);
        checkBinanceRest(r, config);
        return config;
    }

    // 内置默认值（与仓库里的 config.json 相同，只是全局 fee_rate = 0.001）
    static AppConfig getDefaults() {
        AppConfig config;
        VenueConfig binance;
        binance.exchange = Exchange::BINANCE;
        binance.key = "binance";
        binance.name = exchangeName(Exchange::BINANCE);
        binance.host = "stream.binance.com";
        binance.port = "9443";
        binance.path = "/ws/btcusdt@depth@100ms";
        binance.symbol = "BTCUSDT";
        binance.rest_host = "api.binance.com";
        binance.rest_port = "443";
        binance.snapshot_path = "/api/v3/depth?symbol=BTCUSDT&limit=1000";

        VenueConfig okx;
        okx.exchange = Exchange::OKX;
        okx.key = "okx";
        okx.name = exchangeName(Exchange::OKX);
        okx.host = "ws.okx.com";
        okx.port = "8443";
        okx.path = "/ws/v5/public";
        okx.symbol = "BTCUSDT";
        okx.subscribe_msg = R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})";

        VenueConfig bybit;
        bybit.exchange = Exchange::BYBIT;
        bybit.key = "bybit";
        bybit.name = exchangeName(Exchange::BYBIT);
        bybit.host = "stream.bybit.com";
        bybit.port = "443";
        bybit.path = "/v5/public/spot";
        bybit.symbol = "BTCUSDT";
        bybit.subscribe_msg = R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})";

        config.venues = {binance, okx, bybit};
        return config;
    }

    // 一行摘要，如 "Venues: Binance, OKX, Bybit (disabled)  Fee: 0.100% (OKX 0.080%)  Min profit: 0.01  Refresh: 200ms"
    static std::string summary(const AppConfig& c) {
        std::ostringstream ss;
        ss << "Venues: ";
        for (std::size_t i = 0; i < c.venues.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << c.venues[i].name << (c.venues[i].enabled ? "" : " (disabled)");
        }
        ss << std::fixed << std::setprecision(3) << "  Fee: " << c.arbitrage.fee_rate * 100.0 << "%";
        std::string overrides;
        for (const VenueConfig& v : c.venues) {
            if (!v.fee_rate) continue;
            std::ostringstream o;
            o << std::fixed << std::setprecision(3) << (overrides.empty() ? "" : ", ") << v.name << " "
              << *v.fee_rate * 100.0 << "%";
            overrides += o.str();
        }
        if (!overrides.empty()) ss << " (" << overrides << ")";
        ss << "  Min profit: " << config_detail::formatNumber(c.arbitrage.min_profit)
           << "  Refresh: " << c.display.refresh_ms << "ms";
        return ss.str();
    }

private:
    static bool readFile(const std::string& path, std::string& out, std::string& error) {
        errno = 0;
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            error = errno != 0 ? std::strerror(errno) : "cannot open file";
            return false;
        }
        std::ostringstream buffer;
        buffer << file.rdbuf();
        if (file.bad()) {
            error = "read error";
            return false;
        }
        out = buffer.str();
        return true;
    }

    // 一个交易所块。enabled 为 false 时其余字段不读取也不校验（保持默认值）：
    // "bybit": {"enabled": false} 就是关闭 Bybit 的完整写法，不会产生一串 "missing" 告警。
    static void readVenue(config_detail::FieldReader& r, const config_detail::object& root, VenueConfig& v) {
        using namespace config_detail;
        object o;
        if (!r.block(root, v.key.c_str(), o)) return;
        const std::string& p = v.key;
        r.readBool(o, p, "enabled", v.enabled);
        if (!v.enabled) return;
        r.readHost(o, p, "host", v.host);
        r.readPort(o, p, "port", v.port);
        r.readPath(o, p, "path", v.path);
        r.readString(o, p, "symbol", v.symbol);
        if (v.exchange == Exchange::BINANCE) {
            // Binance 通过 URL 路径订阅：subscribe 可选（例如组合流 /ws 上发 SUBSCRIBE）；
            // rest_host / snapshot_path 只有增量深度流需要，可以为空（见 checkBinanceRest）
            readSubscribe(r, o, p, v.subscribe_msg, false);
            r.readHost(o, p, "rest_host", v.rest_host, true);
            r.readPort(o, p, "rest_port", v.rest_port);
            r.readString(o, p, "snapshot_path", v.snapshot_path, true, true);
            if (!v.snapshot_path.empty() && v.snapshot_path.front() != '/') {
                r.warn(p + ".snapshot_path", quote(v.snapshot_path) + " must start with '/', using \"\"");
                v.snapshot_path.clear();
            }
        } else {
            readSubscribe(r, o, p, v.subscribe_msg, true);
        }
        double fee = 0.0;
        if (r.readFeeRate(o, p, "fee_rate", fee, false, "the global fee_rate")) v.fee_rate = fee;
    }

    // Binance 增量深度流（path 含 "@depth" / "@depth@100ms"）必须先拉 REST 快照才能同步：缺少 rest_host 或
    // snapshot_path 时 feed 每个会话都会以错误结束并重连，这里提前告警
    static void checkBinanceRest(config_detail::FieldReader& r, const AppConfig& c) {
        for (const VenueConfig& v : c.venues) {
            if (v.exchange != Exchange::BINANCE || !v.enabled) continue;
            if (!config_detail::isBinanceDiffDepthPath(v.path)) continue;
            if (v.rest_host.empty() || v.snapshot_path.empty()) {
                r.warn(v.key + (v.rest_host.empty() ? ".rest_host" : ".snapshot_path"),
                       "required by the diff-depth stream in " + v.key + ".path " + config_detail::quote(v.path) +
                           " (REST snapshot); the feed cannot sync without it");
            }
        }
    }

    // 订阅消息：必须是合法 JSON（拼写错误时交易所会静默不推送数据，所以在这里拦住）
    static void readSubscribe(config_detail::FieldReader& r, const config_detail::object& o, const std::string& p,
                              std::string& value, bool required) {
        std::string candidate = value;
        r.readString(o, p, "subscribe", candidate, required, !required);
        if (candidate == value || candidate.empty()) {
            value = std::move(candidate);
            return;
        }
        simdjson::dom::parser check;  // 独立的解析器：不能使配置文档的元素失效
        const simdjson::padded_string padded(candidate);
        simdjson::dom::element el;
        if (check.parse(padded).get(el) != simdjson::SUCCESS) {
            r.warn(p + ".subscribe", "is not valid JSON, using default " + config_detail::quote(value));
            return;
        }
        value = std::move(candidate);
    }

    // 一致性检查：symbol 只是标签，真正订阅的品种来自 path / subscribe
    static void checkSymbols(config_detail::FieldReader& r, const AppConfig& c) {
        using config_detail::normalizeSymbol;
        std::string first_symbol, listing;
        bool any = false, differs = false;
        for (const VenueConfig& v : c.venues) {
            if (!v.enabled) continue;
            const std::string sym = normalizeSymbol(v.symbol);
            const std::string stream = normalizeSymbol(v.path + " " + v.subscribe_msg);
            if (stream.find(sym) == std::string::npos) {
                std::string where = v.key + (v.exchange != Exchange::BINANCE ? ".subscribe"
                                             : v.subscribe_msg.empty()    ? ".path"
                                                                          : ".path / " + v.key + ".subscribe");
                r.warn(v.key + ".symbol", config_detail::quote(v.symbol) + " does not appear in " + where +
                                              " (symbol is only a label; the streamed instrument comes from " + where +
                                              ")");
            }
            listing += (any ? ", " : "") + v.name + " " + v.symbol;
            if (!any) first_symbol = sym;
            differs = differs || sym != first_symbol;
            any = true;
        }
        if (differs) {
            r.warn("symbol", "enabled venues use different symbols (" + listing +
                                 "): arbitrage signals would compare different instruments");
        }
    }
};
