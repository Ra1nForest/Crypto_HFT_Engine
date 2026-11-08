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
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
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
    int stall_timeout_ms = 15000;    // 已连接但这么久没有收到任何帧（含心跳回应）→ 踢断重连
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
};

namespace config_detail {

using simdjson::dom::element;
using simdjson::dom::object;

// double 的最短往返表示（0.001 → "0.001"）
inline std::string formatNumber(double v) {
    char buf[64];
    const auto r = std::to_chars(buf, buf + sizeof(buf), v);
    if (r.ec != std::errc()) return "?";
    return std::string(buf, r.ptr);
}

inline std::string quote(const std::string& s) { return "\"" + s + "\""; }

// 一条告警："[Config] <field>: <message>"
inline void addWarning(std::vector<std::string>& warnings, const std::string& field, const std::string& message) {
    warnings.push_back("[Config] " + field + ": " + message);
}

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

// 一个配置块（顶层对象的子对象，如 "display"）的逐字段读取器。构造时取出该块：缺失或不是对象时告警一次
// （整块用默认值），operator bool 为 false。每个读取函数在字段缺失 / 类型错误 / 校验失败时保留默认值
// （或钳制到合法范围）并记录一条告警 "[Config] <块>.<键>: ..."。value 参数进来时就是默认值。
class FieldReader {
public:
    static constexpr double kMaxFeeRate = 0.1;

    FieldReader(std::vector<std::string>& warnings, const object& root, const std::string& block)
        : warnings_(warnings), prefix_(block) {
        element el;
        if (root.at_key(block).get(el) != simdjson::SUCCESS) {
            addWarning(warnings_, block, "missing, using defaults");
        } else if (el.get_object().get(o_) != simdjson::SUCCESS) {
            addWarning(warnings_, block, "expected an object, using defaults");
        } else {
            ok_ = true;
        }
    }

    explicit operator bool() const noexcept { return ok_; }

    void warn(const char* key, const std::string& message) { addWarning(warnings_, prefix_ + "." + key, message); }

    void readBool(const char* key, bool& value) {
        const char* def = value ? "true" : "false";
        element el;
        bool b = false;
        if (!find(key, el, def)) return;
        if (el.get_bool().get(b) != simdjson::SUCCESS) return reject(key, "expected true or false", def);
        value = b;
    }

    // 字符串。required = false 时缺失不告警；allow_empty = false 时空串告警并用默认值
    void readString(const char* key, std::string& value, bool required = true, bool allow_empty = false) {
        const std::string def = quote(value);
        element el;
        std::string_view sv;
        if (!find(key, el, def, required)) return;
        if (el.get_string().get(sv) != simdjson::SUCCESS) return reject(key, "expected a string", def);
        if (sv.empty() && !allow_empty) return reject(key, "must not be empty", def);
        value.assign(sv.data(), sv.size());
    }

    // URL 路径：非空且以 '/' 开头
    void readPath(const char* key, std::string& value) {
        readChecked(key, value, true, false, [](const std::string& s) {
            return s.front() == '/' ? std::string() : quote(s) + " must start with '/'";
        });
    }

    // 主机名（见 validHost）。allow_empty：空串合法（如可选的 rest_host）
    void readHost(const char* key, std::string& value, bool allow_empty = false) {
        readChecked(key, value, true, allow_empty, [](const std::string& s) {
            return validHost(s) ? std::string()
                                : quote(s) + " is not a host name (no scheme, path or spaces, e.g. \"ws.okx.com\")";
        });
    }

    // 订阅消息（键 "subscribe"）：必须是合法 JSON（拼写错误时交易所会静默不推送数据，所以在这里拦住）。
    // required = false 时可以缺失或为空
    void readSubscribe(std::string& value, bool required) {
        readChecked("subscribe", value, required, !required, [](const std::string& s) {
            simdjson::dom::parser check;  // 独立的解析器：不能使配置文档的元素失效
            const simdjson::padded_string padded(s);
            element el;
            return check.parse(padded).get(el) == simdjson::SUCCESS ? std::string() : std::string("is not valid JSON");
        });
    }

    // 端口：JSON 字符串或整数，取值 1-65535
    void readPort(const char* key, std::string& value) {
        const std::string def = quote(value);
        element el;
        if (!find(key, el, def)) return;
        std::string_view sv;
        std::int64_t i = 0;
        std::string candidate;
        if (el.get_string().get(sv) == simdjson::SUCCESS) {
            candidate.assign(sv.data(), sv.size());
        } else if (el.get_int64().get(i) == simdjson::SUCCESS) {
            candidate = std::to_string(i);
        } else {
            return reject(key, "expected string or integer", def);
        }
        if (!validPort(candidate)) return reject(key, quote(candidate) + " is not a valid port (1-65535)", def);
        value = std::move(candidate);
    }

    // 数字，钳制到 [lo, hi]（hi 为 double 最大值时显示为 inf）。int 字段也接受值为整数的小数（如 200.0），拒绝 2.5
    template <class T>
    void readNumber(const char* key, T& value, T lo, T hi) {
        constexpr bool kInt = std::is_integral_v<T>;
        std::string def;
        if constexpr (kInt) {
            def = std::to_string(value);
        } else {
            def = formatNumber(value);
        }
        element el;
        double d = 0.0;  // get_double 也接受 JSON 整数（int64 / uint64 换算成 double）
        if (!find(key, el, def)) return;
        if (el.get_double().get(d) != simdjson::SUCCESS || (kInt && d != std::floor(d))) {
            return reject(key, kInt ? "expected an integer" : "expected a number", def);
        }
        const double l = static_cast<double>(lo), h = static_cast<double>(hi);
        if (d < l || d > h) {
            const double c = d < l ? l : h;
            const std::string upper = h >= std::numeric_limits<double>::max() ? std::string("inf") : formatNumber(h);
            warn(key, formatNumber(d) + " is out of range [" + formatNumber(l) + ", " + upper + "], clamped to " +
                          formatNumber(c));
            d = c;
        }
        value = static_cast<T>(d);
    }

    // 手续费（键 "fee_rate"）：[0, 0.1)。负数钳制为 0；>= 0.1（10%）几乎一定是单位写错（0.001 = 0.1%），
    // 上界是开区间无法钳制到边界，改用 fallback 描述的值（如 "default 0.001" 或 "the global fee_rate"）。
    // 返回读到的可用值；缺失（required 时告警）或不可用时返回 nullopt。
    std::optional<double> readFeeRate(bool required, const std::string& fallback) {
        element el;
        double d = 0.0;
        if (o_.at_key("fee_rate").get(el) != simdjson::SUCCESS) {
            if (required) warn("fee_rate", "missing, using " + fallback);
            return std::nullopt;
        }
        if (el.get_double().get(d) != simdjson::SUCCESS) {
            warn("fee_rate", "expected a number, using " + fallback);
            return std::nullopt;
        }
        if (d < 0.0) {
            warn("fee_rate", formatNumber(d) + " is negative, clamped to 0");
            return 0.0;
        }
        if (d >= kMaxFeeRate) {
            warn("fee_rate", formatNumber(d) + " must be below " + formatNumber(kMaxFeeRate) +
                                 " (fee rates are fractions: 0.001 = 0.1%), using " + fallback);
            return std::nullopt;
        }
        return d;
    }

private:
    // 字段不可用："<problem>, using default <def>"（def 是默认值的显示形式）
    void reject(const char* key, const std::string& problem, const std::string& def) {
        warn(key, problem + ", using default " + def);
    }

    // 查找字段；缺失时（required）告警 "missing, using default <def>"
    bool find(const char* key, element& el, const std::string& def, bool required = true) {
        if (o_.at_key(key).get(el) == simdjson::SUCCESS) return true;
        if (required) warn(key, "missing, using default " + def);
        return false;
    }

    // 字符串字段，取值变化且非空时再校验：check 返回问题描述（空串 = 通过），不通过时保留默认值
    template <class Check>
    void readChecked(const char* key, std::string& value, bool required, bool allow_empty, Check check) {
        std::string candidate = value;
        readString(key, candidate, required, allow_empty);
        if (candidate != value && !candidate.empty()) {
            const std::string problem = check(candidate);
            if (!problem.empty()) return reject(key, problem, quote(value));
        }
        value = std::move(candidate);
    }

    std::vector<std::string>& warnings_;
    std::string prefix_;
    object o_;
    bool ok_ = false;
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
// - 告警按读取顺序排列：各交易所（固定顺序，块内按字段）、arbitrage、display、connection，最后是一致性检查。
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
        object root;
        const simdjson::error_code err = parser.parse(padded).get(doc);
        if (err != simdjson::SUCCESS) {
            config.warnings.push_back("[Config] " + source + ": invalid JSON (" + simdjson::error_message(err) +
                                      "), using defaults");
            return config;
        }
        if (doc.get_object().get(root) != simdjson::SUCCESS) {
            config.warnings.push_back("[Config] " + source + ": top-level value is not a JSON object, using defaults");
            return config;
        }
        config.source = source;
        std::vector<std::string>& w = config.warnings;
        constexpr double kUnbounded = std::numeric_limits<double>::max();

        for (VenueConfig& v : config.venues) readVenue(FieldReader(w, root, v.key), v);
        if (FieldReader r{w, root, "arbitrage"}) {
            ArbitrageConfig& a = config.arbitrage;
            if (const auto fee = r.readFeeRate(true, "default " + formatNumber(a.fee_rate))) a.fee_rate = *fee;
            r.readNumber("min_profit", a.min_profit, 0.0, kUnbounded);
            r.readNumber("max_qty", a.max_qty, 0.0, kUnbounded);
            r.readNumber("max_levels", a.max_levels, kMinMaxLevels, kMaxMaxLevels);
        }
        if (FieldReader r{w, root, "display"}) {
            DisplayConfig& d = config.display;
            r.readNumber("refresh_ms", d.refresh_ms, kMinRefreshMs, kMaxRefreshMs);
            r.readNumber("levels", d.levels, kMinLevels, kMaxLevels);
            r.readNumber("spread_log_interval", d.spread_log_interval, kMinSpreadLogInterval, kMaxSpreadLogInterval);
        }
        if (FieldReader r{w, root, "connection"}) {
            ConnectionConfig& c = config.connection;
            r.readBool("tls_verify", c.tls_verify);
            r.readString("ca_file", c.ca_file, true, true);
            r.readNumber("stall_timeout_ms", c.stall_timeout_ms, kMinStallTimeoutMs, kMaxStallTimeoutMs);
            r.readNumber("reconnect_initial_ms", c.reconnect_initial_ms, kMinReconnectMs, kMaxReconnectMs);
            // 上限不能小于初始值（给出的值按此钳制；缺失时默认值也可能小于给出的初始值）
            r.readNumber("reconnect_max_ms", c.reconnect_max_ms, c.reconnect_initial_ms, kMaxReconnectMs);
            if (c.reconnect_max_ms < c.reconnect_initial_ms) {
                r.warn("reconnect_max_ms", std::to_string(c.reconnect_max_ms) +
                                               " is below connection.reconnect_initial_ms, using " +
                                               std::to_string(c.reconnect_initial_ms));
                c.reconnect_max_ms = c.reconnect_initial_ms;
            }
            // CA 文件读不了时每次连接都会在 TLS 设置阶段失败：提前告警（值保留）
            if (!c.ca_file.empty() && !std::ifstream(c.ca_file).is_open()) {
                r.warn("ca_file", "cannot read " + quote(c.ca_file) + ", every TLS connection will fail");
            }
        }
        checkSymbols(config);
        checkBinanceRest(config);
        return config;
    }

    // 内置默认值（与仓库里的 config.json 相同，只是全局 fee_rate = 0.001）
    static AppConfig getDefaults() {
        auto venue = [](Exchange e, const char* key, const char* host, const char* port, const char* path,
                        const char* subscribe) {
            VenueConfig v;
            v.exchange = e;
            v.key = key;
            v.name = exchangeName(e);
            v.host = host;
            v.port = port;
            v.path = path;
            v.symbol = "BTCUSDT";
            v.subscribe_msg = subscribe;
            return v;
        };
        AppConfig config;
        config.venues = {
            venue(Exchange::BINANCE, "binance", "stream.binance.com", "9443", "/ws/btcusdt@depth@100ms", ""),
            venue(Exchange::OKX, "okx", "ws.okx.com", "8443", "/ws/v5/public",
                  R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]})"),
            venue(Exchange::BYBIT, "bybit", "stream.bybit.com", "443", "/v5/public/spot",
                  R"({"op":"subscribe","args":["orderbook.50.BTCUSDT"]})"),
        };
        VenueConfig& binance = config.venues[0];
        binance.rest_host = "api.binance.com";
        binance.snapshot_path = "/api/v3/depth?symbol=BTCUSDT&limit=1000";
        return config;
    }

    // 一行摘要，如 "Venues: Binance, OKX, Bybit (disabled)  Fee: 0.100% (OKX 0.080%)  Min profit: 0.01  Refresh: 200ms"
    static std::string summary(const AppConfig& c) {
        auto percent = [](double fee) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%.3f%%", fee * 100.0);
            return std::string(buf);
        };
        std::string s = "Venues: ";
        for (std::size_t i = 0; i < c.venues.size(); ++i) {
            s += (i > 0 ? ", " : "") + c.venues[i].name + (c.venues[i].enabled ? "" : " (disabled)");
        }
        s += "  Fee: " + percent(c.arbitrage.fee_rate);
        std::string overrides;
        for (const VenueConfig& v : c.venues) {
            if (v.fee_rate) overrides += (overrides.empty() ? "" : ", ") + v.name + " " + percent(*v.fee_rate);
        }
        if (!overrides.empty()) s += " (" + overrides + ")";
        return s + "  Min profit: " + config_detail::formatNumber(c.arbitrage.min_profit) +
               "  Refresh: " + std::to_string(c.display.refresh_ms) + "ms";
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
    static void readVenue(config_detail::FieldReader r, VenueConfig& v) {
        using config_detail::quote;
        if (!r) return;
        r.readBool("enabled", v.enabled);
        if (!v.enabled) return;
        r.readHost("host", v.host);
        r.readPort("port", v.port);
        r.readPath("path", v.path);
        r.readString("symbol", v.symbol);
        // Binance 通过 URL 路径订阅：subscribe 可选（例如组合流 /ws 上发 SUBSCRIBE）
        const bool binance = v.exchange == Exchange::BINANCE;
        r.readSubscribe(v.subscribe_msg, !binance);
        if (binance) {
            // rest_host / snapshot_path 只有增量深度流需要，可以为空（见 checkBinanceRest）
            r.readHost("rest_host", v.rest_host, true);
            r.readPort("rest_port", v.rest_port);
            r.readString("snapshot_path", v.snapshot_path, true, true);
            if (!v.snapshot_path.empty() && v.snapshot_path.front() != '/') {
                r.warn("snapshot_path", quote(v.snapshot_path) + " must start with '/', using \"\"");
                v.snapshot_path.clear();
            }
        }
        v.fee_rate = r.readFeeRate(false, "the global fee_rate");
    }

    // Binance 增量深度流（path 含 "@depth" / "@depth@100ms"）必须先拉 REST 快照才能同步：缺少 rest_host 或
    // snapshot_path 时 feed 每个会话都会以错误结束并重连，这里提前告警
    static void checkBinanceRest(AppConfig& c) {
        for (const VenueConfig& v : c.venues) {
            if (v.exchange != Exchange::BINANCE || !v.enabled) continue;
            if (!config_detail::isBinanceDiffDepthPath(v.path)) continue;
            if (v.rest_host.empty() || v.snapshot_path.empty()) {
                config_detail::addWarning(c.warnings, v.key + (v.rest_host.empty() ? ".rest_host" : ".snapshot_path"),
                                          "required by the diff-depth stream in " + v.key + ".path " +
                                              config_detail::quote(v.path) +
                                              " (REST snapshot); the feed cannot sync without it");
            }
        }
    }

    // 一致性检查：symbol 只是标签，真正订阅的品种来自 path / subscribe
    static void checkSymbols(AppConfig& c) {
        using config_detail::addWarning;
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
                addWarning(c.warnings, v.key + ".symbol",
                           config_detail::quote(v.symbol) + " does not appear in " + where +
                               " (symbol is only a label; the streamed instrument comes from " + where + ")");
            }
            listing += (any ? ", " : "") + v.name + " " + v.symbol;
            if (!any) first_symbol = sym;
            differs = differs || sym != first_symbol;
            any = true;
        }
        if (differs) {
            addWarning(c.warnings, "symbol",
                       "enabled venues use different symbols (" + listing +
                           "): arbitrage signals would compare different instruments");
        }
    }
};
