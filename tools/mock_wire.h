//
// mock_wire.h
// 模拟交易所的线上报文：各交易所风格的十进制格式化、深度差分、Binance / OKX / Bybit 推送与回执的编码，
// 以及按实际发送的字符串计算的 OKX checksum。纯函数，无状态。
// Wire messages of the mock exchange: exchange-style decimal formatting, depth diffs, encoding of
// Binance / OKX / Bybit pushes and replies, and the OKX checksum computed from the strings actually
// sent. Pure functions, no state.
//

#pragma once

#include "hft/crc32.h"
#include "mock_market.h"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mock_exchange::wire {

using sim::Price;
using sim::Qty;

// 一档深度（整数价格 tick / 数量 lot / 订单数）：直接沿用撮合引擎 depth() 的行类型，取深度时无需转换。
// orders 只用于 OKX 档位数组的第 4 个字段。
// One depth level (integer price ticks / quantity lots / order count): reuses the row type of the
// matching engine's depth() directly, so taking depth needs no conversion.
// orders is used only for the 4th field of OKX level arrays.
using Level = sim::mt::LevelInfo;
using Levels = std::vector<Level>;   // 最优在前：买盘价格降序、卖盘价格升序
                                     // best first: bids by descending price, asks by ascending price

// 模拟交易所唯一的交易对在三家的写法
// How each of the three exchanges writes the mock exchange's only trading pair
inline constexpr std::string_view kBinanceSymbol = "BTCUSDT";        // REST 的 symbol 参数、推送里的 "s"
                                                                     // the REST symbol parameter, "s" in pushes
inline constexpr std::string_view kBinanceStreamSymbol = "btcusdt";  // 流名中的小写形式
                                                                     // lowercase form used in stream names
inline constexpr std::string_view kOkxInstId = "BTC-USDT";
inline constexpr std::string_view kBybitSymbol = "BTCUSDT";

// ---------------------------------------------------------------------------
// 数字格式
// Number formats
// ---------------------------------------------------------------------------

struct NumberFormat {
    int price_decimals;  // 输出的小数位数（少于 sim::kPriceDecimals 时按 kPriceDecimals 输出，不丢精度）
                         // number of decimals printed (if fewer than sim::kPriceDecimals,
                         // kPriceDecimals are printed, so no precision is lost)
    bool price_strip;    // 去掉末尾的 0（全为 0 时连小数点一起去掉）
                         // strip trailing zeros (and the decimal point too when all are 0)
    int qty_decimals;
    bool qty_strip;
};

// Binance：价格与数量都是 8 位小数（"65000.12000000" / "0.12345600"），与真实接口一致
// Binance: 8 decimals for both price and quantity ("65000.12000000" / "0.12345600"), as in the real API
inline constexpr NumberFormat kBinanceFormat{8, false, 8, false};
// OKX：最短表示（"65000.1"、"65000"、"0.0123"）。checksum 必须用原样字符串拼接，这种格式最能检验这一点
// OKX: shortest form ("65000.1", "65000", "0.0123"). The checksum must concatenate the verbatim
// strings, and this format tests that best
inline constexpr NumberFormat kOkxFormat{2, true, 6, true};
// Bybit：价格固定 2 位、数量固定 6 位（"65000.10" / "0.012000"）
// Bybit: prices with exactly 2 decimals, quantities with exactly 6 ("65000.10" / "0.012000")
inline constexpr NumberFormat kBybitFormat{2, false, 6, false};

template <class Int>
inline void appendInt(std::string& out, Int v) {
    char buf[24];
    out.append(buf, std::to_chars(buf, buf + sizeof(buf), v).ptr);
}

// 定点数 value × 10^-scale（value >= 0）的十进制表示：小数部分先补足到 decimals 位（至少 scale 位），
// strip 时再去掉末尾的 0
// Decimal representation of the fixed-point number value × 10^-scale (value >= 0): the fraction is
// first padded to decimals digits (at least scale digits), then trailing zeros are removed when strip
// is set
inline void appendScaled(std::string& out, std::uint64_t value, int scale, int decimals, bool strip) {
    std::uint64_t pow10 = 1;
    for (int i = 0; i < scale; ++i) pow10 *= 10;
    appendInt(out, value / pow10);
    char frac[32];
    std::uint64_t f = value % pow10;
    for (int i = scale - 1; i >= 0; --i, f /= 10) frac[i] = static_cast<char>('0' + f % 10);
    int n = scale;
    while (n < decimals) frac[n++] = '0';
    if (strip) {
        while (n > 0 && frac[n - 1] == '0') --n;
    }
    if (n > 0) {
        out.push_back('.');
        out.append(frac, static_cast<std::size_t>(n));
    }
}

inline void appendPrice(std::string& out, Price p, const NumberFormat& f) {
    appendScaled(out, static_cast<std::uint64_t>(p), sim::kPriceDecimals, f.price_decimals, f.price_strip);
}
inline void appendQty(std::string& out, Qty q, const NumberFormat& f) {
    appendScaled(out, static_cast<std::uint64_t>(q), sim::kQtyDecimals, f.qty_decimals, f.qty_strip);
}
inline std::string formatPrice(Price p, const NumberFormat& f) {
    std::string s;
    appendPrice(s, p, f);
    return s;
}
inline std::string formatQty(Qty q, const NumberFormat& f) {
    std::string s;
    appendQty(s, q, f);
    return s;
}

// JSON 字符串字面量（含引号，转义 " \ 与控制字符）
// JSON string literal (including the quotes, escaping " \ and control characters)
inline void appendJsonString(std::string& out, std::string_view s) {
    static constexpr char kHex[] = "0123456789abcdef";
    out.push_back('"');
    for (const char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += "\\u00";
                    out.push_back(kHex[(static_cast<unsigned char>(c) >> 4) & 0xF]);
                    out.push_back(kHex[static_cast<unsigned char>(c) & 0xF]);
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

// ---------------------------------------------------------------------------
// 深度差分
// Depth diffs
// ---------------------------------------------------------------------------

inline bool better(bool bids, Price a, Price b) noexcept { return bids ? a > b : a < b; }

// 同一侧两个窗口的差分（都按最优在前排序），追加到 out：新出现或数量变化的档位给出新数量，消失的档位
// （被撤光、被吃光或被挤出窗口）给出数量 0。compare_orders 为 false 时，只有订单数变化、数量不变的档位
// 不算变化（Binance / Bybit 的档位不带订单数）；OKX 的档位带订单数，传 true。输出最优在前。
// 把输出按"数量 0 删除、否则覆盖"应用到 prev 上恰好得到 cur。
// Diff of two windows of the same side (both sorted best first), appended to out: new levels and
// levels whose quantity changed get their new quantity, vanished levels (fully cancelled, fully
// filled or pushed out of the window) get quantity 0. When compare_orders is false, a level whose
// order count changed but whose quantity did not is not a change (Binance / Bybit levels carry no
// order count); OKX levels carry order counts, so pass true. The output is best first.
// Applying the output to prev with "quantity 0 deletes, otherwise overwrite" yields exactly cur.
inline void diffSide(const Levels& prev, const Levels& cur, bool bids, bool compare_orders, Levels& out) {
    std::size_t i = 0;
    std::size_t j = 0;
    while (i < prev.size() || j < cur.size()) {
        if (j == cur.size() || (i < prev.size() && better(bids, prev[i].price, cur[j].price))) {
            out.push_back(Level{prev[i].price, 0, 0});
            ++i;
        } else if (i == prev.size() || better(bids, cur[j].price, prev[i].price)) {
            out.push_back(cur[j]);
            ++j;
        } else {
            if (prev[i].qty != cur[j].qty || (compare_orders && prev[i].orders != cur[j].orders)) out.push_back(cur[j]);
            ++i;
            ++j;
        }
    }
}

// 档位数组 [["px","qty"],...]（最多前 limit 档）；okx_style 时为 OKX 的 [px, sz, "0", 订单数]（删除档订单数为 "0"）
// Level array [["px","qty"],...] (at most the first limit levels); with okx_style, OKX's
// [px, sz, "0", order count] (deleted levels have order count "0")
inline void appendLevels(std::string& out, const Levels& levels, const NumberFormat& f, bool okx_style,
                         std::size_t limit = SIZE_MAX) {
    out.push_back('[');
    const std::size_t n = std::min(limit, levels.size());
    for (std::size_t i = 0; i < n; ++i) {
        const Level& l = levels[i];
        if (i > 0) out.push_back(',');
        out += "[\"";
        appendPrice(out, l.price, f);
        out += "\",\"";
        appendQty(out, l.qty, f);
        out.push_back('"');
        if (okx_style) {
            out += ",\"0\",\"";
            appendInt(out, l.qty > 0 ? l.orders : 0);
            out.push_back('"');
        }
        out.push_back(']');
    }
    out.push_back(']');
}

// ---------------------------------------------------------------------------
// OKX checksum：前 25 档 bid0:ask0:bid1:ask1:...（每档 "价格:数量"，一侧不足时继续拼另一侧），
// 用与推送完全相同的字符串；CRC32 按有符号 int32 表示
// OKX checksum: the top 25 levels as bid0:ask0:bid1:ask1:... (each level "price:quantity", and when
// one side runs short the other side continues), using exactly the same strings as the push; the
// CRC32 is represented as a signed int32
// ---------------------------------------------------------------------------

inline constexpr std::size_t kOkxChecksumLevels = 25;

inline std::string okxChecksumString(const Levels& bids, const Levels& asks) {
    std::string s;
    s.reserve(kOkxChecksumLevels * 2 * 24);
    const std::size_t nb = std::min(kOkxChecksumLevels, bids.size());
    const std::size_t na = std::min(kOkxChecksumLevels, asks.size());
    auto add = [&s](const Level& l) {
        if (!s.empty()) s.push_back(':');
        appendPrice(s, l.price, kOkxFormat);
        s.push_back(':');
        appendQty(s, l.qty, kOkxFormat);
    };
    for (std::size_t i = 0; i < std::max(nb, na); ++i) {
        if (i < nb) add(bids[i]);
        if (i < na) add(asks[i]);
    }
    return s;
}

inline std::int32_t okxChecksum(const Levels& bids, const Levels& asks) {
    return hft::crc32Signed(okxChecksumString(bids, asks));
}

// ---------------------------------------------------------------------------
// Binance
// ---------------------------------------------------------------------------

// 增量深度事件 <symbol>@depth[@100ms]
// Incremental depth event <symbol>@depth[@100ms]
inline std::string binanceDepthUpdate(std::string_view symbol, std::int64_t event_ms, std::uint64_t first_id,
                                      std::uint64_t final_id, const Levels& bids, const Levels& asks) {
    std::string s;
    s.reserve(112 + 44 * (bids.size() + asks.size()));
    s += R"({"e":"depthUpdate","E":)";
    appendInt(s, event_ms);
    s += R"(,"s":)";
    appendJsonString(s, symbol);
    s += R"(,"U":)";
    appendInt(s, first_id);
    s += R"(,"u":)";
    appendInt(s, final_id);
    s += R"(,"b":)";
    appendLevels(s, bids, kBinanceFormat, false);
    s += R"(,"a":)";
    appendLevels(s, asks, kBinanceFormat, false);
    s.push_back('}');
    return s;
}

// REST GET /api/v3/depth 的响应体，以及部分深度推送 <symbol>@depth<N>[@100ms]（两者形状相同）：每侧最多前 limit 档
// Response body of REST GET /api/v3/depth, and the partial depth push <symbol>@depth<N>[@100ms]
// (both have the same shape): at most the first limit levels per side
inline std::string binanceDepth(std::uint64_t last_update_id, const Levels& bids, const Levels& asks,
                                std::size_t limit = SIZE_MAX) {
    std::string s;
    s.reserve(64 + 44 * (std::min(limit, bids.size()) + std::min(limit, asks.size())));
    s += R"({"lastUpdateId":)";
    appendInt(s, last_update_id);
    s += R"(,"bids":)";
    appendLevels(s, bids, kBinanceFormat, false, limit);
    s += R"(,"asks":)";
    appendLevels(s, asks, kBinanceFormat, false, limit);
    s.push_back('}');
    return s;
}

// combined stream 包装 {"stream":"<name>","data":<payload>}
// Combined stream wrapper {"stream":"<name>","data":<payload>}
inline std::string binanceCombined(std::string_view stream, std::string_view payload) {
    std::string s;
    s.reserve(stream.size() + payload.size() + 24);
    s += R"({"stream":)";
    appendJsonString(s, stream);
    s += R"(,"data":)";
    s += payload;
    s.push_back('}');
    return s;
}

// 方法请求的应答 {"result":<result_json>,"id":<id_json>}（id_json 为原样的 JSON 记号）
// Reply to a method request {"result":<result_json>,"id":<id_json>} (id_json is the verbatim JSON token)
inline std::string binanceResult(std::string_view result_json, std::string_view id_json) {
    std::string s = R"({"result":)";
    s += result_json;
    s += R"(,"id":)";
    s += id_json;
    s.push_back('}');
    return s;
}

// WebSocket 请求错误 {"code":N,"msg":"...","id":<id_json>}（id_json 为空时不带 id）；REST 错误体同形（无 id）
// WebSocket request error {"code":N,"msg":"...","id":<id_json>} (no id when id_json is empty); REST
// error bodies have the same shape (without id)
inline std::string binanceError(int code, std::string_view msg, std::string_view id_json = std::string_view()) {
    std::string s = R"({"code":)";
    appendInt(s, code);
    s += R"(,"msg":)";
    appendJsonString(s, msg);
    if (!id_json.empty()) {
        s += R"(,"id":)";
        s += id_json;
    }
    s.push_back('}');
    return s;
}

// ---------------------------------------------------------------------------
// OKX
// ---------------------------------------------------------------------------

// books 频道：action = "snapshot" / "update"（快照的 prevSeqId 为 -1；心跳是 bids/asks 皆空且 prevSeqId == seqId 的 update）
// books channel: action = "snapshot" / "update" (a snapshot's prevSeqId is -1; a heartbeat is an
// update whose bids/asks are both empty and whose prevSeqId == seqId)
inline std::string okxBooks(std::string_view channel, std::string_view inst_id, std::string_view action,
                            const Levels& bids, const Levels& asks, std::int64_t ts_ms, std::int32_t checksum,
                            std::int64_t prev_seq, std::int64_t seq) {
    std::string s;
    s.reserve(176 + 48 * (bids.size() + asks.size()));
    s += R"({"arg":{"channel":)";
    appendJsonString(s, channel);
    s += R"(,"instId":)";
    appendJsonString(s, inst_id);
    s += R"(},"action":)";
    appendJsonString(s, action);
    s += R"(,"data":[{"asks":)";
    appendLevels(s, asks, kOkxFormat, true);
    s += R"(,"bids":)";
    appendLevels(s, bids, kOkxFormat, true);
    s += R"(,"ts":")";
    appendInt(s, ts_ms);
    s += R"(","checksum":)";
    appendInt(s, checksum);
    s += R"(,"prevSeqId":)";
    appendInt(s, prev_seq);
    s += R"(,"seqId":)";
    appendInt(s, seq);
    s += "}]}";
    return s;
}

// books5 / bbo-tbt 频道：每次都是完整的前 5 / 1 档（没有 action / checksum / prevSeqId）
// books5 / bbo-tbt channels: always the complete top 5 / 1 levels (no action / checksum / prevSeqId)
inline std::string okxTopBook(std::string_view channel, std::string_view inst_id, const Levels& bids,
                              const Levels& asks, std::int64_t ts_ms, std::int64_t seq) {
    std::string s;
    s.reserve(160 + 48 * (bids.size() + asks.size()));
    s += R"({"arg":{"channel":)";
    appendJsonString(s, channel);
    s += R"(,"instId":)";
    appendJsonString(s, inst_id);
    s += R"(},"data":[{"asks":)";
    appendLevels(s, asks, kOkxFormat, true);
    s += R"(,"bids":)";
    appendLevels(s, bids, kOkxFormat, true);
    s += R"(,"instId":)";
    appendJsonString(s, inst_id);
    s += R"(,"ts":")";
    appendInt(s, ts_ms);
    s += R"(","seqId":)";
    appendInt(s, seq);
    s += "}]}";
    return s;
}

// 回执的开头："{" 加上可选的 "id":"<request_id>",（request_id 为空时省略）
// Start of a reply: "{" plus an optional "id":"<request_id>", (omitted when request_id is empty)
inline std::string okxReplyHead(std::string_view request_id) {
    std::string s = "{";
    if (!request_id.empty()) {
        s += R"("id":)";
        appendJsonString(s, request_id);
        s.push_back(',');
    }
    return s;
}

// 订阅 / 退订回执 {"id":..,"event":"subscribe","arg":{"channel":..,"instId":..},"connId":..}
// Subscribe / unsubscribe reply {"id":..,"event":"subscribe","arg":{"channel":..,"instId":..},"connId":..}
inline std::string okxEvent(std::string_view event, std::string_view channel, std::string_view inst_id,
                            std::string_view conn_id, std::string_view request_id) {
    std::string s = okxReplyHead(request_id);
    s += R"("event":)";
    appendJsonString(s, event);
    s += R"(,"arg":{"channel":)";
    appendJsonString(s, channel);
    s += R"(,"instId":)";
    appendJsonString(s, inst_id);
    s += R"(},"connId":)";
    appendJsonString(s, conn_id);
    s.push_back('}');
    return s;
}

inline std::string okxError(std::string_view code, std::string_view msg, std::string_view conn_id,
                            std::string_view request_id) {
    std::string s = okxReplyHead(request_id);
    s += R"("event":"error","code":)";
    appendJsonString(s, code);
    s += R"(,"msg":)";
    appendJsonString(s, msg);
    s += R"(,"connId":)";
    appendJsonString(s, conn_id);
    s.push_back('}');
    return s;
}

// ---------------------------------------------------------------------------
// Bybit
// ---------------------------------------------------------------------------

// orderbook.{depth}.{symbol}：type = "snapshot" / "delta"（data.b / data.a 总是存在，可以为空数组）
// orderbook.{depth}.{symbol}: type = "snapshot" / "delta" (data.b / data.a are always present,
// possibly as empty arrays)
inline std::string bybitBook(std::string_view topic, std::string_view type, std::string_view symbol,
                             const Levels& bids, const Levels& asks, std::uint64_t update_id, std::uint64_t seq,
                             std::int64_t ts_ms, std::int64_t cts_ms) {
    std::string s;
    s.reserve(160 + 40 * (bids.size() + asks.size()));
    s += R"({"topic":)";
    appendJsonString(s, topic);
    s += R"(,"type":)";
    appendJsonString(s, type);
    s += R"(,"ts":)";
    appendInt(s, ts_ms);
    s += R"(,"data":{"s":)";
    appendJsonString(s, symbol);
    s += R"(,"b":)";
    appendLevels(s, bids, kBybitFormat, false);
    s += R"(,"a":)";
    appendLevels(s, asks, kBybitFormat, false);
    s += R"(,"u":)";
    appendInt(s, update_id);
    s += R"(,"seq":)";
    appendInt(s, seq);
    s += R"(},"cts":)";
    appendInt(s, cts_ms);
    s.push_back('}');
    return s;
}

// 请求应答 {"success":..,"ret_msg":..,"conn_id":..,"req_id":..,"op":..}（req_id 为空时省略）
// Request reply {"success":..,"ret_msg":..,"conn_id":..,"req_id":..,"op":..} (req_id omitted when empty)
inline std::string bybitResponse(bool success, std::string_view ret_msg, std::string_view conn_id,
                                 std::string_view req_id, std::string_view op) {
    std::string s = R"({"success":)";
    s += success ? "true" : "false";
    s += R"(,"ret_msg":)";
    appendJsonString(s, ret_msg);
    s += R"(,"conn_id":)";
    appendJsonString(s, conn_id);
    if (!req_id.empty()) {
        s += R"(,"req_id":)";
        appendJsonString(s, req_id);
    }
    s += R"(,"op":)";
    appendJsonString(s, op);
    s.push_back('}');
    return s;
}

// ---------------------------------------------------------------------------
// 订阅名称与 HTTP 请求目标的解析
// Parsing of subscription names and HTTP request targets
// ---------------------------------------------------------------------------

// Binance 流名：btcusdt@depth[@100ms|@1000ms]（增量）或 btcusdt@depth<5|10|20>[@100ms|@1000ms]（部分深度）
// Binance stream names: btcusdt@depth[@100ms|@1000ms] (incremental) or
// btcusdt@depth<5|10|20>[@100ms|@1000ms] (partial depth)
struct BinanceStream {
    int levels = 0;          // 0 = 增量深度；5 / 10 / 20 = 部分深度
                             // 0 = incremental depth; 5 / 10 / 20 = partial depth
    int period_ms = 1000;    // 100 或 1000
                             // 100 or 1000
};

inline bool parseBinanceStream(std::string_view name, BinanceStream& out) {
    constexpr std::string_view kDepth = "@depth";
    if (name.substr(0, kBinanceStreamSymbol.size()) != kBinanceStreamSymbol) return false;
    std::string_view rest = name.substr(kBinanceStreamSymbol.size());
    if (rest.substr(0, kDepth.size()) != kDepth) return false;
    rest.remove_prefix(kDepth.size());
    BinanceStream s;
    if (rest.substr(0, 2) == "20") {
        s.levels = 20;
        rest.remove_prefix(2);
    } else if (rest.substr(0, 2) == "10") {
        s.levels = 10;
        rest.remove_prefix(2);
    } else if (rest.substr(0, 1) == "5") {
        s.levels = 5;
        rest.remove_prefix(1);
    }
    if (rest.empty() || rest == "@1000ms") {
        s.period_ms = 1000;
    } else if (rest == "@100ms") {
        s.period_ms = 100;
    } else {
        return false;
    }
    out = s;
    return true;
}

// Bybit 主题 orderbook.<depth>.BTCUSDT，depth 取 1 / 50 / 200（现货公共频道支持的档数）
// Bybit topic orderbook.<depth>.BTCUSDT with depth 1 / 50 / 200 (the depths offered by the public
// spot channel)
inline bool parseBybitTopic(std::string_view topic, int& depth) {
    constexpr std::string_view kPrefix = "orderbook.";
    if (topic.substr(0, kPrefix.size()) != kPrefix) return false;
    topic.remove_prefix(kPrefix.size());
    const std::size_t dot = topic.find('.');
    if (dot == std::string_view::npos || topic.substr(dot + 1) != kBybitSymbol) return false;
    const std::string_view d = topic.substr(0, dot);
    if (d == "1") depth = 1;
    else if (d == "50") depth = 50;
    else if (d == "200") depth = 200;
    else return false;
    return true;
}

// HTTP 请求目标：路径 + 查询参数（%XX 与 '+' 解码；重复的参数保留第一个）
// HTTP request target: path + query parameters (%XX and '+' are decoded; a repeated parameter keeps
// its first value)
struct HttpTarget {
    std::string path;
    std::vector<std::pair<std::string, std::string>> query;

    const std::string* param(std::string_view key) const {
        for (const auto& kv : query) {
            if (kv.first == key) return &kv.second;
        }
        return nullptr;
    }
};

inline std::string urlDecode(std::string_view s) {
    auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size() && hex(s[i + 1]) >= 0 && hex(s[i + 2]) >= 0) {
            out.push_back(static_cast<char>(hex(s[i + 1]) * 16 + hex(s[i + 2])));
            i += 2;
        } else if (s[i] == '+') {
            out.push_back(' ');
        } else {
            out.push_back(s[i]);
        }
    }
    return out;
}

inline HttpTarget parseHttpTarget(std::string_view target) {
    HttpTarget t;
    const std::size_t q = target.find('?');
    t.path = urlDecode(target.substr(0, q));
    if (q == std::string_view::npos) return t;
    std::string_view query = target.substr(q + 1);
    while (!query.empty()) {
        const std::size_t amp = query.find('&');
        const std::string_view pair = query.substr(0, amp);
        if (!pair.empty()) {
            const std::size_t eq = pair.find('=');
            std::string key = urlDecode(pair.substr(0, eq));
            std::string value = eq == std::string_view::npos ? std::string() : urlDecode(pair.substr(eq + 1));
            if (t.param(key) == nullptr) t.query.emplace_back(std::move(key), std::move(value));
        }
        if (amp == std::string_view::npos) break;
        query.remove_prefix(amp + 1);
    }
    return t;
}

}  // namespace mock_exchange::wire
