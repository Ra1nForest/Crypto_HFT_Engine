//
// json_util.h
// 适配器共用的 JSON 工具：可复用的带 padding 缓冲区（不再每条消息构造 padded_string）、档位数组与数值字段解析。
//

#pragma once

#include "simdjson.h"

#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

namespace adapter_detail {

namespace od = simdjson::ondemand;

// 单条消息（含 REST 快照响应体）的大小上限：超过即视为格式错误，不复制、不解析（与 feed 的 REST 响应体上限一致）
inline constexpr std::size_t kMaxMessageBytes = std::size_t{16} << 20;
// 单个档位数组的元素上限（Binance REST 最多 5000 档；远超任何真实消息，用于限制恶意/损坏消息的内存占用）
inline constexpr std::size_t kMaxLevelsPerArray = 200000;
// 本地簿每侧保留的最大档位数：增量协议会不断带来快照范围之外的远端档位，
// 超出时裁掉最差的远端档位（不影响顶部；Binance 文档本来就说明快照范围外的档位不可靠）
inline constexpr std::size_t kMaxBookLevels = 5000;

// 解析出的原始档位：数值 + 原始字符串（字符串视图指向 simdjson parser 的内部缓冲区，下一次解析前有效）。
// 前两个字段与 PriceLevel 同名，因此可以直接交给 OrderBook::applyLevels
struct RawLevel {
    double price;
    double quantity;
    std::string_view px;
    std::string_view sz;
};

// 可复用的带 padding 缓冲区：把消息复制进来并在末尾补 SIMDJSON_PADDING 个 0 字节。
// 只有遇到比以往更大的消息时才（按 2 倍）扩容，预热之后每条消息零分配。
class PaddedBuffer {
public:
    simdjson::padded_string_view assign(std::string_view data) {
        const std::size_t need = data.size() + simdjson::SIMDJSON_PADDING;
        if (need > capacity_) grow(need);
        if (!data.empty()) std::memcpy(buf_.get(), data.data(), data.size());
        std::memset(buf_.get() + data.size(), 0, simdjson::SIMDJSON_PADDING);
        return simdjson::padded_string_view(buf_.get(), data.size(), capacity_);
    }

private:
    void grow(std::size_t need) {
        std::size_t cap = capacity_ < 4096 ? 4096 : capacity_;
        while (cap < need) cap *= 2;
        buf_.reset(new char[cap]);
        capacity_ = cap;
    }

    std::unique_ptr<char[]> buf_;
    std::size_t capacity_ = 0;
};

// simdjson On-Demand 解析器 + 可复用缓冲区。载入成功后 doc 及其产生的字符串视图在下一次载入之前有效。
// 使用约定（各适配器都遵守）：任何嵌套读取失败都立即中止整条消息。On-Demand 遇到致命错误（TAPE_ERROR、
// INCOMPLETE_ARRAY_OR_OBJECT 等，除 INCORRECT_TYPE / NO_SUCH_FIELD 以外）会废弃文档，外层循环随之静默结束；
// 此时再访问文档（例如 doc.at_end()）是空指针访问。因此 doc.at_end() 只在整个根对象成功迭代完之后调用。
// 没有读取的值由迭代器自动跳过（结构错误仍会在下一次迭代时报告）。
// 各适配器手写字段循环而不用回调：回调 lambda 不会被内联，每个字段都要多一次调用并把迭代器状态写回内存，
// 小消息（bbo-tbt、books5）因此慢几个百分点。
class JsonReader {
public:
    simdjson::error_code load(std::string_view payload, od::document& doc) {
        if (payload.size() > kMaxMessageBytes) return simdjson::CAPACITY;
        return parser_.iterate(buffer_.assign(payload)).get(doc);
    }

private:
    od::parser parser_;
    PaddedBuffer buffer_;
};

// 记录一个字段：它（或与它互斥的 conflicts 中任一字段）已经出现过时返回 false（重复字段 = 格式错误）
inline bool firstSeen(unsigned& seen, unsigned bit, unsigned conflicts = 0) noexcept {
    if ((seen & (bit | conflicts)) != 0) return false;
    seen |= bit;
    return true;
}

// 10^0 .. 10^18：快路径最多 18 位小数（总位数 <= 19 且整数部分至少 1 位），全部可以被 double 精确表示
inline constexpr double kExactPow10[19] = {1e0, 1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
                                           1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};

// 慢路径：std::from_chars（正确舍入），必须整串被消费且结果为有限值（拒绝 "nan"/"inf"/前后缀垃圾）
inline bool parseDecimalSlow(std::string_view s, double& out) noexcept {
    const char* const last = s.data() + s.size();
    const auto res = std::from_chars(s.data(), last, out);
    return res.ec == std::errc() && res.ptr == last && std::isfinite(out);
}

// 把 [p, end) 开头的连续数字累加进 m（m = m * 10^n + 数字），返回第一个非数字的位置。
// 超过 19 位时 m 会回绕，由调用方按位数判定转去慢路径。
// （试过每次并行处理 4 位，以及小数部分一次 8 位的 SWAR：指令数少了，但实测没有变快——Binance 持平，
// OKX / Bybit 反而慢 2-5%；瓶颈在精确除法与循环出口分支，因此保留最简单的写法。）
inline const char* accumulateDigits(const char* p, const char* end, std::uint64_t& m) noexcept {
    while (p != end && static_cast<unsigned>(*p - '0') <= 9u) {
        m = m * 10 + static_cast<unsigned>(*p - '0');
        ++p;
    }
    return p;
}

// 十进制字符串 -> double，结果与 std::from_chars 逐位相同。
// 快路径（Clinger）：交易所的数量/价格几乎都是 [0-9]+(\.[0-9]+)? 的形式。若总位数 <= 19、整数尾数 m <= 2^53，
// 则 m 与 10^k（k <= 18）都能被 double 精确表示，IEEE 除法 m / 10^k 的结果就是精确十进制值的正确舍入，
// 与 from_chars（正确舍入）完全一致。其余情况（指数、超长、符号、"nan" 等）交给 from_chars。
// tests/test_market_data.cpp 用 300 万个随机字符串验证与 from_chars 逐位一致。
// 实测（g++ 13 -O2）：整条消息改用 from_chars 时 depth20 慢约 6%、Bybit delta 慢约 12%，指令数多 11-15%
// （benchmark/market_data_benchmark.cpp 第 1 节）。depth20 一条消息有 80 个数。
inline bool parseDecimal(std::string_view s, double& out) noexcept {
    const char* p = s.data();
    const char* const end = p + s.size();
    std::uint64_t m = 0;
    const char* const int_start = p;
    p = accumulateDigits(p, end, m);
    const int int_digits = static_cast<int>(p - int_start);
    int frac = 0;
    if (p != end && *p == '.') {
        ++p;
        const char* const frac_start = p;
        p = accumulateDigits(p, end, m);
        frac = static_cast<int>(p - frac_start);
        if (frac == 0) return parseDecimalSlow(s, out);  // "1." 之类：交给 from_chars 判定
    }
    if (p != end || int_digits == 0 || int_digits + frac > 19 || m > (std::uint64_t{1} << 53)) {
        return parseDecimalSlow(s, out);
    }
    out = static_cast<double>(m) / kExactPow10[frac];
    return true;
}

// 读取整数字段（std::int64_t 或 std::uint64_t）：接受 JSON 整数或整串都是十进制整数的字符串
// （OKX 的 ts 是字符串，其余交易所是数字）
template <class Int>
inline bool readInt(od::value& v, Int& out) noexcept {
    static_assert(std::is_same_v<Int, std::int64_t> || std::is_same_v<Int, std::uint64_t>);
    od::json_type t;
    if (v.type().get(t) != simdjson::SUCCESS) return false;
    if (t == od::json_type::number) {
        if constexpr (std::is_signed_v<Int>) {
            return v.get_int64().get(out) == simdjson::SUCCESS;
        } else {
            return v.get_uint64().get(out) == simdjson::SUCCESS;
        }
    }
    std::string_view s;
    if (t != od::json_type::string || v.get_string().get(s) != simdjson::SUCCESS) return false;
    const auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

inline bool readString(od::value& v, std::string_view& out) noexcept {
    return v.get_string().get(out) == simdjson::SUCCESS;
}

inline bool readBool(od::value& v, bool& out) noexcept { return v.get_bool().get(out) == simdjson::SUCCESS; }

// 读取用于展示的标量文本：字符串取内容，数字取原始记号（去掉尾随空白）；其他类型失败
inline bool readScalarText(od::value& v, std::string_view& out) noexcept {
    od::json_type t;
    if (v.type().get(t) != simdjson::SUCCESS) return false;
    if (t == od::json_type::string) return v.get_string().get(out) == simdjson::SUCCESS;
    if (t != od::json_type::number) return false;
    std::string_view raw = v.raw_json_token();
    while (!raw.empty() && (raw.back() == ' ' || raw.back() == '\t' || raw.back() == '\n' || raw.back() == '\r')) {
        raw.remove_suffix(1);
    }
    out = raw;
    return true;
}

// 解析档位数组 [[price, size, ...], ...] 并追加到 out。
// 每个元素必须是至少 2 个字符串的数组（多余元素如 OKX 的 "0"、订单数被跳过）；
// 价格必须为正的有限值，数量为非负有限值。任何违例返回 false（调用方丢弃整条消息）。
// 会分配内存（out 扩容，仅预热阶段），因此不是 noexcept；调用方负责捕获 std::bad_alloc。
inline bool parseLevels(od::value& v, std::vector<RawLevel>& out) {
    od::array arr;
    if (v.get_array().get(arr) != simdjson::SUCCESS) return false;
    std::size_t count = 0;
    for (auto element : arr) {
        od::value level_value;
        if (element.get(level_value) != simdjson::SUCCESS) return false;
        od::array level;
        if (level_value.get_array().get(level) != simdjson::SUCCESS) return false;
        RawLevel r{0.0, 0.0, {}, {}};
        std::size_t idx = 0;
        for (auto item : level) {
            od::value item_value;
            if (item.get(item_value) != simdjson::SUCCESS) return false;
            if (idx == 0) {
                if (item_value.get_string().get(r.px) != simdjson::SUCCESS) return false;
            } else if (idx == 1) {
                if (item_value.get_string().get(r.sz) != simdjson::SUCCESS) return false;
            }
            ++idx;  // 其余元素不读取，迭代器会自动跳过（结构错误仍会在下一次迭代时报告）
        }
        if (idx < 2) return false;
        if (!parseDecimal(r.px, r.price) || !(r.price > 0.0)) return false;
        if (!parseDecimal(r.sz, r.quantity) || r.quantity < 0.0) return false;
        if (++count > kMaxLevelsPerArray) return false;
        out.push_back(r);
    }
    return true;
}

// 拼接错误信息（只在错误路径上调用，允许分配）
inline std::string concat(std::initializer_list<std::string_view> parts) {
    std::size_t n = 0;
    for (std::string_view p : parts) n += p.size();
    std::string s;
    s.reserve(n);
    for (std::string_view p : parts) s.append(p.data(), p.size());
    return s;
}

}  // namespace adapter_detail
