//
// text_util.h
// 终端文本工具：按"可见宽度"（UTF-8 码点数，跳过 ANSI 转义序列）填充、居中、截断、去色与清洗。
//

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

// 可见宽度的定义（所有函数一致）：
// - ANSI 转义序列宽度为 0：CSI（ESC [ 参数字节 0x30-0x3F* 中间字节 0x20-0x2F* 终止字节 0x40-0x7E，
//   例如颜色 "\033[32m"）、OSC（ESC ] ... 以 BEL 或 ESC \ 结束）以及其他 ESC 开头的短序列。
//   未结束的序列同样宽度为 0。
// - 其余每个 UTF-8 码点宽度为 1（'µ'、'═'、'│' 等单宽字符都是 1 列）；C0 控制字符与 DEL 宽度为 0。
// - 非法的 UTF-8 字节（孤立的续字节、缺少续字节的首字节、过长编码、代理码点、0xF5-0xFF 等）每个按 1 列计
//   （终端显示为 U+FFFD）。
// - 已知局限：东亚宽字符、emoji 在终端中占 2 列，组合字符占 0 列，这里都按 1 列计。
//   本程序的输出只有 ASCII、框线字符（'─'、'│'、'═' 等 U+2500 区段）、'…' 与 'µ'，都是单宽字符，不受影响。
// - "悬空"转义序列：字符串在一个转义序列中间结束（如孤立的 ESC、"\033[31"、没有结束符的 OSC）。在它后面追加的
//   文本会被终端（和 visibleWidth）当作序列的一部分吞掉，所以生成文本的函数（truncateVisible / padRight / padLeft /
//   center / fit）都会丢掉结尾的悬空序列（它本来就不可见），保证结果可以安全地与其他文本拼接。
// 截断永远不会切开一个码点或一个完整的转义序列。
namespace text_util {

namespace detail {

// 一个词法单元：转义序列（宽度 0）或一个码点 / 非法字节。
// 未结束的转义序列（complete = false）如果一直延续到字符串末尾，就是"悬空"序列（见文件头说明）。
struct Token {
    std::size_t len = 0;      // 字节数（>= 1）
    std::size_t width = 0;    // 可见宽度（0 或 1）
    bool escape = false;      // 是否为转义序列
    bool complete = false;    // 转义序列以合法的终止字节（或 OSC 的 BEL / ST）结束
};

inline bool isContinuation(unsigned char b) noexcept { return (b & 0xC0) == 0x80; }

inline bool inRange(unsigned char b, unsigned char lo, unsigned char hi) noexcept { return b >= lo && b <= hi; }

// 解析 s[i] 开始的转义序列（s[i] == ESC）
inline Token escapeToken(std::string_view s, std::size_t i) noexcept {
    const std::size_t n = s.size();
    std::size_t j = i + 1;
    if (j >= n) return Token{1, 0, true, false};  // 孤立的 ESC
    const unsigned char kind = static_cast<unsigned char>(s[j]);
    if (kind == '[') {  // CSI
        ++j;
        while (j < n && inRange(static_cast<unsigned char>(s[j]), 0x30, 0x3F)) ++j;
        while (j < n && inRange(static_cast<unsigned char>(s[j]), 0x20, 0x2F)) ++j;
        if (j < n && inRange(static_cast<unsigned char>(s[j]), 0x40, 0x7E)) return Token{j + 1 - i, 0, true, true};
        // 终止字节缺失：字符串在这里结束（悬空），或后面是非法字节（序列在此中断，之后的字节按普通字符处理）
        return Token{j - i, 0, true, false};
    }
    if (kind == ']') {  // OSC：到 BEL 或 ST（ESC \）为止
        ++j;
        while (j < n) {
            const unsigned char b = static_cast<unsigned char>(s[j]);
            if (b == 0x07) return Token{j + 1 - i, 0, true, true};
            if (b == 0x1B && j + 1 < n && s[j + 1] == '\\') return Token{j + 2 - i, 0, true, true};
            ++j;
        }
        return Token{j - i, 0, true, false};
    }
    // 其他 ESC 序列：中间字节 0x20-0x2F*，再加一个终止字节 0x30-0x7E
    while (j < n && inRange(static_cast<unsigned char>(s[j]), 0x20, 0x2F)) ++j;
    if (j < n && inRange(static_cast<unsigned char>(s[j]), 0x30, 0x7E)) return Token{j + 1 - i, 0, true, true};
    return Token{j - i, 0, true, false};
}

// 读取 s[i] 开始的一个词法单元（i < s.size()）。
// 多字节序列按 RFC 3629 严格校验：第二个字节的合法范围随首字节而定（E0: A0-BF，ED: 80-9F，F0: 90-BF，
// F4: 80-8F，其余 80-BF），从而拒绝过长编码、UTF-16 代理码点（U+D800-U+DFFF）与大于 U+10FFFF 的值。
inline Token nextToken(std::string_view s, std::size_t i) noexcept {
    const unsigned char b = static_cast<unsigned char>(s[i]);
    if (b == 0x1B) return escapeToken(s, i);
    if (b < 0x80) return Token{1, (b < 0x20 || b == 0x7F) ? std::size_t{0} : std::size_t{1}, false, false};
    std::size_t need = 0;
    unsigned char lo = 0x80, hi = 0xBF;  // 第二个字节的合法范围
    if (inRange(b, 0xC2, 0xDF)) {
        need = 1;
    } else if (inRange(b, 0xE0, 0xEF)) {
        need = 2;
        if (b == 0xE0) lo = 0xA0;
        if (b == 0xED) hi = 0x9F;
    } else if (inRange(b, 0xF0, 0xF4)) {
        need = 3;
        if (b == 0xF0) lo = 0x90;
        if (b == 0xF4) hi = 0x8F;
    } else {
        return Token{1, 1, false, false};  // 孤立续字节或非法首字节（C0、C1、F5-FF）
    }
    for (std::size_t k = 1; k <= need; ++k) {
        // 缺少续字节（含字符串在多字节序列中间结束）或第二个字节越界：首字节单独算一个非法字符，后面的字节重新解析
        if (i + k >= s.size()) return Token{1, 1, false, false};
        const unsigned char c = static_cast<unsigned char>(s[i + k]);
        if (k == 1 ? !inRange(c, lo, hi) : !isContinuation(c)) return Token{1, 1, false, false};
    }
    return Token{need + 1, 1, false, false};
}

// 一次扫描得到可见宽度，以及去掉结尾悬空转义序列后的长度。
// 去掉的是"以悬空序列结尾的一串未完成转义序列"：例如 "a\033\033" 中第一个 ESC 只是被第二个 ESC 打断，
// 去掉第二个之后它自己就悬空了，所以两个都要去掉。去掉的都是宽度 0 的字节，之前的词法单元不受影响。
struct Measure {
    std::size_t width = 0;
    std::size_t keep = 0;   // s 的前 keep 个字节结尾不悬空
};

inline Measure measure(std::string_view s) noexcept {
    constexpr std::size_t kNone = static_cast<std::size_t>(-1);
    Measure m;
    m.keep = s.size();
    std::size_t run = kNone;  // 结尾处连续的未完成转义序列的起点
    for (std::size_t i = 0; i < s.size();) {
        const Token t = nextToken(s, i);
        if (!t.escape || t.complete) {
            run = kNone;
        } else if (run == kNone) {
            run = i;
        }
        if (run != kNone && i + t.len == s.size()) {  // 悬空：未结束的序列一直延续到末尾（必然是最后一个词法单元）
            m.keep = run;
            break;
        }
        m.width += t.width;
        i += t.len;
    }
    return m;
}

}  // namespace detail

// 可见宽度（列数）
inline std::size_t visibleWidth(std::string_view s) noexcept { return detail::measure(s).width; }

// 去掉所有转义序列（得到纯文本，用于写日志或测试比较）
inline std::string stripAnsi(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size();) {
        const detail::Token t = detail::nextToken(s, i);
        if (!t.escape) out.append(s.substr(i, t.len));
        i += t.len;
    }
    return out;
}

// 外部来源的文本（交易所错误信息、配置里的标签等）在显示或写日志前先清洗：C0 控制字符（含 ESC、换行、制表符）、
// DEL 与 UTF-8 编码的 C1 控制字符（U+0080-U+009F，部分终端把 U+009B 当作 CSI）替换为空格，非法 UTF-8 字节替换为 '?'。
// 结果是合法的 UTF-8，不会向终端注入转义序列，也不会用换行破坏单行布局
// （ESC 变成空格后，转义序列的其余字节作为普通可见文本保留）。
inline std::string sanitize(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size();) {
        const unsigned char b = static_cast<unsigned char>(s[i]);
        if (b < 0x80) {
            out.push_back(b < 0x20 || b == 0x7F ? ' ' : static_cast<char>(b));
            ++i;
            continue;
        }
        const detail::Token t = detail::nextToken(s, i);
        if (t.len == 1) {
            out.push_back('?');
        } else if (b == 0xC2 && static_cast<unsigned char>(s[i + 1]) <= 0x9F) {
            out.push_back(' ');
        } else {
            out.append(s.substr(i, t.len));
        }
        i += t.len;
    }
    return out;
}

// 把字符串 s 重复 n 次（如 repeat("─", 43)）
inline std::string repeat(std::string_view s, std::size_t n) {
    std::string out;
    out.reserve(s.size() * n);
    for (std::size_t i = 0; i < n; ++i) out.append(s);
    return out;
}

// 截断到最多 width 列。被截掉的部分里的完整转义序列全部保留（宽度为 0），所以颜色的复位码不会丢失。
// 发生截断且 ellipsis 不比 width 宽时，在截断处放 ellipsis（如 "…"），总宽度仍不超过 width。
// 结尾的悬空转义序列被丢弃。
inline std::string truncateVisible(std::string_view s, std::size_t width, std::string_view ellipsis = {}) {
    const detail::Measure m = detail::measure(s);
    s = s.substr(0, m.keep);
    if (m.width <= width) return std::string(s);
    std::size_t ell_width = visibleWidth(ellipsis);
    if (ell_width > width) {
        ellipsis = {};
        ell_width = 0;
    }
    const std::size_t target = width - ell_width;
    std::string out;
    out.reserve(s.size() + ellipsis.size());
    std::size_t used = 0;
    bool cut = false;
    for (std::size_t i = 0; i < s.size();) {
        const detail::Token t = detail::nextToken(s, i);
        if (t.escape) {
            out.append(s.substr(i, t.len));
        } else if (!cut) {
            if (used + t.width <= target) {
                out.append(s.substr(i, t.len));
                used += t.width;
            } else {
                cut = true;
                out.append(ellipsis);
            }
        }
        i += t.len;
    }
    // 总宽度 > width >= target，必然在某处发生了截断，省略号已经放好。
    // 截掉中间的字符可能让截断点之前被打断的转义序列变成悬空：再去一次结尾
    out.resize(detail::measure(out).keep);
    return out;
}

// 右侧补空格到至少 width 列（已足够宽时不截断）；结尾的悬空转义序列被丢弃
inline std::string padRight(std::string_view s, std::size_t width) {
    const detail::Measure m = detail::measure(s);
    std::string out(s.substr(0, m.keep));
    if (m.width < width) out.append(width - m.width, ' ');
    return out;
}

// 左侧补空格到至少 width 列（右对齐）；结尾的悬空转义序列被丢弃
inline std::string padLeft(std::string_view s, std::size_t width) {
    const detail::Measure m = detail::measure(s);
    std::string out;
    if (m.width < width) out.assign(width - m.width, ' ');
    out.append(s.substr(0, m.keep));
    return out;
}

// 居中到恰好 width 列：多出的一个空格放在右侧；过宽时截断
inline std::string center(std::string_view s, std::size_t width) {
    const detail::Measure m = detail::measure(s);
    if (m.width >= width) return truncateVisible(s, width);
    const std::size_t left = (width - m.width) / 2;
    std::string out(left, ' ');
    out.append(s.substr(0, m.keep));
    out.append(width - m.width - left, ' ');
    return out;
}

// 恰好 width 列：过宽截断（可选省略号），不足补空格
inline std::string fit(std::string_view s, std::size_t width, std::string_view ellipsis = {}) {
    return padRight(truncateVisible(s, width, ellipsis), width);
}

}  // namespace text_util
