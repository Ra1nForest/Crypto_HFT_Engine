//
// display.h
// 终端 UI 渲染：N 个交易所的订单簿、所有有序交易所对的套利监控、最近信号与行情源性能；整帧拼成一个字符串一次写出。
//

#pragma once

#include "arbitrage_detector.h"
#include "latency_tracker.h"
#include "market_data_feed.h"
#include "orderbook.h"
#include "text_util.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <string>
#include <string_view>
#include <vector>

// 程序名与版本（标题栏、启动横幅与日志共用）
inline constexpr const char* kAppVersion = "Crypto HFT Engine v2.0";

// 一个交易所在某一时刻的展示数据（主线程每次刷新时从 feed 与本地簿收集；指针可为空 = 无数据）
struct VenueView {
    std::string name;                            // 显示名称，如 "Binance"
    const OrderBook* book = nullptr;             // 主线程的本地簿（标签 "<name>:<symbol>"）
    FeedStatus status = FeedStatus::Idle;
    FeedStats stats;
    std::string last_error;                      // 只在非 LIVE 时显示
    const LatencyTracker* processing = nullptr;  // 处理延迟：读取返回 → 快照入队（feed 线程）
    const LatencyTracker* exchange = nullptr;    // 交易所延迟：本地墙钟 - 交易所时间戳
    const LatencyTracker* queue = nullptr;       // 队列延迟：快照入队 → 主线程取出
    double fee_rate = 0.0;                       // 该交易所生效的单边手续费
};

// 渲染参数
struct FrameOptions {
    int levels = 5;                 // 每个订单簿显示的档位数（1..20）
    int recent_signals = 5;         // 显示的最近信号条数
    int max_rows = 0;               // 终端高度（0 = 不限）：帧更高时逐步精简，见 Display::renderFrame
    bool color = true;              // false：不输出任何 ANSI 颜色（NO_COLOR）
    double uptime_s = 0.0;
    std::size_t config_warnings = 0;
    std::string footer = "Press Ctrl+C to exit";
};

namespace display_detail {

// 颜色（color = false 时全部为空串）
struct Palette {
    const char* green = "";
    const char* red = "";
    const char* yellow = "";
    const char* cyan = "";
    const char* bold = "";
    const char* dim = "";
    const char* reset = "";

    static Palette make(bool color) {
        Palette p;
        if (color) {
            p.green = "\033[32m";
            p.red = "\033[31m";
            p.yellow = "\033[33m";
            p.cyan = "\033[36m";
            p.bold = "\033[1m";
            p.dim = "\033[2m";
            p.reset = "\033[0m";
        }
        return p;
    }

    // 给 text 加颜色（code 为空时原样返回）
    std::string paint(const std::string& text, const char* code) const {
        if (code == nullptr || code[0] == '\0') return text;
        return std::string(code) + text + reset;
    }
};

}  // namespace display_detail

// 纯静态显示类：不持有状态。
// - renderFrame() 返回整帧文本：每行恰好 kFrameWidth 个可见列（用 text_util 按 UTF-8 码点计宽、跳过 ANSI 颜色），
//   只含 SGR 颜色序列与换行，不含光标控制；present() 加上归位 / 行尾擦除后一次 fwrite 写出（不清屏，减少闪烁）。
// - 布局：标题栏；订单簿两个一行（43 列内宽的框）；套利监控（每个有序交易所对一行 + 汇总 + 最近信号）；
//   行情源性能（状态、消息数、处理延迟 p50/p99 µs、交易所延迟 p50/p99 ms、重连、缺口/校验失败/重同步、丢弃）。
// - 空簿（断线时 feed 会发布空簿）表示"无数据"：价格与差价显示 "--"。
// - 终端宽度至少需要 kFrameWidth 列；高度不够时见 FrameOptions::max_rows。
class Display {
public:
    static constexpr std::size_t kFrameWidth = 93;     // 每行可见宽度：2 列缩进 + 91 列
    static constexpr std::size_t kIndent = 2;
    static constexpr std::size_t kBookInner = 43;      // 订单簿框内宽（两个框 + 1 列间隔 = 91）
    static constexpr std::size_t kWideInner = 89;      // 通栏框内宽
    static constexpr std::size_t kLatencyInner = 42;   // 最终报告延迟表内宽（两个表 + 3 列间隔 = 91）
    static constexpr const char* kHideCursor = "\033[?25l";
    static constexpr const char* kShowCursor = "\033[?25h";

    // ---------------- 实时画面 ----------------

    // 帧高于 max_rows（终端高度）时逐步精简：档位减到 3 → 最近信号减到 1 条 → 档位减到 1 → 去掉最近信号区 →
    // 去掉错误行与区块之间的空行；仍然放不下时只保留前 max_rows - 1 行，最后一行提示终端太小（画面不会滚动错位）。
    static std::string renderFrame(const std::vector<VenueView>& venues, const ArbitrageDetector& detector,
                                   const FrameOptions& opt) {
        Layout lay;
        lay.levels = std::clamp(opt.levels, 1, BookSnapshot::kMaxLevels);
        lay.recent = std::clamp(opt.recent_signals, 0, 50);
        std::string frame = buildFrame(venues, detector, opt, lay);
        if (opt.max_rows <= 0) return frame;
        const std::size_t max_rows = static_cast<std::size_t>(opt.max_rows);
        while (countLines(frame) > max_rows) {
            if (lay.levels > 3) {
                --lay.levels;
            } else if (lay.recent > 1) {
                --lay.recent;
            } else if (lay.levels > 1) {
                --lay.levels;
            } else if (lay.recent > 0) {
                lay.recent = 0;
            } else if (lay.details) {
                lay.details = false;
            } else {
                return cutToRows(frame, max_rows);
            }
            frame = buildFrame(venues, detector, opt, lay);
        }
        return frame;
    }

    // 把一帧写到 out：光标归位（首帧先清屏），每行后擦除到行尾，最后擦除帧下方的残留；一次 fwrite。
    // max_cols > 0（终端宽度）且小于帧宽时把每行截断到终端宽度：右侧被裁掉，但不会折行打乱整个画面。
    // - 最后一行之后不换行：帧可以占满终端的全部 max_rows 行而不引起滚动（下一帧从光标归位开始）。
    // - 占满整行的行不再发擦除到行尾：光标停在最后一列时 "\033[K" 会把该列（右边框）也擦掉。
    static void present(const std::string& frame, bool clear_screen, std::size_t max_cols = 0,
                        std::FILE* out_file = stdout) {
        std::string out;
        out.reserve(frame.size() + 8 * countLines(frame) + 16);
        out += clear_screen ? "\033[2J\033[H" : "\033[H";
        const bool clip = max_cols > 0 && max_cols < kFrameWidth;
        std::size_t start = 0;
        while (start < frame.size()) {
            std::size_t nl = frame.find('\n', start);
            if (nl == std::string::npos) nl = frame.size();
            const std::string_view line(frame.data() + start, nl - start);
            std::size_t width = 0;
            if (clip) {
                const std::string cut = text_util::truncateVisible(line, max_cols);
                width = text_util::visibleWidth(cut);
                out += cut;
            } else {
                width = text_util::visibleWidth(line);
                out.append(line.data(), line.size());
            }
            if (max_cols == 0 || width < max_cols) out += "\033[K";
            start = nl + 1;
            if (start < frame.size()) out += '\n';
        }
        out += "\033[J";
        std::fwrite(out.data(), 1, out.size(), out_file);
        std::fflush(out_file);
    }

    // ---------------- 最终报告 ----------------

    // 延迟表（每行两个）、行情源统计表、套利汇总
    static std::string renderFinalReport(const std::vector<VenueView>& venues, const ArbitrageDetector& detector,
                                         bool color) {
        using namespace text_util;
        const display_detail::Palette pal = display_detail::Palette::make(color);
        std::string out;
        out += "\n" + pal.paint("═══ Final Report ═══", pal.bold) + "\n\n";

        // 延迟表：两个一行
        for (std::size_t i = 0; i < venues.size(); i += 2) {
            std::vector<std::string> left = latencyTable(venues[i], pal);
            std::vector<std::string> right;
            if (i + 1 < venues.size()) right = latencyTable(venues[i + 1], pal);
            const std::size_t rows = std::max(left.size(), right.size());
            for (std::size_t r = 0; r < rows; ++r) {
                std::string line(kIndent, ' ');
                line += r < left.size() ? left[r] : std::string(kLatencyInner + 2, ' ');
                if (!right.empty()) {
                    line += "   ";
                    line += r < right.size() ? right[r] : std::string(kLatencyInner + 2, ' ');
                }
                out += line + "\n";
            }
            out += "\n";
        }
        if (venues.empty()) out += "  (no venues)\n\n";

        out += feedStatsTable(venues, pal);
        out += "\n";
        out += arbitrageSummary(venues, detector, pal);
        return out;
    }

    // ---------------- 格式化工具（公开以便测试）----------------

    // 以 precision 位小数格式化并右对齐到 width 列；放不下时逐步减少小数位，再改用 %g，最后用 '#' 填满。
    // 非有限值显示 "--"。show_plus：正数带 '+'。
    static std::string formatNumber(double v, int precision, std::size_t width, bool show_plus = false) {
        using text_util::padLeft;
        if (!std::isfinite(v)) return padLeft("--", width);
        char buf[64];
        for (int p = std::clamp(precision, 0, 12); p >= 0; --p) {
            const int n = show_plus ? std::snprintf(buf, sizeof(buf), "%+.*f", p, v)
                                    : std::snprintf(buf, sizeof(buf), "%.*f", p, v);
            if (n > 0 && static_cast<std::size_t>(n) <= width) return padLeft(buf, width);
        }
        for (int sig = 6; sig >= 1; --sig) {
            const int n = show_plus ? std::snprintf(buf, sizeof(buf), "%+.*g", sig, v)
                                    : std::snprintf(buf, sizeof(buf), "%.*g", sig, v);
            if (n > 0 && static_cast<std::size_t>(n) <= width) return padLeft(buf, width);
        }
        return std::string(width, '#');
    }

    // 价格的小数位：至少 5 位有效数字，2..8 位小数（67000 → 2，2.5 → 4，0.12 → 5）
    static int pricePrecision(double price) noexcept {
        const double a = std::fabs(price);
        if (!(a > 0.0) || !std::isfinite(a)) return 2;
        const int magnitude = static_cast<int>(std::floor(std::log10(a)));
        return std::clamp(4 - magnitude, 2, 8);
    }

    // 状态的颜色：LIVE 绿；CONNECTING / SYNCING / RECONNECTING 黄；STOPPED 红；IDLE 暗
    static const char* statusColor(FeedStatus s, const display_detail::Palette& pal) noexcept {
        switch (s) {
            case FeedStatus::Live: return pal.green;
            case FeedStatus::Connecting:
            case FeedStatus::Syncing:
            case FeedStatus::Reconnecting: return pal.yellow;
            case FeedStatus::Stopped: return pal.red;
            case FeedStatus::Idle: return pal.dim;
        }
        return pal.dim;
    }

    // 延迟（纳秒）→ "12.3"（微秒一位小数）
    static std::string formatUs(double ns) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.1f", ns / 1000.0);
        return buf;
    }

    // HH:MM:SS（本地时间）
    static std::string formatClock(long long timestamp_ms) {
        const std::time_t t = static_cast<std::time_t>(timestamp_ms / 1000);
        std::tm tm{};
        localtime_r(&t, &tm);
        char buf[16];
        std::strftime(buf, sizeof(buf), "%H:%M:%S", &tm);
        return buf;
    }

    // 时长 → HH:MM:SS
    static std::string formatDuration(double seconds) {
        long long s = seconds > 0.0 ? static_cast<long long>(seconds) : 0;
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%02lld:%02lld:%02lld", s / 3600, (s / 60) % 60, s % 60);
        return buf;
    }

private:
    using Palette = display_detail::Palette;

    // 帧的精简程度（见 renderFrame）
    struct Layout {
        int levels = 5;        // 每个订单簿的档位数
        int recent = 5;        // 最近信号行数；0 = 不显示最近信号区
        bool details = true;   // 错误行与区块之间的空行
    };

    static std::size_t countLines(const std::string& s) {
        return static_cast<std::size_t>(std::count(s.begin(), s.end(), '\n'));
    }

    // 只保留前 rows - 1 行，最后一行提示终端太小（每行仍是 kFrameWidth 列）
    static std::string cutToRows(const std::string& frame, std::size_t rows) {
        const std::size_t total = countLines(frame);
        std::string out;
        std::size_t pos = 0;
        for (std::size_t i = 0; i + 1 < rows; ++i) {
            const std::size_t nl = frame.find('\n', pos);
            if (nl == std::string::npos) break;
            out.append(frame, pos, nl + 1 - pos);
            pos = nl + 1;
        }
        const std::string notice = "  ... terminal too small: this view needs " + std::to_string(total) + " rows, " +
                                   std::to_string(rows) + " available";
        out += text_util::fit(notice, kFrameWidth, "…");
        out += '\n';
        return out;
    }

    // 横线：left + fill × inner + right
    static std::string rule(const char* left, const char* right, std::size_t inner, const char* fill = "─") {
        return std::string(left) + text_util::repeat(fill, inner) + right;
    }

    // 带标题的横线："┌─ title ─────┐"（标题过长时截断）
    static std::string titledRule(const char* left, const char* right, const std::string& title, std::size_t inner,
                                  const char* fill = "─") {
        using namespace text_util;
        std::string content = std::string(fill) + " " + title + " ";
        content = truncateVisible(content, inner);
        const std::size_t w = visibleWidth(content);
        return std::string(left) + content + repeat(fill, inner - w) + right;
    }

    // 框内一行："│" + 恰好 inner 列 + "│"
    static std::string row(const std::string& content, std::size_t inner, const char* border = "│") {
        return std::string(border) + text_util::fit(content, inner, "…") + border;
    }

    // 右对齐到恰好 width 列（过宽时截断，表格的列宽永远不变）
    static std::string cell(const std::string& s, std::size_t width) {
        return text_util::padLeft(text_util::truncateVisible(s, width, "…"), width);
    }

    static std::string plainNumber(double v, int precision) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.*f", precision, v);
        return buf;
    }

    static std::string fmtCount(std::uint64_t n) { return std::to_string(n); }

    static std::string statusText(FeedStatus s, const Palette& pal) {
        return pal.paint(feedStatusName(s), statusColor(s, pal));
    }

    static std::string venueName(const std::vector<VenueView>& venues, std::size_t i, const std::string& fallback) {
        return i < venues.size() ? venues[i].name : fallback;
    }

    static bool hasData(const OrderBook* book) { return book != nullptr && !book->empty(); }

    // ---------------- 帧 ----------------

    static std::string buildFrame(const std::vector<VenueView>& venues, const ArbitrageDetector& detector,
                                  const FrameOptions& opt, const Layout& lay) {
        const int levels = lay.levels;
        const Palette pal = Palette::make(opt.color);
        std::vector<std::string> lines;
        lines.reserve(64);
        const std::string indent(kIndent, ' ');

        // 标题栏："Crypto HFT Engine v2.0  |  3 venues, 2 live  |  uptime 00:01:23"（居中）
        {
            std::size_t live = 0;
            for (const VenueView& v : venues) live += v.status == FeedStatus::Live ? 1 : 0;
            const std::string subtitle = "  |  " + std::to_string(venues.size()) +
                                         (venues.size() == 1 ? " venue, " : " venues, ") + std::to_string(live) +
                                         " live  |  uptime " + formatDuration(opt.uptime_s);
            const std::string title = pal.paint(kAppVersion, pal.bold) + pal.paint(subtitle, pal.dim);
            lines.push_back(indent + rule("╔", "╗", kWideInner, "═"));
            lines.push_back(indent + row(text_util::center(title, kWideInner), kWideInner, "║"));
            lines.push_back(indent + rule("╚", "╝", kWideInner, "═"));
            if (lay.details) lines.push_back("");
        }

        // 订单簿：两个一行
        for (std::size_t i = 0; i < venues.size(); i += 2) {
            const std::vector<std::string> left = bookBox(venues[i], levels, pal);
            std::vector<std::string> right;
            if (i + 1 < venues.size()) right = bookBox(venues[i + 1], levels, pal);
            for (std::size_t r = 0; r < left.size(); ++r) {
                std::string line = indent + left[r];
                if (r < right.size()) line += " " + right[r];
                lines.push_back(line);
            }
        }
        if (!venues.empty() && lay.details) lines.push_back("");

        arbitragePanel(lines, venues, detector, lay.recent, pal);
        if (lay.details) lines.push_back("");
        performancePanel(lines, venues, lay.details, pal);
        if (lay.details) lines.push_back("");

        std::string footer = indent + opt.footer;
        if (opt.config_warnings > 0) {
            footer += "  |  " + pal.paint(std::to_string(opt.config_warnings) + " config warning" +
                                              (opt.config_warnings == 1 ? "" : "s") + " (see the log)",
                                          pal.yellow);
        }
        lines.push_back(footer);

        std::string frame;
        frame.reserve(lines.size() * (kFrameWidth + 48));
        for (const std::string& l : lines) {
            frame += text_util::fit(l, kFrameWidth);
            frame += '\n';
        }
        return frame;
    }

    // 数量的紧凑表示（去掉多余的 0）："0.5"、"2"
    static std::string trimmed(double v) {
        std::string s = plainNumber(v, 8);
        if (s.find('.') != std::string::npos) {
            while (!s.empty() && s.back() == '0') s.pop_back();
            if (!s.empty() && s.back() == '.') s.pop_back();
        }
        return s;
    }

    // 订单簿框（每行 kBookInner + 2 列）：
    //   ┌─ Binance:BTCUSDT  LIVE ───────────┐
    //   │      Qty        Bid │        Ask      Qty │
    //   ├───────────────────────────────────┤
    //   │   0.5000   67000.00 │   67000.01   0.2500 │   × levels
    //   └─ Spread 0.01  Mid 67000.005 ──────┘
    static std::vector<std::string> bookBox(const VenueView& v, int levels, const Palette& pal) {
        using namespace text_util;
        std::vector<std::string> out;
        const OrderBook* book = v.book;
        const std::string label = text_util::sanitize(book != nullptr && !book->getSymbol().empty() ? book->getSymbol() : v.name);
        out.push_back(titledRule("┌", "┐", pal.paint(label, pal.bold) + "  " + statusText(v.status, pal), kBookInner));
        out.push_back(row(" " + padLeft("Qty", 8) + " " + padLeft("Bid", 10) + " │ " + padLeft("Ask", 10) + " " +
                              padLeft("Qty", 8) + " ",
                          kBookInner));
        out.push_back(rule("├", "┤", kBookInner));

        const bool data = hasData(book);
        int prec = 2;
        if (data) prec = pricePrecision(!book->getBids().empty() ? book->getBids()[0].price : book->getAsks()[0].price);
        for (int i = 0; i < levels; ++i) {
            std::string content;
            if (!data) {
                if (i == 0) content = center(pal.paint("-- no data --", pal.dim), kBookInner);
            } else {
                const std::vector<PriceLevel>& bids = book->getBids();
                const std::vector<PriceLevel>& asks = book->getAsks();
                const std::size_t k = static_cast<std::size_t>(i);
                if (k < bids.size()) {
                    content += " " + formatNumber(bids[k].quantity, 4, 8) + " " +
                               pal.paint(formatNumber(bids[k].price, prec, 10), pal.green);
                } else {
                    content += std::string(20, ' ');
                }
                content += " │ ";
                if (k < asks.size()) {
                    content += pal.paint(formatNumber(asks[k].price, prec, 10), pal.red) + " " +
                               formatNumber(asks[k].quantity, 4, 8) + " ";
                } else {
                    content += std::string(20, ' ');
                }
            }
            out.push_back(row(content, kBookInner));
        }

        std::string foot = "Spread --  Mid --";
        if (data && !book->getBids().empty() && !book->getAsks().empty()) {
            foot = "Spread " + plainNumber(book->getSpread(), prec) + "  Mid " + plainNumber(book->getMid(), prec + 1);
        }
        out.push_back(titledRule("└", "┘", foot, kBookInner));
        return out;
    }

    // 套利监控：每个有序交易所对一行（当前最优价、原始差价、扣费后的扫单净利润与数量、信号数），
    // 汇总放在分隔线上，最后是最近的信号（新的在上）
    static void arbitragePanel(std::vector<std::string>& lines, const std::vector<VenueView>& venues,
                               const ArbitrageDetector& det, int recent, const Palette& pal) {
        using namespace text_util;
        const std::string indent(kIndent, ' ');
        // 标题带上扫单参数："Arbitrage Monitor  net of fees, min profit 0.01, ≤20 levels/side, no qty cap"
        const std::string params = "  net of fees, min profit " + trimmed(det.minProfit()) + ", " +
                                   (det.maxLevels() > 0 ? "≤" + std::to_string(det.maxLevels()) + " levels/side"
                                                        : std::string("all levels")) +
                                   ", " + (det.maxQty() > 0.0 ? "qty ≤" + trimmed(det.maxQty()) : std::string("no qty cap"));
        lines.push_back(indent + titledRule("┌", "┐", pal.paint("Arbitrage Monitor", pal.bold) + pal.paint(params, pal.dim),
                                            kWideInner));
        lines.push_back(indent + row("  " + padRight("Buy", 9) + padRight("Sell", 9) + padLeft("Best Ask", 12) +
                                         padLeft("Best Bid", 12) + padLeft("Diff", 11) + padLeft("Net", 13) +
                                         padLeft("Qty", 10) + padLeft("Signals", 9),
                                     kWideInner));
        const std::size_t n = std::min(venues.size(), det.venueCount());
        if (n < 2) {
            lines.push_back(indent + row(pal.paint("  needs at least two venues", pal.dim), kWideInner));
        }
        for (std::size_t buy = 0; buy < n; ++buy) {
            for (std::size_t sell = 0; sell < n; ++sell) {
                if (buy == sell) continue;
                lines.push_back(indent + row(pairRow(venues, det, buy, sell, pal), kWideInner));
            }
        }

        // 汇总（在分隔线上）
        std::string summary = "Signals " + pal.paint(fmtCount(det.getTotalSignals()), pal.bold) + "  Net profit " +
                              pal.paint(plainNumber(det.getTotalNetProfit(), 2),
                                        det.getTotalNetProfit() > 0.0 ? pal.green : "") +
                              "  Fees " + plainNumber(det.getTotalFees(), 2) + "  Volume " +
                              plainNumber(det.getTotalQty(), 4);
        lines.push_back(indent + titledRule("├", "┤", summary, kWideInner));

        // 最近的信号（recent == 0 时整个区域省略）
        if (recent > 0) {
            lines.push_back(indent + row("  " + padRight("Time", 10) + padRight("Pair", 18) + padLeft("VWAP Buy", 12) +
                                             padLeft("VWAP Sell", 13) + padLeft("Qty", 10) + padLeft("Net", 11) +
                                             padLeft("Lvls", 6),
                                         kWideInner));
        }
        const std::deque<ArbitrageSignal>& signals = det.getSignals();
        for (int k = 0; k < recent; ++k) {
            std::string content;
            const std::size_t idx = static_cast<std::size_t>(k);
            if (idx < signals.size()) {
                const ArbitrageSignal& s = signals[signals.size() - 1 - idx];
                const int prec = pricePrecision(s.buy_price);
                const std::string pair = text_util::sanitize(venueName(venues, s.buy_venue, s.buy_exchange) + " -> " +
                                                             venueName(venues, s.sell_venue, s.sell_exchange));
                content = "  " + padRight(formatClock(s.timestamp_ms), 10) + fit(pair, 18) +
                          formatNumber(s.vwap_buy, prec, 12) + formatNumber(s.vwap_sell, prec, 13) +
                          formatNumber(s.quantity, 4, 10) +
                          pal.paint(formatNumber(s.net_profit, 2, 11), pal.green) + padLeft(std::to_string(s.levels_used), 6);
            } else if (k == 0) {
                content = pal.paint("  no signals yet", pal.dim);
            }
            lines.push_back(indent + row(content, kWideInner));
        }
        lines.push_back(indent + rule("└", "┘", kWideInner));
    }

    static std::string pairRow(const std::vector<VenueView>& venues, const ArbitrageDetector& det, std::size_t buy,
                               std::size_t sell, const Palette& pal) {
        using namespace text_util;
        const OrderBook* bb = venues[buy].book;
        const OrderBook* sb = venues[sell].book;
        const bool have_ask = bb != nullptr && !bb->getAsks().empty();
        const bool have_bid = sb != nullptr && !sb->getBids().empty();
        const double ask = have_ask ? bb->getAsks()[0].price : 0.0;
        const double bid = have_bid ? sb->getBids()[0].price : 0.0;
        const int prec = pricePrecision(have_ask ? ask : bid);
        std::string s = "  " + fit(text_util::sanitize(venues[buy].name), 9) + fit(text_util::sanitize(venues[sell].name), 9);
        s += have_ask ? formatNumber(ask, prec, 12) : padLeft("--", 12);
        s += have_bid ? formatNumber(bid, prec, 12) : padLeft("--", 12);
        if (have_ask && have_bid) {
            const double diff = bid - ask;
            s += pal.paint(formatNumber(diff, prec, 11, true), diff > 0.0 ? pal.green : pal.red);
        } else {
            s += padLeft("--", 11);
        }
        const ArbitrageDetector::PairState& st = det.pairState(buy, sell);
        if (st.has_data && have_ask && have_bid) {
            const char* c = st.sweep.net > det.minProfit() ? pal.green : (st.sweep.qty > 0.0 ? pal.yellow : pal.dim);
            s += pal.paint(formatNumber(st.sweep.net, 2, 13), c);
            s += formatNumber(st.sweep.qty, 4, 10);
        } else {
            s += padLeft("--", 13) + padLeft("--", 10);
        }
        s += padLeft(fmtCount(st.signals), 9);
        return s;
    }

    // 行情源性能：每个交易所一行；非 LIVE 且有错误时（errors = true）下一行显示最近的错误
    static void performancePanel(std::vector<std::string>& lines, const std::vector<VenueView>& venues, bool errors,
                                 const Palette& pal) {
        using namespace text_util;
        const std::string indent(kIndent, ' ');
        lines.push_back(indent + titledRule("┌", "┐",
                                            pal.paint("Feed Performance", pal.bold) +
                                                pal.paint("  processing µs, exchange latency ms", pal.dim),
                                            kWideInner));
        // 列宽：2 + 8 + 13 + 9 + 14 + 14 + 7 + 13 + 7 = 87（右侧留 2 列空白）
        lines.push_back(indent + row("  " + padRight("Venue", 8) + padRight("Status", 13) + padLeft("Msgs", 9) +
                                         padLeft("Proc p50/p99", 14) + padLeft("Exch p50/p99", 14) +
                                         padLeft("Reconn", 7) + padLeft("Gap/Cks/Rsy", 13) + padLeft("Drops", 7),
                                     kWideInner));
        if (venues.empty()) lines.push_back(indent + row(pal.paint("  no venues", pal.dim), kWideInner));
        for (const VenueView& v : venues) {
            std::string proc = "--";
            if (v.processing != nullptr && v.processing->count() > 0) {
                proc = formatUs(static_cast<double>(v.processing->percentileNs(0.50))) + "/" +
                       formatUs(static_cast<double>(v.processing->percentileNs(0.99)));
            }
            std::string exch = "--";
            if (v.exchange != nullptr && v.exchange->count() > 0) {
                exch = plainNumber(static_cast<double>(v.exchange->percentileNs(0.50)) / 1e6, 0) + "/" +
                       plainNumber(static_cast<double>(v.exchange->percentileNs(0.99)) / 1e6, 0);
            }
            const FeedStats& st = v.stats;
            const std::string integrity = fmtCount(st.gaps) + "/" + fmtCount(st.checksum_failures) + "/" + fmtCount(st.resyncs);
            lines.push_back(indent + row("  " + fit(text_util::sanitize(v.name), 8) + fit(statusText(v.status, pal), 13) +
                                             cell(fmtCount(st.messages), 9) + cell(proc, 14) + cell(exch, 14) +
                                             cell(fmtCount(st.reconnects), 7) + cell(integrity, 13) +
                                             cell(fmtCount(st.dropped_snapshots), 7),
                                         kWideInner));
            if (errors && v.status != FeedStatus::Live && !v.last_error.empty()) {
                lines.push_back(indent + row(pal.paint("    ! " + text_util::sanitize(v.last_error), pal.yellow), kWideInner));
            }
        }
        lines.push_back(indent + rule("└", "┘", kWideInner));
    }

    // ---------------- 最终报告的组成部分 ----------------

    // 统计行："  Avg:          12.3 µs"
    static std::string statRow(const std::string& label, const std::string& value, const std::string& unit) {
        return "  " + text_util::padRight(label, 18) + text_util::padLeft(value, 10) + (unit.empty() ? "" : " " + unit);
    }

    // 一个交易所的延迟表（每行 kLatencyInner + 2 列）
    static std::vector<std::string> latencyTable(const VenueView& v, const Palette& pal) {
        using namespace text_util;
        std::vector<std::string> out;
        out.push_back(rule("┌", "┐", kLatencyInner));
        out.push_back(row(center(pal.paint(v.name + " Latency", pal.bold), kLatencyInner), kLatencyInner));
        out.push_back(rule("├", "┤", kLatencyInner));
        const LatencyTracker* t = v.processing;
        const std::uint64_t n = t != nullptr ? t->count() : 0;
        out.push_back(row(statRow("Processed:", fmtCount(n), ""), kLatencyInner));
        auto us = [&](double ns) { return n > 0 ? formatUs(ns) : std::string("--"); };
        out.push_back(row(statRow("Avg:", us(n > 0 ? t->meanNs() : 0.0), "µs"), kLatencyInner));
        out.push_back(row(statRow("Min:", us(n > 0 ? static_cast<double>(t->minNs()) : 0.0), "µs"), kLatencyInner));
        out.push_back(row(statRow("P50:", us(n > 0 ? static_cast<double>(t->percentileNs(0.50)) : 0.0), "µs"),
                          kLatencyInner));
        out.push_back(row(statRow("P95:", us(n > 0 ? static_cast<double>(t->percentileNs(0.95)) : 0.0), "µs"),
                          kLatencyInner));
        out.push_back(row(statRow("P99:", us(n > 0 ? static_cast<double>(t->percentileNs(0.99)) : 0.0), "µs"),
                          kLatencyInner));
        out.push_back(row(statRow("Max:", us(n > 0 ? static_cast<double>(t->maxNs()) : 0.0), "µs"), kLatencyInner));
        out.push_back(row(std::string(kLatencyInner, '-'), kLatencyInner));
        for (const std::string& h : buildHistogramRows(t)) out.push_back(row(h, kLatencyInner));
        out.push_back(row(std::string(kLatencyInner, '-'), kLatencyInner));

        std::string queue = "--";
        if (v.queue != nullptr && v.queue->count() > 0) {
            queue = formatUs(static_cast<double>(v.queue->percentileNs(0.50))) + " / " +
                    formatUs(static_cast<double>(v.queue->percentileNs(0.99)));
        }
        out.push_back(row(statRow("Queue p50/p99:", queue, "µs"), kLatencyInner));
        std::string exch = "--";
        if (v.exchange != nullptr && v.exchange->count() > 0) {
            exch = plainNumber(static_cast<double>(v.exchange->percentileNs(0.50)) / 1e6, 0) + " / " +
                   plainNumber(static_cast<double>(v.exchange->percentileNs(0.99)) / 1e6, 0);
        }
        out.push_back(row(statRow("Exchange p50/p99:", exch, "ms"), kLatencyInner));
        out.push_back(rule("└", "┘", kLatencyInner));
        return out;
    }

    // 处理延迟分布（区间 0-5 / 5-10 / 10-20 / 20-50 / 50-100 / 100+ µs）：各区间占比由 fractionBelowNs
    // 在区间边界处的差值得到（覆盖全部样本）。无样本时只有一行 "No data"，但总行数固定，表格高度不变。
    static std::vector<std::string> buildHistogramRows(const LatencyTracker* lat) {
        static const std::uint64_t kBoundariesNs[] = {5000, 10000, 20000, 50000, 100000};
        static const char* const kLabels[] = {" 0-5   µs", " 5-10  µs", "10-20  µs", "20-50  µs", "50-100 µs", " 100+  µs"};
        constexpr std::size_t kBuckets = sizeof(kLabels) / sizeof(kLabels[0]);
        std::vector<std::string> rows;
        if (lat == nullptr || lat->count() == 0) {
            rows.push_back("  No data");
            while (rows.size() < kBuckets) rows.push_back("");
            return rows;
        }
        // 相邻边界的累计占比之差；行情线程并发写入时累计值可能轻微不单调，钳制为非负
        double fractions[kBuckets] = {};
        double below_prev = 0.0;
        for (std::size_t i = 0; i + 1 < kBuckets; ++i) {
            double below = lat->fractionBelowNs(kBoundariesNs[i]);
            if (below < below_prev) below = below_prev;
            fractions[i] = below - below_prev;
            below_prev = below;
        }
        fractions[kBuckets - 1] = below_prev < 1.0 ? 1.0 - below_prev : 0.0;
        double max_fraction = *std::max_element(fractions, fractions + kBuckets);
        if (max_fraction <= 0.0) max_fraction = 1.0;
        constexpr int kBarMax = 18;
        for (std::size_t i = 0; i < kBuckets; ++i) {
            const int bar = static_cast<int>(fractions[i] / max_fraction * kBarMax);
            char pct[16];
            std::snprintf(pct, sizeof(pct), "%5.1f%%", 100.0 * fractions[i]);
            rows.push_back(" " + std::string(kLabels[i]) + " " + std::string(static_cast<std::size_t>(bar), '#') +
                           std::string(static_cast<std::size_t>(kBarMax - bar), ' ') + " " + pct);
        }
        return rows;
    }

    // 行情源统计表：每行一个指标，每列一个交易所
    static std::string feedStatsTable(const std::vector<VenueView>& venues, const Palette& pal) {
        using namespace text_util;
        constexpr std::size_t kLabelW = 20, kColW = 14;
        const std::string indent(kIndent, ' ');
        auto border = [&](const char* l, const char* m, const char* r) {
            std::string s = indent + l + repeat("─", kLabelW);
            for (std::size_t i = 0; i < venues.size(); ++i) s += m + repeat("─", kColW);
            return s + r + "\n";
        };
        auto line = [&](const std::string& label, const std::vector<std::string>& cells) {
            std::string s = indent + "│" + fit(" " + label, kLabelW);
            for (const std::string& c : cells) s += "│" + cell(c, kColW - 1) + " ";
            return s + "│\n";
        };
        std::string out;
        out += border("┌", "┬", "┐");
        std::vector<std::string> names;
        for (const VenueView& v : venues) names.push_back(pal.paint(v.name, pal.bold));
        out += line("Feed statistics", names);
        out += border("├", "┼", "┤");
        auto metric = [&](const std::string& label, auto get) {
            std::vector<std::string> cells;
            for (const VenueView& v : venues) cells.push_back(get(v));
            out += line(label, cells);
        };
        metric("Status", [&](const VenueView& v) { return statusText(v.status, pal); });
        metric("Messages", [](const VenueView& v) { return fmtCount(v.stats.messages); });
        metric("Bytes", [](const VenueView& v) { return fmtCount(v.stats.bytes); });
        metric("Book updates", [](const VenueView& v) { return fmtCount(v.stats.book_updates); });
        metric("Published", [](const VenueView& v) { return fmtCount(v.stats.published); });
        metric("Dropped snapshots", [](const VenueView& v) { return fmtCount(v.stats.dropped_snapshots); });
        metric("Reconnects", [](const VenueView& v) { return fmtCount(v.stats.reconnects); });
        metric("Connect failures", [](const VenueView& v) { return fmtCount(v.stats.connect_failures); });
        metric("Stalls", [](const VenueView& v) { return fmtCount(v.stats.stalls); });
        metric("Gaps", [](const VenueView& v) { return fmtCount(v.stats.gaps); });
        metric("Checksum failures", [](const VenueView& v) { return fmtCount(v.stats.checksum_failures); });
        metric("Resyncs", [](const VenueView& v) { return fmtCount(v.stats.resyncs); });
        metric("REST snapshots", [](const VenueView& v) { return fmtCount(v.stats.rest_snapshots); });
        metric("Parse errors", [](const VenueView& v) { return fmtCount(v.stats.parse_errors); });
        metric("Fee rate", [](const VenueView& v) { return plainNumber(v.fee_rate * 100.0, 3) + "%"; });
        out += border("└", "┴", "┘");
        for (const VenueView& v : venues) {
            if (!v.last_error.empty()) out += indent + v.name + " last error: " + text_util::sanitize(v.last_error) + "\n";
        }
        return out;
    }

    // 套利汇总：每个有序交易所对一行 + 合计
    static std::string arbitrageSummary(const std::vector<VenueView>& venues, const ArbitrageDetector& det,
                                        const Palette& pal) {
        using namespace text_util;
        static const std::size_t kW[] = {20, 10, 14, 12, 12};
        const std::string indent(kIndent, ' ');
        auto border = [&](const char* l, const char* m, const char* r) {
            std::string s = indent + l;
            for (std::size_t i = 0; i < 5; ++i) s += (i > 0 ? m : "") + repeat("─", kW[i]);
            return s + r + "\n";
        };
        auto line = [&](const std::string& c0, const std::string& c1, const std::string& c2, const std::string& c3,
                        const std::string& c4) {
            return indent + "│" + fit(" " + c0, kW[0]) + "│" + cell(c1, kW[1] - 1) + " │" + cell(c2, kW[2] - 1) +
                   " │" + cell(c3, kW[3] - 1) + " │" + cell(c4, kW[4] - 1) + " │\n";
        };
        std::string out;
        out += border("┌", "┬", "┐");
        out += line(pal.paint("Arbitrage summary", pal.bold), "Signals", "Net profit", "Fees", "Best net");
        out += border("├", "┼", "┤");
        const std::size_t n = std::min(venues.size(), det.venueCount());
        double best = 0.0;
        for (std::size_t buy = 0; buy < n; ++buy) {
            for (std::size_t sell = 0; sell < n; ++sell) {
                if (buy == sell) continue;
                const ArbitrageDetector::PairState& st = det.pairState(buy, sell);
                best = std::max(best, st.best_net);
                out += line(venues[buy].name + " -> " + venues[sell].name, fmtCount(st.signals),
                            plainNumber(st.net_total, 2), plainNumber(st.fees_total, 2), plainNumber(st.best_net, 2));
            }
        }
        if (n < 2) out += line(pal.paint("needs two venues", pal.dim), "", "", "", "");
        out += border("├", "┼", "┤");
        out += line(pal.paint("Total", pal.bold), fmtCount(det.getTotalSignals()), plainNumber(det.getTotalNetProfit(), 2),
                    plainNumber(det.getTotalFees(), 2), plainNumber(best, 2));
        out += border("└", "┴", "┘");
        out += indent + "Min profit " + trimmed(det.minProfit()) + "  |  Max qty " +
               (det.maxQty() > 0.0 ? trimmed(det.maxQty()) : std::string("unlimited")) + "  |  Max levels " +
               (det.maxLevels() > 0 ? std::to_string(det.maxLevels()) : std::string("all")) + "  |  Volume " +
               plainNumber(det.getTotalQty(), 4) + "\n";
        return out;
    }
};
