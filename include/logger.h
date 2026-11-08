//
// logger.h
// 日志模块（线程安全）：按日期写日志文件，记录启动信息、行情源连接事件、套利信号、价差、延迟统计与最终报告。
// Logging module (thread-safe): writes log files by date, recording startup information, feed connection
// events, arbitrage signals, spreads, latency statistics and the final report.
//

#pragma once

#include "arbitrage_detector.h"
#include "display.h"
#include "latency_tracker.h"
#include "text_util.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

// 文件日志器：
// - 日志文件 <log_dir>/arb_<YYYY-MM-DD>.log（启动日期），追加写入；目录不存在时创建（mkdir 0755）。
// - 线程安全：所有写入在同一把互斥锁内完成，行情线程的连接事件（logFeed）与主线程的信号日志不会交错在一行里。
// - 每行 "<YYYY-MM-DD HH:MM:SS.mmm> [TAG] message"；消息中的控制字符（换行、ESC 等）被替换为空格，一条消息永远是一行。
// - 刷新策略：INFO / WARN / FEED 立即刷新（少见且对排查问题重要）；SIGNAL / SPREAD / PERF 由调用方定期 flush()
//   （主循环每个 tick，即每 refresh_ms 调用一次），避免高频信号时每行一次系统调用。
// - 可选的控制台回显（setEcho，用于 --headless）：INFO / WARN / FEED / SIGNAL 行同时写到给定的 FILE*，
//   整行一次 fwrite，与其他线程整行写 stdout 的输出不会交错在一行内。
// File logger:
// - Log file <log_dir>/arb_<YYYY-MM-DD>.log (the start date), written in append mode; the directory is
//   created if it does not exist (mkdir 0755).
// - Thread-safe: every write happens under the same mutex, so connection events from the feed threads
//   (logFeed) and the main thread's signal logs never interleave within a line.
// - Each line is "<YYYY-MM-DD HH:MM:SS.mmm> [TAG] message"; control characters in a message (newline, ESC
//   etc.) are replaced by spaces, and a message is always a single line.
// - Flush policy: INFO / WARN / FEED are flushed immediately (rare and important for troubleshooting);
//   SIGNAL / SPREAD / PERF wait for the caller's periodic flush() (called by the main loop every tick,
//   i.e. once per refresh_ms), avoiding one system call per line when signals are frequent.
// - Optional console echo (setEcho, used for --headless): INFO / WARN / FEED / SIGNAL lines are also
//   written to the given FILE*, each whole line with one fwrite, so they do not interleave within a line
//   with output that other threads write to stdout as whole lines.
class Logger {
public:
    // log_dir：日志目录；announce：是否在 stdout 打印 "[Logger] Log file: <path>"
    // log_dir: the log directory; announce: whether to print "[Logger] Log file: <path>" to stdout
    explicit Logger(const std::string& log_dir = "logs", bool announce = true) {
        if (::mkdir(log_dir.c_str(), 0755) != 0 && errno != EEXIST) {
            std::cerr << "[Logger] Cannot create log directory " << log_dir << ": " << std::strerror(errno)
                      << std::endl;
        }
        path_ = log_dir + "/arb_" + now().date + ".log";
        file_.open(path_, std::ios::app);
        if (!file_.is_open()) {
            std::cerr << "[Logger] Failed to open log file: " << path_ << std::endl;
            return;
        }
        log(std::string("=== ") + kAppVersion + " Started ===");
        log("Log file: " + path_);
        if (announce) std::cout << "[Logger] Log file: " << path_ << std::endl;
    }

    // 析构时写入停止信息
    // Writes the stop message on destruction
    ~Logger() {
        if (file_.is_open()) {
            log(std::string("=== ") + kAppVersion + " Stopped ===");
            file_.close();
        }
    }

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    bool isOpen() const noexcept { return file_.is_open(); }
    const std::string& path() const noexcept { return path_; }

    // 控制台回显目标（nullptr 关闭）
    // Console echo target (nullptr turns it off)
    void setEcho(std::FILE* out) {
        std::lock_guard<std::mutex> lock(mutex_);
        echo_ = out;
    }

    // 通用文本日志
    // General text log
    void log(const std::string& message) { write("INFO", message, true, true); }

    // 告警（如配置告警）
    // Warning (e.g. config warnings)
    void warn(const std::string& message) { write("WARN", message, true, true); }

    // 行情源连接事件（feed 线程上的日志回调，参数已是 "[name] message"）
    // Feed connection event (the log callback on a feed thread; the argument is already "[name] message")
    void logFeed(const std::string& message) { write("FEED", message, true, true); }

    // 套利信号
    // Arbitrage signal
    void logSignal(const ArbitrageSignal& s) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(4);
        ss << "SIGNAL | Buy " << s.buy_exchange << " @ " << s.buy_price << " | Sell " << s.sell_exchange << " @ "
           << s.sell_price << " | Qty: " << s.quantity << " | VWAP: " << s.vwap_buy << " / " << s.vwap_sell
           << " | Gross: $" << s.gross_profit << " | Fees: $" << s.fees << " | Net: $" << s.net_profit
           << " | Levels: " << s.levels_used;
        write("SIGNAL", ss.str(), true, false);
    }

    // 价差：exchange_a 的卖一与 exchange_b 的买一（在 a 买、在 b 卖），diff = bid_b - ask_a
    // Spread: exchange_a's best ask and exchange_b's best bid (buy on a, sell on b), diff = bid_b - ask_a
    void logSpread(const std::string& exchange_a, double ask_a, const std::string& exchange_b, double bid_b,
                   double diff) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "SPREAD | " << exchange_a << " Ask: " << ask_a << " | " << exchange_b << " Bid: " << bid_b
           << " | Diff: $" << diff;
        write("SPREAD", ss.str(), false, false);
    }

    // 最终报告：每个交易所的处理 / 队列 / 交易所延迟与连接统计，每个有序交易所对的信号汇总，以及总计
    // Final report: each venue's processing / queue / exchange latency and connection statistics, the
    // signal summary of every ordered venue pair, and the totals
    void logFinalReport(const std::vector<VenueView>& venues, const ArbitrageDetector& detector) {
        write("INFO", "=== Final Report ===", false, false);
        for (const VenueView& v : venues) {
            if (v.processing != nullptr) {
                write("PERF", latencyLine("LATENCY", *v.processing, v.name), false, false);
            }
            if (v.queue != nullptr) write("PERF", latencyLine("QUEUE", *v.queue, v.name), false, false);
            if (v.exchange != nullptr) {
                const LatencyTracker& e = *v.exchange;
                std::ostringstream ss;
                ss << std::fixed << std::setprecision(1) << "EXCHANGE | " << v.name << " | Samples: " << e.count()
                   << " | P50: " << static_cast<double>(e.percentileNs(0.50)) / 1e6
                   << "ms | P99: " << static_cast<double>(e.percentileNs(0.99)) / 1e6 << "ms";
                write("PERF", ss.str(), false, false);
            }
            std::string feed = "FEED | " + v.name + " | Status: " + feedStatusName(v.status);
            for (const FeedCounter& c : kFeedCounters) {
                feed += std::string(" | ") + c.log_label + ": " + std::to_string(v.stats.*c.field);
            }
            write("FEED", feed, false, false);
            if (!v.last_error.empty()) write("FEED", "FEED | " + v.name + " | Last error: " + v.last_error, false, false);
        }
        const std::size_t n = std::min(venues.size(), detector.venueCount());
        for (std::size_t buy = 0; buy < n; ++buy) {
            for (std::size_t sell = 0; sell < n; ++sell) {
                if (buy == sell) continue;
                const ArbitrageDetector::PairState& st = detector.pairState(buy, sell);
                std::ostringstream ss;
                ss << std::fixed << std::setprecision(2) << "PAIR | " << venues[buy].name << " -> " << venues[sell].name
                   << " | Signals: " << st.signals << " | Net: $" << st.net_total << " | Fees: $" << st.fees_total
                   << " | Best: $" << st.best_net;
                write("SUMMARY", ss.str(), false, false);
            }
        }
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << "SUMMARY | Signals: " << detector.getTotalSignals()
           << " | Net Profit: $" << detector.getTotalNetProfit() << " | Fees: $" << detector.getTotalFees()
           << " | Qty: " << std::setprecision(4) << detector.getTotalQty();
        write("SUMMARY", ss.str(), false, true);
    }

    // 强制刷新缓冲区
    // Forces the buffer to be flushed
    void flush() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) file_.flush();
    }

private:
    // 写一行：清洗在锁外完成；时间戳在锁内读取，保证文件中各行（来自不同线程）的时间戳单调不减
    // Writes one line: sanitizing is done outside the lock; the timestamp is taken inside the lock, which
    // keeps the timestamps of the lines in the file (from different threads) monotonically non-decreasing
    void write(const char* tag, const std::string& message, bool echo, bool flush_now) {
        const std::string clean = text_util::sanitize(message);
        std::lock_guard<std::mutex> lock(mutex_);
        const Stamp stamp = now();
        const std::string line = stamp.date + " " + stamp.time + " [" + tag + "] " + clean + "\n";
        if (file_.is_open()) {
            file_.write(line.data(), static_cast<std::streamsize>(line.size()));
            if (flush_now) file_.flush();
        }
        if (echo && echo_ != nullptr) {
            const std::string console = stamp.time + " [" + tag + "] " + clean + "\n";
            std::fwrite(console.data(), 1, console.size(), echo_);
            std::fflush(echo_);
        }
    }

    static std::string latencyLine(const char* kind, const LatencyTracker& t, const std::string& name) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1) << kind << " | " << name
           << " | Msgs: " << t.count() << " | Avg: " << t.meanNs() / 1000.0
           << "µs | P50: " << static_cast<double>(t.percentileNs(0.50)) / 1000.0
           << "µs | P95: " << static_cast<double>(t.percentileNs(0.95)) / 1000.0
           << "µs | P99: " << static_cast<double>(t.percentileNs(0.99)) / 1000.0
           << "µs | Max: " << static_cast<double>(t.maxNs()) / 1000.0 << "µs";
        return ss.str();
    }

    struct Stamp {
        std::string date;  // YYYY-MM-DD
        std::string time;  // HH:MM:SS.mmm
    };

    // 当前本地时间（localtime_r：线程安全）
    // Current local time (localtime_r: thread-safe)
    static Stamp now() {
        const auto tp = std::chrono::system_clock::now();
        const std::time_t t = std::chrono::system_clock::to_time_t(tp);
        const long long ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count() % 1000;
        std::tm tm{};
        localtime_r(&t, &tm);
        char date[16], time[24];
        std::strftime(date, sizeof(date), "%Y-%m-%d", &tm);
        const std::size_t n = std::strftime(time, sizeof(time), "%H:%M:%S", &tm);
        std::snprintf(time + n, sizeof(time) - n, ".%03lld", ms < 0 ? 0 : ms);
        return Stamp{date, time};
    }

    std::string path_;
    std::mutex mutex_;          // 保护 file_ 与 echo_
                                // guards file_ and echo_
    std::ofstream file_;
    std::FILE* echo_ = nullptr;
};
