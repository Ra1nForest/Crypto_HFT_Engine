//
// app.h
// 监控程序主体：命令行解析，按配置创建行情源与套利检测器，事件驱动主循环（排空快照队列 → 更新本地簿 → 套利检测 →
// 定时健康检查 / 渲染 / 价差日志），停止后输出最终报告。src/main.cpp 只负责信号处理并调用这里（测试直接驱动本类）。
//

#pragma once

#include "arbitrage_detector.h"
#include "book_snapshot.h"
#include "config.h"
#include "display.h"
#include "hft/timing.h"
#include "latency_tracker.h"
#include "logger.h"
#include "market_data_feed.h"
#include "orderbook.h"
#include "text_util.h"

#include <pthread.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// 命令行
// ---------------------------------------------------------------------------

struct CliOptions {
    std::string config_path = "config.json";  // --config <path>（相对当前目录）
    bool config_given = false;                // 显式给出了 --config：文件必须能读取并解析（见 main.cpp）
    bool headless = false;                    // --headless：不渲染终端界面，每秒一行状态
    double duration_s = 0.0;                  // --duration <seconds>：> 0 时到时自动停止；0 = 直到收到信号
    bool help = false;                        // --help / -h
};

// 用法说明（英文，--help 与参数错误时输出）
inline std::string usageText(const std::string& program) {
    const std::string prog = program.empty() ? std::string("crypto_hft_engine") : program;
    return std::string(kAppVersion) +
           " - cross-exchange arbitrage monitor (Binance, OKX, Bybit)\n"
           "\n"
           "Usage: " + prog + " [--config <path>] [--headless] [--duration <seconds>] [--help]\n"
           "\n"
           "Options:\n"
           "  --config <path>       configuration file (default: config.json in the current directory;\n"
           "                        an explicitly given file must exist and contain valid JSON)\n"
           "  --headless            no terminal UI: print feed events, signals and one status line per second\n"
           "  --duration <seconds>  stop automatically after this many seconds (default: run until Ctrl+C)\n"
           "  -h, --help            show this help and exit\n"
           "\n"
           "Ctrl+C or SIGTERM stops the monitor gracefully and prints the final report (a second signal exits\n"
           "immediately). Logs are appended to logs/arb_<date>.log under the current directory.\n";
}

namespace app_detail {

// "--name=value" 或 "--name value"：取出选项的值；缺失时返回 false
inline bool optionValue(int argc, const char* const* argv, int& i, const std::string& name, std::string& value,
                        std::string& error) {
    const std::string arg = argv[i];
    if (arg.size() > name.size() && arg.compare(0, name.size(), name) == 0 && arg[name.size()] == '=') {
        value = arg.substr(name.size() + 1);
    } else if (i + 1 < argc) {
        value = argv[++i];
    } else {
        error = name + " requires a value";
        return false;
    }
    if (value.empty()) {
        error = name + " requires a non-empty value";
        return false;
    }
    return true;
}

// 秒数：有限的正数，上限 1e9 秒（换算成纳秒不会溢出）
inline bool parseSeconds(const std::string& text, double& out) {
    const char* begin = text.c_str();
    char* end = nullptr;
    errno = 0;
    const double v = std::strtod(begin, &end);
    if (end == begin || *end != '\0' || errno == ERANGE || !std::isfinite(v) || !(v > 0.0) || v > 1e9) return false;
    out = v;
    return true;
}

}  // namespace app_detail

// 解析命令行；失败时返回 false 并在 error 中说明。选项可重复（后者覆盖前者），不接受位置参数。
inline bool parseCommandLine(int argc, const char* const* argv, CliOptions& out, std::string& error) {
    CliOptions opt;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i] != nullptr ? argv[i] : "";
        auto is = [&arg](const char* name) {
            const std::size_t n = std::strlen(name);
            return arg == name || (arg.size() > n && arg.compare(0, n, name) == 0 && arg[n] == '=');
        };
        if (arg == "--help" || arg == "-h") {
            opt.help = true;
        } else if (arg == "--headless") {
            opt.headless = true;
        } else if (is("--config")) {
            if (!app_detail::optionValue(argc, argv, i, "--config", opt.config_path, error)) return false;
            opt.config_given = true;
        } else if (is("--duration")) {
            std::string value;
            if (!app_detail::optionValue(argc, argv, i, "--duration", value, error)) return false;
            if (!app_detail::parseSeconds(value, opt.duration_s)) {
                error = "--duration expects a positive number of seconds, got \"" + text_util::sanitize(value) + "\"";
                return false;
            }
        } else if (!arg.empty() && arg[0] == '-') {
            error = "unknown option \"" + text_util::sanitize(arg) + "\"";
            return false;
        } else {
            error = "unexpected argument \"" + text_util::sanitize(arg) + "\"";
            return false;
        }
    }
    out = std::move(opt);
    return true;
}

// ---------------------------------------------------------------------------
// 监控程序
// ---------------------------------------------------------------------------

// 运行参数（测试可替换输出流）
struct RunOptions {
    bool headless = false;
    double duration_s = 0.0;      // > 0：运行这么久后自动停止
    std::FILE* out = stdout;      // 画面 / 状态行 / 最终报告的输出（headless 时日志行也回显到这里）
    bool color = true;            // 终端界面与最终报告的 ANSI 颜色（main 在设置了 NO_COLOR 时关闭；headless 总是无色）
};

// 监控程序：
// - 每个已启用的交易所一个 MarketDataFeed + 线程（编号 = 在已启用交易所中的下标，固定顺序 Binance, OKX, Bybit），
//   主线程持有每个交易所的本地 OrderBook（标签 "<name>:<symbol>"）与一个队列延迟统计。
// - 事件循环（只在主线程）：
//   1) 排空每个 feed 的 SPSC 队列（每轮每个 feed 最多 kMaxDrainPerVenue 个，防止单个 feed 饿死其他工作）：
//      记录队列延迟 nowNs() - publish_ns，快照直接从队列槽位复制进本地簿，然后立即检测涉及该交易所的
//      所有有序交易所对（事件驱动：每次簿更新都检测），新信号写日志。断线时 feed 发布的空簿使相关交易所对变为"无数据"。
//   2) 每 refresh_ms 一次 tick：对每个 feed 调用 checkHealth(nowNs())（停顿 / 连接超时恢复依赖它），
//      终端模式下渲染一帧，每 spread_log_interval 次 tick 记录一次所有有序交易所对的价差（headless 模式没有渲染，
//      按 tick 计数，频率相同），并刷新日志。tick 落后时不补跑（下一次从当前时刻起算）。
//   3) headless 模式每秒输出一行状态。
//   4) 本轮没有任何快照时睡眠 kIdleSleepUs（约 200 µs）：空闲时 CPU 占用很低，事件延迟不超过约 0.2 ms。
// - 停止：stop 标志（信号）或 duration 到期 → 对所有 feed 调用 stop()（踢断阻塞的读写 / 握手 / 退避等待）并 join，
//   然后输出最终报告（stdout 与日志）。
// - feed 线程在屏蔽 SIGINT/SIGTERM 的情况下创建：信号只投递给主线程，feed 线程里阻塞的系统调用不会被打断。
// - feed 的日志回调写入线程安全的 Logger；FeedConfig::log_to_stderr 总是关闭（终端模式下 stderr 会破坏画面，
//   headless 模式由 Logger 把同样的行回显到 out）。
class MonitorApp {
public:
    static constexpr int kIdleSleepUs = 200;
    static constexpr std::size_t kMaxDrainPerVenue = 256;
    static constexpr std::int64_t kStatusIntervalNs = 1000000000;  // headless 状态行间隔

    MonitorApp(const AppConfig& cfg, Logger& logger, RunOptions opt)
        : cfg_(cfg),
          logger_(logger),
          opt_(opt),
          enabled_(enabledVenues(cfg)),
          detector_(feeRates(cfg, enabled_), cfg.arbitrage.min_profit, cfg.arbitrage.max_qty, cfg.arbitrage.max_levels) {
        if (opt_.out == nullptr) opt_.out = stdout;
        books_.reserve(enabled_.size());
        for (std::size_t i = 0; i < enabled_.size(); ++i) {
            const VenueConfig& v = enabled_[i];
            feeds_.push_back(std::make_unique<MarketDataFeed>(feedConfigFor(cfg_, v, static_cast<std::uint32_t>(i))));
            books_.emplace_back(v.name + ":" + v.symbol);
            books_.back().reserve(BookSnapshot::kMaxLevels);
            queue_latency_.push_back(std::make_unique<LatencyTracker>(v.name + " queue"));
            applied_.push_back(0);
        }
    }

    // 正常情况下 run() 已经停止 feed 并恢复光标；run() 因异常退出时由析构函数补做
    ~MonitorApp() {
        stopFeeds();
        restoreCursor();
    }

    MonitorApp(const MonitorApp&) = delete;
    MonitorApp& operator=(const MonitorApp&) = delete;

    // 一个已启用交易所的 FeedConfig（venue_id = 它在已启用交易所中的下标）
    static FeedConfig feedConfigFor(const AppConfig& cfg, const VenueConfig& v, std::uint32_t venue_id) {
        FeedConfig f;
        f.exchange = v.exchange;
        f.venue_id = venue_id;
        f.name = v.name;
        f.host = v.host;
        f.port = v.port;
        f.path = v.path;
        f.symbol = v.symbol;
        f.subscribe_msg = v.subscribe_msg;
        f.rest_host = v.rest_host;
        f.rest_port = v.rest_port;
        f.snapshot_path = v.snapshot_path;
        f.tls_verify = cfg.connection.tls_verify;
        f.ca_file = cfg.connection.ca_file;
        f.stall_timeout_ms = cfg.connection.stall_timeout_ms;
        f.reconnect_initial_ms = cfg.connection.reconnect_initial_ms;
        f.reconnect_max_ms = cfg.connection.reconnect_max_ms;
        f.publish_levels = BookSnapshot::kMaxLevels;
        f.log_to_stderr = false;
        return f;
    }

    // 运行直到 stop 为 true 或 duration 到期；返回进程退出码（0 = 正常结束，1 = 没有启用的交易所 / 无法启动）。
    // 只能调用一次。
    int run(const std::atomic<bool>& stop) {
        if (feeds_.empty()) {
            const std::string msg = "No venue is enabled in the configuration, nothing to monitor";
            std::fprintf(stderr, "[App] %s\n", msg.c_str());
            logger_.warn(msg);
            return 1;
        }
        logStartup();
        if (opt_.headless) logger_.setEcho(opt_.out);
        if (!startFeeds()) {
            logger_.setEcho(nullptr);
            return 1;
        }

        start_ns_ = hft::nowNs();
        const std::int64_t refresh_ns = static_cast<std::int64_t>(std::max(1, cfg_.display.refresh_ms)) * 1000000;
        const std::int64_t deadline = opt_.duration_s > 0.0
                                          ? start_ns_ + static_cast<std::int64_t>(opt_.duration_s * 1e9)
                                          : std::numeric_limits<std::int64_t>::max();
        std::int64_t next_tick = start_ns_;
        std::int64_t next_status = start_ns_ + kStatusIntervalNs;
        if (!opt_.headless) {
            writeOut(Display::kHideCursor);
            cursor_hidden_ = true;
        }

        const char* reason = "stop requested";
        while (!stop.load(std::memory_order_acquire)) {
            std::int64_t now = hft::nowNs();
            if (now >= deadline) {
                reason = "duration elapsed";
                break;
            }
            const bool busy = drainQueues();
            now = hft::nowNs();
            if (now >= next_tick) {
                onTick(now);
                next_tick = nextDeadline(next_tick, refresh_ns, now);
            }
            if (opt_.headless && now >= next_status) {
                printStatusLine(now);
                next_status = nextDeadline(next_status, kStatusIntervalNs, now);
            }
            if (!busy) std::this_thread::sleep_for(std::chrono::microseconds(kIdleSleepUs));
        }

        const double ran_s = static_cast<double>(hft::nowNs() - start_ns_) / 1e9;
        restoreCursor();
        char buf[160];
        std::snprintf(buf, sizeof(buf), "Shutting down (%s after %.1f s): stopping %zu feed%s...", reason, ran_s,
                      feeds_.size(), feeds_.size() == 1 ? "" : "s");
        announce(buf);
        const std::int64_t t0 = hft::nowNs();
        stopFeeds();
        std::snprintf(buf, sizeof(buf), "All feeds stopped in %.1f ms", static_cast<double>(hft::nowNs() - t0) / 1e6);
        announce(buf);
        logger_.setEcho(nullptr);

        finalReport();
        return 0;
    }

    // ---------------- 访问器（主线程 / 测试；run() 返回后读取）----------------

    std::size_t venueCount() const noexcept { return feeds_.size(); }
    const VenueConfig& venueConfig(std::size_t i) const { return enabled_.at(i); }
    const MarketDataFeed& feed(std::size_t i) const { return *feeds_.at(i); }
    const OrderBook& book(std::size_t i) const { return books_.at(i); }
    const LatencyTracker& queueLatency(std::size_t i) const { return *queue_latency_.at(i); }
    std::uint64_t snapshotsApplied(std::size_t i) const { return applied_.at(i); }
    const ArbitrageDetector& detector() const noexcept { return detector_; }
    std::uint64_t ticks() const noexcept { return ticks_; }
    std::uint64_t renders() const noexcept { return renders_; }

    // 当前各交易所的展示数据（指针指向本对象内部，有效期与本对象相同）
    std::vector<VenueView> collectViews() const {
        std::vector<VenueView> views;
        views.reserve(feeds_.size());
        for (std::size_t i = 0; i < feeds_.size(); ++i) {
            const MarketDataFeed& f = *feeds_[i];
            VenueView v;
            v.name = enabled_[i].name;
            v.book = &books_[i];
            v.status = f.status();
            v.stats = f.stats();
            // lastError() 在恢复之后仍保留：只在非 LIVE 时展示
            if (v.status != FeedStatus::Live) v.last_error = f.lastError();
            v.processing = &f.getLatencyTracker();
            v.exchange = &f.getExchangeLatency();
            v.queue = queue_latency_[i].get();
            v.fee_rate = cfg_.feeRate(enabled_[i]);
            views.push_back(std::move(v));
        }
        return views;
    }

private:
    struct TermSize {
        int rows = 0;
        int cols = 0;
    };

    static std::vector<VenueConfig> enabledVenues(const AppConfig& cfg) {
        std::vector<VenueConfig> out;
        for (const VenueConfig& v : cfg.venues) {
            if (v.enabled) out.push_back(v);
        }
        return out;
    }

    static std::vector<double> feeRates(const AppConfig& cfg, const std::vector<VenueConfig>& venues) {
        std::vector<double> fees;
        for (const VenueConfig& v : venues) fees.push_back(cfg.feeRate(v));
        return fees;
    }

    // 周期任务的下一次时刻：落后时不补跑，从当前时刻重新起算
    static std::int64_t nextDeadline(std::int64_t previous, std::int64_t period, std::int64_t now) noexcept {
        const std::int64_t next = previous + period;
        return next > now ? next : now + period;
    }

    void writeOut(const std::string& s) const {
        std::fwrite(s.data(), 1, s.size(), opt_.out);
        std::fflush(opt_.out);
    }

    // 终端模式结束（或异常退出）时显示光标；不分配内存、不抛异常
    void restoreCursor() noexcept {
        if (!cursor_hidden_) return;
        cursor_hidden_ = false;
        static const char kShow[] = "\033[?25h\n";
        std::fwrite(kShow, 1, sizeof(kShow) - 1, opt_.out);
        std::fflush(opt_.out);
    }

    // 一行进度信息：写日志并输出到 out（headless 时日志行本来就回显到 out，不再重复输出）
    void announce(const std::string& line) {
        if (!opt_.headless) writeOut(line + "\n");
        logger_.log(line);
    }

    // 终端尺寸（out 不是终端时为 0 = 不限制）
    TermSize terminalSize() const {
        TermSize t;
        const int fd = ::fileno(opt_.out);
        winsize ws{};
        if (fd >= 0 && ::isatty(fd) && ::ioctl(fd, TIOCGWINSZ, &ws) == 0) {
            t.rows = ws.ws_row;
            t.cols = ws.ws_col;
        }
        return t;
    }

    void logStartup() {
        logger_.log("Config: " + (cfg_.source.empty() ? std::string("built-in defaults") : cfg_.source));
        logger_.log("Config: " + ConfigReader::summary(cfg_));
        for (const std::string& w : cfg_.warnings) logger_.warn(w);
        for (const VenueConfig& v : cfg_.venues) {
            if (!v.enabled) {
                logger_.log("Venue " + v.name + ": disabled");
                continue;
            }
            std::string line = "Venue " + v.name + ": wss://" + v.host + ":" + v.port + v.path + "  symbol " + v.symbol +
                               "  fee " + config_detail::formatNumber(cfg_.feeRate(v));
            if (!v.rest_host.empty() && !v.snapshot_path.empty()) {
                line += "  REST https://" + v.rest_host + ":" + v.rest_port + v.snapshot_path;
            }
            logger_.log(line);
        }
        const ArbitrageConfig& a = cfg_.arbitrage;
        logger_.log("Arbitrage: min_profit " + config_detail::formatNumber(a.min_profit) + "  max_qty " +
                    (a.max_qty > 0.0 ? config_detail::formatNumber(a.max_qty) : std::string("unlimited")) +
                    "  max_levels " + std::to_string(a.max_levels));
        const ConnectionConfig& c = cfg_.connection;
        logger_.log(std::string("Connection: tls_verify ") + (c.tls_verify ? "true" : "false") + "  ca_file " +
                    (c.ca_file.empty() ? std::string("(system CA store)") : c.ca_file) + "  stall_timeout_ms " +
                    std::to_string(c.stall_timeout_ms) + "  reconnect " + std::to_string(c.reconnect_initial_ms) + ".." +
                    std::to_string(c.reconnect_max_ms) + " ms");
        std::string mode = opt_.headless ? "headless" : "terminal UI";
        mode += ", refresh " + std::to_string(cfg_.display.refresh_ms) + " ms";
        if (opt_.duration_s > 0.0) mode += ", duration " + config_detail::formatNumber(opt_.duration_s) + " s";
        logger_.log("Mode: " + mode);
    }

    // 启动 feed 线程。线程创建期间屏蔽 SIGINT/SIGTERM：新线程继承信号掩码，信号因此只投递给主线程。
    bool startFeeds() {
        for (auto& f : feeds_) {
            f->setLogCallback([this](const std::string& line) { logger_.logFeed(line); });
        }
        sigset_t block, previous;
        sigemptyset(&block);
        sigaddset(&block, SIGINT);
        sigaddset(&block, SIGTERM);
        const bool masked = ::pthread_sigmask(SIG_BLOCK, &block, &previous) == 0;
        std::string error;
        try {
            for (auto& f : feeds_) {
                MarketDataFeed* feed = f.get();
                threads_.emplace_back([feed] { feed->run(); });
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
        if (masked) ::pthread_sigmask(SIG_SETMASK, &previous, nullptr);
        if (!error.empty()) {
            std::fprintf(stderr, "[App] Cannot start feed threads: %s\n", error.c_str());
            logger_.warn("Cannot start feed threads: " + error);
            stopFeeds();
            return false;
        }
        std::string names;
        for (std::size_t i = 0; i < enabled_.size(); ++i) names += (i > 0 ? ", " : "") + enabled_[i].name;
        logger_.log("Started " + std::to_string(feeds_.size()) + " market data feed" + (feeds_.size() == 1 ? "" : "s") +
                    ": " + names);
        return true;
    }

    // 停止所有 feed 并等待线程结束（可重复调用）
    void stopFeeds() noexcept {
        for (auto& f : feeds_) f->stop();
        for (std::thread& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
    }

    // 排空所有队列；返回本轮是否处理了快照
    bool drainQueues() {
        bool any = false;
        for (std::size_t v = 0; v < feeds_.size(); ++v) {
            hft::SpscQueue<BookSnapshot>& q = feeds_[v]->queue();
            for (std::size_t k = 0; k < kMaxDrainPerVenue; ++k) {
                const BookSnapshot* s = q.front();
                if (s == nullptr) break;
                applySnapshot(v, *s);
                q.pop();
                any = true;
            }
        }
        return any;
    }

    // 应用一个快照（直接读队列槽位，不额外复制）并检测涉及该交易所的所有有序交易所对
    void applySnapshot(std::size_t v, const BookSnapshot& s) {
        const std::int64_t queued = hft::nowNs() - s.publish_ns;
        queue_latency_[v]->recordNs(queued > 0 ? static_cast<std::uint64_t>(queued) : 0);
        s.copyTo(books_[v]);
        ++applied_[v];
        const std::size_t found = detector_.detectVenue(v, books_);
        if (found > 0) {
            // detectVenue 的新信号是 getSignals() 的最后 found 条（found <= 2(N-1)，远小于 deque 上限）
            const std::deque<ArbitrageSignal>& signals = detector_.getSignals();
            for (std::size_t k = signals.size() - found; k < signals.size(); ++k) logger_.logSignal(signals[k]);
        }
    }

    void onTick(std::int64_t now) {
        for (auto& f : feeds_) f->checkHealth(now);
        ++ticks_;
        if (ticks_ % static_cast<std::uint64_t>(std::max(1, cfg_.display.spread_log_interval)) == 0) logSpreads();
        if (!opt_.headless) renderFrame(now);
        logger_.flush();
    }

    // 所有有序交易所对（在 buy 买、在 sell 卖）的最优价差；无数据的交易所对跳过
    void logSpreads() {
        const std::size_t n = books_.size();
        for (std::size_t buy = 0; buy < n; ++buy) {
            for (std::size_t sell = 0; sell < n; ++sell) {
                if (buy == sell) continue;
                const std::vector<PriceLevel>& asks = books_[buy].getAsks();
                const std::vector<PriceLevel>& bids = books_[sell].getBids();
                if (asks.empty() || bids.empty()) continue;
                logger_.logSpread(enabled_[buy].name, asks[0].price, enabled_[sell].name, bids[0].price,
                                  bids[0].price - asks[0].price);
            }
        }
    }

    void renderFrame(std::int64_t now) {
        const std::vector<VenueView> views = collectViews();
        FrameOptions fo;
        fo.levels = cfg_.display.levels;
        fo.color = opt_.color;
        fo.uptime_s = static_cast<double>(now - start_ns_) / 1e9;
        fo.config_warnings = cfg_.warnings.size();
        const TermSize ts = terminalSize();
        fo.max_rows = ts.rows;  // present() 在最后一行之后不换行：帧可以占满全部行而不滚动
        const std::string frame = Display::renderFrame(views, detector_, fo);
        Display::present(frame, !frame_shown_, ts.cols > 0 ? static_cast<std::size_t>(ts.cols) : 0, opt_.out);
        frame_shown_ = true;
        ++renders_;
    }

    // headless 状态行："[00:00:03] Binance LIVE 67000.10/67000.20 (812 msgs) | OKX ... | signals 4, net $1.23"
    void printStatusLine(std::int64_t now) {
        std::string line = "[" + Display::formatDuration(static_cast<double>(now - start_ns_) / 1e9) + "]";
        for (std::size_t i = 0; i < feeds_.size(); ++i) {
            const OrderBook& b = books_[i];
            std::string bbo = "--/--";
            if (!b.getBids().empty() && !b.getAsks().empty()) {
                const int prec = Display::pricePrecision(b.getBids()[0].price);
                bbo = fixed(b.getBids()[0].price, prec) + "/" + fixed(b.getAsks()[0].price, prec);
            }
            line += (i > 0 ? " | " : " ") + enabled_[i].name + " " + feedStatusName(feeds_[i]->status()) + " " + bbo +
                    " (" + std::to_string(feeds_[i]->stats().messages) + " msgs)";
        }
        line += " | signals " + std::to_string(detector_.getTotalSignals()) + ", net $" +
                fixed(detector_.getTotalNetProfit(), 2);
        writeOut(line + "\n");
    }

    void finalReport() {
        const std::vector<VenueView> views = collectViews();
        const bool color = opt_.color && !opt_.headless;
        writeOut(Display::renderFinalReport(views, detector_, color) + "\nGoodbye!\n");
        logger_.logFinalReport(views, detector_);
    }

    static std::string fixed(double v, int precision) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.*f", precision, v);
        return buf;
    }

    const AppConfig cfg_;
    Logger& logger_;
    RunOptions opt_;
    const std::vector<VenueConfig> enabled_;   // 已启用的交易所（下标 = venue id）
    ArbitrageDetector detector_;
    std::vector<std::unique_ptr<MarketDataFeed>> feeds_;
    std::vector<std::thread> threads_;
    std::vector<OrderBook> books_;                                 // 主线程的本地簿（下标 = venue id）
    std::vector<std::unique_ptr<LatencyTracker>> queue_latency_;   // 队列延迟：publish_ns → 主线程取出
    std::vector<std::uint64_t> applied_;                           // 每个交易所已应用的快照数
    std::int64_t start_ns_ = 0;
    std::uint64_t ticks_ = 0;
    std::uint64_t renders_ = 0;
    bool frame_shown_ = false;
    bool cursor_hidden_ = false;
};
