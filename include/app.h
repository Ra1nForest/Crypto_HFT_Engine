//
// app.h
// 监控程序主体：命令行解析，按配置创建行情源与套利检测器，事件驱动主循环（排空快照队列 → 更新本地簿 → 套利检测 →
// 定时健康检查 / 渲染 / 价差日志），停止后输出最终报告。src/main.cpp 负责信号与配置并调用这里（测试直接驱动本类）。
// Monitor core: command-line parsing, creating the feeds and the arbitrage detector from the config, the
// event-driven main loop (drain the snapshot queues → update the local books → arbitrage detection →
// periodic health checks / rendering / spread logging), and the final report after stopping.
// src/main.cpp handles signals and the config, and calls into this (tests drive this class directly).
//

#pragma once

#include "arbitrage_detector.h"
#include "book_snapshot.h"
#include "config.h"
#include "display.h"
#include "hft/common.h"
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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// 命令行
// Command line
// ---------------------------------------------------------------------------

struct CliOptions {
    std::string config_path = "config.json";  // --config <path>（相对当前目录）
                                              // --config <path> (relative to the current directory)
    bool config_given = false;                // 显式给出了 --config：文件必须能读取并解析（见 main.cpp）
                                              // --config given explicitly: the file must be readable
                                              // and parseable (see main.cpp)
    bool headless = false;                    // --headless：不渲染终端界面，每秒一行状态
                                              // --headless: no terminal UI rendering, one status line per second
    double duration_s = 0.0;                  // --duration <seconds>：> 0 时到时自动停止；0 = 直到收到信号
                                              // --duration <seconds>: if > 0, stop automatically when
                                              // it elapses; 0 = until a signal is received
    bool busy_poll = false;                   // --busy-poll：主循环空闲时忙等（见 RunOptions::busy_poll）
                                              // --busy-poll: main loop busy-waits when idle (see RunOptions::busy_poll)
    bool help = false;                        // --help / -h
};

// 用法说明（英文，--help 与参数错误时输出）
// Usage text (in English, printed for --help and on argument errors)
inline std::string usageText(const std::string& program) {
    const std::string prog = program.empty() ? std::string("crypto_hft_engine") : program;
    return std::string(kAppVersion) +
           " - cross-exchange arbitrage monitor (Binance, OKX, Bybit)\n"
           "\n"
           "Usage: " + prog + " [--config <path>] [--headless] [--duration <seconds>] [--busy-poll] [--help]\n"
           "\n"
           "Options:\n"
           "  --config <path>       configuration file (default: config.json in the current directory;\n"
           "                        an explicitly given file must exist and contain valid JSON)\n"
           "  --headless            no terminal UI: print feed events, signals and one status line per second\n"
           "  --duration <seconds>  stop automatically after this many seconds (default: run until Ctrl+C)\n"
           "  --busy-poll           spin on the feed queues instead of sleeping until a feed publishes:\n"
           "                        lowest queue latency (microseconds), but one CPU core stays at 100%\n"
           "  -h, --help            show this help and exit\n"
           "\n"
           "Ctrl+C, SIGTERM or SIGHUP stops the monitor gracefully and prints the final report. A repeated\n"
           "Ctrl+C or SIGTERM within 1 s counts as the same request (wrappers such as timeout forward it); a\n"
           "later one exits immediately. Ctrl+Z restores the cursor before the monitor stops. Logs are appended\n"
           "to logs/arb_<date>.log under the current directory.\n";
}

namespace app_detail {

// 秒数：有限的正数，上限 1e9 秒（换算成纳秒不会溢出）
// Seconds: a finite positive number, at most 1e9 seconds (converting to nanoseconds cannot overflow)
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
// 带值的选项写作 "--name value" 或 "--name=value"；开关选项不接受 "=value"。
// Parses the command line; on failure returns false and explains why in error. Options may be repeated
// (a later one overrides an earlier one); positional arguments are not accepted.
// Options with a value are written "--name value" or "--name=value"; flag options do not accept "=value".
inline bool parseCommandLine(int argc, const char* const* argv, CliOptions& out, std::string& error) {
    CliOptions opt;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i] != nullptr ? argv[i] : "";
        const std::size_t eq = arg.rfind("--", 0) == 0 ? arg.find('=') : std::string::npos;
        const std::string name = arg.substr(0, eq);
        // 选项的值：'=' 之后的部分，或者下一个参数；缺失或为空时返回 false
        // The option's value: the part after '=', or the next argument; returns false when missing or empty
        auto value = [&](std::string& v) {
            if (eq != std::string::npos) {
                v = arg.substr(eq + 1);
            } else if (i + 1 < argc) {
                v = argv[++i];
            } else {
                error = name + " requires a value";
                return false;
            }
            if (v.empty()) error = name + " requires a non-empty value";
            return !v.empty();
        };
        if (arg == "--help" || arg == "-h") {
            opt.help = true;
        } else if (arg == "--headless") {
            opt.headless = true;
        } else if (arg == "--busy-poll") {
            opt.busy_poll = true;
        } else if (name == "--config") {
            if (!value(opt.config_path)) return false;
            opt.config_given = true;
        } else if (name == "--duration") {
            std::string value_text;
            if (!value(value_text)) return false;
            if (!app_detail::parseSeconds(value_text, opt.duration_s)) {
                error = "--duration expects a positive number of seconds, got \"" + text_util::sanitize(value_text) + "\"";
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
// 主循环的唤醒信号
// Wake-up signal for the main loop
// ---------------------------------------------------------------------------

// 多个生产者（feed 线程：快照入队之后 notify()）、一个消费者（主线程：没有数据时 waitUntil()）。
// Dekker 式握手，不会丢失唤醒：
// - 消费者：sleeping_ = true → seq_cst 栅栏 → 再检查一遍所有队列（ready()）→ 确实为空才在条件变量上等待；
// - 生产者：元素入队（release 存写下标）→ seq_cst 栅栏 → 读 sleeping_，为 true 才加锁置位 pending_ 并 notify。
// 两个栅栏在 seq_cst 全序 S 中必有先后：消费者的在前，生产者就读到 sleeping_ == true（或更新的值）；生产者的在前，
// 消费者复查时就读到新的写下标（[atomics.order]：A 先于栅栏 X、栅栏 Y 先于 B、X 在 S 中先于 Y → B 看到 A 或更新的值）。
// pending_ 在互斥锁内设置与检查：notify 落在"复查之后、开始等待之前"同样不会丢。
// 消费者不在睡眠时（正在处理数据），notify() 只有一个栅栏与一次原子读：不加锁、不做系统调用。
// 多余的唤醒（生产者读到的是消费者刚要清除的 true）无害：消费者多醒一次，排空后再等待。
// Multiple producers (feed threads: notify() after enqueuing a snapshot), one consumer (the main thread:
// waitUntil() when there is no data).
// Dekker-style handshake that never loses a wake-up:
// - Consumer: sleeping_ = true → seq_cst fence → check all queues once more (ready()) → wait on the
//   condition variable only if they really are empty;
// - Producer: enqueue the element (release store of the write index) → seq_cst fence → read sleeping_;
//   only if it is true, take the lock, set pending_ and notify.
// The two fences are always ordered in the seq_cst total order S: if the consumer's comes first, the
// producer reads sleeping_ == true (or a newer value); if the producer's comes first, the consumer's
// re-check reads the new write index ([atomics.order]: A before fence X, fence Y before B, X before Y in
// S → B sees A or a newer value).
// pending_ is set and checked under the mutex: a notify that lands "after the re-check, before the wait
// starts" is not lost either.
// While the consumer is not sleeping (it is processing data), notify() is only one fence and one atomic
// load: no lock, no system call.
// An extra wake-up (the producer read a true that the consumer is about to clear) is harmless: the
// consumer wakes once more, drains, and waits again.
class WakeSignal {
public:
    // 生产者（任意线程）：新元素已经入队之后调用
    // Producer (any thread): call after the new element has been enqueued
    void notify() {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (!sleeping_.load(std::memory_order_relaxed)) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_ = true;
        }
        cv_.notify_one();
    }

    // 消费者（只能有一个线程）：ready() 在宣布睡眠之后复查是否已有数据；没有就等到 notify() 或 deadline_ns
    // （hft::nowNs() 的时钟，即 steady_clock）。返回 true = 复查时已有数据或被唤醒，false = 到期。
    // Consumer (one thread only): ready() re-checks for data after the sleep is announced; if there is none,
    // waits until notify() or deadline_ns (the hft::nowNs() clock, i.e. steady_clock). Returns true = data
    // found on the re-check or woken, false = deadline reached.
    template <class Ready>
    bool waitUntil(Ready&& ready, std::int64_t deadline_ns) {
        sleeping_.store(true, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        bool woken = ready();
        if (!woken) {
            const std::chrono::steady_clock::time_point deadline(
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::nanoseconds(deadline_ns)));
            std::unique_lock<std::mutex> lock(mutex_);
            woken = cv_.wait_until(lock, deadline, [this] { return pending_; });
            pending_ = false;
        }
        sleeping_.store(false, std::memory_order_relaxed);
        return woken;
    }

private:
    std::atomic<bool> sleeping_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool pending_ = false;  // 受 mutex_ 保护
                            // guarded by mutex_
};

// ---------------------------------------------------------------------------
// 监控程序
// Monitor
// ---------------------------------------------------------------------------

// 终端界面的光标状态（与信号处理函数共享，见 RunOptions::cursor_state）
// Cursor state of the terminal UI (shared with the signal handler, see RunOptions::cursor_state)
enum class CursorState : int {
    Visible = 0,    // 光标显示着：终端界面未运行，或显示序列已经写完
                    // cursor shown: the terminal UI is not running, or the show sequence has been written
    Hidden = 1,     // 终端界面运行中：写隐藏序列之前就进入此状态
                    // terminal UI running: entered before the hide sequence is written
    Restoring = 2,  // 正在写显示序列：写完（并 fflush）之后才回到 Visible
                    // writing the show sequence: back to Visible only after the write (and fflush) finishes
};

// 运行参数（测试可替换输出流）
// Run options (tests can replace the output stream)
struct RunOptions {
    bool headless = false;
    double duration_s = 0.0;      // > 0：运行这么久后自动停止
                                  // > 0: stop automatically after running this long
    std::FILE* out = stdout;      // 画面 / 状态行 / 最终报告的输出（headless 时日志行也回显到这里）
                                  // output for frames / status lines / the final report
                                  // (in headless mode log lines are echoed here too)
    bool color = true;            // 终端界面与最终报告的 ANSI 颜色（main 在设置了 NO_COLOR 时关闭；headless 总是无色）
                                  // ANSI colors of the terminal UI and the final report
                                  // (main turns them off when NO_COLOR is set; headless never uses color)
    bool busy_poll = false;       // --busy-poll：没有数据时用 hft::cpuRelax() 自旋检查队列，而不是等待唤醒
                                  // --busy-poll: with no data, spin checking the queues with hft::cpuRelax()
                                  // instead of waiting for a wake-up
    // 与信号处理函数共享的状态（main 指向自己的全局原子量；为空则不使用）。处理函数在主线程上运行，
    // 只需保证这里的读写不与前后的输出重排（seq_cst）：
    // - cursor_state：见 CursorState。处理函数在停止进程（SIGTSTP）或强制退出之前，只要状态不是 Visible 就写
    //   显示序列（Restoring 时显示序列可能还没写出去，例如输出被 Ctrl+S 阻塞；多写一次无害）；SIGCONT 之后只在
    //   Hidden 时重新隐藏光标（Restoring 时不能再隐藏，否则会盖掉随后写出的显示序列）。
    // - redraw：处理函数（SIGCONT 之后）置位；下一帧渲染时取走，清屏后整屏重画。
    // State shared with the signal handler (main points these at its own global atomics; null = unused).
    // The handler runs on the main thread, so the loads and stores here only need to stay ordered with the
    // output before and after them (seq_cst):
    // - cursor_state: see CursorState. Before stopping the process (SIGTSTP) or force-exiting, the handler
    //   writes the show sequence whenever the state is not Visible (in Restoring the show sequence may not
    //   have gone out yet, e.g. when output is blocked by Ctrl+S; writing it once more is harmless); after
    //   SIGCONT it re-hides the cursor only in Hidden (in Restoring it must not hide it again, or it would
    //   override the show sequence written afterwards).
    // - redraw: set by the handler (after SIGCONT); taken by the next frame render, which clears the screen
    //   and redraws it in full.
    std::atomic<CursorState>* cursor_state = nullptr;
    std::atomic<bool>* redraw = nullptr;
};

// 监控程序：
// - 每个已启用的交易所一个 MarketDataFeed + 线程（编号 = 在已启用交易所中的下标，固定顺序 Binance, OKX, Bybit），
//   主线程持有每个交易所的本地 OrderBook（标签 "<name>:<symbol>"）与一个队列延迟统计。
// - 事件循环（只在主线程）：
//   1) 排空每个 feed 的 SPSC 队列（每轮每个 feed 最多 kMaxDrainPerVenue 个，防止单个 feed 饿死其他工作）：
//      记录队列延迟 nowNs() - publish_ns，快照直接从队列槽位复制进本地簿，然后立即检测涉及该交易所的
//      所有有序交易所对（事件驱动：每次簿更新都检测），新信号写日志。断线时 feed 发布的空簿使相关交易所对变为"无数据"；
//      此外只有行情源状态为 LIVE 的交易所参与检测与价差日志，即使空簿晚到，过期的簿也不会产生信号（见 participates）。
//   2) 每 refresh_ms 一次 tick：对每个 feed 调用 checkHealth(nowNs())（停顿 / 连接超时恢复依赖它），
//      终端模式下渲染一帧，每 spread_log_interval 次 tick 记录一次所有有序交易所对的价差（headless 模式没有渲染，
//      按 tick 计数，频率相同），并刷新日志。tick 落后时不补跑（下一次从当前时刻起算）。
//   3) headless 模式每秒输出一行状态。
//   4) 本轮没有任何快照时（默认）在 WakeSignal 上等待：feed 每次快照入队后通过发布回调 notify()，主线程立即醒来；
//      否则等到下一个定时任务（tick / headless 状态行 / duration）到期，最长 kMaxIdleWaitNs。信号处理函数与测试线程
//      只设置 stop 标志、不能唤醒等待，所以 stop 最迟在 kMaxIdleWaitNs 之后被发现。
//      空闲时每秒只醒来约 1000 / min(refresh_ms, 50) 次，CPU 占用接近 0；有数据时，快照的队列延迟主要是一次线程唤醒
//      （在 4 vCPU 的 KVM 虚拟机上实测 p50 约 0.1 ms、p99 约 0.2-0.4 ms，取决于内核与虚拟化）。
//      --busy-poll（RunOptions::busy_poll）：改为用 hft::cpuRelax() 自旋检查队列，不睡眠、也不需要发布回调：
//      队列延迟降到微秒级，代价是主线程一直占满一个核（适合独占、绑核的部署）。
// - 停止：stop 标志（信号）或 duration 到期 → 对所有 feed 调用 stop()（踢断阻塞的读写 / 握手 / 退避等待）并 join，
//   然后输出最终报告（stdout 与日志）。
// - feed 线程在屏蔽 SIGINT/SIGTERM/SIGHUP/SIGTSTP/SIGCONT 的情况下创建：这些信号只投递给主线程，
//   feed 线程里阻塞的系统调用不会被打断。
// - feed 的日志回调写入线程安全的 Logger；FeedConfig::log_to_stderr 总是关闭（终端模式下 stderr 会破坏画面，
//   headless 模式由 Logger 把同样的行回显到 out）。
// Monitor:
// - One MarketDataFeed + thread per enabled venue (id = index among the enabled venues, in the fixed order
//   Binance, OKX, Bybit); for each venue the main thread holds a local OrderBook (label "<name>:<symbol>")
//   and a set of queue latency statistics.
// - Event loop (main thread only):
//   1) Drain each feed's SPSC queue (at most kMaxDrainPerVenue per feed per round, so that a single feed
//      cannot starve the other work): record the queue latency nowNs() - publish_ns, copy the snapshot
//      straight from the queue slot into the local book, then immediately check every ordered venue pair
//      involving that venue (event-driven: every book update is checked) and log new signals. The empty
//      book a feed publishes on disconnect turns the affected pairs into "no data"; in addition, only
//      venues whose feed status is LIVE take part in detection and spread logging, so a stale book
//      produces no signal even if the empty book arrives late (see participates).
//   2) One tick every refresh_ms: call checkHealth(nowNs()) on every feed (stall / connect-timeout
//      recovery depends on it), render a frame in terminal mode, log the spreads of all ordered venue
//      pairs every spread_log_interval ticks (headless mode does not render but counts ticks the same
//      way, so the rate is the same), and flush the log. A late tick is not made up (the next one is
//      counted from the current time).
//   3) Headless mode prints one status line per second.
//   4) When this round processed no snapshot, wait (by default) on WakeSignal: after each snapshot it
//      enqueues, a feed calls notify() through the publish callback and the main thread wakes at once;
//      otherwise it waits until the next timed task (tick / headless status line / duration) is due, at
//      most kMaxIdleWaitNs. Signal handlers and test threads only set the stop flag and cannot wake the
//      wait, so stop is noticed at most kMaxIdleWaitNs later.
//      When idle it wakes only about 1000 / min(refresh_ms, 50) times per second, with CPU usage close
//      to 0; with data, a snapshot's queue latency is mostly one thread wake-up (measured on a 4 vCPU KVM
//      VM: p50 about 0.1 ms, p99 about 0.2-0.4 ms, depending on the kernel and the virtualization).
//      --busy-poll (RunOptions::busy_poll): instead spin checking the queues with hft::cpuRelax(), never
//      sleeping and needing no publish callback: queue latency drops to the microsecond level, at the
//      cost of the main thread keeping one core fully busy (suits exclusive, CPU-pinned deployments).
// - Stop: the stop flag (signal) or the duration elapsing → call stop() on all feeds (kicks blocked
//   reads/writes / handshakes / backoff waits) and join them, then print the final report (stdout and
//   the log).
// - Feed threads are created with SIGINT/SIGTERM/SIGHUP/SIGTSTP/SIGCONT blocked: these signals are only
//   delivered to the main thread, and blocking system calls in the feed threads are not interrupted.
// - The feeds' log callback writes to the thread-safe Logger; FeedConfig::log_to_stderr is always off
//   (in terminal mode stderr would corrupt the screen, and in headless mode the Logger echoes the same
//   lines to out).
class MonitorApp {
public:
    static constexpr std::int64_t kMaxIdleWaitNs = 50000000;  // 空闲等待的上限（stop 标志最迟这么久之后被发现）
                                                              // upper bound of an idle wait (the stop flag is
                                                              // noticed at most this much later)
    static constexpr std::size_t kMaxDrainPerVenue = 256;
    static constexpr std::int64_t kStatusIntervalNs = 1000000000;  // headless 状态行间隔
                                                                   // headless status line interval

    MonitorApp(const AppConfig& cfg, Logger& logger, RunOptions opt)
        : cfg_(cfg),
          logger_(logger),
          opt_(opt),
          enabled_(cfg_.enabledVenues()),  // 指向成员 cfg_ 的 venues（cfg_ 先初始化，之后不再修改）
                                           // points into the venues of member cfg_ (cfg_ is initialized
                                           // first and not modified afterwards)
          detector_(feeRates(cfg_, enabled_), cfg_.arbitrage.min_profit, cfg_.arbitrage.max_qty,
                    cfg_.arbitrage.max_levels) {
        if (opt_.out == nullptr) opt_.out = stdout;
        books_.reserve(enabled_.size());
        for (std::size_t i = 0; i < enabled_.size(); ++i) {
            const VenueConfig& v = *enabled_[i];
            feeds_.push_back(std::make_unique<MarketDataFeed>(feedConfigFor(cfg_, v, static_cast<std::uint32_t>(i))));
            books_.emplace_back(v.name + ":" + v.symbol);
            books_.back().reserve(BookSnapshot::kMaxLevels);
            queue_latency_.push_back(std::make_unique<LatencyTracker>(v.name + " queue"));
            applied_.push_back(0);
        }
    }

    // 正常情况下 run() 已经停止 feed 并恢复光标；run() 因异常退出时由析构函数补做
    // Normally run() has already stopped the feeds and restored the cursor; if run() exits through an
    // exception, the destructor does it instead
    ~MonitorApp() {
        stopFeeds();
        restoreCursor();
    }

    MonitorApp(const MonitorApp&) = delete;
    MonitorApp& operator=(const MonitorApp&) = delete;

    // 一个已启用交易所的 FeedConfig（venue_id = 它在已启用交易所中的下标）
    // FeedConfig for one enabled venue (venue_id = its index among the enabled venues)
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
    // Runs until stop is true or the duration elapses; returns the process exit code (0 = normal end,
    // 1 = no venue enabled / cannot start).
    // Can be called only once.
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
        if (!opt_.headless) {
            setCursorState(CursorState::Hidden);  // 先改状态再隐藏（见 RunOptions::cursor_state）
                                                  // change the state first, then hide (see RunOptions::cursor_state)
            cursor_hidden_ = true;
            writeOut(Display::kHideCursor);
        }
        shutdown(eventLoop(stop));
        finalReport();
        return 0;
    }

    // ---------------- 访问器（主线程 / 测试；run() 返回后读取）----------------
    // ---------------- Accessors (main thread / tests; read after run() returns) ----------------

    std::size_t venueCount() const noexcept { return feeds_.size(); }
    const VenueConfig& venueConfig(std::size_t i) const { return *enabled_.at(i); }
    const MarketDataFeed& feed(std::size_t i) const { return *feeds_.at(i); }
    const OrderBook& book(std::size_t i) const { return books_.at(i); }
    const LatencyTracker& queueLatency(std::size_t i) const { return *queue_latency_.at(i); }
    std::uint64_t snapshotsApplied(std::size_t i) const { return applied_.at(i); }
    const ArbitrageDetector& detector() const noexcept { return detector_; }
    std::uint64_t ticks() const noexcept { return ticks_; }
    std::uint64_t renders() const noexcept { return renders_; }

    // 当前各交易所的展示数据（指针指向本对象内部，有效期与本对象相同）
    // Current display data of each venue (the pointers point into this object and share its lifetime)
    std::vector<VenueView> collectViews() const {
        std::vector<VenueView> views;
        views.reserve(feeds_.size());
        for (std::size_t i = 0; i < feeds_.size(); ++i) {
            const MarketDataFeed& f = *feeds_[i];
            VenueView v;
            v.name = enabled_[i]->name;
            v.book = &books_[i];
            v.status = f.status();
            v.stats = f.stats();
            // lastError() 在恢复之后仍保留：只在非 LIVE 时展示
            // lastError() is kept after recovery: show it only when not LIVE
            if (v.status != FeedStatus::Live) v.last_error = f.lastError();
            v.processing = &f.getLatencyTracker();
            v.exchange = &f.getExchangeLatency();
            v.queue = queue_latency_[i].get();
            v.fee_rate = cfg_.feeRate(*enabled_[i]);
            views.push_back(std::move(v));
        }
        return views;
    }

private:
    struct TermSize {
        int rows = 0;
        int cols = 0;
    };

    static std::vector<double> feeRates(const AppConfig& cfg, const std::vector<const VenueConfig*>& venues) {
        std::vector<double> fees;
        for (const VenueConfig* v : venues) fees.push_back(cfg.feeRate(*v));
        return fees;
    }

    // 周期任务的下一次时刻：落后时不补跑，从当前时刻重新起算
    // Next time of a periodic task: when behind, missed runs are not made up and timing restarts from now
    static std::int64_t nextDeadline(std::int64_t previous, std::int64_t period, std::int64_t now) noexcept {
        const std::int64_t next = previous + period;
        return next > now ? next : now + period;
    }

    // 事件循环（见类注释）：运行到 stop 为 true 或 duration 到期，返回结束原因
    // Event loop (see the class comment): runs until stop is true or the duration elapses; returns the
    // reason it ended
    const char* eventLoop(const std::atomic<bool>& stop) {
        const std::int64_t refresh_ns = static_cast<std::int64_t>(std::max(1, cfg_.display.refresh_ms)) * 1000000;
        const std::int64_t deadline = opt_.duration_s > 0.0
                                          ? start_ns_ + static_cast<std::int64_t>(opt_.duration_s * 1e9)
                                          : std::numeric_limits<std::int64_t>::max();
        std::int64_t next_tick = start_ns_;
        std::int64_t next_status = start_ns_ + kStatusIntervalNs;
        while (!stop.load(std::memory_order_acquire)) {
            std::int64_t now = hft::nowNs();
            if (now >= deadline) return "duration elapsed";
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
            if (busy) continue;
            if (opt_.busy_poll) {
                hft::cpuRelax();
                continue;
            }
            // 没有数据：等到有 feed 发布快照，或下一个定时任务到期（最长 kMaxIdleWaitNs）
            // No data: wait until a feed publishes a snapshot or the next timed task is due
            // (at most kMaxIdleWaitNs)
            std::int64_t wake_at = std::min({next_tick, deadline, now + kMaxIdleWaitNs});
            if (opt_.headless) wake_at = std::min(wake_at, next_status);
            wake_.waitUntil([this] { return anyQueued(); }, wake_at);
        }
        return "stop requested";
    }

    // 恢复光标，停止所有 feed 并报告进度（之后日志不再回显）
    // Restore the cursor, stop all feeds and report progress (log lines are no longer echoed afterwards)
    void shutdown(const char* reason) {
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
    }

    void writeOut(const std::string& s) const {
        std::fwrite(s.data(), 1, s.size(), opt_.out);
        std::fflush(opt_.out);
    }

    // 终端模式结束（或异常退出）时显示光标并换行；不分配内存、不抛异常
    // When terminal mode ends (or exits through an exception), show the cursor and write a newline;
    // does not allocate memory or throw
    void restoreCursor() noexcept {
        if (!cursor_hidden_) return;
        cursor_hidden_ = false;
        setCursorState(CursorState::Restoring);  // 显示序列写完之后才是 Visible（见 RunOptions::cursor_state）
                                                 // Visible only after the show sequence is written
                                                 // (see RunOptions::cursor_state)
        std::fwrite(Display::kShowCursor, 1, sizeof(Display::kShowCursor) - 1, opt_.out);
        std::fputc('\n', opt_.out);
        std::fflush(opt_.out);
        setCursorState(CursorState::Visible);
    }

    void setCursorState(CursorState s) noexcept {
        if (opt_.cursor_state != nullptr) opt_.cursor_state->store(s, std::memory_order_seq_cst);
    }

    // 一行进度信息：写日志并输出到 out（headless 时日志行本来就回显到 out，不再重复输出）
    // One progress line: log it and write it to out (in headless mode log lines are already echoed to out,
    // so it is not written twice)
    void announce(const std::string& line) {
        if (!opt_.headless) writeOut(line + "\n");
        logger_.log(line);
    }

    // 终端尺寸（out 不是终端时为 0 = 不限制）
    // Terminal size (0 = unlimited when out is not a terminal)
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
        if (opt_.busy_poll) mode += ", busy-poll";
        if (opt_.duration_s > 0.0) mode += ", duration " + config_detail::formatNumber(opt_.duration_s) + " s";
        logger_.log("Mode: " + mode);
    }

    // 启动 feed 线程。线程创建期间屏蔽 SIGINT/SIGTERM/SIGHUP/SIGTSTP/SIGCONT：新线程继承信号掩码，这些信号
    // 因此只由主线程处理（处理函数与主线程共享光标状态，见 RunOptions::cursor_state）。
    // 发布回调（必须在 run() 之前设置）：快照入队后唤醒等待中的主循环；忙等模式不需要。
    // Starts the feed threads. SIGINT/SIGTERM/SIGHUP/SIGTSTP/SIGCONT are blocked while the threads are
    // created: new threads inherit the signal mask, so these signals are handled only by the main thread
    // (the handlers share the cursor state with the main thread, see RunOptions::cursor_state).
    // Publish callback (must be set before run()): wakes the waiting main loop after a snapshot is enqueued;
    // not needed in busy-wait mode.
    bool startFeeds() {
        for (auto& f : feeds_) {
            f->setLogCallback([this](const std::string& line) { logger_.logFeed(line); });
            if (!opt_.busy_poll) f->setPublishCallback([this] { wake_.notify(); });
        }
        sigset_t block, previous;
        sigemptyset(&block);
        for (int sig : {SIGINT, SIGTERM, SIGHUP, SIGTSTP, SIGCONT}) sigaddset(&block, sig);
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
        for (std::size_t i = 0; i < enabled_.size(); ++i) names += (i > 0 ? ", " : "") + enabled_[i]->name;
        logger_.log("Started " + std::to_string(feeds_.size()) + " market data feed" + (feeds_.size() == 1 ? "" : "s") +
                    ": " + names);
        return true;
    }

    // 停止所有 feed 并等待线程结束（可重复调用）
    // Stop all feeds and wait for the threads to finish (may be called repeatedly)
    void stopFeeds() noexcept {
        for (auto& f : feeds_) f->stop();
        for (std::thread& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
    }

    // 是否有队列非空（消费端操作，只在主线程调用；等待之前的复查）
    // Whether any queue is non-empty (a consumer-side operation, called only on the main thread; the
    // re-check before waiting)
    bool anyQueued() {
        for (auto& f : feeds_) {
            if (f->queue().front() != nullptr) return true;
        }
        return false;
    }

    // 排空所有队列；返回本轮是否处理了快照
    // Drain all queues; returns whether this round processed a snapshot
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

    // 应用一个快照（直接读队列槽位，不额外复制），检测涉及该交易所的所有有序交易所对并记录新信号。
    // 不参与检测的交易所（见 participates）用空簿代替：该交易所对按"无数据"处理（不出信号，has_data = false）。
    // Applies one snapshot (read directly from the queue slot, without an extra copy), checks every ordered
    // venue pair involving that venue and logs new signals.
    // A venue that does not take part in detection (see participates) is replaced by an empty book: such a
    // pair is treated as "no data" (no signal, has_data = false).
    void applySnapshot(std::size_t v, const BookSnapshot& s) {
        const std::int64_t queued = hft::nowNs() - s.publish_ns;
        queue_latency_[v]->recordNs(queued > 0 ? static_cast<std::uint64_t>(queued) : 0);
        s.copyTo(books_[v]);
        ++applied_[v];
        const std::size_t found = detector_.detectVenue(
            v, [this](std::size_t i) -> const OrderBook& { return participates(i) ? books_[i] : no_data_; });
        // 新信号是 getSignals() 的最后 found 条（found <= 2(N-1)，远小于 deque 上限）
        // The new signals are the last found entries of getSignals() (found <= 2(N-1), far below the deque cap)
        const std::deque<ArbitrageSignal>& signals = detector_.getSignals();
        for (std::size_t k = signals.size() - found; k < signals.size(); ++k) logger_.logSignal(signals[k]);
    }

    // 交易所 v 是否参与套利检测与价差日志：只有行情源状态为 LIVE 时才参与（簿还必须非空，由检测器判断）。
    // 不是 LIVE 时本地簿可能已经过期：例如主线程曾被阻塞，断线时发布的空簿还排在队列里、在过期的簿之后。
    // 状态在取出快照时读取：feed 先更新状态再发布，所以它至少与取出的快照一样新。
    // Whether venue v takes part in arbitrage detection and spread logging: only while its feed status is
    // LIVE (the book must also be non-empty, which the detector checks).
    // When not LIVE the local book may already be stale: e.g. the main thread was blocked and the empty
    // book published on disconnect is still in the queue, behind the stale book.
    // The status is read when the snapshot is taken out: a feed updates its status before publishing, so
    // the status is at least as new as the snapshot taken out.
    bool participates(std::size_t v) const noexcept { return feeds_[v]->status() == FeedStatus::Live; }

    void onTick(std::int64_t now) {
        for (auto& f : feeds_) f->checkHealth(now);
        ++ticks_;
        if (ticks_ % static_cast<std::uint64_t>(std::max(1, cfg_.display.spread_log_interval)) == 0) logSpreads();
        if (!opt_.headless) renderFrame(now);
        logger_.flush();
    }

    // 所有有序交易所对（在 buy 买、在 sell 卖）的最优价差；无数据或有一方不是 LIVE 的交易所对跳过
    // Best-price spread of every ordered venue pair (buy at buy, sell at sell); pairs with no data or where
    // either venue is not LIVE are skipped
    void logSpreads() {
        const std::size_t n = books_.size();
        for (std::size_t buy = 0; buy < n; ++buy) {
            if (!participates(buy)) continue;
            for (std::size_t sell = 0; sell < n; ++sell) {
                if (buy == sell || !participates(sell)) continue;
                const std::vector<PriceLevel>& asks = books_[buy].getAsks();
                const std::vector<PriceLevel>& bids = books_[sell].getBids();
                if (asks.empty() || bids.empty()) continue;
                logger_.logSpread(enabled_[buy]->name, asks[0].price, enabled_[sell]->name, bids[0].price,
                                  bids[0].price - asks[0].price);
            }
        }
    }

    void renderFrame(std::int64_t now) {
        // 进程被停止后又继续（SIGCONT）：停止期间 shell 可能改写了屏幕，这一帧先清屏再整屏重画
        // The process was stopped and then continued (SIGCONT): the shell may have rewritten the screen
        // meanwhile, so this frame clears the screen first and redraws it in full
        if (opt_.redraw != nullptr && opt_.redraw->exchange(false, std::memory_order_seq_cst)) frame_shown_ = false;
        const std::vector<VenueView> views = collectViews();
        FrameOptions fo;
        fo.levels = cfg_.display.levels;
        fo.color = opt_.color;
        fo.uptime_s = static_cast<double>(now - start_ns_) / 1e9;
        fo.config_warnings = cfg_.warnings.size();
        const TermSize ts = terminalSize();
        fo.max_rows = ts.rows;  // present() 在最后一行之后不换行：帧可以占满全部行而不滚动
                                // present() writes no newline after the last line:
                                // the frame can fill all rows without scrolling
        const std::string frame = Display::renderFrame(views, detector_, fo);
        Display::present(frame, !frame_shown_, ts.cols > 0 ? static_cast<std::size_t>(ts.cols) : 0, opt_.out);
        frame_shown_ = true;
        ++renders_;
    }

    // headless 状态行："[00:00:03] Binance LIVE 67000.10/67000.20 (812 msgs) | OKX ... | signals 4, net $1.23"
    // Headless status line: "[00:00:03] Binance LIVE 67000.10/67000.20 (812 msgs) | OKX ... | signals 4, net $1.23"
    void printStatusLine(std::int64_t now) {
        std::string line = "[" + Display::formatDuration(static_cast<double>(now - start_ns_) / 1e9) + "]";
        for (std::size_t i = 0; i < feeds_.size(); ++i) {
            const OrderBook& b = books_[i];
            std::string bbo = "--/--";
            if (!b.getBids().empty() && !b.getAsks().empty()) {
                const int prec = Display::pricePrecision(b.getBids()[0].price);
                bbo = Display::plainNumber(b.getBids()[0].price, prec) + "/" +
                      Display::plainNumber(b.getAsks()[0].price, prec);
            }
            line += (i > 0 ? " | " : " ") + enabled_[i]->name + " " + feedStatusName(feeds_[i]->status()) + " " + bbo +
                    " (" + std::to_string(feeds_[i]->stats().messages) + " msgs)";
        }
        line += " | signals " + std::to_string(detector_.getTotalSignals()) + ", net $" +
                Display::plainNumber(detector_.getTotalNetProfit(), 2);
        writeOut(line + "\n");
    }

    void finalReport() {
        const std::vector<VenueView> views = collectViews();
        const bool color = opt_.color && !opt_.headless;
        writeOut(Display::renderFinalReport(views, detector_, color) + "\nGoodbye!\n");
        logger_.logFinalReport(views, detector_);
    }

    const AppConfig cfg_;
    Logger& logger_;
    RunOptions opt_;
    const std::vector<const VenueConfig*> enabled_;   // 已启用的交易所，指向 cfg_.venues（下标 = venue id）
                                                      // enabled venues, pointing into cfg_.venues (index = venue id)
    ArbitrageDetector detector_;
    WakeSignal wake_;                                              // feed 线程 notify，主线程等待（见 run()）
                                                                   // feed threads notify, main thread waits (see run())
    std::vector<std::unique_ptr<MarketDataFeed>> feeds_;
    std::vector<std::thread> threads_;
    std::vector<OrderBook> books_;                                 // 主线程的本地簿（下标 = venue id）
                                                                   // the main thread's local books (index = venue id)
    const OrderBook no_data_;                                      // 不是 LIVE 的交易所在检测中的替身（空簿）
                                                                   // stand-in for non-LIVE venues in detection
                                                                   // (an empty book)
    std::vector<std::unique_ptr<LatencyTracker>> queue_latency_;   // 队列延迟：publish_ns → 主线程取出
                                                                   // queue latency: publish_ns → taken by main thread
    std::vector<std::uint64_t> applied_;                           // 每个交易所已应用的快照数
                                                                   // snapshots applied per venue
    std::int64_t start_ns_ = 0;
    std::uint64_t ticks_ = 0;
    std::uint64_t renders_ = 0;
    bool frame_shown_ = false;
    bool cursor_hidden_ = false;
};
