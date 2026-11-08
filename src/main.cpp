//
// main.cpp
// 主程序入口：解析命令行、安装信号处理、加载配置与日志，然后交给 MonitorApp（include/app.h）运行事件循环与最终报告。
// Program entry point: parses the command line, installs signal handling, loads the configuration and
// logging, then hands over to MonitorApp (include/app.h) to run the event loop and the final report.
//

#include "app.h"
#include "config.h"
#include "display.h"
#include "logger.h"
#include "text_util.h"

#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

namespace {

// 信号处理函数只调用 async-signal-safe 的函数（clock_gettime、write、sigaction、sigemptyset、sigaddset、
// pthread_sigmask、raise、_Exit），与主循环之间只通过无锁原子量通信。处理函数都在主线程上运行：
// feed 线程创建时屏蔽了这些信号（见 MonitorApp::startFeeds）。
// The signal handlers call only async-signal-safe functions (clock_gettime, write, sigaction, sigemptyset,
// sigaddset, pthread_sigmask, raise, _Exit) and talk to the main loop only through lock-free atomics. The
// handlers all run on the main thread: the feed threads are created with these signals blocked (see
// MonitorApp::startFeeds).
std::atomic<bool> g_stop{false};                // 请求优雅退出（MonitorApp::run 的 stop 标志）
                                                // requests a graceful exit (MonitorApp::run's stop flag)
std::atomic<std::int64_t> g_first_stop_ns{-1};  // 第一次 SIGINT / SIGTERM 的 CLOCK_MONOTONIC 时刻；-1 = 还没有
                                                // CLOCK_MONOTONIC time of the first SIGINT / SIGTERM; -1 = none yet
std::atomic<CursorState> g_cursor{CursorState::Visible};  // 终端界面的光标（MonitorApp 维护，见 RunOptions::cursor_state）
                                                          // cursor of the terminal UI (maintained by MonitorApp,
                                                          // see RunOptions::cursor_state)
std::atomic<bool> g_redraw{false};              // SIGCONT 之后下一帧整屏重画（MonitorApp 取走，见 RunOptions::redraw）
                                                // redraw the whole screen on the next frame after SIGCONT
                                                // (consumed by MonitorApp, see RunOptions::redraw)
std::atomic<bool> g_handle_tstp{false};         // 接管了 SIGTSTP（启动时被忽略则不接管）
                                                // took over SIGTSTP (not done if it was ignored at startup)

static_assert(std::atomic<bool>::is_always_lock_free && std::atomic<std::int64_t>::is_always_lock_free &&
                  std::atomic<CursorState>::is_always_lock_free,
              "signal handlers require lock-free atomics");

// 光标可能处于隐藏状态（Hidden，或显示序列还没写完的 Restoring）：停止 / 强制退出之前要写显示序列
// The cursor may be hidden (Hidden, or Restoring while the show sequence has not been fully written): the
// show sequence must be written before stopping / force-exiting
bool cursorMayBeHidden() noexcept { return g_cursor.load(std::memory_order_seq_cst) != CursorState::Visible; }

// 同一次 Ctrl+C 可能到达两次：终端把 SIGINT 发给整个前台进程组，timeout 之类的包装程序收到后又转发一次
// （第一次已被处理，两次不会合并）。第一次之后这么久之内的 SIGINT / SIGTERM 视为同一次请求。
// The same Ctrl+C may arrive twice: the terminal sends SIGINT to the whole foreground process group, and a
// wrapper such as timeout forwards it once more on receipt (the first has already been handled, so the two
// do not merge). A SIGINT / SIGTERM within this long after the first one counts as the same request.
constexpr std::int64_t kDuplicateSignalWindowNs = 1000000000;

std::int64_t monotonicNs() noexcept {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

// 写完整个缓冲区（被信号打断时重试；出错时放弃）
// Writes the whole buffer (retries when interrupted by a signal; gives up on error)
void writeAll(int fd, const char* s, std::size_t n) noexcept {
    while (n > 0) {
        const ssize_t r = ::write(fd, s, n);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return;
        s += r;
        n -= static_cast<std::size_t>(r);
    }
}

// 写出字符数组（如 Display::kShowCursor），不含结尾的 '\0'
// Writes a character array (e.g. Display::kShowCursor) without the trailing '\0'
template <std::size_t N>
void writeAll(int fd, const char (&s)[N]) noexcept {
    writeAll(fd, s, N - 1);
}

void installHandler(int sig, void (*handler)(int)) noexcept {
    struct sigaction sa {};
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;  // 被打断的 write 等系统调用自动重启（主循环的等待有上限，不受影响）
                               // interrupted system calls such as write restart automatically
                               // (the main loop's waits are bounded, so they are unaffected)
    ::sigaction(sig, &sa, nullptr);
}

// SIGINT / SIGTERM：第一次请求优雅退出（主循环停止 feed、输出最终报告）；1 s 之内的重复信号忽略；
// 更晚的又一次立即退出（终端界面先恢复光标）。
// SIGINT / SIGTERM: the first one requests a graceful exit (the main loop stops the feeds and prints the
// final report); duplicates within 1 s are ignored; a later one exits immediately (the terminal UI
// restores the cursor first).
void onStopSignal(int sig) {
    const int saved_errno = errno;
    g_stop.store(true, std::memory_order_release);
    const std::int64_t now = monotonicNs();
    std::int64_t first = -1;
    if (g_first_stop_ns.compare_exchange_strong(first, now, std::memory_order_relaxed) ||
        now - first < kDuplicateSignalWindowNs) {
        errno = saved_errno;
        return;
    }
    if (cursorMayBeHidden()) {
        // 与 MonitorApp::restoreCursor 相同：显示光标并换行
        // Same as MonitorApp::restoreCursor: show the cursor and start a new line
        writeAll(STDOUT_FILENO, Display::kShowCursor);
        writeAll(STDOUT_FILENO, "\n");
    }
    writeAll(STDERR_FILENO, "[Signal] second signal received, exiting immediately\n");
    std::_Exit(128 + sig);
}

// SIGHUP（终端窗口关闭、SSH 断开）：与 SIGTERM 一样优雅退出（日志刷新、最终报告），但从不强制退出：
// 一次挂断常常带来两个 SIGHUP（shell 转发给作业一次，内核向前台进程组再发一次）。
// SIGHUP (terminal window closed, SSH disconnected): exits gracefully like SIGTERM (log flush, final
// report), but never force-exits: one hangup often brings two SIGHUPs (the shell forwards one to the job,
// and the kernel sends another to the foreground process group).
void onHangup(int) { g_stop.store(true, std::memory_order_release); }

// SIGTSTP（Ctrl+Z）：终端界面隐藏了光标时先恢复它，再按默认动作停止进程：重置为 SIG_DFL 并重新发出。
// 处理函数执行期间 SIGTSTP 被阻塞，所以 raise 的信号先挂起（与期间又按的 Ctrl+Z 合并成一个），解除阻塞时
// 进程才停止，直到 SIGCONT（onContinue 重新接管 SIGTSTP）。
// SIGTSTP (Ctrl+Z): if the terminal UI has hidden the cursor, restore it first, then stop the process with
// the default action: reset to SIG_DFL and raise it again.
// SIGTSTP is blocked while the handler runs, so the raised signal stays pending (merged into one with any
// Ctrl+Z pressed meanwhile); the process stops only once SIGTSTP is unblocked, and stays stopped until
// SIGCONT (onContinue takes over SIGTSTP again).
void onTerminalStop(int) {
    const int saved_errno = errno;
    if (cursorMayBeHidden()) writeAll(STDOUT_FILENO, Display::kShowCursor);
    installHandler(SIGTSTP, SIG_DFL);
    ::raise(SIGTSTP);
    sigset_t tstp;
    sigemptyset(&tstp);
    sigaddset(&tstp, SIGTSTP);
    ::pthread_sigmask(SIG_UNBLOCK, &tstp, nullptr);
    errno = saved_errno;
}

// SIGCONT（fg、bg、kill -CONT）：重新接管 SIGTSTP；终端界面再次隐藏光标，并让下一帧清屏后整屏重画
// （停止期间 shell 可能改写了屏幕）。
// SIGCONT (fg, bg, kill -CONT): takes over SIGTSTP again; the terminal UI hides the cursor again and has
// the next frame clear the screen and redraw it in full (the shell may have overwritten the screen while
// the process was stopped).
void onContinue(int) {
    const int saved_errno = errno;
    if (g_handle_tstp.load(std::memory_order_relaxed)) installHandler(SIGTSTP, onTerminalStop);
    if (g_cursor.load(std::memory_order_seq_cst) == CursorState::Hidden) {  // Restoring 时不能再隐藏
                                                                            // must not hide again while Restoring
        writeAll(STDOUT_FILENO, Display::kHideCursor);
        g_redraw.store(true, std::memory_order_seq_cst);
    }
    errno = saved_errno;
}

// 启动时被忽略（SIG_IGN，由父进程继承而来）的信号保持忽略：nohup 让 SIGHUP 被忽略，
// 没有作业控制的 shell 让后台进程忽略 SIGTSTP
// Signals ignored at startup (SIG_IGN, inherited from the parent process) stay ignored: nohup makes
// SIGHUP ignored, and a shell without job control makes background processes ignore SIGTSTP
bool ignoredAtStartup(int sig) noexcept {
    struct sigaction old {};
    return ::sigaction(sig, nullptr, &old) == 0 && old.sa_handler == SIG_IGN;
}

void installSignalHandlers() {
    installHandler(SIGINT, onStopSignal);
    installHandler(SIGTERM, onStopSignal);
    if (!ignoredAtStartup(SIGHUP)) installHandler(SIGHUP, onHangup);
    if (!ignoredAtStartup(SIGTSTP)) {
        g_handle_tstp.store(true, std::memory_order_relaxed);
        installHandler(SIGTSTP, onTerminalStop);
    }
    installHandler(SIGCONT, onContinue);
    // 写已关闭的管道时返回 EPIPE 而不是被 SIGPIPE 杀死：例如 "| tee" 随终端一起被挂断后，
    // 优雅退出仍能把最终报告写进日志文件
    // Writing to a closed pipe returns EPIPE instead of getting killed by SIGPIPE: e.g. after "| tee" is hung
    // up together with the terminal, the graceful exit can still write the final report into the log file
    ::signal(SIGPIPE, SIG_IGN);
}

void printBanner() {
    constexpr std::size_t kInner = 46;
    const std::string line = text_util::repeat("═", kInner);
    std::string out = "╔" + line + "╗\n";
    out += "║" + text_util::center(kAppVersion, kInner) + "║\n";
    out += "║" + text_util::center("Cross-Exchange Arbitrage Monitor", kInner) + "║\n";
    out += "╚" + line + "╝\n\n";
    std::fputs(out.c_str(), stdout);
    std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
    CliOptions cli;
    std::string error;
    const std::string program = argc > 0 && argv[0] != nullptr ? argv[0] : "crypto_hft_engine";
    if (!parseCommandLine(argc, argv, cli, error)) {
        std::fprintf(stderr, "Error: %s\n\n%s", error.c_str(), usageText(program).c_str());
        return 2;
    }
    if (cli.help) {
        std::fputs(usageText(program).c_str(), stdout);
        return 0;
    }

    installSignalHandlers();
    printBanner();

    // 配置：逐字段回退 + 告警（stderr）。默认的 config.json 缺失时使用内置默认值；
    // 显式给出的 --config 文件读不了或不是合法 JSON 则报错退出（否则会悄悄连到默认的真实交易所）
    // Configuration: per-field fallback + warnings (stderr). A missing default config.json means the built-in
    // defaults are used; an explicitly given --config file that cannot be read or is not valid JSON exits with
    // an error (otherwise it would silently connect to the default real exchanges)
    const AppConfig cfg = ConfigReader::load(cli.config_path, false);
    if (cli.config_given && cfg.source.empty()) {
        for (const std::string& w : cfg.warnings) std::fprintf(stderr, "%s\n", w.c_str());
        std::fprintf(stderr, "Error: cannot use --config %s\n", text_util::sanitize(cli.config_path).c_str());
        return 2;
    }
    ConfigReader::printReport(cfg);

    // 日志目录相对当前目录
    // The log directory is relative to the current directory
    Logger logger("logs");

    RunOptions opt;
    opt.headless = cli.headless;
    opt.duration_s = cli.duration_s;
    opt.busy_poll = cli.busy_poll;
    opt.out = stdout;
    opt.cursor_state = &g_cursor;
    opt.redraw = &g_redraw;
    const char* no_color = std::getenv("NO_COLOR");
    opt.color = no_color == nullptr || no_color[0] == '\0';

    try {
        MonitorApp app(cfg, logger, opt);
        return app.run(g_stop);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Fatal: %s\n", e.what());
        logger.warn(std::string("Fatal: ") + e.what());
        return 1;
    }
}
