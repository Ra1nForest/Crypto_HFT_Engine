//
// main.cpp
// 主程序入口：解析命令行、安装信号处理、加载配置与日志，然后交给 MonitorApp（include/app.h）运行事件循环与最终报告。
//

#include "app.h"
#include "config.h"
#include "display.h"
#include "logger.h"
#include "text_util.h"

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

// 信号处理函数只调用 async-signal-safe 的函数（clock_gettime、write、_Exit），
// 与主循环之间只通过无锁原子量通信。
std::atomic<bool> g_stop{false};                // 请求优雅退出（MonitorApp::run 的 stop 标志）
std::atomic<std::int64_t> g_first_stop_ns{-1};  // 第一次 SIGINT / SIGTERM 的 CLOCK_MONOTONIC 时刻；-1 = 还没有
std::atomic<bool> g_tui_active{false};          // 终端界面运行中（强制退出时需要恢复光标）

static_assert(std::atomic<bool>::is_always_lock_free && std::atomic<std::int64_t>::is_always_lock_free,
              "signal handlers require lock-free atomics");

// 同一次 Ctrl+C 可能到达两次：终端把 SIGINT 发给整个前台进程组，timeout 之类的包装程序收到后又转发一次
// （第一次已被处理，两次不会合并）。第一次之后这么久之内的 SIGINT / SIGTERM 视为同一次请求。
constexpr std::int64_t kDuplicateSignalWindowNs = 1000000000;

std::int64_t monotonicNs() noexcept {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

// 写完整个缓冲区（被信号打断时重试；出错时放弃）
void writeAll(int fd, const char* s, std::size_t n) noexcept {
    while (n > 0) {
        const ssize_t r = ::write(fd, s, n);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return;
        s += r;
        n -= static_cast<std::size_t>(r);
    }
}

void installHandler(int sig, void (*handler)(int)) noexcept {
    struct sigaction sa {};
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;  // 被打断的 write 等系统调用自动重启（主循环的等待有上限，不受影响）
    ::sigaction(sig, &sa, nullptr);
}

// SIGINT / SIGTERM：第一次请求优雅退出（主循环停止 feed、输出最终报告）；1 s 之内的重复信号忽略；
// 更晚的又一次立即退出（终端界面先恢复光标）。
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
    if (g_tui_active.load(std::memory_order_relaxed)) {
        // 与 MonitorApp::restoreCursor 相同：显示光标并换行
        writeAll(STDOUT_FILENO, Display::kShowCursor, sizeof(Display::kShowCursor) - 1);
        writeAll(STDOUT_FILENO, "\n", 1);
    }
    static const char kMsg[] = "[Signal] second signal received, exiting immediately\n";
    writeAll(STDERR_FILENO, kMsg, sizeof(kMsg) - 1);
    std::_Exit(128 + sig);
}

void installSignalHandlers() {
    installHandler(SIGINT, onStopSignal);
    installHandler(SIGTERM, onStopSignal);
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
    const AppConfig cfg = ConfigReader::load(cli.config_path, false);
    if (cli.config_given && cfg.source.empty()) {
        for (const std::string& w : cfg.warnings) std::fprintf(stderr, "%s\n", w.c_str());
        std::fprintf(stderr, "Error: cannot use --config %s\n", text_util::sanitize(cli.config_path).c_str());
        return 2;
    }
    ConfigReader::printReport(cfg);

    // 日志目录相对当前目录
    Logger logger("logs");

    RunOptions opt;
    opt.headless = cli.headless;
    opt.duration_s = cli.duration_s;
    opt.busy_poll = cli.busy_poll;
    opt.out = stdout;
    const char* no_color = std::getenv("NO_COLOR");
    opt.color = no_color == nullptr || no_color[0] == '\0';

    try {
        MonitorApp app(cfg, logger, opt);
        g_tui_active.store(!cli.headless, std::memory_order_relaxed);
        const int rc = app.run(g_stop);
        g_tui_active.store(false, std::memory_order_relaxed);
        return rc;
    } catch (const std::exception& e) {
        g_tui_active.store(false, std::memory_order_relaxed);
        std::fprintf(stderr, "Fatal: %s\n", e.what());
        logger.warn(std::string("Fatal: ") + e.what());
        return 1;
    }
}
