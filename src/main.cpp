//
// main.cpp
// 主程序入口：解析命令行、安装信号处理、加载配置与日志，然后交给 MonitorApp（include/app.h）运行事件循环与最终报告。
//

#include "app.h"
#include "config.h"
#include "logger.h"
#include "text_util.h"

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

namespace {

// 停止请求（信号处理函数与主循环之间只通过无锁原子量通信，信号安全）
std::atomic<bool> g_stop{false};
std::atomic<int> g_signal_count{0};
std::atomic<bool> g_tui_active{false};  // 终端界面运行中（强制退出时需要恢复光标）

static_assert(std::atomic<bool>::is_always_lock_free && std::atomic<int>::is_always_lock_free,
              "signal handler requires lock-free atomics");

// SIGINT / SIGTERM：第一次请求优雅退出（主循环停止 feed、输出最终报告）；
// 第二次立即退出（只调用 async-signal-safe 的 write / _Exit）
void onSignal(int sig) {
    g_stop.store(true, std::memory_order_release);
    if (g_signal_count.fetch_add(1, std::memory_order_relaxed) == 0) return;
    if (g_tui_active.load(std::memory_order_relaxed)) {
        static const char kShowCursor[] = "\033[?25h\n";
        const ssize_t r = ::write(STDOUT_FILENO, kShowCursor, sizeof(kShowCursor) - 1);
        (void)r;
    }
    static const char kMsg[] = "[Signal] second signal received, exiting immediately\n";
    const ssize_t r = ::write(STDERR_FILENO, kMsg, sizeof(kMsg) - 1);
    (void)r;
    std::_Exit(128 + sig);
}

void installSignalHandlers() {
    struct sigaction sa {};
    sa.sa_handler = onSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;  // 被打断的 write 等系统调用自动重启（主线程的短睡眠被打断也无妨）
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
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
