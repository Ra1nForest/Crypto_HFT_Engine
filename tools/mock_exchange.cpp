//
// mock_exchange.cpp
// 离线模拟交易所：Binance / OKX / Bybit 三家的订单簿来自真实的撮合引擎与模拟交易者，按真实协议经 TLS WebSocket 推送
// （Binance 增量深度 + REST 快照、OKX books 序列号与 checksum、Bybit orderbook snapshot/delta），让监控程序完全离线运行。
// 用法：./build/mock_exchange [--binance-port N] [--okx-port N] [--bybit-port N] [--duration S] [--seed X] [--help]
//

#include "cli_util.h"
#include "mock_exchange.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace {

using mock_exchange::MockExchange;
using mock_exchange::MockExchangeOptions;

volatile std::sig_atomic_t g_signal = 0;

extern "C" void onSignal(int sig) { g_signal = sig; }

struct Options {
    MockExchangeOptions ex;
    std::string ca_out = "/tmp/mock_exchange_ca.pem";
    double duration_s = 0.0;          // 0 = 直到 Ctrl+C / SIGTERM
    double status_interval_s = 5.0;   // 0 = 不打印状态行
    bool verbose = false;
};

void printUsage(std::FILE* out) {
    std::fprintf(out,
                 "Usage: mock_exchange [options]\n"
                 "Offline mock exchange for crypto_hft_engine: three venues whose order books come from real\n"
                 "matching engines driven by simulated market makers and takers (shared random-walk fair value,\n"
                 "per-venue noise and occasional dislocations), streamed over TLS WebSockets in the real wire\n"
                 "formats:\n"
                 "  Binance  wss://localhost:<port>/ws/btcusdt@depth@100ms   diff depth (U/u), also @depth,\n"
                 "           @depth5/10/20[@100ms], /stream?streams=..., SUBSCRIBE/UNSUBSCRIBE;\n"
                 "           REST https://localhost:<port>/api/v3/depth?symbol=BTCUSDT&limit=1000\n"
                 "  OKX      wss://localhost:<port>/ws/v5/public   books (400 levels, seqId/prevSeqId, checksum),\n"
                 "           books5, bbo-tbt for BTC-USDT; \"ping\" -> \"pong\"\n"
                 "  Bybit    wss://localhost:<port>/v5/public/spot   orderbook.{1,50,200}.BTCUSDT snapshot/delta;\n"
                 "           {\"op\":\"ping\"} -> pong\n"
                 "\n"
                 "  --binance-port N      Binance WebSocket + REST port (default 19443; 0 = any free port)\n"
                 "  --okx-port N          OKX WebSocket port (default 18443)\n"
                 "  --bybit-port N        Bybit WebSocket port (default 18444)\n"
                 "  --bind ADDR           listen address (default 127.0.0.1)\n"
                 "  --ca-out PATH         write the self-signed CA certificate (PEM) here for the monitor's\n"
                 "                        connection.ca_file (default /tmp/mock_exchange_ca.pem; \"\" = don't)\n"
                 "  --duration S          stop after S seconds (default 0 = run until Ctrl+C / SIGTERM)\n"
                 "  --seed X              RNG seed: same seed = same simulated order flow (default 1)\n"
                 "  --warmup S            simulated seconds before the ports open (default 5)\n"
                 "  --status-interval S   print a status line every S seconds (default 5; 0 = off)\n"
                 "  --drop-rate P         fault injection: withhold each pushed message from a session with\n"
                 "                        probability P (0..1, default 0) so clients see gaps and resync\n"
                 "  --verbose             log every connection, subscription and REST request to stderr\n"
                 "  --help                show this help\n"
                 "\n"
                 "Prints \"mock_exchange: ready\" once all three ports are listening and the CA file is written.\n"
                 "Exit status: 0 = clean shutdown, 1 = startup failure (e.g. port in use), 2 = bad command line.\n");
}

using cli_util::parseUnsigned;

bool parseSeconds(const char* text, double& out) {
    if (text == nullptr || *text == '\0') return false;
    errno = 0;
    char* end = nullptr;
    const double v = std::strtod(text, &end);
    if (errno != 0 || end == text || *end != '\0' || !(v >= 0.0) || v > 1e9) return false;
    out = v;
    return true;
}

// 返回 0 = 继续运行，1 = 已打印帮助，2 = 参数错误
int parseOptions(int argc, char** argv, Options& o) {
    auto bad = [](const std::string& msg) {
        std::fprintf(stderr, "mock_exchange: %s\n(try --help)\n", msg.c_str());
        return 2;
    };
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(stdout);
            return 1;
        }
        if (arg == "--verbose") {
            o.verbose = true;
            continue;
        }
        static const char* const kWithValue[] = {"--binance-port", "--okx-port", "--bybit-port", "--bind",
                                                 "--ca-out", "--duration", "--seed", "--warmup",
                                                 "--drop-rate", "--status-interval"};
        if (std::find(std::begin(kWithValue), std::end(kWithValue), arg) == std::end(kWithValue)) {
            return bad("unknown option '" + arg + "'");
        }
        const char* value = i + 1 < argc ? argv[i + 1] : nullptr;
        if (value == nullptr) return bad("option '" + arg + "' needs a value");
        ++i;
        std::uint64_t u = 0;
        double d = 0.0;
        if (arg == "--binance-port" || arg == "--okx-port" || arg == "--bybit-port") {
            if (!parseUnsigned(value, u) || u > 65535) return bad(arg + " needs a port number in [0, 65535]");
            const auto port = static_cast<unsigned short>(u);
            if (arg == "--binance-port") o.ex.binance_port = port;
            if (arg == "--okx-port") o.ex.okx_port = port;
            if (arg == "--bybit-port") o.ex.bybit_port = port;
        } else if (arg == "--bind") {
            o.ex.bind_address = value;
        } else if (arg == "--ca-out") {
            o.ca_out = value;
        } else if (arg == "--duration") {
            if (!parseSeconds(value, d)) return bad("--duration needs a non-negative number of seconds");
            o.duration_s = d;
        } else if (arg == "--seed") {
            if (!parseUnsigned(value, u)) return bad("--seed needs a non-negative integer");
            o.ex.seed = u;
        } else if (arg == "--warmup") {
            if (!parseSeconds(value, d) || d > 3600.0) return bad("--warmup needs a number of seconds in [0, 3600]");
            o.ex.warmup_s = d;
        } else if (arg == "--drop-rate") {
            if (!parseSeconds(value, d) || d > 1.0) return bad("--drop-rate needs a probability in [0, 1]");
            o.ex.drop_rate = d;
        } else if (arg == "--status-interval") {
            if (!parseSeconds(value, d)) return bad("--status-interval needs a non-negative number of seconds");
            o.status_interval_s = d;
        } else {
            return bad("unknown option '" + arg + "'");
        }
    }
    const unsigned short ports[3] = {o.ex.binance_port, o.ex.okx_port, o.ex.bybit_port};
    for (int a = 0; a < 3; ++a) {
        for (int b = a + 1; b < 3; ++b) {
            if (ports[a] != 0 && ports[a] == ports[b]) return bad("the three venues need different ports");
        }
    }
    return 0;
}

// 连接事件日志：整行一次写出（多个连接线程的输出不会交错）
void logLine(const std::string& line) {
    static std::mutex mutex;
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    std::tm tm{};
    localtime_r(&t, &tm);
    char stamp[32];
    std::snprintf(stamp, sizeof(stamp), "%02d:%02d:%02d.%03lld", tm.tm_hour, tm.tm_min, tm.tm_sec,
                  static_cast<long long>(ms));
    const std::string full = std::string(stamp) + " " + line + "\n";
    std::lock_guard<std::mutex> lock(mutex);
    std::fwrite(full.data(), 1, full.size(), stderr);
    std::fflush(stderr);
}

std::string fmt(const char* format, double v) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), format, v);
    return buf;
}

double percent(std::uint64_t part, std::uint64_t whole) {
    return whole == 0 ? 0.0 : 100.0 * static_cast<double>(part) / static_cast<double>(whole);
}

void printStatus(const MockExchange::Status& st, double wall_s) {
    std::string line = "[" + fmt("%7.1f", wall_s) + "s] fair " + fmt("%.2f", st.market_fair);
    for (const auto& v : st.venues) {
        line += " | " + v.name + " " + fmt("%.2f", v.best_bid) + "/" + fmt("%.2f", v.best_ask) + " (" +
                std::to_string(v.net.sessions_open) + " conn, " + std::to_string(v.net.messages_sent) + " msgs)";
    }
    if (st.arb_samples > 0) {
        line += " | max cross now " + fmt("%+.1f", st.last_gap_bps) + " bp (" + st.last_gap_pair +
                "); since start crossed " + fmt("%.1f", percent(st.arb_cross, st.arb_samples)) + "% of samples, " +
                fmt("%.1f", percent(st.arb_cross_after_fees, st.arb_samples)) + "% after 0.1% fees";
    }
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

void printSummary(const MockExchange::Status& st, double wall_s) {
    std::printf("\nmock_exchange: stopped after %.1f s (%.1f simulated seconds incl. warm-up)\n", wall_s, st.sim_seconds);
    std::printf("  %-8s %10s %10s %7s %9s %12s %9s %9s %8s %5s %8s\n", "venue", "best bid", "best ask", "orders",
                "trades", "dislocations", "sessions", "messages", "MB sent", "REST", "dropped");
    for (const auto& v : st.venues) {
        std::printf("  %-8s %10.2f %10.2f %7zu %9" PRIu64 " %12" PRIu64 " %9" PRIu64 " %9" PRIu64 " %8.2f %5" PRIu64
                    " %8" PRIu64 "\n",
                    v.name.c_str(), v.best_bid, v.best_ask, v.orders, v.trades, v.large_dislocations,
                    v.net.sessions_total, v.net.messages_sent, static_cast<double>(v.net.bytes_sent) / 1e6,
                    v.net.rest_requests, v.net.dropped);
    }
    std::printf("  cross-venue crosses: %.1f%% of %" PRIu64 " samples (every 100 ms); %.1f%% still profitable after "
                "0.1%% fees on both legs\n",
                percent(st.arb_cross, st.arb_samples), st.arb_samples, percent(st.arb_cross_after_fees, st.arb_samples));
    if (st.lag_resets > 0) {
        std::printf("  note: the simulation fell behind real time %" PRIu64 " time(s) and skipped ahead\n",
                    st.lag_resets);
    }
    std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    const int parsed = parseOptions(argc, argv, o);
    if (parsed == 1) return 0;
    if (parsed == 2) return 2;

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGPIPE, SIG_IGN);  // 写已断开的连接只应返回错误
    if (o.verbose) o.ex.log = logLine;

    std::unique_ptr<MockExchange> ex;
    try {
        ex = std::make_unique<MockExchange>(o.ex);
        if (!o.ca_out.empty()) ex->writeCaFile(o.ca_out);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "mock_exchange: cannot start: %s\n", e.what());
        return 1;
    }

    const std::string host = o.ex.bind_address == "127.0.0.1" ? std::string("localhost") : o.ex.bind_address;
    std::printf("mock_exchange: seed %" PRIu64 ", %.1f s warm-up, fair value %.2f\n", o.ex.seed, o.ex.warmup_s,
                ex->status().market_fair);
    if (o.ex.drop_rate > 0.0) {
        std::printf("  fault injection: each pushed message is withheld from a session with probability %g\n",
                    o.ex.drop_rate);
    }
    std::printf("  Binance  wss://%s:%u/ws/btcusdt@depth@100ms  (REST https://%s:%u/api/v3/depth?symbol=BTCUSDT&limit=1000)\n",
                host.c_str(), static_cast<unsigned>(ex->binancePort()), host.c_str(),
                static_cast<unsigned>(ex->binancePort()));
    std::printf("  OKX      wss://%s:%u/ws/v5/public  (books / books5 / bbo-tbt, instId BTC-USDT)\n", host.c_str(),
                static_cast<unsigned>(ex->okxPort()));
    std::printf("  Bybit    wss://%s:%u/v5/public/spot  (orderbook.1/50/200.BTCUSDT)\n", host.c_str(),
                static_cast<unsigned>(ex->bybitPort()));
    if (!o.ca_out.empty()) std::printf("  CA certificate: %s\n", o.ca_out.c_str());
    std::printf("mock_exchange: ready\n");
    std::fflush(stdout);

    const auto start = std::chrono::steady_clock::now();
    auto elapsed = [&start] {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    };
    double next_status = o.status_interval_s;
    while (g_signal == 0) {
        const double t = elapsed();
        if (o.duration_s > 0.0 && t >= o.duration_s) break;
        if (o.status_interval_s > 0.0 && t >= next_status) {
            printStatus(ex->status(), t);
            next_status += o.status_interval_s;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    const double wall = elapsed();
    if (g_signal != 0) std::printf("mock_exchange: signal %d, shutting down\n", static_cast<int>(g_signal));
    ex->stop();
    printSummary(ex->status(), wall);
    return 0;
}
