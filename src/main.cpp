//
// main.cpp
// 主程序入口：负责初始化配置、日志、行情订阅与套利检测主循环。
// 该文件串联了各模块：MarketDataFeed、ArbitrageDetector、Display、Logger。
// （本轮只做最小适配：两个交易所的 feed 改为经 SPSC 队列发布快照，主循环每轮把队列排空到本地 OrderBook。）
//

#include "market_data_feed.h"
#include "arbitrage_detector.h"
#include "display.h"
#include "logger.h"
#include "config.h"

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

// 全局运行标记：用于线程间安全退出
std::atomic<bool> g_running{true};

// 信号处理：捕获 Ctrl+C / SIGTERM，通知主循环退出（无锁原子写，信号安全）
void signalHandler(int) {
    g_running = false;
}

// 由交易所配置构建 feed 配置
static FeedConfig makeFeedConfig(Exchange exchange, std::uint32_t venue_id, const std::string& name,
                                 const ExchangeConfig& ex) {
    FeedConfig f;
    f.exchange = exchange;
    f.venue_id = venue_id;
    f.name = name;
    f.host = ex.host;
    f.port = ex.port;
    f.path = ex.path;
    f.symbol = ex.symbol;
    f.subscribe_msg = ex.subscribe_msg;
    return f;
}

// 把 feed 队列中的快照全部取出，最新的一个留在 book 中；返回是否有更新
static bool drainFeed(MarketDataFeed& feed, OrderBook& book) {
    bool updated = false;
    auto& queue = feed.queue();
    while (const BookSnapshot* snap = queue.front()) {
        snap->copyTo(book);
        queue.pop();
        updated = true;
    }
    return updated;
}

// 最终报告中的连接统计
static void printFeedSummary(const MarketDataFeed& feed) {
    const FeedStats s = feed.stats();
    std::cout << "  " << feed.getName() << ": " << feedStatusName(feed.status())
              << "  msgs=" << s.messages << "  published=" << s.published
              << "  dropped=" << s.dropped_snapshots << "  reconnects=" << s.reconnects
              << "  connect_failures=" << s.connect_failures << "  stalls=" << s.stalls
              << "  gaps=" << s.gaps << "  resyncs=" << s.resyncs << std::endl;
    const std::string err = feed.lastError();
    if (!err.empty()) std::cout << "    last error: " << err << std::endl;
}

int main() {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    std::cout << "╔══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║       Crypto HFT Engine v1.0                 ║" << std::endl;
    std::cout << "║  Dual Exchange Arbitrage Monitor             ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;

    // 读取配置：包含交易所连接信息、套利参数与显示刷新频率
    AppConfig cfg = ConfigReader::load("config.json");

    // 初始化日志：输出系统启动信息与关键参数
    Logger logger("logs");
    logger.log("=== Crypto HFT Engine v1.0 ===");
    logger.log("Config: fee_rate=" + std::to_string(cfg.arbitrage.fee_rate)
             + " min_profit=" + std::to_string(cfg.arbitrage.min_profit)
             + " refresh=" + std::to_string(cfg.display.refresh_ms) + "ms");

    // 配置交易所：构建 Binance 与 OKX 的行情订阅配置
    MarketDataFeed binance_feed(makeFeedConfig(Exchange::BINANCE, 0, "Binance", cfg.binance));
    MarketDataFeed okx_feed(makeFeedConfig(Exchange::OKX, 1, "OKX", cfg.okx));
    ArbitrageDetector detector(cfg.arbitrage.fee_rate, cfg.arbitrage.min_profit);

    // 主线程持有的本地订单簿（由 feed 发布的快照更新；标签与旧 feed 一致："<name>:<symbol>"）
    OrderBook binance_book("Binance:" + cfg.binance.symbol);
    OrderBook okx_book("OKX:" + cfg.okx.symbol);

    // 启动行情订阅线程
    logger.log("Starting market data feeds...");
    std::cout << "Starting market data feeds..." << std::endl;

    // 启动行情线程：每个交易所一个接收线程
    std::thread binance_thread([&binance_feed]() {
        binance_feed.run();
    });

    std::thread okx_thread([&okx_feed]() {
        okx_feed.run();
    });

    // 等待行情：最多 3 秒，两个 feed 都 LIVE 或收到退出信号时提前结束
    std::cout << "Waiting for market data..." << std::endl;
    for (int i = 0; i < 60 && g_running; ++i) {
        if (binance_feed.status() == FeedStatus::Live && okx_feed.status() == FeedStatus::Live) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    logger.log("Market data feeds started");

    std::cout << std::fixed << std::setprecision(2);

    int loop_count = 0;

    // 主循环：周期性排空快照队列并渲染 UI，检测套利信号
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.display.refresh_ms));

        drainFeed(binance_feed, binance_book);
        drainFeed(okx_feed, okx_book);

        // 停顿检测：长时间无数据的连接会被踢断并重连
        const std::int64_t now = hft::nowNs();
        binance_feed.checkHealth(now);
        okx_feed.checkHealth(now);

        // 周期性记录价差：方便回放与离线分析
        loop_count++;
        if (loop_count % cfg.display.spread_log_interval == 0) {
            auto a_ask = binance_book.getBestAsk();
            auto b_bid = okx_book.getBestBid();
            auto b_ask = okx_book.getBestAsk();
            auto a_bid = binance_book.getBestBid();

            logger.logSpread("Binance", a_ask.price, "OKX", b_bid.price,
                           b_bid.price - a_ask.price);
            logger.logSpread("OKX", b_ask.price, "Binance", a_bid.price,
                           a_bid.price - b_ask.price);
            logger.flush();
        }

        // 显示：渲染双边盘口、套利面板与延迟统计
        Display::render(
            binance_book, okx_book, detector,
            binance_feed.getLatencyTracker(),
            okx_feed.getLatencyTracker(),
            binance_feed.status() == FeedStatus::Live,
            okx_feed.status() == FeedStatus::Live,
            cfg.display.levels
        );
        // 套利检测：检测是否存在可执行套利机会
        bool signal_found = detector.detect(binance_book, okx_book);

        if (signal_found) {
            auto& signals = detector.getSignals();
            if (!signals.empty()) {
                logger.logSignal(signals.back());
            }
        }
    }

    // 优雅退出：停止订阅、等待线程收尾并输出最终报告
    std::cout << "\nClosing connections..." << std::endl;
    logger.log("Shutting down...");

    // 先停止 feed：踢断阻塞中的读取/连接，唤醒退避等待
    binance_feed.stop();
    okx_feed.stop();

    // 等待线程结束（比 detach 更安全，不会资源泄漏）
    if (binance_thread.joinable()) binance_thread.join();
    if (okx_thread.joinable()) okx_thread.join();

    // 最终报告：延迟统计 + 连接统计 + 套利统计
    std::cout << "\n═══ Final Report ═══" << std::endl;
    Display::printDualLatencyReport(
        binance_feed.getLatencyTracker(),
        okx_feed.getLatencyTracker()
    );
    printFeedSummary(binance_feed);
    printFeedSummary(okx_feed);
    std::cout << "  Arbitrage signals: " << detector.getTotalSignals() << std::endl;
    std::cout << "  Total net profit:  $" << detector.getTotalNetProfit() << std::endl;

    logger.logFinalReport(
        binance_feed.getLatencyTracker(),
        okx_feed.getLatencyTracker(),
        detector
    );

    std::cout << "\nGoodbye!" << std::endl;
    return 0;
}
