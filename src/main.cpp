//
// main.cpp
// 主程序入口：负责初始化配置、日志、行情订阅与套利检测主循环。
// 该文件串联了各模块：MarketDataFeed、ArbitrageDetector、Display、Logger。
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

// 信号处理：捕获 Ctrl+C，通知主循环退出
void signalHandler(int) {
    g_running = false;
}

int main() {
    std::signal(SIGINT, signalHandler);

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
    FeedConfig binance_config;
    binance_config.exchange = Exchange::BINANCE;
    binance_config.name = "Binance";
    binance_config.host = cfg.binance.host;
    binance_config.port = cfg.binance.port;
    binance_config.path = cfg.binance.path;
    binance_config.symbol = cfg.binance.symbol;
    binance_config.subscribe_msg = cfg.binance.subscribe_msg;

    FeedConfig okx_config;
    okx_config.exchange = Exchange::OKX;
    okx_config.name = "OKX";
    okx_config.host = cfg.okx.host;
    okx_config.port = cfg.okx.port;
    okx_config.path = cfg.okx.path;
    okx_config.symbol = cfg.okx.symbol;
    okx_config.subscribe_msg = cfg.okx.subscribe_msg;

    MarketDataFeed binance_feed(binance_config);
    MarketDataFeed okx_feed(okx_config);
    ArbitrageDetector detector(cfg.arbitrage.fee_rate, cfg.arbitrage.min_profit);

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

    std::cout << "Waiting for market data..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));
    logger.log("Market data feeds connected");

    std::cout << std::fixed << std::setprecision(2);

    int loop_count = 0;

    // 主循环：周期性拉取快照并渲染 UI，检测套利信号
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.display.refresh_ms));

        OrderBook binance_book = binance_feed.getOrderBookSnapshot();
        OrderBook okx_book = okx_feed.getOrderBookSnapshot();

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
            binance_feed.isRunning(),
            okx_feed.isRunning(),
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

    // 先停止 feed（让 receiveLoop 退出）
    binance_feed.stop();
    okx_feed.stop();

    // 等待线程结束（比 detach 更安全，不会资源泄漏）
    if (binance_thread.joinable()) binance_thread.join();
    if (okx_thread.joinable()) okx_thread.join();

    // 最终报告：延迟统计 + 套利统计
    std::cout << "\n═══ Final Report ═══" << std::endl;
    Display::printDualLatencyReport(
        binance_feed.getLatencyTracker(),
        okx_feed.getLatencyTracker()
    );
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