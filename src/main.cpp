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

std::atomic<bool> g_running{true};

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

    // 读取配置
    AppConfig cfg = ConfigReader::load("config.json");

    // 初始化日志
    Logger logger("logs");
    logger.log("=== Crypto HFT Engine v1.0 ===");
    logger.log("Config: fee_rate=" + std::to_string(cfg.arbitrage.fee_rate)
             + " min_profit=" + std::to_string(cfg.arbitrage.min_profit)
             + " refresh=" + std::to_string(cfg.display.refresh_ms) + "ms");

    // 配置交易所
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

    logger.log("Starting market data feeds...");
    std::cout << "Starting market data feeds..." << std::endl;

    // 启动行情线程
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

    // 主循环
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.display.refresh_ms));

        OrderBook binance_book = binance_feed.getOrderBookSnapshot();
        OrderBook okx_book = okx_feed.getOrderBookSnapshot();

        // 套利检测
        bool signal_found = detector.detect(binance_book, okx_book);

        if (signal_found) {
            auto& signals = detector.getSignals();
            if (!signals.empty()) {
                logger.logSignal(signals.back());
            }
        }

        // 周期性记录价差
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

        // 显示
        Display::render(
            binance_book, okx_book, detector,
            binance_feed.getLatencyTracker(),
            okx_feed.getLatencyTracker(),
            binance_feed.isRunning(),
            okx_feed.isRunning(),
            cfg.display.levels
        );
    }

    // 优雅退出
    std::cout << "\nClosing connections..." << std::endl;
    logger.log("Shutting down...");

    // 先停止feed（让receiveLoop退出）
    binance_feed.stop();
    okx_feed.stop();

    // 等待线程结束（比detach更安全，不会资源泄漏）
    if (binance_thread.joinable()) binance_thread.join();
    if (okx_thread.joinable()) okx_thread.join();

    // 最终报告
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