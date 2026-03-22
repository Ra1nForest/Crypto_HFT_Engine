#include "market_data_feed.h"
#include "arbitrage_detector.h"
#include "display.h"
#include "logger.h"

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

    // 初始化日志
    Logger logger("logs");
    logger.log("Initializing exchanges...");

    std::cout << "╔══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║       Crypto HFT Engine v0.3                 ║" << std::endl;
    std::cout << "║  Dual Exchange Arbitrage Monitor             ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;

    // 配置交易所
    FeedConfig binance_config;
    binance_config.exchange = Exchange::BINANCE;
    binance_config.name = "Binance";
    binance_config.host = "stream.binance.com";
    binance_config.port = "9443";
    binance_config.path = "/ws/btcusdt@depth20@100ms";
    binance_config.symbol = "BTCUSDT";
    binance_config.subscribe_msg = "";

    FeedConfig okx_config;
    okx_config.exchange = Exchange::OKX;
    okx_config.name = "OKX";
    okx_config.host = "ws.okx.com";
    okx_config.port = "8443";
    okx_config.path = "/ws/v5/public";
    okx_config.symbol = "BTCUSDT";
    okx_config.subscribe_msg = R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})";

    MarketDataFeed binance_feed(binance_config);
    MarketDataFeed okx_feed(okx_config);
    ArbitrageDetector detector(0.000001, 0.01);

    logger.log("Binance config: " + binance_config.host + binance_config.path);
    logger.log("OKX config: " + okx_config.host + okx_config.path);

    // 启动行情线程
    std::cout << "Starting market data feeds..." << std::endl;
    logger.log("Starting market data feeds...");

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

    // 价差日志计数器（每50次循环记录一次价差，不用每次都写）
    int loop_count = 0;

    // 主循环
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        OrderBook binance_book = binance_feed.getOrderBookSnapshot();
        OrderBook okx_book = okx_feed.getOrderBookSnapshot();

        // 套利检测
        bool signal_found = detector.detect(binance_book, okx_book);

        // 如果发现套利信号，写入日志
        if (signal_found) {
            auto signals = detector.getSignals();
            if (!signals.empty()) {
                logger.logSignal(signals.back());
            }
        }

        // 每10秒记录一次价差快照（50次 × 200ms = 10秒）
        loop_count++;
        if (loop_count % 50 == 0) {
            auto a_ask = binance_book.getBestAsk();
            auto b_bid = okx_book.getBestBid();
            auto b_ask = okx_book.getBestAsk();
            auto a_bid = binance_book.getBestBid();

            logger.logSpread("Binance", a_ask.price, "OKX", b_bid.price,
                           b_bid.price - a_ask.price);
            logger.logSpread("OKX", b_ask.price, "Binance", a_bid.price,
                           a_bid.price - b_ask.price);
        }

        // 显示
        Display::render(
            binance_book, okx_book, detector,
            binance_feed.getLatencyTracker(),
            okx_feed.getLatencyTracker(),
            binance_feed.isRunning(),
            okx_feed.isRunning(),
            5
        );
    }

    // 优雅退出
    std::cout << "\nClosing connections..." << std::endl;
    logger.log("Shutting down...");

    binance_feed.stop();
    okx_feed.stop();
    binance_thread.detach();
    okx_thread.detach();

    // 最终报告
    std::cout << "\n═══ Final Report ═══" << std::endl;
    Display::printDualLatencyReport(
        binance_feed.getLatencyTracker(),
        okx_feed.getLatencyTracker()
    );
    std::cout << "  Arbitrage signals: " << detector.getTotalSignals() << std::endl;
    std::cout << "  Total net profit:  $" << detector.getTotalNetProfit() << std::endl;

    // 写入最终报告到日志
    logger.logFinalReport(
        binance_feed.getLatencyTracker(),
        okx_feed.getLatencyTracker(),
        detector
    );

    std::cout << "\nGoodbye!" << std::endl;
    return 0;
}