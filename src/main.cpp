#include "market_data_feed.h"
#include "arbitrage_detector.h"
#include "display.h"
 
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
    std::cout << "║       Crypto HFT Engine v0.2                 ║" << std::endl;
    std::cout << "║  Dual Exchange Arbitrage Monitor             ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;

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

    ArbitrageDetector detector(0.001, 0.01);

    std::cout << "Formatting market access..." << std::endl;

    std::thread binance_thread([&binance_feed]() {
        binance_feed.run();
    });

    std::thread okx_thread([&okx_feed]() {
        okx_feed.run();
    });

    std::cout << "Waiting for market data..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::cout << std::fixed << std::setprecision(2);

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        OrderBook binance_book = binance_feed.getOrderBookSnapshot();
        OrderBook okx_book = okx_feed.getOrderBookSnapshot();

        Display::render(
            binance_book, okx_book, detector,
            binance_feed.getMessageCount(), binance_feed.getAvgLatency(),
            binance_feed.getMaxLatency(), binance_feed.isRunning(),
            okx_feed.getMessageCount(), okx_feed.getAvgLatency(),
            okx_feed.getMaxLatency(), okx_feed.isRunning(),
            5
);
    }

    std::cout << "\n Closing connections..." << std::endl;
 
    binance_feed.stop();
    okx_feed.stop();

    binance_thread.detach();
    okx_thread.detach();

    std::cout << "\n═══ Final Report ═══" << std::endl;
    std::cout << "  Binance messages: " << binance_feed.getMessageCount() << std::endl;
    std::cout << "  OKX messages:     " << okx_feed.getMessageCount() << std::endl;
    std::cout << "  Arbitrage signals: " << detector.getTotalSignals() << std::endl;
    std::cout << "  Total net profit:  $" << detector.getTotalNetProfit() << std::endl;
    std::cout << "\nGoodbye!" << std::endl;
 
    return 0;


}