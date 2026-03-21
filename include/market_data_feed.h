#pragma once

#include <boost/asio/ssl/error.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <openssl/err.h>

#include "simdjson.h"
#include "orderbook.h"
#include "latency_tracker.h"

#include <string>
#include <iostream>
#include <chrono>
#include <mutex>
#include <atomic>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

enum class Exchange {
    BINANCE,
    OKX
};

struct FeedConfig {
    Exchange exchange;
    std::string name;
    std::string host;
    std::string port;
    std::string path;
    std::string symbol;
    std::string subscribe_msg;
};

class MarketDataFeed {
    FeedConfig config_;
    OrderBook orderbook_;
    mutable std::mutex mutex_;
    std::atomic<bool> running_;

    LatencyTracker latency_;

    void connect() {

    }

    void receiveLoop() {
        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        tcp::resolver resolver(ioc);
        websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);

        auto const results = resolver.resolve(config_.host, config_.port);

        auto ep = net::connect(ws.next_layer().next_layer(), results);

        if (!SSL_set_tlsext_host_name(
                ws.next_layer().native_handle(), config_.host.c_str())) {
            
            throw beast::system_error(
                beast::error_code(
                    static_cast<int>(::ERR_get_error()),
                    net::error::get_ssl_category()),
                    "Failed to set SNI Hostname"
                );
        }

        ws.next_layer().handshake(ssl::stream_base::client);

        std::string ws_host = config_.host + ":" + std::to_string(ep.port());
        ws.handshake(ws_host, config_.path);

        std::cout << "[" << config_.name << "]" << "Connected." << std::endl;

        if (!config_.subscribe_msg.empty()) {
            ws.write(net::buffer(config_.subscribe_msg));
            std::cout << "[" << config_.name << "] Subscription message sent." << std::endl;
        }

        simdjson::ondemand::parser parser;
        beast::flat_buffer buffer;

        while (running_) {
            buffer.clear();
            ws.read(buffer);

            auto start = std::chrono::high_resolution_clock::now();

            std::string msg = beast::buffers_to_string(buffer.data());
            simdjson::padded_string padded_msg(msg);

            simdjson::ondemand::document doc;
            auto error = parser.iterate(padded_msg).get(doc);
            if (error) {
                continue;
            }

            bool success = false;
            if (config_.exchange == Exchange::BINANCE) {
                success = parseBinanceDepth(doc);
            } else if (config_.exchange == Exchange::OKX) {
                success = parseOKXDepth(doc);
            }

            if (success) {
                auto end = std::chrono::high_resolution_clock::now();
                long latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                latency_.record(latency);
            }
        }

        ws.close(websocket::close_code::normal);
    }

    bool parseBinanceDepth(simdjson::ondemand::document& doc){
        try{
            std::lock_guard<std::mutex> lock(mutex_);
            orderbook_.clearBids();
            orderbook_.clearAsks();

            auto bids = doc["bids"].get_array();
            for (auto bid : bids) {
                auto arr = bid.get_array();
                auto iter = arr.begin();
                std::string_view price_str = (*iter).get_string();
                ++iter;
                std::string_view qty_str = (*iter).get_string();
                double price = std::stod(std::string(price_str));
                double qty = std::stod(std::string(qty_str));
                if (qty > 0.0) orderbook_.updateBid(price, qty);
            }

            auto asks = doc["asks"].get_array();
            for (auto ask : asks) {
                auto arr = ask.get_array();
                auto iter = arr.begin();
                std::string_view price_str = (*iter).get_string();
                ++iter;
                std::string_view qty_str = (*iter).get_string();
                double price = std::stod(std::string(price_str));
                double qty = std::stod(std::string(qty_str));
                if (qty > 0.0) orderbook_.updateAsk(price, qty);
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    bool parseOKXDepth(simdjson::ondemand::document& doc) {
        try {
            simdjson::ondemand::array data;
            auto err = doc["data"].get_array().get(data);
            if (err) return false;
            
            std::lock_guard<std::mutex> lock(mutex_);
            orderbook_.clearBids();
            orderbook_.clearAsks();

            for (auto item : data) {
                auto bids = item["bids"].get_array();
                for (auto bid : bids) {
                    auto arr = bid.get_array();
                    auto iter = arr.begin();
                    std::string_view price_str = (*iter).get_string();
                    ++iter;
                    std::string_view qty_str = (*iter).get_string();
                    double price = std::stod(std::string(price_str));
                    double qty = std::stod(std::string(qty_str));
                    if (qty > 0.0) orderbook_.updateBid(price, qty);
                }

                for (auto ask : item["asks"].get_array()) {
                    auto arr = ask.get_array();
                    auto iter = arr.begin();
                    std::string_view price_str = (*iter).get_string();
                    ++iter;
                    std::string_view qty_str = (*iter).get_string();
                    double price = std::stod(std::string(price_str));
                    double qty = std::stod(std::string(qty_str));
                    if (qty > 0.0) orderbook_.updateAsk(price, qty);
                }
            }
            return true;
        } catch (...) {
            return false;
        }
    }

public:
    MarketDataFeed(const FeedConfig& config)
        : config_(config)
        , orderbook_(config.name + ":" + config.symbol)
        , running_(false)
        , latency_(config.name) {}
    
    void run() {
        running_ = true;
        try {
            connect();
            receiveLoop();
        } catch (const std::exception& e) {
            std::cerr << "[" << config_.name << "] Error: " << e.what() << std::endl;
            running_ = false;
        }
    }

    void stop() {
        running_ = false;
    }

    OrderBook getOrderBookSnapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return orderbook_;
    }

    const LatencyTracker& getLatencyTracker() const { return latency_; }

    int getMessageCount() const { return latency_.getCount(); }

    bool isRunning() const { return running_; }

    const std::string& getName() const { return config_.name; }
};