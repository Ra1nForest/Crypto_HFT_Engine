//
// market_data_feed.h
// 行情订阅模块：负责连接交易所 WebSocket，解析深度数据并更新 OrderBook。
// 该实现使用 Boost.Beast + OpenSSL，simdjson 进行高性能解析。
//

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
#include <charconv>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

// 支持的交易所枚举
enum class Exchange {
    BINANCE,
    OKX
};

// 行情订阅配置
struct FeedConfig {
    Exchange exchange;
    std::string name;
    std::string host;
    std::string port;
    std::string path;
    std::string symbol;
    std::string subscribe_msg;
};

// 行情订阅器：
// - 维护连接生命周期
// - 解析深度并更新 OrderBook
// - 记录解析延迟
class MarketDataFeed {
    FeedConfig config_;
    OrderBook orderbook_;
    mutable std::mutex mutex_;
    std::atomic<bool> running_;

    LatencyTracker latency_;

    // 预留的连接初始化入口（当前实现为空）
    void connect() {

    }

    // 主接收循环：建立 WebSocket，持续读取并解析消息
    void receiveLoop() {
        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        tcp::resolver resolver(ioc);
        websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);

    // 解析 DNS 并连接
    auto const results = resolver.resolve(config_.host, config_.port);

        auto ep = net::connect(ws.next_layer().next_layer(), results);

    // 设置 SNI，避免多证书环境下握手失败
    if (!SSL_set_tlsext_host_name(
                ws.next_layer().native_handle(), config_.host.c_str())) {
            
            throw beast::system_error(
                beast::error_code(
                    static_cast<int>(::ERR_get_error()),
                    net::error::get_ssl_category()),
                    "Failed to set SNI Hostname"
                );
        }

    // TLS + WebSocket 握手
    ws.next_layer().handshake(ssl::stream_base::client);

        std::string ws_host = config_.host + ":" + std::to_string(ep.port());
        ws.handshake(ws_host, config_.path);

    std::cout << "[" << config_.name << "]" << "Connected." << std::endl;

        // 发送订阅消息（部分交易所需要）
        if (!config_.subscribe_msg.empty()) {
            ws.write(net::buffer(config_.subscribe_msg));
            std::cout << "[" << config_.name << "] Subscription message sent." << std::endl;
        }

        simdjson::ondemand::parser parser;
        beast::flat_buffer buffer;

        std::string msg;
        msg.reserve(4096);

        // 持续读取消息，解析深度并记录延迟
        while (running_) {
            buffer.clear();
            ws.read(buffer);

            auto start = std::chrono::high_resolution_clock::now();

            // 复制到字符串，便于 simdjson 解析
            msg.assign(boost::beast::buffers_to_string(buffer.data()));
            
            simdjson::padded_string padded_msg(msg);

            simdjson::ondemand::document doc;
            auto error = parser.iterate(padded_msg).get(doc);
            if (error) {
                continue;
            }

            // 根据交易所类型选择解析函数
            bool success = false;
            if (config_.exchange == Exchange::BINANCE) {
                success = parseBinanceDepth(doc);
            } else if (config_.exchange == Exchange::OKX) {
                success = parseOKXDepth(doc);
            }

            // 记录单条消息解析延迟
            if (success) {
                auto end = std::chrono::high_resolution_clock::now();
                long latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                latency_.record(latency);
            }
        }

        ws.close(websocket::close_code::normal);
    }

    // Binance 深度解析：数组 bids/asks 为 [[price, qty], ...]
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
                double price = fastParseDouble(price_str);
                double qty = fastParseDouble(qty_str);
                if (qty > 0.0) orderbook_.updateBid(price, qty);
            }

            auto asks = doc["asks"].get_array();
            for (auto ask : asks) {
                auto arr = ask.get_array();
                auto iter = arr.begin();
                std::string_view price_str = (*iter).get_string();
                ++iter;
                std::string_view qty_str = (*iter).get_string();
                double price = fastParseDouble(price_str);
                double qty = fastParseDouble(qty_str);
                if (qty > 0.0) orderbook_.updateAsk(price, qty);
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    // OKX 深度解析：data 数组中包含 bids/asks
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
                    double price = fastParseDouble(price_str);
                    double qty = fastParseDouble(qty_str);
                    if (qty > 0.0) orderbook_.updateBid(price, qty);
                }

                for (auto ask : item["asks"].get_array()) {
                    auto arr = ask.get_array();
                    auto iter = arr.begin();
                    std::string_view price_str = (*iter).get_string();
                    ++iter;
                    std::string_view qty_str = (*iter).get_string();
                    double price = fastParseDouble(price_str);
                    double qty = fastParseDouble(qty_str);
                    if (qty > 0.0) orderbook_.updateAsk(price, qty);
                }
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    // 高性能字符串转 double（基于 from_chars）
    static double fastParseDouble(std::string_view sv) {
        double val = 0.0;
        std::from_chars(sv.data(), sv.data() + sv.size(), val);
        return val;
    }

public:
    // 构造函数：初始化配置、订单簿与延迟统计
    MarketDataFeed(const FeedConfig& config)
        : config_(config)
        , orderbook_(config.name + ":" + config.symbol)
        , running_(false)
        , latency_(config.name) {}
    
    // 启动订阅：建立连接并进入接收循环
    void run() {
        running_ = true;
        try {
            connect();
            receiveLoop();
        } catch (const std::exception& e) {
            if (running_) {
                std::cerr << "[" << config_.name << "] Error: " << e.what() << std::endl;
            }
            running_ = false;
        }
    }

    // 通知退出
    void stop() {
        running_ = false;
    }

    // 获取订单簿快照（线程安全拷贝）
    OrderBook getOrderBookSnapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return orderbook_;
    }

    // 访问延迟统计
    const LatencyTracker& getLatencyTracker() const { return latency_; }

    // 已处理消息数量
    int getMessageCount() const { return latency_.getCount(); }

    // 订阅线程是否仍在运行
    bool isRunning() const { return running_; }

    // 交易所显示名称
    const std::string& getName() const { return config_.name; }
};