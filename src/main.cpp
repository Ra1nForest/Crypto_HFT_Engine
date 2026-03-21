#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <openssl/err.h>
 
#include "simdjson.h"
#include "orderbook.h"
 
#include <iostream>
#include <string>
#include <chrono>
#include <iomanip>
//#include <thread>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

double toDouble(std::string_view sv){
    return std::stod(std::string(sv));
}

void clearScreen() {
    std::cout << "\033[2J\033[1;1H";
}

int main() {
    try {
        const std::string host = "stream.binance.com";
        const std::string port = "9443";
        const std::string path = "/ws/btcusdt@depth20@100ms";

        std::cout << "Connecting to Binance WebSocket..." << std::endl;
        std::cout << "Data Stream: wss://" << host << ":" << port << path << std::endl;

        std::cout << std::endl;

        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        tcp::resolver resolver(ioc);
        websocket::stream<beast::ssl_stream<tcp::socket>> ws{ioc, ctx};

        auto const results = resolver.resolve(host, port);

        auto ep = net::connect(ws.next_layer().next_layer(), results);

        if (!SSL_set_tlsext_host_name(
                ws.next_layer().native_handle(), host.c_str())) {
            throw beast::system_error(
                beast::error_code(
                    static_cast<int>(::ERR_get_error()),
                    net::error::get_ssl_category()),
                "Failed to set SNI hostname");
        }

        ws.next_layer().handshake(ssl::stream_base::client);

        std::string ws_host = host + ":" + std::to_string(ep.port());
        ws.handshake(ws_host, path);

        std::cout << "Connected successfully!" << std::endl;
        std::cout << std::fixed << std::setprecision(2);

        OrderBook binance("Binance:BTCUSDT");

        simdjson::ondemand::parser parser;
        beast::flat_buffer buffer;

        int message_count = 0;

        long total_latency_us = 0;
        long max_latency_us = 0;
        long min_latency_us = 999999;

        while (true) {
            buffer.clear();
            ws.read(buffer);

            auto start = std::chrono::high_resolution_clock::now();

            std::string msg = beast::buffers_to_string(buffer.data());
            simdjson::padded_string padded(msg);

            simdjson::ondemand::document doc;
            auto error = parser.iterate(padded).get(doc);
            if (error) {
                std::cerr << "JSON Analysis Error: " << error << std::endl;
                continue;
            }
            
            binance.clearBids();
            binance.clearAsks();

            auto bids = doc["bids"].get_array();
            for (auto bid : bids) {
                auto arr = bid.get_array();
                auto iter = arr.begin();
                
                // 第一个元素是价格字符串
                std::string_view price_str = (*iter).get_string();
                ++iter;
                // 第二个元素是数量字符串
                std::string_view qty_str = (*iter).get_string();
 
                double price = toDouble(price_str);
                double qty = toDouble(qty_str);
 
                if (qty > 0.0) {
                    binance.updateBid(price, qty);
                }
            }

            auto asks = doc["asks"].get_array();
            for (auto ask : asks) {
                auto arr = ask.get_array();
                auto iter = arr.begin();
 
                std::string_view price_str = (*iter).get_string();
                ++iter;
                std::string_view qty_str = (*iter).get_string();
 
                double price = toDouble(price_str);
                double qty = toDouble(qty_str);
 
                if (qty > 0.0) {
                    binance.updateAsk(price, qty);
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            long latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                end - start).count();
            
            total_latency_us += latency_us;
            message_count++;
            if (latency_us > max_latency_us) max_latency_us = latency_us;
            if (latency_us < min_latency_us) min_latency_us = latency_us;

            clearScreen();
 
            std::cout << "╔══════════════════════════════════════════════╗" << std::endl;
            std::cout << "║     Crypto Arbitrage Monitor v0.1            ║" << std::endl;
            std::cout << "╚══════════════════════════════════════════════╝" << std::endl;
            std::cout << std::endl;

            binance.print(10);
 
            std::cout << "═══ Performance Statistics ═══" << std::endl;
            std::cout << "  Message Count: " << message_count << std::endl;
            std::cout << "  Current Latency: " << latency_us << " μs" << std::endl;
            std::cout << "  Average Latency: " << total_latency_us / message_count 
                      << " μs" << std::endl;
            std::cout << "  Minimum Latency: " << min_latency_us << " μs" << std::endl;
            std::cout << "  Maximum Latency: " << max_latency_us << " μs" << std::endl;
            std::cout << std::endl;
            std::cout << "Press Ctrl+C to Exit" << std::endl;
        }

        ws.close(websocket::close_code::normal);
        
    }  catch (const std::exception& e){
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
        return 0;
}