#pragma once

#include "orderbook.h"
#include "arbitrage_detector.h"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

class Display {
public:
    static void clearScreen() {
        std::cout << "\033[2J\033[1;1H";
    }
 
    static void render(
        const OrderBook& book_a,
        const OrderBook& book_b,
        const ArbitrageDetector& detector,
        int binance_msgs, long binance_avg_lat, long binance_max_lat, bool binance_live,
        int okx_msgs, long okx_avg_lat, long okx_max_lat, bool okx_live,
        int levels = 5
    ) {
        clearScreen();
        std::cout << std::fixed << std::setprecision(2);
 
        std::cout << "╔═══════════════════════════════════════════════════════════════════════════════════════╗" << std::endl;
        std::cout << "║                           Crypto HFT Engine v0.2                                      ║" << std::endl;
        std::cout << "╚═══════════════════════════════════════════════════════════════════════════════════════╝" << std::endl;
        std::cout << std::endl;
 
        printDualOrderBook(book_a, book_b, levels);
        std::cout << std::endl;
 
        printArbitragePanel(book_a, book_b, detector);
        std::cout << std::endl;
 
        std::cout << "═══ Performance ═══" << std::endl;
        std::cout << "  " << book_a.getSymbol() << ": "
                  << binance_msgs << " msgs  Avg: " << binance_avg_lat << "μs"
                  << "  Max: " << binance_max_lat << "μs"
                  << "  " << (binance_live ? "[\033[32mLIVE\033[0m]" : "[\033[31mDOWN\033[0m]")
                  << std::endl;
        std::cout << "  " << book_b.getSymbol() << ": "
                  << okx_msgs << " msgs  Avg: " << okx_avg_lat << "μs"
                  << "  Max: " << okx_max_lat << "μs"
                  << "  " << (okx_live ? "[\033[32mLIVE\033[0m]" : "[\033[31mDOWN\033[0m]")
                  << std::endl;
        std::cout << std::endl;
        std::cout << "Press Ctrl+C to exit" << std::endl;
    }
 
private:
    static std::string fmtPrice(double price) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << std::setw(10) << price;
        return ss.str();
    }
 
    static std::string fmtQty(double qty) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(4) << std::setw(8) << qty;
        return ss.str();
    }
 
    static std::string center(const std::string& text, int width) {
        int pad = width - static_cast<int>(text.size());
        if (pad <= 0) return text.substr(0, width);
        int left = pad / 2;
        int right = pad - left;
        return std::string(left, ' ') + text + std::string(right, ' ');
    }
 
    static void printDualOrderBook(const OrderBook& book_a, const OrderBook& book_b, int levels) {
        auto a_bids = collectBids(book_a, levels);
        auto a_asks = collectAsks(book_a, levels);
        auto b_bids = collectBids(book_b, levels);
        auto b_asks = collectAsks(book_b, levels);
 
        const int ob_width = 43;
 
        std::string name_a = book_a.getSymbol();
        std::string name_b = book_b.getSymbol();

        std::cout << "  ┌" << repeat("─", ob_width) << "┐ ┌" << repeat("─", ob_width) << "┐" << std::endl;
        std::cout << "  │" << center(name_a, ob_width) << "│ │" << center(name_b, ob_width) << "│" << std::endl;
        std::cout << "  │" << "   Qty       Price   │    Price       Qty  " << "│ │" << "   Qty       Price   │    Price       Qty  " << "│" << std::endl;
 
        int max_rows = std::max({(int)a_bids.size(), (int)a_asks.size(),
                                  (int)b_bids.size(), (int)b_asks.size()});
 
        for (int i = 0; i < max_rows; ++i) {
            std::cout << "  │";
            std::cout << formatRow(a_bids, a_asks, i);
            std::cout << "│ │";
            std::cout << formatRow(b_bids, b_asks, i);
            std::cout << "│" << std::endl;
        }
 
        std::string spread_a = "Spread: " + fmtPrice(book_a.getSpread()) + "  Mid: " + fmtPrice((book_a.getBestBid().price + book_a.getBestAsk().price) / 2.0);
        std::string spread_b = "Spread: " + fmtPrice(book_b.getSpread()) + "  Mid: " + fmtPrice((book_b.getBestBid().price + book_b.getBestAsk().price) / 2.0);
 
        std::cout << "  ├" << repeat("─", ob_width) << "┤ ├" << repeat("─", ob_width) << "┤" << std::endl;
        std::cout << "  │" << center(spread_a, ob_width) << "│ │" << center(spread_b, ob_width) << "│" << std::endl;
        std::cout << "  └" << repeat("─", ob_width) << "┘ └" << repeat("─", ob_width) << "┘" << std::endl;
    }
 
    static std::string formatRow(const std::vector<PriceLevel>& bids,
                                  const std::vector<PriceLevel>& asks, int i) {
        std::ostringstream ss;
        ss << std::fixed;
 
        if (i < static_cast<int>(bids.size()) && bids[i].price > 0) {
            ss << std::setprecision(4) << std::setw(8) << bids[i].quantity
               << "  "
               << std::setprecision(2) << std::setw(10) << bids[i].price;
        } else {
            ss << std::setw(20) << " ";
        }
 
        ss << " │ ";
 
        if (i < static_cast<int>(asks.size()) && asks[i].price > 0) {
            ss << std::setprecision(2) << std::setw(10) << asks[i].price
               << "  "
               << std::setprecision(4) << std::setw(8) << asks[i].quantity;
        } else {
            ss << std::setw(20) << " ";
        }
 
        return ss.str();
    }
 
    static std::vector<PriceLevel> collectBids(const OrderBook& book, int levels) {
        std::vector<PriceLevel> result;
        auto bids = book.getBids();
        int count = 0;
        for (auto it = bids.rbegin(); it != bids.rend() && count < levels; ++it, ++count) {
            result.push_back({it->first, it->second});
        }
        return result;
    }

    static std::string repeat(const std::string& s, int n) {
        std::string result;
        result.reserve(s.size() * n);
        for (int i = 0; i < n; ++i) {
            result += s;
        }
        return result;
    }
 
    static std::vector<PriceLevel> collectAsks(const OrderBook& book, int levels) {
        std::vector<PriceLevel> result;
        auto asks = book.getAsks();
        int count = 0;
        for (auto it = asks.begin(); it != asks.end() && count < levels; ++it, ++count) {
            result.push_back({it->first, it->second});
        }
        return result;
    }
 
    static void printArbitragePanel(const OrderBook& book_a, const OrderBook& book_b,
                                     const ArbitrageDetector& detector) {
        auto a_ask = book_a.getBestAsk();
        auto a_bid = book_a.getBestBid();
        auto b_ask = book_b.getBestAsk();
        auto b_bid = book_b.getBestBid();

        double diff1 = b_bid.price - a_ask.price;
        double diff2 = a_bid.price - b_ask.price;

        // 颜色代码
        std::string g = "\033[32m";
        std::string r = "\033[31m";
        std::string x = "\033[0m";
        std::string c1 = diff1 > 0 ? g : r;
        std::string c2 = diff2 > 0 ? g : r;

        std::string line = repeat("─", 89);

        std::ostringstream row1, row2, row3;
        row1 << std::fixed << std::setprecision(2)
             << "  Buy Binance  Ask: " << std::setw(10) << a_ask.price
             << "  ->  Sell OKX      Bid: " << std::setw(10) << b_bid.price
             << "  Diff: " << std::setw(10) << diff1 << "  ";
        
        row2 << std::fixed << std::setprecision(2)
             << "  Buy OKX      Ask: " << std::setw(10) << b_ask.price
             << "  ->  Sell Binance  Bid: " << std::setw(10) << a_bid.price
             << "  Diff: " << std::setw(10) << diff2 << "  ";

        row3 << "  Signals: " << std::setw(5) << detector.getTotalSignals()
             << "   |   Net Profit: $" << std::fixed << std::setprecision(2) 
             << std::setw(10) << detector.getTotalNetProfit();
        

        std::string s1 = row1.str();
        std::string s2 = row2.str();
        std::string s3 = row3.str();
        int w = 89;
        if ((int)s1.size() < w) s1 += std::string(w - s1.size(), ' ');
        if ((int)s2.size() < w) s2 += std::string(w - s2.size(), ' ');
        if ((int)s3.size() < w) s3 += std::string(w - s3.size(), ' ');

        std::cout << "  ┌" << line << "┐" << std::endl;
        std::cout << "  │" << center("Arbitrage Monitor", w) << "│" << std::endl;
        std::cout << "  ├" << line << "┤" << std::endl;
        

        std::string d1 = s1, d2 = s2;
        size_t pos1 = d1.find("Diff:");
        size_t pos2 = d2.find("Diff:");
        if (pos1 != std::string::npos) { d1.insert(pos1, c1); d1 += x; }
        if (pos2 != std::string::npos) { d2.insert(pos2, c2); d2 += x; }

        std::cout << "  │" << d1 << "│" << std::endl;
        std::cout << "  │" << d2 << "│" << std::endl;
        std::cout << "  ├" << line << "┤" << std::endl;
        std::cout << "  │" << s3 << "│" << std::endl;
        std::cout << "  └" << line << "┘" << std::endl;
    }
};