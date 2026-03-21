#pragma once

#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include <algorithm>

struct Pricelevel {
    double price;
    double quantity;
};

class OrderBook {

    std::string symbol_;
    std::map<double, double> bids_; // buy price -> quantity
    std::map<double, double> asks_; // sell price -> quantity

public:
    OrderBook(const std::string& symbol) : symbol_(symbol) {}

    void updateBid(double price, double quantity) {
        if (quantity == 0.0) {
            bids_.erase(price);
        } else {
            bids_[price] = quantity;
        }
    }

    void updateAsk(double price, double quantity) {
        if (quantity == 0.0) {
            asks_.erase(price);
        } else {
            asks_[price] = quantity;
        }
    }

    Pricelevel getBestBid() const {
        if (bids_.empty()) {
            return {0.0, 0.0};
        }
        auto it = bids_.rbegin();
        return {it->first, it->second};
    }

    Pricelevel getBestAsk() const {
        if (asks_.empty()) {
            return {0.0, 0.0};
        }
        auto it = asks_.begin();
        return {it->first, it->second};
    }

    double getSpread() const {
        Pricelevel bid = getBestBid();
        Pricelevel ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) {
            return 0.0;
        }
        return ask.price - bid.price;
    }

        void print(int levels = 10) const {
        // 收集买盘数据（从高到低）
        std::vector<Pricelevel> bidLevels;
        int count = 0;
        for (auto it = bids_.rbegin(); it != bids_.rend() && count < levels; ++it, ++count) {
            bidLevels.push_back({it->first, it->second});
        }
 
        // 收集卖盘数据（从低到高）
        std::vector<Pricelevel> askLevels;
        count = 0;
        for (auto it = asks_.begin(); it != asks_.end() && count < levels; ++it, ++count) {
            askLevels.push_back({it->first, it->second});
        }
 
        // 获取统计数据
        auto bestBid = getBestBid();
        auto bestAsk = getBestAsk();
        double spread = getSpread();
        double midPrice = (bestBid.price + bestAsk.price) / 2.0;
 
        // 表头
        std::cout << "  ┌──────────── " << symbol_ << " ────────────┐" << std::endl;
        std::cout << "  │     BID (买盘)     │     ASK (卖盘)     │" << std::endl;
        std::cout << "  │  Qty       Price   │   Price      Qty   │" << std::endl;
        std::cout << "  ├────────────────────┼────────────────────┤" << std::endl;
 
        // 逐行打印买卖盘
        int maxRows = std::max(bidLevels.size(), askLevels.size());
        for (int i = 0; i < maxRows; ++i) {
            std::cout << "  │";
 
            // 左边：买盘
            if (i < static_cast<int>(bidLevels.size())) {
                std::cout << std::setw(7) << std::fixed << std::setprecision(4) 
                          << bidLevels[i].quantity
                          << "  " 
                          << std::setw(10) << std::setprecision(2) 
                          << bidLevels[i].price;
            } else {
                std::cout << std::setw(20) << " ";
            }
 
            std::cout << " │ ";
 
            // 右边：卖盘
            if (i < static_cast<int>(askLevels.size())) {
                std::cout << std::setw(9) << std::fixed << std::setprecision(2) 
                          << askLevels[i].price
                          << "  " 
                          << std::setw(7) << std::setprecision(4) 
                          << askLevels[i].quantity;
            } else {
                std::cout << std::setw(20) << " ";
            }
 
            std::cout << " │" << std::endl;
        }
 
        // 底部统计
        std::cout << "  ├────────────────────┴────────────────────┤" << std::endl;
        std::cout << "  │ Spread: " << std::setw(8) << std::setprecision(2) << spread
                  << "    Mid: " << std::setw(12) << std::setprecision(2) << midPrice
                  << "   │" << std::endl;
        std::cout << "  └─────────────────────────────────────────┘" << std::endl;
    }

    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    const std::string& getSymbol() const { return symbol_; }
};