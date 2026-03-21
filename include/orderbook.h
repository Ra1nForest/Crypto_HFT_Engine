#pragma once

#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include <algorithm>

struct PriceLevel {
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

    PriceLevel getBestBid() const {
        if (bids_.empty()) {
            return {0.0, 0.0};
        }
        auto it = bids_.rbegin();
        return {it->first, it->second};
    }

    PriceLevel getBestAsk() const {
        if (asks_.empty()) {
            return {0.0, 0.0};
        }
        auto it = asks_.begin();
        return {it->first, it->second};
    }

    double getSpread() const {
        PriceLevel bid = getBestBid();
        PriceLevel ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) {
            return 0.0;
        }
        return ask.price - bid.price;
    }

        void print(int levels = 10) const {
        std::vector<PriceLevel> bidLevels;
        int count = 0;
        for (auto it = bids_.rbegin(); it != bids_.rend() && count < levels; ++it, ++count) {
            bidLevels.push_back({it->first, it->second});
        }
 
        std::vector<PriceLevel> askLevels;
        count = 0;
        for (auto it = asks_.begin(); it != asks_.end() && count < levels; ++it, ++count) {
            askLevels.push_back({it->first, it->second});
        }
 
        auto bestBid = getBestBid();
        auto bestAsk = getBestAsk();
        double spread = getSpread();
        double midPrice = (bestBid.price + bestAsk.price) / 2.0;
 
        std::cout << "  ┌──────────── " << symbol_ << " ────────────┐" << std::endl;
        std::cout << "  │        BID         │         ASK        │" << std::endl;
        std::cout << "  │  Qty       Price   │   Price      Qty   │" << std::endl;
        std::cout << "  ├────────────────────┼────────────────────┤" << std::endl;
 
        int maxRows = std::max(bidLevels.size(), askLevels.size());
        for (int i = 0; i < maxRows; ++i) {
            std::cout << "  │";
 
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
 
        std::cout << "  ├────────────────────┴────────────────────┤" << std::endl;
        std::cout << "  │ Spread: " << std::setw(8) << std::setprecision(2) << spread
                  << "    Mid: " << std::setw(12) << std::setprecision(2) << midPrice
                  << "   │" << std::endl;
        std::cout << "  └─────────────────────────────────────────┘" << std::endl;
    }

    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    const std::string& getSymbol() const { return symbol_; }

    const std::map<double, double>& getBids() const { return bids_; }
    const std::map<double, double>& getAsks() const { return asks_; }
};