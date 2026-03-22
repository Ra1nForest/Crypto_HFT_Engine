#pragma once

#include <string>
#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>

struct PriceLevel {
    double price;
    double quantity;
};

class OrderBook {
public:
    OrderBook() : symbol_("") {
        bids_.reserve(20);
        asks_.reserve(20);
    }
 
    OrderBook(const std::string& symbol) : symbol_(symbol) {
        bids_.reserve(20);
        asks_.reserve(20);
    }

    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    void updateBid(double price, double quantity) {
        bids_.push_back({price, quantity});
    }

    void updateAsk(double price, double quantity) {
        asks_.push_back({price, quantity});
    }

    PriceLevel getBestBid() const {
        if (bids_.empty()) return {0.0, 0.0};
        return bids_[0];
    }

    PriceLevel getBestAsk() const {
        if (asks_.empty()) return {0.0, 0.0};
        return asks_[0];
    }

    double getSpread() const {
        auto bid = getBestBid();
        auto ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) return 0.0;
        return ask.price - bid.price;
    }

    const std::string& getSymbol() const { return symbol_; }

    const std::vector<PriceLevel>& getBids() const { return bids_; }
    const std::vector<PriceLevel>& getAsks() const { return asks_; }
    int getBidCount() const { return static_cast<int>(bids_.size()); }
    int getAskCount() const { return static_cast<int>(asks_.size()); }

    void print(int levels = 5) const {
        std::cout << std::fixed;
        std::cout << "  " << symbol_ << std::endl;
        int show = std::min(levels, static_cast<int>(std::max(bids_.size(), asks_.size())));
        for (int i = 0; i < show; ++i) {
            if (i < static_cast<int>(bids_.size())) {
                std::cout << "  " << std::setw(8) << std::setprecision(4) << bids_[i].quantity
                          << "  " << std::setw(10) << std::setprecision(2) << bids_[i].price;
            } else {
                std::cout << std::setw(20) << " ";
            }
            std::cout << " | ";
            if (i < static_cast<int>(asks_.size())) {
                std::cout << std::setw(10) << std::setprecision(2) << asks_[i].price
                          << "  " << std::setw(8) << std::setprecision(4) << asks_[i].quantity;
            } else {
                std::cout << std::setw(20) << " ";
            }
            std::cout << std::endl;
        }
    }

private:
    std::string symbol_;
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
};