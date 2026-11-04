//
// orderbook.h
// 订单簿数据结构：用于保存某交易对的买卖盘快照（深度）。
// 该实现采用 vector 存储，适合固定深度的低延迟场景。
//

#pragma once

#include <string>
#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>

// 价格档位：价格 + 数量
struct PriceLevel {
    double price;
    double quantity;
};

// 简化订单簿：只保存最近一次快照的前 N 档（由上层控制）
class OrderBook {
public:
    // 默认构造：symbol 为空
    OrderBook() : symbol_("") {
        bids_.reserve(20);
        asks_.reserve(20);
    }
 
    // 指定交易对构造
    OrderBook(const std::string& symbol) : symbol_(symbol) {
        bids_.reserve(20);
        asks_.reserve(20);
    }

    // 清空买盘/卖盘（用于更新快照）
    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    // 追加买盘/卖盘档位
    void updateBid(double price, double quantity) {
        bids_.push_back({price, quantity});
    }

    void updateAsk(double price, double quantity) {
        asks_.push_back({price, quantity});
    }

    // 最优买/卖：约定 vector[0] 为最优档（由解析顺序保证）
    PriceLevel getBestBid() const {
        if (bids_.empty()) return {0.0, 0.0};
        return bids_[0];
    }

    PriceLevel getBestAsk() const {
        if (asks_.empty()) return {0.0, 0.0};
        return asks_[0];
    }

    // 点差（卖一 - 买一）
    double getSpread() const {
        auto bid = getBestBid();
        auto ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) return 0.0;
        return ask.price - bid.price;
    }

    // 基本访问器
    const std::string& getSymbol() const { return symbol_; }

    const std::vector<PriceLevel>& getBids() const { return bids_; }
    const std::vector<PriceLevel>& getAsks() const { return asks_; }
    int getBidCount() const { return static_cast<int>(bids_.size()); }
    int getAskCount() const { return static_cast<int>(asks_.size()); }

    // 简单打印：用于调试/控制台展示
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
    // 交易对/来源标识
    std::string symbol_;
    // 买盘（从高到低）与卖盘（从低到高）的快照
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
};