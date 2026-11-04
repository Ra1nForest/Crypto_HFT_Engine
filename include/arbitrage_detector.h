//
// arbitrage_detector.h
// 双交易所套利检测：对比两个订单簿的买一/卖一，判断是否存在可执行套利。
//

#pragma once

#include "orderbook.h"

#include <cmath>
#include <string>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>

// 套利信号：记录一次可执行套利的关键字段
struct ArbitrageSignal {
    std::string buy_exchange;
    std::string sell_exchange;
    double buy_price;
    double sell_price;
    double quantity;
    double gross_profit;
    double net_profit;
    long long timestamp_ms;
};

// 套利检测器：
// - 支持双向检测（A 买 B 卖 & B 买 A 卖）
// - 内置手续费模型与最小净利润阈值
class ArbitrageDetector {
public:
    // fee_rate: 手续费比例（单边）
    // min_profit: 最小净利润阈值
    ArbitrageDetector(double fee_rate = 0.001, double min_profit = 0.01)
        : fee_rate_(fee_rate)
        , min_profit_(min_profit)
        , total_signals_(0)
        , total_net_profit_(0.0)
        , last_d1_buy_(0.0)
        , last_d1_sell_(0.0)
        , last_d2_buy_(0.0)
        , last_d2_sell_(0.0) {}

    // 检测两个订单簿是否存在套利机会，返回是否发现信号
    bool detect(const OrderBook& book_a, const OrderBook& book_b) {
        bool found = false;

        auto a_ask = book_a.getBestAsk();
        auto a_bid = book_a.getBestBid();
        auto b_ask = book_b.getBestAsk();
        auto b_bid = book_b.getBestBid();

        // 方向 1：A 买、B 卖
        found |= checkDirection(
            book_a.getSymbol(), a_ask.price, a_ask.quantity,
            book_b.getSymbol(), b_bid.price, b_bid.quantity,
            last_d1_buy_, last_d1_sell_
        );

        // 方向 2：B 买、A 卖
        found |= checkDirection(
            book_b.getSymbol(), b_ask.price, b_ask.quantity,
            book_a.getSymbol(), a_bid.price, a_bid.quantity,
            last_d2_buy_, last_d2_sell_
        );

        return found;
    }

    // 统计与信号访问器
    int getTotalSignals() const { return total_signals_; }
    double getTotalNetProfit() const { return total_net_profit_; }
    const std::vector<ArbitrageSignal>& getSignals() const { return signals_; }

private:
    // 手续费比例与最小净利润阈值
    double fee_rate_;
    double min_profit_;
    // 统计累积信息
    int total_signals_;
    double total_net_profit_;
    std::vector<ArbitrageSignal> signals_;

    // 每个方向独立的去重价格（避免重复打印/记录）
    double last_d1_buy_;
    double last_d1_sell_;
    double last_d2_buy_;
    double last_d2_sell_;

    // 单方向检测：给定买卖价格与数量，计算净利润并决定是否输出信号
    bool checkDirection(
        const std::string& buy_sym, double buy_price, double buy_qty,
        const std::string& sell_sym, double sell_price, double sell_qty,
        double& last_buy, double& last_sell
    ) {
        if (buy_price <= 0 || sell_price <= 0) return false;

    // 计算毛利润与净利润（扣除双边手续费）
    double gross = sell_price - buy_price;
        if (gross <= 0) return false;

        double qty = std::min(buy_qty, sell_qty);
    double fee = (buy_price + sell_price) * qty * fee_rate_;
        double net = gross * qty - fee;

        if (net <= min_profit_) return false;

        // 去重：买卖价与上次极为接近则跳过
        if (std::abs(buy_price - last_buy) < 0.01 &&
            std::abs(sell_price - last_sell) < 0.01) {
            return false;
        }

        last_buy = buy_price;
        last_sell = sell_price;

    // 构建信号对象
    ArbitrageSignal signal;
        signal.buy_exchange = buy_sym;
        signal.sell_exchange = sell_sym;
        signal.buy_price = buy_price;
        signal.sell_price = sell_price;
        signal.quantity = qty;
        signal.gross_profit = gross * qty;
        signal.net_profit = net;
        signal.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        signals_.push_back(signal);
        total_signals_++;
        total_net_profit_ += net;
        // 控制台输出
        printSignal(signal);
        return true;
    }

    // 美观打印：用于控制台提示套利机会
    void printSignal(const ArbitrageSignal& s) const {
        std::cout << std::fixed << std::setprecision(4);
        std::cout << "\n  ╔══════════════════ ARBITRAGE DETECTED ══════════════════╗" << std::endl;
        std::cout << "  ║ BUY  " << std::setw(20) << std::left << s.buy_exchange
                  << " @ " << std::right << std::setw(12) << s.buy_price << "               ║" << std::endl;
        std::cout << "  ║ SELL " << std::setw(20) << std::left << s.sell_exchange
                  << " @ " << std::right << std::setw(12) << s.sell_price << "               ║" << std::endl;
        std::cout << "  ║ Qty: " << std::setw(10) << s.quantity << " BTC"
                  << "  Gross: $" << std::setw(8) << s.gross_profit
                  << "  Net: $" << std::setw(8) << s.net_profit << "  ║" << std::endl;
        std::cout << "  ╚════════════════════════════════════════════════════════╝\n" << std::endl;
    }
};