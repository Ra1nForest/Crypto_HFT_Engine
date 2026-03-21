#pragma once

#include "orderbook.h"

#include <cstdlib>
#include <string>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>

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

class ArbitrageDetector {
    int total_signals_;
    double fee_rate_;
    double min_profit_;
    double total_net_profit_;
    std::vector<ArbitrageSignal> signals_;

    void printSignal(const ArbitrageSignal& s) const {
        std::cout << std::fixed << std::setprecision(4);
        std::cout << "\n  ╔═══════════════ 🚨 ARBITRAGE DETECTED! ═══════════════╗" << std::endl;
        std::cout << "  ║ BUY  " << s.buy_exchange 
                  << " @ " << s.buy_price << std::endl;
        std::cout << "  ║ SELL " << s.sell_exchange 
                  << " @ " << s.sell_price << std::endl;
        std::cout << "  ║ Qty: " << s.quantity << " BTC" << std::endl;
        std::cout << "  ║ Gross: $" << s.gross_profit 
                  << "  Net: $" << s.net_profit << std::endl;
        std::cout << "  ╚════════════════════════════════════════════════════════╝\n" << std::endl;
    }

public:
    ArbitrageDetector(double fee_rate = 0.001, double min_profit = 0.01)
        : total_signals_(0),
        fee_rate_(fee_rate),
        min_profit_(min_profit),
        total_net_profit_(0.0) {}
    
    bool detect(const OrderBook& book_a, const OrderBook& book_b) {
        bool found = false;

        auto a_ask = book_a.getBestAsk();
        auto b_bid = book_b.getBestBid();

        if (a_ask.price > 0 && b_bid.price > 0) {
            double gross = b_bid.price - a_ask.price;
            if (gross > 0) {
                double qty = std::min(a_ask.quantity, b_bid.quantity);
                double fee = (a_ask.price + b_bid.price) * qty * fee_rate_;
                double net = gross * qty - fee;

                if (net > min_profit_) {
                    ArbitrageSignal signal;
                    signal.buy_exchange = book_a.getSymbol();
                    signal.sell_exchange = book_b.getSymbol();
                    signal.buy_price = a_ask.price;
                    signal.sell_price = b_bid.price;
                    signal.quantity = qty;
                    signal.gross_profit = gross * qty;
                    signal.net_profit = net;
                    signal.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                    signals_.push_back(signal);
                    total_signals_++;
                    total_net_profit_ += net;
                    printSignal(signal);
                    found = true;
                }
            }
        }


        auto b_ask = book_b.getBestAsk();
        auto a_bid = book_a.getBestBid();
 
        if (b_ask.price > 0 && a_bid.price > 0) {
            double gross = a_bid.price - b_ask.price;
            if (gross > 0) {
                double qty = std::min(b_ask.quantity, a_bid.quantity);
                double fee = (b_ask.price + a_bid.price) * qty * fee_rate_;
                double net = gross * qty - fee;
 
                if (net > min_profit_) {
                    ArbitrageSignal signal;
                    signal.buy_exchange = book_b.getSymbol();
                    signal.sell_exchange = book_a.getSymbol();
                    signal.buy_price = b_ask.price;
                    signal.sell_price = a_bid.price;
                    signal.quantity = qty;
                    signal.gross_profit = gross * qty;
                    signal.net_profit = net;
                    signal.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
 
                    signals_.push_back(signal);
                    total_signals_++;
                    total_net_profit_ += net;
                    printSignal(signal);
                    found = true;
                }
            }
        }

        return found;
    }

    void printSpreadStatus(const OrderBook& book_a, const OrderBook& book_b) const {
        auto a_ask = book_a.getBestAsk();
        auto a_bid = book_a.getBestBid();
        auto b_ask = book_b.getBestAsk();
        auto b_bid = book_b.getBestBid();
 
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "  ┌─────────────── Arbitrage Monitor ───────────────┐" << std::endl;
        std::cout << "  │ Direction 1: Buy " << book_a.getSymbol() 
                  << " → Sell " << book_b.getSymbol() << std::endl;
        std::cout << "  │   " << book_a.getSymbol() << " Ask: " << a_ask.price
                  << "  vs  " << book_b.getSymbol() << " Bid: " << b_bid.price
                  << "  Diff: " << (b_bid.price - a_ask.price) << std::endl;
        std::cout << "  │" << std::endl;
        std::cout << "  │ Direction 2: Buy " << book_b.getSymbol() 
                  << " → Sell " << book_a.getSymbol() << std::endl;
        std::cout << "  │   " << book_b.getSymbol() << " Ask: " << b_ask.price
                  << "  vs  " << book_a.getSymbol() << " Bid: " << a_bid.price
                  << "  Diff: " << (a_bid.price - b_ask.price) << std::endl;
        std::cout << "  │" << std::endl;
        std::cout << "  │ Fee Rate: " << (fee_rate_ * 100) << "% per side"
                  << "  │  Min Profit: $" << min_profit_ << std::endl;
        std::cout << "  │ Total Signals: " << total_signals_
                  << "  │  Total Net Profit: $" << total_net_profit_ << std::endl;
        std::cout << "  └──────────────────────────────────────────────────┘" << std::endl;
    }
 
    int getTotalSignals() const { return total_signals_; }
    double getTotalNetProfit() const { return total_net_profit_; }
};