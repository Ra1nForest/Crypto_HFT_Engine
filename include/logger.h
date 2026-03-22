#pragma once

#include "arbitrage_detector.h"
#include "latency_tracker.h"

#include <string>
#include <fstream>
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <sys/stat.h>

class Logger {
public:
    Logger(const std::string& log_dir = "logs") : log_dir_(log_dir) {
        mkdir(log_dir_.c_str(), 0755);

        std::string date = getCurrentDate();
        std::string filepath = log_dir_ + "/arb_" + date + ".log";

        file_.open(filepath, std::ios::app);
        
        if (!file_.is_open()) {
            std::cerr << "[Logger] Failed to open log file: " << filepath << std::endl;
            return ;
        }

        log("=== Crypto HFT Engine Started ===");
        log("Log file: " + filepath);
        std::cout << "[Logger] Log file: " << filepath << std::endl;
    }

    ~Logger() {
        if (file_.is_open()) {
            log("=== Crypto HFT Engine Stopped ===");
            file_.close();
        }
    }

    void log(const std::string& message) {
        if (!file_.is_open()) return;
        file_ << getTimestamp() << " [INFO] " << message << std::endl;
    }

    void logSignal(const ArbitrageSignal& signal){
        if (!file_.is_open()) return;

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(4);
        ss << "SIGNAL | "
            << " Buy " << signal.buy_exchange << " @ " << signal.buy_price
            << " | Sell" << signal.sell_exchange << " @ " << signal.sell_price
            << " | Qty: " << signal.quantity
            << " | Gross: $" << signal.gross_profit
            << " | Net: $" << signal.net_profit;
        
        file_ << getTimestamp() << " [SIGNAL] " << ss.str() << std::endl;
        file_.flush();
    }

    void logSpread(const std::string& exchang_a, double ask_a,
         const std::string& exchange_b, double bid_b, double diff) {
            if (!file_.is_open()) return;

            std::ostringstream ss;
            ss << std::fixed << std::setprecision(2);
            ss << "SPREAD | "
                << exchang_a << " Ask: " << ask_a
                << " | " << exchange_b << " Bid: " << bid_b
                << " | Diff: $" << diff;
            
            file_ << getTimestamp() << " [SPREAD] " << ss.str() << std::endl;
    }

    void logLatency(const LatencyTracker& tracker) {
        if (!file_.is_open()) return;

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "LATENCY | " << tracker.getName()
            << " | Msgs: " << tracker.getCount()
            << " | Avg: " << tracker.getAvg() << "μs"
            << " | P50: " << tracker.getP50() << "μs"
            << " | P95: " << tracker.getP95() << "μs"
            << " | P99: " << tracker.getP99() << "μs"
            << " | Max: " << tracker.getMax() << "μs";
        
        file_ << getTimestamp() << " [PERF] " << ss.str() << std::endl;
    }

    void logFinalReport(const LatencyTracker& lat_a, const LatencyTracker& lat_b, 
            const ArbitrageDetector& detector) {
        log("=== Final Report ===");
        logLatency(lat_a);
        logLatency(lat_b);

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "SUMMARY | Signals: " << detector.getTotalSignals()
            << " | Net Profit: $" << detector.getTotalNetProfit();
        file_ << getTimestamp() << " [SUMMARY] " << ss.str() << std::endl;

        file_.flush();
    }

    void flush() {
        if (file_.is_open()) {
            file_.flush();
        }
    }

private:
    std::string log_dir_;
    std::ofstream file_;

    static std::string getTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds> (now.time_since_epoch()) % 1000;
        
        std::ostringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
            << "." << std::setfill('0') << std::setw(3) << ms.count();
        return ss.str();
    }

    static std::string getCurrentDate() {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::ostringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d");
        return ss.str();
    }
};