//
// logger.h
// 日志模块：记录系统启动、套利信号、价差与延迟统计。
//

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

// 文件日志器：按日期创建日志文件，支持多种类型的记录
class Logger {
public:
    // log_dir: 日志目录，默认 logs
    Logger(const std::string& log_dir = "logs") : log_dir_(log_dir) {
        mkdir(log_dir_.c_str(), 0755);

        std::string date = getCurrentDate();
        std::string filepath = log_dir_ + "/arb_" + date + ".log";

    // 以追加方式打开文件，避免覆盖
    file_.open(filepath, std::ios::app);
        
        if (!file_.is_open()) {
            std::cerr << "[Logger] Failed to open log file: " << filepath << std::endl;
            return ;
        }

        log("=== Crypto HFT Engine Started ===");
        log("Log file: " + filepath);
        std::cout << "[Logger] Log file: " << filepath << std::endl;
    }

    // 析构时写入停止信息
    ~Logger() {
        if (file_.is_open()) {
            log("=== Crypto HFT Engine Stopped ===");
            file_.close();
        }
    }

    // 通用文本日志
    void log(const std::string& message) {
        if (!file_.is_open()) return;
        file_ << getTimestamp() << " [INFO] " << message << std::endl;
    }

    // 记录套利信号
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

    // 记录双边价差（周期性写入）
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

    // 记录延迟统计
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

    // 记录最终报告（汇总统计）
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

    // 强制刷新缓冲区
    void flush() {
        if (file_.is_open()) {
            file_.flush();
        }
    }

private:
    // 日志目录与文件句柄
    std::string log_dir_;
    std::ofstream file_;

    // 获取精确到毫秒的时间戳
    static std::string getTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds> (now.time_since_epoch()) % 1000;
        
        std::ostringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
            << "." << std::setfill('0') << std::setw(3) << ms.count();
        return ss.str();
    }

    // 当前日期（用于按天分文件）
    static std::string getCurrentDate() {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::ostringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d");
        return ss.str();
    }
};