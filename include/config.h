//
// config.h
// 配置加载模块：从 JSON 读取交易所/套利/显示配置，并提供默认值。
//

#pragma once

#include "simdjson.h"

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

// 交易所连接配置
struct ExchangeConfig {
    bool enabled;
    std::string host;
    std::string port;
    std::string path;
    std::string symbol;
    std::string subscribe_msg;
};

// 套利参数配置
struct ArbitrageConfig {
    double fee_rate;
    double min_profit;
};

// UI 刷新配置
struct DisplayConfig {
    int refresh_ms;
    int levels;
    int spread_log_interval;
};

// 应用级配置汇总
struct AppConfig {
    ExchangeConfig binance;
    ExchangeConfig okx;
    ArbitrageConfig arbitrage;
    DisplayConfig display;
    bool valid;
};

// 配置读取器：负责 JSON 解析与默认值回退
class ConfigReader {
public:
    // 读取配置文件，失败时回退默认值
    static AppConfig load(const std::string& filepath) {
        AppConfig config;
        config.valid = false;

        // 读取配置文件内容
        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "[Config] Cannot open: " << filepath
                      << ", using defaults" << std::endl;
            return getDefaults();
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string json_str = buffer.str();
        file.close();

    // 使用 simdjson 解析配置
    simdjson::dom::parser parser;
        simdjson::dom::element doc;
        auto error = parser.parse(json_str).get(doc);
        if (error) {
            std::cerr << "[Config] JSON parse error: " << error
                      << ", using defaults" << std::endl;
            return getDefaults();
        }

        try {
            // 逐字段解析（保持与 config.json 对齐）
            auto binance = doc["binance"];
            config.binance.enabled = binance["enabled"].get_bool().value();
            config.binance.host = std::string(binance["host"].get_string().value());
            config.binance.port = std::string(binance["port"].get_string().value());
            config.binance.path = std::string(binance["path"].get_string().value());
            config.binance.symbol = std::string(binance["symbol"].get_string().value());
            // Binance 使用 URL/路径订阅，这里保持为空
            config.binance.subscribe_msg = "";


            auto okx = doc["okx"];
            config.okx.enabled = okx["enabled"].get_bool().value();
            config.okx.host = std::string(okx["host"].get_string().value());
            config.okx.port = std::string(okx["port"].get_string().value());
            config.okx.path = std::string(okx["path"].get_string().value());
            config.okx.symbol = std::string(okx["symbol"].get_string().value());
            config.okx.subscribe_msg = std::string(okx["subscribe"].get_string().value());


            auto arb = doc["arbitrage"];
            config.arbitrage.fee_rate = arb["fee_rate"].get_double().value();
            config.arbitrage.min_profit = arb["min_profit"].get_double().value();


            auto disp = doc["display"];
            config.display.refresh_ms = static_cast<int>(disp["refresh_ms"].get_int64().value());
            config.display.levels = static_cast<int>(disp["levels"].get_int64().value());
            config.display.spread_log_interval = static_cast<int>(disp["spread_log_interval"].get_int64().value());

            config.valid = true;
            std::cout << "[Config] Loaded: " << filepath << std::endl;
            std::cout << "[Config] Symbol: " << config.binance.symbol
                      << "  Fee: " << (config.arbitrage.fee_rate * 100) << "%"
                      << "  Refresh: " << config.display.refresh_ms << "ms" << std::endl;

        } catch (...) {
            std::cerr << "[Config] Error reading fields, using defaults" << std::endl;
            return getDefaults();
        }

        return config;
    }

private:
    // 默认配置（用于解析失败或缺省字段）
    static AppConfig getDefaults() {
        AppConfig config;
        config.binance = {true, "stream.binance.com", "9443",
                         "/ws/btcusdt@depth20@100ms", "BTCUSDT", ""};
        config.okx = {true, "ws.okx.com", "8443", "/ws/v5/public", "BTCUSDT",
                     R"({"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]})"};
        config.arbitrage = {0.001, 0.01};
        config.display = {200, 5, 50};
        config.valid = true;
        return config;
    }
};