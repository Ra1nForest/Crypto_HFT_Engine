//
// adapters.h
// 交易所适配器工厂：按 Exchange 创建对应的协议适配器（Binance / OKX / Bybit）。
// Exchange adapter factory: creates the protocol adapter for a given Exchange (Binance / OKX / Bybit).
//

#pragma once

#include "exchanges/binance_adapter.h"
#include "exchanges/bybit_adapter.h"
#include "exchanges/exchange_adapter.h"
#include "exchanges/okx_adapter.h"

#include <memory>

// 创建适配器；未知的枚举值返回 nullptr
// Creates an adapter; returns nullptr for an unknown enum value
inline std::unique_ptr<ExchangeAdapter> makeAdapter(Exchange exchange, const AdapterConfig& cfg) {
    switch (exchange) {
        case Exchange::BINANCE: return std::make_unique<BinanceAdapter>(cfg);
        case Exchange::OKX: return std::make_unique<OkxAdapter>(cfg);
        case Exchange::BYBIT: return std::make_unique<BybitAdapter>(cfg);
    }
    return nullptr;
}
