//
// types.h
// 撮合引擎公共类型：订单请求、成交回报、深度/订单查询行，以及枚举的字符串化与相等比较。
// 参考模型（tests/reference_order_book.h）与优化引擎（matching/order_book.h）共用本文件，
// 两者必须对同一命令序列产生完全相同的事件流（语义见设计文档 2.4）。
//

#pragma once

#include <cstdint>
#include <type_traits>

namespace hft::matching {

using OrderId   = std::uint64_t;   // 客户端分配，在活动（挂单中）订单里唯一
using AccountId = std::uint32_t;   // 订单所有者，用于自成交保护（STP）
using Price     = std::int64_t;    // 整数 tick
using Qty       = std::int64_t;    // 整数手数
using TradeId   = std::uint64_t;   // 每个订单簿从 1 开始、每笔成交加 1
using SymbolId  = std::uint32_t;   // 多交易对路由使用

enum class Side : std::uint8_t { Buy, Sell };
enum class OrderType : std::uint8_t { Limit, Market };
enum class TimeInForce : std::uint8_t { GTC, IOC, FOK };
enum class StpMode : std::uint8_t { None, CancelTaker, CancelMaker, CancelBoth };
enum class RequestType : std::uint8_t { New, Cancel, Modify };
enum class RejectReason : std::uint8_t {
    InvalidQuantity, InvalidPrice, InvalidFlags, DuplicateOrderId,
    UnknownOrder, PostOnlyWouldCross, CapacityExceeded, UnknownSymbol };
enum class CancelReason : std::uint8_t {
    UserRequested, IocRemainder, MarketRemainder, FokUnfillable, SelfTradePrevention };

// 新订单请求（市价单忽略 price 字段）
struct OrderRequest {
    OrderId id = 0; AccountId account = 0; Side side = Side::Buy;
    OrderType type = OrderType::Limit; TimeInForce tif = TimeInForce::GTC;
    Price price = 0; Qty qty = 0; bool post_only = false; StpMode stp = StpMode::None;
};

// 成交回报：价格恒为 maker（挂单方）价格；*_remaining 为本笔成交之后的剩余量
struct Trade {
    TradeId trade_id; OrderId maker_id; OrderId taker_id;
    AccountId maker_account; AccountId taker_account; Side taker_side;
    Price price; Qty qty; Qty maker_remaining; Qty taker_remaining;
};

// 深度查询的一行：价格、该档剩余总量、该档订单数
struct LevelInfo { Price price; Qty qty; std::uint32_t orders; };

// 订单查询的一行：remaining 为当前未成交（挂单）数量
struct OrderInfo { OrderId id; AccountId account; Side side; Price price;
                   Qty remaining; bool post_only; StpMode stp; };

// 引擎线程的命令/事件需要按位拷贝穿过 SPSC 队列
static_assert(std::is_trivially_copyable_v<OrderRequest>);
static_assert(std::is_trivially_copyable_v<Trade>);
static_assert(std::is_trivially_copyable_v<LevelInfo>);
static_assert(std::is_trivially_copyable_v<OrderInfo>);

// ---- 枚举转字符串（未知取值返回 "?"）----

constexpr const char* toString(Side v) noexcept {
    switch (v) {
        case Side::Buy: return "Buy";
        case Side::Sell: return "Sell";
    }
    return "?";
}

constexpr const char* toString(OrderType v) noexcept {
    switch (v) {
        case OrderType::Limit: return "Limit";
        case OrderType::Market: return "Market";
    }
    return "?";
}

constexpr const char* toString(TimeInForce v) noexcept {
    switch (v) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
    }
    return "?";
}

constexpr const char* toString(StpMode v) noexcept {
    switch (v) {
        case StpMode::None: return "None";
        case StpMode::CancelTaker: return "CancelTaker";
        case StpMode::CancelMaker: return "CancelMaker";
        case StpMode::CancelBoth: return "CancelBoth";
    }
    return "?";
}

constexpr const char* toString(RequestType v) noexcept {
    switch (v) {
        case RequestType::New: return "New";
        case RequestType::Cancel: return "Cancel";
        case RequestType::Modify: return "Modify";
    }
    return "?";
}

constexpr const char* toString(RejectReason v) noexcept {
    switch (v) {
        case RejectReason::InvalidQuantity: return "InvalidQuantity";
        case RejectReason::InvalidPrice: return "InvalidPrice";
        case RejectReason::InvalidFlags: return "InvalidFlags";
        case RejectReason::DuplicateOrderId: return "DuplicateOrderId";
        case RejectReason::UnknownOrder: return "UnknownOrder";
        case RejectReason::PostOnlyWouldCross: return "PostOnlyWouldCross";
        case RejectReason::CapacityExceeded: return "CapacityExceeded";
        case RejectReason::UnknownSymbol: return "UnknownSymbol";
    }
    return "?";
}

constexpr const char* toString(CancelReason v) noexcept {
    switch (v) {
        case CancelReason::UserRequested: return "UserRequested";
        case CancelReason::IocRemainder: return "IocRemainder";
        case CancelReason::MarketRemainder: return "MarketRemainder";
        case CancelReason::FokUnfillable: return "FokUnfillable";
        case CancelReason::SelfTradePrevention: return "SelfTradePrevention";
    }
    return "?";
}

// ---- 逐字段相等比较（!= 为其取反，C++17 不会自动生成）----

constexpr bool operator==(const Trade& a, const Trade& b) noexcept {
    return a.trade_id == b.trade_id && a.maker_id == b.maker_id && a.taker_id == b.taker_id &&
           a.maker_account == b.maker_account && a.taker_account == b.taker_account &&
           a.taker_side == b.taker_side && a.price == b.price && a.qty == b.qty &&
           a.maker_remaining == b.maker_remaining && a.taker_remaining == b.taker_remaining;
}
constexpr bool operator!=(const Trade& a, const Trade& b) noexcept { return !(a == b); }

constexpr bool operator==(const LevelInfo& a, const LevelInfo& b) noexcept {
    return a.price == b.price && a.qty == b.qty && a.orders == b.orders;
}
constexpr bool operator!=(const LevelInfo& a, const LevelInfo& b) noexcept { return !(a == b); }

constexpr bool operator==(const OrderInfo& a, const OrderInfo& b) noexcept {
    return a.id == b.id && a.account == b.account && a.side == b.side && a.price == b.price &&
           a.remaining == b.remaining && a.post_only == b.post_only && a.stp == b.stp;
}
constexpr bool operator!=(const OrderInfo& a, const OrderInfo& b) noexcept { return !(a == b); }

}  // namespace hft::matching
