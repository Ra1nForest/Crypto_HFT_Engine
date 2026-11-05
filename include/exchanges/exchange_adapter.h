//
// exchange_adapter.h
// 交易所协议适配器接口：纯协议状态机（不含网络），由行情 feed 驱动；定义 Exchange 枚举、配置、统计与处理结果。
//

#pragma once

#include "exchanges/json_util.h"
#include "orderbook.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 支持的交易所（从 market_data_feed.h 移到这里；保留旧的全大写拼写）
enum class Exchange { BINANCE, OKX, BYBIT };

// 交易所显示名称
inline const char* exchangeName(Exchange exchange) noexcept {
    switch (exchange) {
        case Exchange::BINANCE: return "Binance";
        case Exchange::OKX: return "OKX";
        case Exchange::BYBIT: return "Bybit";
    }
    return "Unknown";
}

// 适配器配置
struct AdapterConfig {
    std::string name;            // 显示名称，如 "OKX"
    std::string symbol;          // 交易对显示标签
    std::string subscribe_msg;   // 连接后发送的原始订阅 JSON（"" = 不发送）
    bool verify_checksum = true; // 是否校验 OKX books 的 checksum
    std::size_t max_buffered_events = 2000; // Binance 同步前事件缓冲上限
};

// 适配器统计：普通计数器，只由行情线程写入（feed 每条消息后复制到原子变量供其他线程读取）。
// 含义：
// - messages：onMessage 调用次数；book_updates：使簿发生变化（含重建、因重同步而清空）的消息数
// - snapshots：用快照重建簿的次数（WS 快照、Binance 部分深度、已应用的 REST 快照）
// - deltas：已应用的增量更新数（含同步后回放的缓冲事件）
// - ignored：合法但无关的消息（订阅回执、pong、过期/未同步时的增量等）；parse_errors：格式错误
// - gaps：序列号缺口（含 Binance 同步前缓冲区溢出）；checksum_failures：OKX 校验和不符
// - resyncs：发起的重同步次数（缺口、校验和不符、交叉簿）；rest_snapshots：成功解析的 REST 快照数
// - crossed_books：应用更新后出现交叉簿的次数
struct AdapterStats {            // 普通计数器，只由行情线程写入
    std::uint64_t messages = 0, book_updates = 0, snapshots = 0, deltas = 0, ignored = 0,
                  parse_errors = 0, gaps = 0, checksum_failures = 0, resyncs = 0,
                  rest_snapshots = 0, crossed_books = 0;
};

// 单条消息的处理结果：feed 复用同一个对象，每条消息前调用 reset()
struct AdapterResult {           // 每条消息复用；使用前先调用 reset()
    bool book_updated = false;        // 簿发生变化：需要发布快照
    bool need_rest_snapshot = false;  // feed 需拉取 REST 快照并调用 onRestSnapshot
    bool reconnect = false;           // 断开当前连接并重连
    std::vector<std::string> send;    // 需要立即发送的文本帧（如 unsubscribe + subscribe）
    std::int64_t exchange_ts_ms = 0;  // 本条消息的交易所时间戳（毫秒，无则为 0）
    std::string error;                // 可读的问题描述（空 = 无）；本身不是致命错误

    // 清空全部字段（保留 send/error 的容量，不分配内存）
    void reset() noexcept {
        book_updated = false;
        need_rest_snapshot = false;
        reconnect = false;
        send.clear();
        exchange_ts_ms = 0;
        error.clear();
    }
};

// 适配器基类（非虚接口：公有入口由基类统一实现，各交易所的协议逻辑写在受保护的虚函数里）：
// - 每个连接开始时 feed 调用 onConnected()（重置全部协议状态并返回要发送的帧，通常是订阅消息），
//   之后每读到一条文本消息调用一次 onMessage()。
// - onMessage / onRestSnapshot 是非虚函数，从不抛异常：它们在同一个保护层（runGuarded）里调用子类的
//   handleMessage / handleRestSnapshot，并统一维护 messages / book_updates 计数。格式错误由子类调用
//   malformed() 计入 stats().parse_errors；处理中抛出的任何异常（如内存不足）由基类捕获：计入 parse_errors、
//   resetState() 清空簿与协议状态、要求重连。新交易所因此无需（也无法）自己再写一遍这层保护。
// - 子类必须实现 exchange()、synced()、handleMessage()、resetState()；按需覆盖 onConnected()（默认：重置状态并
//   发送订阅消息）、handleRestSnapshot()（默认：忽略）、pingMessage() / pingIntervalMs()、trimBook()。
// - book() 是完整的本地簿（行情线程私有，其他线程不得直接读取；跨线程用 BookSnapshot 传递）。
// - 统计在适配器的整个生命周期内累计（onConnected 不清零）。
class ExchangeAdapter {
public:
    virtual ~ExchangeAdapter() = default;
    ExchangeAdapter(const ExchangeAdapter&) = delete;
    ExchangeAdapter& operator=(const ExchangeAdapter&) = delete;

    virtual Exchange exchange() const = 0;

    // 新连接：重置全部协议状态，返回要发送的帧（默认：配置了订阅消息就发送它；Binance 通过 URL 路径订阅，通常为空）
    virtual std::vector<std::string> onConnected() {
        resetState();
        std::vector<std::string> frames;
        if (!cfg_.subscribe_msg.empty()) frames.push_back(cfg_.subscribe_msg);
        return frames;
    }

    // 一条 WebSocket 文本消息
    void onMessage(std::string_view payload, AdapterResult& out) noexcept {
        ++stats_.messages;
        runGuarded(out, [&] { handleMessage(payload, out); });
    }

    // REST 快照响应体（feed 在 need_rest_snapshot 后调用；只有 Binance 增量深度模式需要，其他交易所忽略）
    void onRestSnapshot(std::string_view body, AdapterResult& out) noexcept {
        runGuarded(out, [&] { handleRestSnapshot(body, out); });
    }

    virtual std::string pingMessage() const { return std::string(); }  // 应用层心跳消息（"" = 无）
    virtual int pingIntervalMs() const { return 0; }                    // 心跳间隔毫秒（0 = 无）
    virtual bool synced() const = 0;

    const OrderBook& book() const noexcept { return book_; }    // 完整本地簿（行情线程私有）
    const AdapterStats& stats() const noexcept { return stats_; }
    const AdapterConfig& config() const noexcept { return cfg_; }

protected:
    explicit ExchangeAdapter(AdapterConfig cfg) : cfg_(std::move(cfg)) {
        // 与旧 feed 的约定一致：簿标签为 "<name>:<symbol>"
        book_.setSymbol(cfg_.name + ":" + cfg_.symbol);
    }

    // ---------------- 子类实现的协议逻辑 ----------------

    // 处理一条消息。可以抛异常（由 onMessage 捕获并按内部错误处理）；格式错误用 malformed() 报告
    virtual void handleMessage(std::string_view payload, AdapterResult& out) = 0;

    // 处理 REST 快照响应体（默认：忽略）。异常处理同 handleMessage
    virtual void handleRestSnapshot(std::string_view body, AdapterResult& out) {
        (void)body;
        (void)out;
    }

    // 重置全部协议状态（清空簿、回到未同步、丢弃缓冲）。onConnected() 与内部错误时调用
    virtual void resetState() noexcept = 0;

    // 每侧只保留最优的 kMaxBookLevels 档：增量协议会不断带来快照范围之外的远端档位。
    // 子类可扩展（OKX 同步删除被裁掉档位的原始字符串），扩展时最后调用 ExchangeAdapter::trimBook()
    virtual void trimBook() {
        if (static_cast<std::size_t>(book_.getBidCount()) > adapter_detail::kMaxBookLevels ||
            static_cast<std::size_t>(book_.getAskCount()) > adapter_detail::kMaxBookLevels) {
            book_.truncate(adapter_detail::kMaxBookLevels);
        }
    }

    // ---------------- 子类共用的辅助 ----------------

    // 格式错误：parse_errors++，error = "<交易所>: <what> (<bytes> bytes)"
    void malformed(AdapterResult& out, const char* what, std::size_t bytes) {
        ++stats_.parse_errors;
        out.error =
            adapter_detail::concat({exchangeName(exchange()), ": ", what, " (", std::to_string(bytes), " bytes)"});
    }

    // 重同步时重新订阅：send = {unsubscribe, subscribe}（同一条消息里已安排过就不重复）；
    // 没有订阅消息或推导不出退订帧时改为要求重连。unsubscribe_msg 通常来自 makeUnsubscribeMessage
    void requestResubscribe(AdapterResult& out, const std::string& unsubscribe_msg) {
        if (cfg_.subscribe_msg.empty() || unsubscribe_msg.empty()) {
            out.reconnect = true;
            return;
        }
        if (out.send.empty()) {
            out.send.push_back(unsubscribe_msg);
            out.send.push_back(cfg_.subscribe_msg);
        }
    }

    OrderBook book_;
    LevelBatchScratch batch_;   // book_.applyLevels 的工作区（增量消息批量应用，稳态零分配）
    AdapterStats stats_;
    AdapterConfig cfg_;

private:
    // onMessage / onRestSnapshot 共用的保护层：任何异常都转为 internalError；之后若簿有变化则 book_updates++
    template <class Body>
    void runGuarded(AdapterResult& out, Body&& body) noexcept {
        try {
            body();
        } catch (const std::exception& e) {
            internalError(out, e.what());
        } catch (...) {
            internalError(out, "unknown exception");
        }
        if (out.book_updated) ++stats_.book_updates;
    }

    // 内部异常（如内存不足）：状态可能不一致，清空一切并要求重连
    void internalError(AdapterResult& out, const char* what) noexcept {
        ++stats_.parse_errors;
        if (!book_.empty()) out.book_updated = true;
        resetState();
        out.reconnect = true;
        try {
            out.error = std::string(exchangeName(exchange())) + ": internal error: " + what;
        } catch (...) {
        }
    }
};

// 由订阅消息推导退订消息：把 "op":"subscribe" 换成 "op":"unsubscribe"（冒号两侧允许空白）。
// 订阅消息里没有这个记号时返回空串（调用方改为重连）。OKX 与 Bybit 共用。
inline std::string makeUnsubscribeMessage(std::string_view subscribe_msg) {
    constexpr std::string_view kOpKey = "\"op\"";
    constexpr std::string_view kSubscribe = "\"subscribe\"";
    auto isSpace = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; };
    for (std::size_t pos = subscribe_msg.find(kOpKey); pos != std::string_view::npos;
         pos = subscribe_msg.find(kOpKey, pos + 1)) {
        std::size_t i = pos + kOpKey.size();
        while (i < subscribe_msg.size() && isSpace(subscribe_msg[i])) ++i;
        if (i >= subscribe_msg.size() || subscribe_msg[i] != ':') continue;
        ++i;
        while (i < subscribe_msg.size() && isSpace(subscribe_msg[i])) ++i;
        if (subscribe_msg.substr(i, kSubscribe.size()) != kSubscribe) continue;
        std::string out;
        out.reserve(subscribe_msg.size() + 2);
        out.append(subscribe_msg.substr(0, i));
        out.append("\"unsubscribe\"");
        out.append(subscribe_msg.substr(i + kSubscribe.size()));
        return out;
    }
    return std::string();
}
