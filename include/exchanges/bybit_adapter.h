//
// bybit_adapter.h
// Bybit v5 现货深度适配器：orderbook.{depth}.{symbol} 的 snapshot/delta 按更新号 u 衔接，缺口时退订 + 重订阅。
//

#pragma once

#include "exchanges/exchange_adapter.h"
#include "exchanges/json_util.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 现货公共频道 wss://stream.bybit.com:443/v5/public/spot，订阅 {"op":"subscribe","args":["orderbook.50.BTCUSDT"]}。
// 消息类型：
// - {"success":true,"ret_msg":"subscribe",...,"op":"subscribe"} → 忽略；"success":false → error。
//   pong（现货 {"op":"pong",...}；合约 {"success":true,"ret_msg":"pong",...}）→ 忽略。
// - {"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":ms,"data":{"s":..,"b":[[p,q]],"a":[[p,q]],"u":N,"seq":S}}
//   → 替换整簿，last_u = u，已同步。
// - "type":"delta" → 未同步时忽略；u == 1 视为快照（服务重启，按文档覆盖本地簿）；u <= last_u 忽略（过期）；
//   u != last_u + 1 → 缺口：gaps++、resyncs++、清空、未同步、send = {unsubscribe, subscribe}；
//   否则逐档 upsert（size "0" 删除），last_u = u。exchange_ts_ms = 顶层 ts。
// - 安全阀：文档只说 u "is a sequence"，并未保证现货严格 +1。若连续 3 次重同步都是由"新快照之后的第一条 delta"
//   触发的，就关闭严格 +1 检查（只设置一次 error），保留过期检查与交叉检查。
//   快照后第一条 delta 正常衔接、或由其他原因触发的重同步，都会把连续计数清零。
// - 应用每条更新（含快照）后若簿交叉 → crossed_books++，按缺口方式重同步。
// - 重同步之后若持续收到 delta 却迟迟没有新快照（超过 kMaxUnsyncedUpdates 条），要求重连。
class BybitAdapter final : public ExchangeAdapter {
public:
    static constexpr int kStrictCheckStrikes = 3;
    static constexpr std::size_t kMaxUnsyncedUpdates = 500;

    explicit BybitAdapter(const AdapterConfig& cfg)
        : ExchangeAdapter(cfg), unsubscribe_msg_(makeUnsubscribeMessage(cfg.subscribe_msg)) {
        book_.reserve(1024);
        bids_.reserve(256);
        asks_.reserve(256);
    }

    Exchange exchange() const override { return Exchange::BYBIT; }

    std::string pingMessage() const override { return R"({"op":"ping"})"; }
    int pingIntervalMs() const override { return 20000; }
    bool synced() const override { return synced_; }

    // ---------------- 诊断（测试用） ----------------
    bool strictSequence() const noexcept { return strict_; }
    std::uint64_t lastUpdateId() const noexcept { return last_u_; }
    const std::string& unsubscribeMessage() const noexcept { return unsubscribe_msg_; }

private:
    using RawLevel = adapter_detail::RawLevel;

    enum : unsigned {
        kTopic = 1u << 0,
        kType = 1u << 1,
        kTs = 1u << 2,
        kData = 1u << 3,
        kSuccess = 1u << 4,
        kRetMsg = 1u << 5,
        kOp = 1u << 6,
        kBids = 1u << 7,   // data.b
        kAsks = 1u << 8,   // data.a
        kUpdateId = 1u << 9,  // data.u
    };

    struct Msg {
        unsigned seen = 0;
        std::string_view topic, type, ret_msg, op;
        bool success = false;
        std::int64_t ts = 0;
        std::uint64_t u = 0;
    };

    // ---------------- 解析 ----------------

    bool parse(std::string_view payload, Msg& m) {
        using namespace adapter_detail;
        bids_.clear();
        asks_.clear();
        od::document doc;
        if (reader_.load(payload, doc) != simdjson::SUCCESS) return false;
        od::object obj;
        if (doc.get_object().get(obj) != simdjson::SUCCESS) return false;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& v = field.value();
            unsigned bit = 0;
            if (key == "topic") bit = kTopic;
            else if (key == "type") bit = kType;
            else if (key == "ts") bit = kTs;
            else if (key == "data") bit = kData;
            else if (key == "success") bit = kSuccess;
            else if (key == "ret_msg") bit = kRetMsg;
            else if (key == "op") bit = kOp;
            else continue;  // cts / conn_id / req_id / args 等
            if ((m.seen & bit) != 0) return false;
            m.seen |= bit;
            bool ok = false;
            switch (bit) {
                case kTopic: ok = readString(v, m.topic); break;
                case kType: ok = readString(v, m.type); break;
                case kTs: ok = readInt64(v, m.ts); break;
                case kData:
                    // 已知是其他主题（Bybit 总是先发 topic）：data 的形状可能完全不同，不解析，迭代器自动跳过。
                    // 注意不能"解析失败后继续"：simdjson On-Demand 在嵌套迭代中遇到致命错误会废弃整个文档，
                    // 外层循环随之静默结束，所以任何嵌套解析失败都必须立即中止整条消息。
                    ok = ((m.seen & kTopic) != 0 && !isOrderbookTopic(m.topic)) || parseData(v, m);
                    break;
                case kSuccess: ok = readBool(v, m.success); break;
                case kRetMsg: ok = readString(v, m.ret_msg); break;
                case kOp: ok = readString(v, m.op); break;
                default: break;
            }
            if (!ok) return false;
        }
        return doc.at_end();
    }

    bool parseData(adapter_detail::od::value& v, Msg& m) {
        using namespace adapter_detail;
        od::object obj;
        if (v.get_object().get(obj) != simdjson::SUCCESS) return false;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& fv = field.value();
            unsigned bit = 0;
            if (key == "b") bit = kBids;
            else if (key == "a") bit = kAsks;
            else if (key == "u") bit = kUpdateId;
            else continue;  // s / seq 等
            if ((m.seen & bit) != 0) return false;
            m.seen |= bit;
            bool ok = false;
            switch (bit) {
                case kBids: ok = parseLevels(fv, bids_); break;
                case kAsks: ok = parseLevels(fv, asks_); break;
                case kUpdateId: ok = readUint64(fv, m.u); break;
                default: break;
            }
            if (!ok) return false;
        }
        return true;
    }

    static bool isOrderbookTopic(std::string_view topic) noexcept { return topic.substr(0, 10) == "orderbook."; }

    // ---------------- 处理 ----------------

    void handleMessage(std::string_view payload, AdapterResult& out) override {
        Msg m;
        if (!parse(payload, m)) {
            malformed(out, "malformed message", payload.size());
            return;
        }
        if ((m.seen & kSuccess) != 0) {
            ++stats_.ignored;  // 订阅/退订回执、合约 pong
            if (!m.success) {
                out.error = adapter_detail::concat({"Bybit ", m.op.empty() ? std::string_view("request") : m.op,
                                                    " failed: ", m.ret_msg});
            }
            return;
        }
        if ((m.seen & kTopic) == 0) {
            if ((m.seen & kData) != 0) {
                malformed(out, "data without topic", payload.size());
            } else {
                ++stats_.ignored;  // 现货 pong {"op":"pong",...} 等
            }
            return;
        }
        if (!isOrderbookTopic(m.topic)) {
            ++stats_.ignored;
            return;
        }
        constexpr unsigned kRequired = kType | kData | kBids | kAsks | kUpdateId;
        if ((m.seen & kRequired) != kRequired) {
            malformed(out, "incomplete orderbook message", payload.size());
            return;
        }
        if ((m.seen & kTs) != 0) out.exchange_ts_ms = m.ts;
        if (m.type == "snapshot") {
            applySnapshot(m.u, out);
        } else if (m.type == "delta") {
            applyDelta(m.u, out);
        } else {
            ++stats_.ignored;
        }
    }

    void applySnapshot(std::uint64_t u, AdapterResult& out) {
        book_.clear();
        for (const RawLevel& l : bids_) book_.setLevel(BookSide::Bid, l.price, l.qty);
        for (const RawLevel& l : asks_) book_.setLevel(BookSide::Ask, l.price, l.qty);
        trimBook();
        book_.setUpdateId(u);
        last_u_ = u;
        synced_ = true;
        deltas_since_snapshot_ = 0;
        unsynced_updates_ = 0;
        ++stats_.snapshots;
        out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            streak_ = 0;
            resync(out);
            out.error = "Bybit: crossed book snapshot, resyncing";
        }
    }

    void applyDelta(std::uint64_t u, AdapterResult& out) {
        if (!synced_) {
            ++stats_.ignored;
            if (++unsynced_updates_ > kMaxUnsyncedUpdates) {
                unsynced_updates_ = 0;
                out.reconnect = true;
                out.error = "Bybit: no snapshot after resubscribing, reconnecting";
            }
            return;
        }
        if (u == 1) {  // 服务重启：这条 delta 实际是完整快照
            applySnapshot(u, out);
            return;
        }
        if (u <= last_u_) {
            ++stats_.ignored;
            return;
        }
        if (strict_ && u != last_u_ + 1) {
            ++stats_.gaps;
            out.error = adapter_detail::concat({"Bybit: update id gap (u=", std::to_string(u), ", last=",
                                                std::to_string(last_u_), "), resyncing"});
            if (deltas_since_snapshot_ == 0) {
                if (++streak_ >= kStrictCheckStrikes) {
                    strict_ = false;
                    out.error += "; update ids are not consecutive right after snapshots (" +
                                 std::to_string(kStrictCheckStrikes) +
                                 " times in a row): strict +1 sequence check disabled";
                }
            } else {
                streak_ = 0;
            }
            resync(out);
            return;
        }
        bool changed = false;
        const auto qty = [](const RawLevel& l) { return l.qty; };
        changed |= book_.applyLevels(BookSide::Bid, bids_.data(), bids_.size(), batch_, qty);
        changed |= book_.applyLevels(BookSide::Ask, asks_.data(), asks_.size(), batch_, qty);
        trimBook();
        book_.setUpdateId(u);
        last_u_ = u;
        if (deltas_since_snapshot_ == 0) streak_ = 0;  // 快照后第一条 delta 正常衔接
        ++deltas_since_snapshot_;
        ++stats_.deltas;
        if (changed) out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            streak_ = 0;
            resync(out);
            out.error = "Bybit: crossed book after delta, resyncing";
        }
    }

    // 重同步：清空簿并发送 unsubscribe + subscribe（推导不出退订帧时要求重连）
    void resync(AdapterResult& out) {
        ++stats_.resyncs;
        if (!book_.empty()) out.book_updated = true;
        book_.clear();
        synced_ = false;
        last_u_ = 0;
        deltas_since_snapshot_ = 0;
        unsynced_updates_ = 0;
        requestResubscribe(out, unsubscribe_msg_);
    }

    void resetState() noexcept override {
        book_.clear();
        synced_ = false;
        last_u_ = 0;
        deltas_since_snapshot_ = 0;
        unsynced_updates_ = 0;
    }

    std::string unsubscribe_msg_;
    adapter_detail::JsonReader reader_;
    std::vector<RawLevel> bids_;
    std::vector<RawLevel> asks_;
    std::uint64_t last_u_ = 0;               // 最近一条已应用消息的 u
    std::uint64_t deltas_since_snapshot_ = 0;  // 最近一次快照之后已应用的 delta 数
    std::size_t unsynced_updates_ = 0;       // 未同步期间收到的 delta 数
    int streak_ = 0;                         // 连续"快照后第一条 delta 即缺口"的重同步次数
    bool strict_ = true;                     // 严格 +1 检查（安全阀触发后永久关闭）
    bool synced_ = false;
};
