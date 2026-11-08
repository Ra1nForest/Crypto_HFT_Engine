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

// 现货公共频道 wss://stream.bybit.com:443/v5/public/spot，订阅 {"op":"subscribe","args":["orderbook.50.BTCUSDT"]}。
// 消息类型：
// - {"success":true,"ret_msg":"subscribe",...,"op":"subscribe"} → 忽略；"success":false → error。
//   pong（现货 {"op":"pong",...}；合约 {"success":true,"ret_msg":"pong",...}）→ 忽略。
// - {"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":ms,"data":{"s":..,"b":[[p,q]],"a":[[p,q]],"u":N,"seq":S}}
//   → 替换整簿，last_u = u（即 book().getUpdateId()），已同步。
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

    explicit BybitAdapter(const AdapterConfig& cfg) : ExchangeAdapter(cfg) {
        book_.reserve(1024);
        bids_.reserve(256);
        asks_.reserve(256);
    }

    Exchange exchange() const override { return Exchange::BYBIT; }

    std::string pingMessage() const override { return R"({"op":"ping"})"; }
    int pingIntervalMs() const override { return 20000; }

    // ---------------- 诊断（测试用） ----------------
    bool strictSequence() const noexcept { return strict_; }
    std::uint64_t lastUpdateId() const noexcept { return book_.getUpdateId(); }

private:
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

    // 单遍解析整条消息（档位写入 bids_ / asks_）；JSON 错误、根不是对象、尾随内容、重复的关键字段都返回 false
    bool parse(std::string_view payload, Msg& m) {
        using namespace adapter_detail;
        bids_.clear();
        asks_.clear();
        od::document doc;
        od::object obj;
        if (reader_.load(payload, doc) != simdjson::SUCCESS || doc.get_object().get(obj) != simdjson::SUCCESS) return false;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& v = field.value();
            bool ok = true;  // cts / conn_id / req_id / args 等无关字段不读取
            if (key == "topic") ok = firstSeen(m.seen, kTopic) && readString(v, m.topic);
            else if (key == "type") ok = firstSeen(m.seen, kType) && readString(v, m.type);
            else if (key == "ts") ok = firstSeen(m.seen, kTs) && readInt(v, m.ts);
            else if (key == "data") ok = firstSeen(m.seen, kData) && parseData(v, m);
            else if (key == "success") ok = firstSeen(m.seen, kSuccess) && readBool(v, m.success);
            else if (key == "ret_msg") ok = firstSeen(m.seen, kRetMsg) && readString(v, m.ret_msg);
            else if (key == "op") ok = firstSeen(m.seen, kOp) && readString(v, m.op);
            if (!ok) return false;
        }
        return doc.at_end();
    }

    // data 对象。已知是其他主题（Bybit 总是先发 topic）：data 的形状可能完全不同，不解析，迭代器自动跳过
    bool parseData(adapter_detail::od::value& v, Msg& m) {
        using namespace adapter_detail;
        if ((m.seen & kTopic) != 0 && !isOrderbookTopic(m.topic)) return true;
        od::object obj;
        if (v.get_object().get(obj) != simdjson::SUCCESS) return false;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& fv = field.value();
            bool ok = true;  // s / seq 等
            if (key == "b") ok = firstSeen(m.seen, kBids) && parseLevels(fv, bids_);
            else if (key == "a") ok = firstSeen(m.seen, kAsks) && parseLevels(fv, asks_);
            else if (key == "u") ok = firstSeen(m.seen, kUpdateId) && readInt(fv, m.u);
            if (!ok) return false;
        }
        return true;
    }

    static bool isOrderbookTopic(std::string_view topic) noexcept { return topic.substr(0, 10) == "orderbook."; }

    // ---------------- 处理 ----------------

    void handleMessage(std::string_view payload, AdapterResult& out) override {
        Msg m;
        if (!parse(payload, m)) return malformed(out, "malformed message", payload.size());
        if ((m.seen & kSuccess) != 0) {
            ++stats_.ignored;  // 订阅/退订回执、合约 pong
            if (!m.success) {
                out.error = adapter_detail::concat({"Bybit ", m.op.empty() ? std::string_view("request") : m.op,
                                                    " failed: ", m.ret_msg});
            }
            return;
        }
        if ((m.seen & kTopic) == 0) {
            if ((m.seen & kData) != 0) return malformed(out, "data without topic", payload.size());
            ++stats_.ignored;  // 现货 pong {"op":"pong",...} 等
            return;
        }
        if (!isOrderbookTopic(m.topic)) {
            ++stats_.ignored;
            return;
        }
        constexpr unsigned kRequired = kType | kData | kBids | kAsks | kUpdateId;
        if ((m.seen & kRequired) != kRequired) return malformed(out, "incomplete orderbook message", payload.size());
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
        replaceBook(bids_.data(), bids_.size(), asks_.data(), asks_.size());
        book_.setUpdateId(u);
        synced_ = true;
        first_delta_ = true;
        ++stats_.snapshots;
        out.book_updated = true;
        resyncIfCrossed(out, "Bybit: crossed book snapshot, resyncing");
    }

    void applyDelta(std::uint64_t u, AdapterResult& out) {
        if (!synced_) return ignoreUnsynced(out);
        if (u == 1) return applySnapshot(u, out);  // 服务重启：这条 delta 实际是完整快照
        const std::uint64_t last_u = book_.getUpdateId();
        if (u <= last_u) {
            ++stats_.ignored;
            return;
        }
        if (strict_ && u != last_u + 1) {
            ++stats_.gaps;
            out.error = adapter_detail::concat({"Bybit: update id gap (u=", std::to_string(u), ", last=",
                                                std::to_string(last_u), "), resyncing"});
            if (!first_delta_) {
                streak_ = 0;
            } else if (++streak_ >= kStrictCheckStrikes) {
                strict_ = false;
                out.error += "; update ids are not consecutive right after snapshots (" +
                             std::to_string(kStrictCheckStrikes) +
                             " times in a row): strict +1 sequence check disabled";
            }
            return resync(out);
        }
        const bool changed = updateBook(bids_.data(), bids_.size(), asks_.data(), asks_.size());
        book_.setUpdateId(u);
        if (first_delta_) streak_ = 0;  // 快照后第一条 delta 正常衔接
        first_delta_ = false;
        ++stats_.deltas;
        if (changed) out.book_updated = true;
        resyncIfCrossed(out, "Bybit: crossed book after delta, resyncing");
    }

    // 应用更新后簿交叉 → crossed_books++，按缺口方式重同步（不算"快照后第一条 delta 的缺口"，连续计数清零）
    void resyncIfCrossed(AdapterResult& out, const char* error) {
        if (!book_.isCrossed()) return;
        ++stats_.crossed_books;
        streak_ = 0;
        resync(out);
        out.error = error;
    }

    // 重同步：清空簿并发送 unsubscribe + subscribe（推导不出退订帧时要求重连）
    void resync(AdapterResult& out) {
        ++stats_.resyncs;
        dropBook(out);
        requestResubscribe(out);
    }

    // 只有快照能让适配器进入已同步状态（并设置 first_delta_），因此没有需要在这里清除的协议状态
    void resetState() noexcept override {}

    bool first_delta_ = false;  // 下一条被应用的 delta 是最近一次快照之后的第一条
    int streak_ = 0;            // 连续"快照后第一条 delta 即缺口"的重同步次数
    bool strict_ = true;        // 严格 +1 检查（安全阀触发后永久关闭）
};
