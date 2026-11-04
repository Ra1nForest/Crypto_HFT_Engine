//
// binance_adapter.h
// Binance 现货深度适配器：部分深度快照（depth20）直接替换；增量深度（depth）按官方"本地订单簿维护"流程与 REST 快照同步。
//

#pragma once

#include "exchanges/exchange_adapter.h"
#include "exchanges/json_util.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 每条消息自动识别类型：
// - combined stream 包装 {"stream":"...","data":{...}} → 处理 data。
// - 部分深度 {"lastUpdateId":N,"bids":[...],"asks":[...]} → 替换整簿，update id = N，已同步（无时间戳）。
// - 增量深度 {"e":"depthUpdate","E":ms,"U":first,"u":last,"b":[...],"a":[...]}，官方流程
//   （docs/binance_ws.md "How to manage a local order book correctly"）：
//   1. 未同步时缓冲事件（上限 max_buffered_events，溢出 → 丢弃缓冲、计一次 gap、重新请求快照），
//      每轮尝试只置一次 need_rest_snapshot；feed 拉取 REST 快照后调用 onRestSnapshot。
//   2. 快照 lastUpdateId L < 第一条缓冲事件的 U → 快照太旧，再次请求（缓冲保留）。
//   3. 否则用快照替换簿，丢弃 u <= L 的缓冲事件，其余按更新规则回放。
//   更新规则：u <= 本地 id → 忽略（过期）；U > 本地 id + 1 → 缺口：gaps++、resyncs++、清空簿、未同步、
//   缓冲本事件并请求快照；否则逐档 upsert（数量 0 删除），本地 id = u。
//   （因此快照后第一条被应用的事件满足 U <= L+1 <= u。）
// - {"e":"serverShutdown"} → reconnect；{"result":null,"id":N} → 忽略；{"code":..,"msg":..} → error。
// - 每条更新（含快照）应用后若簿交叉 → crossed_books++，按缺口方式重同步（本事件同样进入缓冲）。
// 解析采用"先完整解析到暂存区、再应用"：格式错误的消息整条丢弃，不会把簿改到一半；
// 被丢弃的增量会在下一条事件上表现为缺口，从而自动重同步。
class BinanceAdapter final : public ExchangeAdapter {
public:
    // 同步前缓冲的档位总数上限（防止异常大的事件耗尽内存；正常远远达不到）
    static constexpr std::size_t kMaxBufferedLevels = std::size_t{1} << 21;

    explicit BinanceAdapter(const AdapterConfig& cfg) : ExchangeAdapter(cfg) {
        book_.reserve(2048);
        bids_.reserve(1024);
        asks_.reserve(1024);
    }

    Exchange exchange() const override { return Exchange::BINANCE; }

    // 新连接：重置全部协议状态；Binance 通过 URL 路径订阅，subscribe_msg 通常为空
    std::vector<std::string> onConnected() override {
        resetState();
        std::vector<std::string> frames;
        if (!cfg_.subscribe_msg.empty()) frames.push_back(cfg_.subscribe_msg);
        return frames;
    }

    void onMessage(std::string_view payload, AdapterResult& out) override {
        ++stats_.messages;
        try {
            handleMessage(payload, out);
        } catch (const std::exception& e) {
            internalError(out, e.what());
        } catch (...) {
            internalError(out, "unknown exception");
        }
        if (out.book_updated) ++stats_.book_updates;
    }

    // REST 深度快照响应体（feed 在 need_rest_snapshot 后调用）。拉取失败时 feed 可传入空串或错误体：
    // 解析失败 / 错误体 {"code":..,"msg":..} → error + need_rest_snapshot（feed 退避后重试）。
    // 未在等待快照时收到的响应体被忽略。
    void onRestSnapshot(std::string_view body, AdapterResult& out) override {
        try {
            handleRestSnapshot(body, out);
        } catch (const std::exception& e) {
            internalError(out, e.what());
        } catch (...) {
            internalError(out, "unknown exception");
        }
        if (out.book_updated) ++stats_.book_updates;
    }

    bool synced() const override { return synced_; }

    // ---------------- 诊断（测试用） ----------------
    std::size_t bufferedEvents() const noexcept { return events_.size(); }
    bool awaitingSnapshot() const noexcept { return awaiting_snapshot_; }
    std::uint64_t lastUpdateId() const noexcept { return local_id_; }
    // 第一条缓冲事件的 U（无缓冲时为 0）
    std::uint64_t firstBufferedId() const noexcept { return events_.empty() ? 0 : events_.front().first_id; }

private:
    using RawLevel = adapter_detail::RawLevel;

    // 消息中出现过的字段（位掩码）
    enum : unsigned {
        kEventType = 1u << 0,      // "e"
        kEventTime = 1u << 1,      // "E"
        kFirstId = 1u << 2,        // "U"
        kFinalId = 1u << 3,        // "u"
        kDiffBids = 1u << 4,       // "b"
        kDiffAsks = 1u << 5,       // "a"
        kLastUpdateId = 1u << 6,   // "lastUpdateId"
        kSnapBids = 1u << 7,       // "bids"
        kSnapAsks = 1u << 8,       // "asks"
        kResult = 1u << 9,         // "result"
        kCode = 1u << 10,          // "code"
        kMsgText = 1u << 11,       // "msg"
        kData = 1u << 12,          // "data"（combined stream 包装）
        kDiffKeys = kFirstId | kFinalId | kDiffBids | kDiffAsks,
        kSnapKeys = kLastUpdateId | kSnapBids | kSnapAsks,
    };

    // 单遍扫描得到的字段（档位写入 bids_ / asks_）
    struct Msg {
        unsigned seen = 0;
        std::string_view event;
        std::int64_t event_time = 0;
        std::uint64_t first_id = 0;
        std::uint64_t final_id = 0;
        std::uint64_t last_update_id = 0;
        std::string_view code;
        std::string_view text;
    };

    // 同步前缓冲的增量事件：档位存放在 event_levels_ 中（先买后卖，按事件顺序连续存放）
    struct BufferedEvent {
        std::uint64_t first_id;
        std::uint64_t final_id;
        std::size_t bid_begin;
        std::size_t bid_count;
        std::size_t ask_begin;
        std::size_t ask_count;
    };

    enum class Outcome { Applied, Stale, Gap };

    static double qtyOf(const RawLevel& l) noexcept { return l.qty; }
    static double qtyOf(const PriceLevel& l) noexcept { return l.quantity; }

    // ---------------- 解析 ----------------

    // 载入并单遍解析整条消息；JSON 错误、根不是对象、尾随内容、重复的关键字段都返回 false
    bool parse(std::string_view payload, Msg& m) {
        bids_.clear();
        asks_.clear();
        adapter_detail::od::document doc;
        if (reader_.load(payload, doc) != simdjson::SUCCESS) return false;
        adapter_detail::od::object obj;
        if (doc.get_object().get(obj) != simdjson::SUCCESS) return false;
        if (!parseObject(obj, m, true)) return false;
        return doc.at_end();
    }

    bool parseObject(adapter_detail::od::object& obj, Msg& m, bool allow_wrapper) {
        using namespace adapter_detail;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& v = field.value();
            unsigned bit = 0;
            unsigned conflict = 0;  // 与本字段互斥的字段（买/卖档位数组只能出现一次，无论 "b" 还是 "bids"）
            if (key == "e") {
                bit = kEventType;
            } else if (key == "E") {
                bit = kEventTime;
            } else if (key == "U") {
                bit = kFirstId;
            } else if (key == "u") {
                bit = kFinalId;
            } else if (key == "b" || key == "bids") {
                bit = key.size() == 1 ? kDiffBids : kSnapBids;
                conflict = kDiffBids | kSnapBids;
            } else if (key == "a" || key == "asks") {
                bit = key.size() == 1 ? kDiffAsks : kSnapAsks;
                conflict = kDiffAsks | kSnapAsks;
            } else if (key == "lastUpdateId") {
                bit = kLastUpdateId;
            } else if (key == "result") {
                bit = kResult;
            } else if (key == "code") {
                bit = kCode;
            } else if (key == "msg") {
                bit = kMsgText;
            } else if (key == "data" && allow_wrapper) {
                bit = kData;
            } else {
                continue;  // 无关字段（"s"、"stream"、"id" 等）：不读取，迭代器自动跳过
            }
            if ((m.seen & (bit | conflict)) != 0) return false;  // 重复字段
            m.seen |= bit;
            bool ok = false;
            switch (bit) {
                case kEventType: ok = readString(v, m.event); break;
                case kEventTime: ok = readInt64(v, m.event_time); break;
                case kFirstId: ok = readUint64(v, m.first_id); break;
                case kFinalId: ok = readUint64(v, m.final_id); break;
                case kDiffBids:
                case kSnapBids: ok = parseLevels(v, bids_); break;
                case kDiffAsks:
                case kSnapAsks: ok = parseLevels(v, asks_); break;
                case kLastUpdateId: ok = readUint64(v, m.last_update_id); break;
                case kResult: ok = true; break;  // 值不关心（null / true / 列表）
                case kCode: ok = readScalarText(v, m.code); break;
                case kMsgText: ok = readString(v, m.text); break;
                case kData: {
                    od::object inner;
                    ok = v.get_object().get(inner) == simdjson::SUCCESS && parseObject(inner, m, false);
                    break;
                }
                default: break;
            }
            if (!ok) return false;
        }
        return true;
    }

    // ---------------- WebSocket 消息 ----------------

    void handleMessage(std::string_view payload, AdapterResult& out) {
        Msg m;
        if (!parse(payload, m)) {
            malformed(out, "malformed message", payload.size());
            return;
        }
        if ((m.seen & kEventType) != 0) {
            if (m.event == "depthUpdate") {
                const bool complete = (m.seen & kDiffKeys) == kDiffKeys;
                const bool mixed = (m.seen & kSnapKeys) != 0;
                if (!complete || mixed || m.first_id > m.final_id) {
                    malformed(out, "malformed depthUpdate", payload.size());
                    return;
                }
                if ((m.seen & kEventTime) != 0) out.exchange_ts_ms = m.event_time;
                handleDiff(m, out);
            } else if (m.event == "serverShutdown") {
                ++stats_.ignored;
                out.reconnect = true;
                out.error = "Binance: serverShutdown event, reconnecting";
            } else {
                ++stats_.ignored;  // 其他事件类型（同一连接上订阅了别的流）
            }
            return;
        }
        if ((m.seen & kSnapKeys) == kSnapKeys && (m.seen & kDiffKeys) == 0) {
            handlePartialDepth(m, out);
            return;
        }
        if ((m.seen & (kSnapKeys | kDiffKeys)) != 0) {
            malformed(out, "incomplete depth message", payload.size());
            return;
        }
        ++stats_.ignored;
        if ((m.seen & kCode) != 0) {
            out.error = adapter_detail::concat({"Binance error ", m.code, ": ", m.text});
        }
    }

    // 部分深度快照：直接替换（比当前簿旧的快照忽略）
    void handlePartialDepth(const Msg& m, AdapterResult& out) {
        if (synced_ && m.last_update_id < local_id_) {
            ++stats_.ignored;
            return;
        }
        replaceBook(m.last_update_id);
        clearBuffer();
        awaiting_snapshot_ = false;
        synced_ = true;
        ++stats_.snapshots;
        out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            ++stats_.resyncs;
            book_.clear();
            local_id_ = 0;
            synced_ = false;
            out.error = "Binance: crossed depth snapshot discarded";
        }
    }

    void handleDiff(const Msg& m, AdapterResult& out) {
        if (!synced_) {
            bufferEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(), asks_.size(), out);
            return;
        }
        bool changed = false;
        const Outcome r = applyEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(),
                                     asks_.size(), changed);
        if (r == Outcome::Stale) {
            ++stats_.ignored;
            return;
        }
        if (r == Outcome::Gap) {
            ++stats_.gaps;
            ++stats_.resyncs;
            out.error = adapter_detail::concat({"Binance: update id gap (U=", std::to_string(m.first_id),
                                                ", local=", std::to_string(local_id_), "), resyncing"});
            dropBook(out);
            bufferEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(), asks_.size(), out);
            return;
        }
        ++stats_.deltas;
        if (changed) out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            ++stats_.resyncs;
            out.error = "Binance: crossed book after update, resyncing";
            dropBook(out);
            bufferEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(), asks_.size(), out);
        }
    }

    // ---------------- REST 快照 ----------------

    void handleRestSnapshot(std::string_view body, AdapterResult& out) {
        if (!awaiting_snapshot_) {
            ++stats_.ignored;
            return;
        }
        Msg m;
        if (!parse(body, m)) {
            ++stats_.parse_errors;
            out.error = adapter_detail::concat(
                {"Binance: REST snapshot is not valid depth JSON (", std::to_string(body.size()), " bytes)"});
            out.need_rest_snapshot = true;
            return;
        }
        if ((m.seen & kSnapKeys) != kSnapKeys || (m.seen & (kDiffKeys | kEventType)) != 0) {
            if ((m.seen & kCode) != 0) {
                out.error = adapter_detail::concat({"Binance: REST snapshot error ", m.code, ": ", m.text});
            } else {
                ++stats_.parse_errors;
                out.error = "Binance: REST snapshot has an unexpected shape";
            }
            out.need_rest_snapshot = true;
            return;
        }
        ++stats_.rest_snapshots;
        const std::uint64_t last_id = m.last_update_id;
        if (!events_.empty() && last_id < events_.front().first_id) {
            // 快照比第一条缓冲事件还旧：缓冲保留，重新拉取
            out.error = adapter_detail::concat({"Binance: REST snapshot too old (lastUpdateId=",
                                                std::to_string(last_id), " < first buffered U=",
                                                std::to_string(events_.front().first_id), "), fetching again"});
            out.need_rest_snapshot = true;
            return;
        }
        replaceBook(last_id);
        ++stats_.snapshots;
        synced_ = true;
        awaiting_snapshot_ = false;
        out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            ++stats_.resyncs;
            out.error = "Binance: crossed REST snapshot, fetching again";
            book_.clear();
            local_id_ = 0;
            synced_ = false;
            awaiting_snapshot_ = true;  // 缓冲保留，同一轮尝试继续
            out.need_rest_snapshot = true;
            return;
        }
        replayBuffer(out);
    }

    // 快照之后回放缓冲：丢弃 u <= L 的事件，其余按更新规则应用；中途出现缺口/交叉则从该事件起保留缓冲并重新请求
    void replayBuffer(AdapterResult& out) {
        for (std::size_t i = 0; i < events_.size(); ++i) {
            const BufferedEvent& e = events_[i];
            bool changed = false;
            const PriceLevel* levels = event_levels_.data();
            const Outcome r = applyEvent(e.first_id, e.final_id, levels + e.bid_begin, e.bid_count,
                                         levels + e.ask_begin, e.ask_count, changed);
            if (r == Outcome::Stale) {
                ++stats_.ignored;
                continue;
            }
            const bool crossed = r == Outcome::Applied && book_.isCrossed();
            if (r == Outcome::Applied && !crossed) {
                ++stats_.deltas;
                continue;
            }
            if (crossed) {
                ++stats_.deltas;
                ++stats_.crossed_books;
                out.error = "Binance: crossed book while replaying buffered events, resyncing";
            } else {
                ++stats_.gaps;
                out.error = adapter_detail::concat({"Binance: update id gap in buffered events (U=",
                                                    std::to_string(e.first_id), ", local=",
                                                    std::to_string(local_id_), "), resyncing"});
            }
            ++stats_.resyncs;
            dropBufferedPrefix(i);
            book_.clear();
            local_id_ = 0;
            synced_ = false;
            awaiting_snapshot_ = true;
            out.need_rest_snapshot = true;
            return;
        }
        clearBuffer();
    }

    // ---------------- 簿与缓冲操作 ----------------

    // 更新规则（同步后）
    template <class Level>
    Outcome applyEvent(std::uint64_t first_id, std::uint64_t final_id, const Level* bids, std::size_t nb,
                       const Level* asks, std::size_t na, bool& changed) {
        if (final_id <= local_id_) return Outcome::Stale;
        if (first_id > local_id_ + 1) return Outcome::Gap;
        for (std::size_t i = 0; i < nb; ++i) changed |= book_.setLevel(BookSide::Bid, bids[i].price, qtyOf(bids[i]));
        for (std::size_t i = 0; i < na; ++i) changed |= book_.setLevel(BookSide::Ask, asks[i].price, qtyOf(asks[i]));
        trimBook();
        local_id_ = final_id;
        book_.setUpdateId(final_id);
        return Outcome::Applied;
    }

    // 用暂存区的档位替换整簿
    void replaceBook(std::uint64_t update_id) {
        book_.clear();
        for (const RawLevel& l : bids_) book_.setLevel(BookSide::Bid, l.price, l.qty);
        for (const RawLevel& l : asks_) book_.setLevel(BookSide::Ask, l.price, l.qty);
        trimBook();
        book_.setUpdateId(update_id);
        local_id_ = update_id;
    }

    void trimBook() {
        if (static_cast<std::size_t>(book_.getBidCount()) > adapter_detail::kMaxBookLevels ||
            static_cast<std::size_t>(book_.getAskCount()) > adapter_detail::kMaxBookLevels) {
            book_.truncate(adapter_detail::kMaxBookLevels);
        }
    }

    // 丢弃本地簿与缓冲，回到"缓冲事件 + 等待 REST 快照"阶段（之后的 bufferEvent 发起新一轮请求）
    void dropBook(AdapterResult& out) {
        if (!book_.empty()) out.book_updated = true;
        book_.clear();
        local_id_ = 0;
        synced_ = false;
        awaiting_snapshot_ = false;
        clearBuffer();
    }

    // 缓冲一条事件；溢出时丢弃旧缓冲、计 gap 并开始新一轮尝试；每轮尝试只请求一次快照
    template <class Level>
    void bufferEvent(std::uint64_t first_id, std::uint64_t final_id, const Level* bids, std::size_t nb,
                     const Level* asks, std::size_t na, AdapterResult& out) {
        const std::size_t max_events = std::max<std::size_t>(1, cfg_.max_buffered_events);
        if (events_.size() >= max_events || event_levels_.size() + nb + na > kMaxBufferedLevels) {
            ++stats_.gaps;
            clearBuffer();
            awaiting_snapshot_ = false;
            out.error = "Binance: pre-sync event buffer overflow, requesting a fresh snapshot";
        }
        BufferedEvent e{first_id, final_id, event_levels_.size(), nb, event_levels_.size() + nb, na};
        for (std::size_t i = 0; i < nb; ++i) event_levels_.push_back({bids[i].price, qtyOf(bids[i])});
        for (std::size_t i = 0; i < na; ++i) event_levels_.push_back({asks[i].price, qtyOf(asks[i])});
        events_.push_back(e);
        if (!awaiting_snapshot_) {
            awaiting_snapshot_ = true;
            out.need_rest_snapshot = true;
        }
    }

    // 删除前 n 条缓冲事件及其档位
    void dropBufferedPrefix(std::size_t n) {
        if (n == 0) return;
        if (n >= events_.size()) {
            clearBuffer();
            return;
        }
        const std::size_t level_start = events_[n].bid_begin;
        event_levels_.erase(event_levels_.begin(), event_levels_.begin() + static_cast<std::ptrdiff_t>(level_start));
        events_.erase(events_.begin(), events_.begin() + static_cast<std::ptrdiff_t>(n));
        for (BufferedEvent& e : events_) {
            e.bid_begin -= level_start;
            e.ask_begin -= level_start;
        }
    }

    void clearBuffer() noexcept {
        events_.clear();
        event_levels_.clear();
    }

    void resetState() noexcept {
        book_.clear();
        local_id_ = 0;
        synced_ = false;
        awaiting_snapshot_ = false;
        clearBuffer();
    }

    void malformed(AdapterResult& out, const char* what, std::size_t bytes) {
        ++stats_.parse_errors;
        out.error = adapter_detail::concat({"Binance: ", what, " (", std::to_string(bytes), " bytes)"});
    }

    // 内部异常（如内存不足）：状态可能不一致，清空一切并要求重连
    void internalError(AdapterResult& out, const char* what) noexcept {
        ++stats_.parse_errors;
        if (!book_.empty()) out.book_updated = true;
        resetState();
        out.reconnect = true;
        try {
            out.error = std::string("Binance: internal error: ") + what;
        } catch (...) {
        }
    }

    adapter_detail::JsonReader reader_;
    std::vector<RawLevel> bids_;              // 当前消息的买档暂存
    std::vector<RawLevel> asks_;              // 当前消息的卖档暂存
    std::vector<BufferedEvent> events_;       // 同步前缓冲的事件
    std::vector<PriceLevel> event_levels_;    // 缓冲事件的档位
    std::uint64_t local_id_ = 0;              // 本地簿的更新号
    bool synced_ = false;
    bool awaiting_snapshot_ = false;          // 本轮尝试已请求 REST 快照、尚未成功应用
};
