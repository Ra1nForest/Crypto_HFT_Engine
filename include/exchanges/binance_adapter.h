//
// binance_adapter.h
// Binance 现货深度适配器：部分深度快照（depth20）直接替换；增量深度（depth）按官方"本地订单簿维护"流程与 REST 快照同步。
// Binance spot depth adapter: partial depth snapshots (depth20) replace the book directly; incremental depth
// (depth) is synced with a REST snapshot following the official "local order book maintenance" procedure.
//

#pragma once

#include "exchanges/exchange_adapter.h"
#include "exchanges/json_util.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 每条消息自动识别类型：
// - combined stream 包装 {"stream":"...","data":{...}} → 处理 data。
// - 部分深度 {"lastUpdateId":N,"bids":[...],"asks":[...]} → 替换整簿，update id = N，已同步（无时间戳）。
// - 增量深度 {"e":"depthUpdate","E":ms,"U":first,"u":last,"b":[...],"a":[...]}，官方流程
//   （https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
//   "How to manage a local order book correctly"）：
//   1. 未同步时缓冲事件（上限 max_buffered_events，溢出 → 丢弃缓冲、计一次 gap、重新请求快照），
//      每轮尝试只置一次 need_rest_snapshot；feed 拉取 REST 快照后调用 onRestSnapshot。
//   2. 快照 lastUpdateId L < 第一条缓冲事件的 U → 快照太旧，再次请求（缓冲保留）。
//   3. 否则用快照替换簿，丢弃 u <= L 的缓冲事件，其余按更新规则回放。
//   更新规则：u <= 本地 id → 忽略（过期）；U > 本地 id + 1 → 缺口：gaps++、resyncs++、清空簿、未同步、
//   缓冲本事件并请求快照；否则逐档 upsert（数量 0 删除），本地 id = u。
//   （因此快照后第一条被应用的事件满足 U <= L+1 <= u。）本地 id 即 book().getUpdateId()。
// - {"e":"serverShutdown"} → reconnect；{"result":null,"id":N} → 忽略；{"code":..,"msg":..} → error。
// - 每条更新（含快照）应用后若簿交叉 → crossed_books++，按缺口方式重同步（本事件同样进入缓冲）。
// 解析采用"先完整解析到暂存区、再应用"：格式错误的消息整条丢弃，不会把簿改到一半；
// 被丢弃的增量会在下一条事件上表现为缺口，从而自动重同步。
// The type of every message is detected automatically:
// - Combined stream wrapper {"stream":"...","data":{...}} → process data.
// - Partial depth {"lastUpdateId":N,"bids":[...],"asks":[...]} → replace the whole book, update id = N,
//   synced (no timestamp).
// - Incremental depth {"e":"depthUpdate","E":ms,"U":first,"u":last,"b":[...],"a":[...]}, official procedure
//   (https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
//   "How to manage a local order book correctly"):
//   1. While not synced, buffer events (limit max_buffered_events, overflow → drop the buffer, count one
//      gap, request a snapshot again), setting need_rest_snapshot only once per attempt; the feed fetches
//      the REST snapshot and then calls onRestSnapshot.
//   2. Snapshot lastUpdateId L < U of the first buffered event → the snapshot is too old, request it again
//      (the buffer is kept).
//   3. Otherwise replace the book with the snapshot, drop the buffered events with u <= L, and replay the
//      rest with the update rule.
//   Update rule: u <= local id → ignore (stale); U > local id + 1 → gap: gaps++, resyncs++, clear the
//   book, not synced, buffer this event and request a snapshot; otherwise upsert level by level
//   (quantity 0 deletes), local id = u.
//   (So the first event applied after a snapshot satisfies U <= L+1 <= u.) The local id is
//   book().getUpdateId().
// - {"e":"serverShutdown"} → reconnect; {"result":null,"id":N} → ignore; {"code":..,"msg":..} → error.
// - If the book is crossed after applying any update (snapshots included) → crossed_books++, resync as for
//   a gap (this event is buffered too).
// Parsing follows "first parse fully into a staging area, then apply": a malformed message is discarded
// as a whole, so the book is never left half-modified;
// a discarded delta shows up as a gap on the next event and so triggers a resync automatically.
class BinanceAdapter final : public ExchangeAdapter {
public:
    // 同步前缓冲的档位总数上限（防止异常大的事件耗尽内存；正常远远达不到）
    // Upper bound on the total number of levels buffered before sync (keeps abnormally large events from
    // exhausting memory; normal operation stays far below it)
    static constexpr std::size_t kMaxBufferedLevels = std::size_t{1} << 21;

    explicit BinanceAdapter(const AdapterConfig& cfg) : ExchangeAdapter(cfg) {
        book_.reserve(2048);
        bids_.reserve(1024);
        asks_.reserve(1024);
    }

    Exchange exchange() const override { return Exchange::BINANCE; }

    // ---------------- 诊断（测试用） ----------------
    // ---------------- Diagnostics (for tests) ----------------
    std::size_t bufferedEvents() const noexcept { return events_.size(); }
    bool awaitingSnapshot() const noexcept { return awaiting_snapshot_; }
    std::uint64_t lastUpdateId() const noexcept { return book_.getUpdateId(); }
    // 第一条缓冲事件的 U（无缓冲时为 0）
    // U of the first buffered event (0 when nothing is buffered)
    std::uint64_t firstBufferedId() const noexcept { return events_.empty() ? 0 : events_.front().first_id; }

private:
    // 消息中出现过的字段（位掩码）
    // Fields that appeared in the message (bit mask)
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
                                   // "data" (combined stream wrapper)
        kDiffKeys = kFirstId | kFinalId | kDiffBids | kDiffAsks,
        kSnapKeys = kLastUpdateId | kSnapBids | kSnapAsks,
    };

    // 单遍扫描得到的字段（档位写入 bids_ / asks_）
    // Fields collected in a single pass (levels go into bids_ / asks_)
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

    // 同步前缓冲的增量事件：档位按事件顺序连续存放在 event_levels_[begin, begin + bid_count + ask_count)，先买后卖
    // An incremental event buffered before sync: its levels are stored contiguously, in event order, in
    // event_levels_[begin, begin + bid_count + ask_count), bids first, then asks
    struct BufferedEvent {
        std::uint64_t first_id;
        std::uint64_t final_id;
        std::size_t begin;
        std::size_t bid_count;
        std::size_t ask_count;
    };

    // ---------------- 解析 ----------------
    // ---------------- Parsing ----------------

    // 载入并单遍解析整条消息；JSON 错误、根不是对象、尾随内容、重复的关键字段都返回 false
    // Loads and parses the whole message in a single pass; a JSON error, a root that is not an object,
    // trailing content or a duplicated key field all return false
    bool parse(std::string_view payload, Msg& m) {
        bids_.clear();
        asks_.clear();
        adapter_detail::od::document doc;
        adapter_detail::od::object obj;
        return reader_.load(payload, doc) == simdjson::SUCCESS && doc.get_object().get(obj) == simdjson::SUCCESS &&
               parseObject(obj, m, true) && doc.at_end();
    }

    // 读取一个对象的全部字段；top 表示根对象（只有根对象里的 "data" 是 combined stream 包装）
    // Reads all fields of one object; top marks the root object (only "data" in the root object is the
    // combined stream wrapper)
    bool parseObject(adapter_detail::od::object& obj, Msg& m, bool top) {
        using namespace adapter_detail;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            const std::string_view key = field.escaped_key();
            od::value& v = field.value();
            bool ok = true;  // 无关字段（"s"、"stream"、"id" 等）不读取
                             // unrelated fields ("s", "stream", "id", etc.) are not read
            if (key == "e") ok = firstSeen(m.seen, kEventType) && readString(v, m.event);
            else if (key == "E") ok = firstSeen(m.seen, kEventTime) && readInt(v, m.event_time);
            else if (key == "U") ok = firstSeen(m.seen, kFirstId) && readInt(v, m.first_id);
            else if (key == "u") ok = firstSeen(m.seen, kFinalId) && readInt(v, m.final_id);
            // 买/卖档位数组只能出现一次，无论 "b" 还是 "bids"
            // Each bid/ask level array may appear only once, whether as "b" or as "bids"
            else if (key == "b") ok = firstSeen(m.seen, kDiffBids, kSnapBids) && parseLevels(v, bids_);
            else if (key == "a") ok = firstSeen(m.seen, kDiffAsks, kSnapAsks) && parseLevels(v, asks_);
            else if (key == "bids") ok = firstSeen(m.seen, kSnapBids, kDiffBids) && parseLevels(v, bids_);
            else if (key == "asks") ok = firstSeen(m.seen, kSnapAsks, kDiffAsks) && parseLevels(v, asks_);
            else if (key == "lastUpdateId") ok = firstSeen(m.seen, kLastUpdateId) && readInt(v, m.last_update_id);
            else if (key == "result") ok = firstSeen(m.seen, kResult);  // 值不关心（null / true / 列表）
                                                                        // value irrelevant (null / true / list)
            else if (key == "code") ok = firstSeen(m.seen, kCode) && readScalarText(v, m.code);
            else if (key == "msg") ok = firstSeen(m.seen, kMsgText) && readString(v, m.text);
            else if (key == "data" && top) ok = firstSeen(m.seen, kData) && parseWrapped(v, m);
            if (!ok) return false;
        }
        return true;
    }

    // combined stream 包装里的 data 对象
    // The data object inside the combined stream wrapper
    bool parseWrapped(adapter_detail::od::value& v, Msg& m) {
        adapter_detail::od::object inner;
        return v.get_object().get(inner) == simdjson::SUCCESS && parseObject(inner, m, false);
    }

    // ---------------- WebSocket 消息 ----------------
    // ---------------- WebSocket messages ----------------

    void handleMessage(std::string_view payload, AdapterResult& out) override {
        Msg m;
        if (!parse(payload, m)) return malformed(out, "malformed message", payload.size());
        if ((m.seen & kEventType) != 0) {
            if (m.event != "depthUpdate") {
                ++stats_.ignored;  // serverShutdown 或其他事件类型（同一连接上订阅了别的流）
                                   // serverShutdown or another event type
                                   // (other streams subscribed on the same connection)
                if (m.event == "serverShutdown") {
                    out.reconnect = true;
                    out.error = "Binance: serverShutdown event, reconnecting";
                }
                return;
            }
            if ((m.seen & kDiffKeys) != kDiffKeys || (m.seen & kSnapKeys) != 0 || m.first_id > m.final_id) {
                return malformed(out, "malformed depthUpdate", payload.size());
            }
            if ((m.seen & kEventTime) != 0) out.exchange_ts_ms = m.event_time;
            return handleDiff(m, out);
        }
        if ((m.seen & kSnapKeys) == kSnapKeys && (m.seen & kDiffKeys) == 0) return handlePartialDepth(m, out);
        if ((m.seen & (kSnapKeys | kDiffKeys)) != 0) return malformed(out, "incomplete depth message", payload.size());
        ++stats_.ignored;
        if ((m.seen & kCode) != 0) out.error = adapter_detail::concat({"Binance error ", m.code, ": ", m.text});
    }

    // 部分深度快照：直接替换（比当前簿旧的快照忽略）
    // Partial depth snapshot: replaces the book directly (snapshots older than the current book are ignored)
    void handlePartialDepth(const Msg& m, AdapterResult& out) {
        if (synced_ && m.last_update_id < book_.getUpdateId()) {
            ++stats_.ignored;
            return;
        }
        clearBuffer();
        if (applySnapshot(m.last_update_id, out)) {
            dropBook(out);
            out.error = "Binance: crossed depth snapshot discarded";
        }
    }

    // 增量事件：未同步时缓冲；已同步时按更新规则应用，缺口或交叉则丢弃簿、缓冲本事件并请求快照
    // Incremental event: buffered while not synced; once synced, applied with the update rule, and on a gap
    // or a crossed book the book is dropped, this event is buffered and a snapshot is requested
    void handleDiff(const Msg& m, AdapterResult& out) {
        if (synced_) {
            if (applyEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(), asks_.size(), false, out)) {
                return;
            }
            dropBook(out);
        }
        bufferEvent(m.first_id, m.final_id, bids_.data(), bids_.size(), asks_.data(), asks_.size(), out);
    }

    // ---------------- REST 快照 ----------------
    // ---------------- REST snapshot ----------------

    // REST 深度快照响应体（feed 在 need_rest_snapshot 后经 onRestSnapshot 调用）。拉取失败时 feed 可传入空串或错误体：
    // 解析失败 / 错误体 {"code":..,"msg":..} → error + need_rest_snapshot（feed 退避后重试）。
    // 未在等待快照时收到的响应体被忽略。
    // REST depth snapshot response body (the feed calls this via onRestSnapshot after need_rest_snapshot).
    // When the fetch fails, the feed may pass an empty string or an error body:
    // parse failure / error body {"code":..,"msg":..} → error + need_rest_snapshot (the feed retries after a
    // backoff).
    // A body that arrives while no snapshot is awaited is ignored.
    void handleRestSnapshot(std::string_view body, AdapterResult& out) override {
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
            // The snapshot is older than the first buffered event: keep the buffer and fetch again
            out.error = adapter_detail::concat({"Binance: REST snapshot too old (lastUpdateId=",
                                                std::to_string(last_id), " < first buffered U=",
                                                std::to_string(events_.front().first_id), "), fetching again"});
            out.need_rest_snapshot = true;
            return;
        }
        if (applySnapshot(last_id, out)) {
            out.error = "Binance: crossed REST snapshot, fetching again";
            return refetchSnapshot(out);
        }
        replayBuffer(out);
    }

    // 快照之后回放缓冲：丢弃 u <= L 的事件，其余按更新规则应用；中途出现缺口/交叉则从该事件起保留缓冲并重新请求
    // Replays the buffer after the snapshot: drops events with u <= L and applies the rest with the update
    // rule; if a gap/crossed book occurs midway, keeps the buffer from that event on and requests again
    void replayBuffer(AdapterResult& out) {
        for (std::size_t i = 0; i < events_.size(); ++i) {
            const BufferedEvent& e = events_[i];
            const PriceLevel* bids = event_levels_.data() + e.begin;
            if (applyEvent(e.first_id, e.final_id, bids, e.bid_count, bids + e.bid_count, e.ask_count, true, out)) {
                continue;
            }
            dropBufferedPrefix(i);
            return refetchSnapshot(out);
        }
        clearBuffer();
    }

    // ---------------- 簿与缓冲操作 ----------------
    // ---------------- Book and buffer operations ----------------

    // 用暂存区的档位（快照）替换整簿并进入已同步状态；返回簿是否交叉（已计入 crossed_books 与 resyncs）
    // Replaces the whole book with the staged levels (snapshot) and enters the synced state; returns whether
    // the book is crossed (already counted in crossed_books and resyncs)
    bool applySnapshot(std::uint64_t update_id, AdapterResult& out) {
        replaceBook(bids_.data(), bids_.size(), asks_.data(), asks_.size());
        book_.setUpdateId(update_id);
        synced_ = true;
        awaiting_snapshot_ = false;
        ++stats_.snapshots;
        out.book_updated = true;
        if (!book_.isCrossed()) return false;
        ++stats_.crossed_books;
        ++stats_.resyncs;
        return true;
    }

    // 已同步时按更新规则应用一条事件并计数（replaying：回放缓冲中的事件）：过期 → ignored；
    // 应用 → deltas，簿有变化时置 book_updated。返回 false 表示缺口或应用后簿交叉
    // （已计入 gaps 或 crossed_books、resyncs 并设置 error），调用方随后重同步
    // While synced, applies one event with the update rule and counts it (replaying: replaying an event from
    // the buffer): stale → ignored; applied → deltas, and book_updated is set when the book changed. Returns
    // false on a gap or when the book is crossed after applying (already counted in gaps or crossed_books,
    // and resyncs, with error set); the caller then resyncs
    template <class Level>
    bool applyEvent(std::uint64_t first_id, std::uint64_t final_id, const Level* bids, std::size_t nb,
                    const Level* asks, std::size_t na, bool replaying, AdapterResult& out) {
        const std::uint64_t local = book_.getUpdateId();
        if (final_id <= local) {
            ++stats_.ignored;
            return true;
        }
        if (first_id > local + 1) {
            ++stats_.gaps;
            out.error = adapter_detail::concat({replaying ? "Binance: update id gap in buffered events (U="
                                                          : "Binance: update id gap (U=",
                                                std::to_string(first_id), ", local=", std::to_string(local),
                                                "), resyncing"});
        } else {
            if (updateBook(bids, nb, asks, na)) out.book_updated = true;
            book_.setUpdateId(final_id);
            ++stats_.deltas;
            if (!book_.isCrossed()) return true;
            ++stats_.crossed_books;
            out.error = replaying ? "Binance: crossed book while replaying buffered events, resyncing"
                                  : "Binance: crossed book after update, resyncing";
        }
        ++stats_.resyncs;
        return false;
    }

    // 丢弃本地簿但保留缓冲，在同一轮尝试中再次请求 REST 快照
    // Drops the local book but keeps the buffer, and requests the REST snapshot again in the same attempt
    void refetchSnapshot(AdapterResult& out) noexcept {
        book_.clear();
        synced_ = false;
        awaiting_snapshot_ = true;
        out.need_rest_snapshot = true;
    }

    // 缓冲一条事件；溢出时丢弃旧缓冲、计 gap 并开始新一轮尝试；每轮尝试只请求一次快照
    // Buffers one event; on overflow, drops the old buffer, counts a gap and starts a new attempt; each
    // attempt requests a snapshot only once
    void bufferEvent(std::uint64_t first_id, std::uint64_t final_id, const adapter_detail::RawLevel* bids,
                     std::size_t nb, const adapter_detail::RawLevel* asks, std::size_t na, AdapterResult& out) {
        const std::size_t max_events = std::max<std::size_t>(1, cfg_.max_buffered_events);
        if (events_.size() >= max_events || event_levels_.size() + nb + na > kMaxBufferedLevels) {
            ++stats_.gaps;
            clearBuffer();
            awaiting_snapshot_ = false;
            out.error = "Binance: pre-sync event buffer overflow, requesting a fresh snapshot";
        }
        events_.push_back({first_id, final_id, event_levels_.size(), nb, na});
        for (std::size_t i = 0; i < nb; ++i) event_levels_.push_back({bids[i].price, bids[i].quantity});
        for (std::size_t i = 0; i < na; ++i) event_levels_.push_back({asks[i].price, asks[i].quantity});
        if (!awaiting_snapshot_) {
            awaiting_snapshot_ = true;
            out.need_rest_snapshot = true;
        }
    }

    // 删除前 n 条缓冲事件及其档位（n < 缓冲事件数）
    // Removes the first n buffered events and their levels (n < number of buffered events)
    void dropBufferedPrefix(std::size_t n) {
        const std::size_t level_start = events_[n].begin;
        event_levels_.erase(event_levels_.begin(), event_levels_.begin() + static_cast<std::ptrdiff_t>(level_start));
        events_.erase(events_.begin(), events_.begin() + static_cast<std::ptrdiff_t>(n));
        for (BufferedEvent& e : events_) e.begin -= level_start;
    }

    void clearBuffer() noexcept {
        events_.clear();
        event_levels_.clear();
    }

    void resetState() noexcept override {
        awaiting_snapshot_ = false;
        clearBuffer();
    }

    std::vector<BufferedEvent> events_;       // 同步前缓冲的事件
                                              // events buffered before sync
    std::vector<PriceLevel> event_levels_;    // 缓冲事件的档位
                                              // levels of the buffered events
    bool awaiting_snapshot_ = false;          // 本轮尝试已请求 REST 快照、尚未成功应用
                                              // a REST snapshot was requested in this attempt
                                              // and has not been applied successfully yet
};
