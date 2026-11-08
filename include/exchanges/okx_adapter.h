//
// okx_adapter.h
// OKX 深度适配器：books5/bbo-tbt 全量推送直接替换；books（400 档增量）按 seqId/prevSeqId 衔接并校验 CRC32 checksum。
//

#pragma once

#include "exchanges/exchange_adapter.h"
#include "exchanges/json_util.h"
#include "hft/crc32.h"
#include "hft/flat_hash_map.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// 消息类型：
// - 纯文本 "pong" → 忽略；{"event":"subscribe"/"unsubscribe"/...} → 忽略；
//   {"event":"error","code":..,"msg":..} → error（订阅问题，不重连）；{"event":"notice",..}（服务升级通知）→ reconnect。
// - 无 action 字段的推送（books5 / bbo-tbt）：{"arg":{"channel":"books5",..},"data":[{"asks":..,"bids":..,"ts":"ms"}]}
//   → 替换整簿，已同步，exchange_ts_ms = data[0].ts。这两个频道不带 checksum，因此不维护原始字符串表
//   （每次推送都整簿替换，维护它只是白白清空、重填两张 2048 桶的表）；万一某条推送带了 checksum，照常校验。
// - books 增量："action":"snapshot" → 替换整簿，seq = seqId，已同步；"action":"update" → 未同步时忽略；
//   prevSeqId != 上一条 seqId → 缺口（gaps++、resyncs++、清空、未同步、send = {unsubscribe, subscribe}）；
//   否则逐档 upsert（size "0" 删除），seq = seqId。心跳更新（bids/asks 为空且 prevSeqId == seqId）通过衔接检查但不改变簿；
//   序列重置（seqId < prevSeqId）只要 prevSeqId 衔接即接受。
// - checksum（verify_checksum 开启且消息带该字段）：取前 25 档，按 bid0px:bid0sz:ask0px:ask0sz:bid1px:... 交错拼接，
//   一侧不足 25 档时继续拼另一侧剩余档位，':' 连接，CRC32 按有符号 int32 与 checksum 比较。
//   拼接使用每档收到的**原始字符串**（每侧一个 hft::FlatHashMap<价格位模式, LevelText>，每次档位变化同步更新；
//   只在增量频道或条目自带 checksum 时维护）。
//   更新后不符 → checksum_failures++，按缺口方式重同步。
//   安全阀：刚收到的**快照**本身校验不符，说明是我们的拼接方式（而不是数据）有误 → 设置 error
//   （"checksum verification disabled: ..."），在适配器余下的生命周期内关闭校验，继续运行。
//   档位字符串超过 LevelText::kMaxLen 字节（无法原样保存）时同样关闭校验。
// - 退订帧：订阅 JSON 中 "op":"subscribe" 改为 "op":"unsubscribe"；推导不出时（或没有订阅消息）重同步改为 reconnect。
// - 应用每条更新（含快照）后若簿交叉 → crossed_books++，按缺口方式重同步（全量推送频道只清空等待下一次推送）。
// - 重同步之后若持续收到增量却迟迟没有新快照（超过 kMaxUnsyncedUpdates 条），要求重连。
class OkxAdapter final : public ExchangeAdapter {
public:
    static constexpr std::size_t kChecksumLevels = 25;

    explicit OkxAdapter(const AdapterConfig& cfg)
        : ExchangeAdapter(cfg), verify_(cfg.verify_checksum), bid_text_(1024), ask_text_(1024) {
        book_.reserve(1024);
        bids_.reserve(512);
        asks_.reserve(512);
        entries_.reserve(4);
    }

    Exchange exchange() const override { return Exchange::OKX; }

    std::string pingMessage() const override { return "ping"; }
    int pingIntervalMs() const override { return 20000; }

    // ---------------- 诊断（测试用） ----------------
    bool checksumEnabled() const noexcept { return verify_; }
    std::int64_t lastSeqId() const noexcept { return seq_; }
    // 当前簿的校验串（与计算 checksum 用的完全一致）。不维护原始字符串时返回空串：
    // 校验关闭之后，以及全量推送频道（books5 / bbo-tbt，推送不带 checksum）
    std::string checksumString() const {
        char buf[kChecksumBufSize];
        std::size_t len = 0;
        if (!verify_ || !buildChecksumString(buf, len)) return std::string();
        return std::string(buf, len);
    }
    // 原始字符串表的条目数（维护时与簿的档位数一致；不维护时为 0）
    std::size_t textEntries(BookSide side) const noexcept {
        return side == BookSide::Bid ? bid_text_.size() : ask_text_.size();
    }

private:
    using RawLevel = adapter_detail::RawLevel;

    // 一档的校验串片段 "价格:数量"（原始字符串原样拼好，定长数组存放在哈希表内，无额外分配）。
    // 预先拼好并按定长整体复制（memcpy 常量长度会被内联成几条向量指令），比每档两次变长 memcpy 快得多。
    struct LevelText {
        static constexpr std::size_t kMaxLen = 32;                  // 价格或数量字符串的最大长度
        static constexpr std::size_t kCapacity = 2 * kMaxLen + 1;   // "px:sz"
        std::uint8_t len = 0;
        char text[kCapacity];
    };
    using TextMap = hft::FlatHashMap<std::uint64_t, LevelText>;

    // 校验串缓冲区：25 档 x 2 侧 x ("px:sz" + ':')，末尾留出整块复制的余量
    static constexpr std::size_t kChecksumBufSize = kChecksumLevels * 2 * (LevelText::kCapacity + 1) + LevelText::kCapacity;

    // 顶层字段
    enum : unsigned {
        kEvent = 1u << 0,
        kArg = 1u << 1,
        kAction = 1u << 2,
        kData = 1u << 3,
        kCode = 1u << 4,
        kMsgText = 1u << 5,
        kChannel = 1u << 6,
    };
    // data[i] 的字段
    enum : unsigned {
        kBids = 1u << 0,
        kAsks = 1u << 1,
        kTs = 1u << 2,
        kChecksum = 1u << 3,
        kPrevSeq = 1u << 4,
        kSeq = 1u << 5,
    };

    struct Msg {
        unsigned seen = 0;
        std::string_view event, channel, action, code, text;
    };

    // data 数组中的一项；档位存放在 bids_/asks_ 的 [begin, end) 区间
    struct Entry {
        unsigned seen = 0;
        std::size_t bid_begin = 0, bid_end = 0, ask_begin = 0, ask_end = 0;
        std::int64_t ts = 0, checksum = 0, prev_seq = 0, seq = 0;
    };

    // 价格的 64 位键：double 的二进制位（价格均为正的有限值，相同字符串解析出相同的位模式）
    static std::uint64_t priceKey(double price) noexcept {
        std::uint64_t k = 0;
        std::memcpy(&k, &price, sizeof(k));
        return k;
    }

    // ---------------- 解析 ----------------

    // 单遍解析整条消息（data 的条目写入 entries_，档位写入 bids_ / asks_）；
    // JSON 错误、根不是对象、尾随内容、重复的关键字段都返回 false
    bool parse(std::string_view payload, Msg& m) {
        using namespace adapter_detail;
        entries_.clear();
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
            bool ok = true;  // connId 等无关字段不读取
            if (key == "event") ok = firstSeen(m.seen, kEvent) && readString(v, m.event);
            else if (key == "arg") ok = firstSeen(m.seen, kArg) && parseArg(v, m);
            else if (key == "action") ok = firstSeen(m.seen, kAction) && readString(v, m.action);
            else if (key == "data") ok = firstSeen(m.seen, kData) && parseData(v, m);
            else if (key == "code") ok = firstSeen(m.seen, kCode) && readScalarText(v, m.code);
            else if (key == "msg") ok = firstSeen(m.seen, kMsgText) && readString(v, m.text);
            if (!ok) return false;
        }
        return doc.at_end();
    }

    bool parseArg(adapter_detail::od::value& v, Msg& m) {
        using namespace adapter_detail;
        od::object obj;
        if (v.get_object().get(obj) != simdjson::SUCCESS) return false;
        for (auto field_result : obj) {
            od::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
            if (field.escaped_key() != "channel") continue;
            if (!firstSeen(m.seen, kChannel) || !readString(field.value(), m.channel)) return false;
        }
        return true;
    }

    // data 数组。已知是其他频道（OKX 总是先发 arg）：data 的形状可能完全不同（如 K 线是数组的数组），
    // 不解析，迭代器自动跳过
    bool parseData(adapter_detail::od::value& v, const Msg& m) {
        using namespace adapter_detail;
        if ((m.seen & kChannel) != 0 && !isBookChannel(m.channel)) return true;
        od::array arr;
        if (v.get_array().get(arr) != simdjson::SUCCESS) return false;
        for (auto element : arr) {
            od::object obj;
            if (element.get_object().get(obj) != simdjson::SUCCESS) return false;
            Entry e;
            e.bid_begin = bids_.size();
            e.ask_begin = asks_.size();
            for (auto field_result : obj) {
                od::field field;
                if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
                const std::string_view key = field.escaped_key();
                od::value& fv = field.value();
                bool ok = true;  // instId 等
                if (key == "bids") ok = firstSeen(e.seen, kBids) && parseLevels(fv, bids_);
                else if (key == "asks") ok = firstSeen(e.seen, kAsks) && parseLevels(fv, asks_);
                else if (key == "ts") ok = firstSeen(e.seen, kTs) && readInt(fv, e.ts);
                else if (key == "checksum") ok = firstSeen(e.seen, kChecksum) && readInt(fv, e.checksum);
                else if (key == "prevSeqId") ok = firstSeen(e.seen, kPrevSeq) && readInt(fv, e.prev_seq);
                else if (key == "seqId") ok = firstSeen(e.seen, kSeq) && readInt(fv, e.seq);
                if (!ok) return false;
            }
            e.bid_end = bids_.size();
            e.ask_end = asks_.size();
            entries_.push_back(e);
        }
        return true;
    }

    static bool isBookChannel(std::string_view channel) noexcept {
        return channel.substr(0, 5) == "books" || channel.substr(0, 3) == "bbo";
    }

    // ---------------- 处理 ----------------

    void handleMessage(std::string_view payload, AdapterResult& out) override {
        if (payload == "pong") {
            ++stats_.ignored;
            return;
        }
        Msg m;
        if (!parse(payload, m)) return malformed(out, "malformed message", payload.size());
        if ((m.seen & kEvent) != 0) return handleEvent(m, out);
        if ((m.seen & kData) == 0) {
            ++stats_.ignored;  // 既不是事件也不是推送的合法 JSON
            return;
        }
        if ((m.seen & kChannel) == 0) return malformed(out, "data push without arg.channel", payload.size());
        if (!isBookChannel(m.channel)) {
            ++stats_.ignored;  // 同一连接上订阅的其他频道
            return;
        }
        unsigned common = ~0u;  // 每个条目都带的字段
        for (const Entry& e : entries_) common &= e.seen;
        if ((common & (kBids | kAsks)) != (kBids | kAsks)) {
            return malformed(out, "book entry without bids/asks", payload.size());
        }
        if (entries_.empty()) {
            ++stats_.ignored;
            return;
        }
        const bool has_action = (m.seen & kAction) != 0;
        const bool is_update = has_action && m.action == "update";
        if (has_action && !is_update && m.action != "snapshot") {
            ++stats_.ignored;
            return;
        }
        const unsigned required = is_update ? (kSeq | kPrevSeq) : has_action ? kSeq : 0u;
        if ((common & required) != required) return malformed(out, "books entry without seqId/prevSeqId", payload.size());
        if ((entries_.front().seen & kTs) != 0) out.exchange_ts_ms = entries_.front().ts;
        incremental_ = has_action;
        for (const Entry& e : entries_) {
            const bool keep_going = is_update ? applyUpdate(e, out) : applySnapshot(e, out);
            if (!keep_going) break;
        }
    }

    void handleEvent(const Msg& m, AdapterResult& out) {
        ++stats_.ignored;
        if (m.event == "error") {
            out.error = adapter_detail::concat({"OKX error ", m.code, ": ", m.text});
        } else if (m.event == "notice") {
            out.reconnect = true;
            out.error = adapter_detail::concat({"OKX notice ", m.code, ": ", m.text, " (reconnecting)"});
        }
        // subscribe / unsubscribe / channel-conn-count / login 等：忽略
    }

    // 全量快照（books 的 snapshot，或 books5/bbo-tbt 的每次推送）。返回是否继续处理同一消息的后续条目。
    bool applySnapshot(const Entry& e, AdapterResult& out) {
        // 原始字符串只在用得上时维护：增量频道（之后的 update 要用它们校验），或本条目自带 checksum
        const bool keep_texts = incremental_ || (e.seen & kChecksum) != 0;
        if (keep_texts) checkTextsFit(e, out);
        // 表为空时 clear() 是 O(1)；不维护字符串时也要清，丢掉之前 books 快照留下的条目
        bid_text_.clear();
        ask_text_.clear();
        if (keep_texts) setTexts(e);
        replaceBook(bids_.data() + e.bid_begin, e.bid_end - e.bid_begin, asks_.data() + e.ask_begin,
                    e.ask_end - e.ask_begin);
        seq_ = (e.seen & kSeq) != 0 ? e.seq : 0;
        book_.setUpdateId(seq_ > 0 ? static_cast<std::uint64_t>(seq_) : 0);
        synced_ = true;
        ++stats_.snapshots;
        out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            resync(out, "crossed book snapshot");
            return false;
        }
        std::int32_t computed = 0;
        if (!checksumOk(e, computed)) {
            disableChecksum(out, adapter_detail::concat({"snapshot checksum mismatch (expected ",
                                                         std::to_string(e.checksum), ", computed ",
                                                         std::to_string(computed), ")"}));
        }
        return true;
    }

    // 增量更新。返回是否继续处理同一消息的后续条目。
    bool applyUpdate(const Entry& e, AdapterResult& out) {
        if (!synced_) {
            ignoreUnsynced(out);
            return true;
        }
        if (e.prev_seq != seq_) {
            ++stats_.gaps;
            out.error = adapter_detail::concat({"OKX: sequence gap (prevSeqId=", std::to_string(e.prev_seq),
                                                ", last seqId=", std::to_string(seq_), ")"});
            resync(out, nullptr);
            return false;
        }
        checkTextsFit(e, out);
        // 原始字符串表按消息顺序逐档同步（同价以后者为准）；簿批量排序 + 归并
        setTexts(e);
        const bool changed = updateBook(bids_.data() + e.bid_begin, e.bid_end - e.bid_begin,
                                        asks_.data() + e.ask_begin, e.ask_end - e.ask_begin);
        seq_ = e.seq;
        book_.setUpdateId(seq_ > 0 ? static_cast<std::uint64_t>(seq_) : 0);
        if (e.bid_begin == e.bid_end && e.ask_begin == e.ask_end) {
            ++stats_.ignored;  // 心跳：衔接检查通过，簿不变
        } else {
            ++stats_.deltas;
        }
        if (changed) out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            resync(out, "crossed book after update");
            return false;
        }
        std::int32_t computed = 0;
        if (!checksumOk(e, computed)) {
            out.error = adapter_detail::concat({"OKX: checksum mismatch after update (expected ",
                                                std::to_string(e.checksum), ", computed ",
                                                std::to_string(computed), ")"});
            resync(out, nullptr);
            return false;
        }
        return true;
    }

    // 校验开启且条目带 checksum 时核对当前簿（判断内联，计算在 checksumMatches 里）
    bool checksumOk(const Entry& e, std::int32_t& computed) {
        return !verify_ || (e.seen & kChecksum) == 0 || checksumMatches(e, computed);
    }

    // 核对 e.checksum 与当前簿；不符（或某档缺少原始字符串）时 checksum_failures++ 并返回 false，
    // computed 为算出的值（拼不出校验串时为 0）
    bool checksumMatches(const Entry& e, std::int32_t& computed) {
        char buf[kChecksumBufSize];
        std::size_t len = 0;
        if (buildChecksumString(buf, len)) {
            computed = hft::toSignedCrc(hft::crc32(buf, len));
            // 文档规定 checksum 是有符号 32 位整数；为稳妥起见也接受同一 CRC 的无符号写法（比较低 32 位）
            if (e.checksum >= std::numeric_limits<std::int32_t>::min() &&
                e.checksum <= static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max()) &&
                static_cast<std::uint32_t>(computed) == static_cast<std::uint32_t>(e.checksum)) {
                return true;
            }
        }
        ++stats_.checksum_failures;
        return false;
    }

    // 校验开启时同步条目中每档的原始字符串（数量 0 删除），按消息顺序（同价以后者为准）
    void setTexts(const Entry& e) {
        if (!verify_) return;
        for (std::size_t i = e.bid_begin; i < e.bid_end; ++i) setText(bid_text_, bids_[i]);
        for (std::size_t i = e.ask_begin; i < e.ask_end; ++i) setText(ask_text_, asks_[i]);
    }

    static void setText(TextMap& texts, const RawLevel& l) {
        const std::uint64_t key = priceKey(l.price);
        if (!(l.quantity > 0.0)) {
            texts.erase(key);
            return;
        }
        LevelText& t = texts[key];  // 引用只在本函数内使用（之后才有下一次修改）
        std::memcpy(t.text, l.px.data(), l.px.size());
        t.text[l.px.size()] = ':';
        std::memcpy(t.text + l.px.size() + 1, l.sz.data(), l.sz.size());
        t.len = static_cast<std::uint8_t>(l.px.size() + 1 + l.sz.size());
    }

    // 安全阀：条目中有字符串长到无法原样存进 LevelText 时关闭校验
    void checkTextsFit(const Entry& e, AdapterResult& out) {
        const auto fits = [](const RawLevel& l) {
            return l.px.size() <= LevelText::kMaxLen && l.sz.size() <= LevelText::kMaxLen;
        };
        const RawLevel* b = bids_.data();
        const RawLevel* a = asks_.data();
        if (verify_ && !(std::all_of(b + e.bid_begin, b + e.bid_end, fits) &&
                         std::all_of(a + e.ask_begin, a + e.ask_end, fits))) {
            disableChecksum(out, "a level string is longer than 32 bytes");
        }
    }

    // 拼接校验串；某档缺少原始字符串（内部不一致）时返回 false
    bool buildChecksumString(char* buf, std::size_t& len) const noexcept {
        const std::vector<PriceLevel>& bids = book_.getBids();
        const std::vector<PriceLevel>& asks = book_.getAsks();
        const std::size_t nb = std::min(kChecksumLevels, bids.size());
        const std::size_t na = std::min(kChecksumLevels, asks.size());
        std::size_t pos = 0;
        auto append = [&](const TextMap& texts, double price) {
            const LevelText* t = texts.find(priceKey(price));
            if (t == nullptr) return false;
            if (pos != 0) buf[pos++] = ':';
            std::memcpy(buf + pos, t->text, LevelText::kCapacity);  // 定长整块复制，只推进实际长度
            pos += t->len;
            return true;
        };
        const std::size_t n = std::max(nb, na);
        for (std::size_t i = 0; i < n; ++i) {
            if (i < nb && !append(bid_text_, bids[i].price)) return false;
            if (i < na && !append(ask_text_, asks[i].price)) return false;
        }
        len = pos;
        return true;
    }

    // 安全阀：关闭校验并释放原始字符串表
    void disableChecksum(AdapterResult& out, const std::string& why) {
        verify_ = false;
        bid_text_.clear();
        ask_text_.clear();
        out.error = "OKX: checksum verification disabled: " + why;
    }

    // 裁剪簿时同步删除被裁掉档位的原始字符串（簿在上限之内时两个循环都不执行）
    void trimBook() override {
        if (verify_) {
            const std::size_t limit = adapter_detail::kMaxBookLevels;
            const std::vector<PriceLevel>& bids = book_.getBids();
            const std::vector<PriceLevel>& asks = book_.getAsks();
            for (std::size_t i = limit; i < bids.size(); ++i) bid_text_.erase(priceKey(bids[i].price));
            for (std::size_t i = limit; i < asks.size(); ++i) ask_text_.erase(priceKey(asks[i].price));
        }
        ExchangeAdapter::trimBook();
    }

    // 重同步：清空簿；增量频道发送 unsubscribe + subscribe（推导不出退订帧时要求重连），全量频道等待下一次推送
    void resync(AdapterResult& out, const char* why) {
        ++stats_.resyncs;
        if (why != nullptr) out.error = std::string("OKX: ") + why + ", resyncing";
        dropBook(out);
        if (incremental_) requestResubscribe(out);
    }

    void resetState() noexcept override {
        bid_text_.clear();
        ask_text_.clear();
        seq_ = 0;
    }

    bool verify_;                       // checksum 校验是否开启（安全阀触发后永久关闭）
    TextMap bid_text_;                  // 买盘每档原始字符串
    TextMap ask_text_;                  // 卖盘每档原始字符串
    std::vector<Entry> entries_;        // 当前消息的 data 条目
    std::int64_t seq_ = 0;              // 最近一条已应用消息的 seqId
    bool incremental_ = false;          // 当前消息是否来自带 action 的增量频道（每条订单簿消息都先设置；决定重同步方式）
};
