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
//   → 替换整簿，已同步，exchange_ts_ms = data[0].ts。
// - books 增量："action":"snapshot" → 替换整簿，seq = seqId，已同步；"action":"update" → 未同步时忽略；
//   prevSeqId != 上一条 seqId → 缺口（gaps++、resyncs++、清空、未同步、send = {unsubscribe, subscribe}）；
//   否则逐档 upsert（size "0" 删除），seq = seqId。心跳更新（bids/asks 为空且 prevSeqId == seqId）通过衔接检查但不改变簿；
//   序列重置（seqId < prevSeqId）只要 prevSeqId 衔接即接受。
// - checksum（verify_checksum 开启且消息带该字段）：取前 25 档，按 bid0px:bid0sz:ask0px:ask0sz:bid1px:... 交错拼接，
//   一侧不足 25 档时继续拼另一侧剩余档位，':' 连接，CRC32 按有符号 int32 与 checksum 比较。
//   拼接使用每档收到的**原始字符串**（每侧一个 hft::FlatHashMap<价格位模式, LevelText>，每次档位变化同步更新）。
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
    static constexpr std::size_t kMaxUnsyncedUpdates = 500;

    explicit OkxAdapter(const AdapterConfig& cfg)
        : ExchangeAdapter(cfg),
          verify_(cfg.verify_checksum),
          unsubscribe_msg_(makeUnsubscribeMessage(cfg.subscribe_msg)),
          bid_text_(1024),
          ask_text_(1024) {
        book_.reserve(1024);
        bids_.reserve(512);
        asks_.reserve(512);
        entries_.reserve(4);
    }

    Exchange exchange() const override { return Exchange::OKX; }

    std::string pingMessage() const override { return "ping"; }
    int pingIntervalMs() const override { return 20000; }
    bool synced() const override { return synced_; }

    // ---------------- 诊断（测试用） ----------------
    bool checksumEnabled() const noexcept { return verify_; }
    std::int64_t lastSeqId() const noexcept { return seq_; }
    const std::string& unsubscribeMessage() const noexcept { return unsubscribe_msg_; }
    // 当前簿的校验串（与计算 checksum 用的完全一致；校验关闭后不再维护原始字符串，返回空串）
    std::string checksumString() const {
        char buf[kChecksumBufSize];
        std::size_t len = 0;
        if (!verify_ || !buildChecksumString(buf, len)) return std::string();
        return std::string(buf, len);
    }
    // 原始字符串表的条目数（应与簿的档位数一致）
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

    // ---------------- 解析 ----------------

    bool parse(std::string_view payload, Msg& m) {
        using namespace adapter_detail;
        bids_.clear();
        asks_.clear();
        entries_.clear();
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
            if (key == "event") bit = kEvent;
            else if (key == "arg") bit = kArg;
            else if (key == "action") bit = kAction;
            else if (key == "data") bit = kData;
            else if (key == "code") bit = kCode;
            else if (key == "msg") bit = kMsgText;
            else continue;  // connId 等
            if ((m.seen & bit) != 0) return false;
            m.seen |= bit;
            bool ok = false;
            switch (bit) {
                case kEvent: ok = readString(v, m.event); break;
                case kArg: ok = parseArg(v, m); break;
                case kAction: ok = readString(v, m.action); break;
                case kData:
                    // 已知是其他频道（OKX 总是先发 arg）：data 的形状可能完全不同（如 K 线是数组的数组），
                    // 不解析，迭代器自动跳过。注意不能"解析失败后继续"：simdjson On-Demand 在嵌套迭代中
                    // 遇到致命错误会废弃整个文档，外层循环随之静默结束，所以任何嵌套解析失败都必须立即中止。
                    ok = ((m.seen & kChannel) != 0 && !isBookChannel(m.channel)) || parseData(v);
                    break;
                case kCode: ok = readScalarText(v, m.code); break;
                case kMsgText: ok = readString(v, m.text); break;
                default: break;
            }
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
            if ((m.seen & kChannel) != 0) return false;
            m.seen |= kChannel;
            if (!readString(field.value(), m.channel)) return false;
        }
        return true;
    }

    bool parseData(adapter_detail::od::value& v) {
        using namespace adapter_detail;
        od::array arr;
        if (v.get_array().get(arr) != simdjson::SUCCESS) return false;
        for (auto element : arr) {
            od::value ev;
            if (element.get(ev) != simdjson::SUCCESS) return false;
            od::object obj;
            if (ev.get_object().get(obj) != simdjson::SUCCESS) return false;
            Entry e;
            e.bid_begin = bids_.size();
            e.ask_begin = asks_.size();
            for (auto field_result : obj) {
                od::field field;
                if (std::move(field_result).get(field) != simdjson::SUCCESS) return false;
                const std::string_view key = field.escaped_key();
                od::value& fv = field.value();
                unsigned bit = 0;
                if (key == "bids") bit = kBids;
                else if (key == "asks") bit = kAsks;
                else if (key == "ts") bit = kTs;
                else if (key == "checksum") bit = kChecksum;
                else if (key == "prevSeqId") bit = kPrevSeq;
                else if (key == "seqId") bit = kSeq;
                else continue;  // instId 等
                if ((e.seen & bit) != 0) return false;
                e.seen |= bit;
                bool ok = false;
                switch (bit) {
                    case kBids: ok = parseLevels(fv, bids_); break;
                    case kAsks: ok = parseLevels(fv, asks_); break;
                    case kTs: ok = readInt64(fv, e.ts); break;
                    case kChecksum: ok = readInt64(fv, e.checksum); break;
                    case kPrevSeq: ok = readInt64(fv, e.prev_seq); break;
                    case kSeq: ok = readInt64(fv, e.seq); break;
                    default: break;
                }
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
        if (!parse(payload, m)) {
            malformed(out, "malformed message", payload.size());
            return;
        }
        if ((m.seen & kEvent) != 0) {
            handleEvent(m, out);
            return;
        }
        if ((m.seen & kData) == 0) {
            ++stats_.ignored;  // 既不是事件也不是推送的合法 JSON
            return;
        }
        if ((m.seen & kChannel) == 0) {
            malformed(out, "data push without arg.channel", payload.size());
            return;
        }
        if (!isBookChannel(m.channel)) {
            ++stats_.ignored;  // 同一连接上订阅的其他频道
            return;
        }
        for (const Entry& e : entries_) {
            if ((e.seen & (kBids | kAsks)) != (kBids | kAsks)) {
                malformed(out, "book entry without bids/asks", payload.size());
                return;
            }
        }
        if (entries_.empty()) {
            ++stats_.ignored;
            return;
        }
        const bool has_action = (m.seen & kAction) != 0;
        const bool is_update = has_action && m.action == "update";
        const bool is_snapshot = has_action && m.action == "snapshot";
        if (has_action && !is_update && !is_snapshot) {
            ++stats_.ignored;
            return;
        }
        const unsigned required = is_update ? (kSeq | kPrevSeq) : is_snapshot ? kSeq : 0u;
        for (const Entry& e : entries_) {
            if ((e.seen & required) != required) {
                malformed(out, "books entry without seqId/prevSeqId", payload.size());
                return;
            }
        }
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
        if (verify_ && !textsFit(e)) disableChecksum(out, "a level string is longer than 32 bytes");
        book_.clear();
        bid_text_.clear();
        ask_text_.clear();
        for (std::size_t i = e.bid_begin; i < e.bid_end; ++i) setLevel(BookSide::Bid, bids_[i]);
        for (std::size_t i = e.ask_begin; i < e.ask_end; ++i) setLevel(BookSide::Ask, asks_[i]);
        trimBook();
        seq_ = (e.seen & kSeq) != 0 ? e.seq : 0;
        book_.setUpdateId(seq_ > 0 ? static_cast<std::uint64_t>(seq_) : 0);
        synced_ = true;
        unsynced_updates_ = 0;
        ++stats_.snapshots;
        out.book_updated = true;
        if (book_.isCrossed()) {
            ++stats_.crossed_books;
            resync(out, "crossed book snapshot");
            return false;
        }
        if (verify_ && (e.seen & kChecksum) != 0) {
            std::int32_t computed = 0;
            const bool built = computeChecksum(computed);
            if (!built || !checksumMatches(computed, e.checksum)) {
                ++stats_.checksum_failures;
                disableChecksum(out, adapter_detail::concat({"snapshot checksum mismatch (expected ",
                                                             std::to_string(e.checksum), ", computed ",
                                                             std::to_string(computed), ")"}));
            }
        }
        return true;
    }

    // 增量更新。返回是否继续处理同一消息的后续条目。
    bool applyUpdate(const Entry& e, AdapterResult& out) {
        if (!synced_) {
            ++stats_.ignored;
            if (++unsynced_updates_ > kMaxUnsyncedUpdates) {
                unsynced_updates_ = 0;
                out.reconnect = true;
                out.error = "OKX: no snapshot after resubscribing, reconnecting";
            }
            return true;
        }
        if (e.prev_seq != seq_) {
            ++stats_.gaps;
            out.error = adapter_detail::concat({"OKX: sequence gap (prevSeqId=", std::to_string(e.prev_seq),
                                                ", last seqId=", std::to_string(seq_), ")"});
            resync(out, nullptr);
            return false;
        }
        if (verify_ && !textsFit(e)) disableChecksum(out, "a level string is longer than 32 bytes");
        bool changed = false;
        // 簿：批量排序 + 归并（见 OrderBook::applyLevels）；原始字符串表按消息顺序逐档同步（同价以后者为准）
        const auto qty = [](const RawLevel& l) { return l.qty; };
        changed |= book_.applyLevels(BookSide::Bid, bids_.data() + e.bid_begin, e.bid_end - e.bid_begin, batch_, qty);
        changed |= book_.applyLevels(BookSide::Ask, asks_.data() + e.ask_begin, e.ask_end - e.ask_begin, batch_, qty);
        if (verify_) {
            for (std::size_t i = e.bid_begin; i < e.bid_end; ++i) setText(BookSide::Bid, bids_[i]);
            for (std::size_t i = e.ask_begin; i < e.ask_end; ++i) setText(BookSide::Ask, asks_[i]);
        }
        trimBook();
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
        if (verify_ && (e.seen & kChecksum) != 0) {
            std::int32_t computed = 0;
            const bool built = computeChecksum(computed);
            if (!built || !checksumMatches(computed, e.checksum)) {
                ++stats_.checksum_failures;
                out.error = adapter_detail::concat({"OKX: checksum mismatch after update (expected ",
                                                    std::to_string(e.checksum), ", computed ",
                                                    std::to_string(computed), ")"});
                resync(out, nullptr);
                return false;
            }
        }
        return true;
    }

    // 设置一档并同步原始字符串（数量 0 删除）。返回簿是否变化。
    bool setLevel(BookSide side, const RawLevel& l) {
        const bool changed = book_.setLevel(side, l.price, l.qty);
        if (verify_) setText(side, l);
        return changed;
    }

    // 同步一档的原始字符串（数量 0 删除）；只在校验开启时调用
    void setText(BookSide side, const RawLevel& l) {
        TextMap& texts = side == BookSide::Bid ? bid_text_ : ask_text_;
        const std::uint64_t key = adapter_detail::priceKey(l.price);
        if (!(l.qty > 0.0)) {
            texts.erase(key);
        } else {
            LevelText& t = texts[key];  // 引用只在本语句块内使用（之后才有下一次修改）
            std::memcpy(t.text, l.px.data(), l.px.size());
            t.text[l.px.size()] = ':';
            std::memcpy(t.text + l.px.size() + 1, l.sz.data(), l.sz.size());
            t.len = static_cast<std::uint8_t>(l.px.size() + 1 + l.sz.size());
        }
    }

    // 条目中所有字符串都能原样存进 LevelText
    bool textsFit(const Entry& e) const noexcept {
        auto fits = [](const RawLevel& l) {
            return l.px.size() <= LevelText::kMaxLen && l.sz.size() <= LevelText::kMaxLen;
        };
        for (std::size_t i = e.bid_begin; i < e.bid_end; ++i) {
            if (!fits(bids_[i])) return false;
        }
        for (std::size_t i = e.ask_begin; i < e.ask_end; ++i) {
            if (!fits(asks_[i])) return false;
        }
        return true;
    }

    // 拼接校验串；某档缺少原始字符串（内部不一致）时返回 false
    bool buildChecksumString(char* buf, std::size_t& len) const noexcept {
        const std::vector<PriceLevel>& bids = book_.getBids();
        const std::vector<PriceLevel>& asks = book_.getAsks();
        const std::size_t nb = std::min(kChecksumLevels, bids.size());
        const std::size_t na = std::min(kChecksumLevels, asks.size());
        std::size_t pos = 0;
        auto append = [&](const TextMap& texts, double price) {
            const LevelText* t = texts.find(adapter_detail::priceKey(price));
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

    // 文档规定 checksum 是有符号 32 位整数；为稳妥起见也接受同一 CRC 的无符号写法（比较低 32 位）
    static bool checksumMatches(std::int32_t computed, std::int64_t received) noexcept {
        if (received < std::numeric_limits<std::int32_t>::min() ||
            received > static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())) {
            return false;
        }
        return static_cast<std::uint32_t>(computed) == static_cast<std::uint32_t>(received);
    }

    bool computeChecksum(std::int32_t& out) const noexcept {
        char buf[kChecksumBufSize];
        std::size_t len = 0;
        if (!buildChecksumString(buf, len)) return false;
        out = hft::toSignedCrc(hft::crc32(buf, len));
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
            for (std::size_t i = limit; i < bids.size(); ++i) bid_text_.erase(adapter_detail::priceKey(bids[i].price));
            for (std::size_t i = limit; i < asks.size(); ++i) ask_text_.erase(adapter_detail::priceKey(asks[i].price));
        }
        ExchangeAdapter::trimBook();
    }

    // 重同步：清空簿；增量频道发送 unsubscribe + subscribe（推导不出退订帧时要求重连），全量频道等待下一次推送
    void resync(AdapterResult& out, const char* why) {
        ++stats_.resyncs;
        if (why != nullptr) out.error = std::string("OKX: ") + why + ", resyncing";
        if (!book_.empty()) out.book_updated = true;
        book_.clear();
        bid_text_.clear();
        ask_text_.clear();
        synced_ = false;
        seq_ = 0;
        unsynced_updates_ = 0;
        if (!incremental_) return;
        requestResubscribe(out, unsubscribe_msg_);
    }

    void resetState() noexcept override {
        book_.clear();
        bid_text_.clear();
        ask_text_.clear();
        synced_ = false;
        seq_ = 0;
        unsynced_updates_ = 0;
        incremental_ = false;
    }

    bool verify_;                       // checksum 校验是否开启（安全阀触发后永久关闭）
    std::string unsubscribe_msg_;        // 由订阅消息推导出的退订帧（"" = 推导不出）
    TextMap bid_text_;                   // 买盘每档原始字符串
    TextMap ask_text_;                   // 卖盘每档原始字符串
    adapter_detail::JsonReader reader_;
    std::vector<RawLevel> bids_;         // 当前消息全部条目的买档暂存
    std::vector<RawLevel> asks_;
    std::vector<Entry> entries_;         // 当前消息的 data 条目
    std::int64_t seq_ = 0;               // 最近一条已应用消息的 seqId
    std::size_t unsynced_updates_ = 0;   // 未同步期间收到的增量条数
    bool synced_ = false;
    bool incremental_ = false;           // 当前订阅是否为带 action 的增量频道（决定重同步方式）
};
