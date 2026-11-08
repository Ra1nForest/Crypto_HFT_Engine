//
// book_snapshot.h
// 定长订单簿快照：可平凡复制（trivially copyable），经 hft::SpscQueue 从行情线程传给主线程。
// Fixed-size order book snapshot: trivially copyable, passed from the market data thread to the main thread
// through hft::SpscQueue.
//

#pragma once

#include "orderbook.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <type_traits>

// 订单簿顶部快照：
// - 只有 bids[0, bid_count) 与 asks[0, ask_count) 有意义；assignFrom 不清零其余档位（省去 640 字节的写入）。
// - venue / seq / exchange_ts_ms / recv_ns / publish_ns 由发布者（行情 feed）填写；assignFrom 只写档位与 update_id。
// Snapshot of the top of the order book:
// - Only bids[0, bid_count) and asks[0, ask_count) are meaningful; assignFrom does not zero the remaining
//   levels (saving 640 bytes of writes).
// - venue / seq / exchange_ts_ms / recv_ns / publish_ns are filled in by the publisher (the market data
//   feed); assignFrom writes only the levels and update_id.
struct BookSnapshot {               // 可平凡复制；经 hft::SpscQueue 传递
                                    // trivially copyable; passed through hft::SpscQueue
    static constexpr int kMaxLevels = 20;
    std::uint32_t venue = 0;        // 行情源（feed）编号
                                    // feed id
    std::uint32_t bid_count = 0, ask_count = 0;
    std::uint64_t seq = 0;          // 每个 feed 的发布序号（1, 2, ...）
                                    // per-feed publish sequence number (1, 2, ...)
    std::uint64_t update_id = 0;    // 交易所更新号 / seqId，无则为 0
                                    // exchange update id / seqId, 0 if none
    std::int64_t exchange_ts_ms = 0;// 交易所事件时间（Unix 毫秒），未知为 0
                                    // exchange event time (Unix milliseconds), 0 if unknown
    std::int64_t recv_ns = 0;       // WebSocket 读取返回时的 hft::nowNs()
                                    // hft::nowNs() when the WebSocket read returned
    std::int64_t publish_ns = 0;    // tryPush 之前的 hft::nowNs()
                                    // hft::nowNs() just before tryPush
    PriceLevel bids[kMaxLevels]{};
    PriceLevel asks[kMaxLevels]{};

    // 从订单簿复制每侧最优的 levels 档（钳制到 [0, kMaxLevels]）以及更新号
    // Copy the best price levels of each side (as many as levels, clamped to [0, kMaxLevels]) and the update
    // id from the order book
    void assignFrom(const OrderBook& book, int levels = kMaxLevels) noexcept {
        const std::size_t n = static_cast<std::size_t>(std::clamp(levels, 0, kMaxLevels));
        const std::vector<PriceLevel>& b = book.getBids();
        const std::vector<PriceLevel>& a = book.getAsks();
        const std::size_t nb = std::min(n, b.size());
        const std::size_t na = std::min(n, a.size());
        if (nb > 0) std::memcpy(bids, b.data(), nb * sizeof(PriceLevel));
        if (na > 0) std::memcpy(asks, a.data(), na * sizeof(PriceLevel));
        bid_count = static_cast<std::uint32_t>(nb);
        ask_count = static_cast<std::uint32_t>(na);
        update_id = book.getUpdateId();
    }

    // 用快照替换 book 的全部档位与更新号（book 的 symbol 不变）。
    // OrderBook::assignLevels：有序的正常快照整块复制；损坏的输入（乱序/重复/非法数值）逐档 upsert，
    // 也不会破坏 book 的不变量。计数越界时钳制到 kMaxLevels。
    // Replace all levels and the update id of book with the snapshot (the symbol of book stays unchanged).
    // OrderBook::assignLevels: a normal sorted snapshot is copied as one block; corrupt input (out of order /
    // duplicates / invalid values) is upserted level by level and still cannot break the invariants of book.
    // Out-of-range counts are clamped to kMaxLevels.
    void copyTo(OrderBook& book) const {
        const std::uint32_t nb = std::min<std::uint32_t>(bid_count, kMaxLevels);
        const std::uint32_t na = std::min<std::uint32_t>(ask_count, kMaxLevels);
        book.assignLevels(BookSide::Bid, bids, nb);
        book.assignLevels(BookSide::Ask, asks, na);
        book.setUpdateId(update_id);
    }
};

static_assert(std::is_trivially_copyable_v<BookSnapshot>);
static_assert(std::is_standard_layout_v<BookSnapshot>);
