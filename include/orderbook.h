//
// orderbook.h
// L2 订单簿：买卖盘各一个按价格严格有序的扁平 vector（下标 0 为最优档），支持按价格 upsert/删除、批量应用、截断与自检。
// L2 order book: bids and asks each in a flat vector strictly ordered by price (index 0 is the best level),
// supporting upsert/delete by price, batch apply, truncation and self-check.
//

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// 价格档位：价格 + 数量
// Price level: price + quantity
struct PriceLevel {
    double price;
    double quantity;
};

// 买卖方向
// Bid/ask side
enum class BookSide { Bid, Ask };

// OrderBook::applyLevels 的可复用工作区（由调用者持有，例如每个行情适配器一个）。
// 两个 vector 的容量随使用增长后保持不变，稳态下批量更新零分配。不要在多个线程间共享。
// Reusable workspace for OrderBook::applyLevels (owned by the caller, e.g. one per market data adapter).
// The two vectors grow with use and then keep their capacity, so batch updates are allocation-free in steady
// state. Do not share it between threads.
struct LevelBatchScratch {
    struct Item {
        double price;
        double quantity;
        std::uint32_t seq;  // 在本批中的原始顺序：同价多条更新时以后出现者为准
                            // original order in this batch: with several updates at one price, the last one wins
    };
    std::vector<Item> items;
    std::vector<PriceLevel> merged;
    // 诊断计数（只增不减）：走批量归并路径的批次数，以及其中输入不是"最优在前、价格严格有序"、
    // 需要先排序的批次数（交易所的增量正常都已有序，后者应接近 0）
    // Diagnostic counters (never decrease): the number of batches that took the batch merge path, and of those,
    // the number whose input was not "best first, strictly ordered by price" and had to be sorted first
    // (exchange deltas are normally already ordered, so the latter should be close to 0)
    std::uint64_t batches = 0;
    std::uint64_t sorted_batches = 0;
};

// L2 订单簿：
// - 买盘严格递减、卖盘严格递增，下标 0 为最优档（显示模块与套利检测依赖 getBids()[0] 为买一）。
// - updateBid/updateAsk/setLevel 为 upsert：二分查找价格；数量 <= 0 删除该档；价格或数量不是有限值时忽略
//   （保证簿始终满足 validate()）。
// - 快路径：先比较尾部（追加更差的价格，O(1) 均摊），再顺序检查最优的 8 档（顶部更新最常见），其余情况二分查找。
//   旧行情代码"clearBids + 按交易所顺序逐档 updateBid"因此仍是每档 O(1)。
// - 价格相等用 double 精确比较：相同的交易所字符串经 from_chars（正确舍入）解析出相同的 double。
// - 设计取舍：扁平 vector 让顶部读取与整簿拷贝极快、缓存友好；代价是在顶部插入/删除新档位需要
//   memmove 整个一侧（400 档约 6.4 KB，百纳秒级）。修改已有档位的数量不移动内存。
//   一条增量消息有很多档位变化时用 applyLevels：一次线性归并（输入不是最优在前时先排序），避免逐档 memmove
//   （深簿上快一个数量级）。
// - 非线程安全：由持有者自行同步（行情线程私有，或在锁内拷贝）。
// L2 order book:
// - Bids strictly descending, asks strictly ascending, index 0 is the best level (the display module and the
//   arbitrage detector rely on getBids()[0] being the best bid).
// - updateBid/updateAsk/setLevel are upserts: binary search for the price; quantity <= 0 deletes the level; a
//   non-finite price or quantity is ignored (so the book always satisfies validate()).
// - Fast paths: first compare against the tail (appending a worse price, amortized O(1)), then scan the best 8
//   levels in order (top-of-book updates are the most common), otherwise binary search.
//   The old market data code "clearBids + updateBid level by level in exchange order" is therefore still O(1)
//   per level.
// - Price equality is an exact double comparison: identical exchange strings parse through from_chars
//   (correctly rounded) to identical doubles.
// - Design trade-off: flat vectors make top-of-book reads and whole-book copies very fast and cache-friendly;
//   the cost is that inserting/deleting a level at the top has to memmove the whole side (about 6.4 KB for
//   400 levels, on the order of 100 ns). Changing the quantity of an existing level moves no memory.
//   When one incremental message changes many levels, use applyLevels: a single linear merge (sorting first
//   if the input is not best first) avoids a memmove per level (an order of magnitude faster on deep books).
// - Not thread-safe: the owner synchronizes (private to the market data thread, or copied under a lock).
class OrderBook {
public:
    // 默认构造：symbol 为空
    // Default constructor: empty symbol
    OrderBook() : OrderBook(std::string()) {}

    // 指定交易对构造（保持隐式转换，与旧接口一致）
    // Constructs with a symbol (keeps the implicit conversion, consistent with the old interface)
    OrderBook(const std::string& symbol) : symbol_(symbol) { reserve(20); }

    // ---------------- 修改 ----------------
    // ---------------- Modifiers ----------------

    // 清空买盘/卖盘（保留容量）
    // Clear the bids/asks (keeps capacity)
    void clearBids() noexcept { bids_.clear(); }
    void clearAsks() noexcept { asks_.clear(); }

    // 清空两侧并把更新号归零（保留容量与 symbol）
    // Clear both sides and reset the update id to zero (keeps capacity and symbol)
    void clear() noexcept {
        bids_.clear();
        asks_.clear();
        update_id_ = 0;
    }

    // 为两侧各预留 n 档容量
    // Reserve capacity for n levels on each side
    void reserve(std::size_t n) {
        bids_.reserve(n);
        asks_.reserve(n);
    }

    // 买盘/卖盘 upsert（旧接口名；语义见 setLevel）
    // Bid/ask upsert (old interface names; see setLevel for the semantics)
    void updateBid(double price, double quantity) { setLevel(BookSide::Bid, price, quantity); }
    void updateAsk(double price, double quantity) { setLevel(BookSide::Ask, price, quantity); }

    // 设置某一侧某价格的数量：quantity <= 0 删除该档；价格或数量非有限值时忽略。
    // 返回簿是否真的发生了变化（插入、删除或数量改变）。
    // Sets the quantity at a given price on one side: quantity <= 0 deletes the level; a non-finite price or
    // quantity is ignored.
    // Returns whether the book actually changed (insert, delete or quantity change).
    bool setLevel(BookSide side, double price, double quantity) {
        if (side == BookSide::Bid) return upsert<true>(bids_, price, quantity);
        return upsert<false>(asks_, price, quantity);
    }

    // 批量 upsert：一次性应用一条增量消息中某一侧的全部档位，最终状态与"按顺序逐档 setLevel"完全相同
    // （同一价格以后出现者为准；数量 <= 0 删除；价格或数量非有限值的条目忽略）。
    // - 不超过 kBatchThreshold 条：直接逐档 upsert（顶部附近的少量更新这样最快，返回值也与逐档调用一致）。
    // - 更多条：与现有档位做一次线性归并，取代逐档插入/删除各自 O(档数) 的 memmove
    //   （逐档 upsert 时每个变化在 1000 档的簿上约 0.7 µs、5000 档约 4.5 µs，插入/删除/改量各占 1/3，
    //   见 benchmark/market_data_benchmark.cpp 第 4 节；一条 Binance 100 ms 增量常有上百档变化）。
    //   交易所的增量实际上都按最优在前、价格严格有序发送（Binance / OKX / Bybit 的文档并未承诺这一点，模拟交易所
    //   也这样发送）：收集条目时顺带检查，已有序就直接归并，O(档数 + n)；否则先按价格排序（下标作次序键，
    //   std::sort 原地进行、不分配内存）、同价只保留最后一条，O(档数 + n log n)。两条路径的结果完全相同。
    //   此时返回"最终状态是否与之前不同"：先改后又改回原值的档位不算变化（逐档调用会报告变化）。
    // Level 是带 price 与 quantity 字段的档位类型（PriceLevel、适配器的原始档位）；levels 不得指向本簿自身的存储。
    // Batch upsert: applies all of one side's levels from one incremental message at once; the final state is
    // exactly the same as "setLevel level by level, in order" (for the same price the later entry wins;
    // quantity <= 0 deletes; entries with a non-finite price or quantity are ignored).
    // - At most kBatchThreshold entries: plain per-level upserts (fastest for a few updates near the top, and the
    //   return value also matches the per-level calls).
    // - More entries: one linear merge with the existing levels, replacing the O(levels) memmove of every
    //   per-level insert/delete (with per-level upserts each change costs about 0.7 µs on a 1000-level book and
    //   about 4.5 µs at 5000 levels, inserts/deletes/quantity changes 1/3 each, see section 4 of
    //   benchmark/market_data_benchmark.cpp; a single Binance 100 ms delta often changes a hundred or more levels).
    //   In practice exchanges send deltas best first and strictly ordered by price (the Binance / OKX / Bybit docs
    //   do not promise this; the mock exchange sends them this way too): this is checked while collecting the
    //   entries, and ordered input is merged directly, O(levels + n); otherwise it is first sorted by price (the
    //   index as the secondary key, std::sort runs in place and allocates no memory), keeping only the last entry
    //   per price, O(levels + n log n). Both paths give exactly the same result.
    //   In this case it returns "whether the final state differs from before": a level that is changed and then
    //   changed back to its original value does not count as a change (per-level calls would report a change).
    // Level is a level type with price and quantity fields (PriceLevel, an adapter's raw level); levels must not
    // point into this book's own storage.
    template <class Level>
    bool applyLevels(BookSide side, const Level* levels, std::size_t n, LevelBatchScratch& scratch) {
        if (n <= kBatchThreshold) {
            bool changed = false;
            for (std::size_t i = 0; i < n; ++i) changed |= setLevel(side, levels[i].price, levels[i].quantity);
            return changed;
        }
        if (side == BookSide::Bid) return applyBatch<true>(bids_, levels, n, scratch);
        return applyBatch<false>(asks_, levels, n, scratch);
    }

    // 用 levels[0, n) 整体替换某一侧（应为最优在前的有序档位；levels 不得指向本簿自身的存储）。
    // 输入严格有序、价格有限、数量为正的有限值时直接整块复制（BookSnapshot::copyTo 走这里）；
    // 否则退化为清空 + 逐档 upsert（结果同样满足 validate()，重复价格以后出现者为准）。返回是否走了快路径。
    // Replaces one side wholesale with levels[0, n) (expected to be ordered levels, best first; levels must not
    // point into this book's own storage).
    // Input that is strictly ordered, with finite prices and positive finite quantities, is copied as one block
    // (BookSnapshot::copyTo takes this path); anything else falls back to clear + per-level upsert (the result
    // still satisfies validate(), and for a repeated price the later entry wins). Returns whether the fast path
    // was taken.
    bool assignLevels(BookSide side, const PriceLevel* levels, std::size_t n) {
        const bool bid = side == BookSide::Bid;
        std::vector<PriceLevel>& v = bid ? bids_ : asks_;
        bool sorted = true;
        for (std::size_t i = 0; i < n && sorted; ++i) {
            const PriceLevel& l = levels[i];
            sorted = std::isfinite(l.price) && std::isfinite(l.quantity) && l.quantity > 0.0 &&
                     (i == 0 || (bid ? levels[i - 1].price > l.price : levels[i - 1].price < l.price));
        }
        v.clear();
        if (sorted) {
            v.insert(v.end(), levels, levels + n);
            return true;
        }
        for (std::size_t i = 0; i < n; ++i) setLevel(side, levels[i].price, levels[i].quantity);
        return false;
    }

    // 每侧只保留最优的 max_levels_per_side 档
    // Keep only the best max_levels_per_side levels on each side
    void truncate(std::size_t max_levels_per_side) {
        if (bids_.size() > max_levels_per_side) bids_.resize(max_levels_per_side);
        if (asks_.size() > max_levels_per_side) asks_.resize(max_levels_per_side);
    }

    void setSymbol(const std::string& symbol) { symbol_ = symbol; }

    // 交易所更新号（Binance lastUpdateId/u、OKX seqId、Bybit u；0 表示无）
    // Exchange update id (Binance lastUpdateId/u, OKX seqId, Bybit u; 0 means none)
    std::uint64_t getUpdateId() const noexcept { return update_id_; }
    void setUpdateId(std::uint64_t id) noexcept { update_id_ = id; }

    // ---------------- 查询 ----------------
    // ---------------- Queries ----------------

    // 最优买/卖（空时返回 {0, 0}）
    // Best bid/ask (returns {0, 0} when empty)
    PriceLevel getBestBid() const noexcept {
        if (bids_.empty()) return {0.0, 0.0};
        return bids_[0];
    }

    PriceLevel getBestAsk() const noexcept {
        if (asks_.empty()) return {0.0, 0.0};
        return asks_[0];
    }

    // 点差（卖一 - 买一），任一侧为空时为 0
    // Spread (best ask - best bid), 0 if either side is empty
    double getSpread() const noexcept {
        auto bid = getBestBid();
        auto ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) return 0.0;
        return ask.price - bid.price;
    }

    // 中间价，任一侧为空时为 0
    // Mid price, 0 if either side is empty
    double getMid() const noexcept {
        if (bids_.empty() || asks_.empty()) return 0.0;
        return (bids_[0].price + asks_[0].price) / 2.0;
    }

    // 是否交叉：两侧都非空且买一 >= 卖一
    // Whether the book is crossed: both sides non-empty and best bid >= best ask
    bool isCrossed() const noexcept {
        return !bids_.empty() && !asks_.empty() && bids_[0].price >= asks_[0].price;
    }

    // 基本访问器
    // Basic accessors
    const std::string& getSymbol() const noexcept { return symbol_; }

    const std::vector<PriceLevel>& getBids() const noexcept { return bids_; }
    const std::vector<PriceLevel>& getAsks() const noexcept { return asks_; }
    const std::vector<PriceLevel>& levels(BookSide side) const noexcept {
        return side == BookSide::Bid ? bids_ : asks_;
    }
    int getBidCount() const noexcept { return static_cast<int>(bids_.size()); }
    int getAskCount() const noexcept { return static_cast<int>(asks_.size()); }
    bool empty() const noexcept { return bids_.empty() && asks_.empty(); }

    // 自检：两侧严格有序（买盘递减、卖盘递增）、价格有限、数量为正的有限值。
    // 失败时返回 false 并在 why 中写明第一处违例。交叉不算违例（用 isCrossed() 单独判断）。
    // Self-check: both sides strictly ordered (bids descending, asks ascending), finite prices, positive finite
    // quantities.
    // On failure returns false and describes the first violation in why. Being crossed is not a violation
    // (check it separately with isCrossed()).
    bool validate(std::string* why = nullptr) const {
        return validateSide(bids_, true, why) && validateSide(asks_, false, why);
    }

    // applyLevels 改走排序 + 归并的批量阈值（条数）
    // Batch threshold (number of entries) for applyLevels to switch to sort + merge
    static constexpr std::size_t kBatchThreshold = 8;

private:
    // upsert 头部顺序查找的档数（之后改用二分查找）
    // Number of levels upsert scans sequentially at the head (binary search after that)
    static constexpr std::size_t kLinearProbe = 8;

    // a 是否比 b 更优：买盘价格越高越优，卖盘价格越低越优
    // Whether a is better than b: for bids a higher price is better, for asks a lower price is better
    template <bool kBid>
    static bool better(double a, double b) noexcept {
        if constexpr (kBid) {
            return a > b;
        } else {
            return a < b;
        }
    }

    // 批量路径。kBid = true 表示买盘（降序）。
    // 1. 过滤掉非有限值的条目放进 scratch.items（保持原始顺序），同时检查剩下的条目是否已按最优在前、价格严格有序；
    //    是则没有同价重复、排序结果就是原序列，直接跳过排序。
    // 2. 与现有档位线性归并到 scratch.merged，有变化时与 v 交换存储（旧存储留在 scratch 中供下次复用）。
    // Batch path. kBid = true means bids (descending).
    // 1. Put the entries except the non-finite ones into scratch.items (keeping the original order), checking
    //    at the same time whether they are already best first and strictly ordered by price; if so there are no
    //    duplicate prices and the sorted result is the original sequence, so sorting is skipped.
    // 2. Merge linearly with the existing levels into scratch.merged; if anything changed, swap storage with v
    //    (the old storage stays in scratch for reuse next time).
    template <bool kBid, class Level>
    static bool applyBatch(std::vector<PriceLevel>& v, const Level* levels, std::size_t n,
                           LevelBatchScratch& scratch) {
        using Item = LevelBatchScratch::Item;
        std::vector<Item>& items = scratch.items;
        items.clear();
        items.reserve(n);
        bool best_first = true;
        for (std::size_t i = 0; i < n; ++i) {
            const double price = levels[i].price;
            const double qty = levels[i].quantity;
            if (!std::isfinite(price) || !std::isfinite(qty)) continue;  // 与 setLevel 一致：忽略
                                                                         // ignored, as in setLevel
            if (!items.empty() && !better<kBid>(items.back().price, price)) best_first = false;
            items.push_back({price, qty, static_cast<std::uint32_t>(i)});
        }
        ++scratch.batches;
        if (items.empty()) return false;
        if (!best_first) {
            // 最优在前；同价按原始顺序，这样每个同价段的最后一条就是生效的那条
            // Best first; equal prices keep their original order, so the last entry of each equal-price run is the
            // one that takes effect
            ++scratch.sorted_batches;
            std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) {
                if (a.price != b.price) return better<kBid>(a.price, b.price);
                return a.seq < b.seq;
            });
        }
        std::vector<PriceLevel>& out = scratch.merged;
        out.clear();
        out.reserve(v.size() + items.size());
        bool changed = false;
        std::size_t i = 0;  // 现有档位游标
                            // cursor into the existing levels
        for (std::size_t j = 0; j < items.size();) {
            std::size_t last = j;
            while (last + 1 < items.size() && items[last + 1].price == items[j].price) ++last;
            const Item& up = items[last];
            j = last + 1;
            // 比该价格更优的现有档位原样保留
            // Existing levels better than this price are kept as they are
            while (i < v.size() && better<kBid>(v[i].price, up.price)) out.push_back(v[i++]);
            const bool remove = !(up.quantity > 0.0);
            if (i < v.size() && v[i].price == up.price) {
                if (remove) {
                    changed = true;  // 删除已有档位
                                     // delete an existing level
                } else {
                    if (v[i].quantity != up.quantity) changed = true;
                    out.push_back({up.price, up.quantity});
                }
                ++i;
            } else if (!remove) {
                out.push_back({up.price, up.quantity});  // 新档位
                                                         // new level
                changed = true;
            }
            // 删除不存在的档位：无事发生
            // Deleting a level that does not exist: nothing happens
        }
        if (!changed) return false;
        out.insert(out.end(), v.begin() + static_cast<std::ptrdiff_t>(i), v.end());
        v.swap(out);
        return true;
    }

    // upsert 实现。kBid = true 表示买盘（降序）。
    // upsert implementation. kBid = true means bids (descending).
    template <bool kBid>
    static bool upsert(std::vector<PriceLevel>& v, double price, double qty) {
        if (!std::isfinite(price) || !std::isfinite(qty)) return false;
        const bool remove = !(qty > 0.0);

        // 尾部快路径：比当前最差档更差（或簿为空）→ 直接追加。快照按交易所顺序重建时每档都走这里。
        // Tail fast path: worse than the current worst level (or the book is empty) → append directly. When a
        // snapshot is rebuilt in exchange order, every level goes through here.
        if (v.empty() || better<kBid>(v.back().price, price)) {
            if (remove) return false;
            v.push_back({price, qty});
            return true;
        }

        // 头部快路径：在最优的 kLinearProbe 档内顺序查找（顶部更新最常见，顺序比较的分支几乎总能预测正确，
        // 比二分查找快）；找到第一个"不优于 price"的档位即停
        // Head fast path: sequential search within the best kLinearProbe levels (top-of-book updates are the most
        // common, and the branches of sequential comparisons are almost always predicted correctly, which beats
        // binary search); stops at the first level that is "not better than price"
        const std::size_t probe = std::min(v.size(), kLinearProbe);
        std::size_t i = 0;
        while (i < probe && better<kBid>(v[i].price, price)) ++i;
        auto it = v.begin() + static_cast<std::ptrdiff_t>(i);
        if (i == probe && i < v.size()) {
            // 一般情况：二分查找第一个"不优于 price"的档位（price 不比尾部更差，结果必在 [probe, end) 内）
            // General case: binary search for the first level "not better than price" (price is no worse than the
            // tail, so the result must lie in [probe, end))
            it = std::lower_bound(it, v.end(), price,
                                  [](const PriceLevel& l, double p) { return better<kBid>(l.price, p); });
        }
        if (it != v.end() && it->price == price) return applyAt(v, it, qty, remove);
        if (remove) return false;
        v.insert(it, {price, qty});
        return true;
    }

    // 对已存在的档位执行删除或改数量
    // Delete an existing level or change its quantity
    static bool applyAt(std::vector<PriceLevel>& v, std::vector<PriceLevel>::iterator it, double qty, bool remove) {
        if (remove) {
            v.erase(it);
            return true;
        }
        if (it->quantity == qty) return false;
        it->quantity = qty;
        return true;
    }

    static bool validateSide(const std::vector<PriceLevel>& v, bool bid, std::string* why) {
        const char* name = bid ? "bid" : "ask";
        auto fail = [&](std::size_t i, const char* what) {
            if (why != nullptr) *why = std::string(name) + " level " + std::to_string(i) + ": " + what;
            return false;
        };
        for (std::size_t i = 0; i < v.size(); ++i) {
            const PriceLevel& l = v[i];
            if (!std::isfinite(l.price)) return fail(i, "price is not finite");
            if (!std::isfinite(l.quantity) || !(l.quantity > 0.0)) {
                return fail(i, "quantity is not a positive finite number");
            }
            if (i > 0) {
                const bool ordered = bid ? v[i - 1].price > l.price : v[i - 1].price < l.price;
                if (!ordered) return fail(i, bid ? "bids are not strictly descending" : "asks are not strictly ascending");
            }
        }
        return true;
    }

    // 交易对/来源标识（如 "Binance:BTCUSDT"）
    // Symbol/source identifier (e.g. "Binance:BTCUSDT")
    std::string symbol_;
    // 买盘（从高到低）与卖盘（从低到高）
    // Bids (high to low) and asks (low to high)
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
    // 交易所更新号
    // Exchange update id
    std::uint64_t update_id_ = 0;
};
