//
// orderbook.h
// L2 订单簿：买卖盘各一个按价格严格有序的扁平 vector（下标 0 为最优档），支持按价格 upsert/删除、截断与自检。
//

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

// 价格档位：价格 + 数量
struct PriceLevel {
    double price;
    double quantity;
};

// 买卖方向
enum class BookSide { Bid, Ask };

// L2 订单簿：
// - 买盘严格递减、卖盘严格递增，下标 0 为最优档（显示模块与套利检测依赖 getBids()[0] 为买一）。
// - updateBid/updateAsk/setLevel 为 upsert：二分查找价格；数量 <= 0 删除该档；价格或数量不是有限值时忽略
//   （保证簿始终满足 validate()）。
// - 快路径：先比较尾部（追加更差的价格，O(1) 均摊），再顺序检查最优的 8 档（顶部更新最常见），其余情况二分查找。
//   旧行情代码"clearBids + 按交易所顺序逐档 updateBid"因此仍是每档 O(1)。
// - 价格相等用 double 精确比较：相同的交易所字符串经 from_chars（正确舍入）解析出相同的 double。
// - 设计取舍：扁平 vector 让顶部读取与整簿拷贝极快、缓存友好；代价是在顶部插入/删除新档位需要
//   memmove 整个一侧（400 档约 6.4 KB，百纳秒级）。修改已有档位的数量不移动内存。
// - 非线程安全：由持有者自行同步（行情线程私有，或在锁内拷贝）。
class OrderBook {
public:
    // 默认构造：symbol 为空
    OrderBook() : symbol_("") { reserve(20); }

    // 指定交易对构造（保持隐式转换，与旧接口一致）
    OrderBook(const std::string& symbol) : symbol_(symbol) { reserve(20); }

    // ---------------- 修改 ----------------

    // 清空买盘/卖盘（保留容量）
    void clearBids() noexcept { bids_.clear(); }
    void clearAsks() noexcept { asks_.clear(); }

    // 清空两侧并把更新号归零（保留容量与 symbol）
    void clear() noexcept {
        bids_.clear();
        asks_.clear();
        update_id_ = 0;
    }

    // 为两侧各预留 n 档容量
    void reserve(std::size_t n) {
        bids_.reserve(n);
        asks_.reserve(n);
    }

    // 买盘/卖盘 upsert（旧接口名；语义见 setLevel）
    void updateBid(double price, double quantity) { setLevel(BookSide::Bid, price, quantity); }
    void updateAsk(double price, double quantity) { setLevel(BookSide::Ask, price, quantity); }

    // 设置某一侧某价格的数量：quantity <= 0 删除该档；价格或数量非有限值时忽略。
    // 返回簿是否真的发生了变化（插入、删除或数量改变）。
    bool setLevel(BookSide side, double price, double quantity) {
        if (side == BookSide::Bid) return upsert<true>(bids_, price, quantity);
        return upsert<false>(asks_, price, quantity);
    }

    // 用 levels[0, n) 整体替换某一侧（应为最优在前的有序档位；levels 不得指向本簿自身的存储）。
    // 输入严格有序、价格有限、数量为正的有限值时直接整块复制（BookSnapshot::copyTo 走这里）；
    // 否则退化为清空 + 逐档 upsert（结果同样满足 validate()，重复价格以后出现者为准）。返回是否走了快路径。
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
    void truncate(std::size_t max_levels_per_side) {
        if (bids_.size() > max_levels_per_side) bids_.resize(max_levels_per_side);
        if (asks_.size() > max_levels_per_side) asks_.resize(max_levels_per_side);
    }

    void setSymbol(const std::string& symbol) { symbol_ = symbol; }

    // 交易所更新号（Binance lastUpdateId/u、OKX seqId、Bybit u；0 表示无）
    std::uint64_t getUpdateId() const noexcept { return update_id_; }
    void setUpdateId(std::uint64_t id) noexcept { update_id_ = id; }

    // ---------------- 查询 ----------------

    // 最优买/卖（空时返回 {0, 0}）
    PriceLevel getBestBid() const noexcept {
        if (bids_.empty()) return {0.0, 0.0};
        return bids_[0];
    }

    PriceLevel getBestAsk() const noexcept {
        if (asks_.empty()) return {0.0, 0.0};
        return asks_[0];
    }

    // 点差（卖一 - 买一），任一侧为空时为 0
    double getSpread() const noexcept {
        auto bid = getBestBid();
        auto ask = getBestAsk();
        if (bid.price == 0.0 || ask.price == 0.0) return 0.0;
        return ask.price - bid.price;
    }

    // 中间价，任一侧为空时为 0
    double getMid() const noexcept {
        if (bids_.empty() || asks_.empty()) return 0.0;
        return (bids_[0].price + asks_[0].price) / 2.0;
    }

    // 是否交叉：两侧都非空且买一 >= 卖一
    bool isCrossed() const noexcept {
        return !bids_.empty() && !asks_.empty() && bids_[0].price >= asks_[0].price;
    }

    // 某一侧前 levels 档的累计数量
    double cumulativeQty(BookSide side, std::size_t levels) const noexcept {
        const std::vector<PriceLevel>& v = this->levels(side);
        const std::size_t n = std::min(levels, v.size());
        double sum = 0.0;
        for (std::size_t i = 0; i < n; ++i) sum += v[i].quantity;
        return sum;
    }

    // 基本访问器
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
    bool validate(std::string* why = nullptr) const {
        return validateSide(bids_, true, why) && validateSide(asks_, false, why);
    }

    // 简单打印：用于调试/控制台展示
    void print(int levels = 5) const {
        std::cout << std::fixed;
        std::cout << "  " << symbol_ << std::endl;
        int show = std::min(levels, static_cast<int>(std::max(bids_.size(), asks_.size())));
        for (int i = 0; i < show; ++i) {
            if (i < static_cast<int>(bids_.size())) {
                std::cout << "  " << std::setw(8) << std::setprecision(4) << bids_[i].quantity
                          << "  " << std::setw(10) << std::setprecision(2) << bids_[i].price;
            } else {
                std::cout << std::setw(20) << " ";
            }
            std::cout << " | ";
            if (i < static_cast<int>(asks_.size())) {
                std::cout << std::setw(10) << std::setprecision(2) << asks_[i].price
                          << "  " << std::setw(8) << std::setprecision(4) << asks_[i].quantity;
            } else {
                std::cout << std::setw(20) << " ";
            }
            std::cout << std::endl;
        }
    }

private:
    // upsert 头部顺序查找的档数（之后改用二分查找）
    static constexpr std::size_t kLinearProbe = 8;

    // a 是否比 b 更优：买盘价格越高越优，卖盘价格越低越优
    template <bool kBid>
    static bool better(double a, double b) noexcept {
        if constexpr (kBid) {
            return a > b;
        } else {
            return a < b;
        }
    }

    // upsert 实现。kBid = true 表示买盘（降序）。
    template <bool kBid>
    static bool upsert(std::vector<PriceLevel>& v, double price, double qty) {
        if (!std::isfinite(price) || !std::isfinite(qty)) return false;
        const bool remove = !(qty > 0.0);

        // 尾部快路径：比当前最差档更差（或簿为空）→ 直接追加。快照按交易所顺序重建时每档都走这里。
        if (v.empty() || better<kBid>(v.back().price, price)) {
            if (remove) return false;
            v.push_back({price, qty});
            return true;
        }

        // 头部快路径：在最优的 kLinearProbe 档内顺序查找（顶部更新最常见，顺序比较的分支几乎总能预测正确，
        // 比二分查找快）；找到第一个"不优于 price"的档位即停
        const std::size_t probe = std::min(v.size(), kLinearProbe);
        std::size_t i = 0;
        while (i < probe && better<kBid>(v[i].price, price)) ++i;
        auto it = v.begin() + static_cast<std::ptrdiff_t>(i);
        if (i == probe && i < v.size()) {
            // 一般情况：二分查找第一个"不优于 price"的档位（price 不比尾部更差，结果必在 [probe, end) 内）
            it = std::lower_bound(it, v.end(), price,
                                  [](const PriceLevel& l, double p) { return better<kBid>(l.price, p); });
        }
        if (it != v.end() && it->price == price) {
            return applyAt(v, it, qty, remove);
        }
        if (remove) return false;
        v.insert(it, {price, qty});
        return true;
    }

    // 对已存在的档位执行删除或改数量
    static bool applyAt(std::vector<PriceLevel>& v, std::vector<PriceLevel>::iterator it, double qty,
                        bool remove) {
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
    std::string symbol_;
    // 买盘（从高到低）与卖盘（从低到高）
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
    // 交易所更新号
    std::uint64_t update_id_ = 0;
};
