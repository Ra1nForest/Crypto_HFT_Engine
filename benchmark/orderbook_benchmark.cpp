//
// orderbook_benchmark.cpp
// 对比 std::map 与 vector 订单簿在深度更新场景下的性能。
// Compares the performance of std::map and vector order books for depth updates.
//

#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <vector>

namespace {

// map 版本订单簿：支持价格级别的插入/删除
// map-based order book: supports inserting/erasing price levels
class MapOrderBook {
public:
    // 清空双边
    // Clear both sides
    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    // 更新买/卖盘：quantity==0 表示删除
    // Update bids/asks: quantity==0 means erase
    void updateBid(double price, double quantity) {
        if (quantity == 0.0) bids_.erase(price);
        else bids_[price] = quantity;
    }

    void updateAsk(double price, double quantity) {
        if (quantity == 0.0) asks_.erase(price);
        else asks_[price] = quantity;
    }

    // 最优买/卖价（map 自动有序）
    // Best bid/ask (the map keeps itself sorted)
    double getBestBid() const { return bids_.empty() ? 0.0 : bids_.rbegin()->first; }
    double getBestAsk() const { return asks_.empty() ? 0.0 : asks_.begin()->first; }

    // 点差
    // Bid-ask spread
    double getSpread() const { return getBestAsk() - getBestBid(); }

    // 扫描求和（模拟计算负载）
    // Scan and sum (simulates a compute load)
    double sumBids() const { return sum(bids_); }
    double sumAsks() const { return sum(asks_); }

private:
    static double sum(const std::map<double, double>& side) {
        double s = 0.0;
        for (const auto& [price, qty] : side) s += price * qty;
        return s;
    }

    std::map<double, double> bids_;
    std::map<double, double> asks_;
};

// 简单价格档位结构
// Simple price level struct
struct PriceLevel {
    double price;
    double quantity;
};

// vector 版本订单簿：以数组顺序保存固定深度快照
// vector-based order book: stores a fixed-depth snapshot in array order
class VecOrderBook {
public:
    VecOrderBook() {
        bids_.reserve(20);
        asks_.reserve(20);
    }

    // 清空快照
    // Clear the snapshot
    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }

    // 按顺序追加档位
    // Append levels in order
    void updateBid(double price, double quantity) { bids_.push_back({price, quantity}); }
    void updateAsk(double price, double quantity) { asks_.push_back({price, quantity}); }

    // 最优买/卖价：假定 vector[0] 即最优
    // Best bid/ask: assumes vector[0] is the best
    double getBestBid() const { return bids_.empty() ? 0.0 : bids_[0].price; }
    double getBestAsk() const { return asks_.empty() ? 0.0 : asks_[0].price; }

    // 点差
    // Bid-ask spread
    double getSpread() const { return getBestAsk() - getBestBid(); }

    // 扫描求和（模拟计算负载）
    // Scan and sum (simulates a compute load)
    double sumBids() const { return sum(bids_); }
    double sumAsks() const { return sum(asks_); }

private:
    static double sum(const std::vector<PriceLevel>& side) {
        double s = 0.0;
        for (const auto& level : side) s += level.price * level.quantity;
        return s;
    }

    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
};

// 模拟深度快照（固定 20 档）
// Simulated depth snapshot (fixed at 20 levels)
struct DepthSnapshot {
    double bid_prices[20];
    double bid_qtys[20];
    double ask_prices[20];
    double ask_qtys[20];
};

// 生成随机快照：用于复现实验结果
// Generate random snapshots: lets the results be reproduced
std::vector<DepthSnapshot> generateSnapshots(int count) {
    std::mt19937 gen(42);  // 固定种子，结果可复现
                           // fixed seed, reproducible results
    std::uniform_real_distribution<> price_dist(69000.0, 71000.0);
    std::uniform_real_distribution<> qty_dist(0.001, 5.0);

    std::vector<DepthSnapshot> snapshots(count);
    for (auto& s : snapshots) {
        double mid = price_dist(gen);
        for (int j = 0; j < 20; ++j) {
            s.bid_prices[j] = mid - j * 0.01;  // 买盘从高到低
                                               // bids from high to low
            s.bid_qtys[j] = qty_dist(gen);
            s.ask_prices[j] = mid + 0.01 + j * 0.01;  // 卖盘从低到高
                                                      // asks from low to high
            s.ask_qtys[j] = qty_dist(gen);
        }
    }
    return snapshots;
}

constexpr int kIterations = 100000;
constexpr int kWarmup = 10000;

// 处理一份快照：清空双边 + 40 次插入 + 点差 + 扫描求和。强制内联：预热与计时两个循环都调用它，
// 不内联时每份快照多一次函数调用，测到的就不再是旧版逐行展开的循环
// Process one snapshot: clear both sides + 40 inserts + spread + scan sum. Forced inline: both the warm-up
// and the timed loop call it, and without inlining every snapshot would pay an extra function call, so the
// measurement would no longer match the old version, which wrote the loop body out inline
template <class Book>
__attribute__((always_inline)) inline void applySnapshot(Book& book, const DepthSnapshot& s, volatile double& sink) {
    book.clearBids();
    book.clearAsks();
    for (int j = 0; j < 20; ++j) {
        book.updateBid(s.bid_prices[j], s.bid_qtys[j]);
        book.updateAsk(s.ask_prices[j], s.ask_qtys[j]);
    }
    sink = book.getSpread() + book.sumBids() + book.sumAsks();
}

// 先用前 kWarmup 份快照预热（让 CPU 与缓存进入稳定状态），再对后 kIterations 份正式计时
// Warm up with the first kWarmup snapshots (to bring the CPU and caches to a steady state), then time the
// next kIterations
template <class Book>
void runBenchmark(const char* title, const std::vector<DepthSnapshot>& snapshots) {
    Book book;
    volatile double sink = 0.0;  // 防止编译器优化掉计算
                                 // keeps the compiler from optimizing the computation away
    for (int i = 0; i < kWarmup; ++i) applySnapshot(book, snapshots[i], sink);
    const auto start = std::chrono::high_resolution_clock::now();
    for (int i = kWarmup; i < kWarmup + kIterations; ++i) applySnapshot(book, snapshots[i], sink);
    const auto end = std::chrono::high_resolution_clock::now();

    const auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    const double avg_ns = static_cast<double>(total_ns) / kIterations;
    std::cout << title << ":" << std::endl;
    std::cout << "  Total time:    " << std::setw(10) << total_ns / 1000000 << " ms" << std::endl;
    std::cout << "  Avg per update:" << std::setw(10) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
    std::cout << "  Avg per update:" << std::setw(10) << std::setprecision(2) << avg_ns / 1000.0 << " μs" << std::endl;
    std::cout << std::endl;
}

}  // namespace

int main() {
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║       OrderBook Benchmark: map vs vector          ║" << std::endl;
    std::cout << "╠═══════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Iterations: " << std::setw(8) << kIterations << "                             ║" << std::endl;
    std::cout << "║  Depth: 20 levels per side                        ║" << std::endl;
    std::cout << "║  Operations: clear + 40 inserts + spread + scan   ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;

    // 生成数据集（包含 warmup + 正式测试）
    // Generate the data set (warmup + the timed run)
    const auto snapshots = generateSnapshots(kIterations + kWarmup);
    runBenchmark<MapOrderBook>("std::map OrderBook", snapshots);
    runBenchmark<VecOrderBook>("flat vector OrderBook", snapshots);

    std::cout << "═══ Conclusion ═══" << std::endl;
    std::cout << "  flat vector avoids per-node heap allocation and pointer chasing," << std::endl;
    std::cout << "  resulting in better cache locality for depth-20 order books." << std::endl;
    return 0;
}
