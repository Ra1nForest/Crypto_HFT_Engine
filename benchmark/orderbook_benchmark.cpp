#include <iostream>
#include <iomanip>
#include <chrono>
#include <map>
#include <vector>
#include <random>
#include <string>

class MapOrderBook {
public:
    MapOrderBook(const std::string& symbol) : symbol_(symbol) {}
 
    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }
 
    void updateBid(double price, double quantity) {
        if (quantity == 0.0) bids_.erase(price);
        else bids_[price] = quantity;
    }
 
    void updateAsk(double price, double quantity) {
        if (quantity == 0.0) asks_.erase(price);
        else asks_[price] = quantity;
    }
 
    double getBestBid() const {
        if (bids_.empty()) return 0.0;
        return bids_.rbegin()->first;
    }
 
    double getBestAsk() const {
        if (asks_.empty()) return 0.0;
        return asks_.begin()->first;
    }
 
    double getSpread() const {
        return getBestAsk() - getBestBid();
    }

    double sumBids() const {
        double sum = 0.0;
        for (auto& [price, qty] : bids_) sum += price * qty;
        return sum;
    }
 
    double sumAsks() const {
        double sum = 0.0;
        for (auto& [price, qty] : asks_) sum += price * qty;
        return sum;
    }
 
private:
    std::string symbol_;
    std::map<double, double> bids_;
    std::map<double, double> asks_;
};

struct PriceLevel {
    double price;
    double quantity;
};
 
class VecOrderBook {
public:
    VecOrderBook(const std::string& symbol) : symbol_(symbol) {
        bids_.reserve(20);
        asks_.reserve(20);
    }
 
    void clearBids() { bids_.clear(); }
    void clearAsks() { asks_.clear(); }
 
    void updateBid(double price, double quantity) {
        bids_.push_back({price, quantity});
    }
 
    void updateAsk(double price, double quantity) {
        asks_.push_back({price, quantity});
    }
 
    double getBestBid() const {
        if (bids_.empty()) return 0.0;
        return bids_[0].price;
    }
 
    double getBestAsk() const {
        if (asks_.empty()) return 0.0;
        return asks_[0].price;
    }
 
    double getSpread() const {
        return getBestAsk() - getBestBid();
    }

    double sumBids() const {
        double sum = 0.0;
        for (auto& level : bids_) sum += level.price * level.quantity;
        return sum;
    }
 
    double sumAsks() const {
        double sum = 0.0;
        for (auto& level : asks_) sum += level.price * level.quantity;
        return sum;
    }
 
private:
    std::string symbol_;
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;
};

struct DepthSnapshot {
    double bid_prices[20];
    double bid_qtys[20];
    double ask_prices[20];
    double ask_qtys[20];
};
 
std::vector<DepthSnapshot> generateSnapshots(int count) {
    std::mt19937 gen(42);  // 固定种子，结果可复现
    std::uniform_real_distribution<> price_dist(69000.0, 71000.0);
    std::uniform_real_distribution<> qty_dist(0.001, 5.0);
 
    std::vector<DepthSnapshot> snapshots(count);
    for (int i = 0; i < count; ++i) {
        double mid = price_dist(gen);
        for (int j = 0; j < 20; ++j) {
            snapshots[i].bid_prices[j] = mid - j * 0.01;  // 买盘从高到低
            snapshots[i].bid_qtys[j] = qty_dist(gen);
            snapshots[i].ask_prices[j] = mid + 0.01 + j * 0.01;  // 卖盘从低到高
            snapshots[i].ask_qtys[j] = qty_dist(gen);
        }
    }
    return snapshots;
}
 
int main() {
    const int ITERATIONS = 100000;
    const int WARMUP = 10000;
 
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║       OrderBook Benchmark: map vs vector          ║" << std::endl;
    std::cout << "╠═══════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Iterations: " << std::setw(8) << ITERATIONS << "                             ║" << std::endl;
    std::cout << "║  Depth: 20 levels per side                        ║" << std::endl;
    std::cout << "║  Operations: clear + 40 inserts + spread + scan   ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
 
    auto snapshots = generateSnapshots(ITERATIONS + WARMUP);
 
    // ===== Benchmark: std::map OrderBook =====
    {
        MapOrderBook book("MAP:BTCUSDT");
        volatile double sink = 0.0;  // 防止编译器优化掉计算
 
        // Warmup
        for (int i = 0; i < WARMUP; ++i) {
            book.clearBids();
            book.clearAsks();
            for (int j = 0; j < 20; ++j) {
                book.updateBid(snapshots[i].bid_prices[j], snapshots[i].bid_qtys[j]);
                book.updateAsk(snapshots[i].ask_prices[j], snapshots[i].ask_qtys[j]);
            }
            sink = book.getSpread() + book.sumBids() + book.sumAsks();
        }
 
        // Timed run
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = WARMUP; i < WARMUP + ITERATIONS; ++i) {
            book.clearBids();
            book.clearAsks();
            for (int j = 0; j < 20; ++j) {
                book.updateBid(snapshots[i].bid_prices[j], snapshots[i].bid_qtys[j]);
                book.updateAsk(snapshots[i].ask_prices[j], snapshots[i].ask_qtys[j]);
            }
            sink = book.getSpread() + book.sumBids() + book.sumAsks();
        }
        auto end = std::chrono::high_resolution_clock::now();
 
        auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        double avg_ns = static_cast<double>(total_ns) / ITERATIONS;
 
        std::cout << "std::map OrderBook:" << std::endl;
        std::cout << "  Total time:    " << std::setw(10) << total_ns / 1000000 << " ms" << std::endl;
        std::cout << "  Avg per update:" << std::setw(10) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
        std::cout << "  Avg per update:" << std::setw(10) << std::setprecision(2) << avg_ns / 1000.0 << " μs" << std::endl;
        std::cout << std::endl;
    }
 
    // ===== Benchmark: flat vector OrderBook =====
    {
        VecOrderBook book("VEC:BTCUSDT");
        volatile double sink = 0.0;
 
        // Warmup
        for (int i = 0; i < WARMUP; ++i) {
            book.clearBids();
            book.clearAsks();
            for (int j = 0; j < 20; ++j) {
                book.updateBid(snapshots[i].bid_prices[j], snapshots[i].bid_qtys[j]);
                book.updateAsk(snapshots[i].ask_prices[j], snapshots[i].ask_qtys[j]);
            }
            sink = book.getSpread() + book.sumBids() + book.sumAsks();
        }
 
        // Timed run
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = WARMUP; i < WARMUP + ITERATIONS; ++i) {
            book.clearBids();
            book.clearAsks();
            for (int j = 0; j < 20; ++j) {
                book.updateBid(snapshots[i].bid_prices[j], snapshots[i].bid_qtys[j]);
                book.updateAsk(snapshots[i].ask_prices[j], snapshots[i].ask_qtys[j]);
            }
            sink = book.getSpread() + book.sumBids() + book.sumAsks();
        }
        auto end = std::chrono::high_resolution_clock::now();
 
        auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        double avg_ns = static_cast<double>(total_ns) / ITERATIONS;
 
        std::cout << "flat vector OrderBook:" << std::endl;
        std::cout << "  Total time:    " << std::setw(10) << total_ns / 1000000 << " ms" << std::endl;
        std::cout << "  Avg per update:" << std::setw(10) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
        std::cout << "  Avg per update:" << std::setw(10) << std::setprecision(2) << avg_ns / 1000.0 << " μs" << std::endl;
        std::cout << std::endl;
    }
 
    std::cout << "═══ Conclusion ═══" << std::endl;
    std::cout << "  flat vector avoids per-node heap allocation and pointer chasing," << std::endl;
    std::cout << "  resulting in better cache locality for depth-20 order books." << std::endl;
 
    return 0;
}