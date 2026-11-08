//
// parse_benchmark.cpp
// 对比 std::stod 与 std::from_chars 的字符串转浮点性能。
//

#include <charconv>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr int kIterations = 1000000;
constexpr int kWarmup = 100000;

// 一轮：把 40 对价格/数量字符串解析成 double 并累加乘积。强制内联：预热与计时两个循环都调用它，
// 不内联时每轮多一次函数调用，测到的就不再是旧版逐行展开的循环
template <class Parse>
__attribute__((always_inline)) inline void parseSnapshot(const std::vector<std::string>& prices,
                                                         const std::vector<std::string>& qtys, Parse& parse,
                                                         volatile double& sink) {
    double sum = 0.0;
    for (int j = 0; j < 40; ++j) {
        const double p = parse(std::string_view(prices[j]));
        const double q = parse(std::string_view(qtys[j]));
        sum += p * q;
    }
    sink = sum;
}

// 预热（避免首次调用带来的噪声）后正式计时
template <class Parse>
void runBenchmark(const char* title, const std::vector<std::string>& prices, const std::vector<std::string>& qtys,
                  Parse parse) {
    volatile double sink = 0.0;  // 防止编译器优化掉计算
    for (int i = 0; i < kWarmup; ++i) parseSnapshot(prices, qtys, parse, sink);
    const auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kIterations; ++i) parseSnapshot(prices, qtys, parse, sink);
    const auto end = std::chrono::high_resolution_clock::now();

    const auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    const double avg_ns = static_cast<double>(total_ns) / kIterations;
    std::cout << title << ":" << std::endl;
    std::cout << "  Total time:       " << std::setw(8) << total_ns / 1000000 << " ms" << std::endl;
    std::cout << "  Avg per snapshot: " << std::setw(8) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
    std::cout << "  Avg per snapshot: " << std::setw(8) << std::setprecision(2) << avg_ns / 1000.0 << " us" << std::endl;
    std::cout << std::endl;
}

}  // namespace

int main() {
    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║    Parse Benchmark: std::stod vs from_chars       ║" << std::endl;
    std::cout << "╠═══════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Iterations: " << std::setw(8) << kIterations << "                             ║" << std::endl;
    std::cout << "║  Each iteration: parse 40 price strings           ║" << std::endl;
    std::cout << "║  (simulating 20 bids + 20 asks depth snapshot)    ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;

    // 生成模拟的价格字符串（类似交易所返回的格式）
    std::mt19937 gen(42);
    std::uniform_real_distribution<> price_dist(68000.0, 72000.0);
    std::uniform_real_distribution<> qty_dist(0.0001, 5.0);
    std::vector<std::string> price_strings(40);
    std::vector<std::string> qty_strings(40);
    for (int i = 0; i < 40; ++i) {
        std::ostringstream ps, qs;
        ps << std::fixed << std::setprecision(2) << price_dist(gen);
        qs << std::fixed << std::setprecision(4) << qty_dist(gen);
        price_strings[i] = ps.str();
        qty_strings[i] = qs.str();
    }

    // 模拟实际代码：string_view → string → stod（每次转换都要构造一个 std::string）
    runBenchmark("std::stod (with std::string allocation)", price_strings, qty_strings,
                 [](std::string_view sv) { return std::stod(std::string(sv)); });
    // 直接在原缓冲区上解析，零分配
    runBenchmark("std::from_chars (zero allocation)", price_strings, qty_strings, [](std::string_view sv) {
        double v = 0.0;
        std::from_chars(sv.data(), sv.data() + sv.size(), v);
        return v;
    });

    std::cout << "═══ Conclusion ═══" << std::endl;
    std::cout << "  std::from_chars avoids heap allocation for each string conversion," << std::endl;
    std::cout << "  eliminating 80 malloc/free calls per depth-20 snapshot." << std::endl;
    return 0;
}
