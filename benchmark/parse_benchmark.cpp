#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>
#include <string_view>
#include <charconv>
#include <vector>
#include <random>
#include <sstream>

int main() {
    const int ITERATIONS = 1000000;
    const int WARMUP = 100000;

    std::cout << "╔═══════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║    Parse Benchmark: std::stod vs from_chars       ║" << std::endl;
    std::cout << "╠═══════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Iterations: " << std::setw(8) << ITERATIONS << "                             ║" << std::endl;
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

    // ===== Benchmark: std::stod（需要构造std::string）=====
    {
        volatile double sink = 0.0;

        // Warmup
        for (int i = 0; i < WARMUP; ++i) {
            double sum = 0.0;
            for (int j = 0; j < 40; ++j) {
                std::string_view psv(price_strings[j]);
                std::string_view qsv(qty_strings[j]);
                // 模拟实际代码：string_view → string → stod
                double p = std::stod(std::string(psv));
                double q = std::stod(std::string(qsv));
                sum += p * q;
            }
            sink = sum;
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; ++i) {
            double sum = 0.0;
            for (int j = 0; j < 40; ++j) {
                std::string_view psv(price_strings[j]);
                std::string_view qsv(qty_strings[j]);
                double p = std::stod(std::string(psv));
                double q = std::stod(std::string(qsv));
                sum += p * q;
            }
            sink = sum;
        }
        auto end = std::chrono::high_resolution_clock::now();

        auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        double avg_ns = static_cast<double>(total_ns) / ITERATIONS;

        std::cout << "std::stod (with std::string allocation):" << std::endl;
        std::cout << "  Total time:       " << std::setw(8) << total_ns / 1000000 << " ms" << std::endl;
        std::cout << "  Avg per snapshot: " << std::setw(8) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
        std::cout << "  Avg per snapshot: " << std::setw(8) << std::setprecision(2) << avg_ns / 1000.0 << " us" << std::endl;
        std::cout << std::endl;
    }

    // ===== Benchmark: std::from_chars（零分配）=====
    {
        volatile double sink = 0.0;

        // Warmup
        for (int i = 0; i < WARMUP; ++i) {
            double sum = 0.0;
            for (int j = 0; j < 40; ++j) {
                std::string_view psv(price_strings[j]);
                std::string_view qsv(qty_strings[j]);
                double p = 0.0, q = 0.0;
                std::from_chars(psv.data(), psv.data() + psv.size(), p);
                std::from_chars(qsv.data(), qsv.data() + qsv.size(), q);
                sum += p * q;
            }
            sink = sum;
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; ++i) {
            double sum = 0.0;
            for (int j = 0; j < 40; ++j) {
                std::string_view psv(price_strings[j]);
                std::string_view qsv(qty_strings[j]);
                double p = 0.0, q = 0.0;
                std::from_chars(psv.data(), psv.data() + psv.size(), p);
                std::from_chars(qsv.data(), qsv.data() + qsv.size(), q);
                sum += p * q;
            }
            sink = sum;
        }
        auto end = std::chrono::high_resolution_clock::now();

        auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        double avg_ns = static_cast<double>(total_ns) / ITERATIONS;

        std::cout << "std::from_chars (zero allocation):" << std::endl;
        std::cout << "  Total time:       " << std::setw(8) << total_ns / 1000000 << " ms" << std::endl;
        std::cout << "  Avg per snapshot: " << std::setw(8) << std::fixed << std::setprecision(0) << avg_ns << " ns" << std::endl;
        std::cout << "  Avg per snapshot: " << std::setw(8) << std::setprecision(2) << avg_ns / 1000.0 << " us" << std::endl;
        std::cout << std::endl;
    }

    std::cout << "═══ Conclusion ═══" << std::endl;
    std::cout << "  std::from_chars avoids heap allocation for each string conversion," << std::endl;
    std::cout << "  eliminating 80 malloc/free calls per depth-20 snapshot." << std::endl;

    return 0;
}