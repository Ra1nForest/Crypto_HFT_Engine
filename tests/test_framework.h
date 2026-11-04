//
// test_framework.h
// 极简的头文件测试框架：零依赖，提供用例注册、断言宏与运行器。
// 用法：
//   TEST_CASE("orderbook: upsert keeps order") { CHECK_EQ(1 + 1, 2); }
//   int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
// 运行参数：
//   <exe> [子串过滤...]   只运行名称包含任一子串的用例
//   <exe> --list         列出所有用例
//

#pragma once

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace hft_test {

// 单个测试用例
struct TestCase {
    std::string name;
    std::function<void()> fn;
    const char* file;
    int line;
};

// 全局用例注册表（函数内静态变量，避免静态初始化顺序问题）
inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> cases;
    return cases;
}

// 当前用例的失败计数
inline int& currentFailures() {
    static int failures = 0;
    return failures;
}

// 动态注册用例（模板化测试套件可在运行前批量注册）
inline void registerTest(std::string name, std::function<void()> fn,
                         const char* file = "", int line = 0) {
    registry().push_back({std::move(name), std::move(fn), file, line});
}

// 静态注册辅助对象：TEST_CASE 宏使用
struct Registrar {
    Registrar(const char* name, void (*fn)(), const char* file, int line) {
        registerTest(name, fn, file, line);
    }
};

// REQUIRE 失败时抛出，终止当前用例
struct RequireFailure : std::exception {
    const char* what() const noexcept override { return "REQUIRE failed"; }
};

// 将任意值转成字符串用于失败信息；不可输出的类型显示占位符
template <class T, class = void>
struct IsStreamable : std::false_type {};
template <class T>
struct IsStreamable<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

template <class T>
std::string toString(const T& v) {
    if constexpr (std::is_enum_v<T>) {
        return std::to_string(static_cast<long long>(v));
    } else if constexpr (std::is_same_v<T, bool>) {
        return v ? "true" : "false";
    } else if constexpr (IsStreamable<T>::value) {
        std::ostringstream ss;
        ss.precision(17);
        ss << v;
        return ss.str();
    } else {
        return "<unprintable>";
    }
}

// 记录一次断言失败
inline void reportFailure(const char* file, int line, const std::string& msg) {
    ++currentFailures();
    std::cerr << "    " << file << ":" << line << ": FAILED: " << msg << std::endl;
}

// 运行所有（或匹配过滤条件的）用例，返回进程退出码
inline int runAll(int argc, char** argv) {
    std::vector<std::string> filters;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--list") == 0) {
            for (auto& tc : registry()) std::cout << tc.name << "\n";
            return 0;
        }
        filters.emplace_back(argv[i]);
    }

    int run = 0, failed = 0;
    auto total_start = std::chrono::steady_clock::now();
    // 按下标遍历并拷贝用例：允许用例在运行期间动态注册新用例而不使迭代器失效
    for (std::size_t idx = 0; idx < registry().size(); ++idx) {
        const TestCase tc = registry()[idx];
        if (!filters.empty()) {
            bool match = false;
            for (auto& f : filters) {
                if (tc.name.find(f) != std::string::npos) { match = true; break; }
            }
            if (!match) continue;
        }
        ++run;
        currentFailures() = 0;
        auto start = std::chrono::steady_clock::now();
        try {
            tc.fn();
        } catch (const RequireFailure&) {
            // 失败信息已在断言处输出
        } catch (const std::exception& e) {
            reportFailure(tc.file, tc.line, std::string("unexpected exception: ") + e.what());
        } catch (...) {
            reportFailure(tc.file, tc.line, "unexpected non-std exception");
        }
        auto ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - start).count();
        if (currentFailures() > 0) {
            ++failed;
            std::cout << "[FAIL] " << tc.name << " (" << currentFailures() << " failure(s))" << std::endl;
        } else {
            std::printf("[ OK ] %s (%.1f ms)\n", tc.name.c_str(), ms);
            std::fflush(stdout);
        }
    }
    auto total_ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - total_start).count();
    std::printf("\n%d test(s) run, %d failed, %.1f ms\n", run, failed, total_ms);
    if (run == 0) {
        std::printf("No tests matched.\n");
        return 1;
    }
    return failed == 0 ? 0 : 1;
}

}  // namespace hft_test

#define HFT_TEST_CONCAT_INNER(a, b) a##b
#define HFT_TEST_CONCAT(a, b) HFT_TEST_CONCAT_INNER(a, b)

// 定义并注册一个测试用例
#define TEST_CASE(name)                                                              \
    static void HFT_TEST_CONCAT(hft_test_fn_, __LINE__)();                           \
    static ::hft_test::Registrar HFT_TEST_CONCAT(hft_test_reg_, __LINE__)(           \
        name, &HFT_TEST_CONCAT(hft_test_fn_, __LINE__), __FILE__, __LINE__);         \
    static void HFT_TEST_CONCAT(hft_test_fn_, __LINE__)()

// 断言：失败后继续执行
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) ::hft_test::reportFailure(__FILE__, __LINE__, "CHECK(" #cond ")"); \
    } while (0)

// 断言：失败后终止当前用例
#define REQUIRE(cond)                                                                \
    do {                                                                             \
        if (!(cond)) {                                                               \
            ::hft_test::reportFailure(__FILE__, __LINE__, "REQUIRE(" #cond ")");     \
            throw ::hft_test::RequireFailure();                                      \
        }                                                                            \
    } while (0)

#define HFT_TEST_CMP_IMPL(a, b, op, fatal)                                           \
    do {                                                                             \
        const auto& hft_test_a_ = (a);                                               \
        const auto& hft_test_b_ = (b);                                               \
        if (!(hft_test_a_ op hft_test_b_)) {                                         \
            ::hft_test::reportFailure(__FILE__, __LINE__,                            \
                std::string(#a " " #op " " #b "  (") +                               \
                ::hft_test::toString(hft_test_a_) + " vs " +                         \
                ::hft_test::toString(hft_test_b_) + ")");                            \
            if (fatal) throw ::hft_test::RequireFailure();                           \
        }                                                                            \
    } while (0)

#define CHECK_EQ(a, b) HFT_TEST_CMP_IMPL(a, b, ==, false)
#define CHECK_NE(a, b) HFT_TEST_CMP_IMPL(a, b, !=, false)
#define CHECK_LT(a, b) HFT_TEST_CMP_IMPL(a, b, <, false)
#define CHECK_LE(a, b) HFT_TEST_CMP_IMPL(a, b, <=, false)
#define CHECK_GT(a, b) HFT_TEST_CMP_IMPL(a, b, >, false)
#define CHECK_GE(a, b) HFT_TEST_CMP_IMPL(a, b, >=, false)
#define REQUIRE_EQ(a, b) HFT_TEST_CMP_IMPL(a, b, ==, true)
#define REQUIRE_NE(a, b) HFT_TEST_CMP_IMPL(a, b, !=, true)

// 浮点近似比较
#define CHECK_NEAR(a, b, eps)                                                        \
    do {                                                                             \
        const double hft_test_x_ = static_cast<double>(a);                           \
        const double hft_test_y_ = static_cast<double>(b);                           \
        if (!(std::fabs(hft_test_x_ - hft_test_y_) <= (eps))) {                      \
            ::hft_test::reportFailure(__FILE__, __LINE__,                            \
                std::string("CHECK_NEAR(" #a ", " #b ", " #eps ")  (") +             \
                ::hft_test::toString(hft_test_x_) + " vs " +                         \
                ::hft_test::toString(hft_test_y_) + ")");                            \
        }                                                                            \
    } while (0)

// 期望表达式抛出异常
#define CHECK_THROWS(expr)                                                           \
    do {                                                                             \
        bool hft_test_threw_ = false;                                                \
        try { (void)(expr); } catch (...) { hft_test_threw_ = true; }                \
        if (!hft_test_threw_)                                                        \
            ::hft_test::reportFailure(__FILE__, __LINE__, "CHECK_THROWS(" #expr ")"); \
    } while (0)

// 无条件失败（附带说明）
#define FAIL(msg)                                                                    \
    do {                                                                             \
        ::hft_test::reportFailure(__FILE__, __LINE__, std::string("FAIL: ") + (msg)); \
        throw ::hft_test::RequireFailure();                                          \
    } while (0)
