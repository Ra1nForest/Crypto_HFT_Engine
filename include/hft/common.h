//
// common.h
// HFT 基础库公共定义：缓存行常量、分支预测提示、自旋等待指令、2 的幂工具函数与调试断言。
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

// 分支预测提示：只影响代码布局，不改变语义
#define HFT_LIKELY(x) (__builtin_expect(!!(x), 1))
#define HFT_UNLIKELY(x) (__builtin_expect(!!(x), 0))

// 强制内联：用于热路径上的小函数
#define HFT_ALWAYS_INLINE inline __attribute__((always_inline))

// 调试断言（默认关闭）。本项目所有构建都是 -O2 且未定义 NDEBUG，标准 assert 会留在热路径里；
// 在包含任何 hft 头文件之前定义 HFT_ENABLE_ASSERTS 即可启用（tests/test_hft.cpp 会启用）。
// 关闭时表达式不求值，但仍参与编译（避免"变量未使用"告警）。
#if defined(HFT_ENABLE_ASSERTS)
#define HFT_ASSERT(cond) \
    ((cond) ? static_cast<void>(0) : ::hft::detail::assertFailed(#cond, __FILE__, __LINE__))
#else
#define HFT_ASSERT(cond) static_cast<void>(sizeof(!(cond)))
#endif

// AddressSanitizer 检测：GCC 定义 __SANITIZE_ADDRESS__，Clang 通过 __has_feature 暴露
#if defined(__SANITIZE_ADDRESS__)
#define HFT_HAS_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define HFT_HAS_ASAN 1
#endif
#endif

// 手动标记内存为"不可访问/可访问"，让 ASan 能发现对已归还对象的访问（非 ASan 构建为空操作）。
// 直接声明 ASan 运行时的公开接口，而不包含 <sanitizer/asan_interface.h>：
// 部分 Clang 发行包不带该头文件，但 -fsanitize=address 链接的运行时总是提供这两个函数。
#if defined(HFT_HAS_ASAN)
extern "C" void __asan_poison_memory_region(void const volatile* addr, std::size_t size);
extern "C" void __asan_unpoison_memory_region(void const volatile* addr, std::size_t size);
#define HFT_ASAN_POISON(addr, size) __asan_poison_memory_region((addr), (size))
#define HFT_ASAN_UNPOISON(addr, size) __asan_unpoison_memory_region((addr), (size))
#else
#define HFT_ASAN_POISON(addr, size) (static_cast<void>(addr), static_cast<void>(size))
#define HFT_ASAN_UNPOISON(addr, size) (static_cast<void>(addr), static_cast<void>(size))
#endif

namespace hft {

// 缓存行大小（x86-64 与主流 ARM 服务器均为 64 字节）
inline constexpr std::size_t kCacheLineSize = 64;

// 自旋等待提示：降低忙等对流水线与超线程兄弟核的干扰
inline void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#endif
}

// 是否为 2 的幂（0 不是）
constexpr bool isPow2(std::size_t n) noexcept {
    return n != 0 && (n & (n - 1)) == 0;
}

// 不小于 n 的最小 2 的幂，结果 >= 1（nextPow2(0) == 1）。
// n 超过 size_t 能表示的最大 2 的幂（64 位下为 2^63）时无法表示，饱和返回该最大值；
// 本库的容器在调用前都会先检查容量上限，不会走到饱和分支。
constexpr std::size_t nextPow2(std::size_t n) noexcept {
    constexpr int kBits = std::numeric_limits<std::size_t>::digits;
    if (n <= 1) return 1;
    const int width = std::numeric_limits<unsigned long long>::digits - __builtin_clzll(n - 1);  // n - 1 的有效位数
    return std::size_t{1} << (width < kBits ? width : kBits - 1);
}

namespace detail {

// HFT_ASSERT 失败：打印位置后中止进程
[[noreturn]] inline void assertFailed(const char* expr, const char* file, int line) noexcept {
    std::fprintf(stderr, "%s:%d: HFT_ASSERT(%s) failed\n", file, line, expr);
    std::fflush(stderr);
    std::abort();
}

}  // namespace detail

}  // namespace hft
