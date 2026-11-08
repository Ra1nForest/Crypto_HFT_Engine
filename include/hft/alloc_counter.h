//
// alloc_counter.h
// 计数版全局 operator new/delete：统计堆分配次数，用于验证热路径零分配（测试、基准与模拟器共用）。
// 替换的是整个程序的全局分配函数，因此每个可执行文件只能有一个翻译单元包含本头文件
// （本项目的测试、基准与 exchange_sim 都是单翻译单元程序）。
// Counting global operator new/delete: counts heap allocations to verify that hot paths are
// allocation-free (shared by the tests, the benchmarks and the simulator).
// It replaces the global allocation functions of the whole program, so only one translation unit per
// executable may include this header (this project's tests, benchmarks and exchange_sim are all
// single-translation-unit programs).
//

#pragma once

#include <pthread.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>

namespace hft::alloc_counter {

// 程序启动以来全部线程的分配次数（一直计数）
// Allocations by all threads since program start (always counted)
inline std::atomic<std::uint64_t> g_total{0};

// 计数窗口：g_window_on 为 true 时把分配计入 g_window_count；
// g_window_thread 非 0 时只统计该线程（pthread_self() 的整数值）
// Counting window: while g_window_on is true, allocations are added to g_window_count;
// when g_window_thread is non-zero, only that thread is counted (the integer value of pthread_self())
inline std::atomic<bool> g_window_on{false};
inline std::atomic<std::uint64_t> g_window_thread{0};
inline std::atomic<std::uint64_t> g_window_count{0};

inline std::uint64_t threadId() noexcept { return static_cast<std::uint64_t>(pthread_self()); }

inline std::uint64_t total() noexcept { return g_total.load(std::memory_order_relaxed); }

// 开始一个计数窗口（清零）；only_thread 为 0 表示统计所有线程
// Start a counting window (resets the count); only_thread = 0 means all threads are counted
inline void startWindow(std::uint64_t only_thread = 0) noexcept {
    g_window_count.store(0, std::memory_order_relaxed);
    g_window_thread.store(only_thread, std::memory_order_relaxed);
    g_window_on.store(true, std::memory_order_seq_cst);
}

// 结束窗口并返回窗口内的分配次数
// End the window and return the number of allocations inside it
inline std::uint64_t stopWindow() noexcept {
    g_window_on.store(false, std::memory_order_seq_cst);
    return g_window_count.load(std::memory_order_relaxed);
}

namespace detail {

inline void note() noexcept {
    g_total.fetch_add(1, std::memory_order_relaxed);
    if (g_window_on.load(std::memory_order_relaxed)) {
        const std::uint64_t only = g_window_thread.load(std::memory_order_relaxed);
        if (only == 0 || only == threadId()) g_window_count.fetch_add(1, std::memory_order_relaxed);
    }
}

// 计数并分配 n 字节，失败返回 nullptr。align 为 0 时用 malloc（默认对齐），
// 否则用 aligned_alloc（它要求大小是对齐的整数倍，因此向上取整）
// Count and allocate n bytes; returns nullptr on failure. With align 0 it uses malloc (default alignment),
// otherwise aligned_alloc (which requires the size to be a multiple of the alignment, so it is rounded up)
inline void* allocate(std::size_t n, std::size_t align) noexcept {
    note();
    if (align == 0) return std::malloc(n == 0 ? 1 : n);
    return std::aligned_alloc(align, n == 0 ? align : (n + align - 1) / align * align);
}

inline void* allocateOrThrow(std::size_t n, std::size_t align) {
    if (void* p = allocate(n, align)) return p;
    throw std::bad_alloc();
}

}  // namespace detail
}  // namespace hft::alloc_counter

// 替换全部可替换的分配函数形式（普通 / nothrow / 对齐 / 对齐 + nothrow 及对应的 delete），
// 保证同一块内存总由同一对分配函数申请和释放（ASan 会检查 alloc-dealloc 是否匹配）。
// noinline：-O1（sanitizer 构建）下 GCC 若把它们内联进调用点，会对"operator new 的结果被 free"误报
// -Wmismatched-new-delete；底层本来就是 malloc/aligned_alloc 与 free 配对。
// 注意：标准规定替换的分配函数不得声明为 inline，所以它们是普通的非内联定义。
// Replace every replaceable form of the allocation functions (plain / nothrow / aligned / aligned +
// nothrow and the matching deletes), so a block of memory is always allocated and freed by the same pair
// of functions (ASan checks that alloc and dealloc match).
// noinline: at -O1 (sanitizer builds), if GCC inlined them into the call sites, it would falsely report
// -Wmismatched-new-delete for "the result of operator new is freed"; underneath, malloc/aligned_alloc and
// free are paired anyway.
// Note: the standard says replacement allocation functions must not be declared inline, so these are
// ordinary non-inline definitions.
__attribute__((noinline)) void* operator new(std::size_t n) {
    return hft::alloc_counter::detail::allocateOrThrow(n, 0);
}
__attribute__((noinline)) void* operator new[](std::size_t n) {
    return hft::alloc_counter::detail::allocateOrThrow(n, 0);
}
__attribute__((noinline)) void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    return hft::alloc_counter::detail::allocate(n, 0);
}
__attribute__((noinline)) void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    return hft::alloc_counter::detail::allocate(n, 0);
}
__attribute__((noinline)) void* operator new(std::size_t n, std::align_val_t a) {
    return hft::alloc_counter::detail::allocateOrThrow(n, static_cast<std::size_t>(a));
}
__attribute__((noinline)) void* operator new[](std::size_t n, std::align_val_t a) {
    return hft::alloc_counter::detail::allocateOrThrow(n, static_cast<std::size_t>(a));
}
__attribute__((noinline)) void* operator new(std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return hft::alloc_counter::detail::allocate(n, static_cast<std::size_t>(a));
}
__attribute__((noinline)) void* operator new[](std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
    return hft::alloc_counter::detail::allocate(n, static_cast<std::size_t>(a));
}
__attribute__((noinline)) void operator delete(void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::size_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(p);
}
__attribute__((noinline)) void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
    std::free(p);
}
