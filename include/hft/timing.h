//
// timing.h
// 计时与线程工具：单调/墙上时钟、TSC 读数与校准、线程绑核与命名。
// Timing and thread utilities: monotonic/wall clocks, TSC reads and calibration, thread pinning and naming.
//

#pragma once

#include "common.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace hft {

// 单调时钟（steady_clock），纳秒。用于测量时间间隔，不受系统时间调整影响。
// Monotonic clock (steady_clock), in nanoseconds. Used to measure time intervals, unaffected by system
// time adjustments.
inline std::int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 墙上时钟（system_clock），自 Unix 纪元起的毫秒数。用于与交易所时间戳对比。
// Wall clock (system_clock), in milliseconds since the Unix epoch. Used to compare against exchange
// timestamps.
inline std::int64_t wallClockMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// 读取时间戳计数器（x86 RDTSC，约 20 个周期，不排序）；非 x86 平台退化为 nowNs()
// Reads the time stamp counter (x86 RDTSC, about 20 cycles, unordered); falls back to nowNs() on non-x86
// platforms
inline std::uint64_t rdtsc() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    return __rdtsc();
#else
    return static_cast<std::uint64_t>(nowNs());
#endif
}

// 有序读取（x86 RDTSCP：等之前的指令执行完毕后才读数，适合作为测量区间的结束时间戳）。
// 需要 CPU 支持 RDTSCP（2008 年以后的 x86-64 处理器均支持）；非 x86 平台退化为 nowNs()。
// Ordered read (x86 RDTSCP: reads the counter only after all previous instructions have executed, suitable
// as the end timestamp of a measured interval).
// Requires CPU support for RDTSCP (all x86-64 processors since 2008 have it); falls back to nowNs() on
// non-x86 platforms.
inline std::uint64_t rdtscp() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    unsigned int aux = 0;
    return __rdtscp(&aux);
#else
    return static_cast<std::uint64_t>(nowNs());
#endif
}

// TSC 时钟：把 rdtsc() 的计数换算为纳秒。
// - 首次调用 instance() 时以 steady_clock 为基准校准一次（阻塞约 20 ms），之后系数只读。
// - 假设 TSC 恒定频率（Linux /proc/cpuinfo 中的 constant_tsc / nonstop_tsc）；否则换算只是近似值。
// - 线程安全：函数内静态变量的初始化是线程安全的，校准完成之后各线程只读系数。
// TSC clock: converts rdtsc() counts to nanoseconds.
// - The first call to instance() calibrates once against steady_clock (blocking for about 20 ms); after
//   that the factor is read-only.
// - Assumes a constant TSC frequency (constant_tsc / nonstop_tsc in Linux /proc/cpuinfo); otherwise the
//   conversion is only approximate.
// - Thread safety: initialization of a function-local static is thread-safe, and once calibration is done
//   all threads only read the factor.
class TscClock {
public:
    static TscClock& instance() {
        static TscClock clock;
        return clock;
    }

    TscClock(const TscClock&) = delete;
    TscClock& operator=(const TscClock&) = delete;

    // 每个 tick 的纳秒数（非 x86 平台约为 1.0）
    // Nanoseconds per tick (about 1.0 on non-x86 platforms)
    double nsPerTick() const noexcept { return ns_per_tick_; }

    // tick 数换算为纳秒。ticks 按二进制补码解释为有符号数：end - start 因跨核 TSC 偏差
    // 出现小幅"负值"（无符号回绕）时得到小的负纳秒数，而不是一个巨大的正数。
    // Converts a tick count to nanoseconds. ticks is interpreted as a two's complement signed number: when
    // end - start comes out slightly "negative" (unsigned wraparound) because of TSC skew across cores, the
    // result is a small negative number of nanoseconds rather than a huge positive one.
    std::int64_t toNs(std::uint64_t ticks) const noexcept {
        const double ns = static_cast<double>(static_cast<std::int64_t>(ticks)) * ns_per_tick_;
        constexpr double kLimit = 9.2e18;  // 略小于 INT64_MAX，避免 double -> int64 越界
                                           // slightly below INT64_MAX to avoid an out-of-range double -> int64
        if (ns >= kLimit) return std::numeric_limits<std::int64_t>::max();
        if (ns <= -kLimit) return std::numeric_limits<std::int64_t>::min();
        return static_cast<std::int64_t>(ns);
    }

private:
    // 以 steady_clock 为基准在 20 ms 时间窗内校准；TSC 不单调或时钟未前进时保留系数 1.0
    // Calibrates against steady_clock over a 20 ms window; if the TSC is not monotonic or the clock did not
    // advance, the factor stays 1.0
    TscClock() {
        std::uint64_t tsc0 = 0, tsc1 = 0;
        std::int64_t ns0 = 0, ns1 = 0;
        samplePair(tsc0, ns0);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        samplePair(tsc1, ns1);
        if (tsc1 > tsc0 && ns1 > ns0) {
            ns_per_tick_ = static_cast<double>(ns1 - ns0) / static_cast<double>(tsc1 - tsc0);
        }
    }

    // 同时读取一对 (tsc, steady ns)：取 5 次中两次 rdtsc 间隔最小的一次，
    // 用其 TSC 中点与 steady 读数配对，降低被中断/抢占造成的误差
    // Reads a (tsc, steady ns) pair at the same time: out of 5 tries, takes the one with the smallest gap
    // between its two rdtsc calls and pairs its TSC midpoint with the steady reading, reducing the error
    // caused by interrupts/preemption
    static void samplePair(std::uint64_t& tsc, std::int64_t& ns) noexcept {
        std::uint64_t best = std::numeric_limits<std::uint64_t>::max();
        for (int i = 0; i < 5; ++i) {
            const std::uint64_t a = rdtsc();
            const std::int64_t t = nowNs();
            const std::uint64_t b = rdtsc();
            if (b >= a && b - a < best) {
                best = b - a;
                tsc = a + (b - a) / 2;
                ns = t;
            }
        }
        if (best == std::numeric_limits<std::uint64_t>::max()) {  // 每次 TSC 都倒退（极端情况）
                                                                  // TSC went backwards every time (extreme case)
            tsc = rdtsc();
            ns = nowNs();
        }
    }

    double ns_per_tick_ = 1.0;
};

// 把当前线程绑定到指定 CPU（Linux pthread_setaffinity_np）；失败或非 Linux 平台返回 false
// Pins the current thread to the given CPU (Linux pthread_setaffinity_np); returns false on failure or on
// non-Linux platforms
inline bool pinThreadToCpu(int cpu) noexcept {
#if defined(__linux__)
    if (cpu < 0 || cpu >= CPU_SETSIZE) return false;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<std::size_t>(cpu), &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)cpu;
    return false;
#endif
}

// 设置当前线程名（Linux 上限 15 个字符，超出部分截断）；nullptr 或非 Linux 平台为空操作
// Sets the current thread's name (Linux allows at most 15 characters, the rest is truncated); a no-op for
// nullptr or on non-Linux platforms
inline void setThreadName(const char* name) noexcept {
#if defined(__linux__)
    if (name == nullptr) return;
    char buf[16];  // 15 个字符 + 结尾的 '\0'
                   // 15 characters + the terminating '\0'
    const std::size_t n = ::strnlen(name, sizeof(buf) - 1);
    std::memcpy(buf, name, n);
    buf[n] = '\0';
    pthread_setname_np(pthread_self(), buf);
#else
    (void)name;
#endif
}

}  // namespace hft
