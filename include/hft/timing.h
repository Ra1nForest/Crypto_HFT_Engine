//
// timing.h
// 计时与线程工具：单调/墙上时钟、TSC 读数与校准、线程绑核与命名。
//

#pragma once

#include "common.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
inline std::int64_t nowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 墙上时钟（system_clock），自 Unix 纪元起的毫秒数。用于与交易所时间戳对比。
inline std::int64_t wallClockMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// 读取时间戳计数器（x86 RDTSC，约 20 个周期，不排序）；非 x86 平台退化为 nowNs()
inline std::uint64_t rdtsc() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    return __rdtsc();
#else
    return static_cast<std::uint64_t>(nowNs());
#endif
}

// 有序读取（x86 RDTSCP：等之前的指令执行完毕后才读数，适合作为测量区间的结束时间戳）。
// 需要 CPU 支持 RDTSCP（2008 年以后的 x86-64 处理器均支持）；非 x86 平台退化为 nowNs()。
inline std::uint64_t rdtscp() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    unsigned int aux = 0;
    return __rdtscp(&aux);
#else
    return static_cast<std::uint64_t>(nowNs());
#endif
}

// TSC 时钟：把 rdtsc() 的计数换算为纳秒。
// - 首次调用 instance() 时以 steady_clock 为基准自动校准（默认 20 ms 窗口，此后可再次 calibrate()）。
// - 假设 TSC 恒定频率（Linux /proc/cpuinfo 中的 constant_tsc / nonstop_tsc）；否则换算只是近似值。
// - 线程安全：系数存放在原子变量中，calibrate() 可与 toNs() 并发调用。
class TscClock {
public:
    static TscClock& instance() {
        static TscClock clock;  // C++11 起函数内静态变量的初始化是线程安全的
        return clock;
    }

    TscClock(const TscClock&) = delete;
    TscClock& operator=(const TscClock&) = delete;

    // 每个 tick 的纳秒数（非 x86 平台约为 1.0）
    double nsPerTick() const noexcept { return ns_per_tick_.load(std::memory_order_relaxed); }

    // tick 数换算为纳秒。ticks 按二进制补码解释为有符号数：end - start 因跨核 TSC 偏差
    // 出现小幅"负值"（无符号回绕）时得到小的负纳秒数，而不是一个巨大的正数。
    std::int64_t toNs(std::uint64_t ticks) const noexcept {
        const double ns = static_cast<double>(static_cast<std::int64_t>(ticks)) * nsPerTick();
        constexpr double kLimit = 9.2e18;  // 略小于 INT64_MAX，避免 double -> int64 越界
        if (ns >= kLimit) return std::numeric_limits<std::int64_t>::max();
        if (ns <= -kLimit) return std::numeric_limits<std::int64_t>::min();
        return static_cast<std::int64_t>(ns);
    }

    // 以 steady_clock 为基准在 window 时间窗内重新校准（阻塞约 window 时长）
    void calibrate(std::chrono::milliseconds window = std::chrono::milliseconds(20)) {
        if (window < std::chrono::milliseconds(1)) window = std::chrono::milliseconds(1);
        std::uint64_t tsc0 = 0, tsc1 = 0;
        std::int64_t ns0 = 0, ns1 = 0;
        samplePair(tsc0, ns0);
        std::this_thread::sleep_for(window);
        samplePair(tsc1, ns1);
        if (tsc1 > tsc0 && ns1 > ns0) {
            ns_per_tick_.store(static_cast<double>(ns1 - ns0) / static_cast<double>(tsc1 - tsc0),
                               std::memory_order_relaxed);
        }
        // 否则（TSC 不单调或时钟未前进）保留原系数
    }

private:
    TscClock() { calibrate(); }

    // 同时读取一对 (tsc, steady ns)：取 5 次中两次 rdtsc 间隔最小的一次，
    // 用其 TSC 中点与 steady 读数配对，降低被中断/抢占造成的误差
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
            tsc = rdtsc();
            ns = nowNs();
        }
    }

    std::atomic<double> ns_per_tick_{1.0};
};

// 把当前线程绑定到指定 CPU（Linux pthread_setaffinity_np）；失败或非 Linux 平台返回 false
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
inline void setThreadName(const char* name) noexcept {
#if defined(__linux__)
    if (name == nullptr) return;
    char buf[16];
    std::size_t n = 0;
    while (n < sizeof(buf) - 1 && name[n] != '\0') {
        buf[n] = name[n];
        ++n;
    }
    buf[n] = '\0';
    pthread_setname_np(pthread_self(), buf);
#else
    (void)name;
#endif
}

}  // namespace hft
