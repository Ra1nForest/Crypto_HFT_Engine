//
// latency_tracker.h
// 延迟统计器：以纳秒存入 hft::LatencyHistogram（HDR 直方图），对外保留原有的微秒接口并新增纳秒接口。
//

#pragma once

#include "hft/latency_histogram.h"

#include <chrono>
#include <climits>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

// 延迟统计器：
// - 内部存储纳秒，相对误差 <= 1/64（< 128 ns 精确）；统计覆盖构造（或 reset）以来的全部样本。
//   旧实现只对最近 10000 个样本求分位数，reservoir_size 参数保留但已忽略。
// - 线程安全：一个写者线程调用 record* / reset，任意线程可同时读取统计值，没有数据竞争
//   （旧实现中行情线程写 vector、主线程同时读，存在数据竞争）。
// - 旧的 long 微秒接口（getP50/getAvg 等）语义不变：单位为微秒，由纳秒统计值四舍五入到最近的微秒。
//   通过 record(long us) 写入的整数微秒样本在 64 µs 以内可精确还原；更大时误差不超过桶宽（约 1/64）。
class LatencyTracker {
public:
    // name: 用于展示的标签；reservoir_size: 兼容旧接口，已忽略
    LatencyTracker(const std::string& name, int reservoir_size = 10000) : name_(name) {
        (void)reservoir_size;
    }

    // 记录单次延迟（微秒），负值忽略
    void record(long latency_us) noexcept {
        if (latency_us < 0) return;
        const auto us = static_cast<std::uint64_t>(latency_us);
        constexpr std::uint64_t kMaxUs = std::numeric_limits<std::uint64_t>::max() / 1000;
        recordNs(us > kMaxUs ? std::numeric_limits<std::uint64_t>::max() : us * 1000);
    }

    // 记录时间点区间延迟（任意时钟，如 high_resolution_clock / steady_clock；两端精度可不同），
    // 按纳秒存储；end < start 时忽略
    template <class Clock, class Duration1, class Duration2>
    void record(std::chrono::time_point<Clock, Duration1> start,
                std::chrono::time_point<Clock, Duration2> end) noexcept {
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        if (ns < 0) return;
        recordNs(static_cast<std::uint64_t>(ns));
    }

    // 记录单次延迟（纳秒）
    void recordNs(std::uint64_t latency_ns) noexcept { hist_.record(latency_ns); }

    // ---------------- 兼容接口：微秒，long ----------------

    // 分位数（0~1），四舍五入到微秒
    long getPercentile(double percentile) const { return nsToUs(hist_.percentile(percentile)); }

    long getP50() const { return getPercentile(0.50); }
    long getP95() const { return getPercentile(0.95); }
    long getP99() const { return getPercentile(0.99); }
    long getMin() const { return nsToUs(hist_.min()); }
    long getMax() const { return nsToUs(hist_.max()); }
    long getAvg() const { return static_cast<long>(std::llround(hist_.mean() / 1000.0)); }
    // 样本数（超过 INT_MAX 时饱和）
    int getCount() const {
        const std::uint64_t n = hist_.count();
        return n > static_cast<std::uint64_t>(INT_MAX) ? INT_MAX : static_cast<int>(n);
    }
    const std::string& getName() const { return name_; }

    // 紧凑统计字符串（用于 UI 面板），格式与旧实现一致
    std::string getCompactStats() const {
        std::ostringstream ss;
        ss << std::setw(6) << getCount() << " msgs"
           << "  Avg:" << std::setw(4) << getAvg() << "μs"
           << "  P50:" << std::setw(4) << getP50() << "μs"
           << "  P99:" << std::setw(4) << getP99() << "μs"
           << "  Max:" << std::setw(4) << getMax() << "μs";
        return ss.str();
    }

    // 重置统计（只能由写者线程调用）
    void reset() noexcept { hist_.reset(); }

    // ---------------- 纳秒接口 ----------------

    std::uint64_t count() const noexcept { return hist_.count(); }
    std::uint64_t percentileNs(double q) const noexcept { return hist_.percentile(q); }
    std::uint64_t minNs() const noexcept { return hist_.min(); }
    std::uint64_t maxNs() const noexcept { return hist_.max(); }
    double meanNs() const noexcept { return hist_.mean(); }

    // 延迟 < ns 的样本占比（0~1，桶分辨率）；无样本时为 0
    double fractionBelowNs(std::uint64_t ns) const noexcept {
        const std::uint64_t n = hist_.count();
        if (n == 0) return 0.0;
        const double f = static_cast<double>(hist_.countBelow(ns)) / static_cast<double>(n);
        return f > 1.0 ? 1.0 : f;  // 并发写入时 countBelow 可能比先读到的 count 新
    }

    // 微秒（double）
    double getAvgUs() const noexcept { return hist_.mean() / 1000.0; }
    double getP50Us() const noexcept { return static_cast<double>(hist_.percentile(0.50)) / 1000.0; }
    double getP99Us() const noexcept { return static_cast<double>(hist_.percentile(0.99)) / 1000.0; }

    // 底层直方图（只读）
    const hft::LatencyHistogram& histogram() const noexcept { return hist_; }

private:
    // 纳秒 -> 微秒，四舍五入（避免 ns + 500 溢出）
    static long nsToUs(std::uint64_t ns) noexcept {
        return static_cast<long>(ns / 1000 + (ns % 1000 >= 500 ? 1 : 0));
    }

    // 展示名称（构造后不变，可被多线程同时读取）
    std::string name_;
    // 纳秒直方图
    hft::LatencyHistogram hist_;
};
