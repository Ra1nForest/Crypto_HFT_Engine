//
// latency_tracker.h
// 延迟统计器：带展示名称的纳秒 hft::LatencyHistogram（HDR 直方图），供单个线程写、界面与日志读。
// Latency tracker: a nanosecond hft::LatencyHistogram (HDR histogram) with a display name, written
// by a single thread and read by the UI and logging.
//

#pragma once

#include "hft/latency_histogram.h"

#include <cstdint>
#include <string>
#include <utility>

// 延迟统计器：
// - 内部存储纳秒，相对误差 <= 1/64（< 128 ns 精确）；统计覆盖构造以来的全部样本。
// - 线程安全：一个写者线程调用 recordNs，任意线程可同时读取统计值，没有数据竞争。
// Latency tracker:
// - Stores nanoseconds internally, relative error <= 1/64 (exact below 128 ns); the statistics cover every
//   sample since construction.
// - Thread safety: one writer thread calls recordNs, any thread may read the statistics at the same time,
//   and there are no data races.
class LatencyTracker {
public:
    explicit LatencyTracker(std::string name) : name_(std::move(name)) {}

    // 记录单次延迟（纳秒）
    // Records a single latency (nanoseconds)
    void recordNs(std::uint64_t latency_ns) noexcept { hist_.record(latency_ns); }

    const std::string& getName() const { return name_; }
    std::uint64_t count() const noexcept { return hist_.count(); }
    std::uint64_t percentileNs(double q) const noexcept { return hist_.percentile(q); }
    std::uint64_t minNs() const noexcept { return hist_.min(); }
    std::uint64_t maxNs() const noexcept { return hist_.max(); }
    double meanNs() const noexcept { return hist_.mean(); }

    // 延迟 < ns 的样本占比（0~1，桶分辨率）；无样本时为 0
    // Fraction of samples with latency < ns (0~1, bucket resolution); 0 when there are no samples
    double fractionBelowNs(std::uint64_t ns) const noexcept {
        const std::uint64_t n = hist_.count();
        if (n == 0) return 0.0;
        const double f = static_cast<double>(hist_.countBelow(ns)) / static_cast<double>(n);
        return f > 1.0 ? 1.0 : f;  // 并发写入时 countBelow 可能比先读到的 count 新
                                   // with concurrent writes, countBelow may be newer than the count read first
    }

    // 分位数（微秒，double）
    // Percentiles (microseconds, double)
    double getP50Us() const noexcept { return static_cast<double>(hist_.percentile(0.50)) / 1000.0; }
    double getP99Us() const noexcept { return static_cast<double>(hist_.percentile(0.99)) / 1000.0; }

private:
    std::string name_;            // 展示名称（构造后不变，可被多线程同时读取）
                                  // display name (unchanged after construction,
                                  // may be read by several threads at once)
    hft::LatencyHistogram hist_;  // 纳秒直方图
                                  // nanosecond histogram
};
