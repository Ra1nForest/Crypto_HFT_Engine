//
// latency_tracker.h
// 延迟统计器：采集延迟样本并提供均值、分位数等统计指标。
//

#pragma once

#include <vector>
#include <algorithm>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <cmath>

// 延迟统计器：
// - 支持固定大小的采样池（reservoir）
// - 计算 P50/P95/P99 等常用指标
class LatencyTracker {
public:
    // name: 用于展示的标签
    // reservoir_size: 采样池大小，控制内存与统计精度
    LatencyTracker(const std::string& name, int reservoir_size = 10000)
        : name_(name)
        , reservoir_size_(reservoir_size)
        , count_(0)
        , total_us_(0)
        , min_us_(999999)
        , max_us_(0) {
        samples_.reserve(reservoir_size);
    }

    // 记录单次延迟（微秒）
    void record(long latency_us) {
        if (latency_us < 0) return;
        count_++;
        total_us_ += latency_us;
        if (latency_us < min_us_) min_us_ = latency_us;
        if (latency_us > max_us_) max_us_ = latency_us;

        // 采样池满后循环覆盖，保持固定内存
        if (static_cast<int>(samples_.size()) < reservoir_size_) {
            samples_.push_back(latency_us);
        } else {
            samples_[count_ % reservoir_size_] = latency_us;
        }
    }

    // 记录时间点区间延迟
    void record(std::chrono::high_resolution_clock::time_point start,
                std::chrono::high_resolution_clock::time_point end) {
        long us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        record(us);
    }

    // 获取分位数（0~1）
    long getPercentile(double percentile) const {
        if (samples_.empty()) return 0;
        std::vector<long> sorted = samples_;
        std::sort(sorted.begin(), sorted.end());
        int index = static_cast<int>(std::ceil(percentile * sorted.size())) - 1;
        if (index < 0) index = 0;
        if (index >= static_cast<int>(sorted.size())) index = sorted.size() - 1;
        return sorted[index];
    }

    // 常用指标
    long getP50() const { return getPercentile(0.50); }
    long getP95() const { return getPercentile(0.95); }
    long getP99() const { return getPercentile(0.99); }
    long getMin() const { return count_ > 0 ? min_us_ : 0; }
    long getMax() const { return max_us_; }
    long getAvg() const { return count_ > 0 ? total_us_ / count_ : 0; }
    int getCount() const { return count_; }
    const std::string& getName() const { return name_; }
    const std::vector<long>& getSamples() const { return samples_; }

    // 紧凑统计字符串（用于 UI 面板）
    std::string getCompactStats() const {
        std::ostringstream ss;
        ss << std::setw(6) << count_ << " msgs"
           << "  Avg:" << std::setw(4) << getAvg() << "μs"
           << "  P50:" << std::setw(4) << getP50() << "μs"
           << "  P99:" << std::setw(4) << getP99() << "μs"
           << "  Max:" << std::setw(4) << getMax() << "μs";
        return ss.str();
    }

    // 重置统计
    void reset() {
        samples_.clear();
        samples_.reserve(reservoir_size_);
        count_ = 0;
        total_us_ = 0;
        min_us_ = 999999;
        max_us_ = 0;
    }

private:
    // 展示名称
    std::string name_;
    // 采样池大小与采样数据
    int reservoir_size_;
    std::vector<long> samples_;
    // 统计字段
    int count_;
    long total_us_;
    long min_us_;
    long max_us_;
};