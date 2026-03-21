#pragma once

#include <vector>
#include <algorithm>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <cmath>

class LatencyTracker {
public:
    LatencyTracker(const std::string& name, int reservoir_size = 10000)
        : name_(name)
        , reservoir_size_(reservoir_size)
        , count_(0)
        , total_us_(0)
        , min_us_(999999)
        , max_us_(0) {
        samples_.reserve(reservoir_size);
    }

    void record(long latency_us) {
        if (latency_us < 0) return;
        count_++;
        total_us_ += latency_us;
        if (latency_us < min_us_) min_us_ = latency_us;
        if (latency_us > max_us_) max_us_ = latency_us;

        if (static_cast<int>(samples_.size()) < reservoir_size_) {
            samples_.push_back(latency_us);
        } else {
            samples_[count_ % reservoir_size_] = latency_us;
        }
    }

    void record(std::chrono::high_resolution_clock::time_point start,
                std::chrono::high_resolution_clock::time_point end) {
        long us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        record(us);
    }

    long getPercentile(double percentile) const {
        if (samples_.empty()) return 0;
        std::vector<long> sorted = samples_;
        std::sort(sorted.begin(), sorted.end());
        int index = static_cast<int>(std::ceil(percentile * sorted.size())) - 1;
        if (index < 0) index = 0;
        if (index >= static_cast<int>(sorted.size())) index = sorted.size() - 1;
        return sorted[index];
    }

    long getP50() const { return getPercentile(0.50); }
    long getP95() const { return getPercentile(0.95); }
    long getP99() const { return getPercentile(0.99); }
    long getMin() const { return count_ > 0 ? min_us_ : 0; }
    long getMax() const { return max_us_; }
    long getAvg() const { return count_ > 0 ? total_us_ / count_ : 0; }
    int getCount() const { return count_; }
    const std::string& getName() const { return name_; }
    const std::vector<long>& getSamples() const { return samples_; }

    std::string getCompactStats() const {
        std::ostringstream ss;
        ss << std::setw(6) << count_ << " msgs"
           << "  Avg:" << std::setw(4) << getAvg() << "μs"
           << "  P50:" << std::setw(4) << getP50() << "μs"
           << "  P99:" << std::setw(4) << getP99() << "μs"
           << "  Max:" << std::setw(4) << getMax() << "μs";
        return ss.str();
    }

    void reset() {
        samples_.clear();
        samples_.reserve(reservoir_size_);
        count_ = 0;
        total_us_ = 0;
        min_us_ = 999999;
        max_us_ = 0;
    }

private:
    std::string name_;
    int reservoir_size_;
    std::vector<long> samples_;
    int count_;
    long total_us_;
    long min_us_;
    long max_us_;
};