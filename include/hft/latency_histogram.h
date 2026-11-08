//
// latency_histogram.h
// HDR 风格对数-线性直方图：O(1) 无分配记录，相对误差 <= 1/64，单写者 + 任意多并发读者（原子计数，无数据竞争）。
//

#pragma once

#include "common.h"

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace hft {

// 桶布局（对数-线性）：
// - [0, 128) 每个整数值一个桶（精确）。
// - 之后每个 2 的幂区间 [2^e, 2^(e+1))（e >= 7）均分为 64 个子桶，桶宽 2^(e-6)。
//   桶宽 / 桶下界 <= 1/64，所以用桶内任意值代表样本，相对误差都不超过 1/64。
// - 桶下标 = shift * 64 + (v >> shift)，其中 shift = max(0, msb(v) - 6)。
//   e 最大为 63，共 (63 - 6 + 2) * 64 = 3776 个桶，完整覆盖 uint64 全范围（最高桶的上界即 UINT64_MAX，
//   因此无需额外钳制；内存约 30 KB，记录时只触碰一个桶所在的缓存行）。
// 统计量：
// - count/sum/min/max 精确记录；mean = sum / count 并钳制到 [min, max]（sum 饱和于 UINT64_MAX，
//   只有累计值超过约 1.8e19 时才会发生，例如累计 584 年的纳秒，此后 mean 变为下界）。
// - percentile(q)：最近秩（rank = ceil(q * n)，钳制到 [1, n]）。rank 为 1 / n 时直接返回精确的
//   min / max；否则返回所在桶的中点（相对误差 <= 1/128），并钳制到 [min(), max()]。
// - countBelow(v)：完全位于 v 所在桶之下的样本数，即值 < bucketLow(bucketIndex(v)) 的样本数。
//   v 为桶下界时（v < 128 时总是如此）结果精确，否则少算 v 所在桶内小于 v 的那部分样本。
// 并发：
// - 只允许一个写者线程调用 record/merge（作为目标）；读者可在任意线程并发调用 const 接口。不可拷贝。
// - 所有计数都是 std::atomic<std::uint64_t>，写者用 relaxed 读 + 写更新（单写者，无需 RMW 指令）；
//   count_ 最后用 release 写入，读者先 acquire 读 count_，因此读到 count == n 时至少能看到前 n 次记录：
//   此时 min <= max，[min, max] 覆盖的桶里至少有 n 个样本。
//   写者并发更新期间读者得到的是近似一致的快照（某些字段可能比 count 更新），但不会出现数据竞争或越界。
class LatencyHistogram {
public:
    static constexpr unsigned kSubBucketBits = 6;
    static constexpr std::size_t kSubBucketCount = std::size_t{1} << kSubBucketBits;  // 每个倍程 64 个子桶
    static constexpr std::uint64_t kExactLimit = std::uint64_t{2} << kSubBucketBits;  // [0, 128) 精确
    static constexpr std::size_t kBucketCount = (64 - kSubBucketBits + 1) * kSubBucketCount;  // 3776

    // ---------------- 桶映射（公开以便测试与外部聚合） ----------------

    // 值 v 所在的桶下标，范围 [0, kBucketCount)
    static constexpr std::size_t bucketIndex(std::uint64_t v) noexcept {
        const unsigned msb = 63u - static_cast<unsigned>(__builtin_clzll(v | 1u));
        const unsigned shift = msb > kSubBucketBits ? msb - kSubBucketBits : 0u;
        return (static_cast<std::size_t>(shift) << kSubBucketBits) + static_cast<std::size_t>(v >> shift);
    }

    // 桶内最小值
    static constexpr std::uint64_t bucketLow(std::size_t index) noexcept {
        if (index < kExactLimit) return index;
        const unsigned shift = static_cast<unsigned>(index >> kSubBucketBits) - 1u;
        return (static_cast<std::uint64_t>(index & (kSubBucketCount - 1)) + kSubBucketCount) << shift;
    }

    // 桶内最大值
    static constexpr std::uint64_t bucketHigh(std::size_t index) noexcept {
        if (index < kExactLimit) return index;
        const unsigned shift = static_cast<unsigned>(index >> kSubBucketBits) - 1u;
        return bucketLow(index) + ((std::uint64_t{1} << shift) - 1u);
    }

    // 桶的代表值（中点，向下取整）
    static constexpr std::uint64_t bucketMid(std::size_t index) noexcept {
        const std::uint64_t lo = bucketLow(index);
        return lo + (bucketHigh(index) - lo) / 2;
    }

    // ---------------- 写者接口 ----------------

    // 记录一个样本：O(1)、无分配、无 RMW 原子指令
    void record(std::uint64_t value) noexcept {
        std::atomic<std::uint64_t>& b = buckets_[bucketIndex(value)];
        b.store(b.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        std::uint64_t s = sum_.load(std::memory_order_relaxed);
        if (__builtin_add_overflow(s, value, &s)) s = kMaxValue;
        sum_.store(s, std::memory_order_relaxed);
        if (value < min_.load(std::memory_order_relaxed)) min_.store(value, std::memory_order_relaxed);
        if (value > max_.load(std::memory_order_relaxed)) max_.store(value, std::memory_order_relaxed);
        // 最后发布 count（release）：读者 acquire 读到 n 时，前 n 次记录的全部写入都可见
        count_.store(count_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    // 把 other 的全部样本合并进来（本对象的写者调用；other 可以正被其写者并发更新，也可以是自身）
    void merge(const LatencyHistogram& other) noexcept {
        const std::uint64_t n = other.count_.load(std::memory_order_acquire);
        if (n == 0) return;
        const std::uint64_t lo = other.min_.load(std::memory_order_relaxed);
        const std::uint64_t hi = other.max_.load(std::memory_order_relaxed);
        const std::uint64_t other_sum = other.sum_.load(std::memory_order_relaxed);
        // 用实际合并的桶计数更新 count，保证本对象 count == 各桶之和
        std::uint64_t merged = 0;
        const std::size_t last = bucketIndex(hi);
        for (std::size_t i = bucketIndex(lo); i <= last; ++i) {
            const std::uint64_t c = other.buckets_[i].load(std::memory_order_relaxed);
            if (c == 0) continue;
            buckets_[i].store(buckets_[i].load(std::memory_order_relaxed) + c, std::memory_order_relaxed);
            merged += c;
        }
        std::uint64_t s = sum_.load(std::memory_order_relaxed);
        if (__builtin_add_overflow(s, other_sum, &s)) s = kMaxValue;
        sum_.store(s, std::memory_order_relaxed);
        if (lo < min_.load(std::memory_order_relaxed)) min_.store(lo, std::memory_order_relaxed);
        if (hi > max_.load(std::memory_order_relaxed)) max_.store(hi, std::memory_order_relaxed);
        count_.store(count_.load(std::memory_order_relaxed) + merged, std::memory_order_release);
    }

    // ---------------- 读者接口（任意线程） ----------------

    std::uint64_t count() const noexcept { return count_.load(std::memory_order_acquire); }
    bool empty() const noexcept { return count() == 0; }

    // 最小值（空时为 0）
    std::uint64_t min() const noexcept {
        if (count() == 0) return 0;
        return min_.load(std::memory_order_relaxed);
    }

    // 最大值（空时为 0）
    std::uint64_t max() const noexcept {
        if (count() == 0) return 0;
        return max_.load(std::memory_order_relaxed);
    }

    // 精确均值 sum / count（空时为 0）。结果钳制到 [min, max]：静止时 sum/count 本来就在该范围内，
    // 写者并发更新时 sum 可能比先读到的 count 新，钳制保证结果始终合理；sum 饱和后也能得到合理的下界。
    double mean() const noexcept {
        const std::uint64_t n = count();
        if (n == 0) return 0.0;
        const std::uint64_t s = sum_.load(std::memory_order_relaxed);
        const double lo = static_cast<double>(min_.load(std::memory_order_relaxed));
        const double hi = static_cast<double>(max_.load(std::memory_order_relaxed));
        const double m = static_cast<double>(static_cast<long double>(s) / static_cast<long double>(n));
        return m < lo ? lo : (m > hi ? hi : m);
    }

    // 分位数 q ∈ [0, 1]（越界钳制，NaN 视为 0）：最近秩，桶分辨率，结果钳制到 [min(), max()]；空时为 0
    std::uint64_t percentile(double q) const noexcept {
        if (count() == 0) return 0;
        const std::uint64_t lo = min_.load(std::memory_order_relaxed);
        const std::uint64_t hi = max_.load(std::memory_order_relaxed);
        // 只扫描 [min, max] 覆盖的桶；先求总数再定位秩，两遍之间桶计数只增不减，第二遍必能找到
        const std::size_t first = bucketIndex(lo);
        const std::size_t last = bucketIndex(hi);
        std::uint64_t total = 0;
        for (std::size_t i = first; i <= last; ++i) total += buckets_[i].load(std::memory_order_relaxed);
        if (!(q > 0.0)) q = 0.0;
        if (q > 1.0) q = 1.0;
        const double r = std::ceil(q * static_cast<double>(total));
        if (r >= static_cast<double>(total)) return hi;  // 最大样本：精确（也避免 double -> uint64 越界）
        const std::uint64_t rank = r < 1.0 ? 1 : static_cast<std::uint64_t>(r);
        if (rank >= total) return hi;
        if (rank == 1) return lo;  // 最小样本：精确
        std::uint64_t cum = 0;
        for (std::size_t i = first; i <= last; ++i) {
            cum += buckets_[i].load(std::memory_order_relaxed);
            if (cum >= rank) {
                const std::uint64_t mid = bucketMid(i);
                return mid < lo ? lo : (mid > hi ? hi : mid);
            }
        }
        return hi;
    }

    // 值 < v 的样本数（桶分辨率，见类注释）
    std::uint64_t countBelow(std::uint64_t v) const noexcept {
        if (count() == 0) return 0;
        const std::size_t end_bucket = bucketIndex(v);                                      // 不含 v 所在的桶
        const std::size_t data_end = bucketIndex(max_.load(std::memory_order_relaxed)) + 1;  // 只扫描有数据的范围
        const std::size_t end = end_bucket < data_end ? end_bucket : data_end;
        std::uint64_t below = 0;
        for (std::size_t i = bucketIndex(min_.load(std::memory_order_relaxed)); i < end; ++i) {
            below += buckets_[i].load(std::memory_order_relaxed);
        }
        return below;
    }

    // 某个桶的样本数（测试与导出用）
    std::uint64_t bucketCountAt(std::size_t index) const noexcept {
        return index < kBucketCount ? buckets_[index].load(std::memory_order_relaxed) : 0;
    }

private:
    static constexpr std::uint64_t kMaxValue = std::numeric_limits<std::uint64_t>::max();

    // 写者频繁更新的汇总字段放在同一缓存行
    alignas(kCacheLineSize) std::atomic<std::uint64_t> count_{0};
    std::atomic<std::uint64_t> sum_{0};
    std::atomic<std::uint64_t> min_{kMaxValue};
    std::atomic<std::uint64_t> max_{0};
    alignas(kCacheLineSize) std::atomic<std::uint64_t> buckets_[kBucketCount] = {};
};

}  // namespace hft
