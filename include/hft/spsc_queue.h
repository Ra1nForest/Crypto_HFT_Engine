//
// spsc_queue.h
// 有界无锁单生产者/单消费者环形队列：64 位单调递增下标 + 2 的幂掩码，仅使用 acquire/release 内存序。
// Bounded lock-free single-producer/single-consumer ring queue: 64-bit monotonically increasing
// indices + a power-of-two mask, acquire/release memory ordering only.
//

#pragma once

#include "common.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hft {

// 设计要点：
// - write_idx_ / read_idx_ 是 64 位单调递增计数，槽位 = 下标 & mask_。
//   满：write - read == capacity；空：write == read。没有浪费的槽位。
//   2^64 次操作在 10 亿次/秒下需要 584 年，实际不会回绕；即使回绕，无符号减法与相等比较也仍然正确。
// - 生产者只写 write_idx_，消费者只写 read_idx_。每一端都缓存对方的下标
//   （cached_read_idx_ / cached_write_idx_），只有缓存值显示"满/空"时才去读对方的缓存行。
//   稳态下每次操作只触碰本端的缓存行和槽位所在的缓存行，避免缓存行在两个核之间来回颠簸。
// - 两个下标与两个缓存副本各自 alignas(kCacheLineSize) 独占一个缓存行；只读字段单独一行。
// - 内存序只用 acquire/release（没有 seq_cst）：
//   生产者 placement-new 构造元素后 release 存 write_idx_，消费者 acquire 读到后才访问元素；
//   消费者析构元素后 release 存 read_idx_，生产者 acquire 读到后才复用该槽位。
//   各端读取自己的下标用 relaxed（只有自己会写它）。
// - 原始对齐内存 + placement new：T 无需默认构造，支持只可移动类型；析构时销毁剩余元素。
// - 线程约束：tryEmplace/tryPush/tryPushReserving 只能由一个生产者线程调用，front/pop/tryPop 只能由一个
//   消费者线程调用；size/empty/capacity 任意线程可调用（对方并发操作时结果是近似值）。
// Design notes:
// - write_idx_ / read_idx_ are 64-bit monotonically increasing counters, slot = index & mask_.
//   Full: write - read == capacity; empty: write == read. No slot is wasted.
//   2^64 operations take 584 years at 1 billion per second, so in practice they never wrap; even if they
//   did, unsigned subtraction and equality comparison would still be correct.
// - The producer writes only write_idx_, the consumer only read_idx_. Each side caches the other side's
//   index (cached_read_idx_ / cached_write_idx_) and reads the other side's cache line only when the
//   cached value shows "full/empty".
//   In steady state each operation touches only its own side's cache line and the cache line of the
//   slot, so cache lines do not bounce back and forth between the two cores.
// - The two indices and the two cached copies each occupy a cache line of their own via
//   alignas(kCacheLineSize); the read-only fields get a separate line.
// - Memory ordering uses only acquire/release (no seq_cst):
//   the producer constructs the element with placement new, then release-stores write_idx_; the consumer
//   accesses the element only after acquire-loading that value;
//   the consumer destroys the element, then release-stores read_idx_; the producer reuses the slot only
//   after acquire-loading that value.
//   Each side reads its own index with relaxed (only that side writes it).
// - Raw aligned memory + placement new: T need not be default-constructible, and move-only types are
//   supported; the destructor destroys the remaining elements.
// - Threading constraints: tryEmplace/tryPush/tryPushReserving may be called only from one producer
//   thread, front/pop/tryPop only from one consumer thread; size/empty/capacity may be called from any
//   thread (the result is approximate while the other side operates concurrently).
template <class T>
class SpscQueue {
    static_assert(std::is_nothrow_destructible_v<T>, "hft::SpscQueue<T>: T must be nothrow destructible");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "hft::SpscQueue requires lock-free 64-bit atomics");

public:
    // capacity：期望容量；实际容量向上取整到 2 的幂，最小为 2
    // capacity: the desired capacity; the actual capacity is rounded up to a power of two, at least 2
    explicit SpscQueue(std::size_t capacity)
        : capacity_(roundCapacity(capacity))
        , mask_(static_cast<std::uint64_t>(capacity_ - 1))
        , storage_(allocateStorage(capacity_)) {}

    // 销毁队列中剩余的元素并释放内存（调用者须保证此时两端线程都已停止访问）
    // Destroys the elements still in the queue and frees the memory (the caller must ensure that the threads
    // of both sides have stopped accessing it by then)
    ~SpscQueue() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            const std::uint64_t w = write_idx_.load(std::memory_order_acquire);
            for (std::uint64_t r = read_idx_.load(std::memory_order_acquire); r != w; ++r) {
                slot(r)->~T();
            }
        }
        ::operator delete(storage_, std::align_val_t{kStorageAlign});
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // ---------------- 生产者端 ----------------
    // ---------------- Producer side ----------------

    // 原地构造一个元素；队列满时返回 false（不阻塞、不构造）。
    // T 的构造函数抛异常时队列保持不变，异常继续向外传播。
    // Constructs an element in place; returns false when the queue is full (no blocking, no construction).
    // If T's constructor throws, the queue stays unchanged and the exception propagates.
    template <class... Args>
    bool tryEmplace(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args&&...>) {
        return emplaceBelow(capacity_, std::forward<Args>(args)...);
    }

    bool tryPush(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) { return tryEmplace(v); }

    bool tryPush(T&& v) noexcept(std::is_nothrow_move_constructible_v<T>) { return tryEmplace(std::move(v)); }

    // 带预留的推入：只有推入之后仍至少空着 reserve 个槽位才拷贝并返回 true（reserve >= capacity() 时总是失败；
    // reserve = 0 与 tryPush 相同）。用途：生产者给必须送达的元素留位置——普通元素用 reserve = 1，
    // 关键元素用 0，那么只要最近一次成功推入的是普通元素，下一个关键元素一定能推入（消费者只会腾出更多槽位）。
    // Push with a reserve: copies and returns true only if at least reserve slots are still free after the
    // push (always fails when reserve >= capacity(); reserve = 0 is the same as tryPush). Use: the producer
    // keeps room for elements that must be delivered — ordinary elements use reserve = 1, critical elements
    // 0; then as long as the last successful push was an ordinary element, the next critical element is
    // guaranteed to fit (the consumer only ever frees more slots).
    bool tryPushReserving(const T& v, std::size_t reserve) noexcept(std::is_nothrow_copy_constructible_v<T>) {
        return reserve < capacity_ && emplaceBelow(capacity_ - reserve, v);
    }

    // ---------------- 消费者端 ----------------
    // ---------------- Consumer side ----------------

    // 队首元素指针；队列空时返回 nullptr。指针在 pop() 之前一直有效。
    // Pointer to the front element; nullptr when the queue is empty. The pointer stays valid until pop().
    T* front() noexcept {
        const std::uint64_t r = read_idx_.load(std::memory_order_relaxed);
        return readable(r) ? slot(r) : nullptr;
    }

    // 弹出（析构）队首元素。前置条件：队列非空（例如 front() != nullptr）。
    // 防御：队列为空时为空操作（启用 HFT_ASSERT 时直接报错）。
    // Pops (destroys) the front element. Precondition: the queue is non-empty (e.g. front() != nullptr).
    // Defensive: a no-op on an empty queue (fails outright when HFT_ASSERT is enabled).
    void pop() noexcept {
        const std::uint64_t r = read_idx_.load(std::memory_order_relaxed);
        const bool ok = readable(r);
        HFT_ASSERT(ok && "SpscQueue::pop() on an empty queue");
        if (HFT_UNLIKELY(!ok)) return;
        slot(r)->~T();
        read_idx_.store(r + 1, std::memory_order_release);  // 归还槽位
                                                            // return the slot
    }

    // 弹出队首元素并移动赋值给 out；队列空时返回 false。
    // 移动赋值抛异常时元素仍留在队列中。
    // Pops the front element and move-assigns it to out; returns false when the queue is empty.
    // If the move assignment throws, the element stays in the queue.
    bool tryPop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
        const std::uint64_t r = read_idx_.load(std::memory_order_relaxed);
        if (!readable(r)) return false;
        T* const p = slot(r);
        out = std::move(*p);
        p->~T();
        read_idx_.store(r + 1, std::memory_order_release);
        return true;
    }

    // ---------------- 任意一端 ----------------
    // ---------------- Either side ----------------

    // 元素个数。由某一端调用时，自己的下标是精确的，对方的下标可能稍旧，因此是近似值。
    // Number of elements. When called from one side, that side's own index is exact and the other side's
    // may be slightly stale, so the value is approximate.
    std::size_t size() const noexcept {
        // 先 acquire 读 read 再读 write：消费者写入的 read 值不会超过它当时看到的 write，
        // 经 happens-before 传递后这里读到的 write >= read，差值不会下溢。
        // Acquire-load read first, then load write: the read value the consumer stores never exceeds
        // the write it saw at that time, so through happens-before transitivity the write loaded here
        // is >= read and the difference cannot underflow.
        const std::uint64_t r = read_idx_.load(std::memory_order_acquire);
        const std::uint64_t w = write_idx_.load(std::memory_order_acquire);
        const std::uint64_t n = w - r;
        // 第三方线程调用时两次读取之间对方可能继续推进，结果钳制到容量以内
        // When a third thread calls this, the sides may keep advancing between the two loads, so the result is
        // clamped to the capacity
        return n > capacity_ ? capacity_ : static_cast<std::size_t>(n);
    }

    bool empty() const noexcept { return size() == 0; }

    // 实际可用容量（2 的幂，>= 构造参数，最小 2）
    // Actual usable capacity (a power of two, >= the constructor argument, at least 2)
    std::size_t capacity() const noexcept { return capacity_; }

private:
    // 存储区按缓存行对齐（T 对齐要求更高时取 T 的对齐），避免首尾槽位与堆上其他数据伪共享
    // The storage is aligned to a cache line (to T's alignment when T requires more), so the first and last
    // slots do not false-share with other data on the heap
    static constexpr std::size_t kStorageAlign =
        alignof(T) > kCacheLineSize ? alignof(T) : kCacheLineSize;
    // 容量上限：保证 2 的幂取整与字节数计算都不会溢出
    // Capacity cap: guarantees that neither the power-of-two rounding nor the byte count computation overflows
    static constexpr std::size_t kMaxCapacity =
        (std::numeric_limits<std::size_t>::max() / 4) / sizeof(T);

    static std::size_t roundCapacity(std::size_t requested) {
        if (requested > kMaxCapacity) throw std::length_error("hft::SpscQueue: capacity too large");
        const std::size_t c = nextPow2(requested);
        return c < 2 ? 2 : c;
    }

    // 队列中的元素少于 limit（<= capacity_）个时在队尾构造一个元素并发布；否则返回 false
    // If the queue holds fewer than limit (<= capacity_) elements, constructs an element at the tail and
    // publishes it; otherwise returns false
    template <class... Args>
    bool emplaceBelow(std::size_t limit, Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args&&...>) {
        const std::uint64_t w = write_idx_.load(std::memory_order_relaxed);
        if (HFT_UNLIKELY(w - cached_read_idx_ >= limit)) {
            // 缓存显示已满：读取消费者的最新下标（acquire：保证其析构已完成，槽位可复用）
            // The cache shows full: load the consumer's latest index (acquire: guarantees that the consumer has
            // finished destroying the element and the slot can be reused)
            cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
            if (w - cached_read_idx_ >= limit) return false;
        }
        ::new (slotAddress(w)) T(std::forward<Args>(args)...);
        write_idx_.store(w + 1, std::memory_order_release);  // 发布元素
                                                             // publish the element
        return true;
    }

    // 消费者端：下标 r 处是否已有元素。缓存显示为空时才读取生产者的最新下标
    // （acquire：保证看到元素的完整构造）
    // Consumer side: whether index r already holds an element. Loads the producer's latest index only when
    // the cache shows empty (acquire: guarantees the fully constructed element is visible)
    bool readable(std::uint64_t r) noexcept {
        if (r != cached_write_idx_) return true;
        cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
        return r != cached_write_idx_;
    }

    static unsigned char* allocateStorage(std::size_t capacity) {
        // 字节数向上取整到缓存行，最后一个槽位所在的缓存行不与其他分配共享
        // The byte count is rounded up to whole cache lines, so the cache line of the last slot is not shared
        // with other allocations
        const std::size_t bytes =
            (capacity * sizeof(T) + kCacheLineSize - 1) / kCacheLineSize * kCacheLineSize;
        return static_cast<unsigned char*>(::operator new(bytes, std::align_val_t{kStorageAlign}));
    }

    void* slotAddress(std::uint64_t index) const noexcept {
        return storage_ + static_cast<std::size_t>(index & mask_) * sizeof(T);
    }

    // 指向槽位中已构造的对象
    // Points to the constructed object in a slot
    T* slot(std::uint64_t index) const noexcept {
        return std::launder(static_cast<T*>(slotAddress(index)));
    }

    // 只读区：构造后不再修改，两端共享读取，缓存行常驻 Shared 状态，不会被失效
    // Read-only area: not modified after construction, read by both sides; the cache line stays in the
    // Shared state and is never invalidated
    alignas(kCacheLineSize) const std::size_t capacity_;
    const std::uint64_t mask_;
    unsigned char* const storage_;

    // 生产者区：write_idx_ 由生产者写、消费者偶尔读；cached_read_idx_ 只有生产者访问
    // Producer area: write_idx_ is written by the producer and occasionally read by the consumer;
    // cached_read_idx_ is accessed only by the producer
    alignas(kCacheLineSize) std::atomic<std::uint64_t> write_idx_{0};
    alignas(kCacheLineSize) std::uint64_t cached_read_idx_ = 0;

    // 消费者区：read_idx_ 由消费者写、生产者偶尔读；cached_write_idx_ 只有消费者访问
    // Consumer area: read_idx_ is written by the consumer and occasionally read by the producer;
    // cached_write_idx_ is accessed only by the consumer
    alignas(kCacheLineSize) std::atomic<std::uint64_t> read_idx_{0};
    alignas(kCacheLineSize) std::uint64_t cached_write_idx_ = 0;
};

}  // namespace hft
