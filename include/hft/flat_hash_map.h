//
// flat_hash_map.h
// 整数键开放寻址哈希表：线性探测、2 的幂桶数、最大负载因子 0.5、splitmix64 混合、反向移位删除（无墓碑）。
// Open-addressing hash map with integer keys: linear probing, power-of-two bucket count, maximum load factor
// 0.5, splitmix64 mixing, backward-shift deletion (no tombstones).
//

#pragma once

#include "common.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hft {

namespace detail {

// 键的底层整数类型：枚举取 underlying_type，整数取自身
// Underlying integer type of the key: underlying_type for enums, the type itself for integers
template <class T, bool = std::is_enum_v<T>>
struct KeyUnderlying {
    using type = T;
};
template <class T>
struct KeyUnderlying<T, true> {
    using type = std::underlying_type_t<T>;
};

// splitmix64 终结函数：64 位双射，连续的 id 也能均匀打散到低位
// splitmix64 finalizer: a 64-bit bijection that spreads even consecutive ids evenly over the low bits
constexpr std::uint64_t splitmix64(std::uint64_t x) noexcept {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

}  // namespace detail

// 设计要点：
// - 桶数组为 {key, value 原始存储} 的扁平数组（键值相邻，一次查找通常只碰一个缓存行），线性探测。
// - 空桶用哨兵键 kEmptyKey（底层类型的最大值）表示，因此无需额外的元数据数组；
//   用户真的使用 kEmptyKey 作为键时，它存放在表外的专用槽位（special_value_）中，语义完全一致。
// - 负载因子上限 0.5：插入使 size 超过 bucketCount()/2 时桶数翻倍并重新插入。
// - 删除采用反向移位（backward-shift）：删除后把后续探测链上可以前移的元素依次前移填补空位，
//   不留墓碑，查找性能不会随删除退化。
// - 单次探测的组合操作：extract 删除并取出值；eraseAt 删除 find/insert 返回的指针所指的元素；
//   insert 返回值指针，可以先插入占位值完成"查重 + 预留槽位"，之后经指针写入真正的值或用 eraseAt 撤销。
// - 各查找类操作各自展开"命中 / 遇到空桶"双出口的探测循环，不共用一个返回桶下标的 probe() 辅助函数：
//   实测共用版本在 hft_benchmark 的哈希表各行慢 10% 以上。
// - 指针失效：find/insert/operator[] 返回的指针（引用）在任何触发扩容的插入之后失效；
//   删除会移动同一探测链上的其他元素，因此任何删除之后指向其他元素的指针同样失效。
// - 值类型 V 只需可移动（要求 noexcept 移动构造与析构，扩容与反向移位因此不会中途失败）；
//   V 只在被占用的桶中构造，不要求默认构造（operator[] 除外）。
// - 只能移动构造（不可拷贝、不可赋值）；非线程安全。
// Design notes:
// - The bucket array is a flat array of {key, raw value storage} (key and value sit side by side, so a
//   lookup usually touches just one cache line), with linear probing.
// - Empty buckets are marked by the sentinel key kEmptyKey (the maximum of the underlying type), so no
//   separate metadata array is needed; when a user really does use kEmptyKey as a key, it is stored in a
//   dedicated slot outside the table (special_value_), with exactly the same semantics.
// - Load factor at most 0.5: an insert that takes size past bucketCount()/2 doubles the buckets and rehashes.
// - Deletion uses backward shift: after a delete, the elements further along the probe chain that can move
//   forward are shifted forward one after another to fill the hole, leaving no tombstones, so lookup
//   performance does not degrade with deletions.
// - Combined single-probe operations: extract deletes and takes out the value; eraseAt deletes the element
//   that a pointer returned by find/insert points to; insert returns a value pointer, so one can first
//   insert a placeholder value to do "duplicate check + slot reservation", then write the real value
//   through the pointer or undo it with eraseAt.
// - Each lookup-style operation spells out its own probe loop with the two exits "hit / empty bucket
//   reached" instead of sharing a probe() helper that returns a bucket index: measured, the shared version
//   is more than 10% slower on every hash map row of hft_benchmark.
// - Pointer invalidation: the pointers (references) returned by find/insert/operator[] are invalidated by
//   any insert that triggers growth; a delete moves other elements on the same probe chain, so after any
//   delete, pointers to other elements are invalidated as well.
// - The value type V only needs to be movable (noexcept move construction and destruction are required,
//   so growth and backward shift cannot fail midway); V is only constructed in occupied buckets and need
//   not be default-constructible (except for operator[]).
// - Move construction only (no copy, no assignment); not thread-safe.
template <class K, class V>
class FlatHashMap {
    using Underlying = typename detail::KeyUnderlying<K>::type;
    static_assert(std::is_integral_v<K> || std::is_enum_v<K>, "hft::FlatHashMap: K must be integral or enum");
    static_assert(!std::is_same_v<Underlying, bool>, "hft::FlatHashMap: bool keys are not supported");
    static_assert(std::is_nothrow_move_constructible_v<V>,
                  "hft::FlatHashMap: V must be nothrow move constructible");
    static_assert(std::is_nothrow_destructible_v<V>, "hft::FlatHashMap: V must be nothrow destructible");
    using UnsignedKey = std::make_unsigned_t<Underlying>;

    struct Slot {
        K key;
        alignas(V) unsigned char value[sizeof(V)];
    };

public:
    // 表示空桶的哨兵键（底层类型最大值）；作为普通键使用时存放在表外专用槽位
    // Sentinel key marking an empty bucket (the maximum of the underlying type); when used as an ordinary key,
    // it is stored in the dedicated slot outside the table
    static constexpr K kEmptyKey = static_cast<K>(std::numeric_limits<Underlying>::max());
    // 分配桶数组时的最小桶数
    // Minimum bucket count when allocating the bucket array
    static constexpr std::size_t kMinBuckets = 8;

    // initial_capacity：不触发扩容即可容纳的元素数（等价于构造后 reserve(initial_capacity)）；
    // 0 表示不分配内存，首次插入时再分配
    // initial_capacity: number of elements that fit without growing (equivalent to calling
    // reserve(initial_capacity) after construction); 0 means no memory is allocated until the first insert
    explicit FlatHashMap(std::size_t initial_capacity = 16) {
        if (initial_capacity > 0) reserve(initial_capacity);
    }

    // 移动构造：转移桶数组与表外槽位，源对象变为空表（不持有内存）
    // Move construction: transfers the bucket array and the out-of-table slot; the source becomes an empty
    // table (holding no memory)
    FlatHashMap(FlatHashMap&& other) noexcept
        : slots_(std::exchange(other.slots_, sentinel())),
          mask_(std::exchange(other.mask_, 0)),
          table_size_(std::exchange(other.table_size_, 0)),
          max_load_(std::exchange(other.max_load_, 0)) {
        if (other.has_special_) {
            ::new (static_cast<void*>(special_value_)) V(std::move(*other.specialPtr()));
            has_special_ = true;
            other.destroySpecial();
        }
    }

    ~FlatHashMap() {
        if constexpr (!std::is_trivially_destructible_v<V>) clear();
        if (slots_ != sentinel()) delete[] slots_;
    }

    // 查找：返回值指针，不存在返回 nullptr
    // Lookup: returns a pointer to the value, nullptr if absent
    V* find(K key) noexcept {
        if (HFT_UNLIKELY(key == kEmptyKey)) return has_special_ ? specialPtr() : nullptr;
        for (std::size_t i = homeOf(key);; i = (i + 1) & mask_) {
            Slot& s = slots_[i];
            if (s.key == key) return valuePtr(s);
            if (s.key == kEmptyKey) return nullptr;
        }
    }

    const V* find(K key) const noexcept { return const_cast<FlatHashMap*>(this)->find(key); }

    bool contains(K key) const noexcept { return find(key) != nullptr; }

    // 插入（键已存在时不覆盖）：返回 (值指针, 是否新插入)
    // Insert (an existing key is not overwritten): returns (value pointer, whether newly inserted)
    std::pair<V*, bool> insert(K key, V value) { return emplaceImpl(key, std::move(value)); }

    // 取值引用，键不存在时插入值初始化的 V()
    // Reference to the value; inserts a value-initialized V() if the key is absent
    V& operator[](K key) { return *emplaceImpl(key).first; }

    // 删除：返回是否删除了元素（V 的移动构造与析构均为 noexcept，因此不会抛异常）
    // Erase: returns whether an element was erased (V's move constructor and destructor are both noexcept, so
    // this cannot throw)
    bool erase(K key) noexcept {
        if (HFT_UNLIKELY(key == kEmptyKey)) {
            if (!has_special_) return false;
            destroySpecial();
            return true;
        }
        std::size_t i = homeOf(key);
        for (;; i = (i + 1) & mask_) {
            const K k = slots_[i].key;
            if (k == key) break;
            if (k == kEmptyKey) return false;
        }
        eraseSlot(i);
        return true;
    }

    // 删除并取出值（一次探测）；键不存在时返回 nullopt
    // Erase and take out the value (one probe); returns nullopt if the key is absent
    std::optional<V> extract(K key) noexcept {
        if (HFT_UNLIKELY(key == kEmptyKey)) {
            if (!has_special_) return std::nullopt;
            std::optional<V> out(std::move(*specialPtr()));
            destroySpecial();
            return out;
        }
        std::size_t i = homeOf(key);
        for (;; i = (i + 1) & mask_) {
            const K k = slots_[i].key;
            if (k == key) break;
            if (k == kEmptyKey) return std::nullopt;
        }
        std::optional<V> out(std::move(*valuePtr(slots_[i])));
        eraseSlot(i);
        return out;
    }

    // 删除 p 所指的元素，不再探测。p 必须是 find/insert/operator[] 返回的指针，
    // 且取得之后没有发生过扩容插入或任何删除（见指针失效规则）
    // Erase the element p points to, without probing again. p must be a pointer returned by
    // find/insert/operator[], with no growing insert and no erase of any kind since it was obtained (see the
    // pointer invalidation rules)
    void eraseAt(const V* p) noexcept {
        if (HFT_UNLIKELY(has_special_ && p == specialPtr())) {
            destroySpecial();
            return;
        }
        // 值存储位于所在 Slot 内部：字节偏移整除 sizeof(Slot) 即桶下标
        // The value storage lies inside its Slot: the byte offset divided by sizeof(Slot) (integer division) is
        // the bucket index
        const std::size_t i =
            (reinterpret_cast<std::uintptr_t>(p) - reinterpret_cast<std::uintptr_t>(slots_)) / sizeof(Slot);
        HFT_ASSERT(i <= mask_ && slots_[i].key != kEmptyKey && valuePtr(slots_[i]) == p);
        eraseSlot(i);
    }

    // 保证插入 n 个元素之前不会扩容（size() 超过 n 时才可能扩容）
    // Guarantees no growth before n elements have been inserted (growth can only happen once size() exceeds n)
    void reserve(std::size_t n) {
        if (n > max_load_) rehash(bucketsFor(n));
    }

    // 清空全部元素，保留已分配的桶数组
    // Remove all elements, keeping the allocated bucket array
    void clear() noexcept {
        if (table_size_ != 0) {
            for (std::size_t i = 0; i <= mask_; ++i) {
                Slot& s = slots_[i];
                if (s.key != kEmptyKey) {
                    valuePtr(s)->~V();
                    s.key = kEmptyKey;
                }
            }
            table_size_ = 0;
        }
        destroySpecial();
    }

    std::size_t size() const noexcept { return table_size_ + (has_special_ ? 1 : 0); }
    bool empty() const noexcept { return size() == 0; }
    // 不触发扩容可容纳的元素数（= bucketCount() / 2；未分配时为 0）
    // Number of elements that fit without growing (= bucketCount() / 2; 0 when unallocated)
    std::size_t capacity() const noexcept { return max_load_; }
    // 桶数（2 的幂；未分配时为 1，指向只读的共享空桶）
    // Bucket count (a power of two; 1 when unallocated, pointing at the shared read-only empty bucket)
    std::size_t bucketCount() const noexcept { return mask_ + 1; }

    // 遍历全部元素：f(K, const V&)。顺序未指定；遍历期间不得修改本表。
    // Visit all elements: f(K, const V&). The order is unspecified; do not modify the map while iterating.
    template <class F>
    void forEach(F&& f) const {
        for (std::size_t i = 0; i <= mask_; ++i) {
            const Slot& s = slots_[i];
            if (s.key != kEmptyKey) f(s.key, *valuePtr(s));
        }
        if (has_special_) f(kEmptyKey, *specialPtr());
    }

    // 键的 64 位哈希（splitmix64），桶下标 = hash(key) & (bucketCount() - 1)；公开以便测试构造冲突键
    // 64-bit hash of the key (splitmix64), bucket index = hash(key) & (bucketCount() - 1); public so that tests
    // can construct colliding keys
    static std::uint64_t hash(K key) noexcept {
        return detail::splitmix64(
            static_cast<std::uint64_t>(static_cast<UnsignedKey>(static_cast<Underlying>(key))));
    }

    // 校验内部不变量（测试用，O(n * 探测长度)）：桶数为 2 的幂、负载不超过 0.5、
    // 计数一致、每个元素从 home 到所在位置的探测路径上没有空桶且没有重复键
    // Check the internal invariants (for tests, O(n * probe length)): the bucket count is a power of two, the
    // load is at most 0.5, the counts agree, and each element's probe path from home to its position contains
    // no empty bucket and no duplicate key
    bool checkInvariants(std::string* why = nullptr) const {
        auto fail = [why](const std::string& msg) {
            if (why != nullptr) *why = msg;
            return false;
        };
        const std::size_t buckets = mask_ + 1;
        if (!isPow2(buckets)) return fail("bucket count is not a power of two");
        if (slots_ == sentinel()) {
            if (table_size_ != 0 || max_load_ != 0 || mask_ != 0) return fail("inconsistent empty table");
        } else if (max_load_ != buckets / 2) {
            return fail("max load is not half the bucket count");
        }
        if (table_size_ > max_load_) return fail("load factor above 0.5");
        std::size_t occupied = 0;
        for (std::size_t p = 0; p < buckets; ++p) {
            const K k = slots_[p].key;
            if (k == kEmptyKey) continue;
            ++occupied;
            for (std::size_t i = homeOf(k); i != p; i = (i + 1) & mask_) {
                if (slots_[i].key == kEmptyKey) {
                    return fail("empty bucket inside the probe path of a key at bucket " + std::to_string(p));
                }
                if (slots_[i].key == k) return fail("duplicate key at bucket " + std::to_string(p));
            }
        }
        if (occupied != table_size_) return fail("occupied bucket count does not match size");
        return true;
    }

private:
    // 最大元素数：保证桶数计算与字节数不溢出
    // Maximum element count: keeps the bucket count computation and the byte size from overflowing
    static constexpr std::size_t kMaxElements =
        (std::numeric_limits<std::size_t>::max() / 4) / sizeof(Slot);

    // 只读的共享空桶：未分配内存的表指向它（桶数 1，负载上限 0，首次插入必然先扩容），
    // 因此查找路径无需判空。它位于只读数据段，任何误写都会立即崩溃而不是静默破坏。
    // Shared read-only empty bucket: a table with no memory allocated points at it (bucket count 1, load limit
    // 0, so the first insert always grows first), so the lookup path needs no null check. It lives in the
    // read-only data segment, so any stray write crashes immediately instead of silently corrupting memory.
    static constexpr Slot kSentinelSlot{kEmptyKey, {}};
    static Slot* sentinel() noexcept { return const_cast<Slot*>(&kSentinelSlot); }

    static std::size_t bucketsFor(std::size_t n) {
        if (n > kMaxElements) throw std::length_error("hft::FlatHashMap: capacity too large");
        const std::size_t b = nextPow2(n * 2);
        return b < kMinBuckets ? kMinBuckets : b;
    }

    std::size_t homeOf(K key) const noexcept { return static_cast<std::size_t>(hash(key)) & mask_; }

    // 已知 key 不在表中：返回其探测路径上的第一个空桶
    // key is known not to be in the table: return the first empty bucket on its probe path
    std::size_t findEmptyFor(K key) const noexcept {
        std::size_t i = homeOf(key);
        while (slots_[i].key != kEmptyKey) i = (i + 1) & mask_;
        return i;
    }

    V* specialPtr() noexcept { return std::launder(reinterpret_cast<V*>(special_value_)); }
    const V* specialPtr() const noexcept { return std::launder(reinterpret_cast<const V*>(special_value_)); }

    void destroySpecial() noexcept {
        if (has_special_) {
            specialPtr()->~V();
            has_special_ = false;
        }
    }

    static V* valuePtr(Slot& s) noexcept { return std::launder(reinterpret_cast<V*>(s.value)); }
    static const V* valuePtr(const Slot& s) noexcept {
        return std::launder(reinterpret_cast<const V*>(s.value));
    }

    // 把 from 中的元素移动到空桶 to 并析构原值（from 的键由调用方处理）
    // Move the element in from into the empty bucket to and destroy the original value (the caller handles
    // the key of from)
    static void moveSlot(Slot& from, Slot& to) noexcept {
        V* src = valuePtr(from);
        ::new (static_cast<void*>(to.value)) V(std::move(*src));
        src->~V();
        to.key = from.key;
    }

    // 键不存在时用 args 构造值；返回 (值指针, 是否新插入)
    // Construct the value from args if the key is absent; returns (value pointer, whether newly inserted)
    template <class... Args>
    std::pair<V*, bool> emplaceImpl(K key, Args&&... args) {
        if (HFT_UNLIKELY(key == kEmptyKey)) {
            if (has_special_) return {specialPtr(), false};
            ::new (static_cast<void*>(special_value_)) V(std::forward<Args>(args)...);
            has_special_ = true;
            return {specialPtr(), true};
        }
        std::size_t i = homeOf(key);
        for (;; i = (i + 1) & mask_) {
            const K k = slots_[i].key;
            if (k == key) return {valuePtr(slots_[i]), false};
            if (k == kEmptyKey) break;
        }
        if (HFT_UNLIKELY(table_size_ >= max_load_)) {
            const std::size_t buckets = mask_ + 1;
            rehash(buckets < kMinBuckets ? kMinBuckets : buckets * 2);  // 桶数翻倍（未分配时为 kMinBuckets）
                                                                        // double the buckets (kMinBuckets when
                                                                        // unallocated)
            i = findEmptyFor(key);
        }
        Slot& s = slots_[i];
        ::new (static_cast<void*>(s.value)) V(std::forward<Args>(args)...);  // 抛异常时表内容不变
                                                                             // if this throws, the table is unchanged
        s.key = key;
        ++table_size_;
        return {valuePtr(s), true};
    }

    // 析构桶 i 中的元素，再反向移位：从 i 向后扫描到第一个空桶。元素 j 的探测路径为 [home, j]，
    // 若空位 hole 落在循环区间 [home, j) 内（等价于 dist(home, j) >= dist(hole, j)），
    // 就把它前移到 hole，j 成为新的空位；否则它必须留在原地（前移会越过自己的 home）。
    // Destroy the element in bucket i, then shift backward: scan onward from i up to the first empty bucket.
    // The probe path of element j is [home, j]; if the hole falls inside the circular interval [home, j)
    // (equivalent to dist(home, j) >= dist(hole, j)), move the element forward into hole, and j becomes the
    // new hole; otherwise it must stay where it is (moving it forward would pass its own home).
    void eraseSlot(std::size_t i) noexcept {
        valuePtr(slots_[i])->~V();
        std::size_t hole = i;
        for (std::size_t j = (i + 1) & mask_;; j = (j + 1) & mask_) {
            Slot& sj = slots_[j];
            if (sj.key == kEmptyKey) break;
            const std::size_t home = homeOf(sj.key);
            if (((j - home) & mask_) >= ((j - hole) & mask_)) {
                moveSlot(sj, slots_[hole]);
                hole = j;
            }
        }
        slots_[hole].key = kEmptyKey;
        --table_size_;
    }

    // 重建为 new_buckets 个桶（2 的幂，且能容纳当前元素）；分配失败时原表不变
    // Rebuild with new_buckets buckets (a power of two, large enough for the current elements); if the
    // allocation fails, the original table is unchanged
    void rehash(std::size_t new_buckets) {
        Slot* const fresh = new Slot[new_buckets];
        for (std::size_t i = 0; i < new_buckets; ++i) fresh[i].key = kEmptyKey;
        Slot* const old = slots_;
        const std::size_t old_buckets = mask_ + 1;
        slots_ = fresh;
        mask_ = new_buckets - 1;
        max_load_ = new_buckets / 2;
        if (old == sentinel()) return;
        for (std::size_t i = 0; i < old_buckets; ++i) {
            if (old[i].key != kEmptyKey) moveSlot(old[i], slots_[findEmptyFor(old[i].key)]);
        }
        delete[] old;
    }

    Slot* slots_ = sentinel();
    std::size_t mask_ = 0;        // 桶数 - 1
                                  // bucket count - 1
    std::size_t table_size_ = 0;  // 表内元素数（不含表外专用槽位）
                                  // elements in the table (excluding the dedicated out-of-table slot)
    std::size_t max_load_ = 0;    // 扩容阈值 = 桶数 / 2（未分配时为 0）
                                  // growth threshold = bucket count / 2 (0 when unallocated)
    // kEmptyKey 作为普通键时的值。用标志 + 原始存储，而不用 std::optional<V> 或 union：
    // 实测后两者使 exchange_sim 吞吐下降 7-14%（引擎热路径的代码生成变差）
    // Value of kEmptyKey when used as an ordinary key. Uses a flag + raw storage rather than std::optional<V>
    // or a union: measured, the latter two cut exchange_sim throughput by 7-14% (worse code generation on the
    // engine hot path)
    bool has_special_ = false;
    alignas(V) unsigned char special_value_[sizeof(V)];
};

}  // namespace hft
