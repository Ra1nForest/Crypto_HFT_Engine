//
// object_pool.h
// O(1) 定长对象池：分块原始内存（地址稳定）+ 嵌入空闲槽位的侵入式空闲链表，LIFO 复用（缓存友好）。
//

#pragma once

#include "common.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hft {

// 设计要点：
// - 内存按块（chunk）分配；扩容只追加新块，从不移动已有对象，create() 返回的地址在 destroy() 前一直有效。
// - 空闲槽位里原地存放一个 FreeNode（next 指针），构成单向空闲链表：create() 从表头取，
//   destroy() 压回表头（LIFO：刚释放的槽位还在缓存里，下一次 create() 立即复用）。create/destroy 都是 O(1)。
// - 新块在分配时就把全部槽位串入空闲链表（按地址递增的顺序取出）。这一步顺带写遍新块的每一页，
//   把缺页中断提前到 reserve()/扩容时，而不是留在热路径上。
// - 扩容策略：每次追加一个与当前总容量相同大小的块（总容量翻倍，最少 64 个槽位）；
//   growable == false 时 create() 在耗尽后返回 nullptr。reserve() 不受 growable 限制。
// - 析构：释放全部内存。仍存活的对象（未 destroy 的）也会被析构：不在空闲链表中的槽位就是存活对象。
//   只有析构时才花 O(容量 · log 容量) 时间去找它们，热路径上没有任何活跃度记录开销；
//   T 为平凡析构类型时直接跳过。存活对象的析构顺序未指定，其析构函数不得访问同一对象池中的其他对象。
//   使用者仍应在销毁对象池之前自行 destroy 自己的对象。
// - ASan 构建下，空闲槽位中 FreeNode 之后的字节会被标记为不可访问，可以捕获 destroy() 之后的悬垂访问。
// - 不可拷贝、不可移动；非线程安全：同一对象池只能由一个线程使用。
template <class T>
class ObjectPool {
    static_assert(std::is_nothrow_destructible_v<T>, "hft::ObjectPool<T>: T must be nothrow destructible");

    // 空闲槽位中存放的链表节点
    struct FreeNode {
        FreeNode* next;
    };

    static constexpr std::size_t kSlotAlign =
        alignof(T) > alignof(FreeNode) ? alignof(T) : alignof(FreeNode);
    static constexpr std::size_t kSlotSize =
        ((sizeof(T) > sizeof(FreeNode) ? sizeof(T) : sizeof(FreeNode)) + kSlotAlign - 1) / kSlotAlign *
        kSlotAlign;
    // 块按缓存行对齐（T 要求更高时取 T 的对齐）
    static constexpr std::size_t kChunkAlign = kSlotAlign > kCacheLineSize ? kSlotAlign : kCacheLineSize;
    static constexpr std::size_t kMinGrowth = 64;
    static constexpr std::size_t kMaxSlots = std::numeric_limits<std::size_t>::max() / kSlotSize;

    struct Chunk {
        unsigned char* base;
        std::size_t count;
    };

public:
    // initial_capacity：初始槽位数（立即分配）；growable：耗尽时是否自动扩容
    explicit ObjectPool(std::size_t initial_capacity = 1024, bool growable = true) : growable_(growable) {
        if (initial_capacity > 0) addChunk(initial_capacity);
    }

    ~ObjectPool() {
        destroyLiveObjects();
        for (const Chunk& c : chunks_) {
            HFT_ASAN_UNPOISON(c.base, c.count * kSlotSize);
            ::operator delete(c.base, std::align_val_t{kChunkAlign});
        }
    }

    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;

    // 构造一个对象并返回其地址；耗尽且不可扩容时返回 nullptr。
    // T 的构造函数抛异常时槽位归还给对象池，异常继续传播；扩容分配失败时抛 std::bad_alloc。
    template <class... Args>
    T* create(Args&&... args) {
        if (HFT_UNLIKELY(free_head_ == nullptr)) {
            if (!growable_) return nullptr;
            addChunk(capacity_ > kMinGrowth ? capacity_ : kMinGrowth);
        }
        FreeNode* const node = free_head_;
        HFT_ASAN_UNPOISON(node, kSlotSize);
        free_head_ = node->next;
        try {  // 构造函数不会抛异常时没有可达的处理分支，编译器把它整个删掉
            T* p = ::new (static_cast<void*>(node)) T(std::forward<Args>(args)...);
            ++live_;
            return p;
        } catch (...) {
            pushFree(node);
            throw;
        }
    }

    // 析构对象并把槽位放回空闲链表表头。p 必须是本池中存活的对象；nullptr 为空操作。
    void destroy(T* p) noexcept {
        if (p == nullptr) return;
        HFT_ASSERT(owns(p) && "ObjectPool::destroy(): pointer does not belong to this pool");
        HFT_ASSERT(live_ > 0 && "ObjectPool::destroy(): no live objects");
        p->~T();
        pushFree(p);
        --live_;
    }

    // 保证总容量 >= n（追加一个块，不移动已有对象）；不受 growable 限制
    void reserve(std::size_t n) {
        if (n > capacity_) addChunk(n - capacity_);
    }

    std::size_t size() const noexcept { return live_; }          // 存活对象数
    std::size_t capacity() const noexcept { return capacity_; }  // 总槽位数
    std::size_t available() const noexcept { return capacity_ - live_; }

    // p 是否指向本池某个槽位的起始地址（O(块数)，用于调试断言与测试）
    bool owns(const T* p) const noexcept {
        const std::uintptr_t a = reinterpret_cast<std::uintptr_t>(p);
        for (const Chunk& c : chunks_) {
            const std::uintptr_t base = reinterpret_cast<std::uintptr_t>(c.base);
            if (a >= base && a < base + c.count * kSlotSize) return (a - base) % kSlotSize == 0;
        }
        return false;
    }

private:
    // 把 addr 处的槽位压入空闲链表（槽位中原有对象必须已析构）
    void pushFree(void* addr) noexcept {
        free_head_ = ::new (addr) FreeNode{free_head_};
        HFT_ASAN_POISON(static_cast<unsigned char*>(addr) + sizeof(FreeNode), kSlotSize - sizeof(FreeNode));
    }

    // 追加一个 count 个槽位的块，并把它的槽位串到空闲链表前面
    void addChunk(std::size_t count) {
        if (count > kMaxSlots - capacity_) throw std::length_error("hft::ObjectPool: capacity too large");
        chunks_.reserve(chunks_.size() + 1);  // 先保证 push_back 不抛异常，避免泄漏新块
        auto* base = static_cast<unsigned char*>(
            ::operator new(count * kSlotSize, std::align_val_t{kChunkAlign}));
        chunks_.push_back(Chunk{base, count});
        // 逆序压栈，使 create() 按地址递增的顺序取出槽位（顺序访问对硬件预取友好）
        for (std::size_t i = count; i-- > 0;) pushFree(base + i * kSlotSize);
        capacity_ += count;
    }

    // 析构仍存活的对象：空闲槽位的地址排序后，逐个槽位二分查找，不在其中的就是存活对象
    void destroyLiveObjects() noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            if (live_ == 0) return;
            try {
                std::vector<std::uintptr_t> free_slots;
                free_slots.reserve(capacity_ - live_);
                for (const FreeNode* n = free_head_; n != nullptr; n = n->next) {
                    free_slots.push_back(reinterpret_cast<std::uintptr_t>(n));
                }
                std::sort(free_slots.begin(), free_slots.end());
                for (const Chunk& c : chunks_) {
                    for (std::size_t j = 0; j < c.count; ++j) {
                        unsigned char* const slot = c.base + j * kSlotSize;
                        if (!std::binary_search(free_slots.begin(), free_slots.end(),
                                                reinterpret_cast<std::uintptr_t>(slot))) {
                            std::launder(reinterpret_cast<T*>(slot))->~T();
                        }
                    }
                }
            } catch (...) {
                // 内存不足以建立空闲表：无法区分存活对象，放弃析构（只会泄漏它们持有的资源）
            }
        }
    }

    std::vector<Chunk> chunks_;
    FreeNode* free_head_ = nullptr;
    std::size_t live_ = 0;
    std::size_t capacity_ = 0;
    const bool growable_;
};

}  // namespace hft
