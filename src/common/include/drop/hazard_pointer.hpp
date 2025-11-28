#include <atomic>
#include <thread>
#include <vector>
#include <memory>
#include <functional>
#include <iostream>
#include <cassert>
#include <unordered_set>
#include <mutex>
#include <list>
#include <algorithm> 

namespace hazptr {

// --- 核心组件 ---

// 1. Hazard Pointer 槽
template <typename T>
class HazardPointer {
public:
    std::atomic<T*> hazard_ptr_{nullptr};
    std::atomic<std::thread::id> owner_{std::thread::id{}}; 
    HazardPointer<T>* next_{nullptr}; 

    HazardPointer() = default;
};

// 2. 待回收对象的包装
template <typename T>
struct RetiredPtr {
    T* ptr_;
    std::function<void(T*)> deleter_; 

    RetiredPtr(T* ptr, std::function<void(T*)> deleter)
        : ptr_(ptr), deleter_(std::move(deleter)) {}
};

// 3. 每个线程的本地数据
template <typename T>
class ThreadData {
public:
    std::list<RetiredPtr<T>> retired_list_;
    size_t scan_threshold_ = 100; // 当 retired_list_ 达到此大小时触发 Scan
    std::thread::id thread_id_;

    ThreadData() = default;
};

//线程局部存储
template <typename T>
inline thread_local ThreadData<T> tl_thread_data;

// 4. Hazard Pointer 域 (Domain)
// 简化起见，我们实现一个全局默认域。实际工业级库可能支持多个域。
template <typename T>
class HazPtrDomain {
private:
    // 所有 Hazard Pointer 槽的链表头
    HazardPointer<T>* head_{nullptr};
    mutable std::mutex list_mutex_;
    // 单例实例
    inline static HazPtrDomain<T> default_domain_;
    std::list<RetiredPtr<T>> global_retired_;
    inline static int kGlobalThreshold = 5000;
    std::mutex global_retired_mutex_;

    // 全局数据
    std::vector<ThreadData<T>*> all_thread_data_;
    std::mutex thread_data_mutex_; 
    

public:

    template<typename U>
    friend void RetirePointer(U*, std::function<void(U*)>);

    static HazPtrDomain<T>& defaultDomain() {
        return default_domain_;
    }

    // 注册新线程
    static void RegisterThread() {
        auto& domain = defaultDomain();
        auto& tl = tl_thread_data<T>;
        // 设置线程ID
        tl.thread_id_ = std::this_thread::get_id();
        
        // 添加到全局列表
        {
            std::lock_guard<std::mutex> lock(domain.thread_data_mutex_);
            domain.all_thread_data_.push_back(&tl);
        }
    }
    
    // 注销线程
    static void UnregisterThread() {
        auto& domain = defaultDomain();
        auto current_id = std::this_thread::get_id();
        
        // 从全局列表中移除
        {
            std::lock_guard<std::mutex> lock(domain.thread_data_mutex_);
            auto it = std::remove_if(
                domain.all_thread_data_.begin(),
                domain.all_thread_data_.end(),
                [current_id](const ThreadData<T>*& data) {
                    return data.thread_id_ == current_id;
                }
            );
            domain.all_thread_data_.erase(it, domain.all_thread_data_.end());
        }
    }

    // 为当前线程获取一个 Hazard Pointer 槽
    HazardPointer<T>* acquire() {
        std::thread::id this_id = std::this_thread::get_id();
        // 1. 尝试从空闲列表获取 (简化实现省略了显式的空闲列表管理，直接遍历)
        std::lock_guard<std::mutex> lock(list_mutex_);
        HazardPointer<T>* current = head_;
        while (current != nullptr) {
            std::thread::id no_owner{};
            if (current->owner_.load(std::memory_order_relaxed) == no_owner) {
                current->owner_.store(this_id, std::memory_order_release);
                return current;
            }
            current = current->next_;
        }
        // 2. 如果没有空闲槽，则分配一个新的
        HazardPointer<T>* new_hp = new HazardPointer<T>();
        new_hp->owner_.store(this_id, std::memory_order_release);
            HazardPointer<T>* old_head = head_;
            head_ = new_hp;
            new_hp->next_ = old_head;
        return new_hp;
    }

    // 释放一个 Hazard Pointer 槽
    void release(HazardPointer<T>* hp) {
        if (hp != nullptr) {
            hp->hazard_ptr_.store(nullptr, std::memory_order_release);
            std::thread::id this_id = std::this_thread::get_id();
            assert(hp->owner_.load(std::memory_order_acquire) == this_id); // Debug check
            hp->owner_.store(std::thread::id{}, std::memory_order_release); // Mark as free
        }
    }

    // 扫描所有 Hazard Pointer 槽，检查给定的指针是否仍在被保护
    bool isProtected(T* ptr) const {
        HazardPointer<T>* current = head_;
        while (current != nullptr) {
            if (current->hazard_ptr_.load(std::memory_order_acquire) == ptr) {
                return true;
            }
            current = current->next_;
        }
        return false;
    }

    void Scan() {
        
        {
            std::lock_guard<std::mutex> lock(global_retired_mutex_);
            for (auto* tl : all_thread_data_) {
                if (!tl->retired_list_.empty()) {
                    global_retired_.splice(global_retired_.end(), tl->retired_list_);
                }
            }
        }
        
        reclaim_all();
    }

    // 回收函数：尝试回收给定列表中的对象
    void reclaim_all() {
        std::vector<RetiredPtr<T>> to_reclaim;
        {
            std::lock_guard<std::mutex> g(global_retired_mutex_);
            auto it = global_retired_.begin();
            while (it != global_retired_.end()) {
                if (!isProtected(it->ptr_)) {   // 无 hazard ptr 指向它
                    to_reclaim.push_back(std::move(*it));
                    it = global_retired_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        // 在锁外逐个调用自己的删除器
        for (auto& rec : to_reclaim) rec.deleter_(static_cast<T*>(rec.ptr_));
    }

private:
    HazPtrDomain() = default;
    ~HazPtrDomain() {
        std::lock_guard<std::mutex> lock(list_mutex_);
        HazardPointer<T>* current = head_;
        while (current != nullptr) {
            HazardPointer<T>* next = current->next_;
            delete current;
            current = next;
        }
    }
};



template <typename T>
class HazPtrHolder {
private:
    HazPtrDomain<T>& domain_;
    HazardPointer<T>* hazard_ptr_slot_;

public:
    explicit HazPtrHolder(HazPtrDomain<T>& domain = HazPtrDomain<T>::defaultDomain())
        : domain_(domain), hazard_ptr_slot_(domain_.acquire()) {}

    ~HazPtrHolder() {
        release();
    }

    // 禁止拷贝和赋值
    HazPtrHolder(const HazPtrHolder&) = delete;
    HazPtrHolder& operator=(const HazPtrHolder&) = delete;

    // 允许移动 
    HazPtrHolder(HazPtrHolder&& other) noexcept
        : domain_(other.domain_), hazard_ptr_slot_(other.hazard_ptr_slot_) {
        other.hazard_ptr_slot_ = nullptr;
    }

    HazPtrHolder& operator=(HazPtrHolder&& other) noexcept {
        if (this != &other) {
            release();
            hazard_ptr_slot_ = other.hazard_ptr_slot_;
            other.hazard_ptr_slot_ = nullptr;
        }
        return *this;
    }

    // 设置要保护的指针
    void protect(T* ptr) {
        if (hazard_ptr_slot_) {
            hazard_ptr_slot_->hazard_ptr_.store(ptr, std::memory_order_release);
        }
    }

    // 获取当前保护的指针
    T* get() const {
        return hazard_ptr_slot_ ? hazard_ptr_slot_->hazard_ptr_.load(std::memory_order_acquire) : nullptr;
    }

    // 释放槽位
    void release() {
        if (hazard_ptr_slot_) {
            domain_.release(hazard_ptr_slot_);
            hazard_ptr_slot_ = nullptr;
        }
    }
};

// --- 公共接口 ---

// 将一个指针标记为待回收
template <typename T>
void RetirePointer(T* ptr, std::function<void(T*)> deleter = [](T* p) { delete p; }) {
    auto& tl = tl_thread_data<T>;  // 当前线程的TLS数据

    // 1. 添加到本地待回收列表
    tl.retired_list_.emplace_back(ptr, std::move(deleter));

    // 2. 检查是否需要提交到全局列表
    if (tl.retired_list_.size() >= tl.scan_threshold_) {
        auto& domain = HazPtrDomain<T>::defaultDomain();
        
        // ✅ 关键修改：使用 std::lock_guard 管理锁（安全且高效）
        std::lock_guard<std::mutex> lock(domain.global_retired_mutex_);
        
        // 3. 提交到全局队列（自动清空本地列表）
        domain.global_retired_.splice(
            domain.global_retired_.end(),
            tl.retired_list_
        );
        
        // 4. 检查全局队列大小（锁内安全检查）
        if (domain.global_retired_.size() > domain.kGlobalThreshold) {
            domain.Scan();
        }
    }
}

} // namespace hazptr