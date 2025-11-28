#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <chrono>
#include <cstring>
#include <hazard_pointer.hpp> 

// 链表节点定义
template<typename T>
struct alignas(64) Node { // 缓存行对齐节点
    T data;
    std::atomic<Node*> next;

    Node(const T& data_) : data(data_), next(nullptr) {}
    Node() : next(nullptr) {} // For dummy node
};

// MPMC Queue 实现
template<typename T>
class MPMCQueue {
private:
    alignas(64) std::atomic<Node<T>*> head; // 缓存行对齐 head
    alignas(64) std::atomic<Node<T>*> tail; // 缓存行对齐 tail
    // alignas(64) char pad[64]; // 如果编译器未正确对齐，可手动填充

public:
    MPMCQueue() {
        // 初始化：创建 dummy 节点
        Node<T>* dummy = new Node<T>();
        head.store(dummy, std::memory_order_relaxed);
        tail.store(dummy, std::memory_order_relaxed);
    }

    ~MPMCQueue() {
        // 清理剩余节点
        Node<T>* curr = head.load(std::memory_order_acquire);
        while (curr != nullptr) {
             Node<T>* next = curr->next.load(std::memory_order_acquire);
             delete curr;
             curr = next;
        }
        // 注意：在真实环境中，需要确保所有 retire 的节点都已被 reclaim
    }

    // 入队 (enqueue)
    void enqueue(const T& data) {
        Node<T>* new_node = new Node<T>(data);

        while (true) {
            Node<T>* curr_tail = tail.load(std::memory_order_acquire);
            Node<T>* next = curr_tail->next.load(std::memory_order_acquire);

            // 检查 tail 是否仍然有效（帮助机制的一部分）
            if (curr_tail == tail.load(std::memory_order_acquire)) {
                if (next == nullptr) {
                    // 尝试链接新节点到当前尾部 (线性化点①)
                    if (curr_tail->next.compare_exchange_weak(next, new_node,
                                                              std::memory_order_release,
                                                              std::memory_order_relaxed)) {
                        // 尝试更新 tail 指针 (线性化点②)
                        tail.compare_exchange_weak(curr_tail, new_node,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed);
                        return; // 成功入队
                    }
                } else {
                    // 帮助更新 tail（如果它滞后了）
                    tail.compare_exchange_weak(curr_tail, next,
                                               std::memory_order_release,
                                               std::memory_order_relaxed);
                }
            }
            std::this_thread::yield();
        }
    }

    // 出队 (dequeue)
    bool dequeue(T& result) {
        hazptr::HazPtrHolder<TestObject> hp_head;
        hazptr::HazPtrHolder<TestObject> hp_next;

        while (true) {
            Node<T>* curr_head = head.load(std::memory_order_acquire);
            hp_head.protect(curr_head); // 保护 head
            // 再次检查，确保获取的指针未被其他线程修改
            if (curr_head != head.load(std::memory_order_acquire)) {
                continue;
            }

            Node<T>* curr_tail = tail.load(std::memory_order_acquire);
            Node<T>* next = curr_head->next.load(std::memory_order_acquire);
            hp_next.protect(next);// 保护 next

            // 再次检查 head 和 next
            if (curr_head != head.load(std::memory_order_acquire)) {
                continue;
            }

            if (next == nullptr) {
                // 队列为空
                release_hazard_pointer(hp_head);
                release_hazard_pointer(hp_next);
                return false;
            }

            if (curr_head == curr_tail) {
                // 队列处于中间状态，tail 滞后，帮助更新 tail
                tail.compare_exchange_weak(curr_tail, next,
                                           std::memory_order_release,
                                           std::memory_order_relaxed);
            } else {
                // 尝试弹出节点 (线性化点③)
                if (head.compare_exchange_weak(curr_head, next,
                                               std::memory_order_acquire,
                                               std::memory_order_relaxed)) {
                    result = next->data; // 安全读取数据

                    // 退休旧的 head 节点 (curr_head)，延迟释放
                    hp_head.release();
                    hp_next.release();

                    // 尝试回收节点
                    hazptr::RetirePointer<TestObject>(curr_head);

                    return true; // 成功出队
                }
            }
            std::this_thread::yield();
        }
    }
};




