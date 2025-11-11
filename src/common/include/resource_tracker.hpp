#pragma once
#include <cstdint>
#include "task.hpp"

namespace dts::common {

/*
 * 资源追踪器——空壳版
 * 所有接口直接返回“成功”，后续再替换实现。
 */
class ResourceTracker {
public:
    // 单例
    static ResourceTracker& instance() {
        static ResourceTracker inst;
        return inst;
    }

    // 调度器：预扣资源
    bool try_allocate(const struct Resource& /*req*/, int64_t& ticket) {
        ticket = 0;          // 0 表示“假成功”，后续可改自增 ticket
        return true;         // 永远成功
    }

    // 调度器：归还资源
    void release(int64_t /*ticket*/) { /* 空 */ }

    // 执行器：二次确认
    bool check_available(const struct Resource& /*req*/) const {
        return true;         // 永远够
    }

    // 后台：同步系统真实资源
    void sync_with_system() { /* 空 */ }

private:
    ResourceTracker()  = default;
    ~ResourceTracker() = default;
    ResourceTracker(const ResourceTracker&) = delete;
    ResourceTracker& operator=(const ResourceTracker&) = delete;
};

} // namespace dts::common