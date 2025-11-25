#pragma once

#include <string>
#include <boost/uuid/uuid.hpp>            // boost::uuids::uuid
#include <boost/uuid/uuid_generators.hpp> // boost::uuids::random_generator
#include <boost/uuid/uuid_io.hpp>         // boost::uuids::to_string

namespace dts {
namespace uuid {

/**
 * @brief (已更新) 使用 Boost.Uuid (V4 随机生成器) 生成 UUID。
 *
 * 这是一个线程安全的函数，依赖于 Boost.Uuid 的实现。
 * 它通常使用操作系统的随机设备 (e.g., /dev/urandom) 作为熵源。
 *
 * @return std::string 格式为 "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" 的 UUID。
 */
inline std::string generate() {
    
    // 1. 创建一个 V4 (随机) UUID 生成器。
    // 这个生成器在内部是线程安全的 (从 Boost 1.54 开始)
    // 并且会使用操作系统提供的最佳随机源。
    static boost::uuids::random_generator gen;

    // 2. 生成一个新的 UUID 对象
    boost::uuids::uuid u = gen();

    // 3. 将其转换为标准字符串格式
    return boost::uuids::to_string(u);
}

} // namespace uuid
} // namespace dts