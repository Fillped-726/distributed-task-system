#include <gtest/gtest.h>
#include "converters.hpp"
#include <nlohmann/json.hpp>
#include <google/protobuf/struct.pb.h>

using json = nlohmann::json;

// 测试 1: JSON -> Proto Struct -> JSON (完整性测试)
TEST(JsonConvertTest, RoundTrip) {
    // 1. 构造一个复杂的 JSON
    json original;
    original["url"] = "http://example.com";
    original["count"] = 123;
    original["is_valid"] = true;
    original["nested"]["key"] = "value";
    original["null_val"] = nullptr;

    // 2. 转换: JSON -> Proto
    google::protobuf::Struct proto_struct;
    dts::JsonToStruct(original, &proto_struct);

    // 验证 Proto 中的字段
    auto fields = proto_struct.fields();
    EXPECT_EQ(fields.at("url").string_value(), "http://example.com");
    EXPECT_EQ(fields.at("count").number_value(), 123.0);
    EXPECT_TRUE(fields.at("is_valid").bool_value());

    // 3. 转换回: Proto -> JSON
    json result = dts::StructToJson(proto_struct);

    // 4. 验证结果一致性
    EXPECT_EQ(result["url"], "http://example.com");
    EXPECT_EQ(result["nested"]["key"], "value");
    EXPECT_TRUE(result["null_val"].is_null());
}

// 测试 2: 空 JSON 处理 (防止之前的 Crash)
TEST(JsonConvertTest, EmptyHandling) {
    // Case A: 空 JSON 对象
    json empty_json = json::object();
    google::protobuf::Struct proto;
    dts::JsonToStruct(empty_json, &proto);
    EXPECT_TRUE(proto.fields().empty());

    // Case B: 空 Proto Struct 转 JSON
    // 之前这里如果没初始化就会导致 Worker 崩溃
    google::protobuf::Struct empty_proto;
    json res = dts::StructToJson(empty_proto);
    
    // 必须是一个空对象 {}，不能是 null
    EXPECT_TRUE(res.is_object()); 
    EXPECT_FALSE(res.is_null());
}