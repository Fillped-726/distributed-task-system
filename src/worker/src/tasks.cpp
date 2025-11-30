#include "task_registry.h"
#include "logger.hpp"
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>

// 使用匿名空间，防止函数名污染全局符号表
namespace {

// 1. 定义 download_data 任务
std::string DownloadDataTask(const std::string& params_str) {
  LOG_INFO << "[Debug] Received params_str: " << params_str;
  auto params = nlohmann::json::parse(params_str);

  if (params.is_null()) {
    LOG_ERROR << "[Debug] Parsed JSON is NULL!";
    return "{}";  // 快速返回防止崩馈
  }

  std::string url = params.value("url", "unknown_url");

  LOG_INFO << "[Task] Start downloading from: " << url;

  // 模拟耗时操作
  std::this_thread::sleep_for(std::chrono::seconds(2));

  LOG_INFO << "[Task] Download complete!";

  nlohmann::json result;
  result["status"] = "success";
  result["file_size"] = 1024;
  return result.dump();
}

std::string ProcessDataTask(const std::string& params_str) {
  LOG_INFO << "[Task] Start Processing Data... Params: " << params_str;

  // 模拟 CPU 密集型计算 (3秒)
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // 简单的参数透传或修改
  nlohmann::json params;
  try {
    params = nlohmann::json::parse(params_str);
  } catch (...) {
    params = nlohmann::json::object();
  }

  LOG_INFO << "[Task] Data Processed Successfully.";

  nlohmann::json result;
  result["status"] = "processed";
  result["rows"] = 10000;
  result["source"] = params.value("output", "unknown_source");
  result["output_file"] = "/tmp/processed_data.csv";
  return result.dump();
}

// [新增] 模拟结果上传任务
std::string UploadResultsTask(const std::string& params_str) {
  LOG_INFO << "[Task] Start Uploading Results... Params: " << params_str;

  // 模拟网络上传 (2秒)
  std::this_thread::sleep_for(std::chrono::seconds(2));

  LOG_INFO << "[Task] Upload Complete.";

  nlohmann::json result;
  result["status"] = "uploaded";
  result["cloud_url"] = "s3://bucket/report_final.csv";
  return result.dump();
}

// 2. 定义 echo 任务 (之前的测试任务)
std::string EchoTask(const std::string& params_str) {
  LOG_INFO << "[Task] Echo: " << params_str;

  // 构造一个 JSON 对象作为结果
  nlohmann::json result;
  result["status"] = "success";
  result["raw_output"] = "Echo: " + params_str;

  // 返回序列化后的 JSON 字符串
  return result.dump();
}

}  // namespace

// ========================================================
// 3. 使用宏自动注册 (Key Point)
// ========================================================
// 这行代码会在 main 函数执行前运行，自动将函数注册进 TaskRegistry
DTS_REGISTER_TASK("download_data", DownloadDataTask);
DTS_REGISTER_TASK("echo", EchoTask);
DTS_REGISTER_TASK("process_data", ProcessDataTask);
DTS_REGISTER_TASK("upload_results", UploadResultsTask);