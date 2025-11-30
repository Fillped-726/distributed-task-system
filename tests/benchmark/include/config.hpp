#pragma once
#include <string>

struct BenchmarkConfig {
  std::string api_server_addr = "localhost:45403";
  std::string db_conn_string =
      "postgresql://postgres:password@localhost:5433/dts_db";
  int num_threads = 8;
  int total_requests = 10000;
};