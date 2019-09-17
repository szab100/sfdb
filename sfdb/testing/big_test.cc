/*
 * Copyright (c) 2019 Google LLC.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "sfdb/flags.h"
#include "sfdb/modules.h"
#include "sfdb/service_impl.h"
#include "util/net/port.h"
#include "gtest/gtest.h"

namespace sfdb {
namespace {
using ::absl::GetFlag;
using ::absl::SetFlag;
using ::absl::StrFormat;

class BigTest : public ::testing::Test {
protected:
  void SetFlags() {
    auto port = PickUpFreeLocalPort();
    SetFlag(&FLAGS_port, port);
    SetFlag(&FLAGS_raft_my_target, StrFormat("0.0.0.0:%d", port));
    SetFlag(&FLAGS_raft_targets, StrFormat("0.0.0.0:%d", port));
  }

  void SetUp() override {
    SetFlags();
    modules_.reset(new Modules);
    modules_->Init();
    service_.reset(new SfdbServiceImpl(modules_.get()));
    server_ = modules_->server_builder()->BuildAndStart();
  }

  void TearDown() override {
    server_->Shutdown();
    service_ = nullptr;
    modules_ = nullptr;
    server_ = nullptr;
  }

  // Populates rows_ with results.
  void Go(::absl::string_view sql) {
    ExecSqlRequest request;
    request.set_sql(std::string(sql));
    request.set_include_debug_strings(true);
    ::grpc::ServerContext rpc;
    ExecSqlResponse response;
    service_->ExecSql(&rpc, &request, &response);

    rows_.clear();
    for (int i = 0; i < response.debug_strings_size(); ++i)
      rows_.push_back(response.debug_strings(i));
  }

  std::unique_ptr<Modules> modules_;
  std::unique_ptr<SfdbService::Service> service_;
  std::vector<std::string> rows_;
  std::unique_ptr<::grpc::Server> server_;
};

TEST_F(BigTest, ManyRows) {
  Go("CREATE TABLE People (name string, age int64);");
  EXPECT_TRUE(rows_.empty());

  const int n = 800;
  for (int i = 0; i < n; ++i) {
    Go(StrFormat("INSERT INTO People (name, age) VALUES ('Bob_%d', %d);", i,
                 20 + i % 80));
    EXPECT_TRUE(rows_.empty());
  }

  Go("SELECT name FROM People WHERE age % 80 = 20;");
  EXPECT_EQ(n / 80, rows_.size());

  Go("UPDATE People SET age = age + 1 WHERE age % 80 = 20;");
  EXPECT_TRUE(rows_.empty());

  Go("SELECT name FROM People WHERE age % 80 = 20;");
  EXPECT_TRUE(rows_.empty());
}

// This is more of a benchmark than a test.
TEST_F(BigTest, Indices) {
  Go("CREATE TABLE People (name string, age int64);");
  Go("CREATE INDEX ByName ON People(name);");

  const int n = 1024;
  for (int i = 0; i < n; ++i) {
    Go(StrFormat("INSERT INTO People (name, age) VALUES ('Bob_%d', %d);",
                 666 ^ i, 20 + i * 80));
  }

  for (int i = 0; i < n; ++i) {
    Go(StrFormat("UPDATE People SET age = age + 1 WHERE name = 'Bob_%d';",
                 876 ^ i));
  }

  EXPECT_TRUE(rows_.empty());
}

} // namespace
} // namespace sfdb
