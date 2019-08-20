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
#include "sfdb/modules.h"

#include <memory>

#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_format.h"
#include "sfdb/flags.h"

namespace sfdb {

using ::absl::GetFlag;

void Modules::Init() {
  clock_ = ::util::Clock::RealClock();

  server_builder_ = absl::make_unique<::grpc::ServerBuilder>();
  server_builder_->AddListeningPort(
      absl::StrFormat("0.0.0.0:%d", GetFlag(FLAGS_port)),
      grpc::InsecureServerCredentials());

  built_in_vars_ = absl::make_unique<BuiltIns>();
  raft_ = absl::make_unique<RaftModule>(server_builder_.get(), clock_);
  db_ = absl::make_unique<Db>("MAIN", built_in_vars());
  replicated_db_ = raft_->NewInstance(db_.get());
}

} // namespace sfdb
