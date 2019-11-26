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

#include "server/grpc_modules.h"

#include <memory>

#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_format.h"
#include "sfdb/flags.h"

namespace sfdb {

GrpcModules::GrpcModules() : clock_(::util::Clock::RealClock()) {}

void GrpcModules::Init(const std::string &host, int port,
                       const std::string &raft_targets) {
  server_builder_ = absl::make_unique<::grpc::ServerBuilder>();
  auto my_target = absl::StrFormat("%s:%d", host, port);
  server_builder_->AddListeningPort(my_target,
                                    grpc::InsecureServerCredentials());

  built_in_vars_ = absl::make_unique<BuiltIns>();
  raft_ = absl::make_unique<RaftModule>(server_builder_.get(), clock_);
  db_ = absl::make_unique<Db>("MAIN", built_in_vars_.get());
  replicated_db_ = raft_->NewInstance(my_target, raft_targets, db_.get());
}

}  // namespace sfdb
