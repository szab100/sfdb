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
#include "sfdb/raft/raft_module.h"

#include <memory>

#include "absl/memory/memory.h"
#include "sfdb/base/db.h"

namespace sfdb {

using ::absl::make_unique;
using ::util::Clock;

RaftModule::~RaftModule() = default;

RaftModule::RaftModule(grpc::ServerBuilder *server_builder, Clock *clock)
    : server_builder_(server_builder), clock_(clock) {}

std::unique_ptr<RaftInstance> RaftModule::NewInstance(Db *db) {
  return absl::make_unique<RaftInstance>(db, server_builder_, clock_);
}

} // namespace sfdb
