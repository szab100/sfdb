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

#ifndef SERVER_GRPC_MODULES_H_
#define SERVER_GRPC_MODULES_H_

#include <memory>

#include "grpcpp/server_builder.h"
#include "sfdb/base/db.h"
#include "sfdb/base/replicated_db.h"
#include "sfdb/base/vars.h"
#include "sfdb/raft/raft_module.h"
#include "util/time/clock.h"

namespace sfdb {

// Server-wide singletons.
//
// Thread-safe.
class GrpcModules {
 public:
  GrpcModules();
  virtual ~GrpcModules() = default;

  GrpcModules(const GrpcModules &) = delete;
  GrpcModules &operator=(const GrpcModules &) = delete;

  ::grpc::ServerBuilder *server_builder() { return server_builder_.get(); }
  ReplicatedDb *db() { return replicated_db_.get(); }

  // Call once at server start-up time.
  void Init(const std::string &host, int port, const std::string &raft_targets);

 protected:
  ::util::Clock *const clock_;
  std::unique_ptr<::grpc::ServerBuilder> server_builder_;
  std::unique_ptr<BuiltIns> built_in_vars_;
  std::unique_ptr<RaftModule> raft_;
  std::unique_ptr<Db> db_;
  std::unique_ptr<ReplicatedDb> replicated_db_;
};

}  // namespace sfdb

#endif  // SERVER_GRPC_MODULES_H_
