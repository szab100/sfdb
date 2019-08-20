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
#ifndef SFDB_RAFT_RAFT_MODULE_H_
#define SFDB_RAFT_RAFT_MODULE_H_

#include <memory>

#include "grpcpp/server_builder.h"
#include "sfdb/raft/instance.h"
#include "util/time/clock.h"

namespace sfdb {
class Db;
// A singleton that knows how to create RaftInstance objects, which are
// implementations of the ReplicatedDb interface.
//
// Thread-safe.
class RaftModule {
public:
  RaftModule(grpc::ServerBuilder *server_builder, ::util::Clock *clock);

  std::unique_ptr<RaftInstance> NewInstance(Db *db);
  ~RaftModule();

private:
  grpc::ServerBuilder *server_builder_;
  ::util::Clock *const clock_;
};

} // namespace sfdb

#endif // SFDB_RAFT_RAFT_MODULE_H_
