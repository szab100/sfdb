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
#ifndef SFDB_RAFT_INSTANCE_H_
#define SFDB_RAFT_INSTANCE_H_

#include "absl/strings/string_view.h"
#include "raft/raft.h"
#include "sfdb/api.pb.h"
#include "sfdb/base/db.h"
#include "sfdb/base/replicated_db.h"
#include "util/task/status.h"
#include "util/time/clock.h"

namespace sfdb {

// An instance of RAFT that wraps a Db object.
//
// Thread-safe.
class RaftInstance : public ReplicatedDb {
public:
  RaftInstance(Db *db, grpc::ServerBuilder *server_builder,
               ::util::Clock *clock);
  ~RaftInstance();

  RaftInstance(const RaftInstance &) = delete;
  RaftInstance &operator=(const RaftInstance &) = delete;

  // Executes a SQL statement on the leader. Blocking.
  ::util::Status ExecSql(const ExecSqlRequest &request,
                         ExecSqlResponse *response);

private:
  ::util::Status OnAppend(absl::string_view msg, void *arg);

  grpc::ServerBuilder *server_builder_;
  Db *const db_;
  ::util::Clock *const clock_;
  std::unique_ptr<::raft::Member> raft_;
};

} // namespace sfdb

#endif // SFDB_RAFT_INSTANCE_H_
