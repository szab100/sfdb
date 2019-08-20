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
#ifndef RAFT_RAFT_H_
#define RAFT_RAFT_H_

#include <string>

#include "absl/strings/string_view.h"
#include "raft/options.h"
#include "raft/service_impl.h"
#include "util/task/status.h"

namespace raft {

// A member of a RAFT cluster.
class Member {
 public:
  // Creates a RAFT client and registers its Stubby handler with the given
  // HTTPServer2 instance. Call Start() to actually start communicating with
  // other members of the RAFT cluster.
  explicit Member(const Options &opts);

  Member(const Member &) = delete;
  Member &operator=(const Member &) = delete;

  // Starts communicating with other members of the RAFT cluster.
  void Start();

  // Undoes Start(). It is unsafe to delete the Member until Stop() returns.
  void Stop();

  // Appends a message to the replicated log. Blocks until committed.
  // This works on any member, not just on the leader.
  void Append(absl::string_view msg);

  // Executes a write operation. Returns the result of calling
  // Options.on_append(op) after it executes on this replica.
  // This may take slightly longer than Append().
  ::util::Status Write(absl::string_view msg, void *arg);

 private:
  ServiceImpl impl_;
};

}  // namespace raft

#endif  // RAFT_RAFT_H_
