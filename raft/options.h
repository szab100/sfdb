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
#ifndef RAFT_OPTIONS_H_
#define RAFT_OPTIONS_H_

#include <functional>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "grpcpp/server_builder.h"
#include "util/task/status.h"
#include "util/time/clock.h"

namespace raft {

// Options for configuring a RAFT cluster member.
struct Options {
  // ---------------------------------------------------------------------------
  // Required fields.
  // ---------------------------------------------------------------------------

  // SmartStub-compatible Stubby target of this member.
  std::string my_target;

  // SmartStub-compatible Stubby targets for all of the members of this cluster.
  // May include |my_target|, but doesn't have to -- either way works.
  std::vector<std::string> targets;

  // The server builder instance where the RAFT library will register its gRPC
  // service handler. Must outlive the ::raft::Member object.
  ::grpc::ServerBuilder *server_builder;

  // Called when a new entry has been applied on this member.
  // Args:
  //   string_view msg: the log message passed to Append() or Write().
  //   void *arg: the argument passed to Write() on this member; or nullptr if
  //              appended by a different member, or using Append() anywhere.
  // Returns:
  //   Status: will be passed on to the caller of Write() if Write() was called
  //           on this member. On other members, this return value is ignored.
  std::function<::util::Status(absl::string_view, void *)> on_append;

  // ---------------------------------------------------------------------------
  // Optional fields for performance tweaking and testing.
  // ---------------------------------------------------------------------------

  // The frequency of the alarm loop -- how often the leader heartbeats.
  absl::Duration alarm_timeout = absl::Milliseconds(50);

  // How long to wait for leader's heartbeat before calling a new election.
  absl::Duration election_timeout = absl::Milliseconds(160);

  // How long a candidate will wait for a RequestVote RPC to complete.
  absl::Duration request_vote_rpc_timeout = absl::Milliseconds(100);

  // How long a leader will wait for an AppendEntries RPC to complete.
  absl::Duration append_entries_rpc_timeout = absl::Milliseconds(100);

  // The clock to use for everything except alarm timing.
  // Must outlive the ::raft::Member object.
  util::Clock *clock = util::Clock::RealClock();

  // Number of threads usually allocated for dispatching RPC requests
  size_t num_dispatch_threads = 1;
};

}  // namespace raft

#endif  // RAFT_OPTIONS_H_
