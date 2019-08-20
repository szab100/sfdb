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
#ifndef RAFT_CLUSTER_H_
#define RAFT_CLUSTER_H_

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "raft/options.h"
#include "raft/raft.grpc.pb.h"
#include "util/grpc/async_stub_wrapper.h"
#include "util/types/integral_types.h"

namespace raft {
class RaftServiceStubWrapper
    : public util::AsyncStubWrapper<RaftService::StubInterface> {
 public:
  // This method is for testing only
  RaftServiceStubWrapper(std::unique_ptr<RaftService::StubInterface> &&stub,
                         const Options &opts)
      : util::AsyncStubWrapper<RaftService::StubInterface>(
            std::move(stub), opts.num_dispatch_threads) {}

  DEFINE_ASYNC_GRPC_CALL(RequestVote, RequestVoteRequest, RequestVoteResponse);
  DEFINE_ASYNC_GRPC_CALL(AppendEntries, AppendEntriesRequest,
                         AppendEntriesResponse);
  DEFINE_ASYNC_GRPC_CALL(AppendOnLeader, AppendOnLeaderRequest,
                         AppendOnLeaderResponse);
  DEFINE_SYNC_GRPC_CALL(AppendOnLeader, AppendOnLeaderRequest,
                        AppendOnLeaderResponse);

 protected:
  // This constructor is only for testing
  RaftServiceStubWrapper(std::unique_ptr<RaftService::StubInterface> &&stub)
      : util::AsyncStubWrapper<RaftService::StubInterface>(std::move(stub), 0) {
  }
};

// Represents the whole RAFT cluster and lets a member communicate with others.
//
// Thread-safe because immutable and pointing to thread-safe Stubby2 stubs.
class Cluster {
 public:
  explicit Cluster(const Options &opts);

  // for testing
  Cluster(std::vector<std::string> &&names,
          std::vector<std::unique_ptr<RaftServiceStubWrapper>> &&stubs);

  Cluster(const Cluster &) = delete;
  Cluster &operator=(const Cluster &) = delete;

  size_t size() const { return stubs_.size(); }
  const std::string &me() const { return my_target_; }
  const std::vector<std::string> &others() const { return others_; }
  int64 my_index() const { return my_index_; }

  // Initiates a non-blocking broadcast of RequestVote RPCs.
  // For each successful vote this member receives, calls |on_vote|.
  // Nay votes and failed RPCs are ignored and do not call |on_vote|.
  void BroadcastRequestVote(
      const RequestVoteRequest &request,
      std::function<void(absl::string_view member, const RequestVoteResponse &)>
          on_vote) const;

  // Sends a non-blocking AppendEntries RPC to another RAFT member.
  // Failed RPCs are ignored; successful responses are returned to the
  // |on_response| callback.
  void SendAppendEntries(
      const std::string &member, const AppendEntriesRequest &request,
      std::function<void(const AppendEntriesResponse &)> on_response) const;

  // Sends a blocking Append RPC to |leader|, who is the leader, as far as we
  // know. Returns true if the leader has committed the entry; false on leader
  // change.
  bool SendAppendOnLeader(const std::string &leader, const LogEntry &e) const;

  // Creates a unique message ID.
  uint64 MakeUniqueId() const;

 private:
  // Name of this member.
  const std::string my_target_;

  const absl::Duration request_vote_rpc_timeout_;
  const absl::Duration append_entries_rpc_timeout_;

  // Stubby handles to all members, including |my_target|.
  const std::map<std::string, std::unique_ptr<RaftServiceStubWrapper>> stubs_;

  // Names of the other members.
  const std::vector<std::string> others_;

  // Index of this member in the sorted list of all members.
  const int my_index_;
};

}  // namespace raft

#endif  // RAFT_CLUSTER_H_
