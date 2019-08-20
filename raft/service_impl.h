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
#ifndef RAFT_SERVICE_IMPL_H_
#define RAFT_SERVICE_IMPL_H_

#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/grpcpp.h"
#include "raft/alarm_thread.h"
#include "raft/cluster.h"
#include "raft/msg_ids.h"
#include "raft/options.h"
#include "raft/raft.grpc.pb.h"
#include "util/task/status.h"
#include "util/time/clock.h"
#include "util/types/integral_types.h"

namespace raft {
// Implementation of the RAFT algorithm.
//
// Thread-safe.
class ServiceImpl final : public RaftService::Service {
 public:
  explicit ServiceImpl(const Options &opts);

  ~ServiceImpl() override = default;
  ServiceImpl(const ServiceImpl &) = delete;
  ServiceImpl &operator=(const ServiceImpl &) = delete;

  // Registers the gRPC handler and starts the AlarmThread.
  void Start();

  // Undoes Start(). Make sure to call it before destroying the ServiceImpl.
  void Stop();

  // Blocks until |msg| is committed to the replicated log.
  void Append(absl::string_view msg);

  // Blocks until on_append(msg, arg) on this member has returned.
  ::util::Status Write(absl::string_view msg, void *arg);

 private:
  grpc::Status RequestVote(::grpc::ServerContext *rpc,
                           const RequestVoteRequest *request,
                           RequestVoteResponse *response) override;

  grpc::Status AppendEntries(::grpc::ServerContext *rpc,
                             const AppendEntriesRequest *request,
                             AppendEntriesResponse *response) override;

  grpc::Status AppendOnLeader(::grpc::ServerContext *rpc,
                              const AppendOnLeaderRequest *request,
                              AppendOnLeaderResponse *response) override;

 private:
  // Called periodically by the alarm thread.
  void OnAlarm();

  void AdvanceTermTo(uint64 term) EXCLUSIVE_LOCKS_REQUIRED(mu_);
  void StartElection(absl::Time now) EXCLUSIVE_LOCKS_REQUIRED(mu_);
  void CommitEntries() EXCLUSIVE_LOCKS_REQUIRED(mu_);
  void AppendAsLeader() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Sends the AppendOnLeader() RPC and waits for it to finish.
  // Sets e->term() to the RAFT term in which it is committed.
  void AppendAndWait(LogEntry *e);

  // Sends out a RequestVote RPC broadcast and expects responses to arrive
  // through OnVoteReceived().
  void BroadcastRequestVote() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Called when another member has responded to this member's vote request.
  void OnVoteReceived(uint64 election_term, absl::string_view voter,
                      const RequestVoteResponse &response);

  // Sends out an AppendEntries RPC broadcast and expects responses to arrive
  // through OnAppendEntriesResponse().
  void BroadcastAppendEntries(absl::Time now) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Called when another member has responded to this member's AppendEntries.
  // |log_size| is log_.size() at the time of the request.
  // |request_time| is the |now| passed to BroadcastAppendEntries().
  void OnAppendEntriesResponse(uint64 leader_term, const std::string &member,
                               uint64 log_size, absl::Time request_time,
                               const AppendEntriesResponse &response);

  // Writes internal state to DLOG(INFO).
  void DumpState() const EXCLUSIVE_LOCKS_REQUIRED(mu_);  // DEBUG

  ::grpc::ServerBuilder *server_builder_;
  const std::function<::util::Status(absl::string_view, void *)> on_append_;
  util::Clock *const clock_;
  const absl::Duration election_timeout_;
  const absl::Duration alarm_timeout_;
  Cluster cluster_;
  AlarmThread alarm_thread_;
  MsgIds msg_id_gen_;

  absl::Mutex mu_;

  enum State { FOLLOWER, CANDIDATE, LEADER };
  State state_ GUARDED_BY(mu_) = FOLLOWER;

  // The latest term this member has seen.
  uint64 term_ GUARDED_BY(mu_) = 0;

  // Who the leader is, to the best of this member's knowledge.
  std::string leader_ GUARDED_BY(mu_) = "";

  // Candidate that has received our vote in the current term.
  std::string voted_for_ GUARDED_BY(mu_) = "";

  // The list of log entries, starting with an empty sentinel.
  std::vector<LogEntry> log_ GUARDED_BY(mu_) = {{}};

  // Index of the latest known-to-be-committed log entry.
  uint64 commit_index_ GUARDED_BY(mu_) = 0;

  // Index of the latest log entry applied to the state machine.
  uint64 last_applied_ GUARDED_BY(mu_) = 0;

  // For each member, index of the next log entry to send to that member.
  std::map<std::string, uint64> next_index_ GUARDED_BY(mu_);

  // For each member, index of the highest log entry known to be replicated.
  std::map<std::string, uint64> match_index_ GUARDED_BY(mu_);

  // For each member, the time of the most recent AppendEntries RPC we went.
  std::map<std::string, absl::Time> last_sync_time_ GUARDED_BY(mu_);

  // The time of the most recent leader heartbeat or this member's vote.
  absl::Time last_heartbeat_time_ GUARDED_BY(mu_) = absl::UnixEpoch();

  // The set of voters who have voted for us in an ongoing election.
  std::set<std::string> votes_for_me_ GUARDED_BY(mu_);

  // Maps log entry IDs appended, but not yet committed by this leader, to
  // whether they are ready to be reexamined.
  std::map<uint64, bool> hanging_appends_ GUARDED_BY(mu_);

  // Maps log entry IDs created in Write() calls to their |arg| values.
  std::map<uint64, void *> write_args_ GUARDED_BY(mu_);

  // Maps log entry IDs created in Write() calls to their statuses.
  std::map<uint64, std::unique_ptr<::util::Status>> write_statuses_
      GUARDED_BY(mu_);
};

}  // namespace raft

#endif  // RAFT_SERVICE_IMPL_H_
