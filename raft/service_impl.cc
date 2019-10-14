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
#include "raft/service_impl.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "absl/memory/memory.h"
#include "glog/logging.h"
#include "util/task/canonical_errors.h"
#include "util/varz/varz.h"

DEFINE_VARZ(std::string, leader_name, "");
DEFINE_VARZ(int32, is_leader, 0);

namespace raft {
namespace {

using ::absl::Condition;
using ::absl::Duration;
using ::absl::make_unique;
using ::absl::MutexLock;
using ::absl::string_view;
using ::absl::Time;
using ::util::CancelledError;
using ::util::Status;
}  // namespace

ServiceImpl::ServiceImpl(const Options &opts)
    : server_builder_(opts.server_builder),
      on_append_(opts.on_append),
      clock_(opts.clock),
      election_timeout_(opts.election_timeout),
      alarm_timeout_(opts.alarm_timeout),
      cluster_(opts),
      alarm_thread_(opts.alarm_timeout, [this]() { OnAlarm(); }),
      msg_id_gen_(cluster_.size(), cluster_.my_index()) {
  CHECK(server_builder_) << "::raft::Options.serverBuilder must be non-null";
  CHECK(clock_) << "::raft::Options.clock must be non-null";
}

void ServiceImpl::Start() {
  server_builder_->RegisterService(this);
  alarm_thread_.Start();
  if (!varz::StartVarZService()) {
    LOG(ERROR) << "Failed to start varz service";
  }
}

void ServiceImpl::Stop() {
  varz::StopVarZService();
  alarm_thread_.Stop();
}

void ServiceImpl::Append(string_view msg) {
  LogEntry e;
  e.set_msg(std::string(msg));
  e.set_id(msg_id_gen_.Make());
  AppendAndWait(&e);
}

void ServiceImpl::AppendAndWait(LogEntry *e) {
  VLOG(2) << cluster_.me() << " starts trying to commit "
          << e->ShortDebugString();
  // const Time start_time = clock_->TimeNow();

  std::string leader;
  int num_attempts = 0;
  do {
    ++num_attempts;
    MutexLock lock(&mu_);
    mu_.Await(Condition(
        +[](std::string *l) { return !l->empty(); }, &leader_));
    leader = leader_;
    e->set_term(term_);
    VLOG(2) << cluster_.me() << " forwards Append(id=" << e->id()
            << ") to leader " << leader;
  } while (!cluster_.SendAppendOnLeader(leader, *e));
  // This sends self-RPCs when leader_==cluster_.me().
}

Status ServiceImpl::Write(string_view msg, void *arg) {
  LogEntry e;
  e.set_msg(std::string(msg));
  e.set_id(msg_id_gen_.Make());

  {
    MutexLock lock(&mu_);
    write_args_[e.id()] = arg;
    write_statuses_[e.id()] = nullptr;
  }

  AppendAndWait(&e);

  MutexLock lock(&mu_);
  mu_.Await(Condition(
      +[](std::unique_ptr<Status> *s) { return !!s->get(); },
      &write_statuses_[e.id()]));
  const Status s = *write_statuses_[e.id()];
  write_statuses_.erase(e.id());
  write_args_.erase(e.id());
  return s;
}

grpc::Status ServiceImpl::RequestVote(grpc::ServerContext *context,
                                      const RequestVoteRequest *request,
                                      RequestVoteResponse *response) {
  MutexLock lock(&mu_);

  if (request->term() > term_) AdvanceTermTo(request->term());
  response->set_term(term_);

  if (request->term() < term_) {
    VLOG(2) << "Rejecting RequestVote() from " << request->candidate_id()
            << " because the candidate's term (" << request->term()
            << ") is smaller than " << cluster_.me() << "'s (" << term_ << ")";
    response->set_vote_granted(false);
    return grpc::Status::OK;
  }

  if (!voted_for_.empty() && voted_for_ != request->candidate_id()) {
    VLOG(2) << "Rejecting RequestVote() from " << request->candidate_id()
            << " because " << cluster_.me() << " has already voted for "
            << voted_for_ << " in term " << term_;
    response->set_vote_granted(false);
    return grpc::Status::OK;
  }

  const std::pair<uint64, uint64> candidate_log(request->last_log_term(),
                                                request->last_log_index()),
      my_log(log_[commit_index_].term(), commit_index_);
  if (candidate_log < my_log) {
    VLOG(2) << "Rejecting RequestVote() from " << request->candidate_id()
            << " because the candidate's log is not up to date with "
            << cluster_.me() << "'s";
    response->set_vote_granted(false);
    return grpc::Status::OK;
  }

  VLOG(2) << cluster_.me() << " accepts RequestVote() from "
          << request->candidate_id() << " in term " << request->term();
  response->set_vote_granted(true);

  voted_for_ = request->candidate_id();
  last_heartbeat_time_ = clock_->TimeNow();

  return grpc::Status::OK;
}

grpc::Status ServiceImpl::AppendEntries(grpc::ServerContext *context,
                                        const AppendEntriesRequest *request,
                                        AppendEntriesResponse *response) {
  MutexLock lock(&mu_);

  if (request->term() >= term_) AdvanceTermTo(request->term());
  response->set_term(term_);

  if (request->term() < term_) {
    VLOG(2) << cluster_.me() << " rejects " << request->leader_id()
            << "'s AppendEntries() because the claimed leader's term ("
            << request->term() << ") is smaller than " << term_;
    response->set_success(false);
    return grpc::Status::OK;
  }

  leader_ = request->leader_id();

  if (request->prev_log_index() >= log_.size() ||
      log_[request->prev_log_index()].term() != request->prev_log_term()) {
    VLOG(2) << cluster_.me() << " rejects " << request->leader_id()
            << "'s AppendEntries() because of log mismatch at index "
            << request->prev_log_index();
    response->set_success(false);
    return grpc::Status::OK;
  }

  if (request->entry_size()) {
    VLOG(2) << cluster_.me() << " starts gluing " << request->entry_size()
            << " entries after index " << request->prev_log_index();
  }

  CHECK(request->entry_size() >= 0);
  uint64 entry_size = static_cast<uint64_t>(request->entry_size());
  for (size_t i = 0; i < entry_size; ++i) {
    const auto j = request->prev_log_index() + 1 + i;
    if (j < log_.size() && log_[j].term() != request->entry(i).term()) {
      VLOG(2) << cluster_.me() << " removes the last " << log_.size() - j
              << " entries from its log";
      while (log_.size() > j) log_.pop_back();
    }
    if (j < log_.size()) {
      DCHECK_EQ(request->entry(i).msg(), log_[j].msg());
    } else {
      DCHECK(j == log_.size());
      VLOG(2) << cluster_.me() << " appends log entry at index " << j;
      log_.push_back(request->entry(i));
      if ((i + 1ULL) == entry_size) {
        VLOG(2) << cluster_.me() << " grew the log to size " << log_.size();
      }
    }
  }

  if (request->leader_commit() > commit_index_) {
    commit_index_ =
        std::min<uint64>(request->leader_commit(), log_.size() - 1ULL);
    alarm_thread_.Poke();
  }

  last_heartbeat_time_ = clock_->TimeNow();
  response->set_success(true);

  return grpc::Status::OK;
}

grpc::Status ServiceImpl::AppendOnLeader(grpc::ServerContext *context,
                                         const AppendOnLeaderRequest *request,
                                         AppendOnLeaderResponse *response) {
  const LogEntry &e = request->entry();
  MutexLock lock(&mu_);

  if (state_ != LEADER || e.term() != term_) {
    // rpc->set_util_status(CancelledError("leader change"));
    return grpc::Status::CANCELLED;
  }
  VLOG(2) << cluster_.me() << " gets AppendOnLeader(id=" << e.id() << ")";

  // Find this entry in the log, or append it if it's not there.
  size_t i;
  for (i = log_.size() - 1; log_[i].term() >= e.term(); --i) {
    if (log_[i].id() == e.id()) break;
  }
  if (log_[i].term() < e.term()) {
    i = log_.size();
    log_.push_back(e);
    hanging_appends_[e.id()] = false;
  }

  grpc::Status status = grpc::Status::OK;
  // Wait for it to get committed.
  while (1) {
    mu_.Await(Condition(&hanging_appends_[e.id()]));
    if (commit_index_ >= i && log_[i].id() == e.id()) break;
    if (term_ > e.term() || i >= log_.size() || log_[i].id() != e.id()) {
      // rpc->set_util_status(CancelledError("leader change"));
      status = grpc::Status::CANCELLED;
      break;
    }
    hanging_appends_[e.id()] = false;
  }
  hanging_appends_.erase(e.id());

  return status;
}

void ServiceImpl::OnAlarm() {
  MutexLock lock(&mu_);

  // Apply log entries that are safe to apply.
  if (last_applied_ < commit_index_) {
    VLOG(2) << cluster_.me() << " is about to apply "
            << commit_index_ - last_applied_ << " log entries locally";
  }
  while (last_applied_ < commit_index_) {
    const LogEntry &e = log_[++last_applied_];
    void *arg = (write_args_.count(e.id()) ? write_args_[e.id()] : nullptr);
    const Status s = on_append_(e.msg(), arg);
    if (write_statuses_.count(e.id()))
      write_statuses_[e.id()] = make_unique<Status>(s);
  }

  const Time now = clock_->TimeNow();

  if (state_ == FOLLOWER) {
    if (now - last_heartbeat_time_ >= election_timeout_) StartElection(now);
  }

  if (state_ == CANDIDATE) {
    if ((votes_for_me_.size() * 2ULL) > cluster_.size()) {
      state_ = LEADER;
      leader_ = cluster_.me();
      for (const std::string &member : cluster_.others()) {
        next_index_[member] = log_.size();
        match_index_[member] = 0;
        last_sync_time_[member] = absl::UnixEpoch();
      }
      LOG(INFO) << cluster_.me() << " is now RAFT leader for term " << term_;
    } else if (now - last_heartbeat_time_ >= election_timeout_) {
      StartElection(now);
    }
  }

  if (state_ == LEADER) {
    BroadcastAppendEntries(now);
    CommitEntries();
  }

  // Track current leader state
  VARZ_leader_name = leader_;
  VARZ_is_leader = state_ == LEADER ? 1 : 0;
}

void ServiceImpl::AdvanceTermTo(uint64 term) {
  mu_.AssertHeld();
  if (term > term_) VLOG(2) << cluster_.me() << " advances to term " << term;
  term_ = term;
  state_ = FOLLOWER;
  voted_for_ = "";
  votes_for_me_.clear();
  for (auto &i : hanging_appends_) i.second = true;
}

void ServiceImpl::StartElection(Time now) {
  mu_.AssertHeld();
  state_ = CANDIDATE;
  ++term_;
  leader_ = "";
  voted_for_ = cluster_.me();
  votes_for_me_ = {voted_for_};
  last_heartbeat_time_ = now;
  VLOG(2) << voted_for_ << " starts a new election: term " << term_;
  BroadcastRequestVote();
}

void ServiceImpl::BroadcastRequestVote() {
  mu_.AssertHeld();
  RequestVoteRequest request;
  request.set_term(term_);
  request.set_candidate_id(cluster_.me());
  request.set_last_log_index(log_.size() - 1);
  request.set_last_log_term(log_.back().term());
  const uint64 election_term = term_;
  cluster_.BroadcastRequestVote(
      request, [this, election_term](string_view voter,
                                     const RequestVoteResponse &response) {
        OnVoteReceived(election_term, voter, response);
      });
}

void ServiceImpl::OnVoteReceived(uint64 election_term, string_view voter,
                                 const RequestVoteResponse &response) {
  MutexLock lock(&mu_);
  if (response.term() > term_) {
    AdvanceTermTo(response.term());
    return;
  }
  if (election_term != term_) return;
  if (response.term() != term_) return;
  if (response.vote_granted()) votes_for_me_.insert(std::string(voter));
  if ((votes_for_me_.size() * 2ULL) > cluster_.size()) alarm_thread_.Poke();
}

void ServiceImpl::BroadcastAppendEntries(Time now) {
  mu_.AssertHeld();
  const uint64 leader_term = term_;
  AppendEntriesRequest request;
  request.set_term(leader_term);
  request.set_leader_id(cluster_.me());
  request.set_leader_commit(commit_index_);
  for (const std::string &member : cluster_.others()) {
    const uint64 i = next_index_[member];
    if (i >= log_.size() && now - last_sync_time_[member] < alarm_timeout_)
      continue;
    request.set_prev_log_index(i - 1);
    request.set_prev_log_term(log_[i - 1].term());
    request.clear_entry();
    uint64 j;
    for (j = i; j < log_.size(); ++j) *request.add_entry() = log_[j];

    if (request.entry_size()) {
      VLOG(2) << cluster_.me() << " as leader sends " << request.entry_size()
              << " log entries to " << member;
    }

    cluster_.SendAppendEntries(member, request,
                               [this, leader_term, member, j,
                                now](const AppendEntriesResponse &response) {
                                 OnAppendEntriesResponse(leader_term,
                                                         std::string(member), j, now,
                                                         response);
                               });
  }
}

void ServiceImpl::OnAppendEntriesResponse(
    uint64 leader_term, const std::string &member, uint64 log_size,
    Time request_time, const AppendEntriesResponse &response) {
  MutexLock lock(&mu_);
  if (response.term() > term_) {
    AdvanceTermTo(response.term());
    return;
  }
  if (leader_term < term_) return;
  if (response.success()) {
    if (match_index_[member] < log_size - 1) alarm_thread_.Poke();
    next_index_[member] = log_size;
    match_index_[member] = log_size - 1;
    last_sync_time_[member] = request_time;
  } else if (next_index_[member] > 1) {
    --next_index_[member];
    alarm_thread_.Poke();
  }
}

void ServiceImpl::CommitEntries() {
  mu_.AssertHeld();
  uint64 new_commit_index_ = commit_index_;
  while (++new_commit_index_ < log_.size()) {
    if (log_[new_commit_index_].term() < term_) continue;
    if (log_[new_commit_index_].term() > term_) break;
    size_t k = 1;
    for (const auto &i : match_index_) {
      if (i.second >= new_commit_index_) ++k;
    }
    if (k * 2 <= cluster_.size()) break;
    while (commit_index_ < new_commit_index_) {
      const uint64 id = log_[++commit_index_].id();
      if (hanging_appends_.count(id)) hanging_appends_[id] = true;
    }
  }
}

void ServiceImpl::DumpState() const {
  mu_.AssertHeld();
  std::ostringstream log;
  log << " sz=" << log_.size();
  for (size_t i = 0; i < log_.size(); ++i) {
    log << " T" << log_[i].term() << ":" << log_[i].msg();
    if (i == last_applied_) log << " a";
    if (i == commit_index_) log << " c";
  }
  VLOG(2) << "STATE of " << cluster_.me() << ": T" << term_ << " "
          << (state_ == FOLLOWER ? "F" : (state_ == CANDIDATE ? "C" : "L"))
          << log.str();
}

}  // namespace raft
