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
#include "raft/cluster.h"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "grpcpp/create_channel.h"

namespace raft {

namespace {

using ::absl::Duration;

std::unique_ptr<RaftServiceStubWrapper> MakeStub(const std::string &target,
                                                 const Options &opts) {
  return absl::make_unique<RaftServiceStubWrapper>(
      RaftService::NewStub(
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials())),
      opts);
}

std::map<std::string, std::unique_ptr<RaftServiceStubWrapper>> MakeStubs(
    const Options &opts) {
  std::map<std::string, std::unique_ptr<RaftServiceStubWrapper>> stubs;
  for (const std::string &target : opts.targets) {
    CHECK(!target.empty()) << "Empty target in Options.targets";
    CHECK(!stubs.count(target))
        << "Duplicate target (" << target << ") in Options.targets";
    stubs[target] = MakeStub(target, opts);
  }
  if (!stubs.count(opts.my_target)) {
    CHECK(!opts.my_target.empty()) << "Empty Options.my_target";
    stubs[opts.my_target] = MakeStub(opts.my_target, opts);
  }
  return stubs;
}

// Returns the sorted list of |m|'s keys, except for |me|.
template <class T>
std::vector<std::string> GetOthers(const std::string &me, const std::map<std::string, T> &m) {
  std::vector<std::string> v;
  for (const auto &i : m)
    if (i.first != me) v.push_back(i.first);
  return v;
}

// Returns the index of |me| in the sorted list of keys of |m|.
template <class T>
int ComputeKeyIndexIn(const std::string &me, const std::map<std::string, T> &m) {
  int ans = 0;
  for (const auto &i : m) {
    if (i.first == me)
      break;
    else
      ++ans;
  }
  return ans;
}

// A non-blocking RequestVote RPC that deletes itself on response.
struct RequestVoteRpc {
  std::string member;
  RequestVoteRequest request;
  std::function<void(absl::string_view voter, const RequestVoteResponse &)> on_vote;
  std::unique_ptr<::util::CompletionCallbackIntf> rpc;

  // TODO: implement timeout
  RequestVoteRpc(
      const std::string &member, RaftServiceStubWrapper *stub, Duration timeout,
      const RequestVoteRequest &source_request,
      std::function<void(absl::string_view voter, const RequestVoteResponse &)>
          on_vote)
      : member(member), request(source_request), on_vote(on_vote) {
    // rpc->set_deadline(timeout);
    rpc = stub->RequestVote(
        request,
        [this](grpc::Status status,
               const ::google::protobuf::Message *response) -> void {
          if (status.ok())
            this->on_vote(
                this->member,
                *reinterpret_cast<const RequestVoteResponse *>(response));
          delete this;
        });
  }

  void OnResponse() {}
};

// A non-blocking AppendEntries RPC that deletes itself on response.
struct AppendEntriesRpc {
  std::string member;
  std::unique_ptr<::util::CompletionCallbackIntf> rpc;
  AppendEntriesRequest request;
  std::function<void(const AppendEntriesResponse &)> on_response;

  // TODO: implement timeout
  AppendEntriesRpc(
      const std::string &member, RaftServiceStubWrapper *stub, Duration timeout,
      const AppendEntriesRequest &source_request,
      std::function<void(const AppendEntriesResponse &)> on_response)
      : member(member), request(source_request), on_response(on_response) {
    // rpc->set_deadline(timeout);
    rpc = stub->AppendEntries(
        request,
        [this](grpc::Status status,
               const ::google::protobuf::Message *response) -> void {
          if (status.ok()) {
            this->on_response(
                *reinterpret_cast<const AppendEntriesResponse *>(response));
          }
          delete this;
        });
  }
};

// Makes a map<A, B> from vector<A> and vector<B>.
template <class A, class B>
std::map<A, B> Zip(std::vector<A> &&a, std::vector<B> &&b) {
  CHECK_EQ(a.size(), b.size());
  std::map<A, B> m;
  for (size_t i = 0; i < a.size(); ++i) m[std::move(a[i])] = std::move(b[i]);
  return m;
}

}  // namespace

Cluster::Cluster(const Options &opts)
    : my_target_(opts.my_target),
      request_vote_rpc_timeout_(opts.request_vote_rpc_timeout),
      append_entries_rpc_timeout_(opts.append_entries_rpc_timeout),
      stubs_(MakeStubs(opts)),
      others_(GetOthers(my_target_, stubs_)),
      my_index_(ComputeKeyIndexIn(my_target_, stubs_)) {}

Cluster::Cluster(std::vector<std::string> &&names,
                 std::vector<std::unique_ptr<RaftServiceStubWrapper>> &&stubs)
    : my_target_(names[0]),
      request_vote_rpc_timeout_(::absl::Seconds(1)),
      append_entries_rpc_timeout_(::absl::Seconds(1)),
      stubs_(Zip(std::move(names), std::move(stubs))),
      others_(GetOthers(my_target_, stubs_)),
      my_index_(ComputeKeyIndexIn(my_target_, stubs_)) {}

void Cluster::BroadcastRequestVote(
    const RequestVoteRequest &request,
    std::function<void(absl::string_view member, const RequestVoteResponse &)>
        on_vote) const {
  for (const std::string &voter : others_) {
    DCHECK_EQ(me(), request.candidate_id());
    DCHECK(voter != request.candidate_id());
    RaftServiceStubWrapper *stub = stubs_.at(voter).get();
    new RequestVoteRpc(  // yep!
        voter, stub, request_vote_rpc_timeout_, request, on_vote);
  }
}

void Cluster::SendAppendEntries(
    const std::string &member, const AppendEntriesRequest &request,
    std::function<void(const AppendEntriesResponse &)> on_response) const {
  DCHECK(member != me());
  RaftServiceStubWrapper *const stub = stubs_.at(std::string(member)).get();
  new AppendEntriesRpc(  // uh-huh!
      member, stub, append_entries_rpc_timeout_, request, on_response);
}

bool Cluster::SendAppendOnLeader(const std::string &leader,
                                 const LogEntry &e) const {
  AppendOnLeaderRequest request;
  *request.mutable_entry() = e;
  AppendOnLeaderResponse response;
  RaftServiceStubWrapper *const stub = stubs_.at(leader).get();

  auto rpc = stub->AppendOnLeader(request);

  if (rpc->Await() && rpc->Status().ok()) {
    response = rpc->Message();
    return true;
  }

  return false;
}

}  // namespace raft
