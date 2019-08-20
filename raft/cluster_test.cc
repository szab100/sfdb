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

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "raft/raft_mock.grpc.pb.h"
#include "util/grpc/async_stub_testing.h"
#include "util/proto/matchers.h"
#include "util/proto/parse_text_proto.h"
#include "util/task/canonical_errors.h"
#include "util/thread/concurrent_queue.h"

namespace raft {

namespace {
using ::raft::MockRaftServiceStub;
using ::raft::RequestVoteResponse;

Options GetTestOpts() {
  Options opts;
  opts.num_dispatch_threads = 0;

  return opts;
}

class RaftServiceStubMock : public RaftServiceStubWrapper {
 public:
  RaftServiceStubMock()
      : RaftServiceStubWrapper(
            std::move(absl::make_unique<MockRaftServiceStub>()),
            GetTestOpts()) {}

  MockRaftServiceStub *stub() {
    return static_cast<MockRaftServiceStub *>(stub_.get());
  }
};
}  // namespace

TEST(ClusterTest, BroadcastRequestVote) {
  const RequestVoteRequest request = PARSE_TEST_PROTO(R"(
    term: 2
    candidate_id: "a"
    last_log_index: 0 last_log_term: 1)");

  RequestVoteResponse response1 = PARSE_TEST_PROTO(R"(
    term: 2
    vote_granted: false)");

  RequestVoteResponse response2 = PARSE_TEST_PROTO(R"(
    term: 2 vote_granted: true)");

  std::vector<std::unique_ptr<RaftServiceStubWrapper>> stubs;

  auto stub_a = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_a->stub(), RequestVote).Times(0);
  stubs.push_back(std::move(stub_a));

  auto stub_b = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_b->stub(), RequestVote)
      .Times(1)
      .WithRequest(request)
      .WithResponse(response1);
  stubs.push_back(std::move(stub_b));

  auto stub_c = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_c->stub(), RequestVote)
      .Times(1)
      .WithRequest(request)
      .WithResponse(response2);
  stubs.push_back(std::move(stub_c));

  auto stub_d = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_d->stub(), RequestVote)
      .Times(1)
      .WithRequest(request)
      .WithResponseError(grpc::Status::CANCELLED);
  stubs.push_back(std::move(stub_d));

  auto stub_e = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_e->stub(), RequestVote)
      .Times(1)
      .WithRequest(request)
      .WithResponseError(grpc::Status::CANCELLED);
  stubs.push_back(std::move(stub_e));

  Cluster c({"a", "b", "c", "d", "e"}, std::move(stubs));

  EXPECT_EQ(5, c.size());
  EXPECT_EQ("a", c.me());

  WaitQueue<bool> q;
  c.BroadcastRequestVote(request,
                         [&q](absl::string_view voter, const RequestVoteResponse &r) {
                           CHECK_EQ(2U, r.term());
                           q.push(r.vote_granted());
                         });

  bool seen_false = false;
  bool seen_true = false;
  for (int i = 0; i < 2; ++i) {
    bool x;
    q.pop(x); // Wait for item
    (x ? seen_true : seen_false) = true;
  }
  EXPECT_TRUE(seen_false);
  EXPECT_TRUE(seen_true);
}

TEST(ClusterTest, SendAppendEntries) {
  const AppendEntriesRequest request = PARSE_TEST_PROTO(R"(
    term: 3
    leader_id: "a"
    prev_log_index: 13
    prev_log_term: 2
    entry: { term: 2 msg: "hello" }
    entry: { term: 3 msg: "world" }
    leader_commit: 11)");

  AppendEntriesResponse response = PARSE_TEST_PROTO(R"(
    term: 3 success: true)");

  std::vector<std::unique_ptr<RaftServiceStubWrapper>> stubs;

  auto stub_a = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_a->stub(), AppendEntries).Times(0);
  stubs.push_back(std::move(stub_a));

  auto stub_b = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_b->stub(), AppendEntries)
      .Times(1)
      .WithRequest(request)
      .WithResponse(response);
  stubs.push_back(std::move(stub_b));

  auto stub_c = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_c->stub(), AppendEntries).Times(0);
  stubs.push_back(std::move(stub_c));

  Cluster c({"a", "b", "c"}, std::move(stubs));

  EXPECT_EQ(3, c.size());
  EXPECT_EQ("a", c.me());

  WaitQueue<AppendEntriesResponse> q;
  c.SendAppendEntries("b", request,
                      [&q](const AppendEntriesResponse &r) { q.push(r); });

  // TODO: not needed because we check it when calling WillCallAppendEntries
  AppendEntriesResponse expected_response;
  q.pop(expected_response); // Wait for response
  EXPECT_EQ(3, expected_response.term());
  EXPECT_TRUE(expected_response.success());
}

TEST(ClusterTest, SendAppendOnLeader) {
  AppendOnLeaderRequest request = PARSE_TEST_PROTO(R"(
    entry: { term: 2 id: 12345 msg: "hello, world" })");

  AppendOnLeaderResponse response = PARSE_TEST_PROTO(R"()");

  std::vector<std::unique_ptr<RaftServiceStubWrapper>> stubs;
  auto stub_a = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_a->stub(), AppendOnLeader).Times(0);
  stubs.push_back(std::move(stub_a));

  auto stub_b = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_b->stub(), AppendOnLeader)
      .WithRequest(request)
      .WithResponse(response);
  stubs.push_back(std::move(stub_b));

  auto stub_c = absl::make_unique<RaftServiceStubMock>();
  EXPECT_ASYNC_RPC1(*stub_c->stub(), AppendOnLeader).Times(0);
  stubs.push_back(std::move(stub_c));

  Cluster c({"b", "a", "c"}, std::move(stubs));

  EXPECT_EQ(3, c.size());
  EXPECT_EQ("b", c.me());
  EXPECT_EQ(1, c.my_index());

  const LogEntry e = PARSE_TEST_PROTO(R"(
    term: 2 id: 12345 msg: "hello, world")");

  EXPECT_TRUE(c.SendAppendOnLeader("a", e));
}

}  // namespace raft
