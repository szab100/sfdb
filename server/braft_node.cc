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

#include "server/braft_node.h"

#include <atomic>
#include <string>

#include "absl/memory/memory.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "braft/configuration_manager.h"
#include "braft/node.h"
#include "braft/raft.h"
#include "braft/util.h"
#include "brpc/closure_guard.h"
#include "butil/endpoint.h"
#include "glog/logging.h"
#include "server/braft_state_machine_impl.h"

namespace sfdb {

BraftNode::~BraftNode() = default;

// Because this class accesses external resources(network) it
// may fail, so do lazy initialization via Start method.
bool BraftNode::Start(const BraftNodeOptions &options,
                      const BraftExecSqlHandler &exec_sql_handler) {
  CHECK(!node_) << "BraftNode already started";
  CHECK(!state_machine_) << "BraftNode already started";

  auto state_machine = absl::make_unique<BraftStateMachineImpl>(
      exec_sql_handler, [this](::sfdb::ExecSqlResponse *response) {
        this->FillRedirectResponse(response);
      });

  butil::EndPoint ep;
  if (hostname2endpoint(options.host.c_str(), options.port, &ep) != 0) {
    LOG(ERROR) << "Failed to parse host/port";
    return false;
  }

  // Parse options.raft_members and convert hostnames to IPs
  std::string raft_member_ips;
  for (const auto& part : absl::StrSplit(options.raft_members, ",")) {
    const std::vector<std::string> address_parts = absl::StrSplit(part, ":");
    if (address_parts.size() < 2) {
      LOG(ERROR) << "Failed to parse host/port of RAFT cluster member";
      return false;
    }

    int port;
    if (!absl::SimpleAtoi(address_parts[1], &port)) {
      LOG(ERROR) << "Failed to parse port of RAFT cluster member";
      return false;
    }

    butil::EndPoint member_ep;
    if (butil::hostname2endpoint(address_parts[0].c_str(), port, &member_ep) != 0) {
      LOG(ERROR) << "Failed to create endpoint from host/port of RAFT cluster member";
      return false;
    }

    if (!raft_member_ips.empty()) {
      raft_member_ips += ',';
    }
    auto ep_str = butil::endpoint2str(member_ep);
    raft_member_ips += ep_str.c_str();
  }

  ::braft::NodeOptions node_options;
  if (node_options.initial_conf.parse_from(raft_member_ips) != 0) {
    LOG(ERROR) << "Failed to parse cluster config";
    return false;
  }

  node_options.election_timeout_ms = 5000;
  node_options.fsm = state_machine.get();
  node_options.node_owns_fsm = false;
  node_options.snapshot_interval_s = 0;
  std::string prefix = "local://tmp/" + std::to_string(options.port);
  node_options.log_uri = prefix + "/log";
  node_options.raft_meta_uri = prefix + "/raft_meta";
  node_options.snapshot_uri = prefix + "/snapshot";
  node_options.disable_cli = false;

  auto node = absl::make_unique<::braft::Node>(options.group_name,
                                               ::braft::PeerId(ep));

  if (node->init(node_options) != 0) {
    LOG(ERROR) << "Failed to initialize BRAFT node";
    return false;
  }

  state_machine_.swap(state_machine);
  node_.swap(node);

  return true;
}

void BraftNode::Stop() {
  CHECK(node_);

  if (node_) {
    node_->shutdown(NULL);
  }
}

void BraftNode::WaitTillStopped() {
  CHECK(node_);

  if (node_) {
    node_->join();
  }
}

void BraftNode::FillRedirectResponse(ExecSqlResponse *response) const {
  CHECK(node_);
  ::braft::PeerId leader = node_->leader_id();

  if (!leader.is_empty()) {
    const auto leader_url = leader.to_string();
    LOG(INFO) << "Redirecting to new leader: " << leader_url;
    response->set_redirect(leader_url);
    response->set_status(ExecSqlResponse::REDIRECT);
  } else {
    LOG(INFO) << "Failed to redirect to a new leader...";
    response->set_status(ExecSqlResponse::ERROR);
  }
}

void BraftNode::ExecSql(const ExecSqlRequest *request,
                        ExecSqlResponse *response,
                        google::protobuf::Closure *done) {
  CHECK(state_machine_);

  brpc::ClosureGuard done_guard(done);

  const int64_t term = state_machine_->CurrentTerm();

  if (term < 0) {
    FillRedirectResponse(response);
    return;
  }

  butil::IOBuf log;
  butil::IOBufAsZeroCopyOutputStream wrapper(&log);
  if (!request->SerializeToZeroCopyStream(&wrapper)) {
    LOG(ERROR) << "Failed to serialize request";
    response->set_status(ExecSqlResponse::ERROR);
    return;
  }
  // Apply this log as a braft::Task
  braft::Task task;
  task.data = &log;
  // This callback would be iovoked when the task actually excuted or
  // fail
  task.done = new BraftSqlExecClosure(state_machine_.get(), request, response,
                                      done_guard.release());

  task.expected_term = term;

  // Now the task is applied to the group, waiting for the result.
  return node_->apply(task);
}

}  // namespace sfdb