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

#include "server/braft_state_machine_impl.h"

#include "braft/util.h"
#include "brpc/closure_guard.h"
#include "glog/logging.h"

namespace sfdb {

void BraftSqlExecClosure::Run() {
  ::brpc::ClosureGuard done_guard(done);
  if (!status().ok()) {
    state_machine->redirect_handler_(response);
  }
  delete this;
}

BraftStateMachineImpl::BraftStateMachineImpl(
    const BraftExecSqlHandler &exec_sql_handler,
    const BraftRedirectHandler &redirect_handler)
    : exec_sql_handler_(exec_sql_handler),
      redirect_handler_(redirect_handler) {}

int64_t BraftStateMachineImpl::CurrentTerm() const {
  return leader_term_.load(std::memory_order_relaxed);
}

void BraftStateMachineImpl::on_apply(::braft::Iterator &iter) {
  // A batch of tasks are committed, which must be processed through
  // |iter|
  for (; iter.valid(); iter.next()) {
    std::string sql_query;
    ExecSqlResponse *response = nullptr;
    // CounterResponse* response = NULL;
    // This guard helps invoke iter.done()->Run() asynchronously to
    // avoid that callback blocks the StateMachine.
    ::braft::AsyncClosureGuard closure_guard(iter.done());
    if (iter.done()) {
      // This task is applied by this node, get value from this
      // closure to avoid additional parsing.
      auto c = static_cast<BraftSqlExecClosure *>(iter.done());
      response = c->response;
      sql_query = c->request->sql();
    } else {
      // Have to parse FetchAddRequest from this log.
      ::butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      ExecSqlRequest request;
      CHECK(request.ParseFromZeroCopyStream(&wrapper));
      sql_query = request.sql();
    }

    auto result = exec_sql_handler_(sql_query, response);
    if (response) {
      if (result.first != ::util::error::OK) {
        LOG(INFO) << "SQL failed: " << result.second;
        response->set_status(ExecSqlResponse::ERROR);
      }
    }
  }
}

void BraftStateMachineImpl::on_leader_start(int64_t term) {
  leader_term_.store(term, butil::memory_order_release);
  LOG(INFO) << "BraftStateMachineImpl::on_leader_start: " << term;
}

void BraftStateMachineImpl::on_leader_stop(const ::butil::Status &status) {
  leader_term_.store(-1, butil::memory_order_release);
  LOG(INFO) << "BraftStateMachineImpl::on_leader_stop: " << status;
}

void BraftStateMachineImpl::on_shutdown() {}

void BraftStateMachineImpl::on_error(const ::braft::Error &e) {
  LOG(INFO) << "BraftStateMachineImpl::on_error: " << e.status();
}

void BraftStateMachineImpl::on_configuration_committed(
    const ::braft::Configuration &conf) {
  LOG(INFO) << "BraftStateMachineImpl::on_configuration_committed";
}

void BraftStateMachineImpl::on_stop_following(
    const ::braft::LeaderChangeContext &ctx) {
  LOG(INFO) << "BraftStateMachineImpl::on_start_following";
}

void BraftStateMachineImpl::on_start_following(
    const ::braft::LeaderChangeContext &ctx) {
  LOG(INFO) << "BraftStateMachineImpl::on_start_following";
}

}  // namespace sfdb