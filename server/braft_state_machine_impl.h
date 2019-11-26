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

#ifndef SERVER_BRAFT_STATE_MACHINE_IMPL_H_
#define SERVER_BRAFT_STATE_MACHINE_IMPL_H_

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "braft/raft.h"
#include "server/common_types.h"
#include "sfdb/api.pb.h"
#include "util/task/codes.pb.h"

namespace sfdb {
class BraftStateMachineImpl;

struct BraftSqlExecClosure : public ::braft::Closure {
  BraftSqlExecClosure(BraftStateMachineImpl *state_machine,
                      const ExecSqlRequest *request, ExecSqlResponse *response,
                      ::google::protobuf::Closure *done)
      : state_machine(state_machine),
        request(request),
        response(response),
        done(done) {}

  void Run() override;

  const BraftStateMachineImpl *const state_machine;
  const ExecSqlRequest *const request;
  ExecSqlResponse *const response;
  ::google::protobuf::Closure *const done;
};

class BraftStateMachineImpl : public ::braft::StateMachine {
 public:
  BraftStateMachineImpl(const BraftExecSqlHandler &exec_sql_cb,
                        const BraftRedirectHandler &redirect_cb);

  int64_t CurrentTerm() const;

  void on_apply(::braft::Iterator &iter) override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const ::butil::Status &status) override;
  void on_shutdown() override;
  void on_error(const ::braft::Error &e) override;
  void on_configuration_committed(const ::braft::Configuration &conf) override;
  void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;
  void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

 private:
  std::atomic<int64_t> leader_term_;

  BraftExecSqlHandler exec_sql_handler_;
  BraftRedirectHandler redirect_handler_;

  friend BraftSqlExecClosure;
};
}  // namespace sfdb
#endif  // SERVER_BRAFT_STATE_MACHINE_IMPL_H_