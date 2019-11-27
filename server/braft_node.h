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

#ifndef SERVER_BRAFT_NODE_H_
#define SERVER_BRAFT_NODE_H_

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "server/common_types.h"
#include "sfdb/api.pb.h"  // ExecSqlRequest and ExecSqlResponse

namespace braft {
class Node;
}  // namespace braft

namespace sfdb {
class BraftStateMachineImpl;

// TODO: implement generic class for both implementations of RAFT
class BraftNodeOptions {
 public:
  std::string host;
  int port;
  std::string raft_members;
  std::string group_name;
};

class BraftNode {
 public:
  ~BraftNode();
  void ExecSql(const ExecSqlRequest *request, ExecSqlResponse *response,
               google::protobuf::Closure *done);

  bool Start(const BraftNodeOptions &options,
             const BraftExecSqlHandler &exec_sql_cb);
  void Stop();
  void WaitTillStopped();

 private:
  void FillRedirectResponse(ExecSqlResponse *response) const;

 private:
  BraftExecSqlHandler exec_sql_handler_;
  std::unique_ptr<BraftStateMachineImpl> state_machine_;
  std::unique_ptr<::braft::Node> node_;
};

}  // namespace sfdb

#endif  // SERVER_BRAFT_NODE_H_