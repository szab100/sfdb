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
#include "server/brpc_sfdb_server_impl.h"

#include "absl/memory/memory.h"
#include "brpc/server.h"
#include "butil/at_exit.h"
#include "server/braft_node.h"
#include "server/brpc_sfdb_service_impl.h"

namespace sfdb {

BrpcSfdbServerImpl::BrpcSfdbServerImpl() {}
BrpcSfdbServerImpl::~BrpcSfdbServerImpl() = default;

bool BrpcSfdbServerImpl::Start(const std::string &host, int port,
                               const std::string &raft_targets,
                               const BraftExecSqlHandler &exec_sql_handler) {
  return true;
}

void BrpcSfdbServerImpl::Stop() {}

void BrpcSfdbServerImpl::WaitTillStopped() {}
}  // namespace sfdb