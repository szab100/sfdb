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
#include "braft/raft.h"
#include "butil/at_exit.h"
#include "server/braft_node.h"
#include "server/brpc_sfdb_service_impl.h"
#include "server/braft_state_machine_impl.h"

namespace sfdb {

BrpcSfdbServerImpl::BrpcSfdbServerImpl()
    : at_exit_wrapper_(absl::make_unique<butil::AtExitManager>()),
      node_(absl::make_unique<BraftNode>()),
      service_impl_(absl::make_unique<BrpcSfdbServiceImpl>(node_.get()))
      {}

BrpcSfdbServerImpl::~BrpcSfdbServerImpl() = default;

bool BrpcSfdbServerImpl::Start(const std::string &host, int port,
                               const std::string &raft_targets,
                               const BraftExecSqlHandler &exec_sql_handler) {
  CHECK(!server_) << "server already started";

  auto server = absl::make_unique<brpc::Server>();

  if (server->AddService(service_impl_.get(),
                             brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Failed to add BrpcSfdbServiceImpl";
    return false;
  }

  if (braft::add_service(server.get(), port) != 0) {
    LOG(ERROR) << "Failed to add BRAFT service";
    return false;
  }

  if (server->Start(port, NULL) != 0) {
    LOG(ERROR) << "Failed to start server";
    return false;
  }

  BraftNodeOptions opts;
  opts.host = host;
  opts.port = port;
  opts.raft_members = raft_targets;

  if (!node_->Start(opts, exec_sql_handler)) {
    LOG(ERROR) << "Failed to start BRAFT node";
    return false;
  }

  server_ = std::move(server);

  LOG(INFO) << "BRAFT BRPC server started at " << host << ":" << port;

  return true;
}

void BrpcSfdbServerImpl::Stop() {
  node_->Stop();
  server_->Stop(1000);

  node_->WaitTillStopped();
  server_->Join();

  server_ = nullptr;
}

void BrpcSfdbServerImpl::WaitTillStopped() {
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
}

}  // namespace sfdb