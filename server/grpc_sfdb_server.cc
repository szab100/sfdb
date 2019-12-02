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

#include "server/grpc_sfdb_server.h"

#include "absl/memory/memory.h"
#include "server/grpc_modules.h"
#include "server/grpc_sfdb_service_impl.h"

namespace sfdb {

bool GrpcSfdbServer::StartAndWait(const std::string &host, int port,
                                  const std::string &raft_targets) {
  modules_->Init(host, port, raft_targets);

  auto server_builder = modules_->server_builder();
  server_builder->RegisterService(service_impl_.get());
  auto server = server_builder->BuildAndStart();

  VLOG(2) << "RAFT GRPC server started on " << host << ":" << port;

  server->Wait();

  return true;
}

bool GrpcSfdbServer::Stop() {}

GrpcSfdbServer::GrpcSfdbServer()
    : modules_(absl::make_unique<GrpcModules>()),
      service_impl_(absl::make_unique<GrpcSfdbServiceImpl>(modules_.get())) {}

GrpcSfdbServer::~GrpcSfdbServer() = default;
}  // namespace sfdb