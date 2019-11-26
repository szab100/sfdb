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

#ifndef SERVER_GRPC_SFDB_SERVICE_IMPL_H_
#define SERVER_GRPC_SFDB_SERVICE_IMPL_H_

#include "server/grpc_service.grpc.pb.h"
#include "sfdb/api.pb.h"

namespace sfdb {
class GrpcModules;
// Implements the gRPC service defined in api.proto.
//
// Thread-safe.
class GrpcSfdbServiceImpl : public SfdbService::Service {
 public:
  explicit GrpcSfdbServiceImpl(GrpcModules *modules);

  ~GrpcSfdbServiceImpl() = default;
  GrpcSfdbServiceImpl(const GrpcSfdbServiceImpl &) = delete;
  GrpcSfdbServiceImpl(const GrpcSfdbServiceImpl &&) = delete;
  GrpcSfdbServiceImpl &operator=(const GrpcSfdbServiceImpl &) = delete;
  GrpcSfdbServiceImpl &operator=(const GrpcSfdbServiceImpl &&) = delete;

  ::grpc::Status ExecSql(grpc::ServerContext *context,
                         const ExecSqlRequest *request,
                         ExecSqlResponse *response) override;

 private:
  GrpcModules *const modules_;
};

}  // namespace sfdb

#endif  // SERVER_GRPC_SFDB_SERVICE_IMPL_H_
