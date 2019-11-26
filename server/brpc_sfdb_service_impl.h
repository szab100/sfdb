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

#ifndef SERVER_BRPC_SFDB_SERVICE_IMPL_H_
#define SERVER_BRPC_SFDB_SERVICE_IMPL_H_

#include "sfdb/api.pb.h"
#include "server/brpc_service.pb.h"

namespace sfdb {
class BraftNode;

// Implements the BRPC service defined in api.proto.
//
// Thread-safe.

class BrpcSfdbServiceImpl : public SfdbService {
 public:
  explicit BrpcSfdbServiceImpl(BraftNode *node);

  ~BrpcSfdbServiceImpl();
  BrpcSfdbServiceImpl(const BrpcSfdbServiceImpl &) = delete;
  BrpcSfdbServiceImpl(const BrpcSfdbServiceImpl &&) = delete;
  BrpcSfdbServiceImpl &operator=(const BrpcSfdbServiceImpl &) = delete;
  BrpcSfdbServiceImpl &operator=(const BrpcSfdbServiceImpl &&) = delete;


  void ExecSql(::google::protobuf::RpcController* controller,
                       const ::sfdb::ExecSqlRequest* request,
                       ::sfdb::ExecSqlResponse* response,
                       ::google::protobuf::Closure* done) override;
 private:
  BraftNode * const node_;
};

}  // namespace sfdb

#endif  // SERVER_BRPC_SFDB_SERVICE_IMPL_H_
