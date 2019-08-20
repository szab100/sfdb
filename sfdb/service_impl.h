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
#ifndef SFDB_SERVICE_IMPL_H_
#define SFDB_SERVICE_IMPL_H_

#include "sfdb/api.grpc.pb.h"
#include "sfdb/api.pb.h"
#include "sfdb/base/replicated_db.h"
#include "sfdb/modules.h"

namespace sfdb {

// Implements the gRPC service defined in api.proto.
//
// Thread-safe.
class SfdbServiceImpl : public SfdbService::Service {
public:
  explicit SfdbServiceImpl(Modules *modules);

  ~SfdbServiceImpl() = default;
  SfdbServiceImpl(const SfdbServiceImpl &) = delete;
  SfdbServiceImpl(const SfdbServiceImpl &&) = delete;
  SfdbServiceImpl &operator=(const SfdbServiceImpl &) = delete;
  SfdbServiceImpl &operator=(const SfdbServiceImpl &&) = delete;

  ::grpc::Status ExecSql(grpc::ServerContext *context,
                         const ExecSqlRequest *request,
                         ExecSqlResponse *response) override;

private:
  Modules *modules_;

  ReplicatedDb *db() { return modules_->db(); }
};

} // namespace sfdb

#endif // SFDB_SERVICE_IMPL_H_
