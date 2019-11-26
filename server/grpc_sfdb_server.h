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

#ifndef SERVER_GRPC_SFDB_SERVER_H_
#define SERVER_GRPC_SFDB_SERVER_H_

#include <memory>
#include <string>

#include "server/server.h"

namespace sfdb {
class GrpcModules;
class GrpcSfdbServiceImpl;

class GrpcSfdbServer : public SfdbServer {
 public:
  bool StartAndWait(const std::string &host, int port,
                    const std::string &raft_targets) override;
  bool Stop() override;

 public:
  GrpcSfdbServer();
  ~GrpcSfdbServer();

 private:
  std::unique_ptr<GrpcModules> modules_;
  std::unique_ptr<GrpcSfdbServiceImpl> service_impl_;
};

}  // namespace sfdb

#endif  // SFDB_SERVER_GRPC_SFDB_SERVER_H_
