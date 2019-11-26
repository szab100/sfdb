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

#ifndef SERVER_BRPC_SFDB_SERVER_H_
#define SERVER_BRPC_SFDB_SERVER_H_

#include <memory>
#include <string>

#include "server/server.h"

namespace sfdb {
class Db;
class BuiltIns;
class BrpcSfdbServerImpl;

class BrpcSfdbServer : public SfdbServer {
 public:
  BrpcSfdbServer();
  ~BrpcSfdbServer();

  bool StartAndWait(const std::string &host, int port,
                    const std::string &raft_targets) override;
  bool Stop() override;
 private:

 private:
  std::unique_ptr<Db> db_;
  std::unique_ptr<BuiltIns> built_in_vars_;

  std::unique_ptr<BrpcSfdbServerImpl> pimpl_;
};

}  // namespace sfdb

#endif  // SERVER_BRPC_SFDB_SERVER_H_
