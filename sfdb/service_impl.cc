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
#include <memory>
#include <vector>

#include "glog/logging.h"
#include "sfdb/service_impl.h"

namespace sfdb {

using ::google::protobuf::Message;

SfdbServiceImpl::SfdbServiceImpl(Modules *modules) : modules_(modules) {}

::grpc::Status SfdbServiceImpl::ExecSql(grpc::ServerContext *context,
                                        const ExecSqlRequest *request,
                                        ExecSqlResponse *response) {
  VLOG(2) << "Got SQL: " << request->sql();
  ::grpc::Status ret = db()->ExecSql(*request, response);
  return ret;
}

} // namespace sfdb
