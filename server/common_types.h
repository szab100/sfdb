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

#ifndef SERVER_COMMON_TYPES_H_
#define SERVER_COMMON_TYPES_H_

#include <functional>
#include <string>
#include <utility>

#include "sfdb/api.pb.h"
#include "util/task/codes.pb.h"  // ::util::error::Code

namespace sfdb {
// Type to pass result back from sql query execution.
using BraftExecSqlResult = std::pair<::util::error::Code, const std::string>;
using BraftExecSqlHandler = std::function<BraftExecSqlResult(
    const std::string &, ExecSqlResponse *)>;

using BraftRedirectHandler = std::function<void(ExecSqlResponse *)>;
}  // namespace sfdb

#endif  // SERVER_COMMON_TYPES_H_