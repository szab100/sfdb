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
#ifndef SFDB_BASE_REPLICATED_DB_H_
#define SFDB_BASE_REPLICATED_DB_H_

#include "sfdb/api.pb.h"
#include "util/task/status.h"

namespace sfdb {

// Abstract interface.
//
// Implementations must be thread-safe.
class ReplicatedDb {
 public:
  ReplicatedDb() = default;
  virtual ~ReplicatedDb() = default;

  ReplicatedDb(const ReplicatedDb&) = delete;
  ReplicatedDb(ReplicatedDb&&) = delete;
  ReplicatedDb &operator=(const ReplicatedDb&) = delete;
  ReplicatedDb &operator=(ReplicatedDb&&) = delete;

  // Executes a SQL statement. Blocks.
  virtual ::util::Status ExecSql(
      const ExecSqlRequest &request, ExecSqlResponse *response) = 0;
};

}  // namespace sfdb

#endif  // SFDB_BASE_REPLICATED_DB_H_
