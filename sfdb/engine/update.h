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
#ifndef SFDB_ENGINE_UPDATE_H_
#define SFDB_ENGINE_UPDATE_H_

#include "absl/base/thread_annotations.h"
#include "sfdb/base/db.h"
#include "sfdb/base/typed_ast.h"
#include "util/task/status.h"

namespace sfdb {

::util::Status ExecuteUpdate(const TypedAst &ast, Db *db)
    EXCLUSIVE_LOCKS_REQUIRED(db->mu);

}  // namespace sfdb

#endif  // SFDB_ENGINE_UPDATE_H_
