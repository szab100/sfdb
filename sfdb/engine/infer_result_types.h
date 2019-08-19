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
#ifndef SFDB_ENGINE_INFER_RESULT_TYPES_H_
#define SFDB_ENGINE_INFER_RESULT_TYPES_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/db.h"
#include "sfdb/proto/pool.h"
#include "util/task/statusor.h"

namespace sfdb {

// Computes the result type for the given AST and its descendants, recursively.
// Uses |vars| as the var context; set it to db->vars for ASTs executed in |db|.
//
// Destroys the given Ast object and uses its spare parts to build a TypedAst.
::util::StatusOr<std::unique_ptr<TypedAst>> InferResultTypes(
    std::unique_ptr<Ast> &&ast, ProtoPool *pool, const Db *db, const Vars *vars)
    SHARED_LOCKS_REQUIRED(db->mu);

}  // namespace sfdb

#endif  // SFDB_ENGINE_INFER_RESULT_TYPES_H_
