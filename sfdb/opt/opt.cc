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
#include "sfdb/opt/opt.h"

#include <memory>

#include "absl/base/thread_annotations.h"
#include "sfdb/base/db.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/opt/index_match.h"
#include "absl/memory/memory.h"

namespace sfdb {

using ::absl::make_unique;

std::unique_ptr<TypedAst> MaybeUseIndexForUpdate(
    const Db &db, std::unique_ptr<TypedAst> &&ast)
    SHARED_LOCKS_REQUIRED(db.mu) {
  if (ast->type != Ast::UPDATE) return std::move(ast);
  const Table *t = db.FindTable(ast->table_name());
  CHECK(t) << "Table " << ast->table_name() << " not found in DB " << db.name;

  // Find the best index to use for this UDPATE.
  const TableIndex *best_index = nullptr;
  for (const auto &i : t->indices) {
    const TableIndex *index = i.second;
    if (IndexMatchesWhereExpression(*index, *ast->lhs()))
      if (!best_index || index->columns.size() > best_index->columns.size())
        best_index = index;
  }
  if (!best_index) return std::move(ast);
  return RebuildAstUsingIndex(*best_index, std::move(ast));
}

std::unique_ptr<TypedAst> Optimize(
    const Db &db, std::unique_ptr<TypedAst> &&ast) {
  ast = MaybeUseIndexForUpdate(db, std::move(ast));

  // TODO: add more optimizer matchers
  return std::move(ast);
}

}  // namespace sfdb
