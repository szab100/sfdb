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
#ifndef SFDB_BASE_TYPED_AST_H_
#define SFDB_BASE_TYPED_AST_H_

#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/ast_type.h"
#include "util/types/integral_types.h"

namespace sfdb {

// An AST with an inferred AstType.
// All of its children are also guaranteed to be TypedAst objects.
//
// Immutable with the same caveats as Ast.
struct TypedAst : public Ast {
  const AstType result_type;

  // up-converted accessors
  TypedAst *lhs() const { return reinterpret_cast<TypedAst*>(lhs_.get()); }
  TypedAst *rhs() const { return reinterpret_cast<TypedAst*>(rhs_.get()); }
  using Ast::value;  // sometimes, C++ can be weird
  TypedAst *value(int i) const {
    return reinterpret_cast<TypedAst*>(values_[i].get());
  }

  TypedAst(
      Type type, std::string &&table_name, std::string &&index_name,
      std::unique_ptr<Ast> &&lhs, std::unique_ptr<Ast> &&rhs, Value &&value,
      std::vector<std::string> &&columns, std::vector<std::string> &&column_types,
      std::vector<std::unique_ptr<Ast>> &&values, std::string &&var,
      std::vector<int32> &&column_indices, const AstType &result_type)
      : Ast(
        type, table_name, index_name, std::move(lhs), std::move(rhs),
        std::move(value), std::move(columns), std::move(column_types),
        std::move(values), std::move(var), std::move(column_indices)),
        result_type(result_type) {}
};

}  // namespace sfdb

#endif  // SFDB_BASE_TYPED_AST_H_
