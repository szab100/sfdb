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
#include "sfdb/base/ast.h"

#include <iterator>
#include <ostream>
#include <string>

namespace sfdb {

bool Ast::IsMutation() const {
  switch (type) {
    case CREATE_TABLE:
    case CREATE_INDEX:
    case DROP_TABLE:
    case DROP_INDEX:
    case INSERT:
    case UPDATE:
      return true;
    case IF:
      CHECK(lhs());
      CHECK(rhs());
      return lhs()->IsMutation() || rhs()->IsMutation();
    default:
      return false;
  }
}

// static
std::string Ast::TypeToString(Type t) {
  switch (t) {
#define C(x) case x: return #x
    C(ERROR);
    C(STAR);
    C(IF); C(EXISTS);
    C(SHOW_TABLES); C(DESCRIBE_TABLE);
    C(CREATE_TABLE); C(CREATE_INDEX); C(DROP_TABLE); C(DROP_INDEX);
    C(INSERT); C(UPDATE); C(SINGLE_EMPTY_ROW); C(TABLE_SCAN); C(INDEX_SCAN);
    C(INDEX_SCAN_BOUND_EXCLUSIVE); C(INDEX_SCAN_BOUND_INCLUSIVE);
    C(VALUE); C(VAR); C(FUNC); C(FILTER); C(GROUP_BY); C(ORDER_BY); C(MAP);
    C(OP_IN); C(OP_LIKE); C(OP_OR); C(OP_AND); C(OP_NOT);
    C(OP_EQ); C(OP_LT); C(OP_GT); C(OP_LE); C(OP_GE); C(OP_NE);
    C(OP_PLUS); C(OP_MINUS); C(OP_BITWISE_AND); C(OP_BITWISE_OR);
    C(OP_BITWISE_XOR); C(OP_MUL); C(OP_DIV); C(OP_MOD); C(OP_BITWISE_NOT);
#undef C
  }

  LOG(FATAL) << "Unknown type " << t << " in Ast::TypeToString";
  return "";
}

// static
bool Ast::IsUnaryOp(Type t) {
  switch (t) {
    case OP_NOT:
    case OP_BITWISE_NOT:
    case OP_MINUS:
      return true;
    default:
      return false;
  }
}

// static
bool Ast::IsBinaryOp(Type t) {
  switch (t) {
    case OP_IN:
    case OP_LIKE:
    case OP_OR:
    case OP_AND:
    case OP_EQ:
    case OP_LT:
    case OP_GT:
    case OP_LE:
    case OP_GE:
    case OP_NE:
    case OP_BITWISE_AND:
    case OP_BITWISE_OR:
    case OP_BITWISE_XOR:
    case OP_MUL:
    case OP_DIV:
    case OP_MOD:
    case OP_PLUS:
    case OP_MINUS:
      return true;
    default:
      return false;
  }
}

std::unique_ptr<Ast> Ast::Clone(const Ast *ast) {
  const Ast *lhs = ast->lhs();
  std::unique_ptr<Ast> cloned_lhs;
  if (lhs) {
    cloned_lhs = Ast::Clone(lhs);
  }

  const Ast *rhs = ast->rhs();
  std::unique_ptr<Ast> cloned_rhs;
  if (rhs) {
    cloned_rhs = Ast::Clone(rhs);
  }

  std::vector<std::unique_ptr<Ast>> values;
  std::transform(ast->values_.begin(), ast->values_.end(), std::back_inserter(values),
    [](const std::unique_ptr<Ast> &v) -> std::unique_ptr<Ast> {
      return Ast::Clone(v.get());
    }
  );

  Value value = ast->value_;
  std::vector<std::string> columns = ast->columns();
  std::vector<std::string> column_types = ast->column_types();
  std::vector<int> column_indices = ast->column_indices();

  return std::unique_ptr<Ast>(new Ast(
    ast->type,
    ast->table_name_,
    ast->index_name_,
    std::move(cloned_lhs),
    std::move(cloned_rhs),
    std::move(value),
    std::move(columns),
    std::move(column_types),
    std::move(values),
    ast->var_,
    std::move(column_indices)
  ));
}

}  // namespace sfdb
