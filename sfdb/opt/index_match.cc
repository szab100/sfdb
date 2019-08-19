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
#include "sfdb/opt/index_match.h"

#include <memory>

#include "absl/memory/memory.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/db.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/engine/set_field.h"
#include "util/task/status.h"

namespace sfdb {
namespace {

using ::absl::make_unique;
using ::google::protobuf::Message;
using ::util::Status;

// Creates a partial row Message with a field set to the given value.
std::unique_ptr<Message> MakeBoundMessage(
    const TableIndex &index, const Value &v) {
  // TODO: this only works for single-column indices.
  std::unique_ptr<Message> msg = index.t->pool->NewMessage(index.t->type);
  Status s = SetField(v, index.columns[0], index.t->pool.get(), msg.get());
  CHECK_OK(s) << "botva";
  return msg;
}

}  // namespace

bool IndexMatchesWhereExpression(const TableIndex &index, const TypedAst &ast) {
  // TODO: For now, this only matches single-column indices to ASTs of type
  //       <column> = <value>, in that exact order.
  if (index.columns.size() != 1) return false;
  if (ast.type != Ast::OP_EQ) return false;
  if (ast.lhs()->type != Ast::VAR) return false;
  if (ast.lhs()->var() != index.columns[0]->name()) return false;
  if (ast.rhs()->type != Ast::VALUE) return false;
  return true;
}

std::unique_ptr<TypedAst> RebuildAstUsingIndex(
    const TableIndex &index, std::unique_ptr<TypedAst> &&ast) {
  // TODO: same caveats as above
  std::unique_ptr<Message> begin_msg =
      MakeBoundMessage(index, ast->lhs()->rhs()->value());
  std::unique_ptr<Message> end_msg =
      MakeBoundMessage(index, ast->lhs()->rhs()->value());
  auto lhs = std::unique_ptr<TypedAst>(new TypedAst(
      Ast::VALUE, "", "", nullptr, nullptr, Value::Bool(true), {}, {}, {}, "",
      {}, ast->lhs()->result_type));
  auto begin = std::unique_ptr<TypedAst>(new TypedAst(
      Ast::INDEX_SCAN_BOUND_INCLUSIVE, "", "", nullptr, nullptr,
      Value::Message(std::move(begin_msg)), {}, {}, {}, "", {},
      AstType::Void()));
  auto end = std::unique_ptr<TypedAst>(new TypedAst(
      Ast::INDEX_SCAN_BOUND_INCLUSIVE, "", "", nullptr, nullptr,
      Value::Message(std::move(end_msg)), {}, {}, {}, "", {},
      AstType::Void()));
  auto rhs = std::unique_ptr<TypedAst>(new TypedAst(
      Ast::INDEX_SCAN, "", std::string(index.name), std::move(begin), std::move(end),
      Value::Bool(false), {}, {}, {}, "", {},
      AstType::RepeatedMessage(index.t->type)));
  return make_unique<TypedAst>(
      ast->type, std::move(ast->table_name_), std::move(ast->index_name_),
      std::move(lhs), std::move(rhs), std::move(ast->value_),
      std::move(ast->columns_), std::move(ast->column_types_),
      std::move(ast->values_), std::move(ast->var_),
      std::move(ast->column_indices_), ast->result_type);
}

}  // namespace sfdb
