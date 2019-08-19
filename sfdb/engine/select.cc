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
#include "sfdb/engine/select.h"

#include <memory>

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/empty.pb.h"
#include "sfdb/base/vars.h"
#include "sfdb/engine/expressions.h"
#include "sfdb/engine/proto_streams.h"
#include "sfdb/engine/set_field.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::google::protobuf::Empty;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::util::InternalError;
using ::util::NotFoundError;
using ::util::Status;
using ::util::StatusOr;

std::unique_ptr<ProtoStream> GetSingleEmptyRowProtoStream() {
  std::vector<std::unique_ptr<Message>> v;
  v.emplace_back(new Empty);
  return std::unique_ptr<ProtoStream>(new TmpTableProtoStream(std::move(v)));
}

StatusOr<std::unique_ptr<ProtoStream>> GetTableScanProtoStream(
    const TypedAst &ast, ProtoPool *pool, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  const Table *t = db->FindTable(ast.table_name());
  if (!t) return NotFoundError(StrCat(
      "Table ", ast.table_name(), " not found in database ", db->name));
  return std::unique_ptr<ProtoStream>(new TableProtoStream(t));
}

StatusOr<std::unique_ptr<ProtoStream>> GetFilterProtoStream(
    const TypedAst &ast, ProtoPool *pool, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  StatusOr<std::unique_ptr<ProtoStream>> so =
      GetProtoStream(*ast.rhs(), pool, db);
  if (!so.ok()) return so.status();

  const TypedAst &pred_ast = *ast.lhs();
  auto pred = [&pred_ast, db](const Message &msg) {
    db->mu.AssertReaderHeld();
    std::unique_ptr<Vars> vars = db->vars->Branch(&msg);
    StatusOr<Value> so2 = ExecuteExpression(pred_ast, vars.get());
    if (!so2.ok()) return StatusOr<bool>(so2.status());
    StatusOr<Value> so3 = so2.ValueOrDie().CastTo(FieldDescriptor::TYPE_BOOL);
    if (!so3.ok()) return StatusOr<bool>(so3.status());
    return StatusOr<bool>(so3.ValueOrDie().boo);
  };

  return std::unique_ptr<ProtoStream>(new FilterProtoStream(
      std::move(so.ValueOrDie()), pred));
}

StatusOr<std::unique_ptr<ProtoStream>> GetGroupByProtoStream(
    const TypedAst &ast, ProtoPool *pool, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  return ::util::UnimplementedError("GROUP BY unimplemented");
}

StatusOr<std::unique_ptr<ProtoStream>> GetOrderByProtoStream(
    const TypedAst &ast, ProtoPool *pool, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  return ::util::UnimplementedError("ORDER BY unimplemented");
}

StatusOr<std::unique_ptr<ProtoStream>> GetMapProtoStream(
    const TypedAst &ast, ProtoPool *pool, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  if (ast.columns().size() != ast.values().size())
    return InternalError("Value::Map() is broken");
  if (ast.result_type.type != FieldDescriptor::TYPE_MESSAGE)
    return InternalError("Value::Map() must return a stream of protos.");

  // Get the source of protos.
  StatusOr<std::unique_ptr<ProtoStream>> so =
      GetProtoStream(*ast.rhs(), pool, db);
  if (!so.ok()) return so.status();

  // Define the map function.
  auto f = [&ast, pool, db](const Message &in) {
    db->mu.AssertReaderHeld();
    typedef StatusOr<std::unique_ptr<Message>> Ret;
    std::unique_ptr<Message> out = pool->NewMessage(ast.result_type.d);
    for (size_t i = 0; i < ast.columns().size(); ++i) {
      const FieldDescriptor *fd = ast.result_type.d->FindFieldByNumber(i + 1);
      if (!fd) return Ret(InternalError("Error in ProtoPool"));
      std::unique_ptr<Vars> rhs_vars = db->vars->Branch(&in);
      StatusOr<Value> so2 = ExecuteExpression(*ast.value(i), rhs_vars.get());
      if (!so2.ok()) return Ret(so2.status());
      Status s = SetField(so2.ValueOrDie(), fd, pool, out.get());
      if (!s.ok()) return Ret(s);
    }
    return Ret(std::move(out));
  };

  return std::unique_ptr<ProtoStream>(new MapProtoStream(
      std::move(so.ValueOrDie()), ast.result_type.d, f));
}

}  // namespace

StatusOr<std::unique_ptr<ProtoStream>> GetProtoStream(
    const TypedAst &ast, ProtoPool *p, const Db *db) {
  switch (ast.type) {
    case Ast::ERROR:
      return InternalError("Execute() got an Ast of type ERROR");
    case Ast::SINGLE_EMPTY_ROW:
      return GetSingleEmptyRowProtoStream();
    case Ast::TABLE_SCAN:
      return GetTableScanProtoStream(ast, p, db);
    case Ast::FILTER:
      return GetFilterProtoStream(ast, p, db);
    case Ast::GROUP_BY:
      return GetGroupByProtoStream(ast, p, db);
    case Ast::ORDER_BY:
      return GetOrderByProtoStream(ast, p, db);
    case Ast::MAP:
      return GetMapProtoStream(ast, p, db);
    default:
      return InternalError(StrCat(
          "GetProtoStream() called on Ast of type ",
          Ast::TypeToString(ast.type)));
  }
}

}  // namespace sfdb
