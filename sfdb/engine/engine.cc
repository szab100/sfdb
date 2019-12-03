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
#include "sfdb/engine/engine.h"

#include <memory>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/db.h"
#include "sfdb/base/proto_stream.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/base/value.h"
#include "sfdb/engine/create_and_drop.h"
#include "sfdb/engine/expressions.h"
#include "sfdb/engine/infer_result_types.h"
#include "sfdb/engine/insert.h"
#include "sfdb/engine/proto_streams.h"
#include "sfdb/engine/select.h"
#include "sfdb/engine/update.h"
#include "sfdb/engine/utils.h"
#include "sfdb/opt/opt.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

namespace {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::OutOfRangeError;
using ::util::UnimplementedError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

}  // namespace

StatusOr<std::unique_ptr<ProtoStream>> GetProtoStream(const TypedAst &ast,
                                                      ProtoPool *pool,
                                                      const Db *db) {
  switch(ast.type) {
    case Ast::Type::SHOW_TABLES:
      return ExecuteShowTables(ast, db);
    case Ast::Type::DESCRIBE_TABLE:
      return ExecuteDescribeTable(ast, db);
    default:
      return ExecuteSelect(ast, pool, db);
  }
}

Status ExecuteRead(
    std::unique_ptr<Ast> &&ast, ProtoPool *pool, const Db *db,
    std::vector<std::unique_ptr<Message>> *rows) {
  CHECK(!ast->IsMutation());
  ::absl::ReaderMutexLock lock(&db->mu);

  // Performs Ast preprocessing in given context. Basically it
  // expands "*" in SELECT to full list of columns.
  StatusOr<std::unique_ptr<Ast>> so = ExpandAst(
      std::move(ast), pool, db, db->vars.get());

  if (!so.ok()) return so.status();

  StatusOr<std::unique_ptr<TypedAst>> so2 = InferResultTypes(
      std::move(so.ValueOrDie()), pool, db, db->vars.get());
  if (!so2.ok()) return so2.status();

  std::unique_ptr<TypedAst> oast = Optimize(*db, std::move(so2.ValueOrDie()));
  StatusOr<std::unique_ptr<ProtoStream>> so3 = GetProtoStream(*oast, pool, db);

  if (!so3.ok()) return so3.status();

  ProtoStream &ps = *so3.ValueOrDie();
  while (ps.ok() && !ps.Done()) {
    rows->push_back(pool->NewMessage(ps->GetDescriptor()));
    rows->back()->CopyFrom(*ps);
    ++ps;
  }
  return ps.status();
}

Status ExecuteWriteAST(TypedAst* ast, ProtoPool *pool, Db *db) {
  switch (ast->type) {
    case Ast::CREATE_TABLE:
      return ExecuteCreateTable(*ast, db);
    case Ast::CREATE_INDEX:
      return ExecuteCreateIndex(*ast, db);
    case Ast::DROP_TABLE:
      return ExecuteDropTable(*ast, db);
    case Ast::DROP_INDEX:
      return ExecuteDropIndex(*ast, db);
    case Ast::INSERT:
      return ExecuteInsert(*ast, db);
    case Ast::UPDATE:
      return ExecuteUpdate(*ast, db);
    case Ast::EXISTS:
      return ExecuteExistsCheck(*ast, db);
    case Ast::IF:
      return ExecuteWriteAST(ast->lhs(), pool, db).ok()
                 ? ExecuteWriteAST(ast->rhs(), pool, db)
                 : OkStatus();
    default:
      return InternalError("Bug in Execute()");
  }
}

Status ExecuteWrite(std::unique_ptr<Ast> &&ast, ProtoPool *pool, Db *db) {
  CHECK(ast->IsMutation());
  ::absl::WriterMutexLock lock(&db->mu);

  StatusOr<std::unique_ptr<TypedAst>> so = InferResultTypes(
      std::move(ast), pool, db, db->vars.get());
  if (!so.ok()) return so.status();

  std::unique_ptr<TypedAst> oast = Optimize(*db, std::move(so.ValueOrDie()));

  return ExecuteWriteAST(oast.get(), pool, db);
}

Status Execute(
    std::unique_ptr<Ast> &&ast, ProtoPool *pool, Db *db,
    std::vector<std::unique_ptr<Message>> *rows) {
  return ast->IsMutation() ?
      ExecuteWrite(std::move(ast), pool, db) :
      ExecuteRead(std::move(ast), pool, db, rows);
}

}  // namespace sfdb
