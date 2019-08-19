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
#include "sfdb/engine/update.h"

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "glog/logging.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/base/proto_stream.h"
#include "sfdb/base/vars.h"
#include "sfdb/engine/expressions.h"
#include "sfdb/engine/proto_streams.h"
#include "sfdb/engine/set_field.h"
#include "sfdb/proto/pool.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::absl::make_unique;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::util::InternalError;
using ::util::NotFoundError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

std::unique_ptr<TableIndexProtoStream> MakeTableIndexProtoStream(
    const TableIndex &index, const TypedAst &ast) {
  CHECK(ast.type == Ast::INDEX_SCAN);
  CHECK(ast.lhs()->value().type.IsMessage());
  CHECK(ast.rhs()->value().type.IsMessage());

  const TableIndexProtoStream::Bound begin = {
      ast.lhs()->value().msg.get(),
      ast.lhs()->type == Ast::INDEX_SCAN_BOUND_INCLUSIVE,
  };
  const TableIndexProtoStream::Bound end = {
    ast.rhs()->value().msg.get(),
    ast.rhs()->type == Ast::INDEX_SCAN_BOUND_INCLUSIVE,
  };
  return make_unique<TableIndexProtoStream>(index, begin, end);
}

}  // namespace

Status ExecuteUpdate(const TypedAst &ast, Db *db) {
  if (ast.columns().size() != ast.values().size()) return InternalError(StrCat(
      ast.values().size(), " values for ", ast.columns().size(), " columns",
      " in an UPDATE"));

  Table *t = db->FindTable(ast.table_name());
  if (!t) return NotFoundError(StrCat(
      "Table ", ast.table_name(), " not found in database ", db->name));

  // Get the FieldDescriptor of each column to be modified.
  std::vector<const FieldDescriptor*> fds(ast.columns().size());
  for (size_t j = 0; j < ast.columns().size(); ++j) {
    const std::string &col = ast.columns()[j];
    fds[j] = t->type->FindFieldByName(col);
    if (!fds[j]) return NotFoundError(StrCat(
        "No column named ", col, " in ", t->name));
  }

  // Determine whether to use an index.
  std::unique_ptr<ProtoStream> scan;
  if (ast.rhs() && ast.rhs()->type == Ast::INDEX_SCAN) {
    TableIndex *index = db->FindIndex(ast.rhs()->index_name());
    if (!index) return NotFoundError(StrCat(
        "No index named ", ast.rhs()->index_name(), " in database ", db->name));
    scan = MakeTableIndexProtoStream(*index, *ast.rhs());
  } else {
    scan = make_unique<TableProtoStream>(t);
  }
  CHECK(scan);

  // Scan the table to find rows to update.
  std::vector<int> row_indices_to_update;
  for (; !scan->Done(); ++*scan) {
    if (!scan->ok()) return scan->status();
    int i = scan->GetIndexInTable();
    Message *row = t->rows[i].get();
    std::unique_ptr<Vars> vars = db->vars->Branch(row);

    // Evaluate the WHERE clause.
    StatusOr<Value> so = ExecuteExpression(*ast.lhs(), vars.get());
    if (!so.ok()) return so.status();
    StatusOr<Value> so2 = so.ValueOrDie().CastTo(FieldDescriptor::TYPE_BOOL);
    if (!so2.ok()) return so2.status();
    if (!so2.ValueOrDie().boo) continue;

    row_indices_to_update.push_back(i);
  }

  // Update the rows.
  for (int i : row_indices_to_update) {
    Message *row = t->rows[i].get();
    std::unique_ptr<Vars> vars = db->vars->Branch(row);

    // Remove the row from indices.
    for (auto idx : t->indices) CHECK(idx.second->tree.erase({row, i}));

    // Modify the row proto.
    for (size_t j = 0; j < fds.size(); ++j) {
      StatusOr<Value> so3 = ExecuteExpression(*ast.value(j), vars.get());
      if (!so3.ok()) return so3.status();
      Status s = SetField(so3.ValueOrDie(), fds[j], db->pool.get(), row);
      if (!s.ok()) return s;
    }

    // Re-add the row back to indices.
    for (auto idx : t->indices) idx.second->tree.insert({row, i});
  }

  return OkStatus();
}

}  // namespace sfdb
