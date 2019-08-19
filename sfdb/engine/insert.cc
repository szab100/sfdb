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
#include "sfdb/engine/insert.h"

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/engine/expressions.h"
#include "sfdb/engine/set_field.h"
#include "sfdb/proto/pool.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::StrCat;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::util::InternalError;
using ::util::NotFoundError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

Status ExecuteInsert(const TypedAst &ast, Db *db) {
  if (ast.columns().size() != ast.values().size()) return InternalError(StrCat(
      ast.values().size(), " values for ", ast.columns().size(), " columns",
      " in an INSERT"));

  Table *t = db->FindTable(ast.table_name());
  if (!t) return NotFoundError(StrCat(
      "Table ", ast.table_name(), " not found in database ", db->name));

  std::unique_ptr<Message> row = db->pool->NewMessage(t->type);
  for (size_t i = 0; i < ast.columns().size(); ++i) {
    const std::string &col = ast.columns()[i];
    const FieldDescriptor *fd = t->type->FindFieldByName(col);
    if (!fd) return NotFoundError(StrCat(
        "No column named ", col, " in ", t->name));
    const TypedAst &expr = *ast.value(i);
    StatusOr<Value> so = ExecuteExpression(expr, db->vars.get());
    if (!so.ok()) return so.status();
    Status s = SetField(so.ValueOrDie(), fd, db->pool.get(), row.get());
    if (!s.ok()) return s;
  }

  t->Insert(std::move(row));
  return OkStatus();
}

}  // namespace sfdb
