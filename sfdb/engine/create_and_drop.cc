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
#include "sfdb/engine/create_and_drop.h"

#include "absl/memory/memory.h"
#include "google/protobuf/descriptor.h"
#include "sfdb/base/ast_type.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::StrCat;
using ::absl::make_unique;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::FileDescriptorProto;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

Status ExecuteCreateTable(const TypedAst &ast, Db *db) {
  static const auto LABEL_REPEATED = FieldDescriptorProto::LABEL_REPEATED;
  static const auto LABEL_OPTIONAL = FieldDescriptorProto::LABEL_OPTIONAL;

  if (db->FindTable(ast.table_name())) return InvalidArgumentError(StrCat(
      "Table ", ast.table_name(), " already exists in database ", db->name));
  if (ast.columns().size() != ast.column_types().size()) return InternalError(
      "CREATE_TABLE Ast with columns() and column_types() of different sizes");

  // Validate each column's type.
  std::vector<FieldDescriptorProto> fields;
  for (size_t i = 0; i < ast.columns().size(); ++i) {
    StatusOr<AstType> so = AstType::FromString(
        ast.column_types()[i], *db->pool);
    if (!so.ok()) return so.status();
    const AstType &t = so.ValueOrDie();
    if (t.is_void)
      return InvalidArgumentError("Cannot have void-valued proto fields");
    fields.emplace_back();
    fields.back().set_name(ast.column(i));
    fields.back().set_number(i + 1);
    fields.back().set_label(t.is_repeated ? LABEL_REPEATED : LABEL_OPTIONAL);
    fields.back().set_type(ProtoPool::TypeToType(t.type));
    if (t.type == FieldDescriptor::TYPE_MESSAGE) {
      fields.back().set_type_name(StrCat(".", t.d->full_name()));
    } else if (t.type == FieldDescriptor::TYPE_ENUM) {
      fields.back().set_type_name(StrCat(".", t.ed->full_name()));
    }
  }

  // Create the ProtoPool and proto2::Descriptor describing a row of the table.
  std::unique_ptr<ProtoPool> table_pool = db->pool->Branch();
  StatusOr<const Descriptor*> so2 = table_pool->CreateProtoClass(
      ast.table_name(), fields);
  if (!so2.ok()) return so2.status();

  db->PutTable(ast.table_name(), std::move(table_pool), so2.ValueOrDie());
  return OkStatus();
}

Status ExecuteDropTable(const TypedAst &ast, Db *db) {
  if (!db->DropTable(ast.table_name())) return NotFoundError(StrCat(
      "No table named ", ast.table_name(), " in database ", db->name));
  return OkStatus();
}

Status ExecuteCreateIndex(const TypedAst &ast, Db *db) {
  Table *t = db->FindTable(ast.table_name());
  if (!t) return InvalidArgumentError(StrCat(
      "Table ", ast.table_name(), " not found in database ", db->name));

  std::vector<const FieldDescriptor*> columns;
  for (const std::string &column_name : ast.columns()) {
    const FieldDescriptor *fd = t->type->FindFieldByName(column_name);
    if (!fd) return NotFoundError(StrCat(
        "No column named ", column_name, " in table ", t->name));
    if (fd->is_repeated()) return InvalidArgumentError(StrCat(
        "Repeated column ", column_name, " cannot be indexed"));
    if (fd->type() == FieldDescriptor::TYPE_MESSAGE)
      return InvalidArgumentError(StrCat(
          "Cannot index on the message-valued column ", column_name));
    if (fd->type() == FieldDescriptor::TYPE_GROUP)
      return InvalidArgumentError(
          "How did you end up with a proto group in a table?!");
    columns.push_back(fd);
  }

  db->PutIndex(t, ast.index_name(), std::move(columns));
  return OkStatus();
}

Status ExecuteDropIndex(const TypedAst &ast, Db *db) {
  if (!db->DropIndex(ast.index_name())) return NotFoundError(StrCat(
      "No index named ", ast.index_name(), " in database ", db->name));
  return OkStatus();
}

}  // namespace sfdb
