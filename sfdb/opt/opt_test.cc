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
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/db.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/base/vars.h"
#include "sfdb/proto/pool.h"

namespace sfdb {
namespace {

using ::absl::make_unique;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;

// Utils for constructing TypedAst objects.
std::unique_ptr<TypedAst> TAst(
    const AstType &result_type, Ast::Type type, std::string &&table_name,
    std::string &&index_name, std::unique_ptr<Ast> &&lhs,
    std::unique_ptr<Ast> &&rhs, Value &&value, std::vector<std::string> &&columns,
    std::vector<std::string> &&column_types,
    std::vector<std::unique_ptr<Ast>> &&values, std::string &&var,
    std::vector<int32> &&column_indices) {
  return make_unique<TypedAst>(
      type, std::move(table_name), std::move(index_name), std::move(lhs),
      std::move(rhs), std::move(value), std::move(columns),
      std::move(column_types), std::move(values), std::move(var),
      std::move(column_indices), result_type);
}
std::unique_ptr<TypedAst> TAstVar(const char *name, FieldDescriptor::Type t) {
  return TAst(AstType::Scalar(t), Ast::VAR, "", "", nullptr, nullptr,
              Value::Bool(false), {}, {}, {}, name, {});
}
std::unique_ptr<TypedAst> TAstValue(Value &&value) {
  return TAst(value.type, Ast::VALUE, "", "", nullptr, nullptr,
              std::move(value), {}, {}, {}, "", {});
}
std::unique_ptr<TypedAst> TAstOp(
    const AstType &result_type, Ast::Type op, std::unique_ptr<TypedAst> &&lhs,
    std::unique_ptr<TypedAst> &&rhs) {
  return TAst(result_type, op, "", "", std::move(lhs), std::move(rhs),
              Value::Bool(false), {}, {}, {}, "", {});
}
std::unique_ptr<TypedAst> TAstEq(
    std::unique_ptr<TypedAst> &&lhs, std::unique_ptr<TypedAst> &&rhs) {
  return TAstOp(AstType::Scalar(FieldDescriptor::TYPE_BOOL), Ast::OP_EQ,
                std::move(lhs), std::move(rhs));
}
std::vector<std::unique_ptr<Ast>> TAstVector(
    std::unique_ptr<Ast> &&ast) {
  std::vector<std::unique_ptr<Ast>> v;
  v.push_back(std::move(ast));
  return v;
}

TEST(OptTest, UpdateShouldUseIndex) {
  ProtoPool pool;
  BuiltIns vars;
  Db db("Test", &vars);

  ::absl::WriterMutexLock lock(&db.mu);

  // CREATE TABLE People (name string, age int64);
  std::unique_ptr<ProtoPool> people_pool = pool.Branch();
  const Descriptor *people_d = people_pool->CreateProtoClass("People", {
      {"name", FieldDescriptor::TYPE_STRING},
      {"age", FieldDescriptor::TYPE_INT64}}).ValueOrDie();
  Table *people = db.PutTable("People", std::move(people_pool), people_d);

  // CREATE INDEX ByName on People (name);
  TableIndex *name_index = db.PutIndex(
      people, "ByName", {people_d->FindFieldByName("name")});
  EXPECT_EQ("ByName", name_index->name);

  // INSERT a few rows.
  people->Insert(people->pool->NewMessage(people_d, "name: 'Bob' age: 13"));
  people->Insert(people->pool->NewMessage(people_d, "name: 'Ava' age: 14"));
  people->Insert(people->pool->NewMessage(people_d, "name: 'Eve' age: 15"));
  people->Insert(people->pool->NewMessage(people_d, "name: 'Oto' age: 16"));
  people->Insert(people->pool->NewMessage(people_d, "name: 'Nan' age: 17"));

  // UPDATE People SET age=69 WHERE name="Eve";
  auto ast = TAst(
      AstType::Void(), Ast::UPDATE, "People", "",
      TAstEq(TAstVar("name", FieldDescriptor::TYPE_STRING),
             TAstValue(Value::String("Eve"))),
      nullptr, Value::Bool(false), {std::string("age")}, {},
      TAstVector(TAstValue(Value::Int64(69))), "", {});

  ast = Optimize(db, std::move(ast));

  // lhs should now be True
  // rhs should now be an INDEX_SCAN["Eve" <= name <= "Eve"]
  EXPECT_EQ(Ast::UPDATE, ast->type);
  EXPECT_EQ("People", ast->table_name());
  EXPECT_EQ(1, ast->columns().size());
  EXPECT_EQ("age", ast->columns()[0]);
  EXPECT_EQ(1, ast->values().size());
  EXPECT_EQ(Ast::VALUE, ast->values()[0]->type);
  EXPECT_EQ(Value::Int64(69), ast->values()[0]->value());
  EXPECT_EQ(Ast::VALUE, ast->lhs()->type);
  EXPECT_EQ(Value::Bool(true), ast->lhs()->value());
  ASSERT_TRUE(ast->rhs());
  EXPECT_EQ(Ast::INDEX_SCAN, ast->rhs()->type);
  EXPECT_EQ("ByName", ast->rhs()->index_name());
  EXPECT_EQ(Ast::INDEX_SCAN_BOUND_INCLUSIVE, ast->rhs()->lhs()->type);
  EXPECT_EQ(Ast::INDEX_SCAN_BOUND_INCLUSIVE, ast->rhs()->rhs()->type);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE,
            ast->rhs()->lhs()->value().type.type);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE,
            ast->rhs()->rhs()->value().type.type);
  EXPECT_EQ("name: \"Eve\"",
            ast->rhs()->lhs()->value().msg->ShortDebugString());
  EXPECT_EQ("name: \"Eve\"",
            ast->rhs()->rhs()->value().msg->ShortDebugString());
}

}  // namespace
}  // namespace sdfb
