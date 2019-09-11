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
#include "sfdb/engine/infer_result_types.h"

#include <memory>

#include "absl/synchronization/mutex.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/empty.pb.h"
#include "gtest/gtest.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/db.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/proto/pool.h"
#include "sfdb/sql/parser.h"
#include "sfdb/testing/data.pb.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::google::protobuf::Descriptor;
using ::google::protobuf::Empty;
using ::google::protobuf::FieldDescriptor;

class InferResultTypesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_.reset(new ProtoPool);
    db_.reset(new Db("Test", &vars_));

    // Create some tables.
    ::absl::WriterMutexLock lock(&db_->mu);
    db_->PutTable("Points", db_->pool->Branch(),
                  Point::default_instance().GetDescriptor());
  }

  void TearDown() override {
    db_ = nullptr;
    pool_ = nullptr;
  }

  // Parses a SQL statement and infers its result type.
  std::unique_ptr<TypedAst> Go(const char *sql) {
    std::unique_ptr<Ast> ast = Parse(sql).ValueOrDie();
    ::absl::ReaderMutexLock lock(&db_->mu);
    return InferResultTypes(
        std::move(ast), pool_.get(), db_.get(), db_->vars.get()).ValueOrDie();
  }

  AstType TypeOf(const char *sql) {
    return Go(sql)->result_type;
  }

  std::unique_ptr<ProtoPool> pool_;
  BuiltIns vars_;
  std::unique_ptr<Db> db_;
};

TEST_F(InferResultTypesTest, ShowTables) {
  EXPECT_TRUE(TypeOf("SHOW TABLES;").IsRepeatedMessage());
}

TEST_F(InferResultTypesTest, Void) {
  EXPECT_TRUE(TypeOf("CREATE TABLE Tab (x int64);").is_void);
  EXPECT_TRUE(TypeOf("INSERT INTO Tab (x) VALUES (13);").is_void);
  EXPECT_TRUE(TypeOf("DROP TABLE Tab;").is_void);
}

TEST_F(InferResultTypesTest, MapAndTableScan) {
  std::unique_ptr<TypedAst> tast = Go("SELECT x, y FROM Points;");
  ASSERT_EQ(Ast::MAP, tast->type);
  EXPECT_FALSE(tast->result_type.is_void);
  EXPECT_TRUE(tast->result_type.is_repeated);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, tast->result_type.type);
  const Descriptor *d = tast->result_type.d;
  ASSERT_TRUE(d);
  EXPECT_TRUE(d->FindFieldByName("_1"));
  EXPECT_TRUE(d->FindFieldByName("_2"));
  EXPECT_EQ(FieldDescriptor::TYPE_INT32, d->FindFieldByName("_1")->type());
  EXPECT_EQ(FieldDescriptor::TYPE_INT32, d->FindFieldByName("_2")->type());

  const TypedAst *scan = tast->rhs();
  ASSERT_TRUE(scan);
  ASSERT_EQ(Ast::TABLE_SCAN, scan->type);
  EXPECT_FALSE(scan->result_type.is_void);
  EXPECT_TRUE(scan->result_type.is_repeated);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, scan->result_type.type);
  EXPECT_EQ(Point::default_instance().GetDescriptor(), scan->result_type.d);
}

TEST_F(InferResultTypesTest, SingleRow) {
  std::unique_ptr<TypedAst> tast = Go("SELECT 1 AS age, 'Bob' AS name;");
  ASSERT_EQ(Ast::MAP, tast->type);
  EXPECT_FALSE(tast->result_type.is_void);
  EXPECT_TRUE(tast->result_type.is_repeated);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, tast->result_type.type);
  const Descriptor *d = tast->result_type.d;
  ASSERT_TRUE(d);
  EXPECT_TRUE(d->FindFieldByName("age"));
  EXPECT_TRUE(d->FindFieldByName("name"));
  EXPECT_EQ(FieldDescriptor::TYPE_INT64, d->FindFieldByName("age")->type());
  EXPECT_EQ(FieldDescriptor::TYPE_STRING, d->FindFieldByName("name")->type());

  const TypedAst *scan = tast->rhs();
  ASSERT_TRUE(scan);
  ASSERT_EQ(Ast::SINGLE_EMPTY_ROW, scan->type);
  EXPECT_FALSE(scan->result_type.is_void);
  EXPECT_TRUE(scan->result_type.is_repeated);
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, scan->result_type.type);
  EXPECT_EQ(Empty::default_instance().GetDescriptor(), scan->result_type.d);
}

}  // namespace
}  // namespace sfdb
