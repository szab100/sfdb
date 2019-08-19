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
#include "sfdb/sql/parser.h"

#include <memory>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/value.h"
#include "util/task/canonical_errors.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::testing::HasSubstr;
using ::util::IsInvalidArgument;
using ::util::StatusOr;

TEST(ParserTest, DropTable) {
  std::unique_ptr<Ast> ast = Parse("DROP TABLE Blah;").ValueOrDie();
  EXPECT_EQ(Ast::DROP_TABLE, ast->type);
  EXPECT_EQ("Blah", ast->table_name());
}

TEST(ParserTest, CreateTable) {
  std::unique_ptr<Ast> ast = Parse(
      "CREATE TABLE Mixed_case (name string, age int64);").ValueOrDie();
  EXPECT_EQ(Ast::CREATE_TABLE, ast->type);
  EXPECT_EQ("Mixed_case", ast->table_name());
  EXPECT_EQ(std::vector<std::string>({"name", "age"}), ast->columns());
  EXPECT_EQ(std::vector<std::string>({"string", "int64"}), ast->column_types());
}

TEST(ParserTest, CreateTable_Empty) {
  std::unique_ptr<Ast> ast = Parse("CREATE TABLE t();").ValueOrDie();
  EXPECT_EQ(Ast::CREATE_TABLE, ast->type);
  EXPECT_EQ("t", ast->table_name());
  EXPECT_EQ(std::vector<std::string>(), ast->columns());
  EXPECT_EQ(std::vector<std::string>(), ast->column_types());
}

TEST(ParserTest, CreateTable_TrailingComma) {
  std::unique_ptr<Ast> ast = Parse(
      "CREATE TABLE t(a string,);").ValueOrDie();
  EXPECT_EQ(Ast::CREATE_TABLE, ast->type);
  EXPECT_EQ("t", ast->table_name());
  EXPECT_EQ(std::vector<std::string>({"a"}), ast->columns());
  EXPECT_EQ(std::vector<std::string>({"string"}), ast->column_types());
}

TEST(ParserTest, CreateTable_Errors) {
  auto Err = [](const char *sql) {
    StatusOr<std::unique_ptr<Ast>> s = Parse(sql);
    CHECK(!s.ok());
    CHECK(IsInvalidArgument(s.status())) << s.status();
    return s.status().error_message();
  };

  EXPECT_THAT(Err("CREATE TABLE t1 (,);"), HasSubstr("got ,"));
  EXPECT_THAT(Err("CREATE TABLE t2 (a string,,)"), HasSubstr("got ,"));
}

TEST(ParserTest, DropIndex) {
  std::unique_ptr<Ast> ast = Parse("DROP INDEX Indie;").ValueOrDie();
  EXPECT_EQ(Ast::DROP_INDEX, ast->type);
  EXPECT_EQ("Indie", ast->index_name());
}

TEST(ParseTest, CreateIndex) {
  std::unique_ptr<Ast> ast = Parse(
      "CREATE INDEX Indie ON Tabbie (a, b);").ValueOrDie();
  EXPECT_EQ(Ast::CREATE_INDEX, ast->type);
  EXPECT_EQ("Tabbie", ast->table_name());
  EXPECT_EQ("Indie", ast->index_name());
  EXPECT_EQ(2, ast->columns().size());
  EXPECT_EQ("a", ast->columns()[0]);
  EXPECT_EQ("b", ast->columns()[1]);
}

TEST(ParseTest, CreateIndex_TrailingComma) {
  std::unique_ptr<Ast> ast = Parse(
      "CREATE INDEX Indie ON Tabbie (a,);").ValueOrDie();
  EXPECT_EQ(Ast::CREATE_INDEX, ast->type);
  EXPECT_EQ("Tabbie", ast->table_name());
  EXPECT_EQ("Indie", ast->index_name());
  EXPECT_EQ(1, ast->columns().size());
  EXPECT_EQ("a", ast->columns()[0]);
}

TEST(ParseTest, CreateIndex_Errors) {
  auto Err = [](const char *sql) {
    StatusOr<std::unique_ptr<Ast>> s = Parse(sql);
    CHECK(!s.ok());
    CHECK(IsInvalidArgument(s.status())) << s.status();
    return s.status().error_message();
  };

  EXPECT_THAT(Err("CREATE INDEX i ON (a);"), HasSubstr("Expected table"));
  EXPECT_THAT(Err("CREATE INDEX i ON t;"), HasSubstr("Expected ("));
  EXPECT_THAT(Err("CREATE INDEX i ON t ();"), HasSubstr("At least one column"));
  EXPECT_THAT(Err("CREATE INDEX i ON t (,);"), HasSubstr("Expected column"));
  EXPECT_THAT(Err("CREATE INDEX i ON t (a,,b);"), HasSubstr("Expected column"));
}

TEST(ParserTest, Insert) {
  std::unique_ptr<Ast> ast = Parse(
      "INSERT INTO People (name, age) VALUES ('dude', 99);").ValueOrDie();
  EXPECT_EQ(Ast::INSERT, ast->type);
  EXPECT_EQ("People", ast->table_name());
  EXPECT_EQ(2, ast->columns().size());
  EXPECT_EQ("name", ast->columns()[0]);
  EXPECT_EQ("age", ast->columns()[1]);
  EXPECT_EQ(2, ast->values().size());
  EXPECT_EQ(Ast::VALUE, ast->values()[0]->type);
  EXPECT_EQ(Value::String("dude"), ast->values()[0]->value());
  EXPECT_EQ(Ast::VALUE, ast->values()[1]->type);
  EXPECT_EQ(Value::Int64(99), ast->values()[1]->value());
}

TEST(ParserTest, Update) {
  std::unique_ptr<Ast> ast = Parse(
      "UPDATE People SET age=69, id=0x13 WHERE name = 'dude';").ValueOrDie();
  EXPECT_EQ(Ast::UPDATE, ast->type);
  EXPECT_EQ("People", ast->table_name());
  EXPECT_EQ(2, ast->columns().size());
  EXPECT_EQ("age", ast->columns()[0]);
  EXPECT_EQ("id", ast->columns()[1]);
  EXPECT_EQ(2, ast->values().size());
  EXPECT_EQ(Ast::VALUE, ast->values()[0]->type);
  EXPECT_EQ(Value::Int64(69), ast->values()[0]->value());
  EXPECT_EQ(Ast::VALUE, ast->values()[1]->type);
  EXPECT_EQ(Value::Int64(0x13), ast->values()[1]->value());
  EXPECT_EQ(Ast::OP_EQ, ast->lhs()->type);
  EXPECT_EQ(Ast::VAR, ast->lhs()->lhs()->type);
  EXPECT_EQ("name", ast->lhs()->lhs()->var());
  EXPECT_EQ(Ast::VALUE, ast->lhs()->rhs()->type);
  EXPECT_EQ(Value::String("dude"), ast->lhs()->rhs()->value());
}

TEST(ParserTest, SelectExpr) {
  std::unique_ptr<Ast> ast = Parse(
      "SELECT pi, 2 * acos(1.0);").ValueOrDie();
  EXPECT_EQ(Ast::MAP, ast->type);
  EXPECT_EQ(2, ast->columns().size());
  EXPECT_EQ(2, ast->values().size());
  EXPECT_EQ(Ast::VAR, ast->values()[0]->type);
  EXPECT_EQ("pi", ast->values()[0]->var());
  EXPECT_EQ(Ast::OP_MUL, ast->values()[1]->type);
  EXPECT_EQ(Ast::VALUE, ast->values()[1]->lhs()->type);
  EXPECT_EQ(Value::Int64(2), ast->values()[1]->lhs()->value());
  EXPECT_EQ(Ast::FUNC, ast->values()[1]->rhs()->type);
  EXPECT_EQ("acos", ast->values()[1]->rhs()->var());
  EXPECT_EQ(1, ast->values()[1]->rhs()->values().size());
  EXPECT_EQ(Ast::VALUE, ast->values()[1]->rhs()->values()[0]->type);
  EXPECT_EQ(Value::Double(1.0), ast->values()[1]->rhs()->values()[0]->value());
  EXPECT_EQ(Ast::SINGLE_EMPTY_ROW, ast->rhs()->type);
}

TEST(ParserTest, SelectFunc) {
  std::unique_ptr<Ast> ast = Parse(
      "SELECT LEN('hello');").ValueOrDie();
  EXPECT_EQ(Ast::MAP, ast->type);
  EXPECT_EQ(1, ast->values().size());
  EXPECT_EQ(Ast::FUNC, ast->values()[0]->type);
  EXPECT_EQ("LEN", ast->values()[0]->var());
  EXPECT_EQ(1, ast->values()[0]->values().size());
  EXPECT_EQ(Ast::VALUE, ast->values()[0]->values()[0]->type);
  EXPECT_EQ(Value::String("hello"), ast->values()[0]->values()[0]->value());
  EXPECT_EQ(Ast::SINGLE_EMPTY_ROW, ast->rhs()->type);
}

TEST(ParserTest, SelectWithNames) {
  std::unique_ptr<Ast> ast = Parse(
      "SELECT 3.14 AS pi;").ValueOrDie();
  EXPECT_EQ(Ast::MAP, ast->type);
  EXPECT_EQ(1, ast->values().size());
  EXPECT_EQ("pi", ast->columns()[0]);
  EXPECT_EQ(Ast::VALUE, ast->values()[0]->type);
  EXPECT_EQ(Value::Double(3.14), ast->values()[0]->value());
  EXPECT_EQ(Ast::SINGLE_EMPTY_ROW, ast->rhs()->type);
}

TEST(ParserTest, SelectFromWhere) {
  std::unique_ptr<Ast> ast = Parse(
      "SELECT name, age FROM People WHERE age >= 21;").ValueOrDie();
  EXPECT_EQ(Ast::MAP, ast->type);
  EXPECT_EQ(2, ast->columns().size());
  EXPECT_EQ("", ast->columns()[0]);
  EXPECT_EQ("", ast->columns()[1]);
  EXPECT_EQ(2, ast->values().size());
  EXPECT_EQ(Ast::VAR, ast->values()[0]->type);
  EXPECT_EQ("name", ast->values()[0]->var());
  EXPECT_EQ(Ast::VAR, ast->values()[1]->type);
  EXPECT_EQ("age", ast->values()[1]->var());
  EXPECT_EQ(Ast::FILTER, ast->rhs()->type);
  EXPECT_EQ(Ast::TABLE_SCAN, ast->rhs()->rhs()->type);
  EXPECT_EQ("People", ast->rhs()->rhs()->table_name());
  EXPECT_EQ(Ast::OP_GE, ast->rhs()->lhs()->type);
  EXPECT_EQ(Ast::VAR, ast->rhs()->lhs()->lhs()->type);
  EXPECT_EQ("age", ast->rhs()->lhs()->lhs()->var());
  EXPECT_EQ(Ast::VALUE, ast->rhs()->lhs()->rhs()->type);
  EXPECT_EQ(Value::Int64(21), ast->rhs()->lhs()->rhs()->value());
}

}  // namespace
}  // namespace sfdb
