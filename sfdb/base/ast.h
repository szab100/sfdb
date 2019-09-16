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
#ifndef SFDB_BASE_AST_H_
#define SFDB_BASE_AST_H_

#include <memory>
#include <ostream>
#include <string>

#include "absl/strings/string_view.h"
#include "sfdb/base/value.h"
#include "util/types/integral_types.h"

namespace sfdb {

// These are not dependencies; they are only used in friend declarations.
class Db;
class ProtoPool;
class TableIndex;
class TypedAst;
class Vars;

// Abstract syntax tree for a SQL statement.
//
// Immutable.
// May point to Descriptor and FieldDescriptor objects owned by a ProtoPool.
// May point to other Ast objects that this object owns.
class Ast {
 public:
  virtual ~Ast() = default;

  enum Type {
    ERROR = 0,
    CREATE_TABLE,
    CREATE_INDEX,  // index_name is in var_
    DROP_TABLE,
    DROP_INDEX,
    INSERT,
    UPDATE,
    SINGLE_EMPTY_ROW,
    TABLE_SCAN,  // FROM Table
    INDEX_SCAN,  // created in ../opt/
    INDEX_SCAN_BOUND_EXCLUSIVE,
    INDEX_SCAN_BOUND_INCLUSIVE,
    VALUE,       // TRUE, -7, 3.14, "hi"
    VAR,         // variable called |var|
    FUNC,        // var(values)
    FILTER,      // [x in rhs if lhs(x)]
    GROUP_BY,    // lhs GROUP BY column_indices
    ORDER_BY,    // lhs ORDER BY column_indices
    MAP,         // [[c:v(x) for c, v in zip(columns, values)] for x in rhs]
    IF,          // Used for constructs like CREATE TABLE/INDEX ... IF EXISTS/NOT EXEISTS
                 // rhs will be expression which needs to executed, lhs will be condition
    EXISTS,      // Checks if table(table_name) or index(index_name) exists
    SHOW_TABLES, //  ["People", "Contacts"]
    DESCRIBE_TABLE,  //  ["id", "int64"]["name", "string"]]

    // Operators in increasing order of priority.
    OP_IN, OP_LIKE, OP_OR,
    OP_AND,
    OP_NOT,
    OP_EQ, OP_LT, OP_GT, OP_LE, OP_GE, OP_NE,
    OP_PLUS, OP_MINUS, OP_BITWISE_AND, OP_BITWISE_OR, OP_BITWISE_XOR,
    OP_MUL, OP_DIV, OP_MOD,
    OP_BITWISE_NOT,
  };

 public:
  bool IsMutation() const;

  // accessors
  const Type type;
  const std::string &table_name() const { return table_name_; }
  const std::string &index_name() const { return index_name_; }
  const Ast *lhs() const { return lhs_.get(); }
  const Ast *rhs() const { return rhs_.get(); }
  const Value &value() const { return value_; }
  const std::vector<std::string> &columns() const { return columns_; }
  const std::string &column(int i) const { return columns_[i]; }
  const std::vector<std::string> &column_types() const { return column_types_; }
  const std::vector<std::unique_ptr<Ast>> &values() const { return values_; }
  const Ast *value(int i) const { return values_[i].get(); }
  const std::string &var() const { return var_; }
  const std::vector<int32> column_indices() const { return column_indices_; }

  // static helpers
  static std::string TypeToString(Type t);
  static bool IsUnaryOp(Type t);
  static bool IsBinaryOp(Type t);

  // factory functions
  static std::unique_ptr<Ast> CreateConditionalStatement(std::unique_ptr<Ast> &&lhs, std::unique_ptr<Ast> &&rhs) {
    return std::unique_ptr<Ast>(new Ast(
        IF, "", "", std::move(lhs), std::move(rhs)));
  }

  static std::unique_ptr<Ast> CreateNotStatement() {
    return std::unique_ptr<Ast>(new Ast(
        OP_NOT, "", "", nullptr, nullptr));
  }

  static std::unique_ptr<Ast> CreateObjectExistsStatement(
      ::absl::string_view table_name, ::absl::string_view index_name,
      bool negate) {
    return std::unique_ptr<Ast>(new Ast(EXISTS, table_name, index_name,
                                        negate ? CreateNotStatement() : nullptr,
                                        nullptr));
  }

  static std::unique_ptr<Ast> CreateTable(
      ::absl::string_view table_name, std::vector<std::string> &&columns,
      std::vector<std::string> &&column_types) {
    return std::unique_ptr<Ast>(new Ast(
        CREATE_TABLE, table_name, "", nullptr, nullptr, Value::Bool(false),
        std::move(columns), std::move(column_types)));
  }
  static std::unique_ptr<Ast> CreateIndex(
      ::absl::string_view table_name, std::vector<std::string> &&columns,
      ::absl::string_view index_name) {
    return std::unique_ptr<Ast>(new Ast(
        CREATE_INDEX, table_name, index_name, nullptr, nullptr,
        Value::Bool(false), std::move(columns)));
  }
  static std::unique_ptr<Ast> DropTable(::absl::string_view table_name) {
    return std::unique_ptr<Ast>(new Ast(DROP_TABLE, table_name));
  }
  static std::unique_ptr<Ast> DropIndex(::absl::string_view index_name) {
    return std::unique_ptr<Ast>(new Ast(
        DROP_INDEX, "", index_name, nullptr, nullptr, Value::Bool(false)));
  }
  static std::unique_ptr<Ast> Insert(
      ::absl::string_view table_name, std::vector<std::string> &&columns,
      std::vector<std::unique_ptr<Ast>> &&values) {
    return std::unique_ptr<Ast>(new Ast(
        INSERT, table_name, "", nullptr, nullptr, Value::Bool(false),
        std::move(columns), {}, std::move(values)));
  }
  static std::unique_ptr<Ast> Update(
      ::absl::string_view table_name, std::vector<std::string> &&columns,
      std::vector<std::unique_ptr<Ast>> &&values,
      std::unique_ptr<Ast> &&where) {
    return std::unique_ptr<Ast>(new Ast(
        UPDATE, table_name, "", std::move(where), nullptr,
        Value::Bool(false), std::move(columns), {}, std::move(values)));
  }
  static std::unique_ptr<Ast> SingleEmptyRow() {
    return std::unique_ptr<Ast>(new Ast(SINGLE_EMPTY_ROW));
  }
  static std::unique_ptr<Ast> TableScan(::absl::string_view table_name) {
    return std::unique_ptr<Ast>(new Ast(TABLE_SCAN, table_name));
  }
  static std::unique_ptr<Ast> ShowTables() {
    return std::unique_ptr<Ast>(new Ast(SHOW_TABLES));
  }
  static std::unique_ptr<Ast> DescribeTable(::absl::string_view table_name) {
    return std::unique_ptr<Ast>(new Ast(DESCRIBE_TABLE, table_name));
  }
  static std::unique_ptr<Ast> BinaryOp(
      Type op, std::unique_ptr<Ast> &&lhs, std::unique_ptr<Ast> &&rhs) {
    CHECK(IsBinaryOp(op)) << "Called Ast::BinaryOp(" << TypeToString(op) << ")";
    return std::unique_ptr<Ast>(new Ast(
        op, "", "", std::move(lhs), std::move(rhs)));
  }
  static std::unique_ptr<Ast> UnaryOp(Type op, std::unique_ptr<Ast> &&rhs) {
    CHECK(IsUnaryOp(op)) << "Called Ast::UnaryOp(" << TypeToString(op) << ")";
    return std::unique_ptr<Ast>(new Ast(op, "", "", nullptr, std::move(rhs)));
  }
  static std::unique_ptr<Ast> QuotedString(::absl::string_view v) {
    return std::unique_ptr<Ast>(new Ast(
        VALUE, "", "", nullptr, nullptr, Value::String(v)));
  }
  static std::unique_ptr<Ast> Int64(int64 v) {
    return std::unique_ptr<Ast>(new Ast(
        VALUE, "", "", nullptr, nullptr, Value::Int64(v)));
  }
  static std::unique_ptr<Ast> Double(double v) {
    return std::unique_ptr<Ast>(new Ast(
        VALUE, "", "", nullptr, nullptr, Value::Double(v)));
  }
  static std::unique_ptr<Ast> Bool(bool v) {
    return std::unique_ptr<Ast>(new Ast(
        VALUE, "", "", nullptr, nullptr, Value::Bool(v)));
  }
  static std::unique_ptr<Ast> Var(::absl::string_view var) {
    return std::unique_ptr<Ast>(new Ast(
        VAR, "", "", nullptr, nullptr, Value::Bool(false), {}, {}, {}, var));
  }
  static std::unique_ptr<Ast> Func(
      ::absl::string_view fname, std::vector<std::unique_ptr<Ast>> &&values) {
    return std::unique_ptr<Ast>(new Ast(
        FUNC, "", "", nullptr, nullptr, Value::Bool(false), {}, {},
        std::move(values), fname));
  }
  static std::unique_ptr<Ast> Filter(
      std::unique_ptr<Ast> &&predicate, std::unique_ptr<Ast> &&rows) {
    return std::unique_ptr<Ast>(new Ast(
        FILTER, "", "", std::move(predicate), std::move(rows)));
  }
  static std::unique_ptr<Ast> GroupBy(
      std::unique_ptr<Ast> &&rows, std::vector<int32> &&column_indices) {
    return std::unique_ptr<Ast>(new Ast(
        GROUP_BY, "", "", std::move(rows), nullptr, Value::Bool(false), {}, {},
        {}, "", std::move(column_indices)));
  }
  static std::unique_ptr<Ast> OrderBy(
      std::unique_ptr<Ast> &&rows, std::vector<int32> &&column_indices) {
    return std::unique_ptr<Ast>(new Ast(
        GROUP_BY, "", "", std::move(rows), nullptr, Value::Bool(false), {}, {},
        {}, "", std::move(column_indices)));
  }
  static std::unique_ptr<Ast> Map(
      std::vector<std::string> &&columns,
      std::vector<std::unique_ptr<Ast>> &&values, std::unique_ptr<Ast> &&rhs) {
    return std::unique_ptr<Ast>(new Ast(
        MAP, "", "", nullptr, std::move(rhs), Value::Bool(false),
        std::move(columns), {}, std::move(values)));
  }

 protected:
  Ast(Type type = ERROR,
      ::absl::string_view table_name = "",
      ::absl::string_view index_name = "",
      std::unique_ptr<Ast> &&lhs = nullptr,
      std::unique_ptr<Ast> &&rhs = nullptr,
      Value &&value = Value::Bool(false),
      std::vector<std::string> &&columns = {},
      std::vector<std::string> &&column_types = {},
      std::vector<std::unique_ptr<Ast>> &&values = {},
      ::absl::string_view var = "",
      std::vector<int32> &&column_indices = {})
      : type(type),
        table_name_(table_name),
        index_name_(index_name),
        lhs_(std::move(lhs)),
        rhs_(std::move(rhs)),
        value_(std::move(value)),
        columns_(std::move(columns)),
        column_types_(std::move(column_types)),
        values_(std::move(values)),
        var_(var),
        column_indices_(std::move(column_indices)) {}

  // Some of these are set depending on |type|.
  std::string table_name_;  // for CREATE_TABLE, DROP_TABLE, INSERT, UPDATE, etc.
  std::string index_name_;  // for CREATE_INDEX, DROP_INDEX
  std::unique_ptr<Ast> lhs_;  // for binary OP_*s, FILTER, UPDATE
  std::unique_ptr<Ast> rhs_;  // for unary and binary OP_*s, FILTER
  Value value_;  // for VALUE
  std::vector<std::string> columns_;  // for CREATE_TABLE, INSERT, UPDATE
  std::vector<std::string> column_types_;  // for CREATE_TABLE
  std::vector<std::unique_ptr<Ast>> values_;  // INSERT, UPDATE, FUNC
  std::string var_;  // for VAR and FUNC
  std::vector<int32> column_indices_;  // for GROUP_BY and ORDER_BY

  // in ../engine/infer_result_types.cc
  friend ::util::StatusOr<std::unique_ptr<TypedAst>> InferResultTypes(
      std::unique_ptr<Ast> &&ast, ProtoPool *pool, const Db *db,
      const Vars *vars);

  // in ../opt/index_match.cc
  friend std::unique_ptr<TypedAst> RebuildAstUsingIndex(
      const TableIndex &index, std::unique_ptr<TypedAst> &&ast);
};

}  // namespace sfdb

#endif  // SFDB_BASE_AST_H_
