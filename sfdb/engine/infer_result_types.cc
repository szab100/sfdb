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

#include <string>

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/empty.pb.h"
#include "sfdb/base/typed_ast.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::google::protobuf::Descriptor;
using ::google::protobuf::Empty;
using ::google::protobuf::FieldDescriptor;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::OkStatus;
using ::util::StatusOr;
using ::util::UnimplementedError;

StatusOr<AstType> GetFuncType(
    const std::string &fcn, const std::vector<std::unique_ptr<TypedAst>> &values,
    const Vars *vars) {
  const Func *f = vars->GetFunc(fcn);
  if (!f) return NotFoundError(StrCat("Function ", fcn, " not found"));

  std::vector<const AstType*> arg_types;
  for (const auto &ast : values) arg_types.push_back(&ast->result_type);

  return f->InferReturnType(arg_types);
}

StatusOr<AstType> GetShowTablesType(const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  return AstType::RepeatedMessage(db->GetTableListTableType());
}

StatusOr<AstType> GetDescribeTableType(const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  return AstType::RepeatedMessage(db->GetDescribeTableType());
}

StatusOr<AstType> GetTableScanType(const std::string &table_name, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  const Table *t = db->FindTable(table_name);
  if (!t) return NotFoundError(StrCat(
      "Table ", table_name, " not found in database ", db->name));
  return AstType::RepeatedMessage(t->type);
}

StatusOr<AstType> GetIndexScanType(const std::string &index_name, const Db *db)
    SHARED_LOCKS_REQUIRED(db->mu) {
  const TableIndex *i = db->FindIndex(index_name);
  if (!i) return NotFoundError(StrCat(
      "Index ", index_name, " not found in database ", db->name));
  return AstType::RepeatedMessage(i->t->type);
}

StatusOr<AstType> GetGroupByType(
    const Ast &lhs, const std::vector<int32> &column_indices) {
  return UnimplementedError("GROUP BY type inference not ready");
}

StatusOr<AstType> GetMapType(
    const Ast &ast,
    const std::vector<std::unique_ptr<TypedAst>> &values, ProtoPool *pool) {
  // ast.values() is gone! Use |values| instead.

  const size_t n = ast.columns().size();
  if (values.size() != n) return InternalError(StrCat(
      values.size(), " values for ", n, " columns in a MAP"));

  // Validate each column's type.
  std::vector<std::pair<std::string, FieldDescriptor::Type>> fields;
  for (size_t i = 0; i < n; ++i) {
    const AstType &rt = values[i]->result_type;
    if (rt.is_void) return InvalidArgumentError(StrCat(
        "Column ", i + 1, " has void type; must be a scalar"));
    if (rt.is_repeated) return InvalidArgumentError(StrCat(
        "Column ", i + 1, " is a repeated field; must be a scalar"));
    if (rt.type == FieldDescriptor::TYPE_MESSAGE)
      return UnimplementedError(StrCat(
          "Column ", i + 1, " is a proto; must be a scalar"));
    if (rt.type == FieldDescriptor::TYPE_GROUP)
      return InvalidArgumentError(StrCat(
          "Column ", i + 1, " is a group; must be a scalar"));
    if (rt.type == FieldDescriptor::TYPE_ENUM)
      return UnimplementedError(StrCat(
          "Column ", i + 1, " is an enum; must be a scalar"));
    const std::string column =
        ast.column(i).empty() ? StrCat("_", i + 1) : ast.column(i);
    fields.push_back({column, rt.type});
  }

  // Create a temporary proto class containing the columns.
  StatusOr<const Descriptor*> so = pool->CreateProtoClass(
      StrCat("Map", int64(&ast)), fields);  // XXX: use address as a UID
  if (!so.ok()) return so.status();
  return AstType::RepeatedMessage(so.ValueOrDie());
}

StatusOr<AstType> GetUnaryOpType(Ast::Type op, const AstType &rhs) {
  if (rhs.is_void) return InvalidArgumentError(StrCat(
      "Cannot apply the unary ", Ast::TypeToString(op), " operator to VOID"));
  if (rhs.is_repeated) return UnimplementedError(StrCat(
      "Cannot apply the unary ", Ast::TypeToString(op),
      " operator to a repeated type"));

  const AstType kBool = AstType::Scalar(FieldDescriptor::TYPE_BOOL);

  switch (op) {
    case Ast::OP_NOT:
      if (rhs.IsCastableTo(kBool)) return kBool;
      break;
    case Ast::OP_BITWISE_NOT:
      if (rhs.IsIntegralType()) return rhs;
      break;
    case Ast::OP_MINUS:
      if (rhs.IsNumericType()) return rhs;
      break;
    default:
      return InternalError("Bug in InferResultType() calling GetUnaryOpType()");
  }
  return InvalidArgumentError(StrCat(
      "Cannot apply the unary ", Ast::TypeToString(op), " operator to an",
      " argument of type ", rhs.ToString()));
}

StatusOr<AstType> GetBinaryOpType(
    Ast::Type op, const AstType &lhs, const AstType &rhs) {
  if (lhs.is_void || rhs.is_void) return InvalidArgumentError(StrCat(
      "Cannot apply the ", Ast::TypeToString(op),
      " operator to operands of type ", lhs.ToString(), " and ",
      rhs.ToString()));
  if (lhs.is_repeated || rhs.is_repeated) return UnimplementedError(StrCat(
      "Cannot apply the ", Ast::TypeToString(op),
      " operator to repeated operands"));

  const AstType kBool = AstType::Scalar(FieldDescriptor::TYPE_BOOL);
  const AstType kInt32 =
      AstType::Scalar(FieldDescriptor::TYPE_INT32);
  const AstType kInt64 =
      AstType::Scalar(FieldDescriptor::TYPE_INT64);
  const AstType kDouble =
      AstType::Scalar(FieldDescriptor::TYPE_DOUBLE);
  const AstType kString =
      AstType::Scalar(FieldDescriptor::TYPE_STRING);

  switch (op) {
    case Ast::OP_IN:
    case Ast::OP_LIKE:
    case Ast::OP_EQ:
    case Ast::OP_LT:
    case Ast::OP_GT:
    case Ast::OP_LE:
    case Ast::OP_GE:
    case Ast::OP_NE:
      return kBool;
    case Ast::OP_OR:
    case Ast::OP_AND:
      if (lhs.IsCastableTo(kBool) && rhs.IsCastableTo(kBool)) return kBool;
      break;
    case Ast::OP_BITWISE_AND:
    case Ast::OP_BITWISE_OR:
    case Ast::OP_BITWISE_XOR:
    case Ast::OP_MOD:
      if (lhs.IsInt32() && rhs.IsInt32()) return kInt32;
      if (lhs.IsIntegralType() && rhs.IsIntegralType()) return kInt64;
      // TODO: write a proper function for finding a common lhs/rhs type
      break;
    case Ast::OP_PLUS:
      if (lhs.IsString() || rhs.IsString()) return kString;
      [[clang::fallthrough]];
    case Ast::OP_MINUS:
    case Ast::OP_MUL:
    case Ast::OP_DIV:
      if (lhs.IsInt32() && rhs.IsInt32()) return kInt32;
      if (lhs.IsIntegralType() && rhs.IsIntegralType()) return kInt64;
      if (lhs.IsNumericType() && rhs.IsNumericType()) return kDouble;
      break;
    default:
      return InternalError(
          "Bug in InferResultType() calling GetBinaryOpType()");
  }
  return InvalidArgumentError(StrCat(
      "Cannot apply the ", Ast::TypeToString(op), " operator to operands of"
      " type ", lhs.ToString(), " and ", rhs.ToString()));
}

// The non-recursive, single-AST-object version of InferResultTypes().
// The |ast| object is partially deconstructed at this point, with all of its
// Ast children converted to TypedAst already and passed in separately, through
// |lhs|, |rhs|, and |values|.
StatusOr<AstType> InferResultType(
    const Ast &ast, const TypedAst *lhs, const TypedAst *rhs,
    const std::vector<std::unique_ptr<TypedAst>> &values,
    ProtoPool *pool, const Db *db, const Vars *vars)
  SHARED_LOCKS_REQUIRED(db->mu) {
  switch (ast.type) {
    case Ast::ERROR:
      return InternalError("Cannot get result type of AST of type ERROR");
    case Ast::SHOW_TABLES:
      return GetShowTablesType(db);
    case Ast::DESCRIBE_TABLE:
      return GetDescribeTableType(db);
    case Ast::CREATE_TABLE:
    case Ast::CREATE_INDEX:
    case Ast::DROP_TABLE:
    case Ast::DROP_INDEX:
    case Ast::INSERT:
    case Ast::UPDATE:
    case Ast::INDEX_SCAN_BOUND_EXCLUSIVE:
    case Ast::INDEX_SCAN_BOUND_INCLUSIVE:
      return AstType::Void();
    case Ast::SINGLE_EMPTY_ROW:
      return AstType::RepeatedMessage(
          Empty::default_instance().GetDescriptor());
    case Ast::TABLE_SCAN:
      return GetTableScanType(ast.table_name(), db);
    case Ast::INDEX_SCAN:
      return GetIndexScanType(ast.index_name(), db);
    case Ast::VALUE:
      return AstType::Scalar(ast.value().type.type);  // FIXME: handle AstType
    case Ast::VAR:
      return vars->GetVarType(ast.var());
    case Ast::FUNC:
      return GetFuncType(ast.var(), values, vars);
    case Ast::FILTER:
      return rhs->result_type;
    case Ast::GROUP_BY:
      return GetGroupByType(*lhs, ast.column_indices());
    case Ast::ORDER_BY:
      return lhs->result_type;
    case Ast::MAP:
      return GetMapType(ast, values, pool);
    case Ast::OP_NOT:
      // Special case when OP_NOT used together with EXISTS
      if (!lhs && !rhs) return AstType::Scalar(FieldDescriptor::TYPE_BOOL);
    case Ast::OP_BITWISE_NOT:
      if (lhs) return InternalError("Binary version of a unary operator?!");
      [[clang::fallthrough]];
    case Ast::OP_MINUS:
      if (!lhs) return GetUnaryOpType(ast.type, rhs->result_type);
      [[clang::fallthrough]];
    case Ast::OP_IN:
    case Ast::OP_LIKE:
    case Ast::OP_OR:
    case Ast::OP_AND:
    case Ast::OP_EQ:
    case Ast::OP_LT:
    case Ast::OP_GT:
    case Ast::OP_LE:
    case Ast::OP_GE:
    case Ast::OP_NE:
    case Ast::OP_PLUS:
    case Ast::OP_BITWISE_AND:
    case Ast::OP_BITWISE_OR:
    case Ast::OP_BITWISE_XOR:
    case Ast::OP_MUL:
    case Ast::OP_DIV:
    case Ast::OP_MOD:
      return GetBinaryOpType(ast.type, lhs->result_type, rhs->result_type);
    case Ast::IF:
      return AstType::Void();
    case Ast::EXISTS:
      return AstType::Scalar(FieldDescriptor::TYPE_BOOL);
  }

  return InvalidArgumentError("Wrong Ast.type passed to InferResultType");
}

}  // namespace

::util::StatusOr<std::unique_ptr<TypedAst>> InferResultTypes(
    std::unique_ptr<Ast> &&ast, ProtoPool *pool, const Db *db,
    const Vars *vars) {
  // TODO: Implement proper handling of IF statements.
  // IF statement is not handled here properly, f.ex.
  // CREATE ... IF NOT EXISTS statement will have
  // result type of IF, not CREATE

  // Recurse on the right child.
  std::unique_ptr<TypedAst> lhs = nullptr, rhs = nullptr;
  if (ast->rhs()) {
    StatusOr<std::unique_ptr<TypedAst>> so =
        InferResultTypes(std::move(ast->rhs_), pool, db, vars);
    if (!so.ok()) return so.status();
    rhs = std::move(so.ValueOrDie());
  }

  // Figure out the var context created by the table, if relevant.
  std::unique_ptr<Vars> table_vars = nullptr;
  if (ast->type == Ast::UPDATE) {
    Table *t = db->FindTable(ast->table_name());
    if (!t) return NotFoundError(StrCat(
        "Table ", ast->table_name(), " not found in database ", db->name));
    table_vars = vars->Branch(t->type);
  }

  // Figure out the var context created by the RHS.
  std::unique_ptr<Vars> rhs_vars = nullptr;
  if (ast->type == Ast::FILTER || ast->type == Ast::MAP) {
    if (!rhs) return InternalError(StrCat(
        "Missing RHS for ", Ast::TypeToString(ast->type)));
    if (!rhs->result_type.IsRepeatedMessage()) return InternalError(StrCat(
        "RHS of ", Ast::TypeToString(ast->type), " is not a Proto[]"));
    rhs_vars = vars->Branch(rhs->result_type.d);
  }

  const Vars *const effective_vars =
      table_vars ? table_vars.get() : (rhs_vars ? rhs_vars.get() : vars);

  // Recurse on the left child with a possibly different var context.
  if (ast->lhs()) {
    StatusOr<std::unique_ptr<TypedAst>> so =
        InferResultTypes(std::move(ast->lhs_), pool, db, effective_vars);
    if (!so.ok()) return so.status();
    lhs = std::move(so.ValueOrDie());
  }
  std::vector<std::unique_ptr<TypedAst>> values(ast->values().size());
  for (size_t i = 0; i < values.size(); ++i) {
    StatusOr<::std::unique_ptr<TypedAst>> so =
        InferResultTypes(std::move(ast->values_[i]), pool, db, effective_vars);
    if (!so.ok()) return so.status();
    values[i] = std::move(so.ValueOrDie());
  }

  // Infer the type now that we know the children's types.
  StatusOr<AstType> so = InferResultType(
      *ast, lhs.get(), rhs.get(), values, pool, db, effective_vars);
  if (!so.ok()) return so.status();

  // Cast |values| from unique_ptr<TypedAst> back to unique_ptr<Ast>.
  for (size_t i = 0; i < values.size(); ++i)
    ast->values_[i] = std::move(values[i]);

  return std::unique_ptr<TypedAst>(new TypedAst(
      ast->type,
      std::move(ast->table_name_),
      std::move(ast->index_name_),
      std::move(lhs),
      std::move(rhs),
      std::move(ast->value_),
      std::move(ast->columns_),
      std::move(ast->column_types_),
      std::move(ast->values_),
      std::move(ast->var_),
      std::move(ast->column_indices_),
      so.ValueOrDie()));
}

}  // namespace sfdb
