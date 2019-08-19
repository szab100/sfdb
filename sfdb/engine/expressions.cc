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
#include "sfdb/engine/expressions.h"

#include <vector>

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/base/value.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::google::protobuf::FieldDescriptor;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::OutOfRangeError;
using ::util::StatusOr;
using ::util::UnimplementedError;

StatusOr<Value> ExecuteUnaryOp(Ast::Type op, const Value &v) {
  static const auto TYPE_BOOL = FieldDescriptor::TYPE_BOOL;
  static const auto TYPE_INT64 = FieldDescriptor::TYPE_INT64;
  static const auto TYPE_DOUBLE = FieldDescriptor::TYPE_DOUBLE;

  if (v.type.is_void) return InvalidArgumentError("Cannot negate VOID");
  if (v.type.is_repeated)
    return UnimplementedError("Cannot negate a repeated type");

  if (op == Ast::OP_MINUS) {
    if (v.type.type == TYPE_INT64) return Value::Int64(-v.i64);
    if (v.type.type == TYPE_DOUBLE) return Value::Double(-v.dbl);
    return InvalidArgumentError(StrCat(
        "Cannot negate a value of type ", v.type.ToString()));
  }
  if (op == Ast::OP_NOT) {
    StatusOr<Value> so = v.CastTo(TYPE_BOOL);
    if (!so.ok()) return so.status();
    return Value::Bool(!so.ValueOrDie().boo);
  }
  if (op == Ast::OP_BITWISE_NOT) {
    StatusOr<Value> so = v.CastTo(TYPE_INT64);
    if (!so.ok()) return so.status();
    return Value::Int64(!so.ValueOrDie().i64);
  }
  return InternalError(StrCat(
      "Executing a unary op of type ", Ast::TypeToString(op)));
}

StatusOr<Value> ExecuteBoolBinaryOp(
    Ast::Type op, const Value &lhs, const Value &rhs) {
  StatusOr<Value> a = lhs.CastTo(FieldDescriptor::TYPE_BOOL);
  if (!a.ok()) return a.status();
  StatusOr<Value> b = rhs.CastTo(FieldDescriptor::TYPE_BOOL);
  if (!b.ok()) return b.status();
  switch (op) {
    case Ast::OP_OR:
      return Value::Bool(a.ValueOrDie().boo || b.ValueOrDie().boo);
    case Ast::OP_AND:
      return Value::Bool(a.ValueOrDie().boo && b.ValueOrDie().boo);
    default:
      return InternalError(StrCat(
          "Executing a bool binary op of type ", Ast::TypeToString(op)));
  }
}

StatusOr<Value> ExecuteStringBinaryOp(
    Ast::Type op, const Value &lhs, const Value &rhs) {
  StatusOr<Value> a = lhs.CastTo(FieldDescriptor::TYPE_STRING);
  if (!a.ok()) return a.status();
  StatusOr<Value> b = rhs.CastTo(FieldDescriptor::TYPE_STRING);
  if (!b.ok()) return b.status();
  switch (op) {
    case Ast::OP_PLUS:
      return Value::String(a.ValueOrDie().str + b.ValueOrDie().str);
    case Ast::OP_EQ:
      return Value::Bool(a.ValueOrDie().str == b.ValueOrDie().str);
    case Ast::OP_LT:
      return Value::Bool(a.ValueOrDie().str < b.ValueOrDie().str);
    case Ast::OP_GT:
      return Value::Bool(a.ValueOrDie().str > b.ValueOrDie().str);
    case Ast::OP_LE:
      return Value::Bool(a.ValueOrDie().str <= b.ValueOrDie().str);
    case Ast::OP_GE:
      return Value::Bool(a.ValueOrDie().str >= b.ValueOrDie().str);
    case Ast::OP_NE:
      return Value::Bool(a.ValueOrDie().str != b.ValueOrDie().str);
    default:
      return InternalError(StrCat(
          "Executing a string binary op of type ", Ast::TypeToString(op)));
  }
}

StatusOr<Value> ExecuteDoubleBinaryOp(
    Ast::Type op, const Value &lhs, const Value &rhs) {
  StatusOr<Value> a = lhs.CastTo(FieldDescriptor::TYPE_DOUBLE);
  if (!a.ok()) return a.status();
  StatusOr<Value> b = rhs.CastTo(FieldDescriptor::TYPE_DOUBLE);
  if (!b.ok()) return b.status();
  switch (op) {
    case Ast::OP_PLUS:
      return Value::Double(a.ValueOrDie().dbl + b.ValueOrDie().dbl);
    case Ast::OP_EQ:
      return Value::Bool(a.ValueOrDie().dbl == b.ValueOrDie().dbl);
    case Ast::OP_LT:
      return Value::Bool(a.ValueOrDie().dbl < b.ValueOrDie().dbl);
    case Ast::OP_GT:
      return Value::Bool(a.ValueOrDie().dbl > b.ValueOrDie().dbl);
    case Ast::OP_LE:
      return Value::Bool(a.ValueOrDie().dbl <= b.ValueOrDie().dbl);
    case Ast::OP_GE:
      return Value::Bool(a.ValueOrDie().dbl >= b.ValueOrDie().dbl);
    case Ast::OP_NE:
      return Value::Bool(a.ValueOrDie().dbl != b.ValueOrDie().dbl);
    case Ast::OP_MINUS:
      return Value::Double(a.ValueOrDie().dbl - b.ValueOrDie().dbl);
    case Ast::OP_MUL:
      return Value::Double(a.ValueOrDie().dbl * b.ValueOrDie().dbl);
    case Ast::OP_DIV:
      return Value::Double(a.ValueOrDie().dbl / b.ValueOrDie().dbl);
    default:
      return InternalError(StrCat(
          "Executing a double binary op of type ", Ast::TypeToString(op)));
  }
}

StatusOr<Value> ExecuteInt64BinaryOp(
    Ast::Type op, const Value &lhs, const Value &rhs) {
  StatusOr<Value> a = lhs.CastTo(FieldDescriptor::TYPE_INT64);
  if (!a.ok()) return a.status();
  StatusOr<Value> b = rhs.CastTo(FieldDescriptor::TYPE_INT64);
  if (!b.ok()) return b.status();
  switch (op) {
    case Ast::OP_PLUS:
      return Value::Int64(a.ValueOrDie().i64 + b.ValueOrDie().i64);
    case Ast::OP_EQ:
      return Value::Bool(a.ValueOrDie().i64 == b.ValueOrDie().i64);
    case Ast::OP_LT:
      return Value::Bool(a.ValueOrDie().i64 < b.ValueOrDie().i64);
    case Ast::OP_GT:
      return Value::Bool(a.ValueOrDie().i64 > b.ValueOrDie().i64);
    case Ast::OP_LE:
      return Value::Bool(a.ValueOrDie().i64 <= b.ValueOrDie().i64);
    case Ast::OP_GE:
      return Value::Bool(a.ValueOrDie().i64 >= b.ValueOrDie().i64);
    case Ast::OP_NE:
      return Value::Bool(a.ValueOrDie().i64 != b.ValueOrDie().i64);
    case Ast::OP_MINUS:
      return Value::Int64(a.ValueOrDie().i64 - b.ValueOrDie().i64);
    case Ast::OP_MUL:
      return Value::Int64(a.ValueOrDie().i64 * b.ValueOrDie().i64);
    case Ast::OP_DIV:
      if (!b.ValueOrDie().i64) return OutOfRangeError("Division by zero");
      return Value::Int64(a.ValueOrDie().i64 / b.ValueOrDie().i64);
    case Ast::OP_BITWISE_AND:
      return Value::Int64(a.ValueOrDie().i64 & b.ValueOrDie().i64);
    case Ast::OP_BITWISE_OR:
      return Value::Int64(a.ValueOrDie().i64 | b.ValueOrDie().i64);
    case Ast::OP_BITWISE_XOR:
      return Value::Int64(a.ValueOrDie().i64 ^ b.ValueOrDie().i64);
    case Ast::OP_MOD:
      if (!b.ValueOrDie().i64) return OutOfRangeError("Mod by zero");
      return Value::Int64(a.ValueOrDie().i64 % b.ValueOrDie().i64);
    default:
      return InternalError(StrCat(
          "Executing an int64 binary op of type ", Ast::TypeToString(op)));
  }
}

StatusOr<Value> ExecuteBinaryOp(
    Ast::Type op, const Value &lhs, const Value &rhs) {
  if (lhs.type.is_void || rhs.type.is_void) return InvalidArgumentError(StrCat(
      "Cannot apply ", Ast::TypeToString(op), " to VOID"));
  if (lhs.type.is_repeated || rhs.type.is_repeated)
    return UnimplementedError(StrCat(
        "Cannot apply ", Ast::TypeToString(op), " to a repeated type"));

  switch (op) {
    case Ast::OP_IN:
    case Ast::OP_LIKE:
      return UnimplementedError("IN and LIKE are not implemented yet");
    case Ast::OP_OR:
    case Ast::OP_AND:
      return ExecuteBoolBinaryOp(op, lhs, rhs);
    case Ast::OP_PLUS:
    case Ast::OP_EQ:
    case Ast::OP_LT:
    case Ast::OP_GT:
    case Ast::OP_LE:
    case Ast::OP_GE:
    case Ast::OP_NE:
      if (lhs.type.type == FieldDescriptor::TYPE_STRING ||
          rhs.type.type == FieldDescriptor::TYPE_STRING)
        return ExecuteStringBinaryOp(op, lhs, rhs);
      [[clang::fallthrough]];
    case Ast::OP_MINUS:
    case Ast::OP_MUL:
    case Ast::OP_DIV:
      if (lhs.type.type == FieldDescriptor::TYPE_DOUBLE ||
          rhs.type.type == FieldDescriptor::TYPE_DOUBLE)
        return ExecuteDoubleBinaryOp(op, lhs, rhs);
      [[clang::fallthrough]];
    case Ast::OP_BITWISE_AND:
    case Ast::OP_BITWISE_OR:
    case Ast::OP_BITWISE_XOR:
    case Ast::OP_MOD:
      return ExecuteInt64BinaryOp(op, lhs, rhs);
    default:
      return InternalError(StrCat(
          "Executing a binary op of type ", Ast::TypeToString(op)));
  }
}

StatusOr<Value> ExecuteFunction(const TypedAst &ast, Vars *vars) {
  const Func *f = vars->GetFunc(ast.var());
  if (!f) return NotFoundError(StrCat("No function called ", ast.var()));

  std::vector<Value> args;
  for (size_t i = 0; i < ast.values().size(); ++i) {
    StatusOr<Value> so = ExecuteExpression(*ast.value(i), vars);
    if (!so.ok()) return so.status();
    args.push_back(so.ValueOrDie());
  }

  return (*f)(args);
}

}  // namespace

StatusOr<Value> ExecuteExpression(const TypedAst &ast, Vars *vars) {
  if (ast.type == Ast::VALUE) return ast.value();
  if (ast.type == Ast::VAR) return vars->GetVar(ast.var());
  if (ast.type == Ast::FUNC) return ExecuteFunction(ast, vars);

  if (!ast.rhs()) return InternalError(StrCat(
      "Expression of type ", Ast::TypeToString(ast.type), " without a RHS"));
  StatusOr<Value> rhs = ExecuteExpression(*ast.rhs(), vars);
  if (!rhs.ok()) return rhs.status();

  if (Ast::IsUnaryOp(ast.type))
    return ExecuteUnaryOp(ast.type, rhs.ValueOrDie());
  if (!Ast::IsBinaryOp(ast.type)) return InternalError(StrCat(
      "Trying to execute type ", Ast::TypeToString(ast.type),
      " as a binary operator"));

  if (!ast.lhs()) return InternalError(StrCat(
      "Expression of type ", Ast::TypeToString(ast.type), " without a LHS"));
  StatusOr<Value> lhs = ExecuteExpression(*ast.lhs(), vars);
  if (!lhs.ok()) return lhs.status();

  return ExecuteBinaryOp(ast.type, lhs.ValueOrDie(), rhs.ValueOrDie());
}

}  // namespace sfdb
