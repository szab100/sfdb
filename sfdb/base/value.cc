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
#include "sfdb/base/value.h"

#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "sfdb/proto/dup_message.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::absl::string_view;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::util::MessageDifferencer;
using ::util::InvalidArgumentError;
using ::util::InternalError;
using ::util::StatusOr;
using ::util::UnimplementedError;

StatusOr<Value> CastToBool(const Value &v) {
  switch (v.type.type) {
    case FieldDescriptor::TYPE_INT64: return Value::Bool(v.i64);
    case FieldDescriptor::TYPE_DOUBLE: return Value::Bool(v.dbl);
    case FieldDescriptor::TYPE_STRING: return Value::Bool(v.str.size());
    case FieldDescriptor::TYPE_MESSAGE:
      return InvalidArgumentError(StrCat(
          "Cannot cast a ", AstType::TypeToString(v.type.type), " to a proto"));
    default: return UnimplementedError(StrCat("Bad type: ", v.type.ToString()));
  }
}

StatusOr<Value> CastToInt64(const Value &v) {
  switch (v.type.type) {
    case FieldDescriptor::TYPE_BOOL: return Value::Int64(!!v.boo);
    case FieldDescriptor::TYPE_DOUBLE:
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_MESSAGE:
      return InvalidArgumentError(StrCat(
          "Cannot cast a ", AstType::TypeToString(v.type.type), " to a proto"));
    default: return UnimplementedError(StrCat("Bad type: ", v.type.ToString()));
  }
}

StatusOr<Value> CastToDouble(const Value &v) {
  switch (v.type.type) {
    case FieldDescriptor::TYPE_BOOL: return Value::Double(!!v.boo);
    case FieldDescriptor::TYPE_INT64: return Value::Double(v.i64);
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_MESSAGE:
      return InvalidArgumentError(StrCat(
          "Cannot cast a ", AstType::TypeToString(v.type.type), " to a proto"));
    default: return UnimplementedError(StrCat("Bad type: ", v.type.ToString()));
  }
}

StatusOr<Value> CastToString(const Value &v) {
  switch (v.type.type) {
    case FieldDescriptor::TYPE_BOOL: return Value::String(StrCat(v.boo));
    case FieldDescriptor::TYPE_INT64: return Value::String(StrCat(v.i64));
    case FieldDescriptor::TYPE_DOUBLE: return Value::String(StrCat(v.dbl));
    case FieldDescriptor::TYPE_MESSAGE:
      return Value::String(v.msg->SerializeAsString());
    default: return UnimplementedError(StrCat("Bad type: ", v.type.ToString()));
  }
}

StatusOr<Value> CastToMessage(const Value &v) {
  switch (v.type.type) {
    case FieldDescriptor::TYPE_BOOL:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_DOUBLE:
    case FieldDescriptor::TYPE_STRING:
      return InvalidArgumentError(StrCat(
          "Cannot cast a ", AstType::TypeToString(v.type.type), " to a proto"));
    default: return UnimplementedError(StrCat("Bad type: ", v.type.ToString()));
  }
}

}  // namespace

Value::Value(const Value &v) :
    type(v.type), boo(v.boo), i64(v.i64), dbl(v.dbl), str(v.str),
    msg(DupMessage(v.msg.get())) {}

Value::Value(Value &&v) :
    type(v.type), boo(v.boo), i64(v.i64), dbl(v.dbl), str(v.str),
    msg(std::move(v.msg)) {}

Value::Value() :
    type(AstType::Void()), boo(false), i64(0), dbl(0.), str("") {}
Value::Value(bool boo) :
    type(AstType::Scalar(FieldDescriptor::TYPE_BOOL)), boo(boo), i64(0),
    dbl(0.), str("") {}
Value::Value(int64 i64) :
    type(AstType::Scalar(FieldDescriptor::TYPE_INT64)), boo(false), i64(i64),
    dbl(0.), str("") {}
Value::Value(double dbl) :
    type(AstType::Scalar(FieldDescriptor::TYPE_DOUBLE)), boo(false), i64(0),
    dbl(dbl), str("") {}
Value::Value(string_view str) :
    type(AstType::Scalar(FieldDescriptor::TYPE_STRING)), boo(false), i64(0),
    dbl(0.), str(std::string(str)) {}
Value::Value(std::unique_ptr<::google::protobuf::Message> &&msg) :
    type(AstType::Message(msg->GetDescriptor())), boo(false), i64(0), dbl(0.),
    str(""), msg(std::move(msg)) {}

std::string Value::ToString() const {
  if (type.is_void) return "VOID";
  if (type.is_repeated) return StrCat(type.ToString(), "{...}");
  if (type.type == FieldDescriptor::TYPE_BOOL) return boo ? "TRUE" : "FALSE";
  if (type.IsIntegralType()) return StrCat(i64);
  if (type.IsNumericType()) return absl::StrFormat("%e", dbl);
  if (type.IsString()) return StrCat("\"", ::absl::CEscape(str), "\"");
  if (type.type == FieldDescriptor::TYPE_MESSAGE) return StrCat(
      "Proto<", msg->GetDescriptor()->full_name(), ">{",
      msg->ShortDebugString(), "}");
  return "[INTERNAL ERROR]";
}

StatusOr<Value> Value::CastTo(const AstType &new_type) const {
  if (type == new_type) return *this;
  if (new_type.is_void) return Value::Void();
  if (new_type.is_repeated != type.is_repeated) return InvalidArgumentError(
      "Cannot cast between a repeated and a non-repeated type");
  if (new_type.is_repeated) return UnimplementedError("Cannot cast repeated");
  return CastTo(new_type.type);
}

StatusOr<Value> Value::CastTo(FieldDescriptor::Type new_type) const {
  if (type.is_void) return InvalidArgumentError(StrCat(
      "Cannot cast a VOID to ", AstType::TypeToString(new_type)));
  if (type.is_repeated) return InvalidArgumentError(StrCat(
      "Cannot cast a ", type.ToString(), " to ",
      AstType::TypeToString(new_type)));
  if (type.type == new_type) return *this;
  switch (new_type) {
    case FieldDescriptor::TYPE_BOOL: return CastToBool(*this);
    case FieldDescriptor::TYPE_INT64: return CastToInt64(*this);
    case FieldDescriptor::TYPE_DOUBLE: return CastToDouble(*this);
    case FieldDescriptor::TYPE_STRING: return CastToString(*this);
    case FieldDescriptor::TYPE_MESSAGE: return CastToMessage(*this);
    default: return UnimplementedError(StrCat(
        "Unsupported Value type: ", AstType::TypeToString(new_type)));
  }
}

bool operator==(const Value &a, const Value &b) {
  if (a.type != b.type) return false;
  if (a.type.is_repeated || b.type.is_repeated) return false;
  switch (a.type.type) {
    case FieldDescriptor::TYPE_BOOL: return a.boo == b.boo;
    case FieldDescriptor::TYPE_INT64: return a.i64 == b.i64;
    case FieldDescriptor::TYPE_DOUBLE: return a.dbl == b.dbl;
    case FieldDescriptor::TYPE_STRING: return a.str == b.str;
    case FieldDescriptor::TYPE_MESSAGE:
        return a.msg->GetDescriptor() == b.msg->GetDescriptor() &&
            MessageDifferencer::Equivalent(*a.msg, *b.msg);
    default: return false;  // TODO: implement
  }
}

::std::ostream &operator<<(::std::ostream &out, const Value &v) {
  return out << v.ToString();
}

}  // namespace sfdb
