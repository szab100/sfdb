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
#ifndef SFDB_BASE_VALUE_H_
#define SFDB_BASE_VALUE_H_

#include <ostream>
#include <string>

#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/base/ast_type.h"
#include "util/task/statusor.h"
#include "util/types/integral_types.h"

namespace sfdb {

// A value that can be the result of executing an AST.
//
// Either a single immutable value, or a stream of immutable values.
// May point to Descriptor objects that must outlive this object.
struct Value {
  const AstType type;

  // TODO: wrap these in a union?
  const bool boo;  // if type.type is TYPE_BOOL
  const int64 i64;  // if type.type is TYPE_INT64
  const double dbl;  // if type.type is TYPE_DOUBLE
  const std::string str;  // if type.type is TYPE_STRING
  std::unique_ptr<::google::protobuf::Message> msg;  // if type.type is TYPE_MESSAGE

  Value(const Value &v);
  Value(Value &&v);
  const Value &operator=(const Value &v) = delete;
  const Value &operator=(Value &&v) = delete;

  ::util::StatusOr<Value> CastTo(const AstType &new_type) const;

  // Casts a scalar to another scalar.
  ::util::StatusOr<Value> CastTo(
      ::google::protobuf::FieldDescriptor::Type new_type) const;

  std::string ToString() const;

  // Factory functions.
  static Value Void() { return Value(); }
  static Value Bool(bool boo) { return Value(boo); }
  static Value Int64(int64 i64) { return Value(i64); }
  static Value Double(double dbl) { return Value(dbl); }
  static Value String(::absl::string_view str) { return Value(str); }
  static Value Message(std::unique_ptr<::google::protobuf::Message> &&msg) {
    return Value(std::move(msg));
  }

 private:
  Value();
  Value(bool boo);
  Value(int64 i64);
  Value(double dbl);
  Value(::absl::string_view str);
  Value(std::unique_ptr<::google::protobuf::Message> &&msg);
};

bool operator==(const Value &a, const Value &b);
::std::ostream &operator<<(std::ostream &out, const Value &v);

}  // namespace sfdb

#endif  // SFDB_BASE_VALUE_H_
