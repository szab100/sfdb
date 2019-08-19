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
#ifndef SFDB_BASE_AST_TYPE_H_
#define SFDB_BASE_AST_TYPE_H_

#include <ostream>
#include <string>

#include "absl/strings/string_view.h"
#include "glog/logging.h"
#include "google/protobuf/descriptor.h"
#include "sfdb/proto/pool.h"
#include "util/task/statusor.h"

namespace sfdb {

// The data type of an AST, i.e. any data type in the database.
//
// Immutable.
// May point to descriptors that must outlive this object.
struct AstType {
  const bool is_void;  // For CREATE statements, for example.
  const ::google::protobuf::FieldDescriptor::Type type;
  const bool is_repeated;  // if the result is a list of values.
  const ::google::protobuf::Descriptor *d;  // if type == TYPE_MESSAGE
  const ::google::protobuf::EnumDescriptor *ed;  // if type == TYPE_ENUM

  bool IsInt32() const;  // a non-repeated integer that fits into 32 bits
  bool IsIntegralType() const;  // a non-repeated integer
  bool IsNumericType() const;  // a non-repeated number
  bool IsString() const;  // a non-repeated string or bytes
  bool IsMessage() const;  // a non-repeated Message
  bool IsRepeatedMessage() const;
  bool IsCastableTo(const AstType &to) const;

  // string conversion
  std::string ToString() const;
  static ::util::StatusOr<AstType> FromString(
      ::absl::string_view s, const ProtoPool &pool);
  static std::string TypeToString(::google::protobuf::FieldDescriptor::Type type);
  static ::util::StatusOr<::google::protobuf::FieldDescriptor::Type> TypeFromString(
      ::absl::string_view s);

  // factory functions
  static AstType Void() {
    return {true, static_cast<::google::protobuf::FieldDescriptor::Type>(0), false,
        nullptr, nullptr};
  }
  static AstType Scalar(::google::protobuf::FieldDescriptor::Type type) {
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_ENUM);
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_GROUP);
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE);
    return {false, type, false, nullptr, nullptr};
  }
  static AstType RepeatedScalar(::google::protobuf::FieldDescriptor::Type type) {
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_ENUM);
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_GROUP);
    CHECK_NE(type, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE);
    return {false, type, true, nullptr, nullptr};
  }
  static AstType Enum(const ::google::protobuf::EnumDescriptor *ed) {
    return {false, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE, false, nullptr, ed};
  }
  static AstType RepeatedEnum(const ::google::protobuf::EnumDescriptor *ed) {
    return {false, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE, true, nullptr, ed};
  }
  static AstType Message(const ::google::protobuf::Descriptor *d) {
    return {false, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE, false, d, nullptr};
  }
  static AstType RepeatedMessage(const ::google::protobuf::Descriptor *d) {
    return {false, ::google::protobuf::FieldDescriptor::TYPE_MESSAGE, true, d, nullptr};
  }

  AstType() = delete;
  AstType(const AstType &t) = default;
};

bool operator==(const AstType &a, const AstType &b);
bool operator!=(const AstType &a, const AstType &b);
std::ostream &operator<<(std::ostream &out, const AstType &rt);

}  // namespace sfdb

#endif  // SFDB_BASE_AST_TYPE_H_
