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
#include "sfdb/base/ast_type.h"

#include <ostream>
#include <string>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::EndsWith;
using ::absl::StartsWith;
using ::absl::StrCat;
using ::absl::string_view;
using ::google::protobuf::Descriptor;
using ::google::protobuf::EnumDescriptor;
using ::google::protobuf::FieldDescriptor;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::StatusOr;
using ::util::UnimplementedError;

bool AstType::IsInt32() const {
  if (is_repeated || is_void) return false;
  switch (type) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_SFIXED32:
      return true;
    default:
      return false;
  }
}

bool AstType::IsIntegralType() const {
  if (is_repeated || is_void) return false;
  switch (type) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_FIXED64:
    case FieldDescriptor::TYPE_SFIXED32:
    case FieldDescriptor::TYPE_SFIXED64:
      return true;
    default:
      return false;
  }
}

bool AstType::IsNumericType() const {
  if (is_repeated || is_void) return false;
  switch (type) {
    case FieldDescriptor::TYPE_DOUBLE:
    case FieldDescriptor::TYPE_FLOAT:
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_FIXED64:
    case FieldDescriptor::TYPE_SFIXED32:
    case FieldDescriptor::TYPE_SFIXED64:
      return true;
    default:
      return false;
  }
}

bool AstType::IsString() const {
  if (is_repeated || is_void) return false;
  return type == FieldDescriptor::TYPE_STRING
      || type == FieldDescriptor::TYPE_BYTES;
}

bool AstType::IsMessage() const {
  return !is_repeated && !is_void && type == FieldDescriptor::TYPE_MESSAGE;
}

bool AstType::IsRepeatedMessage() const {
  return is_repeated && type == FieldDescriptor::TYPE_MESSAGE;
}

bool AstType::IsCastableTo(const AstType &to) const {
  // Deal with void and repeated types first.
  if (to.is_repeated != is_repeated) return false;
  if (to.is_void) return true;

  switch (to.type) {
    case FieldDescriptor::TYPE_GROUP:
      return type == to.type;
    case FieldDescriptor::TYPE_DOUBLE:
    case FieldDescriptor::TYPE_FLOAT:
      return IsNumericType();
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_FIXED64:
    case FieldDescriptor::TYPE_SFIXED32:
    case FieldDescriptor::TYPE_SFIXED64:
      return IsIntegralType();
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_BYTES:
    case FieldDescriptor::TYPE_BOOL:
      return type == FieldDescriptor::TYPE_STRING
          || type == FieldDescriptor::TYPE_BYTES
          || type == FieldDescriptor::TYPE_BOOL
          || IsNumericType();
    case FieldDescriptor::TYPE_MESSAGE:
      return type == to.type && d == to.d;
    case FieldDescriptor::TYPE_ENUM:
      return type == to.type && ed == to.ed;
  }

  return false;
}

std::string AstType::ToString() const {
  return (is_void ? "void" : TypeToString(type))
      + (type == FieldDescriptor::TYPE_MESSAGE ?
         StrCat("<", d->full_name(), ">") : "")
      + (type == FieldDescriptor::TYPE_ENUM ?
         StrCat("<", ed->full_name(), ">") : "")
      + (is_repeated ? "[]" : "");
}

// static
StatusOr<AstType> AstType::FromString(string_view s, const ProtoPool &pool) {
  if (EndsWith(s, "[]")) {
    StatusOr<AstType> so = FromString(s.substr(0, s.size() - 2), pool);
    if (!so.ok()) return so.status();
    const AstType &t = so.ValueOrDie();
    if (t.is_repeated)
      return InvalidArgumentError("Lists of lists unsupported");
    return AstType{t.is_void, t.type, true, t.d, t.ed};
  }

  if (s == "void") return Void();

  if (StartsWith(s, "Proto<") && EndsWith(s, ">")) {
    const string_view proto_class = s.substr(6, s.size() - 7);
    const Descriptor *d = pool.FindMessageTypeByName(proto_class);
    if (!d) return NotFoundError(StrCat("Unknown message type: ", proto_class));
    return Message(d);
  }

  if (StartsWith(s, "Enum<") && EndsWith(s, ">")) {
    const string_view enum_class = s.substr(5, s.size() - 6);
    const EnumDescriptor *ed = pool.FindEnumTypeByName(enum_class);
    if (!ed) return NotFoundError(StrCat("Unknown enum type: ", enum_class));
    return Enum(ed);
  }

  StatusOr<FieldDescriptor::Type> so = TypeFromString(s);
  if (!so.ok()) return so.status();
  return Scalar(so.ValueOrDie());
}

// static
std::string AstType::TypeToString(FieldDescriptor::Type type) {
  //TODO: find this function in the proto2 libs?
  switch (type) {
    case FieldDescriptor::TYPE_GROUP: return "ProtoGroup";
    case FieldDescriptor::TYPE_DOUBLE: return "double";
    case FieldDescriptor::TYPE_FLOAT: return "float";
    case FieldDescriptor::TYPE_INT32: return "int32";
    case FieldDescriptor::TYPE_INT64: return "int64";
    case FieldDescriptor::TYPE_SINT32: return "sint32";
    case FieldDescriptor::TYPE_SINT64: return "sint64";
    case FieldDescriptor::TYPE_UINT32: return "uint32";
    case FieldDescriptor::TYPE_UINT64: return "uint64";
    case FieldDescriptor::TYPE_FIXED32: return "fixed32";
    case FieldDescriptor::TYPE_FIXED64: return "fixed53";
    case FieldDescriptor::TYPE_SFIXED32: return "sfixed32";
    case FieldDescriptor::TYPE_SFIXED64: return "sfixed64";
    case FieldDescriptor::TYPE_STRING: return "string";
    case FieldDescriptor::TYPE_BYTES: return "bytes";
    case FieldDescriptor::TYPE_BOOL: return "bool";
    case FieldDescriptor::TYPE_MESSAGE: return "Proto";
    case FieldDescriptor::TYPE_ENUM: return "Enum";
  }

  LOG(FATAL) << "Conversion to string is not defined "
    "for FieldDescriptor::Type of type " << static_cast<int>(type);

  return "";
}

// static
StatusOr<FieldDescriptor::Type> AstType::TypeFromString(string_view s) {
  if (s == "ProtoGroup") return UnimplementedError("Proto groups unsupported");
  if (s == "double") return FieldDescriptor::TYPE_DOUBLE;
  if (s == "float") return FieldDescriptor::TYPE_FLOAT;
  if (s == "int32") return FieldDescriptor::TYPE_INT32;
  if (s == "int64") return FieldDescriptor::TYPE_INT64;
  if (s == "sint32") return FieldDescriptor::TYPE_SINT32;
  if (s == "sint64") return FieldDescriptor::TYPE_SINT64;
  if (s == "uint32") return FieldDescriptor::TYPE_UINT32;
  if (s == "uint64") return FieldDescriptor::TYPE_UINT64;
  if (s == "fixed32") return FieldDescriptor::TYPE_FIXED32;
  if (s == "fixed64") return FieldDescriptor::TYPE_FIXED64;
  if (s == "sfixed32") return FieldDescriptor::TYPE_SFIXED32;
  if (s == "sfixed64") return FieldDescriptor::TYPE_SFIXED64;
  if (s == "string") return FieldDescriptor::TYPE_STRING;
  if (s == "bytes") return FieldDescriptor::TYPE_BYTES;
  if (s == "bool") return FieldDescriptor::TYPE_BOOL;
  if (s == "Proto") return FieldDescriptor::TYPE_MESSAGE;
  if (s == "Enum") return FieldDescriptor::TYPE_ENUM;
  return InvalidArgumentError(StrCat("Unknown type: ", s));
}

bool operator==(const AstType &a, const AstType &b) {
  return a.is_void == b.is_void
      && a.type == b.type
      && a.is_repeated == b.is_repeated
      && a.d == b.d
      && a.ed == b.ed;
}

bool operator!=(const AstType &a, const AstType &b) { return !(a == b); }

std::ostream &operator<<(std::ostream &out, const AstType &rt) {
  return out << rt.ToString();
}

}  // namespace sfdb
